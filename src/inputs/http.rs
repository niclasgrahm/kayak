//! The input a pipeline is *posted to*, and the registry the HTTP handler
//! finds it through.
//!
//! Every other input reaches out to something. This one is reached: building it
//! registers a channel under the pipeline's id in [`Inboxes`], and
//! `POST /api/pipelines/{id}/messages` looks it up there. That indirection is
//! what keeps the axum layer and the runtime apart — the handler never sees an
//! `InputSource`, only a name it can send a batch to.
//!
//! The registration is dropped with the input, i.e. when the run loop that owns
//! it ends, so a deleted pipeline stops accepting posts without anyone having
//! to remember to say so. Doing that safely is what [`Inbox::token`] is for: a
//! revert can build the new pipeline before the old one's task has finished
//! dying, and an unconditional `remove` in `Drop` would then tear down the
//! *new* registration.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard};

use anyhow::Result;
use kayak_core::config::HttpInputConfig;
use tokio::sync::mpsc;

use crate::BuildCtx;
use crate::inputs::envelope::{Envelope, Meta};
use crate::inputs::{BuildInput, InputSource, MessageBatch};
use crate::state::PipelineId;
use serde_json::Value;

/// How many posted batches wait for the run loop when the config doesn't say.
pub const DEFAULT_CAPACITY: usize = 1024;

/// Why a posted batch didn't reach a pipeline.
///
/// Two cases rather than one because they are two different things for the
/// client to do about it: nothing is listening (fix the request), or something
/// is listening and behind (send it again).
#[derive(Debug)]
pub enum IngestError {
    /// Nothing is registered under this id — the pipeline doesn't exist, or it
    /// has no `http` input.
    NoInbox(PipelineId),
    /// The pipeline's queue is full: it is not reading as fast as this is being
    /// posted.
    Full(PipelineId),
}

impl std::fmt::Display for IngestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoInbox(id) => write!(f, "pipeline '{id}' has no http input"),
            Self::Full(id) => write!(
                f,
                "pipeline '{id}' is not keeping up; its http input queue is full"
            ),
        }
    }
}

impl std::error::Error for IngestError {}

/// The request headers an `http` input will pass on, and the only ones.
///
/// An **allow-list, deliberately, and not a deny-list or a prefix rule.** A
/// header carrying a credential — `authorization`, `x-api-key`, a session
/// cookie — written into a file, an object store or a downstream topic is a
/// leak that outlives the request by years, and "everything starting with `x-`"
/// is exactly the rule that passes `x-api-key` through. So the set is fixed,
/// short, and made of headers that identify a request rather than authorise it.
pub const ALLOWED_HEADERS: &[&str] = &[
    "content-type",
    "user-agent",
    "x-request-id",
    "x-correlation-id",
    "traceparent",
];

/// What the handler knows about the request a batch was posted in.
///
/// Built at the HTTP layer and carried to the input through the channel,
/// because by the time the run loop sees the messages the request is long gone.
/// It is built whether or not the input has an `envelope` — one small struct per
/// post is not worth a second code path.
#[derive(Clone, Debug, Default)]
pub struct PostMeta {
    pub method: String,
    pub remote_addr: Option<String>,
    /// Already filtered to [`ALLOWED_HEADERS`]; nothing downstream filters
    /// again, so this is the one place the rule lives.
    pub headers: Vec<(String, String)>,
}

impl PostMeta {
    /// From a request's method, peer address and headers, keeping only the
    /// headers on the allow-list.
    pub fn new<I, N, V>(method: &str, remote_addr: Option<String>, headers: I) -> Self
    where
        I: IntoIterator<Item = (N, V)>,
        N: AsRef<str>,
        V: AsRef<str>,
    {
        let headers = headers
            .into_iter()
            .filter_map(|(name, value)| {
                let name = name.as_ref().to_ascii_lowercase();
                ALLOWED_HEADERS
                    .contains(&name.as_str())
                    .then(|| (name, value.as_ref().to_string()))
            })
            .collect();
        Self {
            method: method.to_string(),
            remote_addr,
            headers,
        }
    }

    /// This request as metadata fields.
    fn as_meta(&self) -> Meta {
        let mut headers = serde_json::Map::new();
        for (name, value) in &self.headers {
            headers.insert(name.clone(), Value::String(value.clone()));
        }
        vec![
            ("method", Value::String(self.method.clone())),
            (
                "remote_addr",
                self.remote_addr
                    .as_ref()
                    .map_or(Value::Null, |a| Value::String(a.clone())),
            ),
            ("headers", Value::Object(headers)),
        ]
    }
}

/// One post: the messages, and what the request they arrived in was.
struct Posted {
    batch: Arc<MessageBatch>,
    meta: PostMeta,
}

struct Inbox {
    /// Which registration this is. Only the holder of the matching token may
    /// remove it, so a late `Drop` from a torn-down pipeline can't unregister
    /// the one that has since taken its id.
    token: u64,
    tx: mpsc::Sender<Posted>,
}

/// Where the http inputs of the running pipelines can be reached, by id.
///
/// Held by `AppState` and handed to every build through [`BuildCtx`], the same
/// way the connections are. The lock is a `std::sync::Mutex` and is never held
/// across an `.await`: sending is a `try_send`, which doesn't have one.
#[derive(Default)]
pub struct Inboxes {
    inner: Mutex<HashMap<PipelineId, Inbox>>,
    next_token: AtomicU64,
}

impl Inboxes {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// A poisoned lock means a thread panicked while holding it. Nothing under
    /// this lock can leave the map half-updated, so recovering the guard is
    /// safe — the same rule `AppState`'s locks follow.
    fn lock(&self) -> MutexGuard<'_, HashMap<PipelineId, Inbox>> {
        self.inner.lock().unwrap_or_else(|poisoned| {
            tracing::warn!("http inbox lock was poisoned; recovering");
            poisoned.into_inner()
        })
    }

    /// Claim `pipeline_id`'s endpoint, giving back the input that reads what is
    /// posted to it.
    ///
    /// Fails if the id is already claimed: two http inputs on one pipeline
    /// would share an endpoint, and which of them a request reached would be
    /// arbitrary.
    fn register(
        self: &Arc<Self>,
        pipeline_id: PipelineId,
        capacity: usize,
        envelope: Envelope,
    ) -> Result<HttpInput> {
        let mut inboxes = self.lock();
        anyhow::ensure!(
            !inboxes.contains_key(&pipeline_id),
            "pipeline '{pipeline_id}' already has an http input; one pipeline is one endpoint"
        );
        let token = self.next_token.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = mpsc::channel(capacity);
        inboxes.insert(pipeline_id.clone(), Inbox { token, tx });
        drop(inboxes);
        Ok(HttpInput {
            pipeline_id,
            token,
            rx,
            envelope,
            inboxes: Arc::clone(self),
        })
    }

    /// Hand a batch to a pipeline's http input.
    ///
    /// `try_send` rather than an awaited send: the caller is an HTTP handler,
    /// and a full queue is something to tell the client about rather than
    /// something to hold a request open for.
    pub fn send(
        &self,
        pipeline_id: &str,
        batch: Arc<MessageBatch>,
        meta: PostMeta,
    ) -> Result<(), IngestError> {
        let sender = {
            let inboxes = self.lock();
            let Some(inbox) = inboxes.get(pipeline_id) else {
                return Err(IngestError::NoInbox(pipeline_id.to_string()));
            };
            inbox.tx.clone()
        };
        sender
            .try_send(Posted { batch, meta })
            .map_err(|_| IngestError::Full(pipeline_id.to_string()))
    }

    /// Whether a pipeline is accepting posts, without sending anything. What an
    /// empty post is answered from — it should still 404 on a pipeline that
    /// isn't listening rather than be accepted by nobody.
    pub fn check(&self, pipeline_id: &str) -> Result<(), IngestError> {
        if self.lock().contains_key(pipeline_id) {
            Ok(())
        } else {
            Err(IngestError::NoInbox(pipeline_id.to_string()))
        }
    }

    /// Take an endpoint down now, whoever holds it.
    ///
    /// What deleting a pipeline calls, so the endpoint stops accepting posts
    /// with the request that removed it rather than whenever the run loop's
    /// task gets round to dropping the input. Safe to do by name alone because
    /// the caller holds the pipelines lock: no replacement can have been
    /// registered yet, and the straggler's own `Drop` is a no-op once its token
    /// is gone.
    pub fn evict(&self, pipeline_id: &str) {
        self.lock().remove(pipeline_id);
    }

    /// The same for every endpoint at once — what a revert's teardown calls, so
    /// a run loop that outlives the grace period can't keep its id claimed
    /// against the pipeline about to be rebuilt under it.
    pub fn clear(&self) {
        self.lock().clear();
    }

    /// Give up a registration, if it is still the one that was claimed.
    fn unregister(&self, pipeline_id: &str, token: u64) {
        let mut inboxes = self.lock();
        if inboxes.get(pipeline_id).is_some_and(|i| i.token == token) {
            inboxes.remove(pipeline_id);
        }
    }

    /// How many pipelines are accepting posts. For tests and diagnostics.
    #[must_use]
    pub fn len(&self) -> usize {
        self.lock().len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl BuildInput for HttpInputConfig {
    fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn InputSource>> {
        let capacity = self.capacity.unwrap_or(DEFAULT_CAPACITY);
        anyhow::ensure!(capacity > 0, "an http input's `capacity` must be at least 1");
        let envelope = ctx.envelope("http", None);
        Ok(Box::new(ctx.inboxes.register(
            ctx.pipeline_id.clone(),
            capacity,
            envelope,
        )?))
    }
}

pub struct HttpInput {
    pipeline_id: PipelineId,
    token: u64,
    rx: mpsc::Receiver<Posted>,
    /// What this input attaches to each message, if the config asked for any.
    envelope: Envelope,
    inboxes: Arc<Inboxes>,
}

impl Drop for HttpInput {
    fn drop(&mut self) {
        self.inboxes.unregister(&self.pipeline_id, self.token);
    }
}

#[async_trait::async_trait]
impl InputSource for HttpInput {
    async fn next(&mut self) -> Result<Arc<MessageBatch>> {
        // The only sender is the one in the registry, which this input owns
        // through its `Drop` — so `None` means we have been dropped, which
        // can't be observed from inside `next`. Reported rather than
        // unreachable!(), since a future refactor could make it possible.
        let posted = self.rx.recv().await.ok_or_else(|| {
            anyhow::anyhow!("the http input of pipeline '{}' is gone", self.pipeline_id)
        })?;

        if !self.envelope.is_enabled() {
            return Ok(posted.batch);
        }
        // one request is one batch, so every message in it shares its metadata
        let meta = posted.meta.as_meta();
        let enveloped = posted
            .batch
            .iter()
            .filter_map(|message| {
                let out = self.envelope.apply((**message).clone(), meta.clone());
                if out.is_none() {
                    tracing::warn!(
                        "skipping a message posted to pipeline '{}': it is not a json object, \
                         so a `merge` envelope has nowhere to attach metadata",
                        self.pipeline_id
                    );
                }
                out.map(Arc::new)
            })
            .collect();
        Ok(Arc::new(enveloped))
    }
}

#[cfg(test)]
mod tests {
    use super::{IngestError, Inboxes, PostMeta};
    use crate::inputs::envelope::Envelope;
    use kayak_core::config::EnvelopeConfig;
    use crate::inputs::{InputSource, MessageBatch};
    use serde_json::json;
    use std::sync::Arc;

    fn batch(n: i64) -> Arc<MessageBatch> {
        Arc::new(vec![Arc::new(json!({ "n": n }))])
    }

    #[tokio::test]
    async fn a_posted_batch_arrives_at_the_input() -> anyhow::Result<()> {
        let inboxes = Arc::new(Inboxes::new());
        let mut input = inboxes.register("p1".to_string(), 4, Envelope::none())?;

        inboxes.send("p1", batch(1), PostMeta::default())?;

        let got = input.next().await?;
        assert_eq!(got.len(), 1);
        assert_eq!(got[0]["n"], json!(1));
        Ok(())
    }

    #[test]
    fn posting_to_a_pipeline_with_no_http_input_says_so() {
        let inboxes = Arc::new(Inboxes::new());
        assert!(matches!(
            inboxes.send("nobody", batch(1), PostMeta::default()),
            Err(IngestError::NoInbox(ref id)) if id == "nobody"
        ));
        assert!(matches!(inboxes.check("nobody"), Err(IngestError::NoInbox(_))));
    }

    /// Two http inputs on one pipeline would share one endpoint, so the second
    /// one doesn't get built.
    #[test]
    fn one_pipeline_can_only_claim_its_endpoint_once() {
        let inboxes = Arc::new(Inboxes::new());
        let _first = inboxes.register("p1".to_string(), 4, Envelope::none());
        assert!(inboxes.register("p1".to_string(), 4, Envelope::none()).is_err());
    }

    /// Past the queue's capacity a post is refused rather than held: the
    /// alternative is an HTTP request open until the pipeline catches up.
    #[test]
    fn a_full_queue_is_refused_rather_than_waited_on() {
        let inboxes = Arc::new(Inboxes::new());
        let _input = inboxes.register("p1".to_string(), 1, Envelope::none());

        assert!(inboxes.send("p1", batch(1), PostMeta::default()).is_ok());
        assert!(matches!(
            inboxes.send("p1", batch(2), PostMeta::default()),
            Err(IngestError::Full(ref id)) if id == "p1"
        ));
    }

    /// Eviction is by name and unconditional — the pipeline is being deleted,
    /// and the input's own `Drop` is then a no-op.
    #[test]
    fn an_endpoint_can_be_taken_down_before_its_input_is_dropped() {
        let inboxes = Arc::new(Inboxes::new());
        let input = inboxes.register("p1".to_string(), 4, Envelope::none());
        inboxes.evict("p1");

        assert!(matches!(
            inboxes.send("p1", batch(1), PostMeta::default()),
            Err(IngestError::NoInbox(_))
        ));
        drop(input);
        assert!(inboxes.is_empty());
    }

    /// The registration is the input's: dropping the run loop takes the
    /// endpoint down with it.
    #[test]
    fn dropping_the_input_gives_up_the_endpoint() {
        let inboxes = Arc::new(Inboxes::new());
        let input = inboxes.register("p1".to_string(), 4, Envelope::none());
        assert_eq!(inboxes.len(), 1);

        drop(input);
        assert!(inboxes.is_empty());
        assert!(matches!(
            inboxes.send("p1", batch(1), PostMeta::default()),
            Err(IngestError::NoInbox(_))
        ));
    }

    /// The allow-list is the whole of the rule, and the reason it is an
    /// allow-list: a deny-list or an `x-` prefix rule passes `x-api-key`
    /// through, and a credential written into a data lake outlives the request
    /// that carried it by years.
    #[test]
    fn only_the_allow_listed_headers_survive_a_post() {
        let meta = PostMeta::new(
            "POST",
            None,
            [
                ("content-type", "application/json"),
                ("X-Request-Id", "abc"),
                ("authorization", "Bearer hunter2"),
                ("x-api-key", "hunter2"),
                ("cookie", "session=hunter2"),
            ],
        );

        let names: Vec<&str> = meta.headers.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, vec!["content-type", "x-request-id"]);
        assert!(
            !format!("{meta:?}").contains("hunter2"),
            "a credential reached the metadata: {meta:?}"
        );
    }

    /// Header names are case-insensitive on the wire, so the list has to match
    /// them that way — and they come out lowercased, so a config downstream
    /// spells one name rather than four.
    #[test]
    fn header_names_are_matched_and_reported_in_lower_case() {
        let meta = PostMeta::new("POST", None, [("Content-Type", "application/json")]);
        assert_eq!(
            meta.headers,
            vec![("content-type".to_string(), "application/json".to_string())]
        );
    }

    /// The request is gone by the time the run loop reads the messages, so what
    /// it was has to travel with them.
    #[tokio::test]
    async fn a_posts_metadata_reaches_the_messages() -> anyhow::Result<()> {
        let inboxes = Arc::new(Inboxes::new());
        let envelope = Envelope::new(
            Some(&EnvelopeConfig::Merge { meta: None }),
            vec![("pipeline", json!("p1"))],
        );
        let mut input = inboxes.register("p1".to_string(), 4, envelope)?;

        inboxes.send(
            "p1",
            batch(1),
            PostMeta::new(
                "POST",
                Some("10.0.0.7:51000".to_string()),
                [("x-request-id", "abc"), ("authorization", "Bearer x")],
            ),
        )?;

        let got = input.next().await?;
        let meta = &got[0]["_meta"];
        assert_eq!(got[0]["n"], json!(1), "the payload is left alone");
        assert_eq!(meta["pipeline"], json!("p1"));
        assert_eq!(meta["method"], json!("POST"));
        assert_eq!(meta["remote_addr"], json!("10.0.0.7:51000"));
        assert_eq!(meta["headers"], json!({ "x-request-id": "abc" }));
        assert!(meta["received_at"].is_string());
        Ok(())
    }

    /// The default, and what every existing pipeline relies on: no envelope,
    /// no change to the message.
    #[tokio::test]
    async fn without_an_envelope_a_posted_message_is_untouched() -> anyhow::Result<()> {
        let inboxes = Arc::new(Inboxes::new());
        let mut input = inboxes.register("p1".to_string(), 4, Envelope::none())?;
        inboxes.send("p1", batch(1), PostMeta::new("POST", None, [("a", "b")]))?;

        assert_eq!(*input.next().await?[0], json!({ "n": 1 }));
        Ok(())
    }

    /// A revert rebuilds a pipeline under the same id, and the old run loop can
    /// still be dying when it does. The late `Drop` must not unregister the
    /// endpoint the new one has just claimed.
    #[tokio::test]
    async fn a_late_drop_does_not_unregister_its_successor() -> anyhow::Result<()> {
        let inboxes = Arc::new(Inboxes::new());
        let old = inboxes.register("p1".to_string(), 4, Envelope::none())?;
        let old_token = old.token;
        drop(old);
        let mut new = inboxes.register("p1".to_string(), 4, Envelope::none())?;

        // as it would be from the old pipeline's task, after the rebuild
        inboxes.unregister("p1", old_token);

        inboxes.send("p1", batch(7), PostMeta::default())?;
        assert_eq!(new.next().await?[0]["n"], json!(7));
        Ok(())
    }
}
