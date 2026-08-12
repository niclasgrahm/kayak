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
use kayak_core::config::{HttpAuthConfig, HttpInputConfig};
use subtle::ConstantTimeEq;
use tokio::sync::mpsc;

use crate::BuildCtx;
use crate::inputs::ack::{self, Delivery};
use crate::inputs::envelope::{Envelope, Meta};
use crate::inputs::{BuildInput, InputSource, MessageBatch};
use crate::secrets::Resolved;
use crate::state::PipelineId;
use serde_json::Value;

/// How many posted batches wait for the run loop when the config doesn't say.
pub const DEFAULT_CAPACITY: usize = 1024;

/// Why a posted batch didn't reach a pipeline.
///
/// Three cases rather than one because they are three different things for the
/// client to do about it: nothing is listening (fix the request), something is
/// listening and behind (send it again), or something is listening and you may
/// not have it (fix the credentials).
#[derive(Debug)]
pub enum IngestError {
    /// Nothing is registered under this id — the pipeline doesn't exist, or it
    /// has no `http` input.
    NoInbox(PipelineId),
    /// The pipeline's queue is full: it is not reading as fast as this is being
    /// posted.
    Full(PipelineId),
    /// The input has an `auth` and the post didn't satisfy it.
    Unauthorized(PipelineId),
}

impl std::fmt::Display for IngestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoInbox(id) => write!(f, "pipeline '{id}' has no http input"),
            Self::Full(id) => write!(
                f,
                "pipeline '{id}' is not keeping up; its http input queue is full"
            ),
            Self::Unauthorized(id) => write!(
                f,
                "pipeline '{id}' requires a credential this post did not carry"
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

/// The headers of a post, before anything has been filtered out of them.
///
/// A **separate type from [`PostMeta`], deliberately.** `PostMeta` is what a
/// message may end up carrying, so it is filtered to [`ALLOWED_HEADERS`] the
/// moment it is built; this is what a credential is checked against, so it is
/// unfiltered — and keeping them apart is what makes it impossible for the
/// credential to take the metadata's path into an object store. Nothing in here
/// is `Serialize`, and it should stay that way.
#[derive(Clone, Default)]
pub struct Credentials {
    /// Lower-cased header names against their values, since header names are
    /// case-insensitive on the wire and a config spells one of them.
    headers: HashMap<String, String>,
}

impl Credentials {
    /// From a request's headers.
    pub fn new<I, N, V>(headers: I) -> Self
    where
        I: IntoIterator<Item = (N, V)>,
        N: AsRef<str>,
        V: AsRef<str>,
    {
        Self {
            headers: headers
                .into_iter()
                .map(|(name, value)| {
                    (
                        name.as_ref().to_ascii_lowercase(),
                        value.as_ref().to_string(),
                    )
                })
                .collect(),
        }
    }

    /// What a caller presented, when they presented nothing. What every
    /// non-HTTP caller — a test, an internal replay — has.
    #[must_use]
    pub fn none() -> Self {
        Self::default()
    }

    fn get(&self, header: &str) -> Option<&str> {
        self.headers.get(header).map(String::as_str)
    }
}

/// A credential printed by mistake is a credential leaked, so this prints the
/// names it holds and none of the values.
impl std::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Credentials")
            .field("headers", &self.headers.keys().collect::<Vec<_>>())
            .finish()
    }
}

/// What a post must carry for an input to take it — the live half of
/// [`HttpAuthConfig`].
///
/// Both spellings come to the same thing, which is why there is one struct and
/// not an enum: a header name, an optional scheme word in front of the value,
/// and the value itself.
pub struct Requirement {
    /// Lower-cased, to match [`Credentials`].
    header: String,
    /// `Bearer` for the bearer spelling, nothing for a plain header. Matched
    /// case-insensitively, since [RFC 7235] says the scheme is.
    ///
    /// [RFC 7235]: https://www.rfc-editor.org/rfc/rfc7235#section-2.1
    scheme: Option<&'static str>,
    expected: Resolved,
}

impl Requirement {
    /// Build the requirement an input's config asks for, resolving its secret.
    ///
    /// Two things are refused here rather than at the first post. An **empty
    /// token** would be satisfied by an empty header, i.e. by nothing, and is
    /// nearly always an unset `${NAME}` that resolved to a blank. And a header
    /// name on [`ALLOWED_HEADERS`] would put the credential into every message
    /// the input envelopes — the one mistake this whole area exists to prevent,
    /// and one that is invisible until you read the data months later.
    fn build(config: &HttpAuthConfig, ctx: &BuildCtx) -> Result<Self> {
        let (header, scheme, secret) = match config {
            HttpAuthConfig::Bearer { token } => ("authorization".to_string(), Some("Bearer"), token),
            HttpAuthConfig::Header { name, value } => {
                let header = name.trim().to_ascii_lowercase();
                anyhow::ensure!(!header.is_empty(), "an http input's `auth` header needs a name");
                anyhow::ensure!(
                    !ALLOWED_HEADERS.contains(&header.as_str()),
                    "an http input's `auth` cannot use the '{header}' header: it is one of the \
                     headers an `envelope` copies into the messages, so the credential would be \
                     written out with the data"
                );
                (header, None, value)
            }
        };
        let expected = ctx.resolve(secret)?;
        anyhow::ensure!(
            !expected.expose().is_empty(),
            "the credential for an http input's `auth` is empty, so any post would satisfy it; \
             check that '{expected}' is set in the secret store"
        );
        Ok(Self {
            header,
            scheme,
            expected,
        })
    }

    /// Whether a post carrying these headers may be accepted.
    ///
    /// The value comparison is constant-time, so how long a rejection takes
    /// doesn't say how much of the token was right. A **missing** header
    /// returns early without one, and that is not the same oversight: whether a
    /// header is present is not a secret-dependent fact, so there is nothing
    /// for the timing to leak.
    fn permits(&self, credentials: &Credentials) -> bool {
        let Some(presented) = credentials.get(&self.header) else {
            return false;
        };
        let presented = match self.scheme {
            None => presented,
            Some(scheme) => {
                let Some(rest) = strip_scheme(presented, scheme) else {
                    return false;
                };
                rest
            }
        };
        // `expose` is one of the few places a real secret is reached; it goes
        // straight into the comparison and is not held, logged or copied
        self.expected
            .expose()
            .as_bytes()
            .ct_eq(presented.as_bytes())
            .into()
    }
}

/// `"Bearer hunter2"` against `"Bearer"` gives `"hunter2"`.
///
/// The scheme is case-insensitive and the separator is at least one space, so
/// `bearer  x` is the same credential as `Bearer x`.
fn strip_scheme<'a>(value: &'a str, scheme: &str) -> Option<&'a str> {
    let (word, rest) = value.split_once(' ')?;
    word.eq_ignore_ascii_case(scheme).then(|| rest.trim_start())
}

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
    /// What a post must present, if the input asked for anything.
    ///
    /// It lives on the registration rather than on the input for the same
    /// reason the channel does: the requirement's lifetime is then exactly the
    /// endpoint's, so deleting the pipeline takes the credential down with the
    /// path it protected and there is no second thing to remember to remove.
    auth: Option<Requirement>,
}

impl Inbox {
    /// Whether these credentials satisfy this registration.
    fn permits(&self, pipeline_id: &str, credentials: &Credentials) -> Result<(), IngestError> {
        match &self.auth {
            None => Ok(()),
            Some(required) if required.permits(credentials) => Ok(()),
            Some(_) => Err(IngestError::Unauthorized(pipeline_id.to_string())),
        }
    }
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
        auth: Option<Requirement>,
    ) -> Result<HttpInput> {
        let mut inboxes = self.lock();
        anyhow::ensure!(
            !inboxes.contains_key(&pipeline_id),
            "pipeline '{pipeline_id}' already has an http input; one pipeline is one endpoint"
        );
        let token = self.next_token.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = mpsc::channel(capacity);
        inboxes.insert(pipeline_id.clone(), Inbox { token, tx, auth });
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
        credentials: &Credentials,
    ) -> Result<(), IngestError> {
        let sender = {
            let inboxes = self.lock();
            let Some(inbox) = inboxes.get(pipeline_id) else {
                return Err(IngestError::NoInbox(pipeline_id.to_string()));
            };
            inbox.permits(pipeline_id, credentials)?;
            inbox.tx.clone()
        };
        sender
            .try_send(Posted { batch, meta })
            .map_err(|_| IngestError::Full(pipeline_id.to_string()))
    }

    /// Whether a pipeline is accepting posts, without sending anything. What an
    /// empty post is answered from — it should still 404 on a pipeline that
    /// isn't listening rather than be accepted by nobody.
    ///
    /// Credentials are checked here too. An empty post is still a post, and an
    /// unauthenticated caller learning which pipelines are up by sending `[]`
    /// to each of them is exactly the enumeration the credential is for.
    pub fn check(&self, pipeline_id: &str, credentials: &Credentials) -> Result<(), IngestError> {
        let inboxes = self.lock();
        let Some(inbox) = inboxes.get(pipeline_id) else {
            return Err(IngestError::NoInbox(pipeline_id.to_string()));
        };
        inbox.permits(pipeline_id, credentials)
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
        // the endpoint's own response code is the closest thing this input has
        // to an ack today, and that isn't wired into `AckMode` yet — see the
        // module docs on `ack` for why this is a deliberate gap, not an
        // oversight
        ack::require_receipt_only(ctx.ack_mode(), "http")?;
        let capacity = self.capacity.unwrap_or(DEFAULT_CAPACITY);
        anyhow::ensure!(capacity > 0, "an http input's `capacity` must be at least 1");
        let auth = self
            .auth
            .as_ref()
            .map(|config| Requirement::build(config, ctx))
            .transpose()?;
        let envelope = ctx.envelope("http", None);
        Ok(Box::new(ctx.inboxes.register(
            ctx.pipeline_id.clone(),
            capacity,
            envelope,
            auth,
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
    async fn next(&mut self) -> Result<Delivery> {
        // The only sender is the one in the registry, which this input owns
        // through its `Drop` — so `None` means we have been dropped, which
        // can't be observed from inside `next`. Reported rather than
        // unreachable!(), since a future refactor could make it possible.
        let posted = self.rx.recv().await.ok_or_else(|| {
            anyhow::anyhow!("the http input of pipeline '{}' is gone", self.pipeline_id)
        })?;

        if !self.envelope.is_enabled() {
            return Ok(Delivery::new(posted.batch));
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
        Ok(Delivery::new(Arc::new(enveloped)))
    }
}

#[cfg(test)]
mod tests {
    use super::{Credentials, HttpAuthConfig, HttpInputConfig, IngestError, Inboxes, PostMeta, Requirement};
    use kayak_core::config::AckMode;
    use crate::BuildCtx;
    use crate::inputs::envelope::Envelope;
    use crate::inputs::{BuildInput, InputSource, MessageBatch};
    use crate::testing::MapSecretStore;
    use kayak_core::config::EnvelopeConfig;
    use serde_json::json;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn batch(n: i64) -> Arc<MessageBatch> {
        Arc::new(vec![Arc::new(json!({ "n": n }))])
    }

    /// A requirement built the way a pipeline builds one, against a secret
    /// store holding `TOKEN`.
    fn requirement(config: &HttpAuthConfig) -> anyhow::Result<Requirement> {
        let mut pipelines = HashMap::new();
        let (events, _rx) = tokio::sync::broadcast::channel(4);
        let secrets = Arc::new(MapSecretStore::new(
            "the test secrets",
            &[("TOKEN", "hunter2"), ("BLANK", "")],
        ));
        let ctx = BuildCtx::with_secrets(&mut pipelines, "p1".to_string(), events, secrets);
        Requirement::build(config, &ctx)
    }

    fn bearer(token: &str) -> HttpAuthConfig {
        HttpAuthConfig::Bearer {
            token: token.into(),
        }
    }

    fn presenting(header: &str, value: &str) -> Credentials {
        Credentials::new([(header, value)])
    }

    #[tokio::test]
    async fn a_posted_batch_arrives_at_the_input() -> anyhow::Result<()> {
        let inboxes = Arc::new(Inboxes::new());
        let mut input = inboxes.register("p1".to_string(), 4, Envelope::none(), None)?;

        inboxes.send("p1", batch(1), PostMeta::default(), &Credentials::none())?;

        let got = input.next().await?;
        assert_eq!(got.len(), 1);
        assert_eq!(got[0]["n"], json!(1));
        Ok(())
    }

    #[test]
    fn posting_to_a_pipeline_with_no_http_input_says_so() {
        let inboxes = Arc::new(Inboxes::new());
        assert!(matches!(
            inboxes.send("nobody", batch(1), PostMeta::default(), &Credentials::none()),
            Err(IngestError::NoInbox(ref id)) if id == "nobody"
        ));
        assert!(matches!(inboxes.check("nobody", &Credentials::none()), Err(IngestError::NoInbox(_))));
    }

    /// Two http inputs on one pipeline would share one endpoint, so the second
    /// one doesn't get built.
    #[test]
    fn one_pipeline_can_only_claim_its_endpoint_once() {
        let inboxes = Arc::new(Inboxes::new());
        let _first = inboxes.register("p1".to_string(), 4, Envelope::none(), None);
        assert!(inboxes.register("p1".to_string(), 4, Envelope::none(), None).is_err());
    }

    /// Past the queue's capacity a post is refused rather than held: the
    /// alternative is an HTTP request open until the pipeline catches up.
    #[test]
    fn a_full_queue_is_refused_rather_than_waited_on() {
        let inboxes = Arc::new(Inboxes::new());
        let _input = inboxes.register("p1".to_string(), 1, Envelope::none(), None);

        assert!(inboxes.send("p1", batch(1), PostMeta::default(), &Credentials::none()).is_ok());
        assert!(matches!(
            inboxes.send("p1", batch(2), PostMeta::default(), &Credentials::none()),
            Err(IngestError::Full(ref id)) if id == "p1"
        ));
    }

    /// Eviction is by name and unconditional — the pipeline is being deleted,
    /// and the input's own `Drop` is then a no-op.
    #[test]
    fn an_endpoint_can_be_taken_down_before_its_input_is_dropped() {
        let inboxes = Arc::new(Inboxes::new());
        let input = inboxes.register("p1".to_string(), 4, Envelope::none(), None);
        inboxes.evict("p1");

        assert!(matches!(
            inboxes.send("p1", batch(1), PostMeta::default(), &Credentials::none()),
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
        let input = inboxes.register("p1".to_string(), 4, Envelope::none(), None);
        assert_eq!(inboxes.len(), 1);

        drop(input);
        assert!(inboxes.is_empty());
        assert!(matches!(
            inboxes.send("p1", batch(1), PostMeta::default(), &Credentials::none()),
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
        let mut input = inboxes.register("p1".to_string(), 4, envelope, None)?;

        inboxes.send(
            "p1",
            batch(1),
            PostMeta::new(
                "POST",
                Some("10.0.0.7:51000".to_string()),
                [("x-request-id", "abc"), ("authorization", "Bearer x")],
            ),
            &Credentials::none(),
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
        let mut input = inboxes.register("p1".to_string(), 4, Envelope::none(), None)?;
        inboxes.send(
            "p1",
            batch(1),
            PostMeta::new("POST", None, [("a", "b")]),
            &Credentials::none(),
        )?;

        assert_eq!(*input.next().await?[0], json!({ "n": 1 }));
        Ok(())
    }

    /// A revert rebuilds a pipeline under the same id, and the old run loop can
    /// still be dying when it does. The late `Drop` must not unregister the
    /// endpoint the new one has just claimed.
    #[tokio::test]
    async fn a_late_drop_does_not_unregister_its_successor() -> anyhow::Result<()> {
        let inboxes = Arc::new(Inboxes::new());
        let old = inboxes.register("p1".to_string(), 4, Envelope::none(), None)?;
        let old_token = old.token;
        drop(old);
        let mut new = inboxes.register("p1".to_string(), 4, Envelope::none(), None)?;

        // as it would be from the old pipeline's task, after the rebuild
        inboxes.unregister("p1", old_token);

        inboxes.send("p1", batch(7), PostMeta::default(), &Credentials::none())?;
        assert_eq!(new.next().await?[0]["n"], json!(7));
        Ok(())
    }

    /// The whole point of the feature: with `auth`, the right token gets in.
    #[tokio::test]
    async fn a_post_carrying_the_bearer_token_is_accepted() -> anyhow::Result<()> {
        let inboxes = Arc::new(Inboxes::new());
        let auth = requirement(&bearer("${TOKEN}"))?;
        let mut input = inboxes.register("p1".to_string(), 4, Envelope::none(), Some(auth))?;

        inboxes.send(
            "p1",
            batch(1),
            PostMeta::default(),
            &presenting("authorization", "Bearer hunter2"),
        )?;

        assert_eq!(input.next().await?[0]["n"], json!(1));
        Ok(())
    }

    /// And the other half: everything else is refused, including the *template*
    /// — a store that didn't resolve must not leave `${TOKEN}` working as a
    /// password.
    #[test]
    fn a_post_without_the_right_token_is_refused() -> anyhow::Result<()> {
        let inboxes = Arc::new(Inboxes::new());
        let auth = requirement(&bearer("${TOKEN}"))?;
        let _input = inboxes.register("p1".to_string(), 4, Envelope::none(), Some(auth))?;

        for (header, value) in [
            ("authorization", "Bearer wrong"),
            ("authorization", "Bearer "),
            ("authorization", "Bearer hunter2x"),
            ("authorization", "hunter2"),
            ("authorization", "${TOKEN}"),
            ("authorization", "Basic hunter2"),
            ("x-api-key", "hunter2"),
        ] {
            assert!(
                matches!(
                    inboxes.send("p1", batch(1), PostMeta::default(), &presenting(header, value)),
                    Err(IngestError::Unauthorized(ref id)) if id == "p1"
                ),
                "'{header}: {value}' was accepted"
            );
        }
        // and no credentials at all
        assert!(matches!(
            inboxes.send("p1", batch(1), PostMeta::default(), &Credentials::none()),
            Err(IngestError::Unauthorized(_))
        ));
        Ok(())
    }

    /// A refused post must not take a place in the queue — otherwise anyone who
    /// can reach the endpoint can fill it and turn the 401 into a 503 for the
    /// sender who *does* have the token.
    #[test]
    fn a_refused_post_never_reaches_the_queue() -> anyhow::Result<()> {
        let inboxes = Arc::new(Inboxes::new());
        let auth = requirement(&bearer("${TOKEN}"))?;
        // capacity of one, so a single wrongly-accepted post would fill it
        let _input = inboxes.register("p1".to_string(), 1, Envelope::none(), Some(auth))?;

        for _ in 0..10 {
            assert!(matches!(
                inboxes.send("p1", batch(1), PostMeta::default(), &Credentials::none()),
                Err(IngestError::Unauthorized(_))
            ));
        }
        // the queue is still empty, so the holder of the token gets in
        inboxes.send(
            "p1",
            batch(1),
            PostMeta::default(),
            &presenting("authorization", "Bearer hunter2"),
        )?;
        Ok(())
    }

    /// Header names are case-insensitive on the wire and RFC 7235 makes the
    /// scheme so too, so `AUTHORIZATION: bearer x` is the same credential.
    #[test]
    fn the_header_name_and_the_scheme_are_both_case_insensitive() -> anyhow::Result<()> {
        let auth = requirement(&bearer("${TOKEN}"))?;
        for (header, value) in [
            ("authorization", "Bearer hunter2"),
            ("Authorization", "bearer hunter2"),
            ("AUTHORIZATION", "BEARER hunter2"),
            // more than one space after the scheme is still one credential
            ("authorization", "Bearer   hunter2"),
        ] {
            assert!(
                auth.permits(&presenting(header, value)),
                "'{header}: {value}' was refused"
            );
        }
        Ok(())
    }

    /// The other spelling, for senders that can't set `Authorization`. No
    /// scheme word: the header's whole value is the credential.
    #[test]
    fn a_header_credential_matches_the_whole_value() -> anyhow::Result<()> {
        let auth = requirement(&HttpAuthConfig::Header {
            name: "X-Api-Key".to_string(),
            value: "${TOKEN}".into(),
        })?;

        assert!(auth.permits(&presenting("x-api-key", "hunter2")));
        assert!(auth.permits(&presenting("X-API-KEY", "hunter2")));
        // not stripped of a scheme, and not matched loosely
        assert!(!auth.permits(&presenting("x-api-key", "Bearer hunter2")));
        assert!(!auth.permits(&presenting("x-api-key", " hunter2")));
        assert!(!auth.permits(&presenting("authorization", "hunter2")));
        Ok(())
    }

    /// The credential must not be reachable through the metadata, which is the
    /// reason `auth` may not name an allow-listed header. Refused at build time
    /// rather than filtered later: filtering is the thing that gets forgotten.
    #[test]
    fn auth_cannot_use_a_header_the_envelope_copies() {
        for name in super::ALLOWED_HEADERS {
            let result = requirement(&HttpAuthConfig::Header {
                name: (*name).to_string(),
                value: "${TOKEN}".into(),
            });
            let Err(err) = result else {
                panic!("'{name}' was accepted as an auth header, so the credential would be \
                        written into the messages");
            };
            assert!(format!("{err:#}").contains(name), "{err:#}");
        }
    }

    /// An empty credential is satisfied by an empty header, i.e. by anything —
    /// and it is nearly always a `${NAME}` nobody set. Refusing to build says so
    /// at startup instead of accepting every post forever.
    #[test]
    fn an_empty_credential_is_refused() {
        for config in [
            bearer(""),
            bearer("${BLANK}"),
            HttpAuthConfig::Header {
                name: "x-api-key".to_string(),
                value: "${BLANK}".into(),
            },
        ] {
            assert!(
                requirement(&config).is_err(),
                "an empty credential built: {config:?}"
            );
        }
    }

    /// A `${NAME}` the store doesn't have stops the pipeline rather than
    /// becoming a token nobody can guess — the same rule every other secret has.
    #[test]
    fn an_unresolvable_token_fails_to_build() {
        let Err(err) = requirement(&bearer("${NOPE}")) else {
            panic!("a pipeline built on a secret that isn't set");
        };
        assert!(format!("{err:#}").contains("NOPE"), "{err:#}");
    }

    /// An auth header may not be one the envelope copies, so a credential can't
    /// reach the messages that way. This is the same fact from the data's side.
    #[tokio::test]
    async fn the_credential_never_reaches_the_metadata() -> anyhow::Result<()> {
        let inboxes = Arc::new(Inboxes::new());
        let auth = requirement(&bearer("${TOKEN}"))?;
        let envelope = Envelope::new(Some(&EnvelopeConfig::Merge { meta: None }), vec![]);
        let mut input = inboxes.register("p1".to_string(), 4, envelope, Some(auth))?;

        inboxes.send(
            "p1",
            batch(1),
            PostMeta::new("POST", None, [("authorization", "Bearer hunter2")]),
            &presenting("authorization", "Bearer hunter2"),
        )?;

        let got = input.next().await?;
        let rendered = serde_json::to_string(&*got[0])?;
        assert!(!rendered.contains("hunter2"), "{rendered}");
        assert!(!rendered.contains("authorization"), "{rendered}");
        Ok(())
    }

    /// An empty post is still a post: `[]` to every id in turn is exactly how
    /// you would find out which pipelines are up without a token.
    #[test]
    fn an_empty_post_is_checked_too() -> anyhow::Result<()> {
        let inboxes = Arc::new(Inboxes::new());
        let auth = requirement(&bearer("${TOKEN}"))?;
        let _input = inboxes.register("p1".to_string(), 4, Envelope::none(), Some(auth))?;

        assert!(matches!(
            inboxes.check("p1", &Credentials::none()),
            Err(IngestError::Unauthorized(_))
        ));
        inboxes.check("p1", &presenting("authorization", "Bearer hunter2"))?;
        Ok(())
    }

    /// Absent `auth` is what every pipeline with an `http` input has always
    /// had, and stays that way: anything that reaches the endpoint gets in.
    #[test]
    fn without_auth_a_post_needs_no_credentials() -> anyhow::Result<()> {
        let inboxes = Arc::new(Inboxes::new());
        let _input = inboxes.register("p1".to_string(), 4, Envelope::none(), None)?;

        inboxes.send("p1", batch(1), PostMeta::default(), &Credentials::none())?;
        inboxes.check("p1", &Credentials::none())?;
        Ok(())
    }

    /// Anything that prints a request is a place a token can escape into a log,
    /// so this one prints the header names and none of the values.
    #[test]
    fn credentials_do_not_print_their_values() {
        let credentials = presenting("authorization", "Bearer hunter2");
        let printed = format!("{credentials:?}");
        assert!(!printed.contains("hunter2"), "{printed}");
        assert!(printed.contains("authorization"), "{printed}");
    }

    /// The endpoint's own response code (202/503/401) is the closest thing
    /// this input has to an ack today, and it isn't wired into `AckMode` yet
    /// — a deliberate gap, not an oversight, so `on_delivery` is refused
    /// rather than silently doing nothing.
    #[test]
    fn on_delivery_is_refused() {
        let mut pipelines = HashMap::new();
        let (events, _rx) = tokio::sync::broadcast::channel(4);
        let mut ctx = BuildCtx::new(&mut pipelines, "p1".to_string(), events);
        ctx.ack_mode = Some(AckMode::OnDelivery);
        let Err(err) = (HttpInputConfig {
            capacity: None,
            auth: None,
        })
        .build(&mut ctx) else {
            panic!("an http input built with `ack: on_delivery`, which it cannot honour");
        };
        assert!(format!("{err:#}").contains("http"), "{err:#}");
    }
}
