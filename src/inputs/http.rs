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
use crate::inputs::{BuildInput, InputSource, MessageBatch};
use crate::state::PipelineId;

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

struct Inbox {
    /// Which registration this is. Only the holder of the matching token may
    /// remove it, so a late `Drop` from a torn-down pipeline can't unregister
    /// the one that has since taken its id.
    token: u64,
    tx: mpsc::Sender<Arc<MessageBatch>>,
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
    fn register(self: &Arc<Self>, pipeline_id: PipelineId, capacity: usize) -> Result<HttpInput> {
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
            inboxes: Arc::clone(self),
        })
    }

    /// Hand a batch to a pipeline's http input.
    ///
    /// `try_send` rather than an awaited send: the caller is an HTTP handler,
    /// and a full queue is something to tell the client about rather than
    /// something to hold a request open for.
    pub fn send(&self, pipeline_id: &str, batch: Arc<MessageBatch>) -> Result<(), IngestError> {
        let sender = {
            let inboxes = self.lock();
            let Some(inbox) = inboxes.get(pipeline_id) else {
                return Err(IngestError::NoInbox(pipeline_id.to_string()));
            };
            inbox.tx.clone()
        };
        sender
            .try_send(batch)
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
        Ok(Box::new(
            ctx.inboxes.register(ctx.pipeline_id.clone(), capacity)?,
        ))
    }
}

pub struct HttpInput {
    pipeline_id: PipelineId,
    token: u64,
    rx: mpsc::Receiver<Arc<MessageBatch>>,
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
        self.rx.recv().await.ok_or_else(|| {
            anyhow::anyhow!("the http input of pipeline '{}' is gone", self.pipeline_id)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{IngestError, Inboxes};
    use crate::inputs::{InputSource, MessageBatch};
    use serde_json::json;
    use std::sync::Arc;

    fn batch(n: i64) -> Arc<MessageBatch> {
        Arc::new(vec![Arc::new(json!({ "n": n }))])
    }

    #[tokio::test]
    async fn a_posted_batch_arrives_at_the_input() -> anyhow::Result<()> {
        let inboxes = Arc::new(Inboxes::new());
        let mut input = inboxes.register("p1".to_string(), 4)?;

        inboxes.send("p1", batch(1))?;

        let got = input.next().await?;
        assert_eq!(got.len(), 1);
        assert_eq!(got[0]["n"], json!(1));
        Ok(())
    }

    #[test]
    fn posting_to_a_pipeline_with_no_http_input_says_so() {
        let inboxes = Arc::new(Inboxes::new());
        assert!(matches!(
            inboxes.send("nobody", batch(1)),
            Err(IngestError::NoInbox(ref id)) if id == "nobody"
        ));
        assert!(matches!(inboxes.check("nobody"), Err(IngestError::NoInbox(_))));
    }

    /// Two http inputs on one pipeline would share one endpoint, so the second
    /// one doesn't get built.
    #[test]
    fn one_pipeline_can_only_claim_its_endpoint_once() {
        let inboxes = Arc::new(Inboxes::new());
        let _first = inboxes.register("p1".to_string(), 4);
        assert!(inboxes.register("p1".to_string(), 4).is_err());
    }

    /// Past the queue's capacity a post is refused rather than held: the
    /// alternative is an HTTP request open until the pipeline catches up.
    #[test]
    fn a_full_queue_is_refused_rather_than_waited_on() {
        let inboxes = Arc::new(Inboxes::new());
        let _input = inboxes.register("p1".to_string(), 1);

        assert!(inboxes.send("p1", batch(1)).is_ok());
        assert!(matches!(
            inboxes.send("p1", batch(2)),
            Err(IngestError::Full(ref id)) if id == "p1"
        ));
    }

    /// Eviction is by name and unconditional — the pipeline is being deleted,
    /// and the input's own `Drop` is then a no-op.
    #[test]
    fn an_endpoint_can_be_taken_down_before_its_input_is_dropped() {
        let inboxes = Arc::new(Inboxes::new());
        let input = inboxes.register("p1".to_string(), 4);
        inboxes.evict("p1");

        assert!(matches!(
            inboxes.send("p1", batch(1)),
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
        let input = inboxes.register("p1".to_string(), 4);
        assert_eq!(inboxes.len(), 1);

        drop(input);
        assert!(inboxes.is_empty());
        assert!(matches!(
            inboxes.send("p1", batch(1)),
            Err(IngestError::NoInbox(_))
        ));
    }

    /// A revert rebuilds a pipeline under the same id, and the old run loop can
    /// still be dying when it does. The late `Drop` must not unregister the
    /// endpoint the new one has just claimed.
    #[tokio::test]
    async fn a_late_drop_does_not_unregister_its_successor() -> anyhow::Result<()> {
        let inboxes = Arc::new(Inboxes::new());
        let old = inboxes.register("p1".to_string(), 4)?;
        let old_token = old.token;
        drop(old);
        let mut new = inboxes.register("p1".to_string(), 4)?;

        // as it would be from the old pipeline's task, after the rebuild
        inboxes.unregister("p1", old_token);

        inboxes.send("p1", batch(7))?;
        assert_eq!(new.next().await?[0]["n"], json!(7));
        Ok(())
    }
}
