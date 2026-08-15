//! The `buffer` transform: hold messages back, hand them on when a trigger
//! says to.
//!
//! Three triggers compose here and they are not the same shape, which is the
//! thing to understand before changing any of it:
//!
//! - **`size` counts, and it hands on batches of exactly that many.** That is
//!   what this transform has always done and the behaviour is unchanged to the
//!   message — an existing `{"type": "buffer", "size": 6}` still emits sixes.
//! - **`seconds` and `until` release *everything held*, as one batch.** They
//!   are answers to "that's enough waiting", and a wait that ended with a
//!   partial batch left behind would not have ended.
//!
//! The gate ([`Gate`]) is the interesting one. It reads a **state bucket**,
//! which is global, so the pipeline that opens the gate is usually not this
//! one — one pipeline marks a run complete and another hands on what it
//! gathered while the run was going. Two consequences worth carrying:
//!
//! - Nothing in *this* pipeline's flow is correlated with the gate opening, so
//!   the buffer cannot wait for a batch to arrive before noticing. That is
//!   what [`Transform::wakeup`] is for, and this is its first caller.
//! - Two run loops have no ordering between them. The gate says "the bucket
//!   said so at the moment we looked", which is the same unenforced rule
//!   sharing a bucket already carries — see `kayak_core::state`'s module docs.
//!   It is not a synchronisation primitive however much it reads like one.
//!
//! What keeps the cost off the hot path is that the gate is evaluated **once
//! per arriving batch**, not once per message, and only when the bucket has
//! actually been written since the last look — a `watch` receiver's version
//! check, which is an atomic load and not the bucket's mutex. That is reasoned
//! rather than measured: there is no `kayak-bench` scenario for a gated buffer
//! yet, and adding one is the way to find out what this really costs. See
//! [`Gate::open`].

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, bail};
use kayak_core::config::{BufferGateConfig, BufferTransformConfig, Condition};
use tokio::sync::watch;
// tokio's clock, not `std`'s, and the difference is load-bearing: the deadline
// and the `sleep_until` that waits for it have to be read off the same clock,
// or a paused-time test passes the sleep and then finds the deadline is still
// in the future.
use tokio::time::Instant;

use crate::{
    BuildCtx,
    buckets::Buckets,
    inputs::MessageBatch,
    transforms::{
        BuildTransform, Transform,
        state::{WHOLE_BUCKET_KEY, matches},
    },
};

impl BuildTransform for BufferTransformConfig {
    fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn Transform>> {
        // Everything contradictory is refused here rather than producing a
        // strange batch once per second forever — the reducer's rule.
        if self.size.is_none() && self.seconds.is_none() && self.until.is_none() {
            bail!(
                "a buffer transform needs at least one trigger: `size`, `seconds` or `until`. \
                 Without one it would hold every message it is ever given"
            );
        }
        for (name, value) in [
            ("size", self.size),
            ("seconds", self.seconds),
            ("max_messages", self.max_messages),
        ] {
            if value == Some(0) {
                bail!("a buffer transform's `{name}` is 0, which can only mean 'never'");
            }
        }
        // The backstop, and the reason it is mandatory: a gate that never
        // opens, or a `seconds` on its own with no count, is otherwise a
        // buffer that grows at the rate of the stream until the process dies.
        // `size` is exempt because `size` *is* a bound.
        let Some(max_messages) = self.max_messages.or(self.size) else {
            bail!(
                "a buffer transform that waits on `seconds` or `until` needs a `max_messages` \
                 — it is the bound on how much can be held while the wait goes on, and \
                 without one a trigger that never fires is a memory leak"
            );
        };
        if let (Some(size), Some(max)) = (self.size, self.max_messages)
            && max < size
        {
            bail!(
                "a buffer transform's `max_messages` ({max}) is below its `size` ({size}), so \
                 the batches it is asked for could never be filled"
            );
        }

        let gate = self.until.map(|gate| Gate::resolve(gate, ctx)).transpose()?;

        Ok(Box::new(BufferTransform {
            size: self.size,
            window: self
                .seconds
                .map(|secs| Duration::from_secs(secs.try_into().unwrap_or(u64::MAX))),
            gate,
            max_messages,
            held: MessageBatch::new(),
            deadline: None,
            warned_about_overflow: false,
        }))
    }
}

/// The state-bucket half of the trigger: which key in which bucket, what has
/// to be true of it, and how this transform hears that it changed.
struct Gate {
    buckets: Arc<Buckets>,
    bucket: String,
    key: String,
    conditions: Vec<Condition>,
    /// Bumped by every write to the bucket. Read for the cheap "has anything
    /// happened" question and awaited by [`Transform::wakeup`].
    changes: watch::Receiver<u64>,
    /// The last answer, kept only for as long as the version says it is still
    /// the answer. `None` means "look properly".
    cached: Option<bool>,
}

impl Gate {
    fn resolve(config: BufferGateConfig, ctx: &BuildCtx) -> Result<Self> {
        // The pipeline's own `state` is the default, which is what makes the
        // common case — a pipeline that already remembers things gating on one
        // of them — need no bucket name at all.
        let Some(bucket) = config
            .bucket
            .or_else(|| ctx.state.as_ref().map(|state| state.bucket.clone()))
        else {
            bail!(
                "a buffer transform's `until` needs a `bucket`: this pipeline has no `state` \
                 block to take one from"
            );
        };
        let Some(changes) = ctx.buckets.watch(&bucket) else {
            bail!(
                "a buffer transform's `until` names state bucket '{bucket}', which is not \
                 declared under `state` at the top of the config"
            );
        };
        if config.conditions.is_empty() {
            bail!(
                "a buffer transform's `until` needs at least one condition — with none it \
                 would release on every write to bucket '{bucket}'"
            );
        }
        Ok(Self {
            buckets: Arc::clone(&ctx.buckets),
            bucket,
            // A literal key, not a field path: this is one gate for the whole
            // buffer, so there is no message to take a key from. Absent means
            // the bucket-wide value, which is the key `remember` writes under
            // when its pipeline's `state` has no `key`.
            key: config.key.unwrap_or_else(|| WHOLE_BUCKET_KEY.to_string()),
            conditions: config.conditions,
            changes,
            cached: None,
        })
    }

    /// Whether the gate is open, answered as cheaply as it can be.
    ///
    /// The fast path is the point: `has_changed` is a load of the `watch`
    /// channel's version, so a bucket nothing has written since the last look
    /// costs an atomic and no lock at all. This matters because *every*
    /// buffering pipeline asks this of a bucket they may share, and one mutex
    /// asked by every pipeline on every pass is exactly what capped the whole
    /// process at ~6.5M passes a second before `events::Watchers` existed.
    /// Don't replace this with an unconditional read.
    fn open(&mut self) -> bool {
        // `Err` means the sender is gone (the buckets were rebuilt under a
        // running pipeline), which is not a state to cache an answer from.
        if let (Some(cached), Ok(false)) = (self.cached, self.changes.has_changed()) {
            return cached;
        }
        self.changes.mark_unchanged();
        let open = self
            .buckets
            .entry(&self.bucket, &self.key)
            .is_some_and(|entry| matches(&self.conditions, &entry));
        self.cached = Some(open);
        open
    }

    /// Resolves when the bucket is written to.
    ///
    /// Cancel-safe, which it has to be — the run loop drops this future on
    /// every pass. A `watch` receiver remembers the version it has seen, so a
    /// write landing in that gap is still waiting to be noticed next time.
    async fn wait(&mut self) {
        // `Err` is the sender being dropped, and it never resolves again;
        // waiting forever is right, since the pipeline is being torn down.
        if self.changes.changed().await.is_ok() {
            self.cached = None;
        } else {
            std::future::pending::<()>().await;
        }
    }
}

pub struct BufferTransform {
    size: Option<usize>,
    window: Option<Duration>,
    gate: Option<Gate>,
    max_messages: usize,
    held: MessageBatch,
    /// When the current window closes. Set when the buffer goes from holding
    /// nothing to holding something, so the clock measures how long the
    /// *oldest held message* has waited — a bound on latency rather than a
    /// cadence, the same promise the input-level buffer makes.
    deadline: Option<Instant>,
    /// Whether the backstop has been reported. Once per transform rather than
    /// once per overflow: a buffer whose gate never opens overflows on a fixed
    /// cadence forever, and a line each time buries the log it is trying to
    /// appear in. The same rule `remember` follows for its missing key.
    warned_about_overflow: bool,
}

impl BufferTransform {
    /// Everything held, as one batch, and the window closed behind it.
    fn take_held(&mut self) -> Arc<MessageBatch> {
        self.deadline = None;
        Arc::new(std::mem::take(&mut self.held))
    }

    fn hold(&mut self, message: Arc<serde_json::Value>) {
        if self.held.is_empty() {
            self.deadline = self.window.map(|window| Instant::now() + window);
        }
        self.held.push(message);
    }
}

#[async_trait::async_trait]
impl Transform for BufferTransform {
    async fn apply(
        &mut self,
        message_batch: Arc<MessageBatch>,
    ) -> anyhow::Result<Vec<Arc<MessageBatch>>> {
        let mut out = Vec::new();
        for msg in message_batch.iter() {
            self.hold(msg.clone());
            // The counting trigger is per message and emits exact batches —
            // unchanged, and the one trigger that does not release everything.
            if self.size.is_some_and(|size| self.held.len() >= size) {
                out.push(self.take_held());
            }
        }

        if !self.held.is_empty() {
            if self.held.len() >= self.max_messages {
                if !self.warned_about_overflow {
                    self.warned_about_overflow = true;
                    tracing::warn!(
                        "buffer: holding {} messages, which is its `max_messages` — releasing \
                         them although no trigger fired. Reported once.",
                        self.held.len()
                    );
                }
                out.push(self.take_held());
            } else if self.gate.as_mut().is_some_and(Gate::open) {
                // Once per arriving batch, never per message: the gate is a
                // property of the bucket, and asking per message would put a
                // lock acquisition on the hottest path there is.
                out.push(self.take_held());
            }
        }

        Ok(out)
    }

    async fn wakeup(&mut self) {
        // Nothing held is nothing to release, so there is nothing to wake for
        // — which is what keeps an idle pipeline free of both the timer and
        // the bucket subscription.
        if self.held.is_empty() {
            std::future::pending::<()>().await;
        }
        match (self.deadline, self.gate.as_mut()) {
            (None, None) => std::future::pending::<()>().await,
            (Some(deadline), None) => tokio::time::sleep_until(deadline).await,
            (None, Some(gate)) => gate.wait().await,
            (Some(deadline), Some(gate)) => {
                tokio::select! {
                    () = tokio::time::sleep_until(deadline) => {}
                    () = gate.wait() => {}
                }
            }
        }
    }

    async fn flush(&mut self) -> anyhow::Result<Vec<Arc<MessageBatch>>> {
        if self.held.is_empty() {
            return Ok(vec![]);
        }
        // Re-checked rather than trusted: a wakeup says "look at me", and the
        // bucket write that woke us may not have opened the gate at all.
        let due = self
            .deadline
            .is_some_and(|deadline| Instant::now() >= deadline);
        if due || self.gate.as_mut().is_some_and(Gate::open) {
            return Ok(vec![self.take_held()]);
        }
        Ok(vec![])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::batch;
    use kayak_core::config::{Condition, StringFilterOperatorKind};
    use kayak_core::state::{PipelineState, StateBucketConfig, StateBuckets};
    use serde_json::json;

    fn transform(size: usize) -> BufferTransform {
        BufferTransform {
            size: Some(size),
            window: None,
            gate: None,
            max_messages: size,
            held: MessageBatch::new(),
            deadline: None,
            warned_about_overflow: false,
        }
    }

    async fn feed(t: &mut BufferTransform, count: usize) -> Vec<Vec<serde_json::Value>> {
        let msgs = (0..count).map(|i| json!({ "i": i })).collect();
        let out = t.apply(batch(msgs)).await.unwrap_or_default();
        out.iter()
            .map(|b| b.iter().map(|m| (**m).clone()).collect())
            .collect()
    }

    /// The buffer is stateful across calls: it holds messages back until it has
    /// `size` of them, then releases exactly one full batch.
    #[tokio::test]
    async fn messages_are_held_until_the_buffer_is_full() {
        let mut t = transform(3);
        assert!(feed(&mut t, 2).await.is_empty(), "2 of 3 should be held");
        let out = feed(&mut t, 1).await;
        assert_eq!(
            out,
            vec![vec![json!({"i": 0}), json!({"i": 1}), json!({"i": 0})]]
        );
    }

    /// One oversized input batch releases every full batch it contains in a
    /// single call — that's the "one batch in, N batches out" contract.
    #[tokio::test]
    async fn one_large_batch_releases_several_full_batches_at_once() {
        let mut t = transform(2);
        let out = feed(&mut t, 5).await;
        assert_eq!(out.len(), 2, "4 of 5 messages form 2 full batches: {out:?}");

        // the 5th is still buffered, and comes out once the next one arrives
        let out = feed(&mut t, 1).await;
        assert_eq!(out.len(), 1);
    }

    // ── building ────────────────────────────────────────────────────────────

    fn declared() -> StateBuckets {
        let mut buckets = StateBuckets::new();
        buckets.insert("control", StateBucketConfig::default());
        buckets
    }

    /// A build against a real bucket, the way `AppState` does it.
    fn build(
        config: BufferTransformConfig,
        buckets: &Arc<Buckets>,
        state: Option<PipelineState>,
    ) -> Result<Box<dyn Transform>> {
        let (events, _) = tokio::sync::broadcast::channel(16);
        let mut pipelines = std::collections::HashMap::new();
        let mut ctx = BuildCtx::new(&mut pipelines, "buffer-test".into(), events)
            .with_buckets(Arc::clone(buckets))
            .with_state(state);
        config.build(&mut ctx)
    }

    fn built(config: BufferTransformConfig) -> Result<Box<dyn Transform>> {
        build(config, &Arc::new(Buckets::from_config(&declared())), None)
    }

    /// The message a config that shouldn't build fails with. A helper because
    /// `Box<dyn Transform>` isn't `Debug`, so `expect_err` is unavailable.
    fn refused(config: BufferTransformConfig) -> String {
        match built(config) {
            Ok(_) => panic!("this config should not have built"),
            Err(e) => format!("{e}"),
        }
    }

    fn ready() -> Vec<Condition> {
        vec![Condition::String {
            field: "status".to_string(),
            operator: StringFilterOperatorKind::EqualTo,
            value: "ready".to_string(),
        }]
    }

    fn config() -> BufferTransformConfig {
        BufferTransformConfig {
            size: None,
            seconds: None,
            until: None,
            max_messages: None,
        }
    }

    /// A buffer with nothing to release on would hold every message it is ever
    /// given, which is never what anyone meant to write.
    #[test]
    fn a_buffer_with_no_trigger_is_refused() {
        let err = refused(config());
        assert!(err.contains("at least one trigger"), "{err}");
    }

    /// The backstop is mandatory for the two triggers that can fail to fire —
    /// without it, a gate that never opens is a memory leak.
    #[test]
    fn waiting_without_a_backstop_is_refused() {
        let err = refused(BufferTransformConfig {
            seconds: Some(30),
            ..config()
        });
        assert!(err.contains("max_messages"), "{err}");

        let err = refused(BufferTransformConfig {
            until: Some(BufferGateConfig {
                bucket: Some("control".to_string()),
                key: None,
                conditions: ready(),
            }),
            ..config()
        });
        assert!(err.contains("max_messages"), "{err}");
    }

    /// `size` is its own bound, so it needs no backstop beside it — which is
    /// what keeps every config written before the other triggers existed valid.
    #[test]
    fn size_is_its_own_backstop() -> Result<()> {
        // size alone has always been valid and must stay so
        built(BufferTransformConfig {
            size: Some(10),
            ..config()
        })?;
        Ok(())
    }

    /// Batches of `size` could never be filled if less than `size` may be held.
    #[test]
    fn a_backstop_below_the_batch_size_is_refused() {
        let err = refused(BufferTransformConfig {
            size: Some(100),
            max_messages: Some(10),
            ..config()
        });
        assert!(err.contains("below its `size`"), "{err}");
    }

    /// An undeclared bucket is a config mistake, and the message has to say
    /// which name it looked for.
    #[test]
    fn a_gate_on_an_undeclared_bucket_is_refused() {
        let err = refused(BufferTransformConfig {
            max_messages: Some(10),
            until: Some(BufferGateConfig {
                bucket: Some("nope".to_string()),
                key: None,
                conditions: ready(),
            }),
            ..config()
        });
        assert!(err.contains("'nope'"), "{err}");
    }

    /// A gate with no conditions would open on any write to the bucket, which
    /// is not a gate.
    #[test]
    fn a_gate_with_no_conditions_is_refused() {
        let err = refused(BufferTransformConfig {
            max_messages: Some(10),
            until: Some(BufferGateConfig {
                bucket: Some("control".to_string()),
                key: None,
                conditions: vec![],
            }),
            ..config()
        });
        assert!(err.contains("at least one condition"), "{err}");
    }

    /// A pipeline with a `state` block needs no bucket name on the gate — the
    /// common case is gating on something this pipeline already remembers.
    #[test]
    fn a_gate_takes_the_pipelines_bucket_when_it_names_none() -> Result<()> {
        // the pipeline's own bucket should be the default
        build(
            BufferTransformConfig {
                max_messages: Some(10),
                until: Some(BufferGateConfig {
                    bucket: None,
                    key: None,
                    conditions: ready(),
                }),
                ..config()
            },
            &Arc::new(Buckets::from_config(&declared())),
            Some(PipelineState {
                bucket: "control".to_string(),
                key: None,
            }),
        )?;
        Ok(())
    }

    /// ...and a pipeline without one has to say which bucket, rather than
    /// building a gate that watches nothing.
    #[test]
    fn a_gate_naming_no_bucket_on_a_stateless_pipeline_is_refused() {
        let err = refused(BufferTransformConfig {
            max_messages: Some(10),
            until: Some(BufferGateConfig {
                bucket: None,
                key: None,
                conditions: ready(),
            }),
            ..config()
        });
        assert!(err.contains("no `state` block"), "{err}");
    }

    // ── the gate ────────────────────────────────────────────────────────────

    fn gated() -> Result<(Arc<Buckets>, Box<dyn Transform>)> {
        let buckets = Arc::new(Buckets::from_config(&declared()));
        let t = build(
            BufferTransformConfig {
                max_messages: Some(1000),
                until: Some(BufferGateConfig {
                    bucket: Some("control".to_string()),
                    key: Some("run-1".to_string()),
                    conditions: ready(),
                }),
                ..config()
            },
            &buckets,
            None,
        )?;
        Ok((buckets, t))
    }

    /// The whole point: messages pile up while the bucket says nothing, and
    /// the *whole* buffer goes out in one batch once it does.
    #[tokio::test]
    async fn a_gate_releases_everything_held_when_the_bucket_opens_it() -> Result<()> {
        let (buckets, mut t) = gated()?;

        let out = t.apply(batch(vec![json!({"i": 0})])).await?;
        assert!(out.is_empty(), "nothing is remembered yet, so nothing goes");
        let out = t.apply(batch(vec![json!({"i": 1})])).await?;
        assert!(out.is_empty());

        buckets.remember(
            "control",
            "run-1",
            vec![("status".to_string(), json!("ready"))],
        );

        // the gate is read once per arriving batch, so the next batch is what
        // notices — and it takes the messages it just added with it
        let out = t.apply(batch(vec![json!({"i": 2})])).await?;
        assert_eq!(out.len(), 1, "one batch, everything held: {out:?}");
        assert_eq!(out[0].len(), 3, "all three, not a subset");
        Ok(())
    }

    /// A write that doesn't satisfy the conditions is not an opening — the
    /// version bump alone must not release the buffer.
    #[tokio::test]
    async fn a_write_that_does_not_match_leaves_the_buffer_held() -> Result<()> {
        let (buckets, mut t) = gated()?;
        t.apply(batch(vec![json!({"i": 0})])).await?;

        buckets.remember(
            "control",
            "run-1",
            vec![("status".to_string(), json!("running"))],
        );

        let out = t.apply(batch(vec![json!({"i": 1})])).await?;
        assert!(out.is_empty(), "'running' is not 'ready': {out:?}");
        Ok(())
    }

    /// The gate reads *that* key and no other — a bucket is keyed, and a run
    /// finishing elsewhere is not this buffer's business.
    #[tokio::test]
    async fn a_gate_reads_only_its_own_key() -> Result<()> {
        let (buckets, mut t) = gated()?;
        t.apply(batch(vec![json!({"i": 0})])).await?;

        buckets.remember(
            "control",
            "run-2",
            vec![("status".to_string(), json!("ready"))],
        );

        let out = t.apply(batch(vec![json!({"i": 1})])).await?;
        assert!(out.is_empty(), "that was another run's key: {out:?}");
        Ok(())
    }

    /// `wakeup` is what makes a gate work at all on a stream that has gone
    /// quiet: the release must not wait for a message that isn't coming.
    #[tokio::test]
    async fn the_gate_wakes_the_run_loop_with_no_batch_arriving() -> Result<()> {
        let (buckets, mut t) = gated()?;
        t.apply(batch(vec![json!({"i": 0})])).await?;

        buckets.remember(
            "control",
            "run-1",
            vec![("status".to_string(), json!("ready"))],
        );

        // a write to the bucket should wake the transform
        tokio::time::timeout(Duration::from_secs(5), t.wakeup()).await?;
        let out = t.flush().await?;
        assert_eq!(out.len(), 1, "the flush should hand the held batch on");
        assert_eq!(out[0].len(), 1);
        Ok(())
    }

    /// An empty buffer has nothing to release, so it must not wake — that is
    /// what keeps an idle pipeline free of both the timer and the bucket.
    #[tokio::test]
    async fn an_empty_buffer_never_wakes() -> Result<()> {
        let (buckets, mut t) = gated()?;
        buckets.remember(
            "control",
            "run-1",
            vec![("status".to_string(), json!("ready"))],
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(50), t.wakeup())
                .await
                .is_err(),
            "an empty buffer woke up with nothing to hand on"
        );
        Ok(())
    }

    /// A wakeup is "look at me", not a promise: a flush with no trigger met
    /// hands nothing on and keeps holding.
    #[tokio::test]
    async fn a_flush_with_no_trigger_met_releases_nothing() -> Result<()> {
        let (_, mut t) = gated()?;
        t.apply(batch(vec![json!({"i": 0})])).await?;
        assert!(t.flush().await?.is_empty());
        Ok(())
    }

    // ── the window ──────────────────────────────────────────────────────────

    /// The clock starts at the first *held* message rather than at the last
    /// release, so what it bounds is how long a message waits.
    #[tokio::test(start_paused = true)]
    async fn the_window_releases_everything_held_when_it_closes() -> Result<()> {
        let mut t = built(BufferTransformConfig {
            seconds: Some(30),
            max_messages: Some(1000),
            ..config()
        })?;

        t.apply(batch(vec![json!({"i": 0}), json!({"i": 1})]))
            .await?;
        assert!(t.flush().await?.is_empty(), "the window is open");

        tokio::time::advance(Duration::from_secs(31)).await;
        t.wakeup().await;
        let out = t.flush().await?;
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].len(), 2, "everything held, not a subset");
        Ok(())
    }

    // ── the backstop ────────────────────────────────────────────────────────

    /// The bound is real and does not depend on a trigger ever firing.
    #[tokio::test]
    async fn the_backstop_releases_a_buffer_whose_gate_never_opens() -> Result<()> {
        let (_, mut t) = gated()?;
        let out = t
            .apply(batch((0..1000).map(|i| json!({ "i": i })).collect()))
            .await?;
        assert_eq!(out.len(), 1, "max_messages should have released them");
        assert_eq!(out[0].len(), 1000);
        Ok(())
    }
}
