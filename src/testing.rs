//! Test doubles for the three plugin traits, plus small builders for the config
//! types.
//!
//! This module is compiled into the normal library rather than hidden behind
//! `#[cfg(test)]`, because the integration tests in `tests/` are separate crates
//! and can only see the public API. The types here are inert — nothing
//! constructs them unless a test does — so the cost is a few hundred bytes of
//! code in the binary.

use std::sync::{Arc, Mutex, MutexGuard};

use anyhow::{Result, anyhow};
use kayak_core::config::{
    Config, DummyConfig, InputConfig, InputKind, OutputConfig, OutputKind, StdoutOutputConfig,
    TransformConfig,
};
use serde_json::Value;

use crate::inputs::ack::Delivery;
use crate::inputs::{InputSource, MessageBatch};
use crate::outputs::OutputDestination;
use crate::secrets::SecretStore;
use crate::transforms::Transform;

/// An in-memory [`SecretStore`], so tests about resolution don't have to touch
/// the process environment or the filesystem.
pub struct MapSecretStore {
    values: std::collections::HashMap<String, String>,
    name: String,
}

impl MapSecretStore {
    /// `name` is what shows up in "not set in ..." errors, which lets a test
    /// tell two chained stores apart.
    #[must_use]
    pub fn new(name: &str, values: &[(&str, &str)]) -> Self {
        Self {
            values: values
                .iter()
                .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
                .collect(),
            name: name.to_string(),
        }
    }

    /// A store that knows nothing — for asserting that a missing secret fails.
    #[must_use]
    pub fn empty() -> Self {
        Self::new("an empty test store", &[])
    }
}

impl SecretStore for MapSecretStore {
    fn get(&self, name: &str) -> Option<String> {
        self.values.get(name).cloned()
    }

    fn describe(&self) -> String {
        self.name.clone()
    }
}

/// Wrap one JSON value as a single-message batch.
#[must_use]
pub fn batch(values: Vec<Value>) -> Arc<MessageBatch> {
    Arc::new(values.into_iter().map(Arc::new).collect())
}

/// Recover from a poisoned test lock instead of panicking — a panic inside an
/// assertion would otherwise be reported as a lock error somewhere unrelated.
fn lock<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// What a [`ScriptedInput`] does once its script runs out.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum WhenExhausted {
    /// Never resolve again. The run loop stays alive until it is cancelled —
    /// use this when the test is about cancellation or about staying up.
    Pend,
    /// Return an error, which makes the run loop log and exit. Use this when
    /// the test wants to await the pipeline's completion.
    Fail,
}

/// An input that replays a fixed script of batches.
pub struct ScriptedInput {
    batches: std::vec::IntoIter<Arc<MessageBatch>>,
    when_exhausted: WhenExhausted,
}

impl ScriptedInput {
    #[must_use]
    pub fn new(batches: Vec<Arc<MessageBatch>>, when_exhausted: WhenExhausted) -> Self {
        Self {
            batches: batches.into_iter(),
            when_exhausted,
        }
    }
}

#[async_trait::async_trait]
impl InputSource for ScriptedInput {
    async fn next(&mut self) -> Result<Delivery> {
        match self.batches.next() {
            Some(b) => Ok(Delivery::new(b)),
            None => match self.when_exhausted {
                WhenExhausted::Pend => std::future::pending().await,
                WhenExhausted::Fail => Err(anyhow!("scripted input exhausted")),
            },
        }
    }
}

/// An input that yields the same message on a fixed interval, forever.
///
/// The point of it is the *waiting*: unlike [`ScriptedInput`], its `next()`
/// spends most of its life pending on a timer, which is what makes it useful
/// for testing that merging several inputs doesn't starve a slow one.
pub struct Ticking {
    interval: std::time::Duration,
    message: serde_json::Value,
}

impl Ticking {
    #[must_use]
    pub fn new(interval: std::time::Duration, message: serde_json::Value) -> Self {
        Self { interval, message }
    }
}

#[async_trait::async_trait]
impl InputSource for Ticking {
    async fn next(&mut self) -> Result<Delivery> {
        tokio::time::sleep(self.interval).await;
        Ok(Delivery::new(Arc::new(vec![Arc::new(self.message.clone())])))
    }
}

/// An [`Ack`] that counts how many times it fired, shared with the test that
/// built it — for tests about *whether* and *how many times* the run loop
/// acknowledges a delivery, not about what acknowledging it does.
#[derive(Clone, Default)]
pub struct CountingAck(Arc<std::sync::atomic::AtomicUsize>);

impl CountingAck {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn count(&self) -> usize {
        self.0.load(std::sync::atomic::Ordering::SeqCst)
    }
}

impl crate::inputs::Ack for CountingAck {
    fn ack(&self) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}

/// An input that yields one batch carrying a given [`Ack`], then fails — the
/// same "one batch, then done" shape [`ScriptedInput`] with
/// [`WhenExhausted::Fail`] gives, but able to carry a real acknowledgement
/// rather than [`Delivery::new`]'s [`crate::inputs::ack::NoAck`].
pub struct AckingInput {
    delivery: Option<(Arc<MessageBatch>, CountingAck)>,
}

impl AckingInput {
    #[must_use]
    pub fn new(batch: Arc<MessageBatch>, ack: CountingAck) -> Self {
        Self {
            delivery: Some((batch, ack)),
        }
    }
}

#[async_trait::async_trait]
impl InputSource for AckingInput {
    async fn next(&mut self) -> Result<Delivery> {
        match self.delivery.take() {
            Some((batch, ack)) => Ok(Delivery::with_ack(batch, Box::new(ack))),
            None => Err(anyhow!("acking input exhausted")),
        }
    }
}

/// Everything a [`CollectingOutput`] saw, shared with the test that spawned it.
#[derive(Clone, Default)]
pub struct Emitted(Arc<Mutex<Vec<Arc<MessageBatch>>>>);

impl Emitted {
    /// Every batch handed to `emit`, in order.
    #[must_use]
    pub fn batches(&self) -> Vec<Arc<MessageBatch>> {
        lock(&self.0).clone()
    }

    /// The emitted batches flattened to plain JSON, which is usually what an
    /// assertion actually wants to compare against.
    #[must_use]
    pub fn values(&self) -> Vec<Vec<Value>> {
        self.batches()
            .iter()
            .map(|b| b.iter().map(|m| (**m).clone()).collect())
            .collect()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        lock(&self.0).len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Wait until at least `n` batches have been emitted. Returns `false` on
    /// timeout so the caller can assert with a useful message.
    pub async fn wait_for(&self, n: usize, timeout: std::time::Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        while tokio::time::Instant::now() < deadline {
            if self.len() >= n {
                return true;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        self.len() >= n
    }
}

/// An output that records what it was given.
pub struct CollectingOutput {
    emitted: Emitted,
    init_calls: Arc<Mutex<usize>>,
    finish_calls: Arc<Mutex<usize>>,
    /// When set, every `emit` fails — used to check that a broken output does
    /// not tear the pipeline down.
    fail_emit: bool,
    /// How many more `init` calls fail before one succeeds. `usize::MAX` for
    /// an output that never initialises — a database that is simply not
    /// running, which is the case the run loop's retry exists for.
    fail_init: usize,
}

impl CollectingOutput {
    #[must_use]
    pub fn new() -> Self {
        Self {
            emitted: Emitted::default(),
            init_calls: Arc::new(Mutex::new(0)),
            finish_calls: Arc::new(Mutex::new(0)),
            fail_emit: false,
            fail_init: 0,
        }
    }

    #[must_use]
    pub fn failing() -> Self {
        Self {
            fail_emit: true,
            ..Self::new()
        }
    }

    /// An output whose first `attempts` `init` calls fail and whose next one
    /// succeeds — a destination that is down when the pipeline starts and
    /// comes back while it is waiting. `usize::MAX` for one that never comes
    /// back.
    #[must_use]
    pub fn failing_init(attempts: usize) -> Self {
        Self {
            fail_init: attempts,
            ..Self::new()
        }
    }

    /// Handle to inspect from the test while the output is owned by a runtime.
    #[must_use]
    pub fn emitted(&self) -> Emitted {
        self.emitted.clone()
    }

    #[must_use]
    pub fn init_calls(&self) -> Arc<Mutex<usize>> {
        Arc::clone(&self.init_calls)
    }

    /// How many times the run loop has called `finish`. The outputs that hold a
    /// part depend on being told the run is over, so "was it told" is worth
    /// being able to ask without a filesystem or a bucket in the way.
    #[must_use]
    pub fn finish_calls(&self) -> Arc<Mutex<usize>> {
        Arc::clone(&self.finish_calls)
    }
}

impl Default for CollectingOutput {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait::async_trait]
impl OutputDestination for CollectingOutput {
    async fn emit(&mut self, message_batch: Arc<MessageBatch>) -> Result<()> {
        if self.fail_emit {
            return Err(anyhow!("collecting output was told to fail"));
        }
        lock(&self.emitted.0).push(message_batch);
        Ok(())
    }

    async fn init(&mut self) -> Result<()> {
        let attempt = {
            let mut calls = lock(&self.init_calls);
            *calls += 1;
            *calls
        };
        if self.fail_init > 0 {
            self.fail_init = self.fail_init.saturating_sub(1);
            return Err(anyhow!(
                "collecting output could not initialise (attempt {attempt})"
            ));
        }
        Ok(())
    }

    async fn finish(&mut self) -> Result<()> {
        *lock(&self.finish_calls) += 1;
        Ok(())
    }
}

/// A transform that fails on the nth batch (0-indexed) and passes everything
/// else straight through.
pub struct FailOnNth {
    pub nth: usize,
    seen: usize,
}

impl FailOnNth {
    #[must_use]
    pub fn new(nth: usize) -> Self {
        Self { nth, seen: 0 }
    }
}

#[async_trait::async_trait]
impl Transform for FailOnNth {
    async fn apply(&mut self, message_batch: Arc<MessageBatch>) -> Result<Vec<Arc<MessageBatch>>> {
        let seen = self.seen;
        self.seen += 1;
        if seen == self.nth {
            return Err(anyhow!("transform failed on batch {seen}"));
        }
        Ok(vec![message_batch])
    }
}

/// A transform that drops every batch it sees — the test-double equivalent of
/// a `filter` or a reducer's `group_by` finding nothing to keep, for tests
/// about what happens to a delivery whose batch never reaches an output.
pub struct DropEverything;

#[async_trait::async_trait]
impl Transform for DropEverything {
    async fn apply(&mut self, _message_batch: Arc<MessageBatch>) -> Result<Vec<Arc<MessageBatch>>> {
        Ok(vec![])
    }
}

/// A minimal valid `Config`. `Pipeline` only reads the config for its id and for
/// `/api/pipelines` output, so tests that drive a runtime directly can use this
/// regardless of which components they actually wire up.
#[must_use]
pub fn stub_config(id: &str) -> Config {
    Config {
        id: Some(id.to_string()),
        inputs: vec![InputConfig {
            kind: InputKind::Dummy(DummyConfig { duration: 3600, payload: None, amplitude: None, period: None }),
            buffer: None,
            envelope: None,
            ack: None,
        }],
        transforms: Vec::<TransformConfig>::new(),
        outputs: vec![OutputConfig {
            kind: OutputKind::Stdout(StdoutOutputConfig {}),
        }],
        state: None,
    }
}

/// An input that yields the same pre-built batch as fast as the run loop asks
/// for one — the load generator the throughput harness (`kayak-bench`) is
/// built on.
///
/// Deliberately **not** an `InputKind`. Nothing in the product's config surface
/// can produce load, and the reason to keep it that way is that a config file
/// is a thing people commit and run: an input whose whole purpose is to
/// saturate a core does not belong in the same list as `nats` and `http`. It
/// lives here with the other doubles, and the bench crate is its only caller.
///
/// Two properties are load-bearing, both about the numbers meaning anything:
///
/// - **It spends cooperative budget once per batch.** A `next()` that returned
///   `Ready` without ever awaiting would never hand a tokio worker back —
///   tokio's budget is spent by *resources* (channels, timers, sockets) and a
///   loop that touches none of them is invisible to it. One pipeline would
///   hold a worker forever, so a sweep over a hundred of them would measure
///   eight running and ninety-two starved.
///
///   [`tokio::task::consume_budget`] rather than `yield_now`, and the
///   difference is not a micro-optimisation: `yield_now` reschedules on
///   *every* call, and with a single task on the runtime that round trip goes
///   through a park and an unpark. Measured, it cost more than the entire run
///   loop — a sweep built on it reported one pipeline as slower than each of
///   ten, and reported adding a filter as making the pipeline three times
///   faster. `consume_budget` decrements the same budget every resource does
///   and only yields when it runs out, so fairness is kept and what the sweep
///   measures is the run loop.
/// - **The message is fixed** ([`LoadInput::MESSAGE_FIELDS`]) and the batch is
///   built once and handed out by `Arc`. Changing either changes what every
///   recorded number means, so a committed baseline taken before the change is
///   no longer comparable to one taken after. Treat this shape as part of the
///   baseline format rather than as a detail of the double.
pub struct LoadInput {
    batch: Arc<MessageBatch>,
}

impl LoadInput {
    /// The field names of the generated message, spelled out because they are
    /// part of what a baseline measures — a wider message is more serialization
    /// per pass and would move every number in the report.
    pub const MESSAGE_FIELDS: [&'static str; 5] =
        ["sensor_id", "value", "recorded_at", "site", "reading"];

    /// A generator of `batch_size` identical messages per batch.
    ///
    /// `batch_size` is the knob that separates per-batch cost from per-message
    /// cost: the run loop does its select, its throttle check and its fan-out
    /// once per batch whatever the size, so comparing a run at 1 against a run
    /// at 1000 is what says which of the two a change moved.
    #[must_use]
    pub fn new(batch_size: usize) -> Self {
        let message = Arc::new(serde_json::json!({
            "sensor_id": "sensor-0042",
            "value": 21.5,
            "recorded_at": "2026-01-01T00:00:00Z",
            "site": "north",
            "reading": { "unit": "celsius", "quality": "good" },
        }));
        Self {
            batch: Arc::new(std::iter::repeat_n(message, batch_size).collect()),
        }
    }

    /// A generator of a batch the caller built, for a bench that needs a
    /// particular shape — a payload a `map` mapping actually reads, say.
    #[must_use]
    pub fn with_batch(batch: Arc<MessageBatch>) -> Self {
        Self { batch }
    }
}

#[async_trait::async_trait]
impl InputSource for LoadInput {
    async fn next(&mut self) -> Result<Delivery> {
        // see the type's docs: this is what keeps a run loop from owning a
        // worker outright, and it is where a real input would be spending the
        // same budget on a socket
        tokio::task::consume_budget().await;
        Ok(Delivery::new(Arc::clone(&self.batch)))
    }
}

/// An output that discards everything, as fast as it is given it.
///
/// The counterpart to [`LoadInput`]: between the two, a run of the pipeline
/// measures the run loop and the transforms in it and nothing else. Unlike
/// [`CollectingOutput`] it keeps nothing, which matters at a million messages —
/// a bench that accumulated its own input would be measuring an allocator.
#[derive(Debug, Default)]
pub struct NullOutput;

#[async_trait::async_trait]
impl OutputDestination for NullOutput {
    async fn init(&mut self) -> Result<()> {
        Ok(())
    }

    async fn emit(&mut self, _message_batch: Arc<MessageBatch>) -> Result<()> {
        Ok(())
    }
}
