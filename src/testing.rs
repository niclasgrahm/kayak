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
    async fn next(&mut self) -> Result<Arc<MessageBatch>> {
        match self.batches.next() {
            Some(b) => Ok(b),
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
    async fn next(&mut self) -> Result<Arc<MessageBatch>> {
        tokio::time::sleep(self.interval).await;
        Ok(Arc::new(vec![Arc::new(self.message.clone())]))
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
    /// When set, every `emit` fails — used to check that a broken output does
    /// not tear the pipeline down.
    fail_emit: bool,
}

impl CollectingOutput {
    #[must_use]
    pub fn new() -> Self {
        Self {
            emitted: Emitted::default(),
            init_calls: Arc::new(Mutex::new(0)),
            fail_emit: false,
        }
    }

    #[must_use]
    pub fn failing() -> Self {
        Self {
            fail_emit: true,
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
        *lock(&self.init_calls) += 1;
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
        }],
        transforms: Vec::<TransformConfig>::new(),
        outputs: vec![OutputConfig {
            kind: OutputKind::Stdout(StdoutOutputConfig {}),
        }],
    }
}
