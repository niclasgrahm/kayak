use anyhow::Result;
use std::sync::Arc;
use tokio::sync::broadcast;

use crate::BuildCtx;
use crate::state::{StreamerId, UiEvent};
use streamer_core::stage;

pub mod dummy;
pub mod kafka;
pub mod nats;
pub mod streamer;

pub trait BuildInput {
    fn build(self, ctx: &mut BuildCtx) -> anyhow::Result<Box<dyn InputSource>>;
}

// pub type MessageBatch = Vec<Arc<serde_json::Value>>;
pub use streamer_core::MessageBatch;

#[async_trait::async_trait]
pub trait InputSource: Send + 'static {
    async fn next(&mut self) -> anyhow::Result<Arc<MessageBatch>>;
}

/// Reads from several inputs at once, handing on whichever produces first.
///
/// Each input gets its own task pumping into a bounded channel rather than the
/// run loop selecting over them directly. Selecting would drop the losing
/// `next()` futures on every iteration, and not every input survives that: the
/// dummy ticker restarts its sleep, so a fast sibling would starve it forever.
/// A pump per input means every `next()` runs to completion.
///
/// The channel holds one batch per input, so a slow consumer still pushes back
/// on every input — the merge adds no unbounded buffering.
pub struct Merged {
    rx: tokio::sync::mpsc::Receiver<anyhow::Result<Arc<MessageBatch>>>,
    /// Inputs that haven't reported a failure yet. Each one reports exactly
    /// once, so this reaches zero exactly when the last input is gone.
    alive: usize,
    /// Kept so the pump tasks die with the streamer they belong to.
    pumps: Vec<tokio::task::JoinHandle<()>>,
    streamer_id: StreamerId,
    events: broadcast::Sender<UiEvent>,
}

impl Merged {
    #[must_use]
    pub fn new(
        inputs: Vec<Box<dyn InputSource>>,
        streamer_id: StreamerId,
        events: broadcast::Sender<UiEvent>,
    ) -> Self {
        let alive = inputs.len();
        let (tx, rx) = tokio::sync::mpsc::channel(alive.max(1));
        let pumps = inputs
            .into_iter()
            .map(|mut input| {
                let tx = tx.clone();
                tokio::spawn(async move {
                    loop {
                        let res = input.next().await;
                        let failed = res.is_err();
                        // stop on a closed channel (the streamer is gone) or on
                        // an error, which is this input's last word either way
                        if tx.send(res).await.is_err() || failed {
                            break;
                        }
                    }
                })
            })
            .collect();
        Self {
            rx,
            alive,
            pumps,
            streamer_id,
            events,
        }
    }
}

impl Drop for Merged {
    fn drop(&mut self) {
        for pump in &self.pumps {
            pump.abort();
        }
    }
}

#[async_trait::async_trait]
impl InputSource for Merged {
    async fn next(&mut self) -> Result<Arc<MessageBatch>> {
        loop {
            // None means every pump has stopped without us noticing, which the
            // counting below should have caught first
            let Some(res) = self.rx.recv().await else {
                return Err(anyhow::anyhow!("every input of this streamer has stopped"));
            };
            let Err(e) = res else {
                return res;
            };

            // One input failing is not the pipeline failing: the others are
            // still feeding it, and returning here would stop the run loop and
            // take them down too. Report it and carry on; only the *last*
            // input's failure ends the streamer.
            self.alive = self.alive.saturating_sub(1);
            if self.alive == 0 {
                return Err(e.context("every input of this streamer has stopped"));
            }
            tracing::error!(
                "[{}]\t one input stopped, {} still running: {:?}",
                self.streamer_id,
                self.alive,
                e
            );
            if self.events.receiver_count() > 0 {
                let _ = self
                    .events
                    .send(UiEvent::error(self.streamer_id.clone(), stage::INPUT, &e));
            }
        }
    }
}

/// One input source from many: the input itself when there is only one, so a
/// single-input pipeline pays nothing for the merge.
pub fn merge(
    mut inputs: Vec<Box<dyn InputSource>>,
    streamer_id: StreamerId,
    events: broadcast::Sender<UiEvent>,
) -> Result<Box<dyn InputSource>> {
    match inputs.len() {
        0 => Err(anyhow::anyhow!(
            "a streamer needs at least one input; `inputs` is empty"
        )),
        1 => Ok(inputs.remove(0)),
        _ => Ok(Box::new(Merged::new(inputs, streamer_id, events))),
    }
}

pub enum BufferKind {
    Static { size: usize },
    Tumbling { window_seconds: usize },
    // Sliding {
    //     window_seconds: usize,
    //     step_seconds: usize,
    // },
}

pub struct Buffered {
    rx: tokio::sync::mpsc::Receiver<anyhow::Result<Arc<MessageBatch>>>,
    kind: BufferKind,
}

impl Buffered {
    #[must_use] 
    pub fn new(inner: Box<dyn InputSource>, kind: BufferKind) -> Self {
        let (tx, rx) = tokio::sync::mpsc::channel(1);
        tokio::spawn(async move {
            let mut inner = inner;
            loop {
                let res = inner.next().await;
                let failed = res.is_err();
                if tx.send(res).await.is_err() || failed {
                    break;
                }
            }
        });
        Self { rx, kind }
    }
}

#[async_trait::async_trait]
impl InputSource for Buffered {
    async fn next(&mut self) -> Result<Arc<MessageBatch>> {
        match self.kind {
            BufferKind::Static { size } => {
                let mut out = Vec::new();
                for _ in 0..size {
                    match self.rx.recv().await {
                        Some(Ok(batch)) => out.extend(batch.iter().cloned()),
                        Some(Err(e)) => return Err(e),
                        None => break,
                    }
                }
                Ok(Arc::new(out))
            }
            BufferKind::Tumbling { window_seconds } => {
                let mut out = Vec::new();
                let deadline = tokio::time::Instant::now()
                    + std::time::Duration::from_secs(window_seconds as u64);
                let mut window_closed = std::pin::pin!(tokio::time::sleep_until(deadline));

                loop {
                    tokio::select! {
                        maybe = self.rx.recv() => match maybe {
                            Some(Ok(batch)) => out.extend(batch.iter().cloned()),
                            Some(Err(e)) => return Err(e),
                            None => return Ok(Arc::new(out)),
                        },
                        () = &mut window_closed => return Ok(Arc::new(out)),
                    }
                }
            } // BufferKind::Tumbling { window_seconds } => {
              //     let start = std::time::Instant::now();
              //     while start.elapsed().as_secs() < window_seconds as u64 {
              //         let inner_batch = self.inner.next().await?;
              //         batch.extend(inner_batch.iter().cloned());
              //     }
              // }
              // BufferKind::Sliding {
              //     window_seconds,
              //     step_seconds,
              // } => {
              //     let mut last_step = std::time::Instant::now();
              //     let start = std::time::Instant::now();
              //     while start.elapsed().as_secs() < window_seconds as u64 {
              //         let inner_batch = self.inner.next().await?;
              //         batch.extend(inner_batch.iter().cloned());
              //         if last_step.elapsed().as_secs() >= step_seconds as u64 {
              //             last_step = std::time::Instant::now();
              //             batch.clear();
              //         }
              //     }
              // }
        }
    }
}
