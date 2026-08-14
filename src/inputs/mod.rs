use anyhow::Result;
use std::sync::Arc;
use tokio::sync::broadcast;

use crate::BuildCtx;
use crate::events::publish;
use crate::state::{PipelineId, UiEvent};
use kayak_core::Stage;

pub mod ack;
pub mod dummy;
pub mod envelope;
pub mod http;
pub mod kafka;
pub mod mqtt;
pub mod nats;
pub mod opcua;
pub mod pipeline;
pub mod redis;

pub use ack::{Ack, Delivery};

pub trait BuildInput {
    fn build(self, ctx: &mut BuildCtx) -> anyhow::Result<Box<dyn InputSource>>;
}

/// How many messages an input may put in one batch, from what the config said.
///
/// **The default is one, and that is a promise rather than a tuning choice.**
/// Batching is worth a great deal when a consumer is catching up — it is the
/// difference between a run loop doing its per-batch work once and doing it a
/// hundred times — but a pipeline whose messages must not be grouped is a real
/// thing, and an input that quietly coalesced them would be wrong in a way that
/// is very hard to see from the outside. So it is opt-in: a config that says
/// nothing gets exactly the one-message batches it always got.
///
/// Zero is read as one rather than refused. It can only mean "don't batch", and
/// failing a pipeline to build over it would be a worse answer than the one the
/// user obviously wanted.
#[must_use]
pub fn batch_cap(configured: Option<usize>) -> usize {
    configured.unwrap_or(1).max(1)
}

// pub type MessageBatch = Vec<Arc<serde_json::Value>>;
pub use kayak_core::MessageBatch;

#[async_trait::async_trait]
pub trait InputSource: Send + 'static {
    async fn next(&mut self) -> anyhow::Result<Delivery>;
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
    rx: tokio::sync::mpsc::Receiver<anyhow::Result<Delivery>>,
    /// Inputs that haven't reported a failure yet. Each one reports exactly
    /// once, so this reaches zero exactly when the last input is gone.
    alive: usize,
    /// Kept so the pump tasks die with the pipeline they belong to.
    pumps: Vec<tokio::task::JoinHandle<()>>,
    pipeline_id: PipelineId,
    events: broadcast::Sender<UiEvent>,
}

impl Merged {
    #[must_use]
    pub fn new(
        inputs: Vec<Box<dyn InputSource>>,
        pipeline_id: PipelineId,
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
                        // stop on a closed channel (the pipeline is gone) or on
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
            pipeline_id,
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
    async fn next(&mut self) -> Result<Delivery> {
        loop {
            // None means every pump has stopped without us noticing, which the
            // counting below should have caught first
            let Some(res) = self.rx.recv().await else {
                return Err(anyhow::anyhow!("every input of this pipeline has stopped"));
            };
            let Err(e) = res else {
                return res;
            };

            // One input failing is not the pipeline failing: the others are
            // still feeding it, and returning here would stop the run loop and
            // take them down too. Report it and carry on; only the *last*
            // input's failure ends the pipeline.
            self.alive = self.alive.saturating_sub(1);
            if self.alive == 0 {
                return Err(e.context("every input of this pipeline has stopped"));
            }
            tracing::error!(
                "[{}]\t one input stopped, {} still running: {:?}",
                self.pipeline_id,
                self.alive,
                e
            );
            publish(&self.events, || {
                UiEvent::error(self.pipeline_id.clone(), Stage::Input, &e)
            });
        }
    }
}

/// One input source from many: the input itself when there is only one, so a
/// single-input pipeline pays nothing for the merge.
pub fn merge(
    mut inputs: Vec<Box<dyn InputSource>>,
    pipeline_id: PipelineId,
    events: broadcast::Sender<UiEvent>,
) -> Result<Box<dyn InputSource>> {
    match inputs.len() {
        0 => Err(anyhow::anyhow!(
            "a pipeline needs at least one input; `inputs` is empty"
        )),
        1 => Ok(inputs.remove(0)),
        _ => Ok(Box::new(Merged::new(inputs, pipeline_id, events))),
    }
}

/// The live half of [`kayak_core::config::BufferConfig`]: a count, a window, or
/// both.
///
/// The three shapes are one rule with halves left off, which is why [`limits`]
/// flattens them into a pair of options rather than [`Buffered::next`] having
/// three loops in it — the combined case is not a third behaviour, it is the
/// other two at once.
///
/// [`limits`]: BufferKind::limits
pub enum BufferKind {
    Static { size: usize },
    Tumbling { window_seconds: usize },
    Batch { size: usize, window_seconds: usize },
    // Sliding {
    //     window_seconds: usize,
    //     step_seconds: usize,
    // },
}

impl BufferKind {
    /// The two limits this kind imposes: how many messages end a batch, and how
    /// long the batch may gather for.
    ///
    /// Zero is read as "one" and as "immediately" rather than refused, the same
    /// answer [`batch_cap`] gives: a zero size can only mean "don't batch", and
    /// returning an empty batch forever is the only other thing it could do.
    fn limits(&self) -> (Option<usize>, Option<std::time::Duration>) {
        let secs = |s: usize| std::time::Duration::from_secs(s as u64);
        match *self {
            Self::Static { size } => (Some(size.max(1)), None),
            Self::Tumbling { window_seconds } => (None, Some(secs(window_seconds))),
            Self::Batch {
                size,
                window_seconds,
            } => (Some(size.max(1)), Some(secs(window_seconds))),
        }
    }
}

impl From<kayak_core::config::BufferConfig> for BufferKind {
    fn from(config: kayak_core::config::BufferConfig) -> Self {
        use kayak_core::config::BufferConfig;
        match config {
            BufferConfig::Static { size } => Self::Static { size },
            BufferConfig::Tumbling { window_seconds } => Self::Tumbling { window_seconds },
            BufferConfig::Batch {
                size,
                window_seconds,
            } => Self::Batch {
                size,
                window_seconds,
            },
        }
    }
}

pub struct Buffered {
    rx: tokio::sync::mpsc::Receiver<anyhow::Result<Delivery>>,
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
    /// Gather messages until whichever configured limit is reached first, and
    /// never hand on an empty batch.
    ///
    /// The window is started by the *first message of the batch* rather than by
    /// the call, which is the whole of what "never empty" means here: a quiet
    /// input parks on the `recv()` with no timer running instead of waking every
    /// window to emit nothing. The cost — deliberate — is that windows are no
    /// longer aligned to a wall clock; the promise a buffer makes is a bound on
    /// how long a message waits, not a cadence.
    ///
    /// One outer [`Delivery`] comes out of several inner ones going in, so its
    /// ack is a [`ack::CombinedAck`] of everything folded into it — the run loop
    /// only ever sees the one outer `Ack`, and acknowledging it acknowledges
    /// every inner delivery this batch was gathered from.
    async fn next(&mut self) -> Result<Delivery> {
        let (size, window) = self.kind.limits();
        let mut out: MessageBatch = Vec::new();
        let mut acks: Vec<Box<dyn Ack>> = Vec::new();
        // only exists once the batch has something in it, so it also *is* the
        // "have we started" flag
        let mut window_closed: Option<std::pin::Pin<Box<tokio::time::Sleep>>> = None;

        loop {
            if size.is_some_and(|size| out.len() >= size) {
                return Ok(Delivery::with_ack(
                    Arc::new(out),
                    Box::new(ack::CombinedAck(acks)),
                ));
            }

            let received = match &mut window_closed {
                // an unbiased `select!` on purpose: with no size limit and a
                // saturated input, preferring the receive would starve the timer
                Some(closed) => tokio::select! {
                    maybe = self.rx.recv() => maybe,
                    () = closed.as_mut() => return Ok(Delivery::with_ack(
                        Arc::new(out),
                        Box::new(ack::CombinedAck(acks)),
                    )),
                },
                None => self.rx.recv().await,
            };

            match received {
                // an empty batch upstream neither fills this one nor starts its
                // clock — it is not a message. Its own ack is not folded into
                // the eventual outer one: nothing of it will ever reach an
                // output, so there is nothing left to wait for.
                Some(Ok(delivery)) if delivery.batch.is_empty() => {
                    delivery.ack.ack();
                }
                Some(Ok(delivery)) => {
                    out.extend(delivery.batch.iter().cloned());
                    acks.push(delivery.ack);
                    if window_closed.is_none() {
                        window_closed = window.map(|w| Box::pin(tokio::time::sleep(w)));
                    }
                }
                Some(Err(e)) => return Err(e),
                // the pump stopped: hand on what we have, and say so on the
                // call after that. Reporting the stop with a batch in hand
                // would throw those messages away, and reporting it as an empty
                // batch would spin the run loop forever.
                None if out.is_empty() => {
                    return Err(anyhow::anyhow!("the buffered input has stopped"));
                }
                None => {
                    return Ok(Delivery::with_ack(
                        Arc::new(out),
                        Box::new(ack::CombinedAck(acks)),
                    ));
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::batch_cap;
    use kayak_core::config::{KafkaConfig, NatsConfig};

    /// The promise: an input that wasn't asked to batch doesn't. A pipeline
    /// that needs its messages one at a time is a real case, so this is the
    /// behaviour to break a build over rather than a default to tune.
    #[test]
    fn an_input_that_was_not_asked_to_batch_emits_one_message_per_batch() {
        assert_eq!(batch_cap(None), 1);
    }

    /// Zero can only mean "don't batch", so it reads as one. Refusing to build
    /// the pipeline would be a worse answer than the obvious one.
    #[test]
    fn a_cap_of_zero_reads_as_one_rather_than_failing() {
        assert_eq!(batch_cap(Some(0)), 1);
    }

    #[test]
    fn a_configured_cap_is_taken_as_it_is() {
        assert_eq!(batch_cap(Some(500)), 500);
    }

    /// The wire default has to match: a config file with no `max_batch` must
    /// deserialize to the un-batched behaviour, not to whatever serde would
    /// otherwise pick.
    #[test]
    fn a_config_without_a_cap_leaves_it_unset() {
        let Ok(kafka) = serde_json::from_value::<KafkaConfig>(serde_json::json!({
            "connection": "local-kafka",
            "topic": "test.events",
            "group": "kayak",
        })) else {
            panic!("a kafka input without `max_batch` should parse");
        };
        assert_eq!(kafka.max_batch, None);
        assert_eq!(batch_cap(kafka.max_batch), 1);

        let Ok(nats) = serde_json::from_value::<NatsConfig>(serde_json::json!({
            "connection": "local-nats",
            "subject": "test.subject",
        })) else {
            panic!("a nats input without `max_batch` should parse");
        };
        assert_eq!(nats.max_batch, None);
        assert_eq!(batch_cap(nats.max_batch), 1);
    }
}
