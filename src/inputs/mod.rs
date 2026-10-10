use anyhow::Result;
use std::sync::Arc;
use tokio::sync::broadcast;

use crate::BuildCtx;
use crate::events::publish;
use crate::state::{PipelineId, UiEvent};
use kayak_core::Stage;

pub mod ack;
pub mod clickhouse;
pub mod dummy;
pub mod envelope;
pub mod http;
pub mod http_poll;
pub mod indu;
pub mod kafka;
pub mod mqtt;
pub mod nats;
pub mod opcua;
pub mod pipeline;
pub mod poll;
pub mod postgres;
pub mod redis;
pub mod wal;

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

    /// Whether checkpoint barriers can arrive through this input — true of a
    /// `pipeline` input and of anything wrapping one. A run loop none of whose
    /// inputs carry barriers is a *root*, and takes its barriers from the
    /// coordinator instead. See `docs/durability.md`.
    fn carries_barriers(&self) -> bool {
        false
    }
}

/// Reads from several inputs at once, handing on whichever produces first.
///
/// Each input gets its own task pumping into a bounded channel rather than the
/// run loop selecting over them directly. Selecting would drop the losing
/// `next()` futures on every iteration, and not every input survives that: the
/// dummy ticker restarts its sleep, so a fast sibling would starve it forever.
/// A pump per input means every `next()` runs to completion.
///
/// Each pump has a channel of its own holding one batch, so a slow consumer
/// still pushes back on every input — the merge adds no unbounded buffering.
/// One channel *per* input rather than one shared is what makes **barrier
/// alignment** possible: once a checkpoint barrier has come through one
/// `pipeline` input, that input is not read again until the same barrier has
/// come through every other one, so nothing it sends after the barrier can
/// reach the run loop ahead of the snapshot. See `docs/durability.md`.
pub struct Merged {
    /// One per input, `None` once that input has stopped.
    rxs: Vec<Option<tokio::sync::mpsc::Receiver<anyhow::Result<Delivery>>>>,
    /// Which inputs barriers come through — the ones alignment waits on.
    carries: Vec<bool>,
    /// The barrier being aligned, and which inputs have delivered it.
    aligning: Option<ack::Barrier>,
    blocked: Vec<bool>,
    /// Where the next poll starts, so a busy input early in the list cannot
    /// starve the ones after it.
    next_start: usize,
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
        let carries = inputs.iter().map(|input| input.carries_barriers()).collect();
        let mut rxs = Vec::with_capacity(alive);
        let pumps = inputs
            .into_iter()
            .map(|mut input| {
                let (tx, rx) = tokio::sync::mpsc::channel(1);
                rxs.push(Some(rx));
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
            rxs,
            carries,
            aligning: None,
            blocked: vec![false; alive],
            next_start: 0,
            alive,
            pumps,
            pipeline_id,
            events,
        }
    }

    /// The next thing any unblocked input has, and which input it came from.
    /// `None` as the second half is a pump whose channel closed without a
    /// last word, which only an aborted pump does.
    fn poll_any(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<(usize, Option<anyhow::Result<Delivery>>)> {
        let count = self.rxs.len();
        for step in 0..count {
            let index = (self.next_start + step) % count;
            if self.blocked[index] {
                continue;
            }
            let Some(rx) = self.rxs[index].as_mut() else {
                continue;
            };
            if let std::task::Poll::Ready(res) = rx.poll_recv(cx) {
                self.next_start = (index + 1) % count;
                return std::task::Poll::Ready((index, res));
            }
        }
        std::task::Poll::Pending
    }

    /// Note that `barrier` came through input `index`, and hand back the
    /// barrier once every live input that carries barriers has delivered it.
    ///
    /// A barrier with a *later* epoch than the one being aligned abandons the
    /// earlier one and unblocks everything: an input that skipped an epoch is
    /// never going to deliver it, and staying blocked on it would deadlock.
    /// An earlier epoch than the one being aligned is stale and dropped.
    fn align(&mut self, index: usize, barrier: ack::Barrier) -> Option<ack::Barrier> {
        match self.aligning {
            Some(current) if barrier.epoch < current.epoch => return None,
            Some(current) if barrier.epoch == current.epoch => {
                self.aligning = Some(ack::Barrier {
                    epoch: current.epoch,
                    stop: current.stop || barrier.stop,
                });
            }
            _ => {
                self.blocked.iter_mut().for_each(|b| *b = false);
                self.aligning = Some(barrier);
            }
        }
        self.blocked[index] = true;
        self.aligned()
    }

    /// The barrier being aligned, if no live carrier is still owing it.
    fn aligned(&mut self) -> Option<ack::Barrier> {
        let owing = (0..self.rxs.len())
            .any(|i| self.carries[i] && self.rxs[i].is_some() && !self.blocked[i]);
        if owing {
            return None;
        }
        self.blocked.iter_mut().for_each(|b| *b = false);
        self.aligning.take()
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
            // cancel-safe: `poll_recv` loses nothing when the future is
            // dropped, and the alignment state lives on `self`
            let (index, res) = std::future::poll_fn(|cx| self.poll_any(cx)).await;
            let e = match res {
                Some(Ok(mut delivery)) => {
                    let Some(barrier) = delivery.barrier() else {
                        // which input a position came from matters only to
                        // a checkpoint, so only an ack that waits for one is
                        // told
                        if delivery.ack.defers_to_checkpoint() {
                            let inner = std::mem::replace(&mut delivery.ack, Box::new(ack::NoAck));
                            delivery.ack = Box::new(ack::FromInput {
                                input: index,
                                inner: Some(inner),
                            });
                        }
                        return Ok(delivery);
                    };
                    match self.align(index, barrier) {
                        Some(barrier) => return Ok(Delivery::of_barrier(barrier)),
                        None => continue,
                    }
                }
                Some(Err(e)) => e,
                None => anyhow::anyhow!("input {index} of this pipeline stopped"),
            };
            self.rxs[index] = None;

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
            // an input that has gone owes no barrier, so the one being
            // aligned may now be complete
            if self.aligning.is_some()
                && let Some(barrier) = self.aligned()
            {
                return Ok(Delivery::of_barrier(barrier));
            }
        }
    }

    fn carries_barriers(&self) -> bool {
        self.carries.iter().any(|c| *c)
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
    /// Whether the wrapped input carries barriers — asked once, before it
    /// moved into the pump task.
    carries: bool,
    // The batch being gathered. On `self` rather than in `next`'s locals so
    // that `next` is **cancel-safe**: the run loop drops this future whenever
    // something else wins its `select!` (a transform's wakeup, a checkpoint
    // trigger), and messages held in a dropped future's locals were simply
    // gone.
    out: MessageBatch,
    acks: Vec<Box<dyn Ack>>,
    /// Only exists once the batch has something in it, so it also *is* the
    /// "have we started" flag.
    window_closed: Option<std::pin::Pin<Box<tokio::time::Sleep>>>,
    /// What to hand on once the gathered batch has gone: a barrier that
    /// arrived while messages were held (it may not overtake them), or the
    /// pump's stop.
    pending: Option<anyhow::Result<Delivery>>,
}

impl Buffered {
    #[must_use]
    pub fn new(inner: Box<dyn InputSource>, kind: BufferKind) -> Self {
        let carries = inner.carries_barriers();
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
        Self {
            rx,
            kind,
            carries,
            out: Vec::new(),
            acks: Vec::new(),
            window_closed: None,
            pending: None,
        }
    }

    /// Hand on what has been gathered, and start again.
    fn take_batch(&mut self) -> Delivery {
        self.window_closed = None;
        Delivery::with_ack(
            Arc::new(std::mem::take(&mut self.out)),
            Box::new(ack::CombinedAck(std::mem::take(&mut self.acks))),
        )
    }

    /// Hand on what has been gathered and keep `next` for the call after —
    /// or hand `next` on now, when nothing is held.
    fn then(&mut self, next: anyhow::Result<Delivery>) -> anyhow::Result<Delivery> {
        if self.out.is_empty() {
            return next;
        }
        self.pending = Some(next);
        Ok(self.take_batch())
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
    ///
    /// A checkpoint barrier ends the batch early: what is held goes first and
    /// the barrier after, because the upstream counted those messages as
    /// delivered when it sent them and they must be inside the snapshot the
    /// barrier asks for.
    async fn next(&mut self) -> Result<Delivery> {
        if let Some(next) = self.pending.take() {
            return next;
        }
        let (size, window) = self.kind.limits();

        loop {
            if size.is_some_and(|size| self.out.len() >= size) {
                return Ok(self.take_batch());
            }

            let received = match &mut self.window_closed {
                // an unbiased `select!` on purpose: with no size limit and a
                // saturated input, preferring the receive would starve the timer
                Some(closed) => tokio::select! {
                    maybe = self.rx.recv() => maybe,
                    () = closed.as_mut() => return Ok(self.take_batch()),
                },
                None => self.rx.recv().await,
            };

            match received {
                Some(Ok(delivery)) if delivery.barrier().is_some() => {
                    return self.then(Ok(delivery));
                }
                // an empty batch upstream neither fills this one nor starts its
                // clock — it is not a message. Its ack fires now unless it is a
                // position a checkpoint has to see, which rides with the batch.
                Some(Ok(delivery)) if delivery.batch.is_empty() => {
                    if delivery.ack.defers_to_checkpoint() {
                        self.acks.push(delivery.ack);
                    } else {
                        delivery.ack.ack();
                    }
                }
                Some(Ok(delivery)) => {
                    self.out.extend(delivery.batch.iter().cloned());
                    self.acks.push(delivery.ack);
                    if self.window_closed.is_none() {
                        self.window_closed = window.map(|w| Box::pin(tokio::time::sleep(w)));
                    }
                }
                // reported at once rather than after what is held: the run
                // loop needs to hear the input is gone, and a partial batch
                // says nothing about it. Under checkpointing nothing is lost
                // by it — the held messages' positions were never recorded,
                // so a restart reads them again.
                Some(Err(e)) => {
                    self.out.clear();
                    self.acks.clear();
                    self.window_closed = None;
                    return Err(e);
                }
                // the pump stopped: hand on what we have, and say so on the
                // call after that. Reporting the stop with a batch in hand
                // would throw those messages away, and reporting it as an empty
                // batch would spin the run loop forever.
                None => {
                    return self.then(Err(anyhow::anyhow!("the buffered input has stopped")));
                }
            }
        }
    }

    fn carries_barriers(&self) -> bool {
        self.carries
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
