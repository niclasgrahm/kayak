//! Acknowledgement: the means by which an input tells its broker "you can
//! forget this message now."
//!
//! **Two modes exist today, and both are the same code path in the run
//! loop.** [`kayak_core::config::AckMode::OnReceipt`] and
//! [`kayak_core::config::AckMode::OnDelivery`] are not two branches in
//! `PipelineRuntime::run` — the run loop always calls [`Ack::ack`] once a
//! batch has cleared every one of this pipeline's outputs and every
//! downstream handoff, and never before. What differs between the two modes
//! is *what `next()` already did* before it returned: an `on_receipt` input
//! (or one with nothing to acknowledge at all — NATS core, `dummy`, `http`,
//! the `pipeline` input) has already told its broker it's done, so the ack
//! object it hands back is [`NoAck`] and the later call is a no-op; an
//! `on_delivery` input holds the acknowledgement open and the deferred call
//! is what actually fires it. This is what keeps the run loop from knowing or
//! caring which mode an input is in.
//!
//! **Scope, deliberately kept narrow for now.** "Delivered" means *this*
//! pipeline's own `outputs` and downstream `pipeline`-input handoffs — a
//! `tx.send` into a downstream inbox counts as delivered the moment it
//! succeeds, the same as an output's `emit` returning `Ok`. It does **not**
//! mean "and that downstream pipeline went on to deliver it too." Following
//! acknowledgement transitively through the graph would couple an input's
//! redelivery behaviour to the liveness of pipelines several hops away —
//! pipelines that can be edited, reverted or deleted independently — which is
//! exactly the kind of cross-pipeline coupling the state-bucket rule (see
//! `kayak_core::state`) already refuses to allow for the same reason. If that
//! is ever needed, it's a deliberate extension of this module, not an
//! accident of it.
//!
//! **A failing output does not withhold the acknowledgement.** `emit` errors
//! are already independent per output — one output failing doesn't stop its
//! siblings or the downstream fan-out — so "delivered" here means "this
//! pipeline finished handling the batch," not "every sink has it." A stronger
//! per-output guarantee (hold the ack until every output has *succeeded*, or
//! until at least one has) is real and was discussed, but is deliberately not
//! built yet: it needs a second `AckMode` and a decision about what a
//! never-ending failure on one output does to redelivery on the others. Revisit
//! there if a durability guarantee stronger than "this pipeline attempted
//! every send" turns out to matter.
//!
//! **Under checkpointing** (`--state-dir`, see `docs/durability.md`) an ack
//! may also stand for a **position** — how far its input had got — and say so
//! with [`Ack::defers_to_checkpoint`]. The run loop then holds it, coalesced
//! per input, and the checkpoint coordinator fires it once a checkpoint that
//! covers it is durable. That is what keeps a broker from ever being ahead of
//! the state that a restart would restore. An ack that does not defer (mqtt's,
//! [`NoAck`]) still fires at the end of its pass.
//!
//! **Batching and cardinality.** One [`Delivery`] can hold several original
//! messages (`max_batch`, `buffer`) and can turn into several outgoing
//! batches (`splitter`), but there is exactly one `Ack` per `Delivery` and it
//! fires once, after *all* of the pass's outgoing batches have been sent to
//! every output and every downstream sender. There is no per-message
//! acknowledgement inside a batch — the same granularity `max_batch` and
//! `buffer` already impose on everything else an input does.

use std::any::Any;
use std::sync::Arc;

use kayak_core::config::AckMode;
use serde_json::Value;

use super::MessageBatch;

/// The means by which an input tells its broker a message is done with — a
/// kafka offset store, and eventually an MQTT PUBACK or an AMQP `basic.ack`.
///
/// The run loop calls `ack()` exactly once per [`Delivery`], after the batch
/// has cleared this pipeline (see the module docs for exactly what that
/// means). Implementations that have nothing to do — because they already did
/// it inside `next()`, or because there is nothing to do at all — use
/// [`NoAck`].
pub trait Ack: Send + Sync + Any {
    fn ack(&self);

    /// Whether this acknowledgement stands for a **position** that a
    /// checkpoint has to cover before it may fire — see `docs/durability.md`.
    ///
    /// Only consulted when checkpointing is on. An ack that says no (the
    /// default, and what [`NoAck`] and the mqtt ack say) fires at the end of
    /// its pass exactly as it would without checkpoints. One that says yes is
    /// held by the run loop and fired by the coordinator once a checkpoint
    /// that includes it is durable.
    fn defers_to_checkpoint(&self) -> bool {
        false
    }

    /// How far the input had got once this delivery was read, as a JSON
    /// object, for the checkpoint. Merged key by key into what the input had
    /// recorded before — a kafka ack names only the partitions it read, and
    /// the others keep their offsets.
    fn position(&self) -> Option<Value> {
        None
    }

    /// Fold a later acknowledgement *of the same input* into this one, and say
    /// whether that worked. This is what keeps the acks held between two
    /// checkpoints per input rather than per message: a kafka ack keeps the
    /// highest offset per partition, a poller's keeps the latest watermark.
    /// `false` means the caller keeps `later` as a separate entry.
    fn absorb(&mut self, _later: &dyn Ack) -> bool {
        false
    }

    /// The acknowledgements this one was folded from, taken out of it — only
    /// [`CombinedAck`] has any. Flattening is what lets the pieces of a
    /// buffered batch coalesce with everything else from their input.
    fn take_parts(&mut self) -> Option<Vec<Box<dyn Ack>>> {
        None
    }

    /// The checkpoint barrier this delivery *is*, rather than messages — see
    /// [`Delivery::barrier`]. Only [`BarrierAck`] says anything.
    fn barrier(&self) -> Option<Barrier> {
        None
    }

    /// Which of the pipeline's inputs this came from, and the ack itself —
    /// taken out of the wrapper [`super::Merged`] puts round a deferring ack.
    /// Only [`FromInput`] has one.
    fn origin(&mut self) -> Option<(usize, Box<dyn Ack>)> {
        None
    }
}

/// Nothing to acknowledge.
///
/// Every input reaches for this unless it is honouring
/// `AckMode::OnDelivery`: an `on_receipt` input (the default, and the only
/// mode most inputs support) has already done whatever it does to acknowledge
/// receipt by the time `next()` returns, so the run loop's later call costs
/// nothing and changes nothing observable.
pub struct NoAck;

impl Ack for NoAck {
    fn ack(&self) {}
}

/// Several acknowledgements travelling as one.
///
/// [`super::Buffered`] folds however many deliveries the wrapped input
/// produced into a single outgoing batch — the run loop only ever sees one
/// [`Delivery`] — so acknowledging that outer batch has to acknowledge every
/// inner one it was built from.
pub struct CombinedAck(pub Vec<Box<dyn Ack>>);

impl Ack for CombinedAck {
    fn ack(&self) {
        for ack in &self.0 {
            ack.ack();
        }
    }

    fn defers_to_checkpoint(&self) -> bool {
        self.0.iter().any(|ack| ack.defers_to_checkpoint())
    }

    fn take_parts(&mut self) -> Option<Vec<Box<dyn Ack>>> {
        Some(std::mem::take(&mut self.0))
    }
}

/// A position with nothing to tell a broker: what an input whose position
/// lives only in kayak's checkpoint (a poller's watermark, the http log's
/// offset) hands back. Firing it does nothing; what matters is that it
/// [defers](Ack::defers_to_checkpoint) and carries the position.
pub struct PositionAck(pub Value);

impl Ack for PositionAck {
    fn ack(&self) {}

    fn defers_to_checkpoint(&self) -> bool {
        true
    }

    fn position(&self) -> Option<Value> {
        Some(self.0.clone())
    }

    fn absorb(&mut self, later: &dyn Ack) -> bool {
        let Some(later) = (later as &dyn Any).downcast_ref::<PositionAck>() else {
            return false;
        };
        merge_position(&mut self.0, &later.0);
        true
    }
}

/// Merge `later` into `position` key by key, the rule [`Ack::position`]
/// documents. A position that isn't an object is replaced whole.
pub fn merge_position(position: &mut Value, later: &Value) {
    match (position, later) {
        (Value::Object(into), Value::Object(from)) => {
            for (key, value) in from {
                into.insert(key.clone(), value.clone());
            }
        }
        (position, later) => *position = later.clone(),
    }
}

/// A barrier travelling through the input layers, as the ack of an empty
/// batch.
///
/// Riding on the ack rather than as a field of [`Delivery`] is a measured
/// decision: a `Delivery` is moved through several futures on every pass, and
/// widening it from 24 bytes to 48 cost a tenth of a busy pipeline's
/// throughput — paid by every pipeline, checkpointed or not.
pub struct BarrierAck(pub Barrier);

impl Ack for BarrierAck {
    fn ack(&self) {}

    fn barrier(&self) -> Option<Barrier> {
        Some(self.0)
    }
}

/// An ack, and which of a pipeline's inputs it came from. [`super::Merged`]
/// wraps only the acks that defer to a checkpoint — the ones whose position
/// has to be kept apart per input — so a pipeline without checkpoints never
/// pays for it. A field on [`Delivery`] would have been paid by all of them;
/// see [`BarrierAck`].
pub struct FromInput {
    pub input: usize,
    pub inner: Option<Box<dyn Ack>>,
}

impl Ack for FromInput {
    fn ack(&self) {
        if let Some(inner) = &self.inner {
            inner.ack();
        }
    }

    fn defers_to_checkpoint(&self) -> bool {
        self.inner.as_ref().is_some_and(|inner| inner.defers_to_checkpoint())
    }

    fn position(&self) -> Option<Value> {
        self.inner.as_ref().and_then(|inner| inner.position())
    }

    fn origin(&mut self) -> Option<(usize, Box<dyn Ack>)> {
        self.inner.take().map(|inner| (self.input, inner))
    }
}

/// What a barrier tells the run loop that reads it: checkpoint `epoch`, and —
/// on a shutdown or a revert — stop once it is done. See
/// `docs/durability.md`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Barrier {
    pub epoch: u64,
    pub stop: bool,
}

/// One batch, and the means to tell the run loop is done with it.
///
/// Kept to these two fields on purpose — see [`BarrierAck`] for what a third
/// one cost.
pub struct Delivery {
    pub batch: Arc<MessageBatch>,
    pub ack: Box<dyn Ack>,
}

impl Delivery {
    /// A delivery with nothing to acknowledge — the common case, and what
    /// every input reaches for except one honouring `AckMode::OnDelivery`.
    #[must_use]
    pub fn new(batch: Arc<MessageBatch>) -> Self {
        Self::with_ack(batch, Box::new(NoAck))
    }

    /// A delivery whose acknowledgement does something real.
    #[must_use]
    pub fn with_ack(batch: Arc<MessageBatch>, ack: Box<dyn Ack>) -> Self {
        Self { batch, ack }
    }

    /// A barrier, travelling through the input layers to the run loop. Only a
    /// `pipeline` input produces these, and the layers above it pass them on;
    /// such a delivery has no messages and is never a pass.
    #[must_use]
    pub fn of_barrier(barrier: Barrier) -> Self {
        Self::with_ack(Arc::new(Vec::new()), Box::new(BarrierAck(barrier)))
    }

    /// The barrier this delivery is, if it is one. Asks the ack only when the
    /// batch is empty, so a delivery of messages costs a length check.
    #[must_use]
    pub fn barrier(&self) -> Option<Barrier> {
        if self.batch.is_empty() {
            self.ack.barrier()
        } else {
            None
        }
    }
}

impl std::ops::Deref for Delivery {
    type Target = MessageBatch;

    fn deref(&self) -> &MessageBatch {
        &self.batch
    }
}

/// Refuses a build when the config asked for [`AckMode::OnDelivery`] on an
/// input with no broker-side notion of "received" vs "delivered" — refused
/// rather than silently treated as `on_receipt`, the same rule the `http`
/// input's header allow-list and the column mapping's build-time checks
/// follow: a promise a component cannot keep should fail loudly, not quietly
/// not happen.
pub fn require_receipt_only(mode: AckMode, kind: &str) -> anyhow::Result<()> {
    match mode {
        AckMode::OnReceipt => Ok(()),
        AckMode::OnDelivery => Err(anyhow::anyhow!(
            "the {kind} input has no broker-side notion of \"received\" vs \"delivered\", so \
             `ack: on_delivery` cannot be honoured — leave `ack` unset, or set it to \
             `on_receipt`"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Counting(Arc<AtomicUsize>);
    impl Ack for Counting {
        fn ack(&self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// The whole point of `CombinedAck`: acknowledging the outer batch
    /// acknowledges every inner delivery it was folded from, not just one.
    #[test]
    fn a_combined_ack_fires_every_inner_ack() {
        let counter = Arc::new(AtomicUsize::new(0));
        let combined = CombinedAck(vec![
            Box::new(Counting(counter.clone())),
            Box::new(Counting(counter.clone())),
            Box::new(Counting(counter.clone())),
        ]);
        combined.ack();
        assert_eq!(counter.load(Ordering::SeqCst), 3);
    }

    /// `on_receipt` is always fine; `on_delivery` is refused unless the caller
    /// says otherwise. This is the check every input with no broker-side ack
    /// runs at build time.
    #[test]
    fn only_on_receipt_passes_the_receipt_only_guard() {
        assert!(require_receipt_only(AckMode::OnReceipt, "nats").is_ok());
        let Err(err) = require_receipt_only(AckMode::OnDelivery, "nats") else {
            panic!("on_delivery was accepted by an input with nothing to acknowledge");
        };
        assert!(format!("{err:#}").contains("nats"));
    }

    /// A plain `Delivery` derefs to its batch, so the common case — every
    /// caller that only wants the messages — needs no `.batch` at all.
    #[test]
    fn a_delivery_derefs_to_its_batch() {
        let batch: Arc<MessageBatch> = Arc::new(vec![Arc::new(serde_json::json!({"n": 1}))]);
        let delivery = Delivery::new(batch);
        assert_eq!(delivery.len(), 1);
        assert_eq!(delivery[0]["n"], serde_json::json!(1));
    }
}
