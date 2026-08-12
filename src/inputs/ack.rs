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
//! **Batching and cardinality.** One [`Delivery`] can hold several original
//! messages (`max_batch`, `buffer`) and can turn into several outgoing
//! batches (`splitter`), but there is exactly one `Ack` per `Delivery` and it
//! fires once, after *all* of the pass's outgoing batches have been sent to
//! every output and every downstream sender. There is no per-message
//! acknowledgement inside a batch — the same granularity `max_batch` and
//! `buffer` already impose on everything else an input does.

use std::sync::Arc;

use kayak_core::config::AckMode;

use super::MessageBatch;

/// The means by which an input tells its broker a message is done with — a
/// kafka offset store, and eventually an MQTT PUBACK or an AMQP `basic.ack`.
///
/// The run loop calls `ack()` exactly once per [`Delivery`], after the batch
/// has cleared this pipeline (see the module docs for exactly what that
/// means). Implementations that have nothing to do — because they already did
/// it inside `next()`, or because there is nothing to do at all — use
/// [`NoAck`].
pub trait Ack: Send + Sync {
    fn ack(&self);
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
}

/// One batch, and the means to tell the run loop is done with it.
pub struct Delivery {
    pub batch: Arc<MessageBatch>,
    pub ack: Box<dyn Ack>,
}

impl Delivery {
    /// A delivery with nothing to acknowledge — the common case, and what
    /// every input reaches for except one honouring `AckMode::OnDelivery`.
    #[must_use]
    pub fn new(batch: Arc<MessageBatch>) -> Self {
        Self {
            batch,
            ack: Box::new(NoAck),
        }
    }

    /// A delivery whose acknowledgement does something real.
    #[must_use]
    pub fn with_ack(batch: Arc<MessageBatch>, ack: Box<dyn Ack>) -> Self {
        Self { batch, ack }
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
