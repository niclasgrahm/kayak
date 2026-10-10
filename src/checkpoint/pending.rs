//! The acknowledgements a run loop holds between two checkpoints.
//!
//! Kept per input and coalesced as they arrive, which is the whole reason this
//! is a type rather than a `Vec`: a kafka pipeline doing a million messages a
//! second between two checkpoints ten seconds apart would otherwise hold ten
//! million boxed acks. Coalesced, it holds one per input — each keeping the
//! highest offset per partition, or the latest watermark — and the memory is
//! flat in throughput.

use crate::inputs::Ack;
use crate::inputs::ack::merge_position;
use serde_json::Value;

/// Acks waiting for a checkpoint, tagged with the input they came from.
#[derive(Default)]
pub struct PendingAcks {
    entries: Vec<(usize, Box<dyn Ack>)>,
}

impl PendingAcks {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Hold `ack` for the next checkpoint — or fire it now, when it is not
    /// one that waits for a checkpoint at all (see
    /// [`Ack::defers_to_checkpoint`]). A [`crate::inputs::ack::CombinedAck`]
    /// is taken apart first so its pieces coalesce with everything else from
    /// their input.
    pub fn push(&mut self, input: usize, mut ack: Box<dyn Ack>) {
        if let Some((origin, inner)) = ack.origin() {
            self.push(origin, inner);
            return;
        }
        if let Some(parts) = ack.take_parts() {
            for part in parts {
                self.push(input, part);
            }
            return;
        }
        if !ack.defers_to_checkpoint() {
            ack.ack();
            return;
        }
        for (held_input, held) in &mut self.entries {
            if *held_input == input && held.absorb(ack.as_ref()) {
                return;
            }
        }
        self.entries.push((input, ack));
    }

    /// Fold every held position into `positions` (one slot per input),
    /// key by key — what the run loop records in its snapshot.
    pub fn record_positions(&self, positions: &mut Vec<Option<Value>>) {
        for (input, ack) in &self.entries {
            let Some(position) = ack.position() else {
                continue;
            };
            if positions.len() <= *input {
                positions.resize(*input + 1, None);
            }
            match &mut positions[*input] {
                Some(held) => merge_position(held, &position),
                slot @ None => *slot = Some(position),
            }
        }
    }

    /// Take everything held, leaving this empty.
    #[must_use]
    pub fn take(&mut self) -> PendingAcks {
        std::mem::take(self)
    }

    /// Move everything from `other` into this, coalescing as [`push`] does.
    ///
    /// [`push`]: PendingAcks::push
    pub fn extend(&mut self, other: PendingAcks) {
        for (input, ack) in other.entries {
            self.push(input, ack);
        }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Fire every held ack. Only the coordinator calls this, once the
    /// checkpoint covering them is durable.
    pub fn fire(self) {
        for (_, ack) in self.entries {
            ack.ack();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inputs::ack::{CombinedAck, NoAck, PositionAck};
    use serde_json::json;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct Immediate(Arc<AtomicUsize>);
    impl Ack for Immediate {
        fn ack(&self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    /// An ack that does not wait for a checkpoint — mqtt's, `NoAck` — fires as
    /// it arrives, exactly as it would without checkpoints.
    #[test]
    fn an_ack_that_does_not_defer_fires_at_once() {
        let fired = Arc::new(AtomicUsize::new(0));
        let mut pending = PendingAcks::new();
        pending.push(0, Box::new(Immediate(Arc::clone(&fired))));
        pending.push(0, Box::new(NoAck));
        assert_eq!(fired.load(Ordering::SeqCst), 1);
        assert!(pending.is_empty());
    }

    /// The memory bound: a stream of positions from one input is one entry,
    /// holding the latest, however many arrive.
    #[test]
    fn positions_from_one_input_coalesce_into_one_entry() {
        let mut pending = PendingAcks::new();
        for n in 0..1000 {
            pending.push(0, Box::new(PositionAck(json!({"watermark": n}))));
        }
        assert_eq!(pending.len(), 1);
        let mut positions = Vec::new();
        pending.record_positions(&mut positions);
        assert_eq!(positions, vec![Some(json!({"watermark": 999}))]);
    }

    /// Two inputs keep two positions — coalescing across inputs would hand one
    /// input's offset to another.
    #[test]
    fn positions_from_different_inputs_are_kept_apart() {
        let mut pending = PendingAcks::new();
        pending.push(0, Box::new(PositionAck(json!({"a": 1}))));
        pending.push(1, Box::new(PositionAck(json!({"b": 2}))));
        pending.push(0, Box::new(PositionAck(json!({"a": 3}))));
        assert_eq!(pending.len(), 2);
        let mut positions = Vec::new();
        pending.record_positions(&mut positions);
        assert_eq!(
            positions,
            vec![Some(json!({"a": 3})), Some(json!({"b": 2}))]
        );
    }

    /// A recorded position is *merged* into what was there: a kafka ack names
    /// only the partitions it read, and the rest keep their offsets.
    #[test]
    fn a_position_is_merged_key_by_key_into_the_recorded_one() {
        let mut positions = vec![Some(json!({"0": 10, "1": 20}))];
        let mut pending = PendingAcks::new();
        pending.push(0, Box::new(PositionAck(json!({"1": 25}))));
        pending.record_positions(&mut positions);
        assert_eq!(positions, vec![Some(json!({"0": 10, "1": 25}))]);
    }

    /// A buffered batch's combined ack is taken apart, so its pieces coalesce
    /// with the rest of their input's.
    #[test]
    fn a_combined_ack_is_flattened_before_it_is_held() {
        let mut pending = PendingAcks::new();
        pending.push(
            0,
            Box::new(CombinedAck(vec![
                Box::new(PositionAck(json!({"w": 1}))),
                Box::new(PositionAck(json!({"w": 2}))),
                Box::new(NoAck),
            ])),
        );
        assert_eq!(pending.len(), 1);
        let mut positions = Vec::new();
        pending.record_positions(&mut positions);
        assert_eq!(positions, vec![Some(json!({"w": 2}))]);
    }
}
