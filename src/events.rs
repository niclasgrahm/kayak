//! Publishing to the UI feed.
//!
//! One function, because it is the one place in the server that reads a clock.
//! `UiEvent` compiles for wasm — where `SystemTime::now` panics — so the type
//! can't stamp itself, and a second publisher that forgot to would put an event
//! on the feed with no time on it.

use kayak_core::UiEvent;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast;

/// Publish to the UI feed. Nothing is built or sent when no one is watching —
/// errors reach the server log either way, and this keeps a headless run from
/// paying to describe them. The event is built by a closure for that reason,
/// and taking one rather than a value also lets this be called from inside a
/// loop that already holds a `&mut` borrow of what it is describing.
///
/// This feed is what the cards' logs are built from: a batch event per pass
/// through a run loop, plus whatever failed on the way.
pub fn publish(events: &broadcast::Sender<UiEvent>, event: impl FnOnce() -> UiEvent) {
    if events.receiver_count() > 0 {
        let _ = events.send(event().at(now_millis()));
    }
}

/// Milliseconds since the epoch, or zero if the clock is before it — which the
/// log reads as "no time" rather than as a date in 1970.
fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests {
    use super::publish;
    use kayak_core::{Stage, UiEvent};
    use tokio::sync::broadcast;

    #[test]
    fn a_published_event_is_stamped_with_the_time_it_was_sent() {
        let (tx, mut rx) = broadcast::channel::<UiEvent>(4);

        publish(&tx, || {
            UiEvent::error("witty-crab".to_string(), Stage::Input, &"upstream gone")
        });

        let Ok(event) = rx.try_recv() else {
            panic!("an event should have been published");
        };
        assert!(
            event.ts > 1_700_000_000_000,
            "expected a wall-clock stamp, got {}",
            event.ts
        );
    }

    /// The gate is what keeps a headless run from serializing batches nobody
    /// reads, so it is worth a test of its own: the closure must not even run.
    #[test]
    fn nothing_is_built_when_no_one_is_watching() {
        let (tx, rx) = broadcast::channel::<UiEvent>(4);
        drop(rx);

        publish(&tx, || {
            panic!("the event should not have been built with no receivers");
        });
    }
}
