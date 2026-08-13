//! Publishing to the UI feed.
//!
//! Two functions, and the clock is the reason: this is where the server reads
//! it. `UiEvent` compiles for wasm — where `SystemTime::now` panics — so the type
//! can't stamp itself, and a second publisher that forgot to would put an event
//! on the feed with no time on it.

use kayak_core::UiEvent;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::broadcast;

/// How many browsers are attached to `/events`, as a number that is cheap to
/// read.
///
/// This exists for one line in the run loop. Every pass asks "is anyone
/// watching" before it does any reporting work — the gate that makes a
/// headless server pay nothing for a UI nobody has opened — and the obvious
/// way to ask, `broadcast::Sender::receiver_count()`, **takes a mutex**
/// (`shared.tail.lock()`, in tokio's implementation). Every pipeline in the
/// process shares one channel, so that turned a per-pass question into a
/// process-wide serialization point: measured, the whole server flatlined at
/// about 6.5M passes a second whether it was running ten pipelines or a
/// thousand, and giving each one a private channel as an experiment was **eight
/// times faster**. See `docs/guide.md`'s "benchmarking" section; the finding
/// came out of the first run of `just bench`.
///
/// So the count is kept here instead, and reading it is a relaxed load.
/// [`Watchers::attach`] is the only way to raise it and the guard it returns is
/// the only way to lower it, which is what keeps it honest — a subscriber that
/// forgot to decrement would hold the gate open for the life of the process.
///
/// `Relaxed` is deliberate and is the same argument [`crate::history::Counters`]
/// makes: a run loop that reads this one pass either side of a browser
/// attaching reports one pass more or less, which is not a question anyone can
/// ask of a sampled feed.
#[derive(Clone, Debug)]
pub struct Watchers(Arc<AtomicUsize>);

impl Watchers {
    /// A feed nobody is attached to.
    #[must_use]
    pub fn empty() -> Self {
        Self(Arc::new(AtomicUsize::new(0)))
    }

    /// A feed that always reports someone attached.
    ///
    /// The default wherever one isn't threaded through — see
    /// [`crate::pipeline::PipelineRuntime::from_parts`] and [`crate::BuildCtx`].
    /// Attached rather than empty because of which mistake it makes: a
    /// component that never got the real count and assumes *nobody* is watching
    /// goes quietly dark and the UI shows nothing, while one that assumes
    /// somebody is pays for reporting it didn't need. The second is a
    /// performance bug, the first is a correctness one.
    #[must_use]
    pub fn attached() -> Self {
        Self(Arc::new(AtomicUsize::new(1)))
    }

    /// Whether anyone is attached. One relaxed load — this is the hot path.
    #[must_use]
    pub fn any(&self) -> bool {
        self.0.load(Ordering::Relaxed) > 0
    }

    /// Count one more watcher, until the returned guard is dropped.
    #[must_use]
    pub fn attach(&self) -> WatchGuard {
        self.0.fetch_add(1, Ordering::AcqRel);
        WatchGuard(Arc::clone(&self.0))
    }

    /// How many are attached. For tests and for saying so in a log; the run
    /// loop wants [`Watchers::any`].
    #[must_use]
    pub fn count(&self) -> usize {
        self.0.load(Ordering::Relaxed)
    }
}

/// One attached watcher. Dropping it is what says the browser went away, so it
/// has to live exactly as long as the receiver it was taken out beside — see
/// [`crate::state::AppState::subscribe_events`].
#[derive(Debug)]
pub struct WatchGuard(Arc<AtomicUsize>);

impl Drop for WatchGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::AcqRel);
    }
}

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
///
/// Public because [`crate::history`] stamps its error signatures with the same
/// clock, and two spellings of "now" in one server is how a UI ends up
/// disagreeing with itself about when something happened.
#[must_use]
pub fn now_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

#[cfg(test)]
mod tests {
    use super::{Watchers, publish};
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

    #[test]
    fn an_empty_feed_has_nobody_watching_and_an_attached_one_does() {
        assert!(!Watchers::empty().any());
        assert!(Watchers::attached().any());
    }

    /// The guard is the whole mechanism: if dropping it didn't decrement, a
    /// browser that visited once would hold the gate open for the life of the
    /// process and every pipeline would pay the reporting cost forever.
    #[test]
    fn a_watcher_is_counted_until_its_guard_is_dropped() {
        let watchers = Watchers::empty();
        assert!(!watchers.any());

        let first = watchers.attach();
        assert!(watchers.any());
        assert_eq!(watchers.count(), 1);

        let second = watchers.attach();
        assert_eq!(watchers.count(), 2);

        drop(first);
        assert!(watchers.any(), "one watcher left should still hold the gate open");
        assert_eq!(watchers.count(), 1);

        drop(second);
        assert!(!watchers.any());
        assert_eq!(watchers.count(), 0);
    }

    /// A clone is the same counter, not a copy of it — the run loops hold
    /// clones and the SSE handler attaches to another, so a count that didn't
    /// travel would leave every pipeline reading its own private zero.
    #[test]
    fn a_clone_shares_the_count() {
        let watchers = Watchers::empty();
        let elsewhere = watchers.clone();
        let _guard = elsewhere.attach();
        assert!(watchers.any(), "the count did not reach the clone");
    }
}
