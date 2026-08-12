//! One reconnect policy, shared by every input and output that talks to
//! something outside the process.
//!
//! `docker compose down` under a running server is the case this exists for:
//! before it, a dropped connection meant either an input dying for good (no
//! retry at all) or an output hammering the dead broker with a fresh
//! reconnect attempt on every single batch — at whatever rate the pipeline
//! was producing them, unthrottled, because nothing here knew the last
//! attempt had just failed. [`Backoff`] is the one thing both sides now
//! consult before trying again.
//!
//! Deliberately just a policy — it has no clock of its own and does not
//! sleep. [`Backoff::failed`] hands back a [`Duration`] and the caller
//! decides what to do with it: an input's reconnect loop awaits it, an
//! output's `emit` compares it against a deadline and fails fast without
//! touching the network at all until that deadline passes. Keeping the sleep
//! out of this type is what makes it usable from both.

use std::time::{Duration, Instant};

use rand::RngExt;

/// Wait before the first retry.
const INITIAL: Duration = Duration::from_millis(250);
/// Never wait longer than this between attempts, however long the outage.
const MAX: Duration = Duration::from_secs(30);
/// `INITIAL * 2^CAP_AT >= MAX`, so the shift below never has to run far
/// enough to overflow.
const CAP_AT: u32 = 7;

/// Tracks consecutive failures for one connection and says how long to wait
/// before trying again. Doubles each time, capped at [`MAX`], with jitter so
/// several pipelines reconnecting to the same broker don't retry in
/// lockstep.
#[derive(Debug, Clone, Default)]
pub struct Backoff {
    attempt: u32,
}

impl Backoff {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Call after a failed attempt. Returns how long to wait before the next
    /// one.
    pub fn failed(&mut self) -> Duration {
        let exp = self.attempt.min(CAP_AT);
        self.attempt = self.attempt.saturating_add(1);
        let base = INITIAL.saturating_mul(1 << exp).min(MAX);
        jitter(base)
    }

    /// Call after a successful attempt, so the next failure starts the
    /// schedule over rather than picking up where a long-past outage left
    /// off.
    pub fn succeeded(&mut self) {
        self.attempt = 0;
    }

    /// Whether anything has failed since the last success (or since this was
    /// built) — the edge a caller reports "down"/"back up" on, rather than
    /// on every attempt in between.
    #[must_use]
    pub fn is_failing(&self) -> bool {
        self.attempt > 0
    }

    /// How many consecutive failures have been recorded. Mostly for tests: a
    /// caller retrying against a downed dependency for some bounded stretch
    /// of time should have a small count here, not one per pass of a run
    /// loop that never paced itself.
    #[must_use]
    pub fn attempts(&self) -> u32 {
        self.attempt
    }
}

/// ±20% of `base`, so a fleet of pipelines that all lost the same broker at
/// once don't all retry on the same tick forever.
fn jitter(base: Duration) -> Duration {
    let factor = rand::rng().random_range(0.8..1.2);
    base.mul_f64(factor)
}

/// [`Backoff`] plus a deadline, for a caller that wants to *skip* an attempt
/// entirely rather than await one — every output in this codebase. An input
/// owns its `next()` call and can afford to sit inside it until a broker
/// comes back; an output's `emit` is on the pipeline's hot path and must not
/// block it, so instead of waiting out the delay it fails the batch
/// immediately, without touching the network at all, until the deadline has
/// passed. That's what turns "a downed broker gets hammered on every batch"
/// into "a downed broker gets one attempt every few seconds" without an
/// output ever sleeping.
///
/// Takes `now` as a parameter rather than reading the clock itself, which is
/// what makes it testable without `tokio::time` — see the tests below.
#[derive(Debug, Clone, Default)]
pub struct Gate {
    backoff: Backoff,
    /// `None` until the first failure, so the very first attempt is never
    /// gated — only a *retry* is paced, not the initial connect.
    ready_at: Option<Instant>,
}

impl Gate {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether an attempt should be made right now.
    #[must_use]
    pub fn ready(&self, now: Instant) -> bool {
        self.ready_at.is_none_or(|deadline| now >= deadline)
    }

    /// Call after a failed attempt: pushes the deadline out by the next
    /// backoff delay.
    pub fn record_failure(&mut self, now: Instant) {
        self.ready_at = Some(now + self.backoff.failed());
    }

    /// Call after a successful attempt: clears the deadline and resets the
    /// schedule, so the next outage starts over rather than continuing a
    /// long-past one's climb.
    pub fn record_success(&mut self) {
        self.backoff.succeeded();
        self.ready_at = None;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn first_failure_waits_close_to_the_initial_delay() {
        let mut backoff = Backoff::new();
        let delay = backoff.failed();
        assert!(
            delay >= INITIAL.mul_f64(0.8) && delay <= INITIAL.mul_f64(1.2),
            "{delay:?}"
        );
    }

    #[test]
    fn delay_grows_with_each_consecutive_failure() {
        let mut backoff = Backoff::new();
        let first = backoff.failed();
        let second = backoff.failed();
        let third = backoff.failed();
        // jittered, so compare against the unjittered floor of the next
        // step rather than the previous sample directly
        assert!(second >= first, "{first:?} {second:?}");
        assert!(third >= second, "{second:?} {third:?}");
    }

    #[test]
    fn delay_never_exceeds_the_cap_however_long_the_outage() {
        let mut backoff = Backoff::new();
        for _ in 0..100 {
            let delay = backoff.failed();
            assert!(delay <= MAX.mul_f64(1.2), "{delay:?} exceeded the cap");
        }
    }

    #[test]
    fn success_resets_the_schedule() {
        let mut backoff = Backoff::new();
        for _ in 0..10 {
            backoff.failed();
        }
        backoff.succeeded();
        let delay = backoff.failed();
        assert!(
            delay <= INITIAL.mul_f64(1.2),
            "a failure right after a success should start over, got {delay:?}"
        );
    }

    #[test]
    fn is_failing_tracks_the_outage_edge() {
        let mut backoff = Backoff::new();
        assert!(!backoff.is_failing());
        backoff.failed();
        assert!(backoff.is_failing());
        backoff.succeeded();
        assert!(!backoff.is_failing());
    }

    #[test]
    fn a_fresh_gate_is_ready_for_the_first_attempt() {
        let gate = Gate::new();
        assert!(gate.ready(Instant::now()));
    }

    #[test]
    fn a_failure_closes_the_gate_until_its_deadline() {
        let mut gate = Gate::new();
        let now = Instant::now();
        gate.record_failure(now);
        assert!(
            !gate.ready(now),
            "a batch right after the failure should not trigger a reconnect attempt"
        );
        assert!(
            !gate.ready(now + INITIAL.mul_f64(0.5)),
            "the gate should still be closed at half the delay"
        );
        assert!(
            gate.ready(now + INITIAL.mul_f64(1.3)),
            "the gate should have reopened comfortably past the (jittered) delay"
        );
    }

    #[test]
    fn success_reopens_the_gate_and_resets_the_schedule() {
        let mut gate = Gate::new();
        let now = Instant::now();
        for _ in 0..5 {
            gate.record_failure(now);
        }
        gate.record_success();
        assert!(gate.ready(now), "a success should reopen the gate at once");

        // and the next failure starts the schedule over — a short, first-step
        // delay — rather than continuing from attempt 5's much longer one
        gate.record_failure(now);
        assert!(
            !gate.ready(now),
            "the gate should be closed again right after the new failure"
        );
        assert!(
            gate.ready(now + INITIAL.mul_f64(1.3)),
            "a failure right after success should back off from attempt 0, not attempt 5"
        );
    }
}
