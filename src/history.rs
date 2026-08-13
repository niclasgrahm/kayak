//! What a pipeline did, kept after the fact — the live store.
//!
//! The declaration is in [`kayak_core::history`]; this is the thing that holds
//! the numbers. Read that module first: it says why history exists beside
//! `/events` rather than on top of it, and why there are two resolutions.
//!
//! Four properties here are load-bearing.
//!
//! - **Nothing on the hot path takes a lock.** A run loop's contribution to
//!   history is `fetch_add` on three [`Counters`], relaxed, unconditional — a
//!   handful of nanoseconds next to the JSON the same pass is already
//!   serializing, and crucially *not* gated on anyone watching. [`sample`] does
//!   the rest on a tick, so the cost of history is O(pipelines) per five
//!   seconds rather than O(messages).
//! - **Everything is bounded, and the error map is the one that needs saying.**
//!   Buckets are ring buffers so memory is flat in uptime and in throughput.
//!   Error *signatures* look self-limiting and are not: an error text carrying a
//!   message id makes every failure distinct, so the map has a cap and an
//!   eviction rule like any other keyed store here.
//! - **Records outlive their pipelines.** A revert rebuilds every pipeline in
//!   the graph, and dropping history at that point would mean an edit to one
//!   pipeline costs the overnight record of all the others — the same argument
//!   [`crate::buckets::Buckets::rebuilt`] makes about state. Records are pruned
//!   by [`sample`] once they are both dead and older than the retention.
//! - **Buckets are stored dense.** A gap and a run of zeroes mean different
//!   things — "the server wasn't asked" against "the pipeline stopped" — and
//!   the second one is the whole point of the feature, so it is written down
//!   rather than inferred from a missing key at render time.
//!
//! Locking follows the house rule: one `std::sync::Mutex` over the map, never
//! held across an `.await`.

use std::collections::{HashMap, VecDeque};
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use kayak_core::history::{
    ErrorSignature, HistoryBucket, MAX_ERROR_SIGNATURES, PipelineHistory, Resolution,
};
use kayak_core::server_config::HistoryConfig;
use kayak_core::{PipelineId, Stage, truncate};

/// What one run loop has done since it started, as three counters it adds to
/// without asking anyone's permission.
///
/// Monotonic for the life of the pipeline; [`sample`] differences them, which
/// is what turns "since startup" into "during this bucket" without the run loop
/// having to know what a bucket is. `Relaxed` throughout: these are statistics,
/// nothing is ordered against them, and the sampler reading a count one pass
/// stale simply moves that message into the next bucket.
#[derive(Debug, Default)]
pub struct Counters {
    inbound: AtomicU64,
    outbound: AtomicU64,
    errors: AtomicU64,
}

impl Counters {
    /// Count messages that arrived at the inputs.
    pub fn add_inbound(&self, messages: usize) {
        self.inbound.fetch_add(messages as u64, Ordering::Relaxed);
    }

    /// Count messages that came out of the transform chain. Called once per
    /// batch rather than once per output — see [`HistoryBucket::outbound`].
    pub fn add_outbound(&self, messages: usize) {
        self.outbound.fetch_add(messages as u64, Ordering::Relaxed);
    }

    /// Count a failure. Called on **every** failure, including the ones the UI
    /// throttle suppresses, which is what makes the bucket's error count the
    /// true one.
    pub fn add_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Messages that have arrived at the inputs since this pipeline started.
    ///
    /// Public because the throughput harness reads it directly: the counters
    /// are unconditional and monotonic, so "run for ten seconds and difference
    /// them" needs no sampler, no history store and no event feed — which is
    /// the whole reason a bench can measure the run loop without changing what
    /// it costs.
    #[must_use]
    pub fn inbound(&self) -> u64 {
        self.inbound.load(Ordering::Relaxed)
    }

    /// Messages that have come out of the transform chain since it started.
    /// Summed over the batches of a pass, so a `splitter` makes this exceed
    /// [`Counters::inbound`] and a `filter` makes it fall short.
    #[must_use]
    pub fn outbound(&self) -> u64 {
        self.outbound.load(Ordering::Relaxed)
    }

    /// Failures counted since it started — every one of them, including the
    /// ones the UI throttle suppressed.
    #[must_use]
    pub fn errors(&self) -> u64 {
        self.errors.load(Ordering::Relaxed)
    }

    fn read(&self) -> Counts {
        Counts {
            inbound: self.inbound(),
            outbound: self.outbound(),
            errors: self.errors(),
        }
    }
}

/// A reading of the three counters, for differencing against the last one.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Counts {
    inbound: u64,
    outbound: u64,
    errors: u64,
}

impl Counts {
    /// What happened between `self` and `newer`.
    ///
    /// Saturating rather than wrapping: a pipeline rebuilt under the same id
    /// gets fresh counters that start below the last reading, and reporting
    /// that as eighteen quintillion messages would be a memorable bug.
    fn delta(self, newer: Self) -> HistoryBucket {
        HistoryBucket {
            start: 0,
            inbound: newer.inbound.saturating_sub(self.inbound),
            outbound: newer.outbound.saturating_sub(self.outbound),
            errors: newer.errors.saturating_sub(self.errors),
        }
    }
}

/// A fixed-capacity, dense run of buckets: written at the head, the oldest
/// dropped off the tail.
///
/// "Dense" is the part worth knowing — [`Ring::advance_to`] fills the buckets
/// between the last write and now with empties, so the series is contiguous and
/// an idle pipeline reads as a run of zeroes rather than as no data.
struct Ring {
    resolution: Resolution,
    capacity: usize,
    buckets: VecDeque<HistoryBucket>,
}

impl Ring {
    fn new(resolution: Resolution, capacity: usize) -> Self {
        Self {
            resolution,
            capacity,
            buckets: VecDeque::with_capacity(capacity.min(1024)),
        }
    }

    /// Make the bucket containing `epoch_secs` the head, filling any gap behind
    /// it with empties and evicting from the tail to stay at capacity.
    fn advance_to(&mut self, epoch_secs: u64) {
        let target = self.resolution.bucket_of(epoch_secs);
        let width = self.resolution.bucket_secs();
        match self.buckets.back().map(|b| b.start) {
            None => self.buckets.push_back(HistoryBucket::empty(target)),
            Some(head) if head >= target => {}
            Some(head) => {
                // Cap the fill at the ring's capacity: a server asleep for a
                // day would otherwise push a day of empties one at a time to
                // evict all but the last `capacity` of them.
                let missing = (target - head) / width;
                let skip = missing.saturating_sub(self.capacity as u64);
                let mut start = head + (skip.saturating_add(1)) * width;
                while start <= target {
                    self.buckets.push_back(HistoryBucket::empty(start));
                    start += width;
                }
            }
        }
        while self.buckets.len() > self.capacity {
            self.buckets.pop_front();
        }
    }

    /// Fold a delta into the head bucket. `advance_to` must have been called
    /// for the same moment first.
    fn absorb(&mut self, delta: &HistoryBucket) {
        if let Some(head) = self.buckets.back_mut() {
            head.absorb(delta);
        }
    }

    /// When the ring last saw anything happen, in epoch seconds.
    fn last_activity(&self) -> Option<u64> {
        self.buckets
            .iter()
            .rev()
            .find(|b| !b.is_empty())
            .map(|b| b.start)
    }
}

/// One pipeline's history.
struct Record {
    fine: Ring,
    coarse: Ring,
    /// Keyed by what makes two failures the same failure — see
    /// [`ErrorSignature`].
    errors: HashMap<(Stage, Option<usize>, String), ErrorSignature>,
    dropped_signatures: u64,
    /// The last counter reading, so the next sample is a difference.
    last: Counts,
    /// Whether a live pipeline of this id has ever been sampled. A record whose
    /// pipeline is gone is kept until the retention runs out — see the module
    /// docs — and this is how [`History::prune`] tells "deleted" from "created
    /// a moment ago and not yet sampled".
    seen: bool,
}

impl Record {
    fn new(config: &HistoryConfig) -> Self {
        Self {
            fine: Ring::new(Resolution::Fine, config.fine_capacity()),
            coarse: Ring::new(Resolution::Coarse, config.coarse_capacity()),
            errors: HashMap::new(),
            dropped_signatures: 0,
            last: Counts::default(),
            seen: false,
        }
    }

    /// The most recent moment anything was recorded, for pruning.
    fn last_activity(&self) -> u64 {
        let buckets = self.coarse.last_activity().unwrap_or(0);
        let errors = self
            .errors
            .values()
            .map(|e| e.last_seen / 1000)
            .max()
            .unwrap_or(0);
        buckets.max(errors)
    }
}

/// Every pipeline's history, bounded by [`HistoryConfig`].
///
/// Held in `AppState` and handed to each run loop, which only ever touches the
/// [`Counters`] it was given and [`History::record_error`].
pub struct History {
    config: HistoryConfig,
    records: Mutex<HashMap<PipelineId, Record>>,
}

impl History {
    #[must_use]
    pub fn new(config: HistoryConfig) -> Self {
        Self {
            config,
            records: Mutex::new(HashMap::new()),
        }
    }

    /// A store that keeps nothing — what tests and a `retention_secs: 0`
    /// deployment run with. Every method is still callable and does nothing,
    /// so no caller needs an `Option<History>`.
    #[must_use]
    pub fn disabled() -> Self {
        Self::new(HistoryConfig { retention_secs: 0 })
    }

    /// Whether anything is being kept.
    #[must_use]
    pub fn enabled(&self) -> bool {
        self.config.enabled()
    }

    /// Record a failure, `count` occurrences of it — one, plus whatever the
    /// UI throttle suppressed since the last time it let this signature
    /// through.
    ///
    /// Called from the run loop's error arms, which are already behind that
    /// throttle, so this is bounded at a few calls a second per component
    /// however fast the pipeline is failing.
    pub fn record_error(
        &self,
        id: &PipelineId,
        stage: Stage,
        component: Option<usize>,
        message: &str,
        count: u64,
        now_millis: u64,
    ) {
        if !self.enabled() {
            return;
        }
        let message = truncate(message);
        let Ok(mut records) = self.records.lock() else {
            return;
        };
        let record = records
            .entry(id.clone())
            .or_insert_with(|| Record::new(&self.config));
        let key = (stage, component, message.clone());
        if let Some(existing) = record.errors.get_mut(&key) {
            existing.last_seen = now_millis;
            existing.count = existing.count.saturating_add(count);
            return;
        }
        if record.errors.len() >= MAX_ERROR_SIGNATURES {
            // Evict the one seen longest ago: a failure that is still
            // happening is the one worth keeping, and a burst of distinct
            // texts is exactly the case this cap exists for.
            let stalest = record
                .errors
                .iter()
                .min_by_key(|(_, e)| e.last_seen)
                .map(|(k, _)| k.clone());
            if let Some(stalest) = stalest {
                record.errors.remove(&stalest);
                record.dropped_signatures = record.dropped_signatures.saturating_add(1);
            }
        }
        record.errors.insert(
            key,
            ErrorSignature {
                stage,
                component,
                message,
                first_seen: now_millis,
                last_seen: now_millis,
                count,
            },
        );
    }

    /// Fold one tick's worth of counters into the rings, and prune records
    /// whose pipelines are gone and whose retention has run out.
    ///
    /// `live` is every running pipeline and its counters. Called on a timer by
    /// [`crate::history::sampler`]; separate from it so it can be driven
    /// directly by tests without a clock.
    pub fn sample<'a>(
        &self,
        live: impl IntoIterator<Item = (&'a PipelineId, &'a Counters)>,
        now_secs: u64,
    ) {
        if !self.enabled() {
            return;
        }
        let Ok(mut records) = self.records.lock() else {
            return;
        };
        let mut alive = Vec::new();
        for (id, counters) in live {
            alive.push(id.clone());
            let record = records
                .entry(id.clone())
                .or_insert_with(|| Record::new(&self.config));
            record.seen = true;
            let now = counters.read();
            let delta = record.last.delta(now);
            record.last = now;
            record.fine.advance_to(now_secs);
            record.coarse.advance_to(now_secs);
            record.fine.absorb(&delta);
            record.coarse.absorb(&delta);
        }
        // Everything else is a pipeline that has been deleted. Its buckets are
        // left exactly as they were rather than advanced: a deleted pipeline
        // did not run and produce nothing, it stopped existing, and filling its
        // ring with zeroes would push the record of what it did off the tail.
        records.retain(|id, record| {
            alive.contains(id)
                || !record.seen
                || now_secs.saturating_sub(record.last_activity()) < self.config.retention_secs
        });
    }

    /// What one pipeline did, at the resolution asked for.
    ///
    /// An id with no record answers with an empty history rather than nothing:
    /// a pipeline created a second ago has no buckets yet, and that is not an
    /// error for the caller to distinguish.
    #[must_use]
    pub fn get(&self, id: &PipelineId, resolution: Resolution) -> PipelineHistory {
        let mut history = PipelineHistory {
            resolution,
            bucket_secs: resolution.bucket_secs(),
            buckets: Vec::new(),
            errors: Vec::new(),
            dropped_signatures: 0,
        };
        let Ok(records) = self.records.lock() else {
            return history;
        };
        let Some(record) = records.get(id) else {
            return history;
        };
        let ring = match resolution {
            Resolution::Fine => &record.fine,
            Resolution::Coarse => &record.coarse,
        };
        history.buckets = ring.buckets.iter().copied().collect();
        history.errors = record.errors.values().cloned().collect();
        // Most recently seen first: the thing that is still broken is the thing
        // being looked for. Ties broken by the text so the order is stable
        // across requests rather than being the map's iteration order.
        history
            .errors
            .sort_by(|a, b| b.last_seen.cmp(&a.last_seen).then(a.message.cmp(&b.message)));
        history.dropped_signatures = record.dropped_signatures;
        history
    }

    /// Forget one pipeline entirely. Not called by `delete_pipeline` — see the
    /// module docs on why records outlive their pipelines — but it is what a
    /// future "clear this card's history" reaches for.
    pub fn forget(&self, id: &PipelineId) {
        if let Ok(mut records) = self.records.lock() {
            records.remove(id);
        }
    }
}

/// How often [`sampler`] folds the counters into the rings.
///
/// The fine bucket width, so every bucket gets exactly one sample and the two
/// never drift — a sampler running slower than the buckets are wide would leave
/// gaps that [`Ring::advance_to`] fills with zeroes, drawing a running pipeline
/// as a dotted line.
const SAMPLE_INTERVAL: std::time::Duration =
    std::time::Duration::from_secs(kayak_core::history::FINE_BUCKET_SECS);

/// Fold every running pipeline's counters into its history, once every
/// [`SAMPLE_INTERVAL`], until the process ends.
///
/// This is the whole cost of the feature at rest: one wake-up every five
/// seconds, one lock, and a walk of the pipelines map. It does not scale with
/// throughput, which is the property the counters exist to buy — see the module
/// docs.
///
/// Spawned by `main` and *not* by [`crate::state::AppState`], for the reason the
/// server's other background work isn't either: a state built by a test should
/// not silently acquire a task, and every integration test constructs one.
pub async fn sampler(state: std::sync::Arc<crate::state::AppState>) {
    if !state.history().enabled() {
        return;
    }
    let mut ticker = tokio::time::interval(SAMPLE_INTERVAL);
    // A tick that is late is not a tick that should be made up for: bunching
    // four samples into one instant would put four buckets' worth of messages
    // into whichever bucket is current, which reads as a spike that never
    // happened.
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        ticker.tick().await;
        state.sample_history(crate::events::now_millis() / 1000);
    }
}

#[cfg(test)]
mod tests {
    use super::{Counters, History, Ring};
    use kayak_core::Stage;
    use kayak_core::history::{COARSE_BUCKET_SECS, FINE_BUCKET_SECS, MAX_ERROR_SIGNATURES, Resolution};
    use kayak_core::server_config::HistoryConfig;

    fn config(retention_secs: u64) -> HistoryConfig {
        HistoryConfig { retention_secs }
    }

    /// The property the whole design rests on: a ring never grows, however
    /// long the server runs. Memory is a function of the retention and nothing
    /// else.
    #[test]
    fn a_ring_evicts_from_the_tail_and_never_grows() {
        let mut ring = Ring::new(Resolution::Coarse, 3);
        for minute in 0..100 {
            ring.advance_to(minute * COARSE_BUCKET_SECS);
            ring.absorb(&super::HistoryBucket {
                start: 0,
                inbound: 1,
                outbound: 0,
                errors: 0,
            });
            assert!(ring.buckets.len() <= 3, "ring grew past its capacity");
        }
        let starts: Vec<u64> = ring.buckets.iter().map(|b| b.start).collect();
        assert_eq!(
            starts,
            vec![97 * COARSE_BUCKET_SECS, 98 * COARSE_BUCKET_SECS, 99 * COARSE_BUCKET_SECS],
            "the ring should hold the three most recent minutes"
        );
        assert!(
            ring.buckets.iter().all(|b| b.inbound == 1),
            "each surviving bucket keeps its own count"
        );
    }

    /// A gap is filled with zeroes rather than left out: "the pipeline stopped"
    /// and "the server was never asked" are different facts, and the first one
    /// is what this store exists to show.
    #[test]
    fn a_gap_is_filled_with_empty_buckets() {
        let mut ring = Ring::new(Resolution::Fine, 100);
        ring.advance_to(0);
        ring.advance_to(FINE_BUCKET_SECS * 4);
        let starts: Vec<u64> = ring.buckets.iter().map(|b| b.start).collect();
        assert_eq!(
            starts,
            vec![0, 5, 10, 15, 20],
            "the four buckets in between should exist and be empty"
        );
        assert!(ring.buckets.iter().take(4).all(super::HistoryBucket::is_empty));
    }

    /// Filling a gap must not cost time proportional to the gap: a server
    /// suspended for a week would otherwise push a week of empty buckets one
    /// at a time to evict all but the last few.
    #[test]
    fn a_gap_longer_than_the_ring_is_not_walked() {
        let mut ring = Ring::new(Resolution::Coarse, 10);
        ring.advance_to(0);
        // a year later
        ring.advance_to(365 * 24 * 60 * COARSE_BUCKET_SECS);
        assert_eq!(ring.buckets.len(), 10, "the ring is still exactly its capacity");
    }

    /// Capacity is derived from the retention, which is the only knob — see
    /// `HistoryConfig`.
    #[test]
    fn capacity_comes_from_the_retention() {
        let day = config(86_400);
        assert_eq!(day.coarse_capacity(), 1_440);
        assert_eq!(day.fine_capacity(), 360);

        // a retention shorter than the fine window shortens the fine ring too:
        // a server told to keep five minutes shouldn't hold half an hour of
        // anything
        let five_minutes = config(300);
        assert_eq!(five_minutes.coarse_capacity(), 5);
        assert_eq!(five_minutes.fine_capacity(), 60);
    }

    /// Zero retention is the off switch, and it has to reach everything —
    /// nothing recorded, nothing sampled, nothing served.
    #[test]
    fn zero_retention_keeps_nothing() {
        let history = History::new(config(0));
        assert!(!history.enabled());

        let id = "quiet".to_string();
        let counters = Counters::default();
        counters.add_inbound(100);
        history.record_error(&id, Stage::Output, Some(0), "broker is down", 1, 1_000);
        history.sample([(&id, &counters)], 60);

        let out = history.get(&id, Resolution::Coarse);
        assert!(out.buckets.is_empty(), "nothing should be bucketed");
        assert!(out.errors.is_empty(), "nothing should be remembered");
    }

    /// Counters are monotonic and history is the difference between two
    /// readings — which is what lets the run loop add to them without knowing
    /// what a bucket is.
    #[test]
    fn sampling_buckets_the_difference_between_readings() {
        let history = History::new(config(3_600));
        let id = "busy".to_string();
        let counters = Counters::default();

        counters.add_inbound(10);
        counters.add_outbound(10);
        history.sample([(&id, &counters)], 0);

        // a minute later, thirty more
        counters.add_inbound(30);
        counters.add_outbound(30);
        counters.add_error();
        history.sample([(&id, &counters)], COARSE_BUCKET_SECS);

        let out = history.get(&id, Resolution::Coarse);
        assert_eq!(out.buckets.len(), 2);
        assert_eq!(out.buckets[0].inbound, 10, "the first minute holds its own ten");
        assert_eq!(
            out.buckets[1].inbound, 30,
            "the second holds the difference, not the running total"
        );
        assert_eq!(out.buckets[1].errors, 1);
        assert_eq!(out.bucket_secs, COARSE_BUCKET_SECS);
    }

    /// A pipeline rebuilt under the same id gets fresh counters that read
    /// *below* the last sample. Wrapping there would report eighteen
    /// quintillion messages.
    #[test]
    fn counters_going_backwards_do_not_wrap() {
        let history = History::new(config(3_600));
        let id = "rebuilt".to_string();

        let first = Counters::default();
        first.add_inbound(1_000);
        history.sample([(&id, &first)], 0);

        let second = Counters::default();
        second.add_inbound(5);
        history.sample([(&id, &second)], COARSE_BUCKET_SECS);

        let out = history.get(&id, Resolution::Coarse);
        assert_eq!(
            out.buckets[1].inbound, 0,
            "a reading below the last one is no messages, not a wrap"
        );
    }

    /// The morning readout: one entry per distinct failure, with a tally that
    /// includes the repeats the UI throttle suppressed.
    #[test]
    fn repeated_failures_aggregate_into_one_signature() {
        let history = History::new(config(3_600));
        let id = "failing".to_string();

        history.record_error(&id, Stage::Output, Some(0), "broker is down", 1, 1_000);
        history.record_error(&id, Stage::Output, Some(0), "broker is down", 400, 2_000);
        history.record_error(&id, Stage::Output, Some(0), "broker is down", 400, 3_000);

        let out = history.get(&id, Resolution::Coarse);
        assert_eq!(out.errors.len(), 1, "one distinct failure is one entry");
        let signature = &out.errors[0];
        assert_eq!(signature.first_seen, 1_000);
        assert_eq!(signature.last_seen, 3_000);
        assert_eq!(
            signature.count, 801,
            "the tally counts suppressed repeats, not reported ones"
        );
    }

    /// The same text from a different component is a different fact — the same
    /// rule the run loop's failure budget uses.
    #[test]
    fn the_same_text_from_two_components_is_two_signatures() {
        let history = History::new(config(3_600));
        let id = "two-outputs".to_string();
        history.record_error(&id, Stage::Output, Some(0), "timed out", 1, 1_000);
        history.record_error(&id, Stage::Output, Some(1), "timed out", 1, 1_000);
        assert_eq!(history.get(&id, Resolution::Coarse).errors.len(), 2);
    }

    /// The bound that is easy to miss: an error text carrying a message id
    /// makes every failure distinct, so the map has to have a cap and an
    /// eviction rule like any other keyed store here.
    #[test]
    fn distinct_error_texts_are_capped_and_the_stalest_goes_first() {
        let history = History::new(config(3_600));
        let id = "noisy".to_string();
        let noise = u64::try_from(MAX_ERROR_SIGNATURES).unwrap_or(0) * 3;
        for n in 0..noise {
            history.record_error(
                &id,
                Stage::Transform,
                Some(0),
                &format!("failed on message {n}"),
                1,
                1_000 + n,
            );
        }
        let out = history.get(&id, Resolution::Coarse);
        assert_eq!(out.errors.len(), MAX_ERROR_SIGNATURES, "the map is bounded");
        assert_eq!(
            out.dropped_signatures,
            noise - u64::try_from(MAX_ERROR_SIGNATURES).unwrap_or(0),
            "and says how much it isn't showing"
        );
        assert!(
            out.errors[0].message.ends_with(&format!("{}", noise - 1)),
            "the most recent failure survives; the stalest is what was evicted"
        );
    }

    /// Errors come back most recently seen first: the thing that is still
    /// broken is the thing being looked for.
    #[test]
    fn errors_are_ordered_by_when_they_were_last_seen() {
        let history = History::new(config(3_600));
        let id = "ordered".to_string();
        history.record_error(&id, Stage::Output, Some(0), "old news", 1, 1_000);
        history.record_error(&id, Stage::Input, None, "still happening", 1, 9_000);
        let out = history.get(&id, Resolution::Coarse);
        assert_eq!(out.errors[0].message, "still happening");
    }

    /// A record outlives its pipeline — a revert rebuilds every pipeline in the
    /// graph, and dropping history there would cost the overnight record of all
    /// of them for an edit to one.
    #[test]
    fn a_deleted_pipeline_keeps_its_history_until_the_retention_runs_out() {
        let history = History::new(config(3_600));
        let id = "gone".to_string();
        let counters = Counters::default();
        counters.add_inbound(10);
        counters.add_error();
        history.sample([(&id, &counters)], 0);
        history.record_error(&id, Stage::Input, None, "connection reset", 1, 0);

        // the pipeline is deleted: subsequent samples don't mention it
        history.sample(std::iter::empty(), COARSE_BUCKET_SECS);
        let out = history.get(&id, Resolution::Coarse);
        assert_eq!(out.errors.len(), 1, "what killed it is still readable");
        assert_eq!(out.buckets.len(), 1, "and so is what it did");

        // an hour later it has aged out
        history.sample(std::iter::empty(), 3_600 * 2);
        assert!(history.get(&id, Resolution::Coarse).buckets.is_empty());
    }

    /// A deleted pipeline's ring is left alone rather than advanced. Filling it
    /// with zeroes would push the record of what it did off the tail long
    /// before the retention was up.
    #[test]
    fn a_deleted_pipelines_buckets_are_not_advanced() {
        let history = History::new(config(3_600));
        let id = "stopped".to_string();
        let counters = Counters::default();
        counters.add_inbound(42);
        history.sample([(&id, &counters)], 0);

        for minute in 1..50 {
            history.sample(std::iter::empty(), minute * COARSE_BUCKET_SECS);
        }
        let out = history.get(&id, Resolution::Coarse);
        assert_eq!(out.buckets.len(), 1, "the record didn't grow while nothing ran");
        assert_eq!(out.buckets[0].inbound, 42);
    }

    /// An id nobody has ever heard of is an empty history, not a failure: a
    /// pipeline created a second ago has no buckets yet and that is not
    /// something a caller should have to tell apart.
    #[test]
    fn an_unknown_pipeline_has_an_empty_history() {
        let history = History::new(config(3_600));
        let out = history.get(&"never-existed".to_string(), Resolution::Fine);
        assert!(out.buckets.is_empty());
        assert!(out.errors.is_empty());
        assert_eq!(out.bucket_secs, FINE_BUCKET_SECS);
    }

    /// The two rings are fed from the same sample and answer at their own
    /// widths.
    #[test]
    fn both_resolutions_see_the_same_messages() {
        let history = History::new(config(3_600));
        let id = "both".to_string();
        let counters = Counters::default();
        counters.add_inbound(7);
        history.sample([(&id, &counters)], 0);

        let fine = history.get(&id, Resolution::Fine);
        let coarse = history.get(&id, Resolution::Coarse);
        assert_eq!(fine.bucket_secs, FINE_BUCKET_SECS);
        assert_eq!(coarse.bucket_secs, COARSE_BUCKET_SECS);
        assert_eq!(fine.buckets[0].inbound, 7);
        assert_eq!(coarse.buckets[0].inbound, 7);
    }
}
