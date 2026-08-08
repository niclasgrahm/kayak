//! The per-card message log: what a card remembers of the `UiEvent` feed.
//!
//! Pure so it can be tested without a DOM — the component in `app.rs` pushes
//! events in and renders what comes out, and holds no log state of its own.

use kayak_core::{EventPayload, Stage, UiEvent, truncate};
use std::collections::VecDeque;

/// How many entries a card keeps. Older ones are dropped from the front, so the
/// log reads like a tail.
pub const LOG_CAPACITY: usize = 200;

/// The window the throughput readout averages over.
pub const RATE_WINDOW_SECS: u64 = 10;

/// What an entry is: a batch that passed a stage, or a failure at one.
///
/// A batch is **one entry holding its messages**, not one entry per message.
/// That is a deliberate reversal of how this started out: a 500-message batch
/// was 500 lines and emptied the whole buffer in one pass, and it made the log
/// unreadable as a record of what the pipeline did. The messages are still
/// there, one expand away.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EntryKind {
    Batch {
        /// Compact JSON, at most [`MESSAGES_PER_BATCH`] of them. Compact rather
        /// than pretty because this is what the collapsed row shows; expanding
        /// one re-parses it.
        messages: Vec<String>,
        /// How many more the batch held. Zero for a batch that fit.
        dropped: usize,
    },
    Error(String),
}

/// One event, as the log stores it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Entry {
    /// Unique for the life of the log, so `<For>` can key on it. Ids are never
    /// reused, even after the entry they belonged to has been dropped.
    pub id: u64,
    /// Server clock, epoch millis. Zero means the server didn't say — see
    /// [`format_time`].
    pub ts: u64,
    /// The run-loop pass this belongs to, or `None` for something that happened
    /// outside one. See [`passes`].
    pub seq: Option<u64>,
    /// Which component of the stage, where the server knew. See
    /// `UiEvent::component`.
    pub component: Option<usize>,
    pub stage: Stage,
    pub kind: EntryKind,
    /// How many times this entry has been seen in a row, counting the first.
    /// Only failures coalesce; see [`Log::push`].
    pub repeats: u32,
}

impl Entry {
    /// How many messages the batch held, including the ones not kept. Zero for
    /// a failure, which carries no batch.
    #[must_use]
    pub fn message_count(&self) -> usize {
        match &self.kind {
            EntryKind::Batch { messages, dropped } => messages.len() + dropped,
            EntryKind::Error(_) => 0,
        }
    }

    #[must_use]
    pub fn is_error(&self) -> bool {
        matches!(self.kind, EntryKind::Error(_))
    }
}

/// Which entries a card is showing.
///
/// Three independent toggles rather than a single mode, because the useful
/// combinations aren't a sequence: "output only" and "errors only" are both
/// reasonable, and so is "input and errors" when a transform is dropping
/// batches. `errors` covers a failure at *any* stage — a transform never
/// produces a batch, so its failures have nowhere else to live.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Filter {
    pub input: bool,
    pub output: bool,
    pub errors: bool,
}

impl Default for Filter {
    fn default() -> Self {
        Self::all()
    }
}

impl Filter {
    #[must_use]
    pub fn all() -> Self {
        Self {
            input: true,
            output: true,
            errors: true,
        }
    }

    /// What the card's error badge switches to when it is clicked.
    #[must_use]
    pub fn errors_only() -> Self {
        Self {
            input: false,
            output: false,
            errors: true,
        }
    }

    #[must_use]
    pub fn is_errors_only(self) -> bool {
        self == Self::errors_only()
    }

    /// Nothing is showing. A real state — the chips can all be turned off — and
    /// the one the empty message has to distinguish from "nothing has arrived".
    #[must_use]
    pub fn is_empty(self) -> bool {
        !self.input && !self.output && !self.errors
    }

    #[must_use]
    pub fn matches(self, entry: &Entry) -> bool {
        match entry.kind {
            EntryKind::Error(_) => self.errors,
            EntryKind::Batch { .. } => match entry.stage {
                Stage::Input => self.input,
                Stage::Output => self.output,
                // a transform publishes failures and nothing else, so this arm
                // is unreachable today. it is not an error if that changes.
                Stage::Transform => self.input || self.output,
            },
        }
    }
}

/// Messages per second over the last [`RATE_WINDOW_SECS`], in one-second
/// buckets.
///
/// Counted separately from the entries because the entries are capped: under
/// the load where the number is worth having, most of what it counts has
/// already been dropped from the log.
#[derive(Clone, Debug, Default)]
pub struct Rate {
    /// `(second since the epoch, messages)`, oldest first.
    buckets: VecDeque<(u64, u32)>,
}

impl Rate {
    /// Record messages seen at `ts`. Events with no timestamp (`ts == 0`, an
    /// older server) are not counted — a rate is meaningless without a clock,
    /// and counting them at the epoch would report zero forever.
    pub fn record(&mut self, ts: u64, messages: usize) {
        if ts == 0 {
            return;
        }
        let second = ts / 1000;
        let count = u32::try_from(messages).unwrap_or(u32::MAX);
        match self.buckets.back_mut() {
            Some((s, n)) if *s == second => *n = n.saturating_add(count),
            _ => self.buckets.push_back((second, count)),
        }
        self.prune(second);
    }

    /// The average over the window ending at `now`. Falls back towards zero on
    /// its own once traffic stops, which is why `now` is passed in rather than
    /// taken from the last event.
    #[must_use]
    pub fn per_second(&self, now_millis: u64) -> f64 {
        let oldest = Self::oldest_second(now_millis / 1000);
        let total: u64 = self
            .buckets
            .iter()
            .filter(|(second, _)| *second >= oldest)
            .map(|(_, n)| u64::from(*n))
            .sum();
        #[allow(clippy::cast_precision_loss)]
        {
            total as f64 / RATE_WINDOW_SECS as f64
        }
    }

    fn prune(&mut self, now_seconds: u64) {
        let oldest = Self::oldest_second(now_seconds);
        while self.buckets.front().is_some_and(|(s, _)| *s < oldest) {
            self.buckets.pop_front();
        }
    }

    /// The first second still inside the window. The window is the last
    /// [`RATE_WINDOW_SECS`] *whole* seconds including the current one, so ten
    /// seconds of traffic average to what was recorded rather than to
    /// nine-tenths of it.
    fn oldest_second(now_seconds: u64) -> u64 {
        now_seconds.saturating_sub(RATE_WINDOW_SECS - 1)
    }
}

/// One card's log.
///
/// Read it with `.with()` rather than `.get()` when it is behind a signal:
/// cloning two hundred entries on every render is exactly what the cap is
/// there to avoid.
#[derive(Clone, Debug, Default)]
pub struct Log {
    entries: VecDeque<Entry>,
    next_id: u64,
    rate: Rate,
    /// Failures since the badge was last acknowledged. Kept separately from the
    /// entries because an error scrolls out of a busy log in seconds, and the
    /// count is the only thing that then says it happened.
    unseen_errors: u32,
}

impl Log {
    #[must_use]
    pub fn entries(&self) -> &VecDeque<Entry> {
        &self.entries
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    #[must_use]
    pub fn unseen_errors(&self) -> u32 {
        self.unseen_errors
    }

    /// Mark the failures as seen — what clicking the card's error badge does.
    pub fn ack_errors(&mut self) {
        self.unseen_errors = 0;
    }

    /// The entries grouped into passes. See [`passes`].
    #[must_use]
    pub fn passes(&self) -> Vec<Pass> {
        passes(&self.entries)
    }

    #[must_use]
    pub fn per_second(&self, now_millis: u64) -> f64 {
        self.rate.per_second(now_millis)
    }

    pub fn clear(&mut self) {
        self.entries.clear();
        self.unseen_errors = 0;
    }

    /// Take everything from an event *except* the row — what a paused log does
    /// with what arrives while nobody is reading it.
    ///
    /// The two things it keeps are the two that would otherwise lie. The rate is
    /// a fact about the pipeline, not about the log, and a paused card reading
    /// `0/s` under a pipeline at full tilt says the wrong thing. And an error
    /// while paused still has to reach the badge: a failure you were not looking
    /// at is exactly the one worth being told about.
    pub fn skip(&mut self, event: &UiEvent) {
        match &event.payload {
            EventPayload::Batch(preview) => self.rate.record(event.ts, preview.counted()),
            EventPayload::Error(_) => {
                self.unseen_errors = self.unseen_errors.saturating_add(1);
            }
        }
    }

    /// Append an event.
    ///
    /// **Identical consecutive failures coalesce** into the entry already at the
    /// back, bumping `repeats` and taking the newer timestamp. A failing output
    /// errors once per batch, so without this a broken connection fills the card
    /// with the same line and pushes everything that explains it out of the log.
    /// Batches never coalesce: the same payload arriving twice is news.
    pub fn push(&mut self, event: &UiEvent) {
        let kind = match &event.payload {
            // The messages arrive already rendered and already cut — the server
            // does that now, so this is a move rather than the per-message
            // `to_string` it used to be. See `kayak_core::BatchPreview`.
            EventPayload::Batch(preview) => {
                self.rate.record(event.ts, preview.counted());
                EntryKind::Batch {
                    messages: preview.messages.clone(),
                    dropped: preview.dropped(),
                }
            }
            EventPayload::Error(message) => {
                self.unseen_errors = self.unseen_errors.saturating_add(1);
                EntryKind::Error(truncate(message))
            }
        };

        // Only within one pass: the same failure on two different batches is
        // two facts, and merging them across passes would put one entry into a
        // group it did not come from.
        if let Some(last) = self.entries.back_mut()
            && last.stage == event.stage
            && last.seq == event.seq
            && last.component == event.component
            && last.kind == kind
            && matches!(kind, EntryKind::Error(_))
        {
            last.repeats = last.repeats.saturating_add(1);
            last.ts = event.ts;
            return;
        }

        if self.entries.len() == LOG_CAPACITY {
            self.entries.pop_front();
        }
        self.entries.push_back(Entry {
            id: self.next_id,
            ts: event.ts,
            seq: event.seq,
            component: event.component,
            stage: event.stage,
            kind,
            repeats: 1,
        });
        self.next_id += 1;
    }
}

/// One batch's journey through the pipeline: what arrived, what the transforms
/// did to it, and what left.
///
/// This is the unit the log reads in, and the reason the events carry a `seq`
/// at all. A row per pass answers "what is this pipeline doing" in a way a row
/// per event cannot — and it is what makes an input and an output legible as
/// two halves of one thing rather than as interleaved noise.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Pass {
    /// `None` for the events that belong to no pass — an input source dying in
    /// its own task, an output that failed to initialise. Each of those is a
    /// pass of one, which is how the log shows them.
    pub seq: Option<u64>,
    /// When the pass started, from its first entry.
    pub ts: u64,
    pub entries: Vec<Entry>,
    /// Messages in, messages out, and how many entries failed.
    pub in_count: usize,
    pub out_count: usize,
    pub errors: usize,
    /// Passes that happened but were never seen, immediately before this one.
    ///
    /// The UI feed is a broadcast channel that drops rather than blocks, so a
    /// browser that fell behind loses passes outright. Rendering the survivors
    /// as consecutive would be a quiet lie about what the pipeline did; a gap
    /// that says "12 passes are missing" is the truth and takes one row.
    pub gap_before: u64,
}

impl Pass {
    /// The one-line summary of a collapsed pass.
    #[must_use]
    pub fn summary(&self) -> String {
        format!("{} in → {} out", self.in_count, self.out_count)
    }

    #[must_use]
    pub fn has_errors(&self) -> bool {
        self.errors > 0
    }

    /// A stable identity for a pass, across the additions it accumulates.
    ///
    /// The id of the pass's first entry rather than its `seq`, because `seq` is
    /// `None` for everything that happened outside a pass and several of those
    /// can be on screen at once. Entry ids are unique for the life of the log
    /// and a pass is never empty, so this is unique too.
    ///
    /// This is what "which passes are open" is keyed on: an open pass must stay
    /// open as the rest of it arrives.
    #[must_use]
    pub fn key(&self) -> u64 {
        self.entries.first().map_or(0, |entry| entry.id)
    }

    /// A key that changes whenever the pass's *content* does — what `<For>` has
    /// to be keyed on.
    ///
    /// A pass is not finished when it first appears: the input event arrives,
    /// the row renders, and the outputs land a moment later. Keyed on identity
    /// alone, `<For>` reuses the existing view and the row is frozen at the
    /// moment it was created — which showed every pass on the canvas as
    /// "1 in → 0 out" for as long as it was on screen.
    ///
    /// Length catches an entry being appended; the repeat total catches an
    /// error coalescing into one already there, which changes what the row says
    /// without changing how many rows there are.
    #[must_use]
    pub fn render_key(&self) -> (u64, usize, u32) {
        (
            self.key(),
            self.entries.len(),
            self.entries.iter().map(|entry| entry.repeats).sum(),
        )
    }
}

/// What a component index means on one pipeline: the kinds of its transforms
/// and outputs, in config order.
///
/// Inputs are absent on purpose — several are merged before the run loop sees a
/// batch, so an input event carries no index to look up.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ComponentNames {
    pub transforms: Vec<String>,
    pub outputs: Vec<String>,
}

impl ComponentNames {
    /// The kind of the component an entry came from, where the server said
    /// which and the config still has one at that index.
    #[must_use]
    pub fn name(&self, stage: Stage, index: Option<usize>) -> Option<&str> {
        let index = index?;
        let names = match stage {
            Stage::Transform => &self.transforms,
            Stage::Output => &self.outputs,
            Stage::Input => return None,
        };
        names.get(index).map(String::as_str)
    }
}

/// Group entries into passes, in arrival order.
///
/// Consecutive entries sharing a `Some(seq)` are one pass. An entry with no
/// `seq` is a pass of its own — it belongs to nothing, and folding it into a
/// neighbour would attribute it to a batch it had nothing to do with. Entries
/// are already in arrival order and the server publishes a pass's events
/// together, so this is a fold rather than a sort.
#[must_use]
pub fn passes<'a>(entries: impl IntoIterator<Item = &'a Entry>) -> Vec<Pass> {
    let mut passes: Vec<Pass> = Vec::new();

    for entry in entries {
        let extends_last = entry.seq.is_some()
            && passes
                .last()
                .is_some_and(|pass| pass.seq == entry.seq);

        if !extends_last {
            let gap_before = match (passes.last().and_then(|p| p.seq), entry.seq) {
                (Some(previous), Some(current)) => current.saturating_sub(previous).saturating_sub(1),
                _ => 0,
            };
            passes.push(Pass {
                seq: entry.seq,
                ts: entry.ts,
                entries: Vec::new(),
                in_count: 0,
                out_count: 0,
                errors: 0,
                gap_before,
            });
        }

        let Some(pass) = passes.last_mut() else {
            continue;
        };
        match (entry.stage, &entry.kind) {
            (Stage::Input, EntryKind::Batch { .. }) => pass.in_count += entry.message_count(),
            (Stage::Output, EntryKind::Batch { .. }) => pass.out_count += entry.message_count(),
            (_, EntryKind::Error(_)) => pass.errors += 1,
            (Stage::Transform, EntryKind::Batch { .. }) => {}
        }
        pass.entries.push(entry.clone());
    }

    passes
}

/// Which passes a filter leaves standing.
///
/// **This is deliberately not the same rule as [`Filter::matches`]**, and the
/// difference is the point of grouping at all. Showing errors on their own in a
/// flat log means the error lines; showing them as passes means the *whole*
/// pass that failed — the batch that went in is what you need in order to make
/// sense of the failure, and hiding it because it is an `in` row would be
/// answering the wrong question.
///
/// Any other combination filters within a pass as usual, and a pass left with
/// nothing is dropped.
#[must_use]
pub fn visible_passes(passes: Vec<Pass>, filter: Filter) -> Vec<Pass> {
    if filter.is_errors_only() {
        return passes.into_iter().filter(Pass::has_errors).collect();
    }
    passes
        .into_iter()
        .filter_map(|pass| {
            let entries: Vec<Entry> = pass
                .entries
                .into_iter()
                .filter(|entry| filter.matches(entry))
                .collect();
            (!entries.is_empty()).then_some(Pass { entries, ..pass })
        })
        .collect()
}

/// The badge saying which stage a row came from.
///
/// Short, because it is a fixed column on a card 18 grid cells wide and every
/// character it takes is one the payload doesn't get. `in` and `out` are the
/// same words as the chips that filter them, which is the point — the badge and
/// the control that hides it should read as the same thing. The full word is on
/// the badge's tooltip.
#[must_use]
pub fn stage_label(stage: Stage) -> &'static str {
    match stage {
        Stage::Input => "in",
        Stage::Transform => "trf",
        Stage::Output => "out",
    }
}

/// What an entry says on its one row.
///
/// A batch leads with how many messages it held when that is more than one —
/// the count is the thing a single row can't otherwise show — and then the
/// first of them, because a payload is what makes a row recognisable. A repeated
/// failure carries its count instead.
///
/// `component` names which of the stage's components this came from, where
/// there is one. It goes in the text rather than in a column of its own: it is
/// only ever present on a failure, and a column that is empty on almost every
/// row costs more width than it returns.
#[must_use]
pub fn summary(entry: &Entry, component: Option<&str>) -> String {
    match &entry.kind {
        EntryKind::Batch { messages, .. } => match (entry.message_count(), messages.first()) {
            (0, _) | (_, None) => "empty batch".to_string(),
            (1, Some(first)) => first.clone(),
            (count, Some(first)) => format!("{count} msgs · {first}"),
        },
        EntryKind::Error(message) => {
            let described = match component {
                Some(name) => format!("{name}: {message}"),
                None => message.clone(),
            };
            if entry.repeats > 1 {
                format!("{described} ×{}", entry.repeats)
            } else {
                described
            }
        }
    }
}

/// The log as text, for the clipboard: one line per row, in the order they are
/// on screen.
///
/// The same three columns a row shows, tab-separated — a log pasted into an
/// issue or a terminal wants to stay in columns, and a tab is the separator
/// that survives both. It renders whatever entries it is handed, so what is
/// copied is what passed the filter rather than everything the card holds.
///
/// Grouped or flat doesn't come into it: the two are arrangements of the same
/// entries, and a copied log is the events, not the arrangement.
#[must_use]
pub fn as_text(entries: &[Entry], names: &ComponentNames, tz_offset_minutes: i32) -> String {
    entries
        .iter()
        .map(|entry| {
            format!(
                "{}\t{}\t{}",
                format_time(entry.ts, tz_offset_minutes),
                stage_label(entry.stage),
                summary(entry, names.name(entry.stage, entry.component))
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

/// A timestamp as `HH:MM:SS.mmm` in the viewer's own zone.
///
/// `tz_offset_minutes` follows the browser's convention — `Date`'s
/// `getTimezoneOffset`, minutes to *add* to local time to get UTC, so it is
/// negative east of Greenwich. Passed in rather than read here so this stays a
/// pure function: there is no clock and no `Date` in it, which is what lets it
/// be tested natively.
///
/// No date, because a card's log covers seconds. A `ts` of zero — a server from
/// before events were stamped — renders as placeholder of the same width, so
/// the column doesn't jump.
#[must_use]
pub fn format_time(ts_millis: u64, tz_offset_minutes: i32) -> String {
    if ts_millis == 0 {
        return "--:--:--.---".to_string();
    }
    let local = i64::try_from(ts_millis).unwrap_or(i64::MAX) - i64::from(tz_offset_minutes) * 60_000;
    let millis = local.rem_euclid(1000);
    let seconds_of_day = local.div_euclid(1000).rem_euclid(86_400);
    format!(
        "{:02}:{:02}:{:02}.{:03}",
        seconds_of_day / 3600,
        (seconds_of_day % 3600) / 60,
        seconds_of_day % 60,
        millis
    )
}

#[cfg(test)]
mod tests {
    use super::{Entry, EntryKind, Filter, LOG_CAPACITY, Log, Rate, format_time};
    use kayak_core::{MAX_MESSAGE_BYTES, MESSAGES_PER_BATCH, Stage, UiEvent};
    use serde_json::json;
    use std::sync::Arc;

    fn batch_of(stage: Stage, messages: usize) -> UiEvent {
        skipping(stage, messages, 0)
    }

    /// A batch event that also reports messages the feed didn't show — what the
    /// throttle produces once it is sampling.
    fn skipping(stage: Stage, messages: usize, skipped: u64) -> UiEvent {
        let batch = (0..messages)
            .map(|n| Arc::new(json!({ "n": n })))
            .collect::<Vec<_>>();
        UiEvent::batch("witty-crab".to_string(), stage, &batch, skipped).at(1_000)
    }

    fn failure(stage: Stage, message: &str) -> UiEvent {
        UiEvent::error("witty-crab".to_string(), stage, &message).at(1_000)
    }

    /// The reversal that shapes the whole type: a batch is a batch, not a run of
    /// unrelated lines that happen to have arrived together.
    #[test]
    fn a_batch_is_one_entry_holding_its_messages() {
        let mut log = Log::default();
        log.push(&batch_of(Stage::Output, 3));

        assert_eq!(log.entries().len(), 1);
        let entry = &log.entries()[0];
        assert_eq!(entry.message_count(), 3);
        assert_eq!(
            entry.kind,
            EntryKind::Batch {
                messages: vec![
                    r#"{"n":0}"#.to_string(),
                    r#"{"n":1}"#.to_string(),
                    r#"{"n":2}"#.to_string(),
                ],
                dropped: 0,
            }
        );
    }

    #[test]
    fn a_batch_wider_than_the_cap_keeps_the_first_messages_and_counts_the_rest() {
        let mut log = Log::default();
        log.push(&batch_of(Stage::Input, MESSAGES_PER_BATCH + 12));

        let EntryKind::Batch { messages, dropped } = &log.entries()[0].kind else {
            panic!("expected a batch entry");
        };
        assert_eq!(messages.len(), MESSAGES_PER_BATCH);
        assert_eq!(*dropped, 12);
        assert_eq!(log.entries()[0].message_count(), MESSAGES_PER_BATCH + 12);
    }

    /// Cutting mid-character would leave something that isn't a string, and the
    /// slice would panic rather than truncate.
    #[test]
    fn an_oversized_message_is_cut_on_a_character_boundary() {
        let mut log = Log::default();
        // 'é' is two bytes, so the cap lands in the middle of one
        let wide = "é".repeat(MAX_MESSAGE_BYTES);
        log.push(&failure(Stage::Output, &wide));

        let EntryKind::Error(message) = &log.entries()[0].kind else {
            panic!("expected a failure entry");
        };
        assert!(message.len() <= MAX_MESSAGE_BYTES + "…".len());
        assert!(message.ends_with('…'));
    }

    /// A failing output errors once per batch. Without coalescing, a broken
    /// connection fills the card with one line and hides why.
    #[test]
    fn identical_consecutive_failures_coalesce_into_one_entry() {
        let mut log = Log::default();
        for _ in 0..47 {
            log.push(&failure(Stage::Output, "connection refused"));
        }

        assert_eq!(log.entries().len(), 1);
        assert_eq!(log.entries()[0].repeats, 47);
    }

    #[test]
    fn a_coalesced_failure_carries_the_time_it_was_last_seen() {
        let mut log = Log::default();
        log.push(&failure(Stage::Output, "connection refused"));
        log.push(&UiEvent::error("witty-crab".to_string(), Stage::Output, &"connection refused").at(9_000));

        assert_eq!(log.entries()[0].ts, 9_000);
    }

    #[test]
    fn a_different_failure_or_an_intervening_batch_starts_a_new_entry() {
        let mut log = Log::default();
        log.push(&failure(Stage::Output, "connection refused"));
        log.push(&failure(Stage::Output, "no route to host"));
        log.push(&failure(Stage::Output, "no route to host"));
        log.push(&batch_of(Stage::Output, 1));
        log.push(&failure(Stage::Output, "no route to host"));

        let repeats: Vec<_> = log.entries().iter().map(|e| e.repeats).collect();
        assert_eq!(repeats, vec![1, 2, 1, 1]);
    }

    /// The same failure at two stages is two different facts.
    #[test]
    fn the_same_message_at_a_different_stage_does_not_coalesce() {
        let mut log = Log::default();
        log.push(&failure(Stage::Transform, "connection refused"));
        log.push(&failure(Stage::Output, "connection refused"));

        assert_eq!(log.entries().len(), 2);
    }

    /// Batches are not coalesced: the same payload twice is news, not noise.
    #[test]
    fn identical_batches_stay_separate_entries() {
        let mut log = Log::default();
        log.push(&batch_of(Stage::Input, 1));
        log.push(&batch_of(Stage::Input, 1));

        assert_eq!(log.entries().len(), 2);
    }

    #[test]
    fn the_log_keeps_the_newest_entries_only() {
        let mut log = Log::default();
        for n in 0..(LOG_CAPACITY + 3) {
            log.push(&batch_of(Stage::Input, n));
        }

        assert_eq!(log.entries().len(), LOG_CAPACITY);
        assert_eq!(log.entries().front().map(Entry::message_count), Some(3));
    }

    /// Keys have to stay unique across the whole run, or `<For>` reuses a card's
    /// row for a different entry and the log shows stale text.
    #[test]
    fn every_entry_gets_its_own_key_even_after_entries_are_dropped() {
        let mut log = Log::default();
        for n in 0..(LOG_CAPACITY * 2) {
            log.push(&batch_of(Stage::Input, n % 7));
        }

        let mut ids: Vec<_> = log.entries().iter().map(|e| e.id).collect();
        let count = ids.len();
        ids.dedup();
        assert_eq!(ids.len(), count);
    }

    #[test]
    fn clearing_leaves_nothing_and_forgets_the_unseen_failures() {
        let mut log = Log::default();
        log.push(&failure(Stage::Output, "connection refused"));
        log.clear();

        assert!(log.is_empty());
        assert_eq!(log.unseen_errors(), 0);
    }

    /// The badge counts every failure, including the ones coalescing hid.
    #[test]
    fn failures_are_counted_until_they_are_acknowledged() {
        let mut log = Log::default();
        log.push(&failure(Stage::Output, "connection refused"));
        log.push(&failure(Stage::Output, "connection refused"));
        log.push(&batch_of(Stage::Input, 1));
        assert_eq!(log.unseen_errors(), 2);

        log.ack_errors();
        assert_eq!(log.unseen_errors(), 0);

        log.push(&failure(Stage::Input, "upstream went away"));
        assert_eq!(log.unseen_errors(), 1);
    }

    fn entry(stage: Stage, error: bool) -> Entry {
        Entry {
            id: 0,
            ts: 1_000,
            seq: Some(1),
            component: None,
            stage,
            kind: if error {
                EntryKind::Error("boom".to_string())
            } else {
                EntryKind::Batch {
                    messages: vec!["{}".to_string()],
                    dropped: 0,
                }
            },
            repeats: 1,
        }
    }

    #[test]
    fn the_default_filter_shows_everything() {
        let filter = Filter::default();
        assert!(filter.matches(&entry(Stage::Input, false)));
        assert!(filter.matches(&entry(Stage::Output, false)));
        assert!(filter.matches(&entry(Stage::Transform, true)));
    }

    /// A transform never produces a batch, so its failures ride on the error
    /// chip — turning inputs and outputs off must not hide them.
    #[test]
    fn errors_only_keeps_failures_from_every_stage() {
        let filter = Filter::errors_only();

        assert!(filter.matches(&entry(Stage::Transform, true)));
        assert!(filter.matches(&entry(Stage::Input, true)));
        assert!(filter.matches(&entry(Stage::Output, true)));
        assert!(!filter.matches(&entry(Stage::Input, false)));
        assert!(!filter.matches(&entry(Stage::Output, false)));
    }

    #[test]
    fn a_stage_chip_selects_batches_from_that_stage_only() {
        let filter = Filter {
            input: false,
            output: true,
            errors: false,
        };

        assert!(filter.matches(&entry(Stage::Output, false)));
        assert!(!filter.matches(&entry(Stage::Input, false)));
        assert!(!filter.matches(&entry(Stage::Output, true)));
    }

    #[test]
    fn a_filter_with_every_chip_off_shows_nothing_and_says_so() {
        let filter = Filter {
            input: false,
            output: false,
            errors: false,
        };

        assert!(filter.is_empty());
        assert!(!filter.matches(&entry(Stage::Input, false)));
        assert!(!filter.matches(&entry(Stage::Output, true)));
        assert!(!Filter::all().is_empty());
    }

    /// Wall-clock times, not seconds from the epoch: `record` ignores a zero
    /// timestamp, and counting from zero would silently drop the first bucket.
    const START: u64 = 1_786_104_221_000;

    #[test]
    fn the_rate_averages_over_the_window() {
        let mut rate = Rate::default();
        // 10 messages a second for ten seconds
        for second in 0..10 {
            rate.record(START + second * 1000, 10);
        }

        assert!((rate.per_second(START + 9_999) - 10.0).abs() < 0.01);
    }

    /// Traffic stopping has to show up as a falling number, which is why `now`
    /// is an argument: nothing calls `record` to say it stopped.
    #[test]
    fn the_rate_decays_once_traffic_stops() {
        let mut rate = Rate::default();
        for second in 0..10 {
            rate.record(START + second * 1000, 10);
        }

        assert!(rate.per_second(START + 15_000) < 5.1);
        assert!((rate.per_second(START + 60_000) - 0.0).abs() < f64::EPSILON);
    }

    /// An unstamped event has no place in a rate; counting it at the epoch would
    /// put it outside every window and report zero for ever.
    #[test]
    fn unstamped_events_are_not_counted_towards_the_rate() {
        let mut log = Log::default();
        log.push(&UiEvent::batch(
            "witty-crab".to_string(),
            Stage::Input,
            &vec![Arc::new(json!({"n": 1}))],
            0,
        ));

        assert_eq!(log.entries().len(), 1, "the entry is still kept");
        assert!((log.per_second(1_000) - 0.0).abs() < f64::EPSILON);
    }

    /// The feed is sampled under load, so an event stands for itself *and* for
    /// the passes that were dropped to get to it. Counting only what arrives
    /// would report a fraction of what the pipeline is really doing.
    #[test]
    fn the_rate_counts_what_the_feed_skipped_as_well_as_what_it_showed() {
        let mut log = Log::default();
        log.push(&skipping(Stage::Input, 10, 990));

        // 1000 messages inside a ten-second window
        assert!(
            (log.per_second(1_000) - 100.0).abs() < f64::EPSILON,
            "expected the skipped messages to be counted, got {}",
            log.per_second(1_000)
        );
    }

    /// A paused log stops keeping rows, but the pipeline's throughput is a fact
    /// about the pipeline — so the skipped count has to reach the rate here too.
    #[test]
    fn a_paused_log_still_counts_what_the_feed_skipped() {
        let mut log = Log::default();
        log.skip(&skipping(Stage::Input, 10, 990));

        assert!(log.entries().is_empty(), "a paused log keeps no rows");
        assert!((log.per_second(1_000) - 100.0).abs() < f64::EPSILON);
    }

    #[test]
    fn a_timestamp_renders_as_a_time_of_day_in_the_viewers_zone() {
        // 2026-08-07T12:03:41.220Z
        let ts = 1_786_104_221_220;
        assert_eq!(format_time(ts, 0), "12:03:41.220");
        // CEST, two hours east: `getTimezoneOffset` reports -120
        assert_eq!(format_time(ts, -120), "14:03:41.220");
        // and five behind, west
        assert_eq!(format_time(ts, 300), "07:03:41.220");
    }

    /// The offset can carry the time into the day before or after, and the
    /// arithmetic has to wrap rather than go negative — which is what the
    /// euclidean remainder is for.
    #[test]
    fn an_offset_that_crosses_midnight_wraps_the_time_of_day() {
        // 2026-08-07T00:30:00.000Z, five hours west, is the previous evening
        let ts = 1_786_062_600_000;
        assert_eq!(format_time(ts, 300), "19:30:00.000");
        // and two hours east of a late evening is the next morning
        let late = ts + 23 * 3_600_000;
        assert_eq!(format_time(late, -120), "01:30:00.000");
    }

    fn at_pass(event: UiEvent, seq: u64) -> UiEvent {
        event.seq(seq)
    }

    /// A pass is the unit the grouped log reads in: one batch's whole journey,
    /// however many events it took to describe.
    #[test]
    fn entries_sharing_a_sequence_number_are_one_pass() {
        let mut log = Log::default();
        log.push(&at_pass(batch_of(Stage::Input, 1), 1));
        log.push(&at_pass(batch_of(Stage::Output, 3), 1));
        log.push(&at_pass(batch_of(Stage::Input, 1), 2));
        log.push(&at_pass(batch_of(Stage::Output, 1), 2));

        let passes = super::passes(log.entries());

        assert_eq!(passes.len(), 2);
        assert_eq!(passes[0].seq, Some(1));
        assert_eq!(passes[0].entries.len(), 2);
        assert_eq!((passes[0].in_count, passes[0].out_count), (1, 3));
        assert_eq!((passes[1].in_count, passes[1].out_count), (1, 1));
    }

    #[test]
    fn a_pass_summarises_as_what_went_in_and_what_came_out() {
        let mut log = Log::default();
        log.push(&at_pass(batch_of(Stage::Input, 1), 1));
        log.push(&at_pass(batch_of(Stage::Output, 3), 1));

        assert_eq!(
            super::passes(log.entries())[0].summary(),
            "1 in → 3 out"
        );
    }

    /// A failure with no pass belongs to nothing, and folding it into whichever
    /// pass happened to be last would blame a batch that had nothing to do
    /// with it.
    #[test]
    fn an_entry_without_a_sequence_number_is_a_pass_of_its_own() {
        let mut log = Log::default();
        log.push(&at_pass(batch_of(Stage::Input, 1), 1));
        log.push(&failure(Stage::Input, "every input has stopped"));
        log.push(&at_pass(batch_of(Stage::Input, 1), 2));

        let passes = super::passes(log.entries());

        assert_eq!(passes.len(), 3);
        assert_eq!(passes[1].seq, None);
        assert_eq!(passes[1].entries.len(), 1);
        assert_eq!(passes[1].errors, 1);
    }

    /// Two unattached failures in a row are still two passes: neither has a
    /// number to be grouped by, so grouping them would be inventing one.
    #[test]
    fn unattached_entries_do_not_group_with_each_other() {
        let mut log = Log::default();
        log.push(&failure(Stage::Output, "could not connect"));
        log.push(&failure(Stage::Output, "no route to host"));

        assert_eq!(super::passes(log.entries()).len(), 2);
    }

    /// The feed drops rather than blocks, so a browser that fell behind loses
    /// passes outright. Drawing the survivors as consecutive would be a quiet
    /// lie about what ran.
    #[test]
    fn a_break_in_the_sequence_is_reported_as_a_gap() {
        let mut log = Log::default();
        log.push(&at_pass(batch_of(Stage::Input, 1), 8));
        log.push(&at_pass(batch_of(Stage::Input, 1), 12));

        let passes = super::passes(log.entries());

        assert_eq!(passes[0].gap_before, 0, "nothing precedes the first");
        assert_eq!(passes[1].gap_before, 3, "9, 10 and 11 were never seen");
    }

    #[test]
    fn consecutive_passes_report_no_gap() {
        let mut log = Log::default();
        log.push(&at_pass(batch_of(Stage::Input, 1), 4));
        log.push(&at_pass(batch_of(Stage::Input, 1), 5));

        assert_eq!(super::passes(log.entries())[1].gap_before, 0);
    }

    /// Errors coalesce inside a pass but never across one: the same failure on
    /// two batches is two facts, and merging them would file an entry under a
    /// pass it did not come from.
    #[test]
    fn failures_do_not_coalesce_across_passes() {
        let mut log = Log::default();
        log.push(&at_pass(failure(Stage::Output, "connection refused"), 1));
        log.push(&at_pass(failure(Stage::Output, "connection refused"), 1));
        log.push(&at_pass(failure(Stage::Output, "connection refused"), 2));

        assert_eq!(log.entries().len(), 2);
        assert_eq!(log.entries()[0].repeats, 2);
        assert_eq!(log.entries()[1].repeats, 1);
    }

    /// Two outputs failing the same way in one pass are two different outputs,
    /// and a card that merged them would say one thing is broken when two are.
    #[test]
    fn failures_from_different_components_do_not_coalesce() {
        let mut log = Log::default();
        log.push(&at_pass(failure(Stage::Output, "connection refused"), 1).component(0));
        log.push(&at_pass(failure(Stage::Output, "connection refused"), 1).component(1));

        assert_eq!(log.entries().len(), 2);
    }

    /// The rule that makes grouping worth having: an error on its own is not
    /// actionable, and the batch that caused it is in the same pass.
    #[test]
    fn errors_only_keeps_the_whole_pass_that_failed() {
        let mut log = Log::default();
        log.push(&at_pass(batch_of(Stage::Input, 1), 1));
        log.push(&at_pass(batch_of(Stage::Output, 1), 1));
        log.push(&at_pass(batch_of(Stage::Input, 1), 2));
        log.push(&at_pass(failure(Stage::Transform, "http failed"), 2));

        let visible = super::visible_passes(
            super::passes(log.entries()),
            Filter::errors_only(),
        );

        assert_eq!(visible.len(), 1, "only the pass that failed");
        assert_eq!(visible[0].seq, Some(2));
        assert_eq!(
            visible[0].entries.len(),
            2,
            "including the batch that went in, which is what explains the failure"
        );
    }

    /// Any other combination filters within a pass, and a pass left with
    /// nothing to show is dropped rather than rendered as an empty header.
    #[test]
    fn a_stage_filter_drops_entries_and_then_empty_passes() {
        let mut log = Log::default();
        log.push(&at_pass(batch_of(Stage::Input, 1), 1));
        log.push(&at_pass(batch_of(Stage::Output, 1), 1));
        log.push(&at_pass(batch_of(Stage::Input, 1), 2));

        let visible = super::visible_passes(
            super::passes(log.entries()),
            Filter {
                input: false,
                output: true,
                errors: false,
            },
        );

        assert_eq!(visible.len(), 1);
        assert_eq!(visible[0].seq, Some(1));
        assert_eq!(visible[0].entries.len(), 1);
    }

    /// The counts describe the pass, not what survived the filter — "1 in → 3
    /// out" has to stay true on a card showing only the outputs.
    #[test]
    fn filtering_a_pass_leaves_its_summary_alone() {
        let mut log = Log::default();
        log.push(&at_pass(batch_of(Stage::Input, 1), 1));
        log.push(&at_pass(batch_of(Stage::Output, 3), 1));

        let visible = super::visible_passes(
            super::passes(log.entries()),
            Filter {
                input: false,
                output: true,
                errors: false,
            },
        );

        assert_eq!(visible[0].summary(), "1 in → 3 out");
    }

    /// A pass grows after it is first rendered — the input event arrives, then
    /// the outputs. Its identity has to survive that (or an open pass would
    /// close under you) while its render key must not (or the row stays frozen
    /// at "1 in → 0 out", which is exactly what it did).
    #[test]
    fn a_pass_keeps_its_identity_as_it_grows_but_not_its_render_key() {
        let mut log = Log::default();
        log.push(&at_pass(batch_of(Stage::Input, 1), 1));
        let before = super::passes(log.entries())[0].clone();

        log.push(&at_pass(batch_of(Stage::Output, 3), 1));
        let after = super::passes(log.entries())[0].clone();

        assert_eq!(before.key(), after.key(), "the same pass");
        assert_ne!(
            before.render_key(),
            after.render_key(),
            "but not the same thing on screen"
        );
    }

    /// A failure repeating changes what the row says without changing how many
    /// rows there are, so length alone is not enough.
    #[test]
    fn a_coalescing_failure_changes_the_render_key() {
        let mut log = Log::default();
        log.push(&at_pass(failure(Stage::Output, "connection refused"), 1));
        let before = super::passes(log.entries())[0].clone();

        log.push(&at_pass(failure(Stage::Output, "connection refused"), 1));
        let after = super::passes(log.entries())[0].clone();

        assert_eq!(before.entries.len(), after.entries.len());
        assert_ne!(before.render_key(), after.render_key());
    }

    /// Pausing stops the log, not the pipeline. No new row, but the throughput
    /// is still the pipeline's and a failure is still worth being told about —
    /// so those two go on being counted while nobody is reading.
    #[test]
    fn a_skipped_event_leaves_no_row_but_still_counts() {
        let mut log = Log::default();
        log.push(&batch_of(Stage::Output, 1));

        log.skip(&batch_of(Stage::Output, 9));
        log.skip(&failure(Stage::Output, "connection refused"));

        assert_eq!(log.entries().len(), 1, "a paused log holds what it held");
        assert_eq!(log.unseen_errors(), 1, "a failure while paused is news");

        // the readout is the same one an unpaused log would show, which is the
        // point: it reports the pipeline, not what the log kept
        let mut watched = Log::default();
        watched.push(&batch_of(Stage::Output, 1));
        watched.push(&batch_of(Stage::Output, 9));
        watched.push(&failure(Stage::Output, "connection refused"));
        assert!(
            (log.per_second(1_000) - watched.per_second(1_000)).abs() < f64::EPSILON,
            "paused: {}, watched: {}",
            log.per_second(1_000),
            watched.per_second(1_000)
        );
        assert!(log.per_second(1_000) > 0.0, "nothing was counted at all");
    }

    /// The clipboard gets the rows as they read on screen: same time, same
    /// stage, same text, in the same order — and only the ones handed to it,
    /// which is what the filter left.
    #[test]
    fn the_copied_text_is_one_line_per_row() {
        let names = super::ComponentNames {
            transforms: Vec::new(),
            outputs: vec!["nats".to_string()],
        };
        let mut first = entry(Stage::Input, false);
        first.ts = 1_786_104_221_000;
        let mut failed = entry(Stage::Output, true);
        failed.ts = 1_786_104_222_500;
        failed.component = Some(0);

        let text = super::as_text(&[first, failed], &names, 0);
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 2);
        assert!(
            lines[0].starts_with("12:03:41.000\tin\t"),
            "got: {}",
            lines[0]
        );
        assert_eq!(lines[1], "12:03:42.500\tout\tnats: boom");
        assert!(!text.ends_with('\n'), "no trailing blank line to paste");
        assert_eq!(super::as_text(&[], &names, 0), "");
    }

    /// "output error" on a pipeline with two outputs is not something anyone
    /// can act on. The index the run loop attaches is only useful once it has
    /// been turned back into the name of the thing that broke.
    #[test]
    fn a_failure_is_named_by_the_component_it_came_from() {
        let names = super::ComponentNames {
            transforms: vec!["http".to_string()],
            outputs: vec!["stdout".to_string(), "nats".to_string()],
        };
        let mut failed = entry(Stage::Output, true);
        failed.component = Some(1);

        assert_eq!(names.name(Stage::Output, Some(1)), Some("nats"));
        assert_eq!(
            super::summary(&failed, names.name(failed.stage, failed.component)),
            "nats: boom"
        );
    }

    /// An index with no name behind it — an input, or a config that has since
    /// changed under a log that outlived it — leaves the row as it was rather
    /// than inventing a name or dropping the message.
    #[test]
    fn an_unnamed_component_leaves_the_row_alone() {
        let names = super::ComponentNames::default();

        assert_eq!(names.name(Stage::Input, Some(0)), None);
        assert_eq!(names.name(Stage::Output, Some(7)), None);
        assert_eq!(names.name(Stage::Output, None), None);
        assert_eq!(super::summary(&entry(Stage::Output, true), None), "boom");
    }

    /// The badge and the chip that filters it are the same word, so a row can
    /// be traced to the control that would hide it without a legend.
    #[test]
    fn the_stage_badge_reads_the_same_as_the_chip_that_filters_it() {
        assert_eq!(super::stage_label(Stage::Input), "in");
        assert_eq!(super::stage_label(Stage::Output), "out");
        assert_eq!(super::stage_label(Stage::Transform), "trf");
    }

    #[test]
    fn an_unstamped_event_renders_as_a_placeholder_of_the_same_width() {
        assert_eq!(format_time(0, -120).len(), format_time(1_000, -120).len());
        assert_eq!(format_time(0, -120), "--:--:--.---");
    }
}
