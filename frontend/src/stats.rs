//! The per-card throughput chart: what a card counts of the `UiEvent` feed.
//!
//! Pure so it can be tested without a DOM, the same convention `log.rs` follows
//! — the component in `app.rs` pushes events in and renders the bars that come
//! out, and holds no counting state of its own.
//!
//! **The counts are the browser's, not the server's.** Nothing is recorded
//! while the section is collapsed and nothing survives a reload, because this
//! is fed by the same `/events` stream the log is: a readout of what this tab
//! has watched happen, not a metrics store. What it does get right is the
//! *rate*, because the feed is sampled under load and every batch carries what
//! was skipped to reach it — see [`kayak_core::BatchPreview::counted`]. Counting
//! the events that arrive would report a fraction of a busy pipeline.

use kayak_core::{EventPayload, Stage, UiEvent};
use std::collections::VecDeque;

/// How many time units the chart shows. The window is this times the unit — two
/// and a half minutes at `5s`, half an hour at `1m`, two and a half hours at
/// `5m`.
///
/// Chosen against the card rather than as a round number: a card is eighteen
/// grid cells wide, so thirty bar *pairs* is about four pixels a bar. Sixty was
/// the first attempt and the pairs stopped being two bars — a fan of hairlines
/// where the whole point is comparing one against the other.
pub const BARS: usize = 30;

/// The width of one bar pair, and so how far back the chart reaches.
///
/// Three rather than a free number because the point of the control is to
/// answer "is this bursty or steady", and that question has about three useful
/// zoom levels. `Seconds5` is the default: a card that has just been opened
/// should draw something within a few seconds rather than after a minute of
/// looking at an empty chart.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub enum Unit {
    #[default]
    Seconds5,
    Minute,
    Minutes5,
}

impl Unit {
    /// Every unit, in the order the chips are drawn.
    pub const ALL: [Self; 3] = [Self::Seconds5, Self::Minute, Self::Minutes5];

    /// How wide one bucket is, in seconds.
    #[must_use]
    pub fn seconds(self) -> u64 {
        match self {
            Self::Seconds5 => 5,
            Self::Minute => 60,
            Self::Minutes5 => 300,
        }
    }

    /// What the chip says.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Seconds5 => "5s",
            Self::Minute => "1m",
            Self::Minutes5 => "5m",
        }
    }

    /// How far back the whole chart reaches, for the chip's tooltip.
    #[must_use]
    pub fn window_label(self) -> &'static str {
        match self {
            Self::Seconds5 => "five seconds a bar — the last two and a half minutes",
            Self::Minute => "a minute a bar — the last half hour",
            Self::Minutes5 => "five minutes a bar — the last two and a half hours",
        }
    }

    /// The start of the bucket `ts` (epoch millis) belongs to, in epoch seconds.
    #[must_use]
    fn bucket_of(self, ts_millis: u64) -> u64 {
        let seconds = ts_millis / 1000;
        seconds - (seconds % self.seconds())
    }
}

/// One time unit's worth of counting.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Default)]
pub struct Bucket {
    /// Where the unit starts, in epoch seconds — always a multiple of the unit.
    pub start: u64,
    /// Messages that arrived at the pipeline's inputs.
    pub inbound: u64,
    /// Messages that left through its outputs, **summed over every output**. A
    /// pipeline with two outputs emits every message twice and says so; that is
    /// what makes one output failing visible as a gap rather than as nothing.
    pub outbound: u64,
}

impl Bucket {
    /// The taller of the two bars.
    #[must_use]
    pub fn peak(&self) -> u64 {
        self.inbound.max(self.outbound)
    }
}

/// One card's throughput history.
///
/// Read it with `.with()` rather than `.get()` when it is behind a signal, for
/// the reason the log gives: it is a fixed number of buckets, but they are
/// copied on every render otherwise.
#[derive(Clone, Debug, Default)]
pub struct Stats {
    unit: Unit,
    /// Oldest first, at most [`BARS`] of them. Sparse: a unit in which nothing
    /// happened has no bucket, and [`Stats::bars`] fills the gap back in.
    buckets: VecDeque<Bucket>,
}

impl Stats {
    #[must_use]
    pub fn unit(&self) -> Unit {
        self.unit
    }

    /// Switch the time unit, **discarding what was counted**.
    ///
    /// Re-bucketing is not possible in the useful direction: five-second
    /// buckets could be summed into minutes, but minutes can't be cut into
    /// seconds, and a chart that fills in one direction and empties in the other
    /// is harder to read than one that always starts fresh.
    pub fn set_unit(&mut self, unit: Unit) {
        if unit == self.unit {
            return;
        }
        self.unit = unit;
        self.buckets.clear();
    }

    pub fn clear(&mut self) {
        self.buckets.clear();
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.buckets.is_empty()
    }

    /// Count what an event carried.
    ///
    /// Failures are not counted — a failed batch produced no messages, and the
    /// error belongs in the log where it can be read. Events with no timestamp
    /// (`ts == 0`, an older server) are dropped for the reason `log::Rate` drops
    /// them: a chart over time is meaningless without a clock, and bucketing
    /// them at the epoch would scroll the real bars off the end.
    pub fn record(&mut self, event: &UiEvent) {
        let EventPayload::Batch(preview) = &event.payload else {
            return;
        };
        if event.ts == 0 {
            return;
        }
        let count = u64::try_from(preview.counted()).unwrap_or(u64::MAX);
        let start = self.unit.bucket_of(event.ts);

        // Newest first is the common case by a long way — one comparison — but a
        // late event is possible (several run loops share the feed, and only the
        // publisher's clock orders them), so an older bucket that is still in
        // the window is found rather than dropped.
        let index = match self.buckets.back() {
            Some(last) if last.start == start => Some(self.buckets.len() - 1),
            Some(last) if last.start < start => None,
            _ => self.buckets.iter().rposition(|b| b.start == start),
        };

        let bucket = match index {
            Some(index) => &mut self.buckets[index],
            None if self.buckets.back().is_none_or(|last| last.start < start) => {
                self.buckets.push_back(Bucket {
                    start,
                    ..Bucket::default()
                });
                self.prune(start);
                // `prune` can drop from the front but never the back
                let last = self.buckets.len() - 1;
                &mut self.buckets[last]
            }
            // older than anything kept, and older than the window: it would be
            // drawn off the left-hand edge anyway
            None => return,
        };

        match event.stage {
            Stage::Input => bucket.inbound = bucket.inbound.saturating_add(count),
            Stage::Output => bucket.outbound = bucket.outbound.saturating_add(count),
            // a transform publishes failures and nothing else, so this arm is
            // unreachable today; counting it as either half would be a guess
            Stage::Transform => {}
        }
    }

    fn prune(&mut self, newest: u64) {
        let oldest = self.oldest_start(newest);
        while self.buckets.front().is_some_and(|b| b.start < oldest) {
            self.buckets.pop_front();
        }
    }

    fn oldest_start(&self, newest: u64) -> u64 {
        let span = self.unit.seconds() * (BARS as u64 - 1);
        newest.saturating_sub(span)
    }

    /// The chart: exactly [`BARS`] buckets ending at `now`, oldest first, with
    /// the empty units filled back in.
    ///
    /// `now` is passed in rather than taken from the last event, which is what
    /// makes the chart *roll*: a pipeline that stops produces no events, and a
    /// chart drawn from its own contents would freeze with the last burst
    /// pinned to the right-hand edge instead of watching it slide away.
    ///
    /// The two clocks are not the same one, though — the timestamps are the
    /// server's and `now` is the browser's — so the window ends at whichever is
    /// further on. A browser a few seconds behind the server would otherwise cut
    /// the newest bars off the chart entirely.
    #[must_use]
    pub fn bars(&self, now_millis: u64) -> Vec<Bucket> {
        let Some(newest) = self.buckets.back().map(|b| b.start) else {
            return Vec::new();
        };
        let end = if now_millis == 0 {
            newest
        } else {
            self.unit.bucket_of(now_millis).max(newest)
        };

        let step = self.unit.seconds();
        let mut bars = Vec::with_capacity(BARS);
        let mut kept = self.buckets.iter().peekable();
        // Counted back from `end` rather than forward from the oldest slot: the
        // right-hand edge is the one that has to be *now*, and adding the width
        // of the window to a start that had saturated would put it somewhere
        // else entirely.
        for slot in (0..BARS as u64).rev() {
            let start = end.saturating_sub(step * slot);
            while kept.peek().is_some_and(|b| b.start < start) {
                kept.next();
            }
            let bucket = match kept.peek() {
                Some(b) if b.start == start => **b,
                _ => Bucket {
                    start,
                    ..Bucket::default()
                },
            };
            bars.push(bucket);
        }
        bars
    }

    /// The tallest bar in the window — what the chart scales against, and the
    /// one number it puts a label on.
    #[must_use]
    pub fn peak(bars: &[Bucket]) -> u64 {
        bars.iter().map(Bucket::peak).max().unwrap_or(0)
    }
}

/// The chart's coordinate space. The `<svg>` is drawn at `preserveAspectRatio:
/// none`, so these are the only numbers involved and the bars stretch to
/// whatever width the card is — a maximized card gets a wider chart rather than
/// a scaled-up one.
const VIEW_W: f64 = 100.0;
const VIEW_H: f64 = 100.0;

/// How much of a slot each bar takes, and the gap between the pair. What is left
/// over is the space between one time unit and the next.
const BAR_W: f64 = 0.36;
const PAIR_GAP: f64 = 0.08;
const LEAD: f64 = 0.1;

/// A bar that is nonzero is never invisible: below this it is drawn at this
/// height. A single message in an hour of thousands is still worth a mark.
const MIN_BAR: f64 = 1.5;

/// The two series as SVG path data — inbound first, outbound second.
///
/// One path per series rather than a rect per bar, which is the whole reason the
/// chart is cheap enough to redraw once a second on every card: a frame is two
/// attribute writes instead of a hundred and twenty elements reconciled.
#[must_use]
pub fn bar_paths(bars: &[Bucket], peak: u64) -> (String, String) {
    if peak == 0 || bars.is_empty() {
        return (String::new(), String::new());
    }
    #[allow(clippy::cast_precision_loss)]
    let slot = VIEW_W / bars.len() as f64;
    let width = slot * BAR_W;
    let mut inbound = String::new();
    let mut outbound = String::new();

    for (index, bucket) in bars.iter().enumerate() {
        #[allow(clippy::cast_precision_loss)]
        let left = index as f64 * slot;
        push_bar(
            &mut inbound,
            left + slot * LEAD,
            width,
            bucket.inbound,
            peak,
        );
        push_bar(
            &mut outbound,
            left + slot * (LEAD + BAR_W + PAIR_GAP),
            width,
            bucket.outbound,
            peak,
        );
    }
    (inbound, outbound)
}

/// One bar, appended as a closed rectangle subpath rooted on the baseline.
fn push_bar(path: &mut String, x: f64, width: f64, value: u64, peak: u64) {
    if value == 0 {
        return;
    }
    #[allow(clippy::cast_precision_loss)]
    let height = (value as f64 / peak as f64 * VIEW_H).max(MIN_BAR);
    let top = VIEW_H - height;
    path.push_str(&format!(
        "M{x:.2} {top:.2}h{width:.2}v{height:.2}h-{width:.2}Z"
    ));
}

/// A count as the peak label shows it: whole numbers up to a thousand, then two
/// significant figures and a suffix, because the label sits in the corner of a
/// chart 360 pixels wide.
#[must_use]
pub fn compact(value: u64) -> String {
    #[allow(clippy::cast_precision_loss)]
    let scaled = |divisor: f64, suffix: &str| {
        let n = value as f64 / divisor;
        if n < 10.0 {
            format!("{n:.1}{suffix}")
        } else {
            format!("{n:.0}{suffix}")
        }
    };
    match value {
        0..=999 => value.to_string(),
        1_000..=999_999 => scaled(1_000.0, "k"),
        1_000_000..=999_999_999 => scaled(1_000_000.0, "M"),
        _ => scaled(1_000_000_000.0, "G"),
    }
}

#[cfg(test)]
mod tests {
    use super::{BARS, Bucket, Stats, Unit, bar_paths, compact};
    use kayak_core::{Stage, UiEvent};
    use std::sync::Arc;

    /// A wall clock the buckets are relative to. Real rather than small: the
    /// window is counted backwards from `now`, so a timestamp near the epoch
    /// would run into the saturation this never meets in a browser.
    /// Divisible by every unit, so an offset is also its bucket's offset.
    const T0: u64 = 1_700_000_400;

    fn batch(messages: usize) -> Vec<Arc<serde_json::Value>> {
        (0..messages)
            .map(|n| Arc::new(serde_json::json!({ "n": n })))
            .collect()
    }

    /// A batch event `offset` seconds after [`T0`] carrying `messages`
    /// messages, none of them skipped by the feed.
    fn event(stage: Stage, offset: u64, messages: usize) -> UiEvent {
        UiEvent::batch("p".to_string(), stage, &batch(messages), 0).at(millis(offset))
    }

    /// `offset` seconds after [`T0`], in epoch millis — what a clock reads.
    fn millis(offset: u64) -> u64 {
        (T0 + offset) * 1000
    }

    /// The feed is sampled, so a reported batch stands for the ones that were
    /// dropped to reach it. Counting only what arrives would draw a chart of the
    /// sampling rate rather than of the pipeline.
    #[test]
    fn a_bar_counts_what_the_feed_skipped() {
        let event = UiEvent::batch("p".to_string(), Stage::Input, &batch(1), 99).at(millis(0));

        let mut stats = Stats::default();
        stats.record(&event);

        let bars = stats.bars(millis(0));
        assert_eq!(bars.last().map(|b| b.inbound), Some(100));
    }

    #[test]
    fn messages_land_in_the_bucket_their_timestamp_falls_in() {
        let mut stats = Stats::default();
        stats.record(&event(Stage::Input, 100, 3));
        stats.record(&event(Stage::Input, 102, 2));
        stats.record(&event(Stage::Output, 104, 4));

        let bars = stats.bars(millis(104));
        let last = bars.last().copied().unwrap_or_default();
        assert_eq!(last.start, T0 + 100, "5s buckets start on a multiple of five");
        assert_eq!(last.inbound, 5, "both input events are one bucket");
        assert_eq!(last.outbound, 4);
    }

    /// Every output gets every batch, so two of them is twice the messages. That
    /// is the honest number and it is what makes one output dying visible.
    #[test]
    fn outputs_are_summed_over_their_components() {
        let mut stats = Stats::default();
        stats.record(&event(Stage::Input, 10, 5));
        stats.record(&event(Stage::Output, 10, 5).component(0));
        stats.record(&event(Stage::Output, 10, 5).component(1));

        let last = stats.bars(millis(10)).last().copied().unwrap_or_default();
        assert_eq!(last.inbound, 5);
        assert_eq!(last.outbound, 10);
    }

    /// A pipeline that stops produces no events, so nothing but the clock can
    /// move the chart along. The burst has to slide away on its own.
    #[test]
    fn the_window_rolls_with_the_clock_rather_than_with_the_events() {
        let mut stats = Stats::default();
        stats.record(&event(Stage::Input, 0, 7));

        let bars = stats.bars(millis(5));
        assert_eq!(bars.len(), BARS);
        assert_eq!(
            bars.last().map(|b| b.inbound),
            Some(0),
            "the newest bar is now, and nothing happened now"
        );
        assert_eq!(
            bars.iter().map(|b| b.inbound).sum::<u64>(),
            7,
            "the burst is still in the window, further left"
        );

        // far enough on and it is gone entirely
        let later = stats.bars(millis(BARS as u64 * 5));
        assert_eq!(later.iter().map(|b| b.inbound).sum::<u64>(), 0);
    }

    /// The timestamps are the server's clock and `now` is the browser's. A
    /// browser a little behind must not cut the newest bars off the chart.
    #[test]
    fn a_browser_clock_behind_the_server_still_shows_the_newest_bar() {
        let mut stats = Stats::default();
        stats.record(&event(Stage::Input, 20, 4));

        let bars = stats.bars(millis(10));
        assert_eq!(
            bars.last().map(|b| b.inbound),
            Some(4),
            "the window ends at whichever clock is further on"
        );
    }

    #[test]
    fn empty_units_are_filled_back_in() {
        let mut stats = Stats::default();
        stats.record(&event(Stage::Input, 0, 1));
        stats.record(&event(Stage::Input, 15, 1));

        let bars = stats.bars(millis(15));
        assert_eq!(bars.len(), BARS);
        let tail: Vec<u64> = bars.iter().rev().take(4).map(|b| b.inbound).collect();
        assert_eq!(tail, vec![1, 0, 0, 1], "two gaps between the two events");
    }

    #[test]
    fn nothing_older_than_the_window_is_kept() {
        let mut stats = Stats::default();
        for unit in 0..(BARS as u64 * 4) {
            stats.record(&event(Stage::Input, unit * 5, 1));
        }
        assert!(
            stats.bars(0).len() == BARS,
            "the chart is always the same width"
        );
        assert_eq!(
            stats.bars(0).iter().map(|b| b.inbound).sum::<u64>(),
            BARS as u64,
            "only the window's worth is still counted"
        );
    }

    /// Several run loops share one feed and only the publisher's clock orders
    /// them, so an event can arrive after one that is newer than it.
    #[test]
    fn a_late_event_still_finds_its_bucket() {
        let mut stats = Stats::default();
        stats.record(&event(Stage::Input, 100, 1));
        stats.record(&event(Stage::Input, 110, 1));
        stats.record(&event(Stage::Input, 100, 5));

        let bars = stats.bars(millis(110));
        let older = bars.iter().find(|b| b.start == T0 + 100).copied();
        assert_eq!(older.map(|b| b.inbound), Some(6));
    }

    #[test]
    fn an_event_with_no_clock_is_not_counted() {
        let mut stats = Stats::default();
        stats.record(&UiEvent::batch("p".to_string(), Stage::Input, &batch(1), 0));
        assert!(stats.is_empty(), "ts == 0 is 'the server didn't say'");
    }

    #[test]
    fn a_failure_is_not_a_message() {
        let mut stats = Stats::default();
        stats.record(&UiEvent::error("p".to_string(), Stage::Output, &"boom").at(millis(0)));
        assert!(stats.is_empty());
    }

    #[test]
    fn changing_the_unit_starts_the_chart_again() {
        let mut stats = Stats::default();
        stats.record(&event(Stage::Input, 10, 3));
        stats.set_unit(Unit::Minute);
        assert!(stats.is_empty(), "minutes can't be cut out of seconds");
        assert_eq!(stats.unit(), Unit::Minute);

        stats.record(&event(Stage::Input, 130, 3));
        stats.set_unit(Unit::Minute);
        assert!(!stats.is_empty(), "setting the unit it already has is a no-op");
        let last = stats.bars(millis(130)).last().copied().unwrap_or_default();
        assert_eq!(last.start, T0 + 120, "minute buckets start on the minute");
    }

    /// What a collapsed section does on the way down: a chart that was not fed
    /// for a while would draw the gap as an idle pipeline.
    #[test]
    fn clearing_leaves_nothing_to_draw() {
        let mut stats = Stats::default();
        stats.record(&event(Stage::Input, 0, 3));
        stats.clear();
        assert!(stats.is_empty());
        assert!(stats.bars(millis(0)).is_empty(), "and no bars either");
    }

    #[test]
    fn a_chart_with_nothing_in_it_draws_nothing() {
        let bars = vec![Bucket::default(); BARS];
        let (inbound, outbound) = bar_paths(&bars, Stats::peak(&bars));
        assert!(inbound.is_empty() && outbound.is_empty());
    }

    /// The chart scales to its tallest bar, so the peak is drawn full height and
    /// nothing is drawn outside the box.
    #[test]
    fn bars_are_scaled_against_the_peak() {
        let bars = vec![
            Bucket {
                start: 0,
                inbound: 10,
                outbound: 0,
            },
            Bucket {
                start: 5,
                inbound: 5,
                outbound: 20,
            },
        ];
        let peak = Stats::peak(&bars);
        assert_eq!(peak, 20);

        let (inbound, outbound) = bar_paths(&bars, peak);
        assert!(
            outbound.contains("v100.00"),
            "the peak is the full height: {outbound}"
        );
        assert!(
            inbound.contains("v50.00"),
            "half the peak is half the height: {inbound}"
        );
        assert_eq!(
            inbound.matches('M').count(),
            2,
            "one subpath per nonzero bar"
        );
        assert_eq!(
            outbound.matches('M').count(),
            1,
            "a zero bar is not drawn at all"
        );
    }

    /// One message in a window of thousands still has to leave a mark, or a
    /// trickle reads as a stopped pipeline.
    #[test]
    fn a_bar_that_is_not_zero_is_never_invisible() {
        let bars = vec![Bucket {
            start: 0,
            inbound: 1,
            outbound: 100_000,
        }];
        let (inbound, _) = bar_paths(&bars, Stats::peak(&bars));
        assert!(inbound.contains("v1.50"), "{inbound}");
    }

    #[test]
    fn counts_are_labelled_compactly() {
        assert_eq!(compact(0), "0");
        assert_eq!(compact(999), "999");
        assert_eq!(compact(1_000), "1.0k");
        assert_eq!(compact(12_345), "12k");
        assert_eq!(compact(1_500_000), "1.5M");
        assert_eq!(compact(2_000_000_000), "2.0G");
    }
}
