//! What a pipeline did, kept after the fact — the declaration half.
//!
//! `/events` is a **sample of what is happening now**: it is gated on a browser
//! being attached, it drops passes under load on purpose, and it keeps nothing.
//! That is the right design for a live card and the wrong one for the question
//! this module answers — *the pipeline failed at 02:14 and I got here at 08:00,
//! what happened?* Nobody was watching at 02:14, so there was no feed.
//!
//! So history is fed from somewhere else entirely: unconditional counters in the
//! run loop, sampled on a tick, plus the error texts that the run loop's
//! existing failure budget already lets through. Nothing here rides on the
//! event feed, and that is the load-bearing property — a persistent subscriber
//! to `/events` would hold `receiver_count() > 0` open forever and make every
//! headless server pay the browser-attached cost of a UI nobody has opened.
//!
//! This module is the declaration: the shapes the API serves and the constants
//! that bound them. [`crate::server_config::HistoryConfig`] is the knob and
//! `kayak::history` is the live store.
//!
//! # Why two resolutions
//!
//! A day at the card chart's finest bar width (five seconds) is 17,280 buckets
//! per pipeline, and almost all of it is detail nobody will ever scroll back
//! to. A day at one minute is 1,440. So there are two rings and they answer
//! different questions:
//!
//! - [`Resolution::Fine`] — [`FINE_BUCKET_SECS`] a bucket, covering
//!   [`FINE_WINDOW_SECS`]. What the card's live chart is backfilled from when a
//!   tab opens, so the chart starts full instead of drawing itself over the
//!   next two minutes. Not configurable: it is bounded by what a card can
//!   display, not by what an operator wants to keep.
//! - [`Resolution::Coarse`] — [`COARSE_BUCKET_SECS`] a bucket, covering the
//!   configured retention. This is the overnight record.
//!
//! Both are ring buffers: fixed capacity, written at the head, the oldest
//! bucket dropped off the tail. Memory is flat in uptime *and* in throughput —
//! a pipeline doing eight million messages a second costs exactly what an idle
//! one costs, because a bucket holds counts rather than messages.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::Stage;

/// How wide a fine bucket is, in seconds. The same width as the card chart's
/// finest bar (`frontend::stats::Unit::Seconds5`), so backfilling one is a copy
/// rather than a re-aggregation.
pub const FINE_BUCKET_SECS: u64 = 5;

/// How far back the fine ring reaches, in seconds. Half an hour — well past the
/// two and a half minutes the chart shows, so the window can be scrolled
/// without the resolution falling away underneath it, and still only
/// [`FINE_WINDOW_SECS`] / [`FINE_BUCKET_SECS`] buckets.
pub const FINE_WINDOW_SECS: u64 = 1_800;

/// How wide a coarse bucket is, in seconds. A minute is the coarsest width that
/// still shows a pipeline stopping as a distinct event rather than as a dip.
pub const COARSE_BUCKET_SECS: u64 = 60;

/// How long the coarse ring keeps its buckets when nothing says otherwise. A
/// day, because the case this exists for is arriving in the morning to a
/// pipeline that broke overnight.
pub const DEFAULT_RETENTION_SECS: u64 = 86_400;

/// The most retention a config may ask for, in seconds.
///
/// A hard cap rather than a warning because the store is in memory: retention
/// *is* an allocation, and the difference between `86400` and a
/// fat-fingered `864000` is the difference between a working server and one the
/// OOM killer takes at 3am. A week is past the point where the honest answer is
/// a real metrics store — see the roadmap.
pub const MAX_RETENTION_SECS: u64 = 7 * 86_400;

/// How many distinct failures one pipeline may remember at once.
///
/// This bound is the one that is easy to miss. Errors look self-limiting —
/// "however many things are broken" is a small number — but an error *text*
/// often carries a message id, an offset or a row number, so a pipeline failing
/// on every message can produce a new distinct signature every time. Without a
/// cap that is an unbounded map fed at the failure rate, which is the exact leak
/// this whole module is supposed to be too boring to have.
pub const MAX_ERROR_SIGNATURES: usize = 64;

/// Which ring a query is asking for. See the module docs for why there are two.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Resolution {
    /// [`FINE_BUCKET_SECS`] a bucket, over [`FINE_WINDOW_SECS`]. What a card
    /// backfills its live chart from.
    Fine,
    /// [`COARSE_BUCKET_SECS`] a bucket, over the configured retention. The
    /// overnight record, and the default because that is what someone asking
    /// for history at all is usually asking for.
    #[default]
    Coarse,
}

impl Resolution {
    /// How wide one of this resolution's buckets is, in seconds.
    #[must_use]
    pub fn bucket_secs(self) -> u64 {
        match self {
            Self::Fine => FINE_BUCKET_SECS,
            Self::Coarse => COARSE_BUCKET_SECS,
        }
    }

    /// The start of the bucket `epoch_secs` falls in — always a multiple of
    /// [`Resolution::bucket_secs`], so two servers bucketing the same moment
    /// agree about which bucket it is.
    #[must_use]
    pub fn bucket_of(self, epoch_secs: u64) -> u64 {
        epoch_secs - (epoch_secs % self.bucket_secs())
    }
}

/// One time unit's worth of counting, as the store keeps it and the API serves
/// it.
///
/// Deliberately the same three questions the card chart asks
/// (`frontend::stats::Bucket`) plus the one it can't answer from a sampled
/// feed: how many failures there were. Counts, not messages — which is what
/// makes a bucket 32 bytes whatever the pipeline is carrying, and what makes
/// keeping a day of them cost less than one message of most real payloads.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct HistoryBucket {
    /// Where the bucket starts, in seconds since the epoch. Always a multiple
    /// of the resolution's width.
    pub start: u64,
    /// Messages that arrived at the pipeline's inputs during the bucket.
    pub inbound: u64,
    /// Messages that came out of the transform chain and were handed to the
    /// outputs during the bucket.
    ///
    /// Counted once per batch, not once per output — it is what the pipeline
    /// *produced*, which is the same thing the card chart's outbound bar counts
    /// off the `Stage::Output` events, so the two agree. A transform that
    /// changes cardinality is what makes this differ from `inbound`; a failing
    /// output is not, and shows up in `errors` instead.
    pub outbound: u64,
    /// Failures at any stage during the bucket. The *true* count, not the
    /// throttled one: this is a counter, so suppressing a repeat in the log
    /// doesn't hide it here.
    pub errors: u64,
}

impl HistoryBucket {
    /// An empty bucket at `start` — what a resolution's gaps are filled with,
    /// so a chart can tell "nothing happened" from "no data".
    #[must_use]
    pub fn empty(start: u64) -> Self {
        Self {
            start,
            ..Self::default()
        }
    }

    /// Fold `other` into this bucket, keeping the earlier start. Used to
    /// aggregate fine buckets into a coarser view.
    pub fn absorb(&mut self, other: &Self) {
        self.inbound = self.inbound.saturating_add(other.inbound);
        self.outbound = self.outbound.saturating_add(other.outbound);
        self.errors = self.errors.saturating_add(other.errors);
    }

    /// Whether anything at all happened in this bucket.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inbound == 0 && self.outbound == 0 && self.errors == 0
    }
}

/// One distinct failure, and how it has behaved over time.
///
/// **Aggregated rather than logged**, which is the difference between a useful
/// morning readout and two million rows to scroll. A pipeline whose broker went
/// down at 02:14 and stayed down is one of these saying so, with a count — and
/// that is both cheaper to keep and easier to read than the log it replaces.
///
/// Identity is (`stage`, `component`, `message`): the same text from the second
/// of two outputs is a different fact from the first one's, which is the same
/// rule the run loop's failure budget already uses.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct ErrorSignature {
    /// Where in the pass it failed.
    pub stage: Stage,
    /// Which component of that stage, indexed into its array in the config.
    /// `None` where the run loop doesn't know — an input failure, since inputs
    /// are merged before the loop sees them.
    pub component: Option<usize>,
    /// The failure's text, as the log line would have shown it, cut to
    /// [`crate::MAX_MESSAGE_BYTES`]. Cut rather than kept whole for the reason
    /// the feed's messages are: an error with a payload embedded in it can be
    /// arbitrarily long, and this is a store that promises to be bounded.
    pub message: String,
    /// When it was first seen, in milliseconds since the epoch. This is the
    /// number the morning question is actually about.
    pub first_seen: u64,
    /// When it was last seen. Equal to `first_seen` for a one-off; far from it
    /// for something still broken, which is how the two are told apart.
    pub last_seen: u64,
    /// How many times it has happened, including the repeats the log
    /// suppressed. See [`HistoryBucket::errors`] — same accounting.
    pub count: u64,
}

/// What `GET /api/pipelines/{id}/history` answers with.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct PipelineHistory {
    /// Which ring this came from, echoed so a client that took the default
    /// knows what it got.
    pub resolution: Resolution,
    /// How wide one bucket is, in seconds. Derivable from `resolution`, sent
    /// anyway so a chart can scale its axis without a table of constants.
    pub bucket_secs: u64,
    /// Oldest first, contiguous — gaps are filled with empty buckets rather
    /// than omitted, so "the pipeline stopped" and "the server wasn't asked"
    /// don't look alike. Empty when the pipeline has produced nothing yet.
    pub buckets: Vec<HistoryBucket>,
    /// Distinct failures, most recently seen first, at most
    /// [`MAX_ERROR_SIGNATURES`] of them. Not scoped to the buckets' window:
    /// a failure that started before the window is exactly the one worth
    /// showing, and `first_seen` says so.
    pub errors: Vec<ErrorSignature>,
    /// Distinct failures dropped to stay under [`MAX_ERROR_SIGNATURES`], since
    /// this pipeline started. Non-zero means the errors above are a selection,
    /// and is itself a diagnosis: a pipeline producing dozens of distinct
    /// failure texts is usually one embedding a message id in each.
    pub dropped_signatures: u64,
}
