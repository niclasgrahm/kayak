//! The streaming statistics transforms — the declarations.
//!
//! Six transforms that each keep a little state *per key* and do arithmetic
//! over it as messages arrive: `deadband`, `derive`, `rolling`, `smooth`,
//! `detect` and `resample`. They share one shape, and the shape is the point:
//!
//! - **The key is `group_by`**, the reducer's list, so a series is a series
//!   per machine, or per machine and signal, the same way a reduction is.
//!   Empty is one series for the whole stream.
//! - **The state lives in the pipeline's declared state bucket**, so the bound
//!   on it, the idle timeout and the revert rule are the ones
//!   [`crate::state`] already writes down and the store already tests. A
//!   stateful transform in a pipeline with no `state` refuses to build, as
//!   `recall` does — and it is that rule, not any of these, that keeps a
//!   thousand-key window from being a leak with a week-long fuse.
//! - **Time is read by the one rule** — a `time` field holding an RFC 3339
//!   string or epoch milliseconds, arrival when it is left out.
//! - **A message missing the field or the key follows `on_missing`**: `error`
//!   fails the batch (the default, as it is the reducer's), `skip` passes the
//!   message through untouched. A value that is *present* and isn't a number
//!   is always an error, whatever `on_missing` says — the same rule every
//!   transform draws between a sparse stream and a wrong one.
//!
//! Declared here rather than in `config.rs` for the reason the column mapping
//! and the field mapping are: it is one family with one vocabulary, and the
//! evaluation lives a crate away in `kayak::transforms`.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::config::{Aggregation, MissingFieldPolicy};

// ── deadband ────────────────────────────────────────────────────────────────

/// Whether a deadband's `delta` is an amount or a fraction of the last value
/// that passed.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DeadbandMode {
    /// `delta` is in the field's own units: `0.5` is half a degree.
    #[default]
    Absolute,
    /// `delta` is a percentage of the last value that passed: `2` is two
    /// percent. A last value of zero passes everything, since a fraction of
    /// nothing is nothing.
    Percent,
}

/// Drops a message unless its field has moved far enough from the last one
/// that passed — the single most used transform in any historian pipeline,
/// and a *stateful* filter, which is why `filter` cannot be it.
///
/// The first message per key always passes. After that a message passes when
/// `field` differs from the last passed value by more than `delta`, or when
/// `max_seconds` have gone by since anything passed, so a steady reading is
/// still confirmed now and then. `flatline_seconds` is the sensor-health half:
/// when the value has not moved in that long the next message passes with
/// `stuck: true` on it, once per flat stretch, so a stuck instrument is
/// distinguishable from a quiet one downstream.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[schemars(title = "deadband")]
pub struct DeadbandTransformConfig {
    /// the numeric field the band is on
    #[schemars(extend("x-message-field" = true))]
    pub field: String,
    /// how far the value has to move to pass, in the field's units or as a
    /// percentage, by `mode`
    pub delta: f64,
    /// what `delta` is measured in
    #[serde(default, skip_serializing_if = "DeadbandMode::is_default")]
    pub mode: DeadbandMode,
    /// pass a message anyway once this many seconds have gone by since the
    /// last one that passed, so a steady value is still reported now and then
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_seconds: Option<f64>,
    /// after this many seconds with no movement, let the next message through
    /// carrying `stuck: true` — once per flat stretch
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flatline_seconds: Option<f64>,
    /// the fields that identify a series, the reducer's way. Leave it out for
    /// one series
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub group_by: Vec<String>,
    /// the field carrying each message's time — RFC 3339 or milliseconds since
    /// the epoch. Leave it out for arrival time
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(extend("x-message-field" = true))]
    pub time: Option<String>,
    /// what to do about a message missing the field or a group field
    #[serde(default, skip_serializing_if = "MissingFieldPolicy::is_default")]
    pub on_missing: MissingFieldPolicy,
}

impl DeadbandMode {
    #[must_use]
    pub fn is_default(&self) -> bool {
        matches!(self, Self::Absolute)
    }
}

// ── derive ──────────────────────────────────────────────────────────────────

/// How one message's value is combined with the previous one's.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DeriveFnKind {
    /// The change per second since the previous message, off the `time`
    /// field. `null` on the first message and when no time has passed.
    Rate,
    /// The change since the previous message. `null` on the first.
    Delta,
    /// The running total of the field, from the first message on.
    Cumsum,
    /// The running total of the *increases* — for a counter that resets or
    /// wraps. A drop below the previous value counts as a wrap when `wrap_at`
    /// is set (the increase runs through the top), and as a reset otherwise
    /// (the new value is the increase).
    Counter,
}

/// One derived value and the field it is written to.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[schemars(title = "derivation")]
pub struct Derivation {
    /// how the value is derived from this message and the previous one
    pub function: DeriveFnKind,
    /// the numeric field it is derived from
    #[schemars(extend("x-message-field" = true))]
    pub field: String,
    /// the field the answer is written under
    #[serde(rename = "as")]
    pub output: String,
    /// for `counter`: the value the counter wraps back to zero at, so a drop
    /// is read as having run through the top rather than as a reset
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wrap_at: Option<f64>,
}

/// Writes onto each message something that needs the previous one: a rate of
/// change, a delta, a running total, a wrap-tolerant counter. Not a `map`
/// operation because a `map` sees one message at a time; this remembers the
/// last per key.
///
/// Several derivations run at once and each is written under its own `as`, so
/// one pass gives both `delta` and `rate`. The first message per key has no
/// previous, and the derivations that need one write `null` for it.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[schemars(title = "derive")]
pub struct DeriveTransformConfig {
    /// what to derive. At least one, each with a distinct `as`
    pub derive: Vec<Derivation>,
    /// the fields that identify a series, the reducer's way. Leave it out for
    /// one series
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub group_by: Vec<String>,
    /// the field carrying each message's time — RFC 3339 or milliseconds since
    /// the epoch. Leave it out for arrival time
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(extend("x-message-field" = true))]
    pub time: Option<String>,
    /// what to do about a message missing a derived field or a group field
    #[serde(default, skip_serializing_if = "MissingFieldPolicy::is_default")]
    pub on_missing: MissingFieldPolicy,
}

// ── rolling ─────────────────────────────────────────────────────────────────

/// Writes onto each message aggregations over the last few messages of its
/// series — the last `size` of them, or the last `seconds`' worth, or both
/// limits at once. The reducer's `{function, field, as}` list, the reducer's
/// functions; a second component rather than a `window` on `reduce` because
/// the cardinality differs — one message out per message in, not one per
/// group per batch.
///
/// `size` is always required, because it is the bound: a window by time alone
/// grows with the rate of the stream, and every piece of state has a bound.
/// `seconds` on top of it also drops what is older than that, off the `time`
/// field. `count` needs a `field` here — it counts how many of the window
/// carried one, which is `size` once the window is warm and the warm-up check
/// before that.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[schemars(title = "rolling")]
pub struct RollingTransformConfig {
    /// what to compute over the window. At least one, each with a distinct `as`
    pub aggregations: Vec<Aggregation>,
    /// how many messages the window holds at most
    pub size: usize,
    /// also drop from the window whatever is older than this many seconds,
    /// off the `time` field
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seconds: Option<f64>,
    /// the fields that identify a series, the reducer's way. Leave it out for
    /// one series
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub group_by: Vec<String>,
    /// the field carrying each message's time — RFC 3339 or milliseconds since
    /// the epoch. Leave it out for arrival time
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(extend("x-message-field" = true))]
    pub time: Option<String>,
    /// what to do about a message missing an aggregated field or a group field
    #[serde(default, skip_serializing_if = "MissingFieldPolicy::is_default")]
    pub on_missing: MissingFieldPolicy,
}

// ── smooth ──────────────────────────────────────────────────────────────────

/// How a value is smoothed against the ones before it.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SmoothMethod {
    /// An exponentially weighted moving average — cheap, no window, follows
    /// the data as closely as `alpha` says. Give `alpha` or `half_life`, not
    /// both.
    Ewma {
        /// the weight of the newest value, 0 to 1
        #[serde(default, skip_serializing_if = "Option::is_none")]
        alpha: Option<f64>,
        /// the number of messages after which a value's weight has halved —
        /// the spelling with an intuition behind it
        #[serde(default, skip_serializing_if = "Option::is_none")]
        half_life: Option<f64>,
    },
    /// The median of the last `size` values, this one included. Removes
    /// single-sample spikes outright, which a mean only spreads out.
    Median {
        /// how many values the window holds
        size: usize,
    },
    /// A Hampel filter: the value is kept unless it is further than
    /// `threshold` robust standard deviations from the window's median, in
    /// which case the median replaces it. The right first stage in front of
    /// any detector — it removes the outliers without smearing the signal.
    Hampel {
        /// how many values the window holds, this one included
        size: usize,
        /// how many scaled MADs from the median count as an outlier. `3`
        /// when left out
        #[serde(default, skip_serializing_if = "Option::is_none")]
        threshold: Option<f64>,
    },
    /// A Savitzky–Golay filter: a polynomial of `order` fitted to the last
    /// `size` values by least squares, evaluated at the newest. Smooths while
    /// keeping the shape of peaks that a moving average flattens. Trailing
    /// rather than centred, because a stream cannot see the future; until
    /// the window holds more than `order` values the value passes untouched.
    SavitzkyGolay {
        /// how many values the window holds, this one included
        size: usize,
        /// the degree of the polynomial, below `size`. `2` when left out
        #[serde(default, skip_serializing_if = "Option::is_none")]
        order: Option<usize>,
    },
}

/// Smooths a numeric field against the values before it in its series, writing
/// the result onto the message — over the field itself, or under `as`.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[schemars(title = "smooth")]
pub struct SmoothTransformConfig {
    /// the numeric field to smooth
    #[schemars(extend("x-message-field" = true))]
    pub field: String,
    /// how
    pub method: SmoothMethod,
    /// the field the smoothed value is written under. Leave it out to replace
    /// the field itself
    #[serde(default, rename = "as", skip_serializing_if = "Option::is_none")]
    pub output: Option<String>,
    /// the fields that identify a series, the reducer's way. Leave it out for
    /// one series
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub group_by: Vec<String>,
    /// what to do about a message missing the field or a group field
    #[serde(default, skip_serializing_if = "MissingFieldPolicy::is_default")]
    pub on_missing: MissingFieldPolicy,
}

// ── detect ──────────────────────────────────────────────────────────────────

/// How an anomaly is decided.
///
/// The window methods compare a value against the values *before* it, never
/// including it, so a spike does not pull the baseline it is measured against.
/// The chart methods freeze their baseline at the end of the warm-up, which is
/// what a control chart is: a fixed idea of normal that the process is held to.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DetectMethod {
    /// Further than `threshold` standard deviations from the mean of the
    /// last `size` values.
    Zscore {
        /// how many earlier values the baseline is drawn from
        size: usize,
        /// how many standard deviations count. `3` when left out
        #[serde(default, skip_serializing_if = "Option::is_none")]
        threshold: Option<f64>,
    },
    /// The robust twin: further than `threshold` scaled median absolute
    /// deviations from the median of the last `size` values. Prefer it when
    /// the baseline itself contains outliers.
    Mad {
        /// how many earlier values the baseline is drawn from
        size: usize,
        /// how many scaled MADs count. `3.5` when left out
        #[serde(default, skip_serializing_if = "Option::is_none")]
        threshold: Option<f64>,
    },
    /// A two-sided CUSUM: sums the drift above and below `target`, and flags
    /// when either sum passes `threshold`. Finds a small sustained shift a
    /// single-point test never sees. The sum that fired is reset.
    Cusum {
        /// the value the series is expected to sit at. Left out, the mean of
        /// the warm-up is used
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target: Option<f64>,
        /// the slack per message that is not counted as drift, in the
        /// field's units
        drift: f64,
        /// the accumulated drift that counts
        threshold: f64,
    },
    /// An EWMA control chart: the smoothed value leaves a band of
    /// `threshold` standard deviations around the warm-up mean. Sensitive to
    /// small shifts, robust to single points.
    EwmaChart {
        /// the weight of the newest value, 0 to 1. `0.2` when left out
        #[serde(default, skip_serializing_if = "Option::is_none")]
        alpha: Option<f64>,
        /// the width of the band, in standard deviations. `3` when left out
        #[serde(default, skip_serializing_if = "Option::is_none")]
        threshold: Option<f64>,
    },
    /// The Western Electric rules against the warm-up mean and deviation: one
    /// point beyond 3σ, two of three beyond 2σ on one side, four of five
    /// beyond 1σ on one side, eight in a row on one side. The rule that fired
    /// is written beside the flag.
    WesternElectric {},
    /// The last `size` values are identical — a stuck instrument.
    Flatline {
        /// how many identical values in a row count
        size: usize,
    },
}

/// Whether every message comes out annotated, or only the anomalies.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DetectMode {
    /// Every message passes, carrying the flag and the score.
    #[default]
    Annotate,
    /// Only the anomalies pass, annotated. The stream becomes an alarm feed.
    OnlyAnomalies,
}

impl DetectMode {
    #[must_use]
    pub fn is_default(&self) -> bool {
        matches!(self, Self::Annotate)
    }
}

/// Flags anomalies in a numeric field against its own series — one component
/// with a `method`, the way `filter` is one component with a kind.
///
/// Writes a boolean under `as` (`anomaly` when left out), and beside it
/// `<as>_score` — how far outside normal the value was, in the method's own
/// units — so a downstream `filter` can be stricter than the threshold. Nothing
/// is flagged during the warm-up of `min_samples` messages per key, because
/// until then there is no idea of normal to be outside of.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[schemars(title = "detect")]
pub struct DetectTransformConfig {
    /// the numeric field to watch
    #[schemars(extend("x-message-field" = true))]
    pub field: String,
    /// how an anomaly is decided
    pub method: DetectMethod,
    /// whether everything comes out annotated or only the anomalies
    #[serde(default, skip_serializing_if = "DetectMode::is_default")]
    pub mode: DetectMode,
    /// how many messages per key to see before flagging anything. The
    /// method's window `size` when left out, or 30 for a method without one
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_samples: Option<usize>,
    /// the field the flag is written under; the score goes under `<as>_score`.
    /// `anomaly` when left out
    #[serde(default, rename = "as", skip_serializing_if = "Option::is_none")]
    pub output: Option<String>,
    /// the fields that identify a series, the reducer's way. Leave it out for
    /// one series
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub group_by: Vec<String>,
    /// what to do about a message missing the field or a group field
    #[serde(default, skip_serializing_if = "MissingFieldPolicy::is_default")]
    pub on_missing: MissingFieldPolicy,
}

// ── resample ────────────────────────────────────────────────────────────────

/// How the readings that fell in one interval become the value at its grid
/// point, and what an interval with none in it gets.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ResampleMethod {
    /// The last reading in the interval. An empty interval emits nothing.
    Last,
    /// The mean of the readings in the interval. An empty interval emits
    /// nothing.
    Mean,
    /// The value at the grid point by a straight line between the last
    /// reading before it and the first after — so a grid point is emitted
    /// once the reading after it has arrived. Empty intervals in between are
    /// filled by the same line.
    Linear,
    /// The last reading seen, carried forward: an empty interval repeats the
    /// last value, for up to `max_gap_seconds`, and then stops. The one method
    /// that emits from a quiet series — which is what makes a sparse
    /// change-on-value signal into a regular one.
    ForwardFill,
}

/// Puts a series onto a regular grid: one message per key per `interval`
/// seconds, at times that are multiples of it, whichever rate the readings
/// arrive at. The precondition every window model has, and the second real
/// user of the run loop's tick — a `forward_fill` series keeps emitting while
/// its readings have gone quiet.
///
/// The message out carries the group fields under their leaf names, the grid
/// time under `time`'s name (or `time` when arrival time is used) as an RFC
/// 3339 string, and the value under `as` (the field's leaf when left out). A
/// grid point is emitted when a reading past it arrives, or — for `forward_fill`
/// only — when the clock passes it with nothing arriving.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[schemars(title = "resample")]
pub struct ResampleTransformConfig {
    /// the numeric field to resample
    #[schemars(extend("x-message-field" = true))]
    pub field: String,
    /// the spacing of the grid, in seconds
    pub interval_seconds: f64,
    /// how the readings in an interval become its value
    pub method: ResampleMethod,
    /// for `forward_fill`: how long a value is carried into empty intervals
    /// before the series is left to go quiet. Carried forever when left out
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_gap_seconds: Option<f64>,
    /// the field the value is written under. The field's leaf when left out
    #[serde(default, rename = "as", skip_serializing_if = "Option::is_none")]
    pub output: Option<String>,
    /// the fields that identify a series, the reducer's way. Leave it out for
    /// one series
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub group_by: Vec<String>,
    /// the field carrying each message's time — RFC 3339 or milliseconds since
    /// the epoch. Leave it out for arrival time, in which case empty intervals
    /// are noticed by the clock rather than by the next reading
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(extend("x-message-field" = true))]
    pub time: Option<String>,
    /// what to do about a message missing the field or a group field
    #[serde(default, skip_serializing_if = "MissingFieldPolicy::is_default")]
    pub on_missing: MissingFieldPolicy,
}
