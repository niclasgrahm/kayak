//! The streaming statistics transforms — the declarations.
//!
//! Transforms that each keep a little state *per key* and do arithmetic over
//! it as messages arrive: `deadband`, `throttle`, `derive`, `rolling`,
//! `smooth`, `detect` and `resample`. They share one shape, and the shape is the point:
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

use crate::config::{Aggregation, Condition, MissingFieldPolicy};

#[allow(clippy::trivially_copy_pass_by_ref)] // serde's `skip_serializing_if` hands a reference
fn is_false(value: &bool) -> bool {
    !*value
}

// ── the gate every one of them takes ───────────────────────────────────────

/// Selects the messages that a keyed transform applies, and the messages that
/// start a series again. The fields `when` and `reset_when` sit beside
/// `group_by` in the config.
///
/// Use `when` when the stream carries more than one type of message. Without
/// it, a text reading in a numeric stream is an error. A message that does not
/// match `when` passes through unchanged and does not change the state. For
/// `resample`, such a message is not part of the series.
///
/// The transform checks `reset_when` first, independently of `when`. A message
/// that matches `reset_when` clears the state of its key. The series starts
/// again from the next message that the transform applies. If this message
/// also matches `when`, the series starts with this message. Use `reset_when`
/// when the measured item changes, for example when a machine stops.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct Gate {
    /// The conditions that a message must match to be applied. All of them
    /// must match. Other messages pass through unchanged. Leave it out to
    /// apply every message.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub when: Vec<Condition>,
    /// The conditions that clear the state of the message's key. All of them
    /// must match. The series then starts again. The transform checks these
    /// before `when`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reset_when: Vec<Condition>,
}

// ── deadband ────────────────────────────────────────────────────────────────

/// The unit of the `delta` of a deadband: an amount, or a percentage of the
/// last value that passed.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DeadbandMode {
    /// `delta` is in the units of the field. For example, `0.5` is half a
    /// degree.
    #[default]
    Absolute,
    /// `delta` is a percentage of the last value that passed. For example,
    /// `2` is two percent. When the last value is zero, every message passes.
    Percent,
}

/// Drops a message when its field did not change sufficiently since the last
/// message that passed. This is a filter that keeps state per key.
///
/// The first message for each key always passes. After that, a message passes
/// in two cases:
///
/// - `field` differs from the last value that passed by more than `delta`.
/// - `max_seconds` went by since the last message passed.
///
/// With `flatline_seconds`, the transform also finds a stuck sensor. When the
/// value does not change for that time, the next message passes with
/// `stuck: true`. This occurs one time for each flat period.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[schemars(title = "deadband")]
pub struct DeadbandTransformConfig {
    /// The numeric field to compare.
    #[schemars(extend("x-message-field" = true))]
    pub field: String,
    /// The change that a message needs to pass. The unit is set by `mode`.
    pub delta: f64,
    /// The unit of `delta`. The default is `absolute`.
    #[serde(default, skip_serializing_if = "DeadbandMode::is_default")]
    pub mode: DeadbandMode,
    /// Pass a message when this number of seconds went by since the last
    /// message passed, also if the value did not change.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_seconds: Option<f64>,
    /// After this number of seconds with no change, pass the next message with
    /// `stuck: true`. This occurs one time for each flat period.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub flatline_seconds: Option<f64>,
    /// The fields that identify a series, as in `reduce`. Leave it out for
    /// one series.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub group_by: Vec<String>,
    /// The field that holds the time of each message, as an RFC 3339 string
    /// or as milliseconds since the epoch. Leave it out to use the arrival
    /// time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(extend("x-message-field" = true))]
    pub time: Option<String>,
    /// What to do with a message that does not have the field or a group
    /// field. The default is `error`.
    #[serde(default, skip_serializing_if = "MissingFieldPolicy::is_default")]
    pub on_missing: MissingFieldPolicy,
    /// The messages to apply, and the messages that start the series again.
    #[serde(flatten)]
    pub gate: Gate,
}

impl DeadbandMode {
    #[must_use]
    pub fn is_default(&self) -> bool {
        matches!(self, Self::Absolute)
    }
}

// ── throttle ────────────────────────────────────────────────────────────────

/// Passes a maximum of one message per key in each period of `seconds`, and
/// drops the other messages.
///
/// The first message for each key passes. The next message that passes is the
/// first one that arrives `seconds` or more after it. The transform drops all
/// messages between them. The period starts at the message that passed. It is
/// not aligned to the clock.
///
/// The transform does not keep messages to send later. A key that becomes
/// quiet sends nothing until its next message. Use `resample` when you need the
/// last value of each period, or a value from a quiet key.
///
/// `throttle` does not read a value. It passes complete messages with all
/// their fields.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[schemars(title = "throttle")]
pub struct ThrottleTransformConfig {
    /// The minimum time between two messages that pass for one key, in
    /// seconds.
    pub seconds: f64,
    /// The fields that identify a series, as in `reduce`. Leave it out for
    /// one series.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub group_by: Vec<String>,
    /// The field that holds the time of each message, as an RFC 3339 string
    /// or as milliseconds since the epoch. Leave it out to use the arrival
    /// time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(extend("x-message-field" = true))]
    pub time: Option<String>,
    /// What to do with a message that does not have a group field. The
    /// default is `error`.
    #[serde(default, skip_serializing_if = "MissingFieldPolicy::is_default")]
    pub on_missing: MissingFieldPolicy,
    /// The messages to apply, and the messages that start the series again.
    #[serde(flatten)]
    pub gate: Gate,
}

// ── pivot ───────────────────────────────────────────────────────────────────

/// Changes a stream with one reading per message into rows. The transform
/// keeps the latest value of each name in `names` for each key. It writes all
/// of these values onto every message.
///
/// For example, the input is `{"sensor": "state", "value": "RUNNING"}` and
/// then `{"sensor": "fault", "value": "NONE"}`. The output row is
/// `{"state": "RUNNING", "fault": "NONE", ...}`.
///
/// When the `name` field of a message holds one of `names`, the transform
/// first records that reading. Then it writes every value that it keeps for
/// the key onto the message. It writes them at the top level, or under
/// `into`. A name with no reading yet for the key is not written. One message
/// goes in and one message comes out.
///
/// `names` is required because it limits the state. A message with a name that
/// is not in `names` adds nothing, but it gets the row.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[schemars(title = "pivot")]
pub struct PivotTransformConfig {
    /// The field that holds the name of the reading. kayak compares its value
    /// with `names`.
    #[schemars(extend("x-message-field" = true))]
    pub name: String,
    /// The field that holds the reading. The value can be any JSON value, for
    /// example a string or a number.
    #[schemars(extend("x-message-field" = true))]
    pub value: String,
    /// The names to keep and write. Each name becomes a field. Give one name
    /// or more.
    pub names: Vec<String>,
    /// An object field to write the values under. Leave it out to write them
    /// at the top level.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub into: Option<String>,
    /// The fields that identify a row, as in `reduce`. Leave it out for one
    /// row.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub group_by: Vec<String>,
    /// What to do with a message that does not have a group field, or that
    /// has a name from `names` and no `value`. The default is `error`.
    #[serde(default, skip_serializing_if = "MissingFieldPolicy::is_default")]
    pub on_missing: MissingFieldPolicy,
    /// The messages to apply, and the messages that start the row again.
    #[serde(flatten)]
    pub gate: Gate,
}

// ── derive ──────────────────────────────────────────────────────────────────

/// How the transform calculates a value from this message and the previous
/// message.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DeriveFnKind {
    /// The change per second since the previous message, from the `time`
    /// field. The value is `null` for the first message and when no time went
    /// by.
    Rate,
    /// The change since the previous message. The value is `null` for the
    /// first message.
    Delta,
    /// The running total of the field, from the first message.
    Cumsum,
    /// The running total of the increases. Use it for a counter that resets
    /// or wraps. When the value decreases and `wrap_at` is set, the transform
    /// adds the increase through `wrap_at`. When `wrap_at` is not set, it adds
    /// the new value.
    Counter,
}

/// One derived value and the field it is written to.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[schemars(title = "derivation")]
pub struct Derivation {
    /// The calculation to do.
    pub function: DeriveFnKind,
    /// The numeric field to read.
    #[schemars(extend("x-message-field" = true))]
    pub field: String,
    /// The field to write the result to.
    #[serde(rename = "as")]
    pub output: String,
    /// For `counter`: the value at which the counter goes back to zero. With
    /// it, the transform reads a decrease as a wrap. Without it, the
    /// transform reads a decrease as a reset.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wrap_at: Option<f64>,
}

/// Writes a value onto each message that needs the previous message of the
/// same key. The value can be a rate of change, a delta, a running total or a
/// counter that wraps.
///
/// You can give many derivations. The transform writes each one to its own
/// `as` field. The first message for each key has no previous message. For
/// that message, `rate` and `delta` write `null`.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[schemars(title = "derive")]
pub struct DeriveTransformConfig {
    /// The values to calculate. Give one or more, each with a different
    /// `as`.
    pub derive: Vec<Derivation>,
    /// The fields that identify a series, as in `reduce`. Leave it out for
    /// one series.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub group_by: Vec<String>,
    /// The field that holds the time of each message, as an RFC 3339 string
    /// or as milliseconds since the epoch. Leave it out to use the arrival
    /// time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(extend("x-message-field" = true))]
    pub time: Option<String>,
    /// What to do with a message that does not have a derived field or a
    /// group field. The default is `error`.
    #[serde(default, skip_serializing_if = "MissingFieldPolicy::is_default")]
    pub on_missing: MissingFieldPolicy,
    /// The messages to apply, and the messages that start the series again.
    #[serde(flatten)]
    pub gate: Gate,
}

// ── rolling ─────────────────────────────────────────────────────────────────

/// Writes aggregations over the last messages of the series onto each
/// message. The aggregations use the same `{function, field, as}` list and
/// functions as `reduce`. One message goes in and one message comes out.
///
/// The window holds a maximum of `size` messages. `size` is always required.
/// With `seconds`, the window also drops messages that are older than that
/// time, from the `time` field.
///
/// Here, `count` needs a `field`. It counts the messages in the window that
/// have the field. When the window is full, the count is `size`.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[schemars(title = "rolling")]
pub struct RollingTransformConfig {
    /// The aggregations to calculate over the window. Give one or more, each
    /// with a different `as`.
    pub aggregations: Vec<Aggregation>,
    /// The maximum number of messages in the window.
    pub size: usize,
    /// Also drop the messages that are older than this number of seconds,
    /// from the `time` field.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seconds: Option<f64>,
    /// The fields that identify a series, as in `reduce`. Leave it out for
    /// one series.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub group_by: Vec<String>,
    /// The field that holds the time of each message, as an RFC 3339 string
    /// or as milliseconds since the epoch. Leave it out to use the arrival
    /// time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(extend("x-message-field" = true))]
    pub time: Option<String>,
    /// What to do with a message that does not have an aggregated field or a
    /// group field. The default is `error`.
    #[serde(default, skip_serializing_if = "MissingFieldPolicy::is_default")]
    pub on_missing: MissingFieldPolicy,
    /// The messages to apply, and the messages that start the series again.
    #[serde(flatten)]
    pub gate: Gate,
}

// ── smooth ──────────────────────────────────────────────────────────────────

/// How the transform smooths a value against the values before it.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SmoothMethod {
    /// An exponentially weighted moving average. It needs no window. Give
    /// exactly one of `alpha`, `half_life` or `tau_seconds`.
    ///
    /// `alpha` and `half_life` count messages. Use them when messages arrive
    /// at a steady rate. `tau_seconds` counts time. The weight of a value is
    /// `1 − e^(−Δt/τ)`, where Δt is the time since the previous value. Thus, a
    /// reading after a long gap has more weight. Use `tau_seconds` for a sensor
    /// that reports on change, or for a stream that stops for periods.
    Ewma {
        /// The weight of the newest value, from 0 to 1.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        alpha: Option<f64>,
        /// The number of messages after which the weight of a value is half.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        half_life: Option<f64>,
        /// The time constant in seconds. After this time, the weight of an old
        /// value is about 37%. It reads the `time` field of the transform.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        tau_seconds: Option<f64>,
    },
    /// The median of the last `size` values, with this value included. It
    /// removes a spike of one sample.
    Median {
        /// The number of values in the window.
        size: usize,
    },
    /// A Hampel filter. When the value is more than `threshold` robust
    /// standard deviations from the median of the window, the median replaces
    /// it. Otherwise the value stays. Use it before `detect` to remove
    /// outliers.
    Hampel {
        /// The number of values in the window, with this value included.
        size: usize,
        /// The number of scaled MADs from the median that makes an outlier.
        /// The default is `3`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        threshold: Option<f64>,
    },
    /// A Savitzky–Golay filter. It fits a polynomial of degree `order` to the
    /// last `size` values by least squares, and gives its value at the newest
    /// point. It keeps the shape of peaks. The window uses only earlier
    /// values. Until the window holds more than `order` values, the value
    /// passes unchanged.
    SavitzkyGolay {
        /// The number of values in the window, with this value included.
        size: usize,
        /// The degree of the polynomial. It must be less than `size`. The
        /// default is `2`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        order: Option<usize>,
    },
}

/// Smooths a numeric field against the earlier values of its series. The
/// transform writes the result into the field, or under `as`.
///
/// All methods except `ewma` with `tau_seconds` use the order of the values,
/// and not their time. Thus, `time` is permitted only with `ewma` and
/// `tau_seconds`. With other methods, `time` is an error when the pipeline
/// builds.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[schemars(title = "smooth")]
pub struct SmoothTransformConfig {
    /// The numeric field to smooth.
    #[schemars(extend("x-message-field" = true))]
    pub field: String,
    /// The method to smooth the value with.
    pub method: SmoothMethod,
    /// The field to write the smoothed value to. Leave it out to replace the
    /// value of `field`.
    #[serde(default, rename = "as", skip_serializing_if = "Option::is_none")]
    pub output: Option<String>,
    /// The fields that identify a series, as in `reduce`. Leave it out for
    /// one series.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub group_by: Vec<String>,
    /// For `ewma` with `tau_seconds` only: the field that holds the time of
    /// each message, as an RFC 3339 string or as milliseconds since the epoch.
    /// Leave it out to use the arrival time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(extend("x-message-field" = true))]
    pub time: Option<String>,
    /// What to do with a message that does not have the field or a group
    /// field. The default is `error`.
    #[serde(default, skip_serializing_if = "MissingFieldPolicy::is_default")]
    pub on_missing: MissingFieldPolicy,
    /// The messages to apply, and the messages that start the series again.
    #[serde(flatten)]
    pub gate: Gate,
}

// ── detect ──────────────────────────────────────────────────────────────────

/// How the transform finds an anomaly.
///
/// The window methods (`zscore`, `mad`) compare a value with the values before
/// it. The window does not include the value, so a spike does not change its
/// own baseline. The chart methods (`cusum` without `target`, `ewma_chart`,
/// `western_electric`) fix their baseline at the end of the warm-up.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum DetectMethod {
    /// An anomaly is more than `threshold` standard deviations from the mean
    /// of the last `size` values.
    Zscore {
        /// The number of earlier values in the baseline.
        size: usize,
        /// The number of standard deviations that makes an anomaly. The
        /// default is `3`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        threshold: Option<f64>,
    },
    /// An anomaly is more than `threshold` scaled median absolute deviations
    /// from the median of the last `size` values. Use it when the baseline
    /// contains outliers.
    Mad {
        /// The number of earlier values in the baseline.
        size: usize,
        /// The number of scaled MADs that makes an anomaly. The default is
        /// `3.5`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        threshold: Option<f64>,
    },
    /// A two-sided CUSUM. It adds the drift above `target` and the drift below
    /// `target` in two sums. An anomaly is a sum that is more than
    /// `threshold`. Then that sum goes back to zero. Use it to find a small
    /// shift that continues.
    Cusum {
        /// The expected value of the series. Leave it out to use the mean of
        /// the warm-up.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        target: Option<f64>,
        /// The change per message that does not count as drift, in the units
        /// of the field.
        drift: f64,
        /// The sum of drift that makes an anomaly.
        threshold: f64,
    },
    /// An EWMA control chart. An anomaly is a smoothed value outside a band of
    /// `threshold` standard deviations around the mean of the warm-up. It
    /// finds small shifts and ignores single points.
    EwmaChart {
        /// The weight of the newest value, from 0 to 1. The default is `0.2`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        alpha: Option<f64>,
        /// The width of the band, in standard deviations. The default is `3`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        threshold: Option<f64>,
    },
    /// The Western Electric rules, against the mean and deviation of the
    /// warm-up:
    ///
    /// - one point more than 3σ from the mean,
    /// - two of three points more than 2σ from the mean on one side,
    /// - four of five points more than 1σ from the mean on one side,
    /// - eight points in a sequence on one side.
    ///
    /// The transform writes the rule that matched beside the flag.
    WesternElectric {},
    /// An anomaly is `size` identical values in a sequence. Use it to find a
    /// stuck instrument.
    Flatline {
        /// The number of identical values in a sequence that makes an anomaly.
        size: usize,
    },
    /// An anomaly is more than `threshold` deviations from a baseline that
    /// continues to learn. The baseline is an exponentially weighted mean and
    /// spread, each with its own time constant. Use it for a series that
    /// drifts slowly and arrives at irregular times. It reads the `time` field
    /// of the transform.
    Ewma {
        /// The time constant of the mean, in seconds. A smaller value makes the
        /// mean follow the series more quickly.
        mean_tau_seconds: f64,
        /// The time constant of the spread, in seconds. Make it longer than
        /// `mean_tau_seconds`, so that a short period of noise does not make
        /// the band wider immediately.
        spread_tau_seconds: f64,
        /// The number of deviations that makes an anomaly. The default is `3`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        threshold: Option<f64>,
        /// The minimum deviation, in the units of the field. Without it, a
        /// signal that was very quiet flags its first small change. The default
        /// is `0`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        min_spread: Option<f64>,
    },
}

/// The readings that a `detect` baseline learns from, for the methods that
/// continue to learn.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DetectLearn {
    /// All readings, with anomalies included. A change that continues becomes
    /// the new normal at the speed of the baseline.
    #[default]
    All,
    /// Only the readings that are not anomalies. An anomaly cannot change the
    /// baseline. Use it with `readapt_after_seconds`. Without it, the
    /// transform flags a real change of level permanently.
    NormalOnly,
}

impl DetectLearn {
    #[must_use]
    pub fn is_default(&self) -> bool {
        matches!(self, Self::All)
    }
}

/// Which messages the `detect` transform sends.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DetectMode {
    /// Every message passes, with the flag and the score.
    #[default]
    Annotate,
    /// Only the anomalies pass, with the flag and the score.
    OnlyAnomalies,
}

impl DetectMode {
    #[must_use]
    pub fn is_default(&self) -> bool {
        matches!(self, Self::Annotate)
    }
}

/// Flags anomalies in a numeric field against its own series. `method` selects
/// how the transform finds an anomaly.
///
/// The transform writes a boolean under `as`. The default is `anomaly`. It also
/// writes a score under `<as>_score`. The score is the distance from normal, in
/// the units of the method. A `filter` after the transform can use a stricter
/// limit on the score.
///
/// The warm-up is `min_samples` messages for each key. The transform flags
/// nothing during the warm-up.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[schemars(title = "detect")]
pub struct DetectTransformConfig {
    /// The numeric field to examine.
    #[schemars(extend("x-message-field" = true))]
    pub field: String,
    /// How the transform finds an anomaly.
    pub method: DetectMethod,
    /// Which messages to send. The default is `annotate`.
    #[serde(default, skip_serializing_if = "DetectMode::is_default")]
    pub mode: DetectMode,
    /// The number of messages for each key before the transform flags
    /// anything. The default is the `size` of the method, or 30 for a method
    /// with no `size`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_samples: Option<usize>,
    /// The field to write the flag to. The score goes to `<as>_score`. The
    /// default is `anomaly`.
    #[serde(default, rename = "as", skip_serializing_if = "Option::is_none")]
    pub output: Option<String>,
    /// Also write the baseline of the reading, in the units of the field. The
    /// normal value goes to `<as>_expected`. The permitted distance from it
    /// goes to `<as>_band`. The value is `null` when the method has no
    /// baseline.
    #[serde(default, skip_serializing_if = "is_false")]
    pub with_baseline: bool,
    /// For `zscore`, `mad` and `ewma`: the readings that the baseline learns
    /// from. The default is `all`.
    #[serde(default, skip_serializing_if = "DetectLearn::is_default")]
    pub learn: DetectLearn,
    /// With `learn: normal_only`: when readings are anomalies for this number
    /// of seconds without a break, the baseline learns from them. A change
    /// that continues then becomes the new normal.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub readapt_after_seconds: Option<f64>,
    /// The field that holds the time of each message, as an RFC 3339 string
    /// or as milliseconds since the epoch. The `ewma` method and
    /// `readapt_after_seconds` use it. Leave it out to use the arrival time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(extend("x-message-field" = true))]
    pub time: Option<String>,
    /// The fields that identify a series, as in `reduce`. Leave it out for
    /// one series.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub group_by: Vec<String>,
    /// What to do with a message that does not have the field or a group
    /// field. The default is `error`.
    #[serde(default, skip_serializing_if = "MissingFieldPolicy::is_default")]
    pub on_missing: MissingFieldPolicy,
    /// The messages to apply, and the messages that start the series again.
    #[serde(flatten)]
    pub gate: Gate,
}

// ── resample ────────────────────────────────────────────────────────────────

/// How the transform calculates the value at a grid point from the readings in
/// its interval, and what it does with an empty interval.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ResampleMethod {
    /// The last reading in the interval. An empty interval sends nothing.
    Last,
    /// The mean of the readings in the interval. An empty interval sends
    /// nothing.
    Mean,
    /// The value on a straight line between the last reading before the grid
    /// point and the first reading after it. The transform sends the grid
    /// point when the reading after it arrives. The same line fills the empty
    /// intervals between the two readings.
    Linear,
    /// The last reading, carried forward. An empty interval repeats the last
    /// value for a maximum of `max_gap_seconds`, and then stops. This is the
    /// only method that sends values from a quiet series. Use it to change a
    /// signal that reports on change into a regular signal.
    ForwardFill,
}

/// Puts a series onto a regular grid. The transform sends one message for each
/// key in each `interval_seconds`, at times that are multiples of the interval.
/// The rate of the readings has no effect on the grid.
///
/// The message that the transform sends contains:
///
/// - the group fields, under their leaf names,
/// - the grid time as an RFC 3339 string, under the name of the `time` field
///   (or `time` when the transform uses the arrival time),
/// - the value, under `as` (the leaf of `field` when you leave it out).
///
/// The transform sends a grid point when a later reading arrives. With
/// `forward_fill` and the arrival time, it also sends a grid point when the
/// clock passes it.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[schemars(title = "resample")]
pub struct ResampleTransformConfig {
    /// The numeric field to resample.
    #[schemars(extend("x-message-field" = true))]
    pub field: String,
    /// The distance between two grid points, in seconds.
    pub interval_seconds: f64,
    /// How the transform calculates the value of an interval.
    pub method: ResampleMethod,
    /// For `forward_fill`: the maximum time to repeat a value into empty
    /// intervals, in seconds. Leave it out to repeat the value with no limit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_gap_seconds: Option<f64>,
    /// The field to write the value to. The default is the leaf of `field`.
    #[serde(default, rename = "as", skip_serializing_if = "Option::is_none")]
    pub output: Option<String>,
    /// The fields that identify a series, as in `reduce`. Leave it out for
    /// one series.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub group_by: Vec<String>,
    /// The field that holds the time of each message, as an RFC 3339 string
    /// or as milliseconds since the epoch. Leave it out to use the arrival
    /// time. With the arrival time, the clock finds the empty intervals.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(extend("x-message-field" = true))]
    pub time: Option<String>,
    /// What to do with a message that does not have the field or a group
    /// field. The default is `error`.
    #[serde(default, skip_serializing_if = "MissingFieldPolicy::is_default")]
    pub on_missing: MissingFieldPolicy,
    /// The messages to apply, and the messages that start the series again.
    #[serde(flatten)]
    pub gate: Gate,
}

// ── features ────────────────────────────────────────────────────────────────

/// A number that describes a window of readings.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum FeatureKind {
    /// The arithmetic mean.
    Mean,
    /// The population standard deviation.
    Std,
    /// The smallest value.
    Min,
    /// The largest value.
    Max,
    /// The largest value minus the smallest value.
    Range,
    /// The least-squares slope. It is per second with the `time` field, and
    /// per message without it.
    Slope,
    /// The skewness: the direction of the longer tail.
    Skew,
    /// The excess kurtosis: the weight of the tails.
    Kurtosis,
    /// The root mean square.
    Rms,
    /// The peak magnitude divided by the RMS.
    CrestFactor,
    /// The number of times that the signal crosses zero.
    ZeroCrossings,
    /// The number of local maxima.
    NPeaks,
    /// The autocorrelation at lag one.
    Autocorr1,
    /// The strongest frequency above DC, in hertz. It needs a sample rate from
    /// the `time` field or from `sample_rate_hz`.
    DominantFrequency,
    /// The number of readings in the window.
    Count,
    /// The time from the first reading to the last reading, in seconds, from
    /// the `time` field.
    Duration,
}

impl FeatureKind {
    /// The field the feature is written under — the name as it is spelled
    /// in the config.
    #[must_use]
    pub fn field_name(self) -> &'static str {
        match self {
            Self::Mean => "mean",
            Self::Std => "std",
            Self::Min => "min",
            Self::Max => "max",
            Self::Range => "range",
            Self::Slope => "slope",
            Self::Skew => "skew",
            Self::Kurtosis => "kurtosis",
            Self::Rms => "rms",
            Self::CrestFactor => "crest_factor",
            Self::ZeroCrossings => "zero_crossings",
            Self::NPeaks => "n_peaks",
            Self::Autocorr1 => "autocorr_1",
            Self::DominantFrequency => "dominant_frequency",
            Self::Count => "count",
            Self::Duration => "duration",
        }
    }

    /// Whether the feature needs a sample rate.
    #[must_use]
    pub fn is_spectral(self) -> bool {
        matches!(self, Self::DominantFrequency)
    }
}

/// The power in one frequency band, as a feature.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[schemars(title = "band")]
pub struct Band {
    /// The lower limit of the band, in hertz. The band includes this value.
    pub low_hz: f64,
    /// The upper limit of the band, in hertz. The band does not include this
    /// value. It must be more than `low_hz`.
    pub high_hz: f64,
    /// The field to write the power of the band to.
    #[serde(rename = "as")]
    pub output: String,
}

/// Changes a batch of readings into one message of features for each group.
/// Use it to send a small set of numbers to a model, in place of the raw
/// readings. Put a `buffer` on the input. Without it, each batch has only one
/// reading.
///
/// The transform writes:
///
/// - each feature in `include` under its own name, for example `mean`, `rms`
///   or `crest_factor`,
/// - each entry in `bands` under its `as`,
/// - the `group_by` fields under their leaf names, as `reduce` does.
///
/// A feature with no value for the window is `null`. For example, a slope of
/// one point is `null`. The spectral features (`dominant_frequency`, `bands`)
/// need a sample rate. kayak uses `sample_rate_hz`, or calculates the rate
/// from the `time` field. Without one of the two, the pipeline does not build.
/// This transform keeps no state and needs no state bucket.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[schemars(title = "features")]
pub struct FeaturesTransformConfig {
    /// The numeric field to read.
    #[schemars(extend("x-message-field" = true))]
    pub field: String,
    /// The features to calculate. The transform writes each one under its own
    /// name.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub include: Vec<FeatureKind>,
    /// The frequency bands to calculate the power of. The transform writes
    /// each one under its `as`. Give `include`, `bands` or both.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub bands: Vec<Band>,
    /// The fields that identify a series, as in `reduce`. Leave it out to use
    /// the full batch as one window.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub group_by: Vec<String>,
    /// The field that holds the time of each reading, as an RFC 3339 string or
    /// as milliseconds since the epoch. `slope` and `duration` use it for
    /// seconds. The spectral features use it for the sample rate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(extend("x-message-field" = true))]
    pub time: Option<String>,
    /// The sample rate of the readings in hertz, for the spectral features.
    /// It must be more than zero. kayak uses it in place of the rate from
    /// `time`. Use it when the source has timestamps with low resolution.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sample_rate_hz: Option<f64>,
    /// What to do with a reading that does not have the field or a group
    /// field. The default is `error`.
    #[serde(default, skip_serializing_if = "MissingFieldPolicy::is_default")]
    pub on_missing: MissingFieldPolicy,
}
