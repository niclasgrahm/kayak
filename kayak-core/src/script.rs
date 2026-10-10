//! The `script` transform's declaration — what a scripted transform *is*, as
//! against what running one does.
//!
//! Split from the evaluation for the reason [`crate::mapping`] and
//! [`crate::columns`] are: this half has to compile for `wasm32` so the "add
//! pipeline" form can render it, while the half that owns an interpreter, an
//! operation budget and a handle on the state buckets cannot. The evaluator is
//! `kayak::transforms::script`.
//!
//! ## Why a scripting language exists here at all
//!
//! Every other transform answers one question, and the set of questions is
//! closed on purpose — a config file that says what it does is worth more than
//! one that can do anything. Three things stayed out of reach of that set and
//! could not be brought into it without inventing an expression language:
//!
//! - **arrays inside a message.** `splitter` turns one message into many and
//!   `reduce` folds a batch, but nothing walks a list *within* a message. There
//!   is no declarative spelling of "total the line items" whose body isn't
//!   arbitrary code.
//! - **conditionals.** `map` deliberately has none and `filter` can only drop
//!   the whole message, so a severity ladder or a fallback deeper than
//!   `coalesce` has nowhere to live.
//! - **string work.** Parsing a log line, a `k=v` pair or a URL query is a long
//!   tail that a `regex_extract` and a `split` would answer perhaps half of.
//!
//! The boundary is meant to stay legible: `map` remains the right answer for
//! reshaping, and a script that only copies fields is a `map` written the hard
//! way. What this is for is the tail, and the rule when something in that tail
//! shows up three times is that it becomes a real transform rather than a
//! snippet everyone copies.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Runs a [rhai](https://rhai.rs) script over each message, or over the full
/// batch, and sends the values that the script emits.
///
/// The script gets the message as `msg`, and sends values with `emit(value)`.
/// Call `emit` zero times to drop the message, one time to replace it, or more
/// times to split it. Use a script when `filter`, `map` and `splitter` are not
/// sufficient.
///
/// kayak **compiles the script when it builds the pipeline**. Thus, a syntax
/// error stops the pipeline from starting. An error that occurs only with a
/// message fails that batch. For example, a missing field or a value that does
/// not convert fails the batch.
///
/// Each run of the script has an **operation limit** (`max_operations`). When
/// the script reaches the limit, kayak stops it and fails the batch. Thus, a
/// script with an endless loop cannot block the pipeline.
///
/// A script can **`import`** other rhai files. Give a literal path relative to
/// the directory of the config file. The path must stay in that directory. You
/// can leave out the `.rhai` extension. kayak reads the imports when it builds
/// the pipeline. Thus, a bad import stops the pipeline from starting, and a
/// running script does not read the filesystem.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[schemars(title = "script")]
pub struct ScriptTransformConfig {
    /// The script: inline in the config, or in a file beside the config.
    pub source: ScriptSource,
    /// Whether the script gets one message at a time or the full batch. The
    /// default is `message`.
    #[serde(default, skip_serializing_if = "ScriptScope::is_default")]
    pub scope: ScriptScope,
    /// The maximum number of rhai operations in one run of the script. At the
    /// limit, kayak stops the script and fails the batch. The default is
    /// 100000. Increase it for a script that walks a large array.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_operations: Option<u64>,
}

/// The source of the script text: `inline` or `file`.
///
/// Use `inline` to send a script through the HTTP API. In a YAML config, an
/// inline script is a literal block. Use `file` to keep the script in its own
/// file, where an editor, a formatter and a test can use it. A save keeps the
/// `file` reference in the config. It does not copy the script into the
/// config.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ScriptSource {
    /// The script text, in the config. Use a YAML config for an inline script.
    /// YAML keeps the script as a literal block. JSON must escape each newline.
    Inline {
        /// The rhai source.
        #[schemars(extend("x-script" = "rhai"))]
        code: String,
    },
    /// A path to a `.rhai` file, relative to the directory of the config file.
    ///
    /// kayak reads the file and its imports when it builds the pipeline. After
    /// you change the file, do a revert to use the change. A server with no
    /// config file does not accept a `file` source. Inline scripts work on that
    /// server, but their imports do not.
    File {
        /// The path, relative to the directory of the config file. The path
        /// must stay in that directory.
        path: String,
    },
}

/// Whether the script gets one message or the full batch.
///
/// `message` is the default. The operation limit then applies to each message,
/// and the batch keeps its structure.
///
/// Use `batch` for work on the full batch. For example, remove duplicates in
/// the batch, or calculate a value that `reduce` has no function for.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ScriptScope {
    /// The script runs one time for each message, with the message in `msg`.
    #[default]
    Message,
    /// The script runs one time for each batch, with the messages in `batch`
    /// as an array. When you emit an array, the transform sends a batch of
    /// those messages.
    Batch,
}

impl ScriptScope {
    /// So the default doesn't have to appear in a saved config — the same rule
    /// every other optional field here follows.
    #[must_use]
    pub fn is_default(&self) -> bool {
        *self == Self::Message
    }
}

// ── what a running script was built from ────────────────────────────────────

/// The text that kayak compiled a running `script` transform from, and the
/// modules that it imported.
///
/// This is the text from the build of the pipeline. If a file changed after
/// the build, `changed_on_disk` is true.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct LoadedScript {
    /// The file of the script, relative to the directory of the config file.
    /// Absent for an inline script.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    /// Whether the script runs for each message or for each batch.
    pub scope: ScriptScope,
    /// The rhai source that the transform compiled.
    pub code: String,
    /// All modules that the script imported, directly or through another
    /// module, in the order of the first import. Empty for a script with no
    /// imports.
    #[serde(default)]
    pub modules: Vec<LoadedModule>,
    /// True when the file at `path` is different from `code`, or cannot be
    /// read. This means that the file changed after the build. Always false for
    /// an inline script. A revert uses the change.
    #[serde(default)]
    pub changed_on_disk: bool,
}

/// One module that a script imported, as it was when kayak built the
/// pipeline.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct LoadedModule {
    /// The file of the module, relative to the directory of the config file,
    /// with the `.rhai` extension.
    pub path: String,
    /// The rhai source of the module.
    pub code: String,
    /// True when the file is different from `code`, or cannot be read.
    #[serde(default)]
    pub changed_on_disk: bool,
}

// ── what a script is given ──────────────────────────────────────────────────

/// Whether a name is something a script *calls* or something it is *handed*.
///
/// The distinction is the one the editor draws in its reference panel, and it
/// is the only thing about a builtin that changes how it is offered: a function
/// completes with its bracket open, a binding completes as the bare word.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BuiltinKind {
    /// A function kayak registered on the engine — `emit`, `recall`, `now`.
    Function,
    /// A value pushed into the script's scope before it runs — `msg`, `batch`.
    Binding,
}

/// One name kayak puts in a script's scope, and what it is for.
///
/// See [`builtins`] for why this list lives here rather than in either of the
/// two crates that read it.
#[derive(Clone, Copy, Debug)]
pub struct Builtin {
    /// The bare name, which is what the highlighter matches and what a
    /// completion is filtered by.
    pub name: &'static str,
    /// How it is written at a call site — `emit(value)`, `msg`. Shown in the
    /// reference panel and in the hint over a name.
    pub signature: &'static str,
    /// Whether it is called or handed over.
    pub kind: BuiltinKind,
    /// The scope it exists in, or `None` for one that exists in both. This is
    /// the only reason a builtin is ever *hidden*: `msg` in a `batch`-scoped
    /// script is not a name that resolves to something else, it is a name that
    /// is not there, and offering it would be the editor telling a lie the
    /// engine then corrects at runtime.
    pub scope: Option<ScriptScope>,
    /// One line, which is what a completion row and a hover hint can hold.
    pub summary: &'static str,
    /// The paragraph under it in the reference panel — the part that says the
    /// thing a signature cannot.
    pub detail: &'static str,
}

impl Builtin {
    /// What accepting a completion puts in the box, and where the caret then
    /// goes — the text, and the offset from its start.
    ///
    /// A function completes with its brackets already open and the caret
    /// between them, because the next thing anyone types is the argument.
    #[must_use]
    pub fn completion(&self) -> (String, usize) {
        match self.kind {
            BuiltinKind::Function => (format!("{}()", self.name), self.name.len() + 1),
            BuiltinKind::Binding => (self.name.to_string(), self.name.len()),
        }
    }

    /// Whether this name exists in a script of that scope.
    #[must_use]
    pub fn in_scope(&self, scope: ScriptScope) -> bool {
        self.scope.is_none_or(|only| only == scope)
    }
}

/// Everything a script can reach that it did not define itself.
///
/// **This is the one declaration of the host surface, and both halves of the
/// product read it.** The editor colours these names apart from ordinary
/// identifiers, offers them as completions, describes them in its reference
/// panel and hints them under the caret; the runner registers them. The two
/// used to be separate lists — a `const HOST: &[&str]` in the frontend beside
/// the `register_fn` calls on the server — and the failure that arrangement has
/// is quiet in the direction that matters: a function added to the engine is
/// simply undiscoverable, so the editor's answer to "what can I call" is wrong
/// with nothing on fire.
///
/// It lives in core because that is the crate both can see, and it is `const`
/// data rather than reflection because there is nothing to reflect over —
/// `Engine::register_fn` records a name and a callable, and reading it back
/// takes rhai's `metadata` feature. `builtins_are_the_functions_the_engine_has`
/// in `kayak::transforms::script::runner` is what keeps the two in step
/// instead, and it fails in **both** directions.
#[must_use]
pub fn builtins() -> &'static [Builtin] {
    &[
        Builtin {
            name: "msg",
            signature: "msg",
            kind: BuiltinKind::Binding,
            scope: Some(ScriptScope::Message),
            summary: "the message of this run",
            detail: "An object map. Index it as rhai indexes a map: `msg.temperature`, \
                     `msg[\"temperature\"]`, `msg.readings[0]`. A change to `msg` has no \
                     effect alone. The transform sends only the values that you `emit`.",
        },
        Builtin {
            name: "batch",
            signature: "batch",
            kind: BuiltinKind::Binding,
            scope: Some(ScriptScope::Batch),
            summary: "all messages of the batch, as an array",
            detail: "Only in `batch` scope, where the script runs one time for the full \
                     batch. `batch.len` is the number of messages.",
        },
        Builtin {
            name: "emit",
            signature: "emit(value)",
            kind: BuiltinKind::Function,
            scope: None,
            summary: "send a value to the rest of the pipeline",
            detail: "Call it zero, one or more times. Zero calls drop the message. One \
                     call replaces it. More calls split it. In `batch` scope, each value \
                     that you emit is a full batch and must be an array: `emit([msg])`, \
                     not `emit(msg)`.",
        },
        Builtin {
            name: "field",
            signature: "field(message, path)",
            kind: BuiltinKind::Function,
            scope: None,
            summary: "read a dotted field path, as the other transforms do",
            detail: "`field(msg, \"sensor.id\")` reads the same paths as `filter`, \
                     `reduce` and `map`. A key that matches exactly has priority over a \
                     path. Use ordinary rhai indexing for most scripts. Use `field` for a \
                     path that the script makes at runtime, or for a key with dots that an \
                     `envelope` writes. A missing path gives `()`.",
        },
        Builtin {
            name: "recall",
            signature: "recall(key)",
            kind: BuiltinKind::Function,
            scope: None,
            summary: "read the values stored under a key",
            detail: "An object map of the values stored under the key, or `()` when there \
                     are none. Thus, `if recall(k) == ()` is a warm-up check. The pipeline \
                     must declare a `state` bucket. Without a bucket, the call fails with \
                     an error.",
        },
        Builtin {
            name: "remember",
            signature: "remember(key, values)",
            kind: BuiltinKind::Function,
            scope: None,
            summary: "write values into the state bucket of the pipeline",
            detail: "`remember(msg.machine, #{ recipe: msg.recipe })`. The bucket has a \
                     key limit, and entries expire. You declare both in the bucket. The \
                     pipeline must declare a `state` bucket, as for `recall`.",
        },
        Builtin {
            name: "now",
            signature: "now()",
            kind: BuiltinKind::Function,
            scope: None,
            summary: "the current time, as RFC 3339",
            detail: "A string. Use it to write the time into a field of a message.",
        },
        Builtin {
            name: "now_millis",
            signature: "now_millis()",
            kind: BuiltinKind::Function,
            scope: None,
            summary: "the current time, in milliseconds since the epoch",
            detail: "A number. Use it for arithmetic on times.",
        },
        Builtin {
            name: "parse_time",
            signature: "parse_time(value)",
            kind: BuiltinKind::Function,
            scope: None,
            summary: "a time, as milliseconds since the epoch",
            detail: "Reads an RFC 3339 string or a number of milliseconds, the formats \
                     that `now()` and `now_millis()` write. Other values fail the message, \
                     and the error names the value. `()` gives `()`, so a missing field \
                     stays missing.",
        },
        Builtin {
            name: "format_time",
            signature: "format_time(millis)",
            kind: BuiltinKind::Function,
            scope: None,
            summary: "milliseconds since the epoch, as an RFC 3339 string",
            detail: "In UTC, with milliseconds. `parse_time` reads the result back \
                     exactly.",
        },
        Builtin {
            name: "pluck",
            signature: "pluck(batch, path)",
            kind: BuiltinKind::Function,
            scope: None,
            summary: "one field from an array of messages, as an array",
            detail: "Gets the numbers from a batch: `pluck(batch, \"value\")`. It leaves \
                     out messages that do not have the field, or that have `null`. Thus, \
                     `pluck(batch, p).len() == batch.len()` checks that no message left \
                     out the field. The path is a dotted path.",
        },
        Builtin {
            name: "sum",
            signature: "sum(array)",
            kind: BuiltinKind::Function,
            scope: None,
            summary: "the total of an array of numbers",
            detail: "`0.0` for an empty array. All the statistics functions take an array \
                     of numbers. A value that is not a number fails the message. `pluck` \
                     already left out the missing values.",
        },
        Builtin {
            name: "mean",
            signature: "mean(array)",
            kind: BuiltinKind::Function,
            scope: None,
            summary: "the arithmetic mean, or `()` for an empty array",
            detail: "A statistics function with no result for its input gives `()`, not \
                     NaN. Thus, `if mean(v) == ()` is a warm-up check.",
        },
        Builtin {
            name: "median",
            signature: "median(array)",
            kind: BuiltinKind::Function,
            scope: None,
            summary: "the middle value, or the mean of the two middle values",
            detail: "`()` for an empty array. One extreme value cannot move the median.",
        },
        Builtin {
            name: "min",
            signature: "min(array)",
            kind: BuiltinKind::Function,
            scope: None,
            summary: "the smallest number in an array",
            detail: "`()` for an empty array. The rhai function `min(a, b)` for two \
                     numbers is also available.",
        },
        Builtin {
            name: "max",
            signature: "max(array)",
            kind: BuiltinKind::Function,
            scope: None,
            summary: "the largest number in an array",
            detail: "`()` for an empty array. The rhai function `max(a, b)` for two \
                     numbers is also available.",
        },
        Builtin {
            name: "std",
            signature: "std(array)",
            kind: BuiltinKind::Function,
            scope: None,
            summary: "the population standard deviation",
            detail: "The population standard deviation, not the sample standard deviation, \
                     as for `stddev` in `reduce`. `()` for an empty array.",
        },
        Builtin {
            name: "variance",
            signature: "variance(array)",
            kind: BuiltinKind::Function,
            scope: None,
            summary: "the population variance",
            detail: "The square of `std`. `()` for an empty array. The name is not `var`, \
                     because rhai reserves `var`.",
        },
        Builtin {
            name: "quantile",
            signature: "quantile(array, q)",
            kind: BuiltinKind::Function,
            scope: None,
            summary: "the value at fraction q of the sorted numbers",
            detail: "`quantile(v, 0.95)`. It interpolates linearly between the two nearest \
                     values. `()` for an empty array, or for a `q` outside 0 to 1.",
        },
        Builtin {
            name: "mad",
            signature: "mad(array)",
            kind: BuiltinKind::Function,
            scope: None,
            summary: "the median absolute deviation from the median",
            detail: "One extreme value cannot move it. Use it as the scale of a robust \
                     detector. The value is raw, not scaled by 1.4826. `()` for an empty \
                     array.",
        },
        Builtin {
            name: "zscore",
            signature: "zscore(array)",
            kind: BuiltinKind::Function,
            scope: None,
            summary: "each value in standard deviations from the mean, as an array",
            detail: "`()` for an empty array. `()` also for a series with no spread, where \
                     each value is the mean.",
        },
        Builtin {
            name: "skew",
            signature: "skew(array)",
            kind: BuiltinKind::Function,
            scope: None,
            summary: "the direction of the longer tail: positive is to the right",
            detail: "The population skewness. `()` for fewer than two values or for no \
                     spread.",
        },
        Builtin {
            name: "kurtosis",
            signature: "kurtosis(array)",
            kind: BuiltinKind::Function,
            scope: None,
            summary: "the weight of the tails: 0 is normal, positive is heavier",
            detail: "The excess kurtosis, so a normal distribution gives 0. `()` for fewer \
                     than two values or for no spread.",
        },
        Builtin {
            name: "rms",
            signature: "rms(array)",
            kind: BuiltinKind::Function,
            scope: None,
            summary: "the root mean square",
            detail: "The magnitude of a signal that goes above and below zero, for example \
                     vibration or current. A mean of such a signal is near zero. `()` for \
                     an empty array.",
        },
        Builtin {
            name: "diff",
            signature: "diff(array)",
            kind: BuiltinKind::Function,
            scope: None,
            summary: "each value minus the value before it",
            detail: "The result has one value less than the input. For one value or none, \
                     the result is empty.",
        },
        Builtin {
            name: "cumsum",
            signature: "cumsum(array)",
            kind: BuiltinKind::Function,
            scope: None,
            summary: "the running total, as an array of the same length",
            detail: "Empty for an empty array.",
        },
        Builtin {
            name: "ewma",
            signature: "ewma(array, alpha)",
            kind: BuiltinKind::Function,
            scope: None,
            summary: "an exponentially weighted moving average, as an array",
            detail: "Starts with the first value. Each output is `alpha * x + (1 - alpha) \
                     * previous`, so a larger `alpha` follows the data more closely. `()` \
                     for an `alpha` outside 0 to 1.",
        },
        Builtin {
            name: "linfit",
            signature: "linfit(array)",
            kind: BuiltinKind::Function,
            scope: None,
            summary: "the least-squares line: `#{slope, intercept, r2}`",
            detail: "Fits against the position (`x` is 0, 1, 2 …), so the slope is per \
                     message. `linfit(xs, ys)` fits against your own `x`. For a slope per \
                     second, use `linfit(pluck(batch, \"t\"), pluck(batch, \"value\"))`. \
                     `()` for fewer than two points, or when all `x` values are the same.",
        },
        Builtin {
            name: "autocorr",
            signature: "autocorr(array, lag)",
            kind: BuiltinKind::Function,
            scope: None,
            summary: "the correlation of each value with the value lag steps before it, −1 \
                      to 1",
            detail: "A value near 1 at a lag shows a period of that length. `()` when the \
                     lag leaves fewer than two pairs, or when the series has no spread.",
        },
        Builtin {
            name: "peaks",
            signature: "peaks(array)",
            kind: BuiltinKind::Function,
            scope: None,
            summary: "the positions of the local maxima",
            detail: "Each value that is larger than both neighbors. A flat top is not a \
                     peak, and the two ends are never peaks. Empty when there are no \
                     peaks.",
        },
        Builtin {
            name: "histogram",
            signature: "histogram(array, bins)",
            kind: BuiltinKind::Function,
            scope: None,
            summary: "counts in buckets of equal width: `#{edges, counts}`",
            detail: "`bins` buckets from the smallest value to the largest value. The \
                     largest value goes in the last bucket. `edges` has one more value \
                     than `counts`. `()` for an empty array.",
        },
        Builtin {
            name: "clamp",
            signature: "clamp(x, low, high)",
            kind: BuiltinKind::Function,
            scope: None,
            summary: "x limited to the range low to high",
            detail: "An integer stays an integer, and a float stays a float.",
        },
        Builtin {
            name: "interp",
            signature: "interp(xs, ys, x)",
            kind: BuiltinKind::Function,
            scope: None,
            summary: "y at x, by straight lines between known points",
            detail: "`xs` must be in ascending order. Outside the range, it gives the \
                     value of the nearest end. It does not extrapolate. Use it for a \
                     calibration curve or a lookup table. `()` for no points.",
        },
        Builtin {
            name: "dtw",
            signature: "dtw(a, b)",
            kind: BuiltinKind::Function,
            scope: None,
            summary: "the dynamic time warping distance between two series",
            detail: "The difference between two series when one series can go faster or \
                     slower than the other. Thus, two cycles of the same shape at \
                     different speeds are close. Compare a cycle against a reference cycle \
                     that you remember. `()` when one series is empty.",
        },
        Builtin {
            name: "warn",
            signature: "warn(text)",
            kind: BuiltinKind::Function,
            scope: None,
            summary: "write a line to the server log, and do not fail the batch",
            detail: "The transform writes each different text only, and a limited number \
                     of texts per run. Use it when the data has an unexpected shape. To \
                     fail the batch, use `throw`.",
        },
    ]
}

/// The builtin by that exact name, whatever scope it belongs to.
#[must_use]
pub fn builtin(name: &str) -> Option<&'static Builtin> {
    builtins().iter().find(|builtin| builtin.name == name)
}

// ── the dry run ─────────────────────────────────────────────────────────────

/// The body of `POST /api/scripts/dry-run`.
///
/// The dry run uses the same runner as the `script` transform, with the same
/// settings.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct DryRunRequest {
    /// The script to run, inline or in a file, as in the transform.
    pub source: ScriptSource,
    /// Whether the script gets one message at a time or the full batch. The
    /// default is `message`.
    #[serde(default)]
    pub scope: ScriptScope,
    /// The operation limit for this run. The default is the same as for the
    /// transform.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_operations: Option<u64>,
    /// The messages to run the script over, as one batch. An empty list is
    /// permitted. Use it to check that the script compiles.
    #[serde(default)]
    pub messages: Vec<serde_json::Value>,
    /// The initial contents of the state bucket of the run, by the key that a
    /// script uses with `recall`.
    ///
    /// A dry run **never uses a live bucket**. It gets a private bucket with
    /// these contents. kayak discards the bucket after the run.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub state: std::collections::BTreeMap<String, std::collections::BTreeMap<String, serde_json::Value>>,
}

/// The response of a script dry run. The `outcome` field selects the variant.
///
/// A script that does not compile gives a **200 with the `failed` outcome**,
/// not a 400.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum DryRunResponse {
    /// The script ran. This includes a script that emitted nothing.
    Emitted {
        /// The batches that the script emitted, in order. In `message` scope,
        /// there is a maximum of one. In `batch` scope, there is one for each
        /// `emit`.
        batches: Vec<Vec<serde_json::Value>>,
        /// The different texts that the script gave to `warn()`.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        warnings: Vec<String>,
        /// The contents of the private bucket after the run. kayak discards
        /// the bucket after the response.
        #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
        state: std::collections::BTreeMap<String, std::collections::BTreeMap<String, serde_json::Value>>,
    },
    /// The script did not compile, or a run of it failed.
    Failed {
        /// Whether the failure occurred when the script compiled or when it
        /// ran.
        stage: DryRunStage,
        /// The error message from rhai, without the position. The position is
        /// in `line` and `column`.
        message: String,
        /// The line, from 1. Absent when the failure has no line, for example
        /// when the operation limit is reached, or when a built-in function
        /// gives an error.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        line: Option<usize>,
        /// The column, from 1. Absent when `line` is absent.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        column: Option<usize>,
    },
}

/// When a script failure occurred.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DryRunStage {
    /// The script does not parse. A pipeline with this script does not start.
    Compile,
    /// The script parsed, and this run of it failed. A pipeline with this
    /// script starts, and fails the batches that cause the error.
    Runtime,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Both the reference panel and the hover hint render every field of a
    /// builtin, so a blank one is a gap on screen rather than a missing test
    /// fixture. Cheap to keep, and the only thing standing between a hurried
    /// addition and an empty row.
    #[test]
    fn every_builtin_says_what_it_is() {
        for builtin in builtins() {
            assert!(!builtin.name.is_empty(), "a builtin with no name");
            assert!(
                builtin.signature.starts_with(builtin.name),
                "{}'s signature should start with its name: {}",
                builtin.name,
                builtin.signature
            );
            assert!(!builtin.summary.is_empty(), "{} has no summary", builtin.name);
            assert!(!builtin.detail.is_empty(), "{} has no detail", builtin.name);
        }
    }

    #[test]
    fn names_are_unique_and_findable() {
        let mut names: Vec<&str> = builtins().iter().map(|b| b.name).collect();
        let count = names.len();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), count, "two builtins share a name");
        for name in names {
            assert!(builtin(name).is_some(), "{name} is not findable by name");
        }
        assert!(builtin("not_a_builtin").is_none());
    }

    /// The editor hides a binding that belongs to the other scope, so the
    /// scoping has to be exactly the runner's: `msg` exists per message and
    /// `batch` exists per batch, and every function exists in both.
    #[test]
    fn the_bindings_are_scoped_and_the_functions_are_not() {
        let msg = builtin("msg").expect("msg");
        assert!(msg.in_scope(ScriptScope::Message));
        assert!(!msg.in_scope(ScriptScope::Batch));

        let batch = builtin("batch").expect("batch");
        assert!(batch.in_scope(ScriptScope::Batch));
        assert!(!batch.in_scope(ScriptScope::Message));

        for builtin in builtins().iter().filter(|b| b.kind == BuiltinKind::Function) {
            assert!(
                builtin.in_scope(ScriptScope::Message) && builtin.in_scope(ScriptScope::Batch),
                "{} should exist in both scopes",
                builtin.name
            );
        }
    }

    /// Accepting a function completion has to leave the caret *inside* the
    /// brackets — a completion that puts it after them means deleting two
    /// characters before typing the argument, which is worse than typing the
    /// name out.
    #[test]
    fn a_function_completes_with_its_brackets_open() {
        let (text, caret) = builtin("emit").expect("emit").completion();
        assert_eq!(text, "emit()");
        assert_eq!(&text[..caret], "emit(");

        let (text, caret) = builtin("msg").expect("msg").completion();
        assert_eq!(text, "msg");
        assert_eq!(caret, text.len());
    }
}
