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

/// Runs a [rhai](https://rhai.rs) script over each message, or over the batch
/// as a whole, and emits whatever the script asks for.
///
/// A script reaches the message as `msg`, and emits with `emit(value)` — zero
/// times to drop it, once to replace it, many times to split it. That covers
/// `filter`, `map` and `splitter` in one, which is the point: what a script is
/// for is the case none of those three reach.
///
/// The script is **compiled when the pipeline is built**, so a syntax error is
/// a pipeline that refuses to start rather than one that fails every batch
/// forever — the same rule the reducer's build-time checks follow. What cannot
/// be checked until a message arrives (a field that isn't there, a type that
/// won't convert) fails that batch and shows up on the card.
///
/// Every script runs under an **operation budget**. That is not a tuning knob
/// with a safe default, it is what makes this component safe to have: the
/// script runs synchronously inside the run loop's task, so a script that loops
/// forever would wedge a worker thread rather than merely breaking its own
/// pipeline.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[schemars(title = "script")]
pub struct ScriptTransformConfig {
    /// the script itself, written inline or kept in a file beside the config
    pub source: ScriptSource,
    /// whether the script sees one message at a time or the whole batch
    #[serde(default, skip_serializing_if = "ScriptScope::is_default")]
    pub scope: ScriptScope,
    /// how many rhai operations one run of the script may take before it is
    /// stopped and the batch failed. Leave it out for the default, which is
    /// generous for anything that isn't looping by mistake; raise it for a
    /// script that legitimately walks a large array.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_operations: Option<u64>,
}

/// Where the script's text comes from.
///
/// Two spellings because the three ways someone writes a pipeline want
/// different things. Inline is what the HTTP API and the UI can carry — a
/// script in a file is a reference the browser cannot edit and a generated
/// config has nowhere to put — and YAML renders it as a literal block, so it
/// reads as code rather than as an escaped string. A file is what an editor can
/// syntax-highlight, a formatter can format and a test can exercise on its own,
/// which is what the file-first workflow wants.
///
/// Inline is the canonical form: a `file` is resolved when the pipeline is
/// built and the config keeps the reference, so saving never inlines someone's
/// file out of existence.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ScriptSource {
    /// The script's text, in the config itself. Prefer a YAML config for this —
    /// a literal block keeps it readable, where JSON has to escape every
    /// newline.
    Inline {
        /// the rhai source
        #[schemars(extend("x-script" = "rhai"))]
        code: String,
    },
    /// A path to a `.rhai` file, relative to the directory the config file is
    /// in — the same place the connections and layout files live.
    ///
    /// The file is read when the pipeline is built, so editing it takes a
    /// revert to pick up. A server running without a config file has no
    /// directory to resolve against and refuses this; inline scripts still
    /// work there.
    File {
        /// the path, relative to the config file's directory. It may not climb
        /// out of that directory.
        path: String,
    },
}

/// Whether a script is handed one message or the whole batch.
///
/// `message` is the default and is what nearly everything wants: the budget is
/// then spent per message rather than per batch, the batch structure is
/// preserved without the script having to rebuild it, and a script that emits
/// nothing for one message has dropped exactly that message.
///
/// `batch` is the escape hatch, and it is needed for the things that are about
/// the batch itself — deduplicating within it, repartitioning it, or computing
/// something across it that `reduce` has no function for.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ScriptScope {
    /// The script runs once per message, with the message in `msg`.
    #[default]
    Message,
    /// The script runs once per batch, with the messages in `batch` as an
    /// array. Emitting an array emits a batch of those messages.
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

// ── the dry run ─────────────────────────────────────────────────────────────

/// What `POST /api/scripts/dry-run` takes.
///
/// The endpoint exists because a script is the one component whose
/// configuration can be *wrong in a way the config's shape cannot express*. For
/// every other component, a config that deserializes and builds is a component
/// that does what it says; for this one, the interesting mistakes are all
/// inside a string. Without somewhere to run it, the only way to find out is to
/// create a pipeline and watch its card — which for the HTTP API means creating
/// a *running* pipeline you then have to tear down.
///
/// Both the transform and this run through the same
/// `kayak::transforms::script::runner`, configured identically. That is not a
/// convenience: a dry run whose agreement with production is a matter of luck
/// is worse than no dry run, because it is trusted.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct DryRunRequest {
    /// the script to run, inline or by reference, exactly as a transform
    /// declares it
    pub source: ScriptSource,
    /// whether the script sees one message at a time or the whole batch
    #[serde(default)]
    pub scope: ScriptScope,
    /// the operation budget for this run. Left out, the same default a
    /// transform gets.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_operations: Option<u64>,
    /// the messages to run it over — one batch. An empty list is allowed and is
    /// how a script is checked for compiling without inventing data for it.
    #[serde(default)]
    pub messages: Vec<serde_json::Value>,
    /// state to seed the run's bucket with, keyed by the key a script would
    /// `recall` it under.
    ///
    /// A dry run **never touches a live bucket**: it gets a private one, seeded
    /// from here and thrown away afterwards. Reading production state would
    /// make a dry run's answer depend on what the server happened to be doing,
    /// and writing it would give a "dry" run side effects — the second being
    /// the one that would be found out late and badly.
    #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
    pub state: std::collections::BTreeMap<String, std::collections::BTreeMap<String, serde_json::Value>>,
}

/// What came back from a dry run.
///
/// A script that does not compile comes back **200 with a `failed` outcome**,
/// not a 400. The request was well formed and the server answered it
/// completely; "this script has a bug on line 3" is the answer, not a failure
/// to produce one. A 400 would conflate a malformed request with a working
/// endpoint reporting what it was asked to find out, and a client would have to
/// tell them apart by reading the body anyway.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "outcome", rename_all = "snake_case")]
pub enum DryRunResponse {
    /// The script ran. Note this includes a script that emitted nothing —
    /// dropping every message is a working filter, not a failure.
    Emitted {
        /// the batches the script emitted, in order. In `message` scope this is
        /// at most one; in `batch` scope it is however many the script asked
        /// for.
        batches: Vec<Vec<serde_json::Value>>,
        /// distinct texts the script passed to `warn()`
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        warnings: Vec<String>,
        /// what the run's private bucket holds afterwards — what the script
        /// would have remembered. Discarded when the response is sent.
        #[serde(default, skip_serializing_if = "std::collections::BTreeMap::is_empty")]
        state: std::collections::BTreeMap<String, std::collections::BTreeMap<String, serde_json::Value>>,
    },
    /// The script did not compile, or a run of it failed.
    Failed {
        /// whether this stopped the script compiling or stopped one run of it.
        /// The first would refuse to start a pipeline; the second would fail a
        /// batch on one that was already running.
        stage: DryRunStage,
        /// what went wrong, in rhai's words and without the position appended —
        /// the position is beside it, as a number a client can use
        message: String,
        /// one-based, as an editor counts. Absent when the failure belongs to
        /// the run rather than to a line — an exhausted budget, or an error
        /// raised by a host function.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        line: Option<usize>,
        /// one-based, beside `line` and absent for the same reasons
        #[serde(default, skip_serializing_if = "Option::is_none")]
        column: Option<usize>,
    },
}

/// Which half of a script's life a failure belongs to.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DryRunStage {
    /// The script does not parse. A pipeline with this script refuses to start.
    Compile,
    /// The script parsed and this run of it failed. A pipeline with this script
    /// starts, and fails the batches that hit it.
    Runtime,
}
