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
///
/// A script may **`import`** other rhai files — shared helpers, written once —
/// by a literal path relative to the config file's directory, which it may not
/// climb out of; the `.rhai` extension is implied. Imports resolve when the
/// pipeline is built, so a broken one refuses to start rather than failing
/// batches, and a running script never touches the filesystem.
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
    /// The file is read when the pipeline is built — as are any modules it
    /// `import`s, which resolve against the same directory — so editing one
    /// takes a revert to pick up. A server running without a config file has
    /// no directory to resolve against and refuses this; inline scripts still
    /// work there, though their imports are refused for the same reason.
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
            summary: "the message this run was handed",
            detail: "An object map, indexed as rhai indexes one: `msg.temperature`, \
                     `msg[\"temperature\"]`, `msg.readings[0]`. Changing it changes nothing on \
                     its own — what leaves the transform is what you `emit`.",
        },
        Builtin {
            name: "batch",
            signature: "batch",
            kind: BuiltinKind::Binding,
            scope: Some(ScriptScope::Batch),
            summary: "every message of the batch, as an array",
            detail: "Only in `batch` scope, where the script runs once for the whole batch \
                     rather than once per message. `batch.len` is how many there are.",
        },
        Builtin {
            name: "emit",
            signature: "emit(value)",
            kind: BuiltinKind::Function,
            scope: None,
            summary: "hand a value on to the rest of the pipeline",
            detail: "Call it none, one or many times: none drops the message, one replaces it, \
                     several split it. In `batch` scope every emitted value is a whole batch and \
                     has to be an array — `emit([msg])`, not `emit(msg)`.",
        },
        Builtin {
            name: "field",
            signature: "field(message, path)",
            kind: BuiltinKind::Function,
            scope: None,
            summary: "read a dotted field path, as every other transform reads one",
            detail: "`field(msg, \"sensor.id\")` — the same paths a `filter`, a `reduce` or a \
                     `map` addresses, including the rule that an exact key beats a path. Ordinary \
                     rhai indexing is what most scripts want; this is for the paths it cannot \
                     spell: ones assembled at runtime, and the literal dotted keys an `envelope` \
                     writes. A path that isn't there reads as `()`.",
        },
        Builtin {
            name: "recall",
            signature: "recall(key)",
            kind: BuiltinKind::Function,
            scope: None,
            summary: "read what was remembered under a key",
            detail: "An object map of the values remembered under that key, or `()` when nothing \
                     has been — so a warm-up check is `if recall(k) == ()`. Needs the pipeline to \
                     declare a `state` bucket; without one the call fails and says so.",
        },
        Builtin {
            name: "remember",
            signature: "remember(key, values)",
            kind: BuiltinKind::Function,
            scope: None,
            summary: "write values into the pipeline's state bucket",
            detail: "`remember(msg.machine, #{ recipe: msg.recipe })`. The bucket is bounded and \
                     entries expire, both declared where the bucket is. Needs a `state` bucket on \
                     the pipeline, like `recall`.",
        },
        Builtin {
            name: "now",
            signature: "now()",
            kind: BuiltinKind::Function,
            scope: None,
            summary: "the current time, RFC 3339",
            detail: "A string rather than a timestamp object, because what a message can carry is \
                     a string and this is nearly always written straight into a field.",
        },
        Builtin {
            name: "now_millis",
            signature: "now_millis()",
            kind: BuiltinKind::Function,
            scope: None,
            summary: "the current time, in milliseconds since the epoch",
            detail: "The number to reach for when the field is arithmetic rather than a label.",
        },
        Builtin {
            name: "warn",
            signature: "warn(text)",
            kind: BuiltinKind::Function,
            scope: None,
            summary: "put a line in the server's log without failing the batch",
            detail: "Distinct texts only, and a bounded number of them per run: a warning is for \
                     the shape of the data being off, and one line per message would bury the \
                     log. Failing the batch is `throw` instead.",
        },
    ]
}

/// The builtin by that exact name, whatever scope it belongs to.
#[must_use]
pub fn builtin(name: &str) -> Option<&'static Builtin> {
    builtins().iter().find(|builtin| builtin.name == name)
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
