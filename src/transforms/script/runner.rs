//! Compiling a script and running it over a batch.
//!
//! This is the half that both consumers share: the `script` transform runs it
//! on every batch, and `POST /api/scripts/dry-run` runs it once over messages
//! somebody pasted in. **They must be the same code**, configured the same way,
//! or the dry run becomes a second implementation whose agreement with the real
//! one is a matter of luck — and a script that passes the dry run and fails in
//! production is worse than having no dry run at all.
//!
//! ## The sandbox, and why each piece of it is here
//!
//! A script runs **synchronously inside the run loop's task**. That single fact
//! is what shapes this module: a script that loops forever does not merely
//! break its own pipeline, it wedges a tokio worker thread and takes every
//! other pipeline scheduled on it down too. So:
//!
//! - **The operation budget is not optional.** [`Engine::set_max_operations`]
//!   is what turns "wedges a worker" into "fails this batch". It is spent per
//!   *run* of the script, which in `message` scope means per message — the
//!   reason that scope is the default.
//! - **The size caps are not the budget said twice.** The budget counts
//!   operations, and one operation can allocate: a doubling loop reaches a
//!   gigabyte in thirty of them. Strings, arrays and maps are bounded
//!   separately.
//! - **There is no module resolver.** rhai's default resolver reads `import`ed
//!   files off disk, which would hand any script the filesystem and go straight
//!   past the point of `--data-dir`. It is replaced with one that resolves
//!   nothing.
//! - **There is no `eval`.** A script that assembles source at runtime is a
//!   script the operation budget still bounds but that nothing else can be said
//!   about — not the editor, not a reviewer reading the config.
//!
//! What is deliberately *not* here is any way to reach the network, the clock's
//! ability to block, or another pipeline. A script that needs a service is the
//! `http` transform's job; that one awaits, and this one cannot.

use std::collections::HashSet;
use std::sync::{Arc, Mutex};

use kayak_core::script::ScriptScope;
use rhai::{AST, Dynamic, Engine, Scope};
use serde_json::Value;

use super::error::{ScriptError, position_of, strip_position};
use super::value::{from_dynamic, to_dynamic};
use crate::inputs::MessageBatch;

/// How many rhai operations one run of a script may take when the config does
/// not say.
///
/// Generous for anything that is not looping by mistake — a script walking a
/// hundred-element array with a few operations per element is nowhere near it —
/// and small enough that a runaway is caught in well under a millisecond rather
/// than being noticed as a stalled server.
pub const DEFAULT_MAX_OPERATIONS: u64 = 100_000;

/// Caps on what one script run may build, in bytes or elements.
///
/// These bound *allocation*, which the operation budget does not: see the
/// module docs.
const MAX_STRING_SIZE: usize = 256 * 1024;
const MAX_ARRAY_SIZE: usize = 100_000;
const MAX_MAP_SIZE: usize = 10_000;

/// How deep an expression and a function body may nest. rhai parses
/// recursively, so this is a stack guard rather than a style rule — the same
/// job [`kayak_core::docs::MAX_NESTING`] does for the schema walk.
const MAX_EXPR_DEPTH: usize = 64;

/// How many *distinct* texts one script's `warn()` will report before it stops.
///
/// Bounded for the reason [`kayak_core::history`]'s error map is: a warning
/// carrying a message id is one distinct text per message, which is an
/// unbounded set fed at the message rate. A warning is for a config mistake,
/// and a config mistake does not have thousands of spellings.
const MAX_DISTINCT_WARNINGS: usize = 16;

/// A compiled script and the engine it runs in.
///
/// Compiling happens once — when the pipeline is built, or when the dry-run
/// endpoint is called — and running it reuses the AST. That matters: parsing
/// per batch would dwarf everything the script itself does.
pub struct ScriptRunner {
    engine: Engine,
    ast: AST,
    scope: ScriptScope,
    /// What the script has asked to emit during the run in progress.
    ///
    /// Shared with the `emit` function registered on the engine, which is the
    /// only way a host function can hand something back to the caller. Drained
    /// at the end of every run and cleared at the start of one, so nothing
    /// survives from one batch into the next — see [`ScriptRunner::run`] on why
    /// that rule is absolute.
    emitted: Arc<Mutex<Vec<Dynamic>>>,
    /// Texts `warn()` has already reported, so a mistake is logged once rather
    /// than once per message.
    warned: Arc<Mutex<HashSet<String>>>,
}

impl ScriptRunner {
    /// Compile a script, failing with a position an editor can point at.
    pub fn compile(
        code: &str,
        scope: ScriptScope,
        max_operations: Option<u64>,
        bindings: Bindings,
    ) -> Result<Self, ScriptError> {
        let emitted = Arc::new(Mutex::new(Vec::new()));
        let warned = Arc::new(Mutex::new(HashSet::new()));
        let engine = build_engine(
            max_operations.unwrap_or(DEFAULT_MAX_OPERATIONS),
            &emitted,
            &warned,
            bindings,
        );
        let ast = engine.compile(code).map_err(|err| {
            ScriptError::compile(err.err_type().to_string(), position_of(err.position()))
        })?;
        Ok(Self {
            engine,
            ast,
            scope,
            emitted,
            warned,
        })
    }

    /// Run the script over a batch, producing the batches it emitted.
    ///
    /// **A fresh [`Scope`] every run, and that is a rule rather than an
    /// implementation detail.** A script-local variable that survived between
    /// calls would be state: unbounded, invisible in the state tab, not rebuilt
    /// by a revert, and outside every limit [`crate::buckets`] enforces. All
    /// persistence goes through a state bucket or it does not exist.
    pub fn run(&mut self, batch: &[Arc<Value>]) -> Result<Vec<Arc<MessageBatch>>, ScriptError> {
        match self.scope {
            ScriptScope::Message => self.run_per_message(batch),
            ScriptScope::Batch => self.run_over_batch(batch),
        }
    }

    /// One run per message; every emitted value is one message, and they all
    /// land in a single output batch.
    ///
    /// Emitting nothing drops that message, which is what makes a script a
    /// filter. Emitting several splits it, which is what makes it a splitter.
    fn run_per_message(&mut self, batch: &[Arc<Value>]) -> Result<Vec<Arc<MessageBatch>>, ScriptError> {
        let mut out: MessageBatch = Vec::with_capacity(batch.len());
        for message in batch {
            let mut scope = Scope::new();
            scope.push_dynamic("msg", to_dynamic(message));
            let returned = self.eval(&mut scope)?;
            for value in self.drain(returned)? {
                out.push(Arc::new(value));
            }
        }
        // An empty batch is not emitted at all: the run loop treats a batch as
        // a thing that happened, and a script that dropped every message did
        // not produce an empty one, it produced none.
        if out.is_empty() {
            return Ok(Vec::new());
        }
        Ok(vec![Arc::new(out)])
    }

    /// One run for the whole batch; every emitted value is a whole batch and
    /// has to be an array.
    fn run_over_batch(&mut self, batch: &[Arc<Value>]) -> Result<Vec<Arc<MessageBatch>>, ScriptError> {
        let mut scope = Scope::new();
        let messages: rhai::Array = batch.iter().map(|m| to_dynamic(m)).collect();
        scope.push_dynamic("batch", Dynamic::from_array(messages));
        let returned = self.eval(&mut scope)?;

        let mut out = Vec::new();
        for value in self.drain(returned)? {
            let Value::Array(messages) = value else {
                return Err(ScriptError::runtime(
                    "in `batch` scope every emitted value is a whole batch and has to be an \
                     array of messages — use `emit([msg])` rather than `emit(msg)`",
                    None,
                ));
            };
            if messages.is_empty() {
                continue;
            }
            out.push(Arc::new(messages.into_iter().map(Arc::new).collect()));
        }
        Ok(out)
    }

    /// Run the compiled AST, turning rhai's failure into one that carries a
    /// position.
    fn eval(&mut self, scope: &mut Scope<'_>) -> Result<Dynamic, ScriptError> {
        self.clear_emitted();
        self.engine
            .eval_ast_with_scope::<Dynamic>(scope, &self.ast)
            .map_err(|err| {
                // rhai's Display appends its own position; this type carries it
                // as numbers beside the message. See `error::strip_position`.
                let message = err.to_string();
                ScriptError::runtime(strip_position(&message), position_of(err.position()))
            })
    }

    /// What the run asked to emit, as messages.
    ///
    /// The returned value is **sugar for a single `emit`**, and only when the
    /// script emitted nothing itself: a script whose last expression is the
    /// message it wants is the common one-liner, and making it call `emit`
    /// explicitly would be ceremony. A script that did both means the `emit`s,
    /// since those were deliberate.
    fn drain(&self, returned: Dynamic) -> Result<Vec<Value>, ScriptError> {
        let emitted: Vec<Dynamic> = std::mem::take(&mut *lock(&self.emitted));
        let values = if emitted.is_empty() && !returned.is_unit() {
            vec![returned]
        } else {
            emitted
        };
        values
            .iter()
            .map(|value| from_dynamic(value).map_err(|err| ScriptError::runtime(err.to_string(), None)))
            .collect()
    }

    fn clear_emitted(&self) {
        lock(&self.emitted).clear();
    }

    /// The distinct warnings this script has produced, for a caller that wants
    /// to show them — the dry run does, the run loop logs them as they happen.
    pub fn warnings(&self) -> Vec<String> {
        let mut warnings: Vec<String> = lock(&self.warned).iter().cloned().collect();
        warnings.sort();
        warnings
    }
}

/// What a script can reach outside its own message.
///
/// Empty is a script that can only compute — which is the whole of the dry
/// run's default and most transforms. The state binding is added when the
/// pipeline declares one.
#[derive(Default, Clone)]
pub struct Bindings {
    pub state: Option<StateBinding>,
}

/// The bucket a script's `remember`/`recall` reach.
///
/// One bucket, the pipeline's own, resolved at build time — the same binding
/// the `remember` and `recall` transforms get, for the same reason: a script
/// that could name any bucket would make "which pipelines touch this bucket" a
/// question nothing could answer by reading the config.
#[derive(Clone)]
pub struct StateBinding {
    pub buckets: Arc<crate::buckets::Buckets>,
    pub bucket: String,
}

/// Take a lock, surviving a poisoned one.
///
/// A script panicking is not something the pipeline that shares this buffer
/// should die of, and the buffer is cleared at the start of every run anyway —
/// so there is no state here worth refusing to look at.
fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}

/// The one place an engine is configured. See the module docs for why each of
/// these is here.
fn build_engine(
    max_operations: u64,
    emitted: &Arc<Mutex<Vec<Dynamic>>>,
    warned: &Arc<Mutex<HashSet<String>>>,
    bindings: Bindings,
) -> Engine {
    let mut engine = Engine::new();

    engine.set_max_operations(max_operations);
    engine.set_max_string_size(MAX_STRING_SIZE);
    engine.set_max_array_size(MAX_ARRAY_SIZE);
    engine.set_max_map_size(MAX_MAP_SIZE);
    engine.set_max_expr_depths(MAX_EXPR_DEPTH, MAX_EXPR_DEPTH);
    engine.set_max_modules(0);
    // rhai's default resolver reads `import`ed files off the filesystem. See
    // the module docs — this is the line between a sandbox and a shell.
    engine.set_module_resolver(rhai::module_resolvers::DummyModuleResolver::new());
    engine.disable_symbol("eval");

    // ── emitting ────────────────────────────────────────────────────────────
    let buffer = Arc::clone(emitted);
    engine.register_fn("emit", move |value: Dynamic| {
        lock(&buffer).push(value);
    });

    // ── field paths ─────────────────────────────────────────────────────────
    // Bound to `fields::get` rather than reimplemented, so a dotted path means
    // in a script exactly what it means in a `filter`, a `reduce` or a `map` —
    // including the rule that an exact key beats a path. Native rhai indexing
    // (`msg.a.b`) is still there and is what most scripts will use; this is for
    // the paths those cannot spell: the ones whose segments are chosen at
    // runtime, and the literal dotted keys an `envelope` writes.
    //
    // Named `field` and **not** `get`: rhai's object maps already have a `get`
    // method, and `get(msg, "a.b")` is method-call sugar for `msg.get("a.b")`,
    // so a function by that name is shadowed by the built-in for exactly the
    // messages it was added to reach. It looked like it worked, because an
    // exact key still resolved.
    engine.register_fn("field", |message: Dynamic, path: &str| -> Dynamic {
        match from_dynamic(&message) {
            Ok(value) => crate::fields::get(&value, path).map_or(Dynamic::UNIT, to_dynamic),
            Err(_) => Dynamic::UNIT,
        }
    });

    // ── the clock ───────────────────────────────────────────────────────────
    // A string rather than rhai's own `timestamp`, because what a message can
    // carry is a string: `now()` is nearly always written straight into a field.
    engine.register_fn("now", || chrono::Utc::now().to_rfc3339());
    engine.register_fn("now_millis", || chrono::Utc::now().timestamp_millis());

    // ── warnings ────────────────────────────────────────────────────────────
    let warned = Arc::clone(warned);
    engine.register_fn("warn", move |text: &str| {
        let mut seen = lock(&warned);
        if seen.len() >= MAX_DISTINCT_WARNINGS || !seen.insert(text.to_string()) {
            return;
        }
        tracing::warn!("script: {text}");
    });

    register_state(&mut engine, bindings.state);
    engine
}

/// `remember` and `recall`, or the errors that say how to get them.
///
/// A pipeline with no `state` block still gets the functions, and they fail
/// with the message that says what to add. This is the one build-time check
/// this component does not make, and the reason is that it cannot: whether a
/// script calls `remember` is not something the text says without walking the
/// AST, and rhai only exposes that behind its `internals` feature.
fn register_state(engine: &mut Engine, state: Option<StateBinding>) {
    let Some(recall_binding) = state else {
        for name in ["remember", "recall"] {
            engine.register_fn(name, move |_key: &str| -> Result<Dynamic, Box<rhai::EvalAltResult>> {
                Err(unbound_state().into())
            });
        }
        engine.register_fn(
            "remember",
            move |_key: &str, _values: rhai::Map| -> Result<(), Box<rhai::EvalAltResult>> {
                Err(unbound_state().into())
            },
        );
        return;
    };
    let remember_binding = recall_binding.clone();

    // Both take the bucket lock for the duration of one call and no longer.
    // `Buckets::with` takes a closure precisely so no guard can escape, and a
    // guard held across a script's execution would block every other pipeline
    // sharing that bucket for as long as the budget allows.
    engine.register_fn("recall", move |key: &str| -> Dynamic {
        // A unit for a key with no entry rather than an empty map, so a
        // script's warm-up check is `if recall(k) == ()` and "nothing
        // remembered yet" cannot be mistaken for "an entry holding nothing".
        let Some(values) = recall_binding
            .buckets
            .recall_all(&recall_binding.bucket, key)
        else {
            return Dynamic::UNIT;
        };
        let map: rhai::Map = values
            .iter()
            .map(|(name, value)| (name.as_str().into(), to_dynamic(value)))
            .collect();
        Dynamic::from_map(map)
    });

    engine.register_fn(
        "remember",
        move |key: &str, values: rhai::Map| -> Result<(), Box<rhai::EvalAltResult>> {
            let mut pairs = Vec::with_capacity(values.len());
            for (name, value) in &values {
                let value = from_dynamic(value)
                    .map_err(|err| Box::new(rhai::EvalAltResult::from(err.to_string())))?;
                pairs.push((name.to_string(), value));
            }
            remember_binding
                .buckets
                .remember(&remember_binding.bucket, key, pairs);
            Ok(())
        },
    );
}

fn unbound_state() -> String {
    "this pipeline declares no `state`, so a script cannot remember or recall — add \
     `state: { bucket: <name> }` to the pipeline, and the bucket itself under `state` at \
     the top of the config"
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use kayak_core::state::{StateBucketConfig, StateBuckets};
    use serde_json::json;

    fn run(code: &str, scope: ScriptScope, batch: &[Value]) -> Result<Vec<Vec<Value>>, ScriptError> {
        run_with(code, scope, batch, None, Bindings::default())
    }

    fn run_with(
        code: &str,
        scope: ScriptScope,
        batch: &[Value],
        max_operations: Option<u64>,
        bindings: Bindings,
    ) -> Result<Vec<Vec<Value>>, ScriptError> {
        let mut runner = ScriptRunner::compile(code, scope, max_operations, bindings)?;
        let batch: Vec<Arc<Value>> = batch.iter().cloned().map(Arc::new).collect();
        let out = runner.run(&batch)?;
        Ok(out
            .into_iter()
            .map(|b| b.iter().map(|m| (**m).clone()).collect())
            .collect())
    }

    fn one(code: &str, message: Value) -> Result<Vec<Vec<Value>>, ScriptError> {
        run(code, ScriptScope::Message, &[message])
    }

    // ── the shape of a run ──────────────────────────────────────────────────

    /// The one-liner: change a field, let the last expression be the message.
    /// If this needed an explicit `emit` the sugar would not be worth having.
    #[test]
    fn a_returned_message_is_emitted() -> Result<(), ScriptError> {
        let out = one("msg.doubled = msg.value * 2; msg", json!({"value": 21}))?;
        assert_eq!(out, vec![vec![json!({"value": 21, "doubled": 42})]]);
        Ok(())
    }

    /// Emitting nothing drops the message — which is `filter`, written as a
    /// script.
    #[test]
    fn emitting_nothing_drops_the_message() -> Result<(), ScriptError> {
        let out = run(
            "if msg.keep { emit(msg); }",
            ScriptScope::Message,
            &[json!({"keep": true, "n": 1}), json!({"keep": false, "n": 2})],
        )?;
        assert_eq!(out, vec![vec![json!({"keep": true, "n": 1})]]);
        Ok(())
    }

    /// A batch every message was dropped from produces *no* batch, not an empty
    /// one: the run loop counts a batch as something that happened.
    #[test]
    fn a_batch_with_every_message_dropped_emits_no_batch_at_all() -> Result<(), ScriptError> {
        let out = one("if false { emit(msg); }", json!({"n": 1}))?;
        assert!(out.is_empty(), "expected no batches, got {out:?}");
        Ok(())
    }

    /// Emitting several times splits the message — which is `splitter`, written
    /// as a script, and with the split decided by the content.
    #[test]
    fn emitting_several_times_splits_the_message() -> Result<(), ScriptError> {
        let out = one(
            "for line in msg.lines { emit(#{ id: msg.id, sku: line }); }",
            json!({"id": "o-1", "lines": ["a", "b"]}),
        )?;
        assert_eq!(
            out,
            vec![vec![
                json!({"id": "o-1", "sku": "a"}),
                json!({"id": "o-1", "sku": "b"}),
            ]]
        );
        Ok(())
    }

    /// Explicit emits win over the returned value. A script that did both meant
    /// the emits — those were deliberate, the trailing expression may just be
    /// the last statement's value.
    #[test]
    fn an_explicit_emit_wins_over_the_returned_value() -> Result<(), ScriptError> {
        let out = one("emit(#{ picked: true }); msg", json!({"n": 1}))?;
        assert_eq!(out, vec![vec![json!({"picked": true})]]);
        Ok(())
    }

    /// The headline capability, and the reason this component exists: nothing
    /// declarative reaches inside a message's array. See
    /// [`kayak_core::script`].
    #[test]
    fn a_script_can_total_an_array_inside_a_message() -> Result<(), ScriptError> {
        let out = one(
            r"
            let total = 0;
            for line in msg.lines {
                total += line.qty * line.price;
            }
            msg.total = total;
            msg
            ",
            json!({"lines": [{"qty": 2, "price": 3}, {"qty": 4, "price": 5}]}),
        )?;
        assert_eq!(out[0][0]["total"], json!(26));
        Ok(())
    }

    // ── batch scope ─────────────────────────────────────────────────────────

    #[test]
    fn a_batch_scoped_script_sees_the_whole_batch() -> Result<(), ScriptError> {
        let out = run(
            "emit([#{ count: batch.len }]);",
            ScriptScope::Batch,
            &[json!({"n": 1}), json!({"n": 2}), json!({"n": 3})],
        )?;
        assert_eq!(out, vec![vec![json!({"count": 3})]]);
        Ok(())
    }

    /// The N-batches-out half of the contract: a batch scope script decides how
    /// many batches leave, which is what makes repartitioning possible.
    #[test]
    fn a_batch_scoped_script_can_emit_several_batches() -> Result<(), ScriptError> {
        let out = run(
            r"
            let small = [];
            let large = [];
            for m in batch {
                if m.n > 1 { large.push(m); } else { small.push(m); }
            }
            emit(small);
            emit(large);
            ",
            ScriptScope::Batch,
            &[json!({"n": 1}), json!({"n": 2}), json!({"n": 3})],
        )?;
        assert_eq!(
            out,
            vec![
                vec![json!({"n": 1})],
                vec![json!({"n": 2}), json!({"n": 3})],
            ]
        );
        Ok(())
    }

    /// In batch scope an emitted value is a *batch*. Emitting a bare message is
    /// the mistake someone coming from message scope makes, so the error says
    /// exactly what to write instead.
    #[test]
    fn a_batch_scoped_script_emitting_a_bare_message_is_told_what_to_write() {
        let Err(err) = run("emit(#{ n: 1 });", ScriptScope::Batch, &[json!({"n": 1})]) else {
            panic!("a bare message is not a batch");
        };
        assert!(
            err.message.contains("emit([msg])"),
            "the error should show the fix: {err}"
        );
    }

    /// An emitted empty batch is skipped rather than passed on, for the reason
    /// message scope drops an empty one.
    #[test]
    fn an_emitted_empty_batch_is_skipped() -> Result<(), ScriptError> {
        let out = run("emit([]);", ScriptScope::Batch, &[json!({"n": 1})])?;
        assert!(out.is_empty(), "expected no batches, got {out:?}");
        Ok(())
    }

    // ── the sandbox ─────────────────────────────────────────────────────────

    /// The property the whole component rests on. A script runs synchronously
    /// in the run loop's task, so without this an unbounded loop wedges a tokio
    /// worker rather than failing a batch. If this test ever hangs, that is the
    /// bug it exists to catch.
    #[test]
    fn an_unbounded_loop_is_stopped_by_the_operation_budget() {
        let Err(err) = one("loop { }", json!({})) else {
            panic!("an infinite loop has to be stopped by the budget");
        };
        assert!(
            err.message.to_lowercase().contains("operation"),
            "the error should name the budget: {err}"
        );
    }

    /// The budget bounds operations and one operation can allocate, so the
    /// sizes are bounded separately — a doubling loop reaches a gigabyte in
    /// thirty operations.
    #[test]
    fn a_runaway_allocation_is_stopped_by_the_size_caps() {
        let Err(err) = one("let s = \"x\"; loop { s += s; }", json!({})) else {
            panic!("a doubling string has to be stopped");
        };
        assert!(
            err.kind == super::super::error::ScriptErrorKind::Runtime,
            "expected a runtime failure, got {err:?}"
        );
    }

    /// rhai's default module resolver reads `import`ed files off disk. That
    /// would hand every script the filesystem and walk straight past the point
    /// of `--data-dir`.
    #[test]
    fn a_script_cannot_import_a_module() {
        assert!(
            one("import \"/etc/passwd\" as p; msg", json!({})).is_err(),
            "a script must not be able to import anything"
        );
    }

    /// A script that assembles source at runtime is one nothing can be said
    /// about — not by a reviewer reading the config, not by the editor.
    #[test]
    fn a_script_cannot_eval() {
        assert!(
            one("eval(\"1 + 1\"); msg", json!({})).is_err(),
            "eval must not be reachable"
        );
    }

    /// Absolute, and the reason is in [`ScriptRunner::run`]: a variable that
    /// survived between runs would be state outside every bound `buckets`
    /// enforces and invisible in the state tab.
    #[test]
    fn nothing_survives_from_one_run_to_the_next() -> Result<(), ScriptError> {
        let out = run(
            "let seen = if is_def_var(\"carried\") { 1 } else { 0 }; let carried = true; \
             emit(#{ seen: seen });",
            ScriptScope::Message,
            &[json!({}), json!({})],
        )?;
        assert_eq!(
            out,
            vec![vec![json!({"seen": 0}), json!({"seen": 0})]],
            "the second run must not see the first run's variable"
        );
        Ok(())
    }

    // ── errors carry a position ─────────────────────────────────────────────

    /// The whole reason [`ScriptError`] has a position: the dry-run endpoint
    /// hands it to a caller and the editor puts a marker on that line.
    #[test]
    fn a_compile_error_carries_the_line_it_is_on() {
        let Err(err) = ScriptRunner::compile(
            "let a = 1;\nlet b = ;\n",
            ScriptScope::Message,
            None,
            Bindings::default(),
        ) else {
            panic!("that does not parse");
        };
        assert_eq!(err.kind, super::super::error::ScriptErrorKind::Compile);
        assert_eq!(
            err.position.map(|p| p.line),
            Some(2),
            "the error is on the second line: {err:?}"
        );
    }

    #[test]
    fn a_thrown_error_carries_the_line_it_is_on() {
        let Err(err) = one("let a = 1;\nthrow \"nope\";", json!({})) else {
            panic!("the script throws");
        };
        assert_eq!(err.kind, super::super::error::ScriptErrorKind::Runtime);
        assert_eq!(err.position.map(|p| p.line), Some(2));
        assert!(err.message.contains("nope"), "{err}");
    }

    // ── the host functions ──────────────────────────────────────────────────

    /// Bound to `fields::get`, so a dotted path means in a script what it means
    /// in a `filter`, a `reduce` or a `map` — including the rule that an exact
    /// key beats a path, which is what makes an `envelope`'s `_meta.subject`
    /// reachable.
    #[test]
    fn field_follows_the_same_field_paths_as_every_other_transform() -> Result<(), ScriptError> {
        let out = one(
            "emit(#{ found: field(msg, \"sensor.id\"), literal: field(msg, \"a.b\") });",
            json!({"sensor": {"id": "s-1"}, "a.b": "exact"}),
        )?;
        assert_eq!(out, vec![vec![json!({"found": "s-1", "literal": "exact"})]]);
        Ok(())
    }

    #[test]
    fn field_of_a_path_that_is_not_there_is_a_unit() -> Result<(), ScriptError> {
        let out = one("emit(#{ found: field(msg, \"nope.gone\") });", json!({}))?;
        assert_eq!(out, vec![vec![json!({"found": null})]]);
        Ok(())
    }

    /// A string rather than rhai's own timestamp, because what a message can
    /// carry is a string and `now()` is nearly always written into a field.
    #[test]
    fn now_is_a_string_a_message_can_carry() -> Result<(), ScriptError> {
        let out = one("msg.at = now(); msg", json!({}))?;
        let at = out[0][0]["at"].as_str().unwrap_or_default();
        assert!(
            chrono::DateTime::parse_from_rfc3339(at).is_ok(),
            "now() should produce an RFC 3339 timestamp, got {at:?}"
        );
        Ok(())
    }

    // ── state ───────────────────────────────────────────────────────────────

    fn with_state() -> (Arc<crate::buckets::Buckets>, Bindings) {
        let mut declared = StateBuckets::new();
        declared.insert("machines", StateBucketConfig::default());
        let buckets = Arc::new(crate::buckets::Buckets::from_config(&declared));
        let bindings = Bindings {
            state: Some(StateBinding {
                buckets: Arc::clone(&buckets),
                bucket: "machines".to_string(),
            }),
        };
        (buckets, bindings)
    }

    /// The combination that unlocks a class rather than a case: a script plus a
    /// bucket is change detection, deduplication and thresholds with
    /// hysteresis, none of which the declarative transforms reach.
    #[test]
    fn a_script_can_remember_and_recall() -> Result<(), ScriptError> {
        let (_buckets, bindings) = with_state();
        let out = run_with(
            r"
            let last = recall(msg.id);
            remember(msg.id, #{ value: msg.value });
            if last == () {
                emit(#{ id: msg.id, first: true });
            } else {
                emit(#{ id: msg.id, delta: msg.value - last.value });
            }
            ",
            ScriptScope::Message,
            &[json!({"id": "m-1", "value": 10}), json!({"id": "m-1", "value": 25})],
            None,
            bindings,
        )?;
        assert_eq!(
            out,
            vec![vec![
                json!({"id": "m-1", "first": true}),
                json!({"id": "m-1", "delta": 15}),
            ]]
        );
        Ok(())
    }

    /// A unit rather than an empty map, so the warm-up check is
    /// `recall(k) == ()` and "nothing remembered yet" cannot be confused with
    /// "an entry holding nothing".
    #[test]
    fn recalling_a_key_that_was_never_written_is_a_unit() -> Result<(), ScriptError> {
        let (_buckets, bindings) = with_state();
        let out = run_with(
            "emit(#{ missing: recall(\"never\") == () });",
            ScriptScope::Message,
            &[json!({})],
            None,
            bindings,
        )?;
        assert_eq!(out, vec![vec![json!({"missing": true})]]);
        Ok(())
    }

    /// The one check this component cannot make at build time — see
    /// `register_state`. So the runtime error has to carry the whole fix.
    #[test]
    fn a_script_without_a_state_block_is_told_what_to_add() {
        let Err(err) = one("remember(\"k\", #{ a: 1 });", json!({})) else {
            panic!("no state block is declared");
        };
        assert!(
            err.message.contains("state:") && err.message.contains("bucket"),
            "the error should say what to add: {err}"
        );
    }

    /// The store enforces the bounds, not the transform — which is what stops a
    /// new stateful component from forgetting them, and a script is the first
    /// one whose author decides how many keys get written.
    #[test]
    fn a_script_cannot_write_past_a_buckets_bound() -> Result<(), ScriptError> {
        let mut declared = StateBuckets::new();
        declared.insert(
            "small",
            StateBucketConfig {
                max_keys: Some(2),
                idle_timeout_secs: None,
            },
        );
        let buckets = Arc::new(crate::buckets::Buckets::from_config(&declared));
        let bindings = Bindings {
            state: Some(StateBinding {
                buckets: Arc::clone(&buckets),
                bucket: "small".to_string(),
            }),
        };
        run_with(
            "remember(msg.id, #{ v: 1 });",
            ScriptScope::Message,
            &[json!({"id": "a"}), json!({"id": "b"}), json!({"id": "c"})],
            None,
            bindings,
        )?;
        let Some(contents) = buckets.contents("small") else {
            panic!("the bucket was declared, so it exists");
        };
        assert_eq!(contents.keys, 2, "the bucket's own bound is what holds");
        Ok(())
    }

    // ── warnings ────────────────────────────────────────────────────────────

    /// A config mistake, not an event: one line however many messages hit it.
    /// Bounded for the reason the history's error map is — see
    /// `MAX_DISTINCT_WARNINGS`.
    #[test]
    fn a_warning_is_reported_once_per_distinct_text() -> Result<(), ScriptError> {
        let mut runner = ScriptRunner::compile(
            "warn(\"the stream does not carry `unit`\"); emit(msg);",
            ScriptScope::Message,
            None,
            Bindings::default(),
        )?;
        let batch: Vec<Arc<Value>> = vec![Arc::new(json!({})), Arc::new(json!({}))];
        runner.run(&batch)?;
        runner.run(&batch)?;
        assert_eq!(runner.warnings(), vec!["the stream does not carry `unit`"]);
        Ok(())
    }
}
