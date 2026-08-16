//! Resolving a script's `import`s, at build time and never after.
//!
//! A script may `import` other rhai files, and the boundary is the one
//! [`super::source`] already draws for the script itself: a module is a
//! relative path of ordinary names under the config file's directory, refused
//! rather than normalised, re-checked after canonicalizing. What this module
//! adds is *when* resolution happens, and that is the load-bearing decision:
//!
//! - **Everything is resolved when the script is compiled** — via
//!   [`rhai::Engine::compile_into_self_contained`], which resolves every
//!   `import` with a constant path and embeds the modules in the AST. A missing
//!   or broken module is therefore a pipeline that refuses to build, the same
//!   rule a script that does not parse follows; and the run loop, which runs
//!   scripts synchronously inside its own task, never touches the filesystem.
//! - **The engine's own resolver stays the [`rhai::module_resolvers::
//!   DummyModuleResolver`]** after compiling. Only the compile sees one of the
//!   resolvers here, so an import whose path is assembled at runtime — the one
//!   kind a self-contained compile cannot resolve — fails when it runs rather
//!   than reaching the filesystem. That is the same refusal `eval` gets, for
//!   the same reason: a script whose dependencies cannot be read off the page
//!   is not one a reviewer can be asked to approve.
//! - **A module's top level runs once, at compile time.** The embedded module
//!   is the *evaluated* module, so what a script reaches through `import` is
//!   its functions and exported constants — not a body re-run per batch. A
//!   `now()` in a module's top level is therefore the moment the pipeline was
//!   built, which is one more reason a module is for functions.
//!
//! Two bounds, both here because resolution is triggered by a config that may
//! have arrived over HTTP: a cycle of imports is detected by the chain being
//! resolved (the `resolving` chain) rather than being left to recurse, and the count of
//! distinct modules one script may pull in is capped at [`MAX_MODULES`].

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use rhai::{Engine, EvalAltResult, Module, Position, Shared};

use super::source;

/// How many distinct modules one script may import, counting the whole tree.
///
/// Not a tuning knob: a script's imports are written by hand and a handful is
/// many. The cap exists because compiling is triggered over HTTP, so the work
/// it can cause has to have a stated bound — the same reason every state
/// bucket has one.
pub const MAX_MODULES: usize = 32;

/// Resolves imports against the config file's directory. Installed on the
/// engine only for the duration of a compile — see the module docs.
pub struct ProjectResolver {
    script_dir: PathBuf,
    /// The chain of modules currently being evaluated, importing each other.
    /// A path already on it importing again is a cycle, and is said to be one
    /// rather than recursed into.
    resolving: Mutex<Vec<String>>,
    /// Modules this compile has already resolved, so a diamond — two scripts'
    /// worth of imports sharing a helper — is read and evaluated once.
    resolved: Mutex<HashMap<String, Shared<Module>>>,
}

impl ProjectResolver {
    #[must_use]
    pub fn new(script_dir: &Path) -> Self {
        Self {
            script_dir: script_dir.to_path_buf(),
            resolving: Mutex::new(Vec::new()),
            resolved: Mutex::new(HashMap::new()),
        }
    }

    fn resolve_module(&self, engine: &Engine, path: &str) -> Result<Shared<Module>, String> {
        if let Some(module) = lock(&self.resolved).get(path) {
            return Ok(module.clone());
        }

        {
            let mut resolving = lock(&self.resolving);
            if resolving.iter().any(|p| p == path) {
                return Err(format!(
                    "the imports are circular: {} → '{path}'. A module cannot import something \
                     that is already importing it",
                    resolving
                        .iter()
                        .map(|p| format!("'{p}'"))
                        .collect::<Vec<_>>()
                        .join(" → ")
                ));
            }
            if lock(&self.resolved).len() >= MAX_MODULES {
                return Err(format!(
                    "this script imports more than {MAX_MODULES} modules, which is past the \
                     point where a script should have become a component"
                ));
            }
            resolving.push(path.to_string());
        }
        // From here on the chain entry has to come off however this ends.
        let result = self.read_and_eval(engine, path);
        lock(&self.resolving).pop();

        let module = result?;
        lock(&self.resolved).insert(path.to_string(), module.clone());
        Ok(module)
    }

    fn read_and_eval(&self, engine: &Engine, path: &str) -> Result<Shared<Module>, String> {
        let text = source::read_module(path, &self.script_dir)
            .map_err(|err| format!("could not read the imported module '{path}': {err:#}"))?;
        let ast = engine
            .compile(&text)
            .map_err(|err| format!("the imported module '{path}' does not compile: {err}"))?;
        // Evaluating is what turns the file's `fn`s and `export`s into a
        // module — and it is also where a module's own imports resolve, back
        // through this resolver, which is what the cycle chain is for.
        let module = Module::eval_ast_as_new(rhai::Scope::new(), &ast, engine)
            .map_err(|err| format!("the imported module '{path}' failed while loading: {err}"))?;
        Ok(module.into())
    }
}

impl rhai::ModuleResolver for ProjectResolver {
    fn resolve(
        &self,
        engine: &Engine,
        _source: Option<&str>,
        path: &str,
        _pos: Position,
    ) -> Result<Shared<Module>, Box<EvalAltResult>> {
        self.resolve_module(engine, path).map_err(|message| refusal(message).into())
    }
}

/// The resolver a server with no config file compiles against: it refuses
/// every import, saying why. The same closed default a file-sourced script
/// gets, and for the same reason — without a config file there is no directory
/// to bound resolution, and the working directory is not a boundary.
pub struct NoProjectResolver;

impl rhai::ModuleResolver for NoProjectResolver {
    fn resolve(
        &self,
        _engine: &Engine,
        _source: Option<&str>,
        path: &str,
        _pos: Position,
    ) -> Result<Shared<Module>, Box<EvalAltResult>> {
        Err(refusal(format!(
            "this script imports '{path}', but the server has no config file to resolve it \
             against — imported modules live beside the config, and this server was started \
             without one. Start the server with --config, or write the script without imports"
        ))
        .into())
    }
}

/// An import that is not going to happen, as rhai spells an error.
///
/// The empty first half of `ErrorSystem` is deliberate: rhai renders that
/// variant as the inner error alone, so the message reaches the build error
/// and the dry run without a prefix saying it twice. The variant carries no
/// position; the messages name the module instead, which is the half an
/// import error needs.
fn refusal(message: String) -> EvalAltResult {
    EvalAltResult::ErrorSystem(String::new(), std::io::Error::other(message).into())
}

/// Take a lock, surviving a poisoned one — same rule as the runner's buffers:
/// nothing in here is state worth refusing to look at.
fn lock<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
}
