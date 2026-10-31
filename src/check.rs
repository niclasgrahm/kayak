//! `kayak check`: would the server start with this configuration?
//!
//! The answer is got by doing what startup does and stopping short of running
//! anything: the same files are read, the same secret store is chained, and
//! every pipeline is put through the **production** `build()` with the same
//! `BuildCtx` [`AppState`] assembles for a started one. A second set of rules
//! written for the checker would be a second opinion that drifts from the
//! first, and the one that matters is the server's.
//!
//! Three differences from startup, each deliberate:
//!
//! - **Nothing runs.** The builds happen on a current-thread runtime that is
//!   entered but never driven, so whatever a build spawns (an input `buffer`'s
//!   pump, `Merged`'s tasks) is dropped unpolled with the runtime. That is
//!   what makes it impossible for a check to reach a broker.
//! - **Every problem is reported**, where startup stops at the first. A
//!   pipeline whose upstream failed is *skipped* rather than built, so it says
//!   so once instead of repeating "upstream not found".
//! - **What can only be answered over the network is not asked**: brokers,
//!   databases and a jwt issuer's signing keys. Neither is `--state-dir`
//!   opened — a running server holds its lock, and the check is most useful
//!   exactly while one is running.
//!
//! What it does touch is what startup would create anyway: the `--data-dir`
//! and each file output's directory under it, since the sandbox check is a
//! check of where a path *really* lands and needs the directory to exist.

use std::collections::HashSet;
use std::fmt::Write as _;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use kayak_core::server_config::AuthConfig;

use crate::auth::Auth;
use crate::secrets::{ChainStore, SecretStore};
use crate::state::AppState;

/// What to check: the server's own flags, read the way the server reads them.
#[derive(Debug, Default, Clone)]
pub struct Options {
    pub config: Option<PathBuf>,
    pub connections: Option<PathBuf>,
    pub secrets: Option<PathBuf>,
    pub data_dir: Option<PathBuf>,
    pub server_config: Option<PathBuf>,
    pub state_dir: Option<PathBuf>,
}

/// One thing that would stop the server starting.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Problem {
    /// What it is about: a file, or a pipeline.
    pub subject: String,
    /// The error, with its whole chain of causes.
    pub detail: String,
}

/// A pipeline that was not built because something it depends on failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Skipped {
    pub subject: String,
    pub upstream: String,
}

/// What a check found.
#[derive(Debug, Default)]
pub struct Report {
    pub config: Option<PathBuf>,
    /// Pipelines that built.
    pub pipelines: usize,
    pub connections: usize,
    pub buckets: usize,
    pub problems: Vec<Problem>,
    pub skipped: Vec<Skipped>,
    /// What was deliberately not checked, said out loud.
    pub notes: Vec<String>,
}

impl Report {
    /// No problems: the exit status of `kayak check` is this.
    #[must_use]
    pub fn is_ok(&self) -> bool {
        self.problems.is_empty()
    }

    fn problem(&mut self, subject: impl Into<String>, error: &anyhow::Error) {
        self.problems.push(Problem {
            subject: subject.into(),
            detail: format!("{error:#}"),
        });
    }

    /// The report as `kayak check` prints it.
    #[must_use]
    pub fn render(&self) -> String {
        let mut out = String::new();
        match &self.config {
            Some(path) => {
                let _ = writeln!(
                    out,
                    "checked {}: {} built, {} {}, {} state {}",
                    path.display(),
                    plural(self.pipelines, "pipeline", "pipelines"),
                    self.connections,
                    if self.connections == 1 { "connection" } else { "connections" },
                    self.buckets,
                    if self.buckets == 1 { "bucket" } else { "buckets" },
                );
            }
            None => {
                let _ = writeln!(
                    out,
                    "no --config given, so no pipelines were checked ({} {})",
                    self.connections,
                    if self.connections == 1 { "connection" } else { "connections" },
                );
            }
        }
        if !self.problems.is_empty() || !self.skipped.is_empty() {
            out.push('\n');
        }
        for problem in &self.problems {
            let _ = writeln!(out, "error: {}: {}", problem.subject, problem.detail);
        }
        for skipped in &self.skipped {
            let _ = writeln!(
                out,
                "skipped: {} (its upstream '{}' has problems)",
                skipped.subject, skipped.upstream
            );
        }
        if !self.notes.is_empty() {
            out.push('\n');
        }
        for note in &self.notes {
            let _ = writeln!(out, "note: {note}");
        }
        out.push('\n');
        if self.is_ok() {
            out.push_str("ok: no problems found\n");
        } else {
            let _ = writeln!(
                out,
                "{}: the server would not start with this configuration",
                plural(self.problems.len(), "problem", "problems")
            );
        }
        out
    }
}

fn plural(n: usize, one: &str, many: &str) -> String {
    format!("{n} {}", if n == 1 { one } else { many })
}

/// Check everything `options` names. Never fails: what goes wrong is the
/// report.
#[must_use]
pub fn check(options: &Options) -> Report {
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(e) => {
            let mut report = Report {
                config: options.config.clone(),
                ..Report::default()
            };
            report.problem("kayak check", &anyhow::Error::new(e).context("failed to start a runtime"));
            return report;
        }
    };
    // entered, never driven: see the module docs. Everything the builds
    // produced is dropped inside `check_in`, and whatever they spawned is
    // dropped unpolled with the runtime after it.
    let report = {
        let _guard = runtime.enter();
        check_in(options)
    };
    drop(runtime);
    report
}

fn check_in(options: &Options) -> Report {
    let mut report = Report {
        config: options.config.clone(),
        ..Report::default()
    };

    if let Some(dir) = &options.state_dir {
        report.notes.push(format!(
            "--state-dir {} was not opened: a running server holds its lock",
            dir.display()
        ));
    }

    // Each step that fails stops the ones that depend on it, so one mistake
    // reads as one problem rather than a cascade of its consequences.
    let secrets: Option<Arc<dyn SecretStore>> = match ChainStore::server(options.secrets.as_deref())
    {
        Ok(store) => Some(Arc::new(store)),
        Err(e) => {
            report.problem("secrets file", &e);
            None
        }
    };

    if let Some(path) = &options.server_config {
        check_server_config(path, secrets.as_deref(), &mut report);
    }

    let data_dir = match AppState::resolve_data_dir(options.data_dir.clone()) {
        Ok(dir) => Some(dir),
        Err(e) => {
            report.problem("--data-dir", &e);
            None
        }
    };

    let state = AppState::for_check(
        options.config.as_deref(),
        secrets.clone().unwrap_or_else(|| Arc::new(crate::secrets::EnvStore)),
        options.connections.as_deref(),
        data_dir.clone().flatten(),
    );
    let connections_ok = match state.load_connections_file() {
        Ok(()) => true,
        Err(e) => {
            report.problem("connections file", &e);
            false
        }
    };
    report.connections = state.connections().len();

    let Some(config) = &options.config else {
        return report;
    };
    let file = match crate::persist::read(config) {
        Ok(file) => file,
        Err(e) => {
            report.problem("config file", &e);
            return report;
        }
    };
    // startup refuses a layout file it cannot read, so this has to be asked
    // too, though it has nothing to do with what runs
    if let Err(e) = crate::layout::read(&crate::layout::layout_path(config)) {
        report.problem("layout file", &e);
    }
    report.buckets = file.state.len();

    if secrets.is_none() || data_dir.is_none() || !connections_ok {
        report.notes.push(
            "the pipelines were not built, since what they are built from did not load".into(),
        );
        return report;
    }

    state.adopt_buckets(file.state);
    build_pipelines(&state, file.pipelines, &mut report);
    report
}

/// The server config, and the accounts in it resolved against the secrets.
fn check_server_config(path: &Path, secrets: Option<&dyn SecretStore>, report: &mut Report) {
    let server_config = match crate::server_config::read_required(path) {
        Ok(config) => config,
        Err(e) => {
            report.problem("server config file", &e);
            return;
        }
    };
    if let Some(secrets) = secrets
        && let Err(e) = Auth::from_config(&server_config, secrets)
    {
        report.problem(
            "server config file",
            &e.context(format!("{}: failed to load the accounts", path.display())),
        );
    }
    if matches!(server_config.auth, AuthConfig::Jwt(_)) {
        report.notes.push(
            "the identity provider's signing keys were not fetched; the server does that at \
             startup"
                .into(),
        );
    }
}

/// Every pipeline in file order, the order the server builds them in.
fn build_pipelines(
    state: &AppState,
    pipelines: Vec<kayak_core::config::Config>,
    report: &mut Report,
) {
    let declared: HashSet<String> = pipelines.iter().filter_map(|c| c.id.clone()).collect();
    let mut built: HashSet<String> = HashSet::new();
    let mut failed: HashSet<String> = HashSet::new();

    for (index, config) in pipelines.into_iter().enumerate() {
        let subject = match &config.id {
            Some(id) => format!("pipeline '{id}'"),
            None => format!("pipeline #{} (no id)", index + 1),
        };
        if let Some(id) = &config.id
            && (built.contains(id) || failed.contains(id))
        {
            report.problems.push(Problem {
                subject,
                detail: format!("the id '{id}' is already used by an earlier pipeline"),
            });
            continue;
        }

        let upstreams: Vec<String> = config.upstreams().into_iter().cloned().collect();
        if let Some(upstream) = upstreams.iter().find(|u| failed.contains(*u)) {
            report.skipped.push(Skipped {
                subject,
                upstream: upstream.clone(),
            });
            if let Some(id) = config.id {
                failed.insert(id);
            }
            continue;
        }
        // declared, but further down (or this very pipeline): the server
        // would report "not found" here, which is true and points the wrong way
        if let Some(upstream) = upstreams
            .iter()
            .find(|u| declared.contains(*u) && !built.contains(*u))
        {
            let detail = if config.id.as_ref() == Some(upstream) {
                format!("it names itself ('{upstream}') as its upstream")
            } else {
                format!(
                    "its upstream '{upstream}' is declared after it; pipelines are built in \
                     file order, so an upstream has to come first"
                )
            };
            report.problems.push(Problem { subject, detail });
            if let Some(id) = config.id {
                failed.insert(id);
            }
            continue;
        }

        let id = config.id.clone();
        match state.build_unstarted(config) {
            Ok(()) => {
                report.pipelines += 1;
                if let Some(id) = id {
                    built.insert(id);
                }
            }
            Err(e) => {
                report.problem(subject, &e);
                if let Some(id) = id {
                    failed.insert(id);
                }
            }
        }
    }
}
