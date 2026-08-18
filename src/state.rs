use anyhow::Context;
use serde::Serialize;
use tokio::sync::broadcast;

use crate::BuildCtx;
use crate::auth::Auth;
use crate::buckets::Buckets;
use crate::history::History;
use crate::inputs::http::{Credentials, Inboxes, IngestError, PostMeta};
use crate::pipeline::Pipeline;
use crate::secrets::{EnvStore, SecretStore};
use kayak_core::config::Config;
use kayak_core::connections::ConnectionKind;
use kayak_core::state::{ConfigFile, StateBuckets};
pub use kayak_core::{ConfigFormat, ConnectionId, Connections, LayoutFile, PipelineId, UiEvent};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};

#[derive(Serialize)]
pub struct PipelineHandle {
    #[serde(skip)]
    pub join_handle: tokio::task::JoinHandle<()>,
    pub shared: Arc<Pipeline>,
}

/// Errors that callers need to distinguish between — the HTTP layer maps these
/// onto status codes, everything else becomes a 500.
#[derive(Debug)]
pub enum PipelineError {
    NotFound(PipelineId),
    DuplicateId(PipelineId),
    /// The pipeline is running, but nothing can be posted to it: it has no
    /// `http` input. A 404 like [`PipelineError::NotFound`], and deliberately
    /// distinct from it — one is fixed by creating the pipeline, the other by
    /// giving it the input that would serve the endpoint.
    NotAccepting(PipelineId),
    /// A pipeline's `http` input queue is full — it is not reading as fast as
    /// something is posting. A 503: try again, nothing was lost but this batch.
    Backpressure(PipelineId),
    /// The pipeline's `http` input has an `auth` and the post didn't satisfy
    /// it. A 401, and separate from the server's own sign-in for the reason
    /// [`kayak_core::config::HttpAuthConfig`] gives: this is the data plane's
    /// credential, and having an account is neither necessary nor sufficient.
    Unauthorized(PipelineId),
    /// A connection someone asked to delete is still named by running
    /// pipelines. Deleting it would leave them running on settings nothing
    /// records, and the next revert would refuse to rebuild them.
    ConnectionInUse(ConnectionId, Vec<PipelineId>),
    /// A save that asked not to overwrite (`overwrite: false`) named files that
    /// are already there. A 409 like [`PipelineError::DuplicateId`], and the
    /// same shape of mistake: the caller asked to create something that exists.
    ///
    /// Carries every file the save would have written, not just the config —
    /// naming only the one that was typed would leave someone renaming around
    /// a refusal they can't see the cause of.
    ConfigExists(Vec<PathBuf>),
    /// The config parsed, but describes a pipeline that can't be built — e.g.
    /// it names an upstream pipeline that doesn't exist. That's the caller's
    /// mistake, not ours.
    InvalidConfig(anyhow::Error),
    Internal(anyhow::Error),
}

impl std::fmt::Display for PipelineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound(id) => write!(f, "pipeline with id '{id}' not found"),
            Self::NotAccepting(id) => write!(
                f,
                "pipeline '{id}' has no http input, so nothing can be posted to it"
            ),
            Self::Backpressure(id) => write!(
                f,
                "pipeline '{id}' is not keeping up; its http input queue is full"
            ),
            Self::Unauthorized(id) => write!(
                f,
                "pipeline '{id}' requires a credential this post did not carry"
            ),
            Self::DuplicateId(id) => write!(f, "pipeline with id '{id}' already exists"),
            Self::ConnectionInUse(id, used_by) => write!(
                f,
                "connection '{id}' is still used by {}",
                used_by.join(", ")
            ),
            Self::ConfigExists(paths) => write!(
                f,
                "{} already exists; save with a different name, or overwrite it deliberately",
                paths
                    .iter()
                    .map(|path| path.display().to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            ),
            // only this layer — the rest of the chain is reachable through
            // source(), so printing it here too would duplicate it
            Self::InvalidConfig(err) | Self::Internal(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for PipelineError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            // Display above already prints the inner error's own layer, so the
            // chain continues from *its* source, not from the error itself
            Self::InvalidConfig(err) | Self::Internal(err) => (**err).source(),
            _ => None,
        }
    }
}

impl From<anyhow::Error> for PipelineError {
    fn from(err: anyhow::Error) -> Self {
        Self::Internal(err)
    }
}

impl From<serde_json::Error> for PipelineError {
    fn from(err: serde_json::Error) -> Self {
        Self::Internal(err.into())
    }
}

/// How long a revert waits for the old run loops to finish before rebuilding on
/// top of them. Only an output stuck mid-`emit()` can take this long; anything
/// else notices its cancellation on the next loop iteration.
const TEARDOWN_GRACE: std::time::Duration = std::time::Duration::from_secs(5);

pub struct AppState {
    pipelines: Mutex<HashMap<PipelineId, PipelineHandle>>,
    events: broadcast::Sender<UiEvent>,
    /// How many browsers are attached to `events`, kept beside it because
    /// asking the channel costs a mutex every pipeline in the process contends
    /// on — see [`crate::events::Watchers`]. Maintained by
    /// [`AppState::subscribe_events`], which is the only place a receiver is
    /// handed out, and read by every run loop through [`BuildCtx`].
    watchers: crate::events::Watchers,
    /// What `${NAME}` references in an incoming config resolve against. Held
    /// here rather than passed per request because it's fixed at startup — a
    /// pipeline posted to `/api/pipelines` gets the same secrets as one loaded
    /// from the config file.
    secrets: Arc<dyn SecretStore>,
    /// The config file this server is working against: the `--config` file it
    /// was started with, or the one a save has since created.
    ///
    /// It is a *load source and a save target*, not a mirror: nothing here
    /// writes to it except an explicit save. See [`crate::persist`].
    ///
    /// Behind a lock because it can be *acquired*: a server started without a
    /// file still lets someone build a graph in the UI and save it, and from
    /// that save on the file it created is the one it reloads, compares against
    /// and arranges beside. It is only ever set from nothing to something —
    /// saving under a second name while a file is already loaded leaves the
    /// loaded one in place, so "revert" keeps meaning what it meant a moment
    /// ago.
    config_path: Mutex<Option<PathBuf>>,
    /// The connections file, when the operator named one with `--connections`.
    ///
    /// Fixed for the life of the process, unlike [`AppState::config_path`]:
    /// pointing two configs at one shared connections file is exactly what the
    /// flag is for, so it must not be quietly replaced by a save. When it is
    /// `None` the file is *derived* from the config file instead — see
    /// [`AppState::connections_path`] — which is what lets a server started
    /// with neither flag still acquire both files from one save.
    connections_file: Option<PathBuf>,
    /// The systems the pipelines talk to, by name.
    ///
    /// Held beside the graph rather than inside it because that is the point of
    /// them: one kafka cluster, named once, referred to by every pipeline that
    /// reads a topic on it. Read when a pipeline is *built*, so a change here
    /// reaches new and rebuilt pipelines rather than running ones.
    connections: Mutex<Connections>,
    /// The connections as last loaded or saved, rendered — the same
    /// fingerprint trick [`AppState::saved`] uses, for the same question.
    saved_connections: Mutex<Option<String>>,
    /// The one directory a save may write to. Fixed at startup and never
    /// derived from a request — see [`crate::persist::save_path`], which is
    /// what makes that a boundary rather than a default.
    save_dir: PathBuf,
    /// The directory tree file outputs may write pipeline data into, from
    /// `--data-dir`. `None` — the default — turns file output off entirely.
    ///
    /// Deliberately *not* [`AppState::save_dir`], though both are directories
    /// fixed at startup: that one bounds where the server writes *configs* on
    /// request, this one bounds where pipelines write *data*, and pointing a
    /// data firehose at the directory holding the config someone is editing is
    /// not a default anyone would choose. See [`crate::outputs::file::Root`].
    data_dir: Option<Arc<PathBuf>>,
    /// Where the running pipelines' `http` inputs are reached, by id.
    ///
    /// Beside the graph rather than inside it because the two ends of it are on
    /// either side of the runtime: a pipeline registers here when it is built,
    /// and the HTTP handler that serves `POST /api/pipelines/{id}/messages`
    /// looks it up here without ever touching an `InputSource`. Entries come
    /// and go with the pipelines themselves — the registration is owned by the
    /// input, so a deleted pipeline's endpoint disappears with its run loop.
    inboxes: Arc<Inboxes>,
    /// The rendered form of the graph as last loaded or saved, which is what
    /// "unsaved changes" is measured against.
    ///
    /// A snapshot rather than a re-read of the file, because the two questions
    /// differ: someone editing the file by hand hasn't made the *running* graph
    /// stale. `None` when there's no file to be in sync with.
    ///
    /// Always rendered as JSON, whatever the file on disk is written in: this
    /// is a fingerprint of the *graph*, and saving the same pipelines as YAML
    /// instead of JSON hasn't changed them.
    saved: Mutex<Option<String>>,
    /// Where the cards have been dragged to on the canvas.
    ///
    /// Held beside the graph rather than inside it because it is not part of
    /// it: no pipeline behaves differently for having a position, and an entry
    /// for a pipeline that has since been deleted is harmless. Unlike the
    /// config file, this one *is* written as a side effect of an edit — moving
    /// a pipeline changes nothing about the running system, so making someone save
    /// it would be ceremony over a cosmetic act. See [`crate::layout`].
    layout: Mutex<LayoutFile>,
    /// The live state buckets, and the declaration they were built from.
    ///
    /// Beside the graph rather than inside a pipeline because that is what
    /// "global and named" means: one bucket, declared once, written by one
    /// pipeline and read by several. Behind a lock because a revert *replaces*
    /// the set — see [`crate::buckets::Buckets::rebuilt`] for what survives
    /// that and what doesn't.
    buckets: Mutex<Arc<Buckets>>,
    /// What the config file declared, kept so a revert can tell an unchanged
    /// bucket from one whose bounds moved.
    declared_buckets: Mutex<StateBuckets>,
    /// The accounts, and who is currently signed in.
    ///
    /// Fixed at startup like the secret store and for the same reason: who may
    /// reach the server is a property of how it was started, and nothing served
    /// over HTTP may change it. [`Auth::disabled`] — the default — is a server
    /// that asks nobody for anything, which is what a `--server-config`-less
    /// process runs.
    auth: Arc<Auth>,
    /// Cancelled once, when the process has been asked to stop.
    ///
    /// It is the *server's* token and not a pipeline's: the run loops are
    /// stopped through their own tokens by [`AppState::stop_pipelines`], and
    /// what this one is for is everything that has to end before those can be
    /// — which today is the `/events` streams. An SSE response never completes
    /// on its own, so axum's graceful shutdown would wait for an attached
    /// browser forever; the stream watches this instead and ends with it. See
    /// [`crate::shutdown`] for the order the whole thing runs in.
    ///
    /// Deliberately **not** consulted by anything on the hot path. It is read
    /// once per SSE stream, not once per event.
    shutdown: tokio_util::sync::CancellationToken,
    /// What the pipelines have done, kept for the UI to show after the fact.
    ///
    /// Beside the graph rather than inside a pipeline because it deliberately
    /// **outlives** one: a revert rebuilds every pipeline in the file, and
    /// history that died with its run loop would make an edit to one card cost
    /// the overnight record of all of them. The same argument
    /// [`crate::buckets`] makes about state, one step further along — buckets
    /// survive a rebuild, and these survive a deletion too, until the retention
    /// runs out.
    ///
    /// [`History::disabled`] by default, which is a server that keeps nothing;
    /// [`AppState::with_history`] is how a `--server-config` turns it on.
    history: Arc<History>,
}

impl Default for AppState {
    fn default() -> Self {
        Self::new()
    }
}

impl AppState {
    /// Resolves secrets from the environment only; see [`AppState::with_secrets`].
    #[must_use]
    pub fn new() -> Self {
        Self::with_secrets(Arc::new(EnvStore))
    }

    /// An empty server that saves into the directory it was started in.
    ///
    /// There is no config file yet, and a save is what creates one — so the
    /// working directory is where it lands. That is the same rule as
    /// `--config`: the operator chose the directory when they started the
    /// process, and no request can move it. Tests that write should use
    /// [`AppState::with_secrets_in`] and name a temporary directory instead of
    /// leaning on the process's.
    pub fn with_secrets(secrets: Arc<dyn SecretStore>) -> Self {
        let cwd = std::env::current_dir().unwrap_or_else(|err| {
            tracing::warn!("could not read the working directory ({err}); saving to '.'");
            PathBuf::from(".")
        });
        Self::with_secrets_in(cwd, secrets)
    }

    /// As [`AppState::with_secrets`], with the save directory named outright.
    pub fn with_secrets_in(save_dir: PathBuf, secrets: Arc<dyn SecretStore>) -> Self {
        tracing::debug!("Initializing empty server state...");
        let (events, _) = broadcast::channel(1024);
        let watchers = crate::events::Watchers::empty();
        Self {
            pipelines: Mutex::new(HashMap::new()),
            events,
            watchers,
            secrets,
            config_path: Mutex::new(None),
            connections_file: None,
            connections: Mutex::new(Connections::new()),
            saved_connections: Mutex::new(None),
            save_dir,
            data_dir: None,
            inboxes: Arc::new(Inboxes::new()),
            saved: Mutex::new(None),
            layout: Mutex::new(LayoutFile::default()),
            buckets: Mutex::new(Arc::new(Buckets::new())),
            declared_buckets: Mutex::new(StateBuckets::new()),
            auth: Arc::new(Auth::disabled()),
            shutdown: tokio_util::sync::CancellationToken::new(),
            history: Arc::new(History::disabled()),
        }
    }

    /// Keep a record of what the pipelines do, per the settings file's
    /// `history` section.
    ///
    /// A builder for the reason [`AppState::with_auth`] is one — except that
    /// here the *server's* default is on and this default is off, which is not
    /// a contradiction: `main` always calls this, and what a bare `AppState`
    /// wants is the cheap thing, because that is every test that isn't about
    /// history.
    #[must_use]
    pub fn with_history(mut self, history: Arc<History>) -> Self {
        self.history = history;
        self
    }

    /// What the pipelines have done, for the handler that serves it and for
    /// the sampler that feeds it.
    #[must_use]
    pub fn history(&self) -> &Arc<History> {
        &self.history
    }

    /// Fold one tick of every running pipeline's counters into the history
    /// rings. Called by [`crate::history::sampler`]; here rather than on
    /// `History` because the pipelines map is this type's to lock.
    ///
    /// The lock is taken and released around a walk of the map with no `.await`
    /// inside it, which is the house rule.
    pub fn sample_history(&self, now_secs: u64) {
        let live: Vec<(PipelineId, Arc<crate::pipeline::Pipeline>)> = {
            let pipelines = self.lock_pipelines();
            pipelines
                .iter()
                .map(|(id, handle)| (id.clone(), Arc::clone(&handle.shared)))
                .collect()
        };
        self.history.sample(
            live.iter().map(|(id, shared)| (id, &shared.counters)),
            now_secs,
        );
    }

    /// Require credentials, per the settings file the server was started with.
    ///
    /// A builder rather than a constructor argument because the default is the
    /// interesting case: every existing call site — and every test that isn't
    /// about authentication — wants a server that asks nobody for anything,
    /// and gets one by not calling this.
    #[must_use]
    pub fn with_auth(mut self, auth: Arc<Auth>) -> Self {
        self.auth = auth;
        self
    }

    /// The accounts and sessions, for the middleware and the auth handlers.
    #[must_use]
    pub fn auth(&self) -> &Arc<Auth> {
        &self.auth
    }

    /// Confine file outputs to `data_dir`, or — with `None` — leave them turned
    /// off.
    ///
    /// Canonicalized here, once, rather than on every build: the checks in
    /// [`crate::outputs::file::Root`] compare resolved paths, so a `--data-dir`
    /// that is itself a symlink (`/tmp` on macOS, which is `/private/tmp`)
    /// would otherwise fail every one of them.
    pub fn with_data_dir(mut self, data_dir: Option<PathBuf>) -> anyhow::Result<Self> {
        self.data_dir = Self::resolve_data_dir(data_dir)?;
        Ok(self)
    }

    /// Canonicalize `--data-dir` once, at startup, rather than on every build:
    /// the checks in [`crate::outputs::file::Root`] compare resolved paths, so
    /// a `--data-dir` that is itself a symlink (`/tmp` on macOS, which is
    /// really `/private/tmp`) would otherwise fail every one of them.
    fn resolve_data_dir(data_dir: Option<PathBuf>) -> anyhow::Result<Option<Arc<PathBuf>>> {
        data_dir
            .map(|dir| {
                std::fs::create_dir_all(&dir)
                    .with_context(|| format!("failed to create {}", dir.display()))?;
                let resolved = std::fs::canonicalize(&dir)
                    .with_context(|| format!("failed to resolve {}", dir.display()))?;
                tracing::info!("file outputs may write under {}", resolved.display());
                anyhow::Ok(Arc::new(resolved))
            })
            .transpose()
    }

    /// An empty server with no secrets beyond the environment, saving into
    /// `save_dir`.
    #[must_use]
    pub fn new_in(save_dir: PathBuf) -> Self {
        Self::with_secrets_in(save_dir, Arc::new(EnvStore))
    }

    /// A receiver on the UI feed, and the guard that counts it.
    ///
    /// **The guard has to outlive the receiver's use.** Dropping it is what
    /// tells every run loop the browser went away; dropping it early makes them
    /// all go quiet while the stream is still being served, and never dropping
    /// it makes them all pay the reporting cost forever. Returned as a pair
    /// rather than hidden inside one type because the receiver goes straight
    /// into a `BroadcastStream`, which takes it by value — see
    /// [`crate::handlers::ui::ui::events_handler`] for where the guard then
    /// lives.
    ///
    /// This is the only place a receiver is handed out, which is what makes the
    /// count trustworthy enough for the run loop to read instead of asking the
    /// channel.
    #[must_use]
    pub fn subscribe_events(
        &self,
    ) -> (crate::events::WatchGuard, broadcast::Receiver<UiEvent>) {
        // in this order: a receiver that existed before it was counted is a
        // window in which the run loops still believe nobody is watching
        let guard = self.watchers.attach();
        (guard, self.events.subscribe())
    }

    /// How many browsers are attached to the feed right now.
    #[must_use]
    pub fn watcher_count(&self) -> usize {
        self.watchers.count()
    }

    /// An empty server whose connections come from a file the operator named.
    ///
    /// Used by a `--connections` without a `--config`: there are no pipelines
    /// yet, but the connections a UI-built one can refer to are already there.
    pub fn with_secrets_and_connections(
        secrets: Arc<dyn SecretStore>,
        connections_file: Option<&Path>,
    ) -> anyhow::Result<Self> {
        let mut state = Self::with_secrets(secrets);
        state.connections_file = connections_file.map(Path::to_path_buf);
        state.load_connections_file()?;
        Ok(state)
    }

    /// Resolves secrets from the environment only; see
    /// [`AppState::from_config_with_secrets`].
    pub fn from_config(path: &Path) -> anyhow::Result<Self> {
        Self::from_config_with_secrets(path, Arc::new(EnvStore))
    }

    /// Load the pipelines from `path` and remember it as the default save
    /// target.
    ///
    /// Loading never writes. The file is what the server *started from*; what
    /// it is running diverges from it the moment anything is created or
    /// deleted, and only an explicit save brings the two back together. See
    /// [`crate::persist`].
    pub fn from_config_with_secrets(
        path: &Path,
        secrets: Arc<dyn SecretStore>,
    ) -> anyhow::Result<Self> {
        Self::from_config_with(path, secrets, None, None, Arc::new(History::disabled()))
    }

    /// As [`AppState::from_config_with_secrets`], with the connections file
    /// named outright rather than derived from the config's name, and the
    /// directory file outputs are confined to.
    ///
    /// The connections are loaded **before** the pipelines, and that order is
    /// not incidental: a component names a connection and cannot be built
    /// without it, so a server that read them the other way round would refuse
    /// to start every pipeline in the file.
    ///
    /// `data_dir` is a parameter here rather than the builder
    /// [`AppState::with_data_dir`] for the same reason: loading the config
    /// *starts* its pipelines, so a data dir applied afterwards would arrive
    /// too late for every file output in the file. The builder is for a state
    /// that has no pipelines yet.
    ///
    /// `history` is a parameter for exactly the same reason, one step less
    /// obviously: the pipelines this starts capture it in their `BuildCtx`, so
    /// a [`AppState::with_history`] afterwards would leave every pipeline in
    /// the config file recording into a store nobody reads.
    pub fn from_config_with(
        path: &Path,
        secrets: Arc<dyn SecretStore>,
        connections_file: Option<&Path>,
        data_dir: Option<PathBuf>,
        history: Arc<History>,
    ) -> anyhow::Result<Self> {
        // a bare `config.json` names a file in the working directory, and
        // `parent()` of that is the empty path rather than "."
        let dir = match path.parent() {
            Some(dir) if !dir.as_os_str().is_empty() => dir.to_path_buf(),
            _ => PathBuf::from("."),
        };
        let mut new_state = AppState::with_secrets_in(dir, secrets);
        new_state.history = history;
        new_state.data_dir = Self::resolve_data_dir(data_dir)?;
        new_state.connections_file = connections_file.map(Path::to_path_buf);
        *new_state.lock_config_path() = Some(path.to_path_buf());
        new_state.load_connections_file()?;
        new_state.load_from_config_file()?;
        new_state.load_layout_file()?;
        Ok(new_state)
    }

    /// The connections file this server loads from and saves to: the
    /// `--connections` one if there is one, otherwise the one derived from the
    /// config file's name. `None` only when there is neither — a server started
    /// bare, which acquires one the moment a save gives it a config file.
    #[must_use]
    pub fn connections_path(&self) -> Option<PathBuf> {
        self.connections_file.clone().or_else(|| {
            self.config_path()
                .as_deref()
                .map(crate::connections::connections_path)
        })
    }

    /// Read whatever connections there are. A derived file that isn't there is
    /// no connections at all; one the operator named has to exist.
    fn load_connections_file(&self) -> anyhow::Result<()> {
        let Some(path) = self.connections_path() else {
            return Ok(());
        };
        let connections = if self.connections_file.is_some() {
            tracing::info!("loading connections from {}", path.display());
            crate::connections::read_required(&path)?
        } else {
            crate::connections::read(&path)?
        };
        if !connections.is_empty() {
            tracing::debug!(
                "loaded {} connections from {}",
                connections.len(),
                path.display()
            );
        }
        *self.lock_saved_connections() =
            crate::connections::render(&connections, kayak_core::ConfigFormat::Json).ok();
        *self.lock_connections() = connections;
        Ok(())
    }

    fn lock_connections(&self) -> MutexGuard<'_, Connections> {
        self.connections.lock().unwrap_or_else(|poisoned| {
            tracing::warn!("connections lock was poisoned; recovering");
            poisoned.into_inner()
        })
    }

    fn lock_saved_connections(&self) -> MutexGuard<'_, Option<String>> {
        self.saved_connections.lock().unwrap_or_else(|poisoned| {
            tracing::warn!("saved-connections lock was poisoned; recovering");
            poisoned.into_inner()
        })
    }

    /// The connections as the UI lists them, and as a build reads them.
    #[must_use]
    pub fn connections(&self) -> Connections {
        self.lock_connections().clone()
    }

    /// Add a connection under a name that isn't taken yet.
    ///
    /// Running pipelines are untouched: a connection is read when a component
    /// is built, so this is only about what the *next* build can name. Nothing
    /// is written to disk — that is the explicit save, the same as a pipeline.
    pub fn create_connection(
        &self,
        id: ConnectionId,
        connection: ConnectionKind,
    ) -> Result<(), PipelineError> {
        let mut connections = self.lock_connections();
        if connections.contains(&id) {
            return Err(PipelineError::DuplicateId(id));
        }
        tracing::debug!("connection created: {id} ({})", connection.type_name());
        connections.insert(id, connection);
        Ok(())
    }

    /// Remove a connection, unless a running pipeline still names it.
    ///
    /// Refusing is the useful answer: the pipelines using it keep running
    /// either way — they hold their own settings — but the graph would no
    /// longer describe itself, and the next revert would fail to rebuild them.
    /// Deleting those pipelines first is the honest order to do it in.
    pub fn delete_connection(&self, id: &str) -> Result<(), PipelineError> {
        let used_by = self.pipelines_using(id);
        if !used_by.is_empty() {
            return Err(PipelineError::ConnectionInUse(id.to_string(), used_by));
        }
        let mut connections = self.lock_connections();
        if connections.remove(id).is_none() {
            return Err(PipelineError::NotFound(id.to_string()));
        }
        tracing::debug!("connection deleted: {id}");
        Ok(())
    }

    /// The ids of the running pipelines that name this connection, in id order
    /// so the error reads the same twice.
    #[must_use]
    pub fn pipelines_using(&self, connection_id: &str) -> Vec<PipelineId> {
        let app = self.lock_pipelines();
        let mut used_by: Vec<PipelineId> = app
            .values()
            .filter(|handle| {
                handle
                    .shared
                    .config
                    .connections()
                    .iter()
                    .any(|named| named.as_str() == connection_id)
            })
            .map(|handle| handle.shared.id.clone())
            .collect();
        used_by.sort();
        used_by
    }

    /// Read the arrangement that belongs to the config file, if there is one.
    ///
    /// Separate from [`AppState::load_from_config_file`] because the two fail
    /// differently: a config that won't load means the server has nothing to
    /// run, while a layout that won't load only costs the arrangement. It is
    /// still surfaced rather than swallowed — see [`crate::layout::read`].
    fn load_layout_file(&self) -> anyhow::Result<()> {
        let Some(path) = self.config_path() else {
            return Ok(());
        };
        let path = crate::layout::layout_path(&path);
        let layout = crate::layout::read(&path)?;
        if !layout.is_empty() {
            tracing::debug!(
                "loaded {} card positions from {}",
                layout.pipelines.len(),
                path.display()
            );
        }
        *self.lock_layout() = layout;
        Ok(())
    }

    fn lock_config_path(&self) -> MutexGuard<'_, Option<PathBuf>> {
        self.config_path.lock().unwrap_or_else(|poisoned| {
            tracing::warn!("config-path lock was poisoned; recovering");
            poisoned.into_inner()
        })
    }

    /// The config file this server loads from and saves to, if it has one yet.
    #[must_use]
    pub fn config_path(&self) -> Option<PathBuf> {
        self.lock_config_path().clone()
    }

    fn lock_layout(&self) -> MutexGuard<'_, LayoutFile> {
        self.layout.lock().unwrap_or_else(|poisoned| {
            tracing::warn!("layout lock was poisoned; recovering");
            poisoned.into_inner()
        })
    }

    /// The canvas arrangement, as the UI reads it on load.
    #[must_use]
    pub fn layout(&self) -> LayoutFile {
        self.lock_layout().clone()
    }

    /// Replace the canvas arrangement and write it to disk.
    ///
    /// The write is the point: this is the one thing the UI changes that has no
    /// "save" step, because there is nothing here worth reviewing before it
    /// lands. Without a `--config` file there is nowhere to put it, and the
    /// arrangement lives only as long as the process — which is honest, and
    /// better than refusing to let someone tidy up the canvas.
    pub fn set_layout(&self, layout: LayoutFile) -> Result<(), PipelineError> {
        let mut held = self.lock_layout();
        if let Some(config_path) = self.config_path() {
            let path = crate::layout::layout_path(&config_path);
            crate::layout::write(&path, &layout)
                .with_context(|| format!("failed to save the layout to {}", path.display()))?;
        }
        *held = layout;
        Ok(())
    }

    /// Read the config file and start everything in it. Shared by startup and
    /// by [`AppState::revert`], which is the same operation done twice.
    fn load_from_config_file(&self) -> anyhow::Result<()> {
        let Some(path) = self.config_path() else {
            anyhow::bail!("the server has no config file");
        };
        tracing::debug!("Loading configuration from {}...", path.display());
        let file = crate::persist::read(&path)?;

        // the buckets have to exist before the pipelines that name them are
        // built, for the same reason the connections do
        self.adopt_buckets(file.state);

        let mut app = self.lock_pipelines();
        for c in file.pipelines {
            let id = c.id.clone().unwrap_or_else(|| "<generated>".to_string());
            Self::create_locked(self, &mut app, c)
                .with_context(|| format!("failed to start pipeline '{id}' from config"))?;
        }
        // what came off disk is, by definition, in sync with disk
        self.mark_saved(&app);
        Ok(())
    }

    /// The token that says the process has been asked to stop.
    ///
    /// Handed out so a long-lived response can end itself; the `/events` SSE
    /// stream is the one that must, since it never completes on its own and
    /// axum's graceful shutdown waits for every open connection. See
    /// [`crate::shutdown`].
    #[must_use]
    pub fn shutdown_token(&self) -> tokio_util::sync::CancellationToken {
        self.shutdown.clone()
    }

    /// Cancel every run loop and wait for the loops to end.
    ///
    /// Returns whether they all stopped inside [`TEARDOWN_GRACE`]. The map is
    /// drained first, so the graph is empty from the caller's point of view
    /// even if a straggler is still unwinding — which is what lets [`revert`]
    /// rebuild on top and [`shutdown`] carry on to exit.
    ///
    /// The awkward shape is load-bearing and is the reason this is one method
    /// rather than two lines at each call site: `pipelines` is a
    /// `std::sync::Mutex`, so the cancelling and the taking-out happen under
    /// the guard, the guard is dropped, and only *then* is anything awaited.
    ///
    /// [`revert`]: AppState::revert
    /// [`shutdown`]: AppState::shutdown
    pub async fn stop_pipelines(&self) -> bool {
        let waiting: Vec<tokio::task::JoinHandle<()>> = {
            let mut app = self.lock_pipelines();
            app.drain()
                .map(|(_, handle)| {
                    handle.shared.cancellation_token.cancel();
                    handle.join_handle
                })
                .collect()
        };

        // A run loop checks its cancellation on every iteration, so this is
        // normally immediate. The bound is for the one thing that can't be
        // cancelled — an output already inside `emit()`, waiting on a socket.
        // Timing out is not fatal: the stragglers are cancelled and will exit
        // on their own.
        tokio::time::timeout(TEARDOWN_GRACE, async {
            for handle in waiting {
                let _ = handle.await;
            }
        })
        .await
        .is_ok()
    }

    /// Begin shutting down: end the long-lived responses, but leave the graph
    /// running.
    ///
    /// Separate from [`AppState::shutdown`] because the two happen either side
    /// of axum's drain, and in that order — the streams have to end *before*
    /// the connections are waited on, or the wait never finishes. Idempotent:
    /// cancelling an already-cancelled token does nothing.
    pub fn begin_shutdown(&self) {
        self.shutdown.cancel();
    }

    /// Stop the graph on the way out of the process.
    ///
    /// The thing this buys, and the whole reason the signal handling exists, is
    /// that every output gets its [`finish`] — the one chance a `file` output
    /// has to close its `json_array` and the only chance the `s3` output has to
    /// upload a part that exists nowhere but in memory. A `^C` used to lose
    /// that part outright.
    ///
    /// Deliberately says nothing about *saving*. The config file is a load
    /// source and a save target and never a mirror, and a process writing the
    /// graph out because it happened to be killed would be exactly the mirror
    /// that rule exists to prevent. Unsaved changes stay unsaved.
    ///
    /// [`finish`]: crate::outputs::OutputDestination::finish
    pub async fn shutdown(&self) {
        self.begin_shutdown();
        if self.stop_pipelines().await {
            tracing::info!("all pipelines stopped");
        } else {
            tracing::warn!(
                "some pipelines were still stopping after {}s; exiting anyway",
                TEARDOWN_GRACE.as_secs()
            );
        }
    }

    /// Throw away the running graph and start again from the config file.
    ///
    /// This is the undo that the file being read-only otherwise takes away:
    /// edits apply to the runtime immediately, so reloading is the only way
    /// back to a known state.
    ///
    /// The file is parsed *before* anything is torn down, so the common failure
    /// — a file that has since been broken by hand — leaves the running graph
    /// alone. A pipeline that parses but won't build still can't be caught that
    /// way; that one fails partway through, and the error says so.
    ///
    /// The old graph is stopped *completely* before the new one is built. That
    /// matters beyond tidiness: two run loops for the same pipeline would
    /// briefly share a kafka consumer group or a nats subscription, and both
    /// would write to the same outputs.
    pub async fn revert(&self) -> anyhow::Result<()> {
        let Some(path) = self.config_path() else {
            anyhow::bail!(
                "the server has no config file, so there is nothing to revert to; save one first"
            );
        };
        // parse first: it costs one read and saves tearing down a working graph
        // for a file that was never going to load. Both files, since either one
        // being broken by hand would strand the rebuild halfway.
        let _ = crate::persist::read(&path)?;
        if let Some(connections) = self.connections_path() {
            let _ = crate::connections::read(&connections)?;
        }

        if !self.stop_pipelines().await {
            tracing::warn!(
                "some pipelines were still shutting down after {}s; reloading anyway",
                TEARDOWN_GRACE.as_secs()
            );
        }
        // The graph is gone, so no endpoint belongs to anything any more. A
        // straggler still holding one would fail the rebuild of the pipeline
        // that takes its id — and its own `Drop`, arriving later, is a no-op
        // once the entry it knows about has been replaced.
        self.inboxes.clear();

        // back to what is on disk means both files, and the connections have to
        // land first: the pipelines about to be rebuilt name them
        self.load_connections_file().context(
            "the running pipelines were stopped, but the connections file could not be reloaded",
        )?;
        self.load_from_config_file().context(
            "the running pipelines were stopped, but the config file could not be reloaded",
        )?;
        // Reverting is "go back to what is on disk", and the arrangement is on
        // disk too. It only warns: the graph is already running by this point,
        // and refusing the revert over a cosmetic file would be the wrong trade.
        if let Err(err) = self.load_layout_file() {
            tracing::warn!("reverted the pipelines, but could not reload the layout: {err:#}");
        }
        Ok(())
    }

    /// The live buckets, as a snapshot a build can hold.
    #[must_use]
    pub fn buckets(&self) -> Arc<Buckets> {
        Arc::clone(&self.lock_buckets())
    }

    /// The buckets as the config declares them — what a save writes back out.
    #[must_use]
    pub fn declared_buckets(&self) -> StateBuckets {
        self.lock_declared_buckets().clone()
    }

    /// Take on a config file's `state` section, keeping what the buckets that
    /// are unchanged already hold.
    ///
    /// Called by a load and by a revert, which is the same operation twice —
    /// and the reason the contents survive one is that a revert rebuilds every
    /// pipeline in the graph, so emptying the buckets would make an edit to an
    /// unrelated pipeline cost an hour of accumulated state.
    fn adopt_buckets(&self, declared: StateBuckets) {
        let rebuilt = {
            let current = self.lock_buckets();
            current.rebuilt(&declared)
        };
        *self.lock_buckets() = Arc::new(rebuilt);
        *self.lock_declared_buckets() = declared;
    }

    fn lock_buckets(&self) -> MutexGuard<'_, Arc<Buckets>> {
        self.buckets.lock().unwrap_or_else(|poisoned| {
            tracing::warn!("buckets lock was poisoned; recovering");
            poisoned.into_inner()
        })
    }

    fn lock_declared_buckets(&self) -> MutexGuard<'_, StateBuckets> {
        self.declared_buckets.lock().unwrap_or_else(|poisoned| {
            tracing::warn!("declared buckets lock was poisoned; recovering");
            poisoned.into_inner()
        })
    }

    /// A poisoned lock means another thread panicked while holding it. None of
    /// the code under this lock can leave the map half-updated, so recovering
    /// the guard is safe and keeps one panic from taking down every request.
    fn lock_pipelines(&self) -> MutexGuard<'_, HashMap<PipelineId, PipelineHandle>> {
        self.pipelines.lock().unwrap_or_else(|poisoned| {
            tracing::warn!("pipelines lock was poisoned; recovering");
            poisoned.into_inner()
        })
    }

    pub fn get_pipeline_ids(&self) -> Vec<PipelineId> {
        self.lock_pipelines().keys().cloned().collect()
    }

    /// The name of the config file the server is working against, if it has one
    /// yet. The UI offers it as the default save target; without it, the UI
    /// offers to create one instead.
    #[must_use]
    pub fn config_file_name(&self) -> Option<String> {
        self.config_path()
            .as_deref()
            .and_then(Path::file_name)
            .map(|name| name.to_string_lossy().into_owned())
    }

    /// The directory a `script` transform's `file` source resolves against: the
    /// directory the config file is in, or `None` when there is no config file.
    ///
    /// Deliberately *not* [`AppState::save_dir`], which falls back to the
    /// process's working directory when there is no config. That fallback is
    /// right for saving — somewhere has to be the place a first save lands —
    /// and wrong for reading, where it would make the boundary depend on where
    /// the server was launched from. See
    /// [`crate::transforms::script::source`].
    #[must_use]
    pub fn script_directory(&self) -> Option<PathBuf> {
        self.config_path()
            .and_then(|path| path.parent().map(Path::to_path_buf))
    }

    /// The directory a save writes into. Fixed for the life of the process.
    #[must_use]
    pub fn save_directory(&self) -> &Path {
        &self.save_dir
    }

    /// Whether the running graph has diverged from what was last loaded or
    /// saved.
    ///
    /// Rendering both sides and comparing is exact rather than a heuristic,
    /// because [`crate::persist::render`] is deterministic: the same graph
    /// always produces the same bytes, so a difference *is* a change. Always
    /// false without a config file — there is nothing to be out of sync with.
    #[must_use]
    pub fn has_unsaved_changes(&self) -> bool {
        if self.config_path().is_none() {
            return false;
        }
        if self.connections_changed() {
            return true;
        }
        let app = self.lock_pipelines();
        let current = self.fingerprint(&app);
        let saved = self.lock_saved();
        match (&current, &*saved) {
            (Some(current), Some(saved)) => current != saved,
            // a render that failed says nothing either way; claiming "unsaved"
            // is the answer that can't cost someone their work
            _ => true,
        }
    }

    /// Whether the connections have diverged from the file, by the same
    /// rendered-bytes comparison the pipelines use.
    ///
    /// It counts as an unsaved change for the same reason a pipeline does: a
    /// connection added in the UI is something the running server can build
    /// against, and a restart without a save would lose it.
    fn connections_changed(&self) -> bool {
        let current =
            crate::connections::render(&self.lock_connections(), kayak_core::ConfigFormat::Json)
                .ok();
        let saved = self.lock_saved_connections();
        match (&current, &*saved) {
            (Some(current), Some(saved)) => current != saved,
            // nothing loaded and nothing added is not a change; anything else
            // that can't be rendered errs towards "unsaved"
            (Some(current), None) => current.trim() != "{}",
            _ => true,
        }
    }

    fn lock_saved(&self) -> MutexGuard<'_, Option<String>> {
        self.saved.lock().unwrap_or_else(|poisoned| {
            tracing::warn!("saved-snapshot lock was poisoned; recovering");
            poisoned.into_inner()
        })
    }

    /// Record the current graph as the one on disk. Takes the held guard so the
    /// snapshot can't be of a map that changed in between.
    fn mark_saved(&self, app: &HashMap<PipelineId, PipelineHandle>) {
        *self.lock_saved() = self.fingerprint(app);
    }

    /// The graph rendered to bytes, for comparing one moment against another.
    ///
    /// Deliberately always JSON: it stands in for "which pipelines, wired how",
    /// and the answer to that doesn't change with the format a file happens to
    /// be written in. `None` if it can't be rendered, which callers read as
    /// "can't tell" rather than as an answer.
    /// Takes `&self` because the fingerprint covers the *file*, and the buckets
    /// are part of it: declaring one is an unsaved change like adding a
    /// pipeline is.
    fn fingerprint(&self, app: &HashMap<PipelineId, PipelineHandle>) -> Option<String> {
        crate::persist::render(
            ConfigFile::new(self.declared_buckets(), Self::configs_of(app)),
            kayak_core::ConfigFormat::Json,
        )
        .ok()
    }

    /// Write the running graph to `name`, a file in the server's save
    /// directory, and treat that as the new baseline.
    ///
    /// Only a bare file name is accepted: this is a write to the server's disk
    /// driven by an HTTP request, and confining it to one known directory is
    /// what keeps that from being an arbitrary-write primitive. Passing the
    /// current file's own name is how you overwrite it.
    ///
    /// A server started **without** a `--config` file saves too, and that save
    /// is what gives it one: the graph someone built in the UI becomes a file,
    /// and the file becomes what `revert` reloads and "unsaved changes" is
    /// measured against. Without this, an afternoon in the UI could only ever
    /// end in a restart that threw it away. It is an *acquisition*, not a
    /// switch — a save under a new name on a server that already has a file
    /// leaves that file as the loaded one, as it always did.
    ///
    /// `format` is what the caller asked for; `None` means "whatever `name`
    /// says", so a client that doesn't know about the choice still writes YAML
    /// into a file called `.yaml`.
    pub fn save_config_as(
        &self,
        name: &str,
        format: Option<ConfigFormat>,
        overwrite: bool,
    ) -> Result<PathBuf, PipelineError> {
        let target = crate::persist::save_path(&self.save_dir, name)
            .map_err(PipelineError::InvalidConfig)?;
        if !overwrite {
            let existing = self.files_written_for(&target);
            if !existing.is_empty() {
                return Err(PipelineError::ConfigExists(existing));
            }
        }
        let format = format.unwrap_or_else(|| crate::persist::format_of(&target));

        let app = self.lock_pipelines();
        let configs = Self::configs_of(&app);
        // saved anyway — refusing would strand the user's work — but the file
        // won't start as it is, so it can't pass silently
        let dangling = crate::persist::dangling_upstreams(&configs);
        if !dangling.is_empty() {
            tracing::warn!(
                "saving {} with upstreams that no longer exist: {}; those pipelines will not start",
                target.display(),
                dangling.join(", ")
            );
        }
        // the same warning for the other file: a pipeline naming a connection
        // that isn't configured is saved, and won't start
        let unknown = self.unknown_connections(&configs);
        if !unknown.is_empty() {
            tracing::warn!(
                "saving {} with connections that are not configured: {}; those pipelines will not start",
                target.display(),
                unknown.join(", ")
            );
        }
        crate::persist::write(
            &target,
            ConfigFile::new(self.declared_buckets(), configs),
            format,
        )
        .with_context(|| format!("failed to save the config to {}", target.display()))?;
        self.mark_saved(&app);
        drop(app);
        self.adopt(&target);
        // after `adopt`, so a server that had no config file until a moment ago
        // now has somewhere to derive the connections file from
        self.save_connections()?;
        tracing::info!("config saved to {} as {format}", target.display());
        Ok(target)
    }

    /// Which of the files a save under `target` would claim are already there.
    ///
    /// A save writes up to three files — the config, the connections beside it
    /// and the canvas layout — so a *create* has to ask about all of them.
    /// Checking only the config would let a create called `config.json` write
    /// `{}` over an existing `config.connections.json`, which is the same data
    /// loss one file along.
    ///
    /// The exception is a `--connections` file the operator named on the
    /// command line: it is fixed for the process and shared deliberately
    /// between configs, so an existing one is the arrangement working rather
    /// than a collision. Only the *derived* sibling is a claim on a name.
    ///
    /// In save order, so the message names the file that was typed first. This
    /// is a check and not a guarantee — nothing stops a file appearing between
    /// here and the write — but the gap is microseconds against a person
    /// choosing a name, and the alternative (`create_new` on the temp file)
    /// cannot cover the rename or the sibling files anyway.
    fn files_written_for(&self, target: &Path) -> Vec<PathBuf> {
        let derived_connections = self
            .connections_file
            .is_none()
            .then(|| crate::connections::connections_path(target));
        [
            Some(target.to_path_buf()),
            derived_connections,
            Some(crate::layout::layout_path(target)),
        ]
        .into_iter()
        .flatten()
        .filter(|path| path.exists())
        .collect()
    }

    /// Write the connections out beside the config, and treat that as the new
    /// baseline.
    ///
    /// Part of the same save rather than a button of its own: the two files
    /// describe one system, and a config saved without the connections it names
    /// is a config that won't start. The path is not the caller's to choose —
    /// it is the `--connections` file or the one derived from the config's
    /// name, so a save-as of the pipelines does not scatter connection files
    /// around.
    fn save_connections(&self) -> Result<(), PipelineError> {
        let Some(path) = self.connections_path() else {
            return Ok(());
        };
        let connections = self.connections();
        // an empty set that was never on disk needs no file: writing `{}` into
        // a repository that has no connections would be noise
        if connections.is_empty() && !path.exists() {
            return Ok(());
        }
        let format = crate::persist::format_of(&path);
        crate::connections::write(&path, &connections, format)
            .with_context(|| format!("failed to save the connections to {}", path.display()))?;
        *self.lock_saved_connections() =
            crate::connections::render(&connections, kayak_core::ConfigFormat::Json).ok();
        tracing::info!("connections saved to {}", path.display());
        Ok(())
    }

    /// The connections these pipelines name that no connection is configured
    /// for, in name order and without repeats. The counterpart of
    /// [`crate::persist::dangling_upstreams`], across the two files.
    fn unknown_connections(&self, configs: &[Config]) -> Vec<ConnectionId> {
        let connections = self.lock_connections();
        let mut unknown: Vec<ConnectionId> = configs
            .iter()
            .flat_map(Config::connections)
            .filter(|id| !connections.contains(id))
            .cloned()
            .collect();
        unknown.sort();
        unknown.dedup();
        unknown
    }

    /// Make a just-written file the server's config file, if it hasn't got one.
    ///
    /// The arrangement goes with it: cards dragged around before there was
    /// anywhere to put them have been living in memory only, and this is the
    /// first moment they have a home. Failing to write it only warns — the
    /// pipelines are already safely on disk, and losing the save over the
    /// cosmetic half would be the wrong trade.
    fn adopt(&self, target: &Path) {
        let mut path = self.lock_config_path();
        if path.is_some() {
            return;
        }
        *path = Some(target.to_path_buf());
        tracing::info!("now working against {}", target.display());
        drop(path);

        let layout = self.lock_layout().clone();
        if layout.is_empty() {
            return;
        }
        let layout_path = crate::layout::layout_path(target);
        if let Err(err) = crate::layout::write(&layout_path, &layout) {
            tracing::warn!(
                "saved the pipelines, but could not write the layout to {}: {err:#}",
                layout_path.display()
            );
        }
    }

    /// Every running pipeline's config, with the id filled in.
    ///
    /// A config posted without an `id` got a random petname, and that name is
    /// the only thing a downstream pipeline's `upstream` reference can point
    /// at — so the *resolved* id is what gets written, not the absent one.
    fn configs_of(app: &HashMap<PipelineId, PipelineHandle>) -> Vec<Config> {
        app.values()
            .map(|handle| Config {
                id: Some(handle.shared.id.clone()),
                ..handle.shared.config.clone()
            })
            .collect()
    }

    pub fn get_pipelines(&self) -> anyhow::Result<serde_json::Value> {
        // the views borrow from the handles, so the guard has to outlive them
        let app = self.lock_pipelines();
        let views: Vec<_> = app.values().map(|h| h.shared.view()).collect();
        serde_json::to_value(views).context("failed to serialize pipelines")
    }

    pub fn create_pipeline(&self, config: Config) -> Result<Arc<Pipeline>, PipelineError> {
        let mut app = self.lock_pipelines();
        Self::create_locked(self, &mut app, config)
    }

    /// The body of [`AppState::create_pipeline`], against a guard the caller
    /// already holds. Loading a whole config file is a run of these under one
    /// lock, so it can't interleave with a request halfway through.
    fn create_locked(
        &self,
        app: &mut HashMap<PipelineId, PipelineHandle>,
        config: Config,
    ) -> Result<Arc<Pipeline>, PipelineError> {
        let pipeline = Arc::new(Pipeline::new(config)?);
        let id = pipeline.id.clone();
        // we require unique ids, so if this id already exists we should error out
        if app.contains_key(id.as_str()) {
            return Err(PipelineError::DuplicateId(id));
        }
        // a snapshot, taken before the map is borrowed: what this pipeline is
        // built from is the connections as they stand now, and editing one
        // later doesn't reach back into it
        let connections = Arc::new(self.connections());
        let ctx = BuildCtx::with_secrets(
            app,
            id.clone(),
            self.events.clone(),
            Arc::clone(&self.secrets),
        )
        .with_connections(connections)
        .with_data_dir(self.data_dir.clone())
        .with_inboxes(Arc::clone(&self.inboxes))
        .with_buckets(self.buckets())
        .with_state(pipeline.config.state.clone())
        .with_history(Arc::clone(&self.history))
        .with_watchers(self.watchers.clone())
        // Read off `config_path` on every build rather than held, because the
        // path is not fixed: a server started without one adopts the file it is
        // first saved as, and from that save on a file-sourced script resolves
        // beside it.
        .with_script_dir(self.script_directory().map(Arc::new));
        // building the runtime only fails on things the config got wrong
        // (unknown upstream, unbuildable component)
        let join_handle = pipeline.start(ctx).map_err(|e| {
            PipelineError::InvalidConfig(e.context(format!("failed to start pipeline '{id}'")))
        })?;

        let pipeline_handle = PipelineHandle {
            join_handle,
            shared: Arc::clone(&pipeline),
        };
        app.insert(id, pipeline_handle);
        tracing::debug!("pipeline created: {}", pipeline.id);
        Ok(pipeline)
    }

    /// Builds one input the way a pipeline would, for a sample — see
    /// [`crate::handlers::rest::sample`].
    ///
    /// The *real* build, through the same `BuildCtx` [`AppState::create_locked`]
    /// assembles, because a sample built any other way could disagree with the
    /// pipeline it is standing in for and would then be worse than none. The
    /// only difference is the id, which is a throwaway rather than a
    /// pipeline's: it is what an input derives a client id from, and a sample
    /// must not turn up at a broker claiming to be a pipeline that already
    /// exists.
    ///
    /// Nothing is registered and nothing is spawned. The caller owns the input
    /// and drops it, which is what ends the subscription.
    pub fn build_sample_input(
        &self,
        id: &PipelineId,
        input: kayak_core::config::InputConfig,
    ) -> anyhow::Result<Box<dyn crate::inputs::InputSource>> {
        use crate::config::BuildInputConfig;
        // the pipelines lock before the connections lock, as everywhere else —
        // and held across the build because a `pipeline` input subscribes to
        // its upstream through the map. It is dropped with this guard, well
        // before anything is awaited.
        let mut app = self.lock_pipelines();
        let connections = Arc::new(self.connections());
        let mut ctx = BuildCtx::with_secrets(
            &mut app,
            id.clone(),
            self.events.clone(),
            Arc::clone(&self.secrets),
        )
        .with_connections(connections)
        .with_data_dir(self.data_dir.clone())
        .with_inboxes(Arc::clone(&self.inboxes))
        .with_buckets(self.buckets())
        .with_history(Arc::clone(&self.history))
        .with_watchers(self.watchers.clone())
        .with_script_dir(self.script_directory().map(Arc::new));
        input.build(&mut ctx)
    }

    pub fn delete_pipeline(&self, id: &str) -> Result<(), PipelineError> {
        let mut app = self.lock_pipelines();
        let Some(handle) = app.remove(id) else {
            tracing::debug!("failed to delete pipeline: {} (not found)", id);
            return Err(PipelineError::NotFound(id.to_string()));
        };
        // signal cancellation here; the run loop drops out on its own
        handle.shared.cancellation_token.cancel();
        // ...but the endpoint goes now rather than whenever that task gets
        // round to dropping the input, so a post that arrives after the delete
        // returns 404 instead of disappearing into a pipeline on its way out.
        // Done under the pipelines lock, which is what makes eviction by name
        // safe — nothing can have re-registered this id yet.
        self.inboxes.evict(id);
        tracing::debug!("pipeline deleted: {}", id);
        Ok(())
    }

    /// Hand posted messages to a pipeline's `http` input, as one batch.
    ///
    /// Returns how many messages were queued for the run loop — *accepted*, not
    /// processed: nothing here waits for the pipeline to work through them.
    ///
    /// An empty post is a no-op that still has to say whether anyone was
    /// listening, so it asks without sending. A pipeline that isn't accepting
    /// posts is not the same thing as one that doesn't exist, and the two are
    /// told apart here rather than at the HTTP layer — both are a 404, but only
    /// one of them is fixed by creating the pipeline.
    ///
    /// `credentials` are what the request presented, unfiltered; whether any of
    /// it is *wanted* is the input's business and is answered inside the inbox
    /// registry, where the requirement lives and dies with the endpoint.
    ///
    /// Note what the three answers give away to a caller with no credentials: a
    /// protected pipeline 401s, an unprotected one 202s and a missing one 404s,
    /// so the status code says whether a pipeline exists and whether it is
    /// guarded. Unavoidable while the credential is per-pipeline — the pipeline
    /// has to be found before its requirement can be read — and acceptable
    /// here, where the ids are on a canvas anyway.
    pub fn ingest(
        &self,
        id: &str,
        messages: Vec<serde_json::Value>,
        meta: PostMeta,
        credentials: &Credentials,
    ) -> Result<usize, PipelineError> {
        let accepted = messages.len();
        // the inbox lock is taken and released inside these, so classifying the
        // failure below can take the pipelines lock without holding it
        let sent = if accepted == 0 {
            self.inboxes.check(id, credentials)
        } else {
            let batch = Arc::new(messages.into_iter().map(Arc::new).collect());
            self.inboxes.send(id, batch, meta, credentials)
        };
        match sent {
            Ok(()) => Ok(accepted),
            Err(IngestError::Full(_)) => Err(PipelineError::Backpressure(id.to_string())),
            Err(IngestError::Unauthorized(_)) => Err(PipelineError::Unauthorized(id.to_string())),
            Err(IngestError::NoInbox(_)) => {
                if self.lock_pipelines().contains_key(id) {
                    Err(PipelineError::NotAccepting(id.to_string()))
                } else {
                    Err(PipelineError::NotFound(id.to_string()))
                }
            }
        }
    }
}
