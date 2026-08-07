use anyhow::Context;
use serde::Serialize;
use tokio::sync::broadcast;

use crate::secrets::{EnvStore, SecretStore};
use crate::streamer::Streamer;
use crate::BuildCtx;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use streamer_core::config::Config;
pub use streamer_core::{ConfigFormat, LayoutFile, StreamerId, UiEvent};

#[derive(Serialize)]
pub struct StreamerHandle {
    #[serde(skip)]
    pub join_handle: tokio::task::JoinHandle<()>,
    pub shared: Arc<Streamer>,
}

/// Errors that callers need to distinguish between — the HTTP layer maps these
/// onto status codes, everything else becomes a 500.
#[derive(Debug)]
pub enum StreamerError {
    NotFound(StreamerId),
    DuplicateId(StreamerId),
    /// The config parsed, but describes a pipeline that can't be built — e.g.
    /// it names an upstream streamer that doesn't exist. That's the caller's
    /// mistake, not ours.
    InvalidConfig(anyhow::Error),
    Internal(anyhow::Error),
}

impl std::fmt::Display for StreamerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound(id) => write!(f, "streamer with id '{id}' not found"),
            Self::DuplicateId(id) => write!(f, "streamer with id '{id}' already exists"),
            // only this layer — the rest of the chain is reachable through
            // source(), so printing it here too would duplicate it
            Self::InvalidConfig(err) | Self::Internal(err) => write!(f, "{err}"),
        }
    }
}

impl std::error::Error for StreamerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            // Display above already prints the inner error's own layer, so the
            // chain continues from *its* source, not from the error itself
            Self::InvalidConfig(err) | Self::Internal(err) => (**err).source(),
            _ => None,
        }
    }
}

impl From<anyhow::Error> for StreamerError {
    fn from(err: anyhow::Error) -> Self {
        Self::Internal(err)
    }
}

impl From<serde_json::Error> for StreamerError {
    fn from(err: serde_json::Error) -> Self {
        Self::Internal(err.into())
    }
}

/// How long a revert waits for the old run loops to finish before rebuilding on
/// top of them. Only an output stuck mid-`emit()` can take this long; anything
/// else notices its cancellation on the next loop iteration.
const TEARDOWN_GRACE: std::time::Duration = std::time::Duration::from_secs(5);

pub struct AppState {
    streamers: Mutex<HashMap<StreamerId, StreamerHandle>>,
    events: broadcast::Sender<UiEvent>,
    /// What `${NAME}` references in an incoming config resolve against. Held
    /// here rather than passed per request because it's fixed at startup — a
    /// pipeline posted to `/api/streams` gets the same secrets as one loaded
    /// from the config file.
    secrets: Arc<dyn SecretStore>,
    /// The `--config` file, when the server was started with one.
    ///
    /// It is a *load source and a save target*, not a mirror: nothing here
    /// writes to it except an explicit save. Its directory also bounds where a
    /// save is allowed to write. See [`crate::persist`].
    config_path: Option<PathBuf>,
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
    /// a node changes nothing about the running system, so making someone save
    /// it would be ceremony over a cosmetic act. See [`crate::layout`].
    layout: Mutex<LayoutFile>,
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

    pub fn with_secrets(secrets: Arc<dyn SecretStore>) -> Self {
        tracing::debug!("Initializing empty server state...");
        let (events, _) = broadcast::channel(1024);
        Self {
            streamers: Mutex::new(HashMap::new()),
            events,
            secrets,
            config_path: None,
            saved: Mutex::new(None),
            layout: Mutex::new(LayoutFile::default()),
        }
    }

    pub fn subscribe_events(&self) -> broadcast::Receiver<UiEvent> {
        self.events.subscribe()
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
        let mut new_state = AppState::with_secrets(secrets);
        new_state.config_path = Some(path.to_path_buf());
        new_state.load_from_config_file()?;
        new_state.load_layout_file()?;
        Ok(new_state)
    }

    /// Read the arrangement that belongs to the config file, if there is one.
    ///
    /// Separate from [`AppState::load_from_config_file`] because the two fail
    /// differently: a config that won't load means the server has nothing to
    /// run, while a layout that won't load only costs the arrangement. It is
    /// still surfaced rather than swallowed — see [`crate::layout::read`].
    fn load_layout_file(&self) -> anyhow::Result<()> {
        let Some(path) = &self.config_path else {
            return Ok(());
        };
        let path = crate::layout::layout_path(path);
        let layout = crate::layout::read(&path)?;
        if !layout.is_empty() {
            tracing::debug!(
                "loaded {} card positions from {}",
                layout.nodes.len(),
                path.display()
            );
        }
        *self.lock_layout() = layout;
        Ok(())
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
    pub fn set_layout(&self, layout: LayoutFile) -> Result<(), StreamerError> {
        let mut held = self.lock_layout();
        if let Some(config_path) = &self.config_path {
            let path = crate::layout::layout_path(config_path);
            crate::layout::write(&path, &layout)
                .with_context(|| format!("failed to save the layout to {}", path.display()))?;
        }
        *held = layout;
        Ok(())
    }

    /// Read the config file and start everything in it. Shared by startup and
    /// by [`AppState::revert`], which is the same operation done twice.
    fn load_from_config_file(&self) -> anyhow::Result<()> {
        let Some(path) = &self.config_path else {
            anyhow::bail!("the server was not started with a --config file");
        };
        tracing::debug!("Loading configuration from {}...", path.display());
        let configs = crate::persist::read(path)?;

        let mut app = self.lock_streamers();
        for c in configs {
            let id = c.id.clone().unwrap_or_else(|| "<generated>".to_string());
            Self::create_locked(self, &mut app, c)
                .with_context(|| format!("failed to start streamer '{id}' from config"))?;
        }
        // what came off disk is, by definition, in sync with disk
        self.mark_saved(&app);
        Ok(())
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
        let Some(path) = &self.config_path else {
            anyhow::bail!("the server was not started with a --config file, so there is nothing to revert to");
        };
        // parse first: it costs one read and saves tearing down a working graph
        // for a file that was never going to load
        let _ = crate::persist::read(path)?;

        // Cancel everything and take the join handles out, then drop the guard:
        // this is a `std::sync::Mutex` and must not be held across an await.
        let waiting: Vec<tokio::task::JoinHandle<()>> = {
            let mut app = self.lock_streamers();
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
        // on their own, and rebuilding on top of them is still better than
        // refusing to revert.
        let stopped = tokio::time::timeout(TEARDOWN_GRACE, async {
            for handle in waiting {
                let _ = handle.await;
            }
        })
        .await;
        if stopped.is_err() {
            tracing::warn!(
                "some pipelines were still shutting down after {}s; reloading anyway",
                TEARDOWN_GRACE.as_secs()
            );
        }

        self.load_from_config_file()
            .context("the running pipelines were stopped, but the config file could not be reloaded")?;
        // Reverting is "go back to what is on disk", and the arrangement is on
        // disk too. It only warns: the graph is already running by this point,
        // and refusing the revert over a cosmetic file would be the wrong trade.
        if let Err(err) = self.load_layout_file() {
            tracing::warn!("reverted the pipelines, but could not reload the layout: {err:#}");
        }
        Ok(())
    }

    /// A poisoned lock means another thread panicked while holding it. None of
    /// the code under this lock can leave the map half-updated, so recovering
    /// the guard is safe and keeps one panic from taking down every request.
    fn lock_streamers(&self) -> MutexGuard<'_, HashMap<StreamerId, StreamerHandle>> {
        self.streamers.lock().unwrap_or_else(|poisoned| {
            tracing::warn!("streamers lock was poisoned; recovering");
            poisoned.into_inner()
        })
    }

    pub fn get_streamer_ids(&self) -> Vec<StreamerId> {
        self.lock_streamers().keys().cloned().collect()
    }

    /// The name of the file the server was started from, if any. The UI offers
    /// it as the default save target, and its directory is the only place a
    /// save is allowed to write.
    #[must_use]
    pub fn config_file_name(&self) -> Option<String> {
        self.config_path
            .as_ref()
            .and_then(|p| p.file_name())
            .map(|name| name.to_string_lossy().into_owned())
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
        if self.config_path.is_none() {
            return false;
        }
        let app = self.lock_streamers();
        let current = Self::fingerprint(&app);
        let saved = self.lock_saved();
        match (&current, &*saved) {
            (Some(current), Some(saved)) => current != saved,
            // a render that failed says nothing either way; claiming "unsaved"
            // is the answer that can't cost someone their work
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
    fn mark_saved(&self, app: &HashMap<StreamerId, StreamerHandle>) {
        *self.lock_saved() = Self::fingerprint(app);
    }

    /// The graph rendered to bytes, for comparing one moment against another.
    ///
    /// Deliberately always JSON: it stands in for "which pipelines, wired how",
    /// and the answer to that doesn't change with the format a file happens to
    /// be written in. `None` if it can't be rendered, which callers read as
    /// "can't tell" rather than as an answer.
    fn fingerprint(app: &HashMap<StreamerId, StreamerHandle>) -> Option<String> {
        crate::persist::render(Self::configs_of(app), streamer_core::ConfigFormat::Json).ok()
    }

    /// Write the running graph to `name`, a file beside the one the server was
    /// started from, and treat that as the new baseline.
    ///
    /// Only a bare file name is accepted: this is a write to the server's disk
    /// driven by an HTTP request, and confining it to one known directory is
    /// what keeps that from being an arbitrary-write primitive. Passing the
    /// current file's own name is how you overwrite it.
    ///
    /// `format` is what the caller asked for; `None` means "whatever `name`
    /// says", so a client that doesn't know about the choice still writes YAML
    /// into a file called `.yaml`.
    pub fn save_config_as(
        &self,
        name: &str,
        format: Option<ConfigFormat>,
    ) -> Result<PathBuf, StreamerError> {
        let Some(config_path) = &self.config_path else {
            return Err(StreamerError::InvalidConfig(anyhow::anyhow!(
                "the server was not started with a --config file, so there is nowhere to save"
            )));
        };
        let target = crate::persist::save_path(config_path, name)
            .map_err(StreamerError::InvalidConfig)?;
        let format = format.unwrap_or_else(|| crate::persist::format_of(&target));

        let app = self.lock_streamers();
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
        crate::persist::write(&target, configs, format)
            .with_context(|| format!("failed to save the config to {}", target.display()))?;
        self.mark_saved(&app);
        tracing::info!("config saved to {} as {format}", target.display());
        Ok(target)
    }

    /// Every running pipeline's config, with the id filled in.
    ///
    /// A config posted without an `id` got a random petname, and that name is
    /// the only thing a downstream pipeline's `upstream` reference can point
    /// at — so the *resolved* id is what gets written, not the absent one.
    fn configs_of(app: &HashMap<StreamerId, StreamerHandle>) -> Vec<Config> {
        app.values()
            .map(|handle| Config {
                id: Some(handle.shared.id.clone()),
                ..handle.shared.config.clone()
            })
            .collect()
    }

    pub fn get_streamers(&self) -> anyhow::Result<serde_json::Value> {
        // the views borrow from the handles, so the guard has to outlive them
        let app = self.lock_streamers();
        let views: Vec<_> = app.values().map(|h| h.shared.view()).collect();
        serde_json::to_value(views).context("failed to serialize streamers")
    }

    pub fn create_streamer(&self, config: Config) -> Result<Arc<Streamer>, StreamerError> {
        let mut app = self.lock_streamers();
        Self::create_locked(self, &mut app, config)
    }

    /// The body of [`AppState::create_streamer`], against a guard the caller
    /// already holds. Loading a whole config file is a run of these under one
    /// lock, so it can't interleave with a request halfway through.
    fn create_locked(
        &self,
        app: &mut HashMap<StreamerId, StreamerHandle>,
        config: Config,
    ) -> Result<Arc<Streamer>, StreamerError> {
        let streamer = Arc::new(Streamer::new(config)?);
        let id = streamer.id.clone();
        // we require unique ids, so if this id already exists we should error out
        if app.contains_key(id.as_str()) {
            return Err(StreamerError::DuplicateId(id));
        }
        let ctx = BuildCtx::with_secrets(
            app,
            id.clone(),
            self.events.clone(),
            Arc::clone(&self.secrets),
        );
        // building the runtime only fails on things the config got wrong
        // (unknown upstream, unbuildable component)
        let join_handle = streamer.start(ctx).map_err(|e| {
            StreamerError::InvalidConfig(e.context(format!("failed to start streamer '{id}'")))
        })?;

        let streamer_handle = StreamerHandle {
            join_handle,
            shared: Arc::clone(&streamer),
        };
        app.insert(id, streamer_handle);
        tracing::debug!("streamer created: {}", streamer.id);
        Ok(streamer)
    }

    pub fn delete_streamer(&self, id: &str) -> Result<(), StreamerError> {
        let mut app = self.lock_streamers();
        let Some(handle) = app.remove(id) else {
            tracing::debug!("failed to delete streamer: {} (not found)", id);
            return Err(StreamerError::NotFound(id.to_string()));
        };
        // signal cancellation here; the run loop drops out on its own
        handle.shared.cancellation_token.cancel();
        tracing::debug!("streamer deleted: {}", id);
        Ok(())
    }
}
