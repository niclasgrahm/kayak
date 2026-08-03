use anyhow::Context;
use serde::Serialize;
use tokio::sync::broadcast;

use crate::streamer::Streamer;
use crate::BuildCtx;
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};
use streamer_core::config::Config;
pub use streamer_core::{StreamerId, UiEvent};

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

pub struct AppState {
    streamers: Mutex<HashMap<StreamerId, StreamerHandle>>,
    events: broadcast::Sender<UiEvent>,
}

impl AppState {
    pub fn new() -> Self {
        tracing::debug!("Initializing empty server state...");
        let (events, _) = broadcast::channel(1024);
        Self {
            streamers: Mutex::new(HashMap::new()),
            events,
        }
    }

    pub fn subscribe_events(&self) -> broadcast::Receiver<UiEvent> {
        self.events.subscribe()
    }

    pub fn from_config(path: &Path) -> anyhow::Result<Self> {
        let new_state = AppState::new();
        tracing::debug!("Loading initial configuration from {}...", path.display());
        let file = std::fs::File::open(path)
            .with_context(|| format!("failed to open config file {}", path.display()))?;
        let configs: Vec<Config> = serde_json::from_reader(file)
            .with_context(|| format!("failed to parse config file {}", path.display()))?;
        for c in configs {
            let id = c.id.clone().unwrap_or_else(|| "<generated>".to_string());
            new_state
                .create_streamer(c)
                .with_context(|| format!("failed to start streamer '{id}' from config"))?;
        }

        Ok(new_state)
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

    pub fn get_streamers(&self) -> anyhow::Result<serde_json::Value> {
        // the views borrow from the handles, so the guard has to outlive them
        let app = self.lock_streamers();
        let views: Vec<_> = app.values().map(|h| h.shared.view()).collect();
        serde_json::to_value(views).context("failed to serialize streamers")
    }

    pub fn create_streamer(&self, config: Config) -> Result<Arc<Streamer>, StreamerError> {
        let streamer = Arc::new(Streamer::new(config)?);
        let mut app = self.lock_streamers();
        let id = streamer.id.clone();
        // we require unique ids, so if this id already exists we should error out
        if app.contains_key(id.as_str()) {
            return Err(StreamerError::DuplicateId(id));
        }
        let ctx = BuildCtx::new(&mut app, self.events.clone());
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
