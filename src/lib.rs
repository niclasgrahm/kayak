//! The kayak stream-processing runtime.
//!
//! Everything except the process entry point lives here rather than in
//! `main.rs`, so that integration tests in `tests/` (which are separate crates)
//! can build pipelines, drive run loops and call the HTTP handlers directly.

use axum::{
    Router,
    routing::{delete, get, post, put},
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::broadcast;

pub mod config;
pub mod connections;
pub mod events;
pub mod handlers;
pub mod inputs;
pub mod layout;
pub mod outputs;
pub mod persist;
pub mod pipeline;
pub mod secrets;
pub mod state;
pub mod testing;
pub mod transforms;

use crate::handlers::{
    rest::{
        connection::{create_connection, delete_connection, get_connections},
        docs::get_docs,
        layout::{get_layout, put_layout},
        pipeline::{create_pipeline, delete_pipeline, get_pipelines},
        settings::{get_settings, revert_config, save_config},
    },
    ui::ui::events_handler,
};
use crate::secrets::{EnvStore, Resolved, SecretStore};
use crate::state::{AppState, PipelineHandle, PipelineId, UiEvent};
use kayak_core::config::Secret;
use std::path::PathBuf;

use kayak_core::connections::{
    Connections, FileConnection, KafkaConnection, NatsConnection, PostgresConnection,
};

/// Threaded through every `build()` call. It carries the pipeline map — needed
/// so a `pipeline` input can look up its upstream and register an mpsc sender
/// on it — the id of the pipeline being built (components that label their
/// output want it), the UI event channel, the store that `${NAME}` references
/// in the config resolve against, and the named connections a component's
/// `connection` field points into.
pub struct BuildCtx<'a> {
    pub pipelines: &'a mut HashMap<PipelineId, PipelineHandle>,
    pub pipeline_id: PipelineId,
    pub events: broadcast::Sender<UiEvent>,
    pub secrets: Arc<dyn SecretStore>,
    /// The connections as they stand at the moment this pipeline is built.
    ///
    /// A snapshot, not a live view: a component reads what it needs here and
    /// then holds its own settings, so editing a connection afterwards does not
    /// reach the pipelines already running on it. Reverting — or deleting and
    /// re-creating the pipeline — is what picks up a change, and that is the
    /// same rule the config file already has.
    pub connections: Arc<Connections>,
    /// The one directory tree the server may write pipeline *data* into, from
    /// `--data-dir`, canonicalized at startup.
    ///
    /// `None` — the default — means no file output can be built at all. That is
    /// deliberately the closed position: a disk writer driven by whatever a
    /// pipeline carries is not something a deployment should get without asking
    /// for it. See [`crate::outputs::file::Root`] for what it is checked
    /// against, and note it is a separate thing from [`AppState`]'s `save_dir`
    /// — that one bounds where *configs* are written, and the two should not be
    /// conflated just because they are both directories.
    pub data_dir: Option<Arc<PathBuf>>,
}

impl<'a> BuildCtx<'a> {
    /// Resolves secrets from the environment only, with no connections
    /// configured. Components that take neither don't care, which is most of
    /// them and every test that isn't about one or the other; use
    /// [`BuildCtx::with_secrets`] for anything else.
    pub fn new(
        pipelines: &'a mut HashMap<PipelineId, PipelineHandle>,
        pipeline_id: PipelineId,
        events: broadcast::Sender<UiEvent>,
    ) -> Self {
        Self::with_secrets(pipelines, pipeline_id, events, Arc::new(EnvStore))
    }

    pub fn with_secrets(
        pipelines: &'a mut HashMap<PipelineId, PipelineHandle>,
        pipeline_id: PipelineId,
        events: broadcast::Sender<UiEvent>,
        secrets: Arc<dyn SecretStore>,
    ) -> Self {
        Self {
            pipelines,
            pipeline_id,
            events,
            secrets,
            connections: Arc::new(Connections::new()),
            data_dir: None,
        }
    }

    /// The same, with the connections a component's `connection` field can name.
    #[must_use]
    pub fn with_connections(mut self, connections: Arc<Connections>) -> Self {
        self.connections = connections;
        self
    }

    /// The same, with the directory file outputs are confined to. Without this
    /// they refuse to build.
    #[must_use]
    pub fn with_data_dir(mut self, data_dir: Option<Arc<PathBuf>>) -> Self {
        self.data_dir = data_dir;
        self
    }

    /// Fill in the `${NAME}` references in a config value. Failing here fails
    /// the build, which is what turns a missing secret into "pipeline 'x' failed
    /// to start" rather than a pipeline quietly running without credentials.
    pub fn resolve(&self, secret: &Secret) -> anyhow::Result<Resolved> {
        secrets::resolve(secret, self.secrets.as_ref())
    }

    /// The connection a component named, of the kind it can use. An unknown
    /// name or the wrong kind fails the build with an error that says which —
    /// see [`kayak_core::connections::ConnectionError`].
    pub fn kafka_connection(&self, id: &str) -> anyhow::Result<&KafkaConnection> {
        Ok(self.connections.kafka(id)?)
    }

    pub fn nats_connection(&self, id: &str) -> anyhow::Result<&NatsConnection> {
        Ok(self.connections.nats(id)?)
    }

    pub fn postgres_connection(&self, id: &str) -> anyhow::Result<&PostgresConnection> {
        Ok(self.connections.postgres(id)?)
    }

    pub fn file_connection(&self, id: &str) -> anyhow::Result<&FileConnection> {
        Ok(self.connections.file(id)?)
    }
}

/// The JSON/SSE surface, without the Leptos routes. `main` merges this with the
/// frontend router; tests call it directly through `tower::ServiceExt::oneshot`,
/// which keeps them off real sockets.
pub fn api_router(state: Arc<AppState>) -> Router {
    Router::new()
        // the /docs *page* is a Leptos route; this is the same data as JSON
        .route("/api/docs", get(get_docs))
        .route("/api/settings", get(get_settings))
        // where the cards sit, not what they run: written on the spot rather
        // than waiting for a save — see `layout`
        .route("/api/layout", get(get_layout))
        .route("/api/layout", put(put_layout))
        // the config file is written only here, never as a side effect of an
        // edit — see `persist`
        .route("/api/config/save", post(save_config))
        .route("/api/config/revert", post(revert_config))
        .route("/events", get(events_handler))
        .route("/api/pipelines", post(create_pipeline))
        .route("/api/pipelines", get(get_pipelines))
        .route("/api/pipelines/{pipeline_id}", delete(delete_pipeline))
        // the systems those pipelines talk to, named once and referred to by
        // the components — written to disk by the same explicit save
        .route("/api/connections", get(get_connections))
        .route("/api/connections", post(create_connection))
        .route(
            "/api/connections/{connection_id}",
            delete(delete_connection),
        )
        .with_state(state)
}
