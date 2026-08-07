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

/// Threaded through every `build()` call. It carries the pipeline map — needed
/// so a `pipeline` input can look up its upstream and register an mpsc sender
/// on it — the id of the pipeline being built (components that label their
/// output want it), the UI event channel, and the store that `${NAME}`
/// references in the config resolve against.
pub struct BuildCtx<'a> {
    pub pipelines: &'a mut HashMap<PipelineId, PipelineHandle>,
    pub pipeline_id: PipelineId,
    pub events: broadcast::Sender<UiEvent>,
    pub secrets: Arc<dyn SecretStore>,
}

impl<'a> BuildCtx<'a> {
    /// Resolves secrets from the environment only. Components that take no
    /// secrets don't care, which is most of them and every test that isn't
    /// about secrets; use [`BuildCtx::with_secrets`] for anything else.
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
        }
    }

    /// Fill in the `${NAME}` references in a config value. Failing here fails
    /// the build, which is what turns a missing secret into "pipeline 'x' failed
    /// to start" rather than a pipeline quietly running without credentials.
    pub fn resolve(&self, secret: &Secret) -> anyhow::Result<Resolved> {
        secrets::resolve(secret, self.secrets.as_ref())
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
        .with_state(state)
}
