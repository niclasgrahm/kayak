//! The kayak stream-processing runtime.
//!
//! Everything except the process entry point lives here rather than in
//! `main.rs`, so that integration tests in `tests/` (which are separate crates)
//! can build pipelines, drive run loops and call the HTTP handlers directly.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::broadcast;

pub mod auth;
pub mod buckets;
pub mod config;
pub mod connections;
pub mod endpoints;
pub mod events;
pub mod fields;
pub mod handlers;
pub mod inputs;
pub mod layout;
pub mod openapi;
pub mod outputs;
pub mod persist;
pub mod pipeline;
pub mod secrets;
pub mod server_config;
pub mod state;
pub mod testing;
pub mod transforms;

use crate::buckets::Buckets;
pub use crate::endpoints::api_router;
use crate::inputs::envelope::{Envelope, Meta};
use crate::inputs::http::Inboxes;
use crate::secrets::{EnvStore, Resolved, SecretStore};
use crate::state::{PipelineHandle, PipelineId, UiEvent};
use kayak_core::config::{EnvelopeConfig, Secret};
use kayak_core::state::PipelineState;
use serde_json::Value;
use std::path::PathBuf;

use kayak_core::connections::{
    Connections, FileConnection, KafkaConnection, NatsConnection, PostgresConnection, S3Connection,
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
    /// Where an `http` input registers the endpoint it is posted to, shared
    /// with the handler that serves it. Held by [`crate::state::AppState`] and
    /// passed in here for the same reason the connections are: the component
    /// needs it at build time and knows nothing about the server.
    pub inboxes: Arc<Inboxes>,
    /// What the input currently being built should attach to its messages, from
    /// the `envelope` on its [`kayak_core::config::InputConfig`].
    ///
    /// It rides here rather than being passed to `BuildInput::build` because it
    /// belongs to the *wrapper* — `envelope` sits beside `buffer` on
    /// `InputConfig`, so every input kind accepts it and none of them declare
    /// it — while only the input itself can supply the interesting half of the
    /// metadata. `BuildInputConfig for InputConfig` sets it around the kind's
    /// build; nothing else should write it.
    pub envelope: Option<EnvelopeConfig>,
    /// The live state buckets, as they stand at the moment this pipeline is
    /// built — the same kind of snapshot the connections are, and held by
    /// [`crate::state::AppState`] for the same reason.
    ///
    /// Unlike the connections this one has *contents*, so the snapshot is of
    /// the store rather than of a config: a rebuilt pipeline reattaches to what
    /// its bucket already holds. See [`crate::buckets::Buckets::rebuilt`] for
    /// the one case where it doesn't.
    pub buckets: Arc<Buckets>,
    /// Which bucket this pipeline's stateful transforms use, and what its
    /// messages are keyed by, from the `state` on its `Config`.
    ///
    /// It rides here for the reason `envelope` does: the block belongs to the
    /// pipeline, and the transforms that need it are built one level down.
    pub state: Option<PipelineState>,
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
            inboxes: Arc::new(Inboxes::new()),
            envelope: None,
            buckets: Arc::new(Buckets::new()),
            state: None,
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

    /// The same, with the server's http-input registry rather than a private
    /// one. Without it an `http` input still builds and still works — it is
    /// just that nothing outside this build can reach it, which is what a test
    /// driving the input directly wants.
    #[must_use]
    pub fn with_inboxes(mut self, inboxes: Arc<Inboxes>) -> Self {
        self.inboxes = inboxes;
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

    pub fn s3_connection(&self, id: &str) -> anyhow::Result<&S3Connection> {
        Ok(self.connections.s3(id)?)
    }

    /// The same, with the live state buckets a pipeline's `state` names.
    #[must_use]
    pub fn with_buckets(mut self, buckets: Arc<Buckets>) -> Self {
        self.buckets = buckets;
        self
    }

    /// The same, with the pipeline's binding to one of those buckets.
    #[must_use]
    pub fn with_state(mut self, state: Option<PipelineState>) -> Self {
        self.state = state;
        self
    }

    /// The envelope this input should attach its messages to.
    ///
    /// Every input calls this, passing its kind and the connection it reads
    /// through if it has one; what it knows per *message* — a subject, an
    /// offset — it adds at `apply` time. An input that forgets to call it
    /// simply attaches nothing, which is why `metadata::for_input` is a
    /// declaration a test enforces rather than the only record of the fact.
    #[must_use]
    pub fn envelope(&self, kind: &'static str, connection: Option<&str>) -> Envelope {
        let mut statics: Meta = vec![
            ("pipeline", Value::String(self.pipeline_id.clone())),
            ("input", Value::String(kind.to_string())),
        ];
        if let Some(connection) = connection {
            statics.push(("connection", Value::String(connection.to_string())));
        }
        Envelope::new(self.envelope.as_ref(), statics)
    }
}
