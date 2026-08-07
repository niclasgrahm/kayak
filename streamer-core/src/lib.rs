use std::sync::Arc;

use crate::config::Config;
use serde::{Deserialize, Serialize};

pub mod config;
pub mod docs;
pub mod format;
pub mod layout;

pub use format::ConfigFormat;
pub use layout::{EdgeEnd, LayoutFile, NodeLayout, PortLayout, Side};

#[derive(Serialize, Deserialize, Clone)]
pub struct StreamerDto {
    pub id: String,
    pub config: Config,
}

/// How the server was started, and whether what it is running still matches
/// the file it started from.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct SettingsDto {
    /// Name of the config file the server is working against — the `--config`
    /// one, or the one a save has since created. Its absence doesn't mean edits
    /// can't be saved: it means there is no file *yet*, so the UI offers to
    /// create one rather than to overwrite one.
    pub config_file: Option<String>,
    /// The directory a save writes into. Shown so "create a config file" can
    /// say where the file will appear, which is the one thing the file name on
    /// its own doesn't tell you.
    ///
    /// Defaults to empty when a client is talking to an older server, which
    /// reads the same as "unknown" — the UI just leaves the location out.
    #[serde(default)]
    pub save_directory: String,
    /// The running graph has diverged from what was last loaded or saved.
    /// Edits apply to the runtime immediately and the file is left alone, so
    /// without this the divergence would be invisible until a restart lost it.
    pub unsaved_changes: bool,
}

/// What `POST /api/config/save` takes: a bare file name, saved beside the
/// config the server was started from. Not a path — see `persist::save_path`.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct SaveConfigRequest {
    pub name: String,
    /// JSON or YAML. Omitted means "whatever `name` says it is", which is what
    /// keeps a client that predates the choice — and a hand-written `curl` —
    /// writing the format the file is named for.
    #[serde(default)]
    pub format: Option<ConfigFormat>,
}

/// Where a save actually landed, so the UI can name it rather than guess.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct SaveConfigResponse {
    pub path: String,
}

pub type StreamerId = String;
pub type MessageBatch = Vec<Arc<serde_json::Value>>;

/// The stage of a run loop an event came from. Also what the frontend matches
/// on to decide whether an edge lights up, so the strings are shared here
/// rather than spelled out at either end.
pub mod stage {
    pub const INPUT: &str = "input";
    pub const TRANSFORM: &str = "transform";
    pub const OUTPUT: &str = "output";
}

/// What a run loop is reporting: a batch that passed through, or something that
/// went wrong while handling one.
#[derive(Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum EventPayload {
    Batch(Arc<MessageBatch>),
    /// A failure at this stage. The batch that caused it is not carried: a
    /// transform that failed has no output to show, and the input that did
    /// arrive was already reported by its own event.
    Error(String),
}

#[derive(Clone, Serialize, Deserialize, PartialEq)]
pub struct UiEvent {
    pub streamer_id: StreamerId,
    pub stage: String,
    pub payload: EventPayload,
}

impl UiEvent {
    pub fn batch(streamer_id: StreamerId, stage: &str, batch: Arc<MessageBatch>) -> Self {
        Self {
            streamer_id,
            stage: stage.to_string(),
            payload: EventPayload::Batch(batch),
        }
    }

    /// `error` is rendered with `{:#}`, so an `anyhow` chain arrives as the
    /// same "context: cause" line the server log shows.
    pub fn error(streamer_id: StreamerId, stage: &str, error: &impl std::fmt::Display) -> Self {
        Self {
            streamer_id,
            stage: stage.to_string(),
            payload: EventPayload::Error(format!("{error:#}")),
        }
    }
}
