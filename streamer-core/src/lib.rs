use std::sync::Arc;

use crate::config::Config;
use serde::{Deserialize, Serialize};

pub mod config;
pub mod docs;

#[derive(Serialize, Deserialize, Clone)]
pub struct StreamerDto {
    pub id: String,
    pub config: Config,
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
