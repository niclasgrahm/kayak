use std::sync::Arc;

use crate::config::Config;
use serde::{Deserialize, Serialize};

pub mod config;

#[derive(Serialize, Deserialize, Clone)]
pub struct StreamerDto {
    pub id: String,
    pub config: Config,
}

pub type StreamerId = String;
pub type MessageBatch = Vec<Arc<serde_json::Value>>;

#[derive(Clone, Serialize, Deserialize, PartialEq)]
pub struct UiEvent {
    pub streamer_id: StreamerId,
    pub stage: String,
    pub batch: Arc<MessageBatch>,
}
