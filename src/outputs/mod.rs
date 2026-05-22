use anyhow::Result;
use chrono::prelude::*;
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufWriter};

use crate::inputs::MessageBatch;

pub mod file;
pub mod stdout;
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OutputKind {
    Stdout,
    File,
}

#[async_trait::async_trait]
pub trait OutputDestination: Send + 'static {
    async fn emit(&mut self, message_batch: Arc<MessageBatch>) -> anyhow::Result<()>;
    async fn init(&mut self) -> Result<()>;
}
