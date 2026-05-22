use anyhow::Result;
use chrono::prelude::*;
use serde::{Deserialize, Serialize};
use std::io::Write;
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufWriter};

use crate::inputs::MessageBatch;
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

pub struct StdoutOutput {}

#[async_trait::async_trait]
impl OutputDestination for StdoutOutput {
    async fn emit(&mut self, message_batch: Arc<MessageBatch>) -> anyhow::Result<()> {
        println!("{}", serde_json::to_string_pretty(&message_batch).unwrap());
        Ok(())
    }
    async fn init(&mut self) -> Result<()> {
        Ok(())
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct FileOutput {
    // there's probably a better type for this
    #[serde(skip)]
    writer: Option<BufWriter<File>>,
}

impl FileOutput {
    pub fn new() -> Self {
        Self { writer: None }
    }
}

#[async_trait::async_trait]
impl OutputDestination for FileOutput {
    async fn emit(&mut self, message_batch: Arc<MessageBatch>) -> anyhow::Result<()> {
        if self.writer.is_none() {
            self.init().await?;
        }

        self.writer
            .as_mut()
            .unwrap()
            .write_all(serde_json::to_string(&message_batch)?.as_bytes())
            .await?;

        self.writer.as_mut().unwrap().flush().await?;

        Ok(())
    }
    async fn init(&mut self) -> Result<()> {
        let path = std::path::PathBuf::from("/Users/niclas/projects/rust/streamer/data.csv");
        let file = tokio::fs::File::create(path).await?;
        self.writer = Some(BufWriter::new(file));
        Ok(())
    }
}
