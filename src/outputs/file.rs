use anyhow::Result;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::fs::File;
use tokio::io::{AsyncWriteExt, BufWriter};

use crate::BuildCtx;
use crate::inputs::MessageBatch;
use crate::outputs::{BuildOutput, OutputDestination};

// we have nats both as input and output; lets differentiate their configs like this
// for now; perhaps we can consolidate later
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "file")]
pub struct FileOutputConfig {}

impl BuildOutput for FileOutputConfig {
    fn build(self, _ctx: &mut BuildCtx) -> Result<Box<dyn OutputDestination>> {
        Ok(Box::new(FileOutput::new()))
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
        let writer = self
            .writer
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("file output writer not initialized"))?;

        writer
            .write_all(serde_json::to_string(&message_batch)?.as_bytes())
            .await?;
        writer.flush().await?;
        Ok(())
    }

    async fn init(&mut self) -> Result<()> {
        let path = std::path::PathBuf::from("/Users/niclas/projects/rust/streamer/data.csv");
        let file = tokio::fs::File::create(path).await?;
        self.writer = Some(BufWriter::new(file));
        Ok(())
    }
}
