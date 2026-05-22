use crate::BuildCtx;
use crate::inputs::Buffered;
use crate::inputs::InputSource;
use crate::inputs::MessageBatch;
use crate::inputs::dummy::DummyInput;
use crate::inputs::nats::NatsInput;
use crate::inputs::streamer::StreamerInput;
use crate::outputs::OutputDestination;
use crate::outputs::OutputKind;
use crate::outputs::file::FileOutput;
use crate::outputs::stdout::StdoutOutput;
use crate::state::StreamerId;

use anyhow::Result;
use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::mpsc;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct InputConfig {
    #[serde(flatten)]
    pub kind: InputKind,

    #[serde(default)]
    pub buffer: Option<usize>,
}

impl InputConfig {
    pub fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn InputSource>> {
        let inner = self.kind.build(ctx)?;
        Ok(match self.buffer {
            Some(n) if n > 1 => Box::new(Buffered { inner, buffer: n }),
            _ => inner,
        })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum InputKind {
    Dummy { duration: usize },
    Nats { urls: String, subject: String },
    Streamer { upstream: StreamerId },
}
impl InputKind {
    pub fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn InputSource>> {
        match self {
            InputKind::Dummy { duration } => Ok(Box::new(DummyInput {
                interval: std::time::Duration::from_secs(duration as u64),
            })),
            InputKind::Nats { urls, subject } => Ok(Box::new(NatsInput {
                urls,
                subject,
                sub: None,
            })),
            InputKind::Streamer { upstream } => {
                let upstream_handle = ctx
                    .streamers
                    .get(&upstream)
                    .ok_or_else(|| anyhow!("upstream streamer '{}' not found", upstream))?;
                let (tx, rx) = mpsc::channel::<Arc<MessageBatch>>(100);
                upstream_handle.shared.subscribe(tx)?;
                Ok(Box::new(StreamerInput { upstream, rx }))
            }
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum TransformConfig {}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct OutputConfig {
    #[serde(flatten)]
    pub kind: OutputKind,
}

impl OutputConfig {
    pub fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn OutputDestination>> {
        match self.kind {
            OutputKind::Stdout => Ok(Box::new(StdoutOutput {})),
            OutputKind::File => Ok(Box::new(FileOutput::new())),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Config {
    pub input: InputConfig,
    pub transforms: Vec<TransformConfig>,
    pub output: OutputConfig,
}
