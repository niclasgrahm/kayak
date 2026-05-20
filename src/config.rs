use crate::BuildCtx;
use crate::inputs::InputSource;
use crate::inputs::dummy::DummyInput;
use crate::inputs::nats::NatsInput;
use crate::inputs::streamer::StreamerInput;
use crate::state::StreamerId;

use anyhow::Result;
use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::mpsc;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct NatsInputConfig {
    pub urls: String,
    pub subject: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum InputConfig {
    Dummy { duration: usize },
    Nats(NatsInputConfig),
    Streamer { upstream: StreamerId },
}
impl InputConfig {
    pub fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn InputSource>> {
        match self {
            InputConfig::Dummy { duration } => Ok(Box::new(DummyInput {
                interval: std::time::Duration::from_secs(duration as u64),
            })),
            InputConfig::Nats(nats_cfg) => Ok(Box::new(NatsInput {
                cfg: nats_cfg,
                sub: None,
            })),
            InputConfig::Streamer { upstream } => {
                let upstream_handle = ctx
                    .streamers
                    .get(&upstream)
                    .ok_or_else(|| anyhow!("upstream streamer '{}' not found", upstream))?;
                let (tx, rx) = mpsc::channel::<Arc<serde_json::Value>>(100);
                upstream_handle.shared.subscribe(tx)?;
                Ok(Box::new(StreamerInput { upstream, rx }))
            }
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum TransformConfig {}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum OutputConfig {
    Stdout,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Config {
    pub input: InputConfig,
    pub transforms: Vec<TransformConfig>,
    pub output: OutputConfig,
}

impl Config {
    fn new() -> Self {
        Self {
            input: InputConfig::Nats(NatsInputConfig {
                urls: "http://yoyo:4222".to_string(),
                subject: "test".to_string(),
            }),
            transforms: Vec::new(),
            output: OutputConfig::Stdout,
        }
    }
}
