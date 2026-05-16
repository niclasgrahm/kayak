use crate::BuildCtx;
use crate::inputs::Input;
use crate::state::StreamerId;

use anyhow::Result;
use anyhow::anyhow;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::mpsc;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct NatsInputConfig {
    brokers: String,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum InputConfig {
    Dummy,
    Nats(NatsInputConfig),
    Streamer { upstream: StreamerId },
}

impl InputConfig {
    pub fn build(self, ctx: &mut BuildCtx) -> Result<Input> {
        match self {
            InputConfig::Dummy => Ok(Input::Dummy),
            InputConfig::Nats(nats_cfg) => {
                todo!()
            }
            InputConfig::Streamer { upstream } => {
                let upstream_handle = ctx
                    .streamers
                    .get(&upstream)
                    .ok_or_else(|| anyhow!("upstream streamer '{}' not found", upstream))?;
                let (tx, rx) = mpsc::channel::<Arc<serde_json::Value>>(100);
                upstream_handle.shared.subscribe(tx)?;
                Ok(Input::Streamer(rx))
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
                brokers: "http://yoyo:4222".to_string(),
            }),
            transforms: Vec::new(),
            output: OutputConfig::Stdout,
        }
    }
}
