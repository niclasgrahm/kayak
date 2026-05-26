use crate::BuildCtx;
use crate::inputs::Buffered;
use crate::inputs::BuildInput;
use crate::inputs::InputSource;
use crate::inputs::dummy::DummyConfig;
use crate::inputs::nats::NatsConfig;
use crate::inputs::streamer::StreamerConfig;
use crate::outputs::BuildOutput;
use crate::outputs::OutputDestination;
use crate::outputs::file::FileOutputConfig;
use crate::outputs::nats::NatsOutputConfig;
use crate::outputs::stdout::StdoutOutputConfig;
use crate::transforms::BuildTransform;
use crate::transforms::Transform;
use crate::transforms::buffer::BufferTransformConfig;

use anyhow::Result;
use serde::{Deserialize, Serialize};

/////// INPUTS

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum InputKind {
    Dummy(DummyConfig),
    Nats(NatsConfig),
    Streamer(StreamerConfig),
}
impl InputKind {
    pub fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn InputSource>> {
        match self {
            InputKind::Dummy(c) => c.build(ctx),
            InputKind::Nats(c) => c.build(ctx),
            InputKind::Streamer(c) => c.build(ctx),
        }
    }
}

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
/////// TRANSFORM
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TransformKind {
    Buffer(BufferTransformConfig),
    // Reduce(ReduceTransformConfig),
    // Http(HttpTransformConfig),
}
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct TransformConfig {
    #[serde(flatten)]
    pub kind: TransformKind,
}

impl TransformConfig {
    pub fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn Transform>> {
        match self.kind {
            TransformKind::Buffer(c) => c.build(ctx),
            // TransformKind::Reduce(c) => c.build(ctx),
            // TransformKind::Http(c) => c.build(ctx),
        }
    }
}

/////// OUTPUT

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OutputKind {
    Stdout(StdoutOutputConfig),
    File(FileOutputConfig),
    Nats(NatsOutputConfig),
}
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct OutputConfig {
    #[serde(flatten)]
    pub kind: OutputKind,
}

impl OutputConfig {
    pub fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn OutputDestination>> {
        match self.kind {
            OutputKind::Stdout(c) => c.build(ctx),
            OutputKind::File(c) => c.build(ctx),
            OutputKind::Nats(c) => c.build(ctx),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Config {
    pub input: InputConfig,
    pub transforms: Vec<TransformConfig>,
    pub output: OutputConfig,
}
