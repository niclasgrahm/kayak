use crate::inputs::BufferKind;
use crate::inputs::Buffered;
use crate::inputs::BuildInput;
use crate::inputs::InputSource;
use crate::outputs::BuildOutput;
use crate::outputs::OutputDestination;
use crate::transforms::BuildTransform;
use crate::transforms::Transform;
use crate::BuildCtx;
use streamer_core::config::BufferConfig;
use streamer_core::config::InputConfig;
use streamer_core::config::InputKind;
use streamer_core::config::OutputConfig;
use streamer_core::config::OutputKind;
use streamer_core::config::TransformConfig;
use streamer_core::config::TransformKind;

use anyhow::Result;

pub trait BuildInputConfig {
    fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn InputSource>>;
}

impl BuildInputConfig for InputKind {
    fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn InputSource>> {
        match self {
            InputKind::Dummy(c) => c.build(ctx),
            InputKind::Kafka(c) => c.build(ctx),
            InputKind::Nats(c) => c.build(ctx),
            InputKind::Streamer(c) => c.build(ctx),
        }
    }
}

impl BuildInputConfig for InputConfig {
    fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn InputSource>> {
        let inner = self.kind.build(ctx)?;
        Ok(match self.buffer {
            Some(BufferConfig::Static { size }) => {
                Box::new(Buffered::new(inner, BufferKind::Static { size }))
            }
            Some(BufferConfig::Tumbling { window_seconds }) => Box::new(Buffered::new(
                inner,
                BufferKind::Tumbling { window_seconds },
            )),
            None => inner,
        })
    }
}

pub trait BuildTransformConfig {
    fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn Transform>>;
}

impl BuildTransformConfig for TransformConfig {
    fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn Transform>> {
        match self.kind {
            TransformKind::Buffer(c) => c.build(ctx),
            TransformKind::Http(c) => c.build(ctx),
            TransformKind::Splitter(c) => c.build(ctx),
            TransformKind::Reducer(c) => c.build(ctx),
            TransformKind::Filter(c) => c.build(ctx),
        }
    }
}

pub trait BuildOutputConfig {
    fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn OutputDestination>>;
}

impl BuildOutputConfig for OutputConfig {
    fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn OutputDestination>> {
        match self.kind {
            OutputKind::Stdout(c) => c.build(ctx),
            OutputKind::File(c) => c.build(ctx),
            OutputKind::Kafka(c) => c.build(ctx),
            OutputKind::Nats(c) => c.build(ctx),
            OutputKind::Postgres(c) => c.build(ctx),
        }
    }
}
