use crate::BuildCtx;
use crate::inputs::BufferKind;
use crate::inputs::Buffered;
use crate::inputs::BuildInput;
use crate::inputs::InputSource;
use crate::outputs::BuildOutput;
use crate::outputs::OutputDestination;
use crate::transforms::BuildTransform;
use crate::transforms::Transform;
use kayak_core::config::InputConfig;
use kayak_core::config::InputKind;
use kayak_core::config::OutputConfig;
use kayak_core::config::OutputKind;
use kayak_core::config::TransformConfig;
use kayak_core::config::TransformKind;

use anyhow::Result;

pub trait BuildInputConfig {
    fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn InputSource>>;
}

impl BuildInputConfig for InputKind {
    fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn InputSource>> {
        match self {
            InputKind::Dummy(c) => c.build(ctx),
            InputKind::Http(c) => c.build(ctx),
            InputKind::Kafka(c) => c.build(ctx),
            InputKind::Nats(c) => c.build(ctx),
            InputKind::Pipeline(c) => c.build(ctx),
        }
    }
}

impl BuildInputConfig for InputConfig {
    fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn InputSource>> {
        // `envelope` is the wrapper's field, but only the input kind knows what
        // there is to attach — so it goes onto the context for the length of
        // that build and comes straight back off, rather than leaking into the
        // next input of the same pipeline.
        let previous = std::mem::replace(&mut ctx.envelope, self.envelope);
        let built = self.kind.build(ctx);
        ctx.envelope = previous;
        let inner = built?;
        Ok(match self.buffer {
            Some(buffer) => Box::new(Buffered::new(inner, BufferKind::from(buffer))),
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
            TransformKind::Remember(c) => c.build(ctx),
            TransformKind::Recall(c) => c.build(ctx),
            TransformKind::Map(c) => c.build(ctx),
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
            OutputKind::S3(c) => c.build(ctx),
            OutputKind::Kafka(c) => c.build(ctx),
            OutputKind::Nats(c) => c.build(ctx),
            OutputKind::Postgres(c) => c.build(ctx),
        }
    }
}
