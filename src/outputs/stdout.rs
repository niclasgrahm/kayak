use std::sync::Arc;

use crate::{
    BuildCtx,
    inputs::MessageBatch,
    outputs::{BuildOutput, OutputDestination},
};
use anyhow::Result;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "stdout")]
pub struct StdoutOutputConfig {}
impl BuildOutput for StdoutOutputConfig {
    fn build(self, _ctx: &mut BuildCtx) -> Result<Box<dyn OutputDestination>> {
        Ok(Box::new(StdoutOutput {}))
    }
}

pub struct StdoutOutput {}

#[async_trait::async_trait]
impl OutputDestination for StdoutOutput {
    async fn emit(&mut self, message_batch: Arc<MessageBatch>) -> anyhow::Result<()> {
        let msg_str = serde_json::to_string_pretty(&message_batch)?;
        println!("{}", msg_str);
        Ok(())
    }
    async fn init(&mut self) -> Result<()> {
        Ok(())
    }
}
