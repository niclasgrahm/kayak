use std::sync::Arc;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    inputs::MessageBatch,
    transforms::{BuildTransform, Transform},
};

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "http")]
pub struct HttpTransformConfig {
    url: String,
    verb: String,
}
impl BuildTransform for HttpTransformConfig {
    fn build(self, _ctx: &mut crate::BuildCtx) -> anyhow::Result<Box<dyn Transform>> {
        Ok(Box::new(HttpTransform {
            url: self.url,
            verb: self.verb,
            client: reqwest::Client::new(),
        }))
    }
}
pub struct HttpTransform {
    url: String,
    verb: String,
    client: reqwest::Client,
}
#[async_trait::async_trait]
impl Transform for HttpTransform {
    async fn apply(
        &mut self,
        message_batch: Arc<MessageBatch>,
    ) -> anyhow::Result<Vec<Arc<MessageBatch>>> {
        // make a single http request with the batch as a json array body
        // parse the  response into the same
        let body = serde_json::to_string(&message_batch)?;
        let out = self
            .client
            .post(&self.url)
            .body(body)
            .send()
            .await?
            .json::<MessageBatch>()
            .await?;
        Ok(vec![Arc::new(out)])
    }
}
