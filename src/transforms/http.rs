use kayak_core::config::{HttpTransformConfig, HttpVerb};
use std::sync::Arc;

use crate::{
    inputs::MessageBatch,
    transforms::{BuildTransform, Transform},
};

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
    // TODO: `verb` is accepted in the config but not honoured yet — every
    // request is a POST. Wiring it up changes behaviour for existing configs,
    // so it needs a decision first (see readme "known issues").
    #[allow(dead_code)]
    verb: HttpVerb,
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
