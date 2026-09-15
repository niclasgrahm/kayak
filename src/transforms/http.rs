//! `http`: send the batch somewhere and carry on with the answer.
//!
//! The reaching-out half of the http family that is *not* the end of the
//! chain: the output throws the reply away, this one is defined by it. The
//! declaration is [`kayak_core::config::HttpTransformConfig`] and what is
//! shared with the output — url parsing, the verb rule, the credential, how a
//! complaint is quoted — is [`crate::outbound`].
//!
//! Two decisions are the transform's own:
//!
//! - **`merge` is the round trip to a model.** `replace` — the reply *is* the
//!   new batch — is what this transform always did, and it is right when the
//!   service on the other end is the transform. It is wrong for a model:
//!   a service that answers `{"score": 0.93}` has thrown away the machine id
//!   the pipeline needs to publish that under. `merge` writes the reply onto
//!   the message that caused it, under `as`, and nothing the pipeline sent is
//!   lost. Under `body: batch` a reply that is an array of the batch's length
//!   is a verdict per message and is written element-wise; anything else is a
//!   verdict on the batch and goes onto every message.
//! - **A retry sleeps; the gate skips.** `retries` is for the transient
//!   failure — a 502 from a proxy, a connection reset — and it waits out a
//!   backoff *inside the pass*, because the alternative is failing a batch of
//!   real readings over a hiccup. The gate is for the outage: once a request
//!   has failed for good, later batches are refused without a round trip
//!   until the backoff says to try again, which is what keeps a down endpoint
//!   from being hammered on every batch. They compose: each pass the gate
//!   allows may retry.
//!
//! `verb` is honoured, which settles a known issue: it used to be accepted
//! and ignored, every request a POST. `GET` and `DELETE` are refused at build
//! time now, the output's rule, since a request with no body sends no
//! messages.

use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result, anyhow, bail};
use kayak_core::config::{HttpBodyKind, HttpResponseKind, HttpTransformConfig};
use reqwest::header::CONTENT_TYPE;
use reqwest::{Client, Method, StatusCode, Url};
use serde_json::Value;

use crate::{
    BuildCtx,
    backoff::{Backoff, Gate},
    fields,
    inputs::MessageBatch,
    outbound::{Credential, DEFAULT_TIMEOUT, describe, method_with_body, parse_url, truncate},
    transforms::{BuildTransform, Transform},
};

impl BuildTransform for HttpTransformConfig {
    fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn Transform>> {
        let url = parse_url("http transform", &self.url)?;
        let method = method_with_body("http transform", self.verb)?;
        let credential = self
            .auth
            .as_ref()
            .map(|auth| Credential::build("http transform", auth, ctx))
            .transpose()?;
        let response = self.response.unwrap_or_default();
        let output = self.output.map(|o| o.trim().to_string()).filter(|o| !o.is_empty());
        match (response, &output) {
            (HttpResponseKind::Merge, None) => {
                bail!("an http transform with `response: merge` needs an `as` to write the reply under")
            }
            (HttpResponseKind::Replace, Some(_)) => {
                bail!("an http transform's `as` only means something with `response: merge`")
            }
            _ => {}
        }
        for (name, key) in [("wrap", &self.wrap), ("unwrap", &self.unwrap)] {
            if key.as_ref().is_some_and(|k| k.trim().is_empty()) {
                bail!("an http transform's `{name}` cannot be blank");
            }
        }
        let client = Client::builder()
            .timeout(self.timeout_seconds.map_or(DEFAULT_TIMEOUT, std::time::Duration::from_secs))
            .build()
            .context("failed to build the http transform's client")?;
        Ok(Box::new(HttpTransform {
            described: describe(&url),
            url,
            method,
            body: self.body.unwrap_or_default(),
            wrap: self.wrap,
            response,
            unwrap: self.unwrap,
            output,
            credential,
            retries: self.retries.unwrap_or(0),
            client,
            gate: Gate::new(),
        }))
    }
}

pub struct HttpTransform {
    url: Url,
    described: String,
    method: Method,
    body: HttpBodyKind,
    wrap: Option<String>,
    response: HttpResponseKind,
    unwrap: Option<String>,
    output: Option<String>,
    credential: Option<Credential>,
    retries: u32,
    client: Client,
    gate: Gate,
}

/// Whether a failed attempt is the kind a retry might fix.
fn transient(status: StatusCode) -> bool {
    status.is_server_error() || status == StatusCode::TOO_MANY_REQUESTS
}

impl HttpTransform {
    /// One round trip: `payload` (wrapped if asked) out, the reply (unwrapped
    /// if asked) back, as JSON. Retried `retries` times on a network failure
    /// or a transient status; failed at once on anything else.
    async fn round_trip(&self, payload: &Value) -> Result<Value> {
        let body = match &self.wrap {
            Some(key) => serde_json::to_string(&serde_json::json!({ key: payload }))?,
            None => serde_json::to_string(payload)?,
        };
        let mut backoff = Backoff::new();
        let mut attempt = 0;
        loop {
            match self.attempt(&body).await {
                Ok(reply) => return self.unwrapped(reply),
                Err(Attempt::Fatal(err)) => return Err(err),
                Err(Attempt::Transient(err)) if attempt >= self.retries => return Err(err),
                Err(Attempt::Transient(err)) => {
                    attempt += 1;
                    tracing::warn!("{err:#}; retry {attempt} of {}", self.retries);
                    tokio::time::sleep(backoff.failed()).await;
                }
            }
        }
    }

    async fn attempt(&self, body: &str) -> std::result::Result<Value, Attempt> {
        let mut request = self
            .client
            .request(self.method.clone(), self.url.clone())
            .header(CONTENT_TYPE, "application/json")
            .body(body.to_string());
        if let Some(credential) = &self.credential {
            request = request.header(credential.name.clone(), credential.value.clone());
        }
        let response = request
            .send()
            .await
            .map_err(|err| Attempt::Transient(anyhow!(err).context(format!("failed to reach {}", self.described))))?;
        let status = response.status();
        if !status.is_success() {
            let detail = response
                .text()
                .await
                .unwrap_or_else(|e| format!("<the error body could not be read: {e}>"));
            let err = anyhow!(
                "{} refused the request ({status}): {}",
                self.described,
                truncate(detail.trim())
            );
            return Err(if transient(status) { Attempt::Transient(err) } else { Attempt::Fatal(err) });
        }
        let text = response
            .text()
            .await
            .map_err(|err| Attempt::Transient(anyhow!(err).context("reading the reply")))?;
        if text.trim().is_empty() {
            return Ok(Value::Null);
        }
        serde_json::from_str(&text)
            .map_err(|err| Attempt::Fatal(anyhow!("{} answered with something that is not JSON: {err}", self.described)))
    }

    fn unwrapped(&self, reply: Value) -> Result<Value> {
        let Some(key) = &self.unwrap else {
            return Ok(reply);
        };
        match reply {
            Value::Object(mut fields) => fields.remove(key).ok_or_else(|| {
                anyhow!("{} answered without the '{key}' the reply is read from", self.described)
            }),
            other => bail!(
                "{} answered with {}, which has no '{key}' to read the reply from",
                self.described,
                fields::describe(&other)
            ),
        }
    }

    /// The whole batch as one request.
    async fn as_batch(&self, batch: &MessageBatch) -> Result<MessageBatch> {
        let payload = Value::Array(batch.iter().map(|m| (**m).clone()).collect());
        let reply = self.round_trip(&payload).await?;
        match self.response {
            HttpResponseKind::Replace => match reply {
                Value::Array(messages) => Ok(messages.into_iter().map(Arc::new).collect()),
                other => bail!(
                    "{} answered with {}, but under `body: batch` and `response: replace` the \
                     reply has to be a JSON array of messages",
                    self.described,
                    fields::describe(&other)
                ),
            },
            HttpResponseKind::Merge => {
                let output = self.output.as_deref().unwrap_or_default();
                let per_message: Vec<Value> = match reply {
                    Value::Array(items) if items.len() == batch.len() => items,
                    Value::Array(items) => bail!(
                        "{} answered with {} items for a batch of {}; an array reply is written \
                         element-wise and has to match",
                        self.described,
                        items.len(),
                        batch.len()
                    ),
                    whole => std::iter::repeat_n(whole, batch.len()).collect(),
                };
                batch
                    .iter()
                    .zip(per_message)
                    .map(|(message, answer)| {
                        let mut written = (**message).clone();
                        fields::set(&mut written, output, answer)?;
                        Ok(Arc::new(written))
                    })
                    .collect()
            }
        }
    }

    /// One request per message, in order; the first failure fails the batch.
    async fn as_messages(&self, batch: &MessageBatch) -> Result<MessageBatch> {
        let mut out = MessageBatch::with_capacity(batch.len());
        for message in batch {
            let reply = self.round_trip(message).await?;
            match self.response {
                HttpResponseKind::Replace => match reply {
                    Value::Array(messages) => out.extend(messages.into_iter().map(Arc::new)),
                    Value::Null => {}
                    one => out.push(Arc::new(one)),
                },
                HttpResponseKind::Merge => {
                    let mut written = (**message).clone();
                    fields::set(&mut written, self.output.as_deref().unwrap_or_default(), reply)?;
                    out.push(Arc::new(written));
                }
            }
        }
        Ok(out)
    }
}

/// How one attempt failed: worth another go, or not.
enum Attempt {
    Transient(anyhow::Error),
    Fatal(anyhow::Error),
}

#[async_trait::async_trait]
impl Transform for HttpTransform {
    async fn apply(&mut self, batch: Arc<MessageBatch>) -> Result<Vec<Arc<MessageBatch>>> {
        // a filter can empty a batch, and a round trip carrying nothing asks
        // the endpoint about nothing
        if batch.is_empty() {
            return Ok(vec![]);
        }
        let now = Instant::now();
        if !self.gate.ready(now) {
            bail!(
                "{} is still being backed off after a failure; this batch was not sent",
                self.described
            );
        }
        let result = match self.body {
            HttpBodyKind::Batch => self.as_batch(&batch).await,
            HttpBodyKind::Message => self.as_messages(&batch).await,
        };
        match result {
            Ok(out) => {
                self.gate.record_success();
                if out.is_empty() {
                    return Ok(vec![]);
                }
                Ok(vec![Arc::new(out)])
            }
            Err(err) => {
                self.gate.record_failure(Instant::now());
                Err(err)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::batch;
    use axum::{Router, body::Bytes, extract::State, http::HeaderMap, http::StatusCode, routing::any};
    use kayak_core::config::{HttpAuthConfig, HttpVerb};
    use serde_json::json;
    use std::collections::VecDeque;
    use std::sync::Mutex;

    /// What one request looked like when it arrived.
    #[derive(Clone, Debug)]
    struct Recorded {
        method: String,
        authorization: Option<String>,
        body: Value,
    }

    /// A scripted endpoint: answers from the front of `replies`, and echoes
    /// the request body back when the script runs out.
    #[derive(Default)]
    struct Endpoint {
        received: Mutex<Vec<Recorded>>,
        replies: Mutex<VecDeque<(u16, String)>>,
    }

    fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
        m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    impl Endpoint {
        fn reply_with(&self, status: u16, body: &str) {
            lock(&self.replies).push_back((status, body.to_string()));
        }
        fn received(&self) -> Vec<Recorded> {
            lock(&self.received).clone()
        }
    }

    async fn endpoint() -> (Arc<Endpoint>, String) {
        let state = Arc::new(Endpoint::default());
        let app = Router::new().route("/model", any(record)).with_state(Arc::clone(&state));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .unwrap_or_else(|e| panic!("bind: {e}"));
        let addr = listener.local_addr().unwrap_or_else(|e| panic!("addr: {e}"));
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (state, format!("http://{addr}/model"))
    }

    async fn record(State(state): State<Arc<Endpoint>>, method: axum::http::Method, headers: HeaderMap, body: Bytes) -> (StatusCode, String) {
        let parsed: Value = serde_json::from_slice(&body).unwrap_or(Value::String("<not json>".into()));
        lock(&state.received).push(Recorded {
            method: method.to_string(),
            authorization: headers.get("authorization").and_then(|v| v.to_str().ok()).map(String::from),
            body: parsed.clone(),
        });
        match lock(&state.replies).pop_front() {
            Some((status, reply)) => (StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR), reply),
            None => (StatusCode::OK, parsed.to_string()),
        }
    }

    fn config(url: &str) -> HttpTransformConfig {
        HttpTransformConfig {
            url: url.into(),
            verb: HttpVerb::Post,
            body: None,
            wrap: None,
            response: None,
            unwrap: None,
            output: None,
            auth: None,
            timeout_seconds: None,
            retries: None,
        }
    }

    fn build(config: HttpTransformConfig) -> Result<Box<dyn Transform>> {
        let (events, _) = tokio::sync::broadcast::channel(16);
        let mut pipelines = std::collections::HashMap::new();
        let mut ctx = BuildCtx::new(&mut pipelines, "http-test".into(), events);
        config.build(&mut ctx)
    }

    async fn run(config: HttpTransformConfig, messages: Vec<Value>) -> Result<Vec<Value>> {
        let out = build(config)?.apply(batch(messages)).await?;
        Ok(out.iter().flat_map(|b| b.iter().map(|m| (**m).clone())).collect())
    }

    /// The behaviour every existing config gets: the batch as an array, the
    /// reply as the batch.
    #[tokio::test]
    async fn by_default_the_batch_goes_as_an_array_and_the_reply_replaces_it() -> Result<()> {
        let (endpoint, url) = endpoint().await;
        endpoint.reply_with(200, r#"[{"scored": true}]"#);
        let out = run(config(&url), vec![json!({"a": 1}), json!({"a": 2})]).await?;
        assert_eq!(out, vec![json!({"scored": true})]);
        let received = endpoint.received();
        assert_eq!(received.len(), 1);
        assert_eq!(received[0].body, json!([{"a": 1}, {"a": 2}]));
        assert_eq!(received[0].method, "POST");
        Ok(())
    }

    #[tokio::test]
    async fn merge_writes_the_reply_onto_the_message_that_caused_it() -> Result<()> {
        let (endpoint, url) = endpoint().await;
        endpoint.reply_with(200, r#"{"score": 0.93}"#);
        let mut config = config(&url);
        config.body = Some(HttpBodyKind::Message);
        config.response = Some(HttpResponseKind::Merge);
        config.output = Some("prediction".into());
        let out = run(config, vec![json!({"machine": "m7", "mean": 21.5})]).await?;
        assert_eq!(out, vec![json!({"machine": "m7", "mean": 21.5, "prediction": {"score": 0.93}})]);
        assert_eq!(endpoint.received()[0].body, json!({"machine": "m7", "mean": 21.5}));
        Ok(())
    }

    #[tokio::test]
    async fn merge_over_a_batch_is_element_wise_or_whole() -> Result<()> {
        let (endpoint, url) = endpoint().await;
        let mut config = config(&url);
        config.response = Some(HttpResponseKind::Merge);
        config.output = Some("score".into());
        let messages = vec![json!({"id": 1}), json!({"id": 2})];

        endpoint.reply_with(200, "[0.1, 0.9]");
        let out = run(config.clone(), messages.clone()).await?;
        assert_eq!(out, vec![json!({"id": 1, "score": 0.1}), json!({"id": 2, "score": 0.9})]);

        endpoint.reply_with(200, r#"{"batch_ok": true}"#);
        let out = run(config.clone(), messages.clone()).await?;
        assert!(out.iter().all(|m| m["score"] == json!({"batch_ok": true})));

        endpoint.reply_with(200, "[0.1]");
        let err = run(config, messages).await.err().map(|e| e.to_string()).unwrap_or_default();
        assert!(err.contains("1 items for a batch of 2"), "{err}");
        Ok(())
    }

    #[tokio::test]
    async fn wrap_and_unwrap_shape_the_request_and_read_the_reply() -> Result<()> {
        let (endpoint, url) = endpoint().await;
        endpoint.reply_with(200, r#"{"predictions": [{"y": 1}]}"#);
        let mut config = config(&url);
        config.wrap = Some("instances".into());
        config.unwrap = Some("predictions".into());
        let out = run(config, vec![json!({"x": 1})]).await?;
        assert_eq!(out, vec![json!({"y": 1})]);
        assert_eq!(endpoint.received()[0].body, json!({"instances": [{"x": 1}]}));
        Ok(())
    }

    #[tokio::test]
    async fn per_message_replace_takes_none_one_or_many() -> Result<()> {
        let (endpoint, url) = endpoint().await;
        endpoint.reply_with(200, "");
        endpoint.reply_with(200, r#"{"one": 1}"#);
        endpoint.reply_with(200, r#"[{"a": 1}, {"a": 2}]"#);
        let mut config = config(&url);
        config.body = Some(HttpBodyKind::Message);
        let out = run(config, vec![json!({"n": 1}), json!({"n": 2}), json!({"n": 3})]).await?;
        assert_eq!(out, vec![json!({"one": 1}), json!({"a": 1}), json!({"a": 2})]);
        assert_eq!(endpoint.received().len(), 3);
        Ok(())
    }

    #[tokio::test]
    async fn the_verb_and_the_credential_are_honoured() -> Result<()> {
        let (endpoint, url) = endpoint().await;
        let mut config = config(&url);
        config.verb = HttpVerb::Put;
        config.auth = Some(HttpAuthConfig::Bearer {
            token: kayak_core::config::Secret::from("${MODEL_TOKEN}"),
        });
        let (events, _) = tokio::sync::broadcast::channel(16);
        let mut pipelines = std::collections::HashMap::new();
        let mut ctx = BuildCtx::new(&mut pipelines, "http-test".into(), events);
        ctx.secrets = Arc::new(crate::testing::MapSecretStore::new("test", &[("MODEL_TOKEN", "hunter2")]));
        let mut transform = config.build(&mut ctx)?;
        transform.apply(batch(vec![json!({"a": 1})])).await?;
        let received = endpoint.received();
        assert_eq!(received[0].method, "PUT");
        assert_eq!(received[0].authorization.as_deref(), Some("Bearer hunter2"));
        Ok(())
    }

    #[tokio::test]
    async fn a_refusal_fails_the_batch_and_a_transient_one_is_retried() -> Result<()> {
        let (endpoint, url) = endpoint().await;
        endpoint.reply_with(400, "no thank you");
        let err = run(config(&url), vec![json!({"a": 1})]).await.err().map(|e| e.to_string()).unwrap_or_default();
        assert!(err.contains("400") && err.contains("no thank you"), "{err}");

        endpoint.reply_with(503, "busy");
        endpoint.reply_with(200, r#"[{"ok": true}]"#);
        let mut retrying = config(&url);
        retrying.retries = Some(2);
        let out = run(retrying, vec![json!({"a": 1})]).await?;
        assert_eq!(out, vec![json!({"ok": true})]);
        assert_eq!(endpoint.received().len(), 3, "one refused, one retried, one ok");

        endpoint.reply_with(503, "busy");
        let err = run(config(&url), vec![json!({"a": 1})]).await.err().map(|e| e.to_string()).unwrap_or_default();
        assert!(err.contains("503"), "without retries a 503 fails at once: {err}");
        Ok(())
    }

    #[tokio::test]
    async fn after_a_failure_the_next_batch_is_gated() -> Result<()> {
        let (endpoint, url) = endpoint().await;
        endpoint.reply_with(400, "no");
        let mut transform = build(config(&url))?;
        assert!(transform.apply(batch(vec![json!({"a": 1})])).await.is_err());
        let err = transform.apply(batch(vec![json!({"a": 2})])).await.err().map(|e| e.to_string()).unwrap_or_default();
        assert!(err.contains("backed off"), "{err}");
        assert_eq!(endpoint.received().len(), 1, "the second batch never went out");
        Ok(())
    }

    #[test]
    fn contradictions_are_refused_at_build() {
        let mut bodyless = config("http://localhost/x");
        bodyless.verb = HttpVerb::Get;
        assert!(build(bodyless).is_err(), "GET sends no messages");
        let mut merge = config("http://localhost/x");
        merge.response = Some(HttpResponseKind::Merge);
        assert!(build(merge).is_err(), "merge needs an `as`");
        let mut stray = config("http://localhost/x");
        stray.output = Some("x".into());
        assert!(build(stray).is_err(), "`as` without merge");
        assert!(build(config("not a url")).is_err());
    }
}
