//! The `http_poll` input: [`super::poll::Poller`] over an [`HttpReader`].
//!
//! The `http` input is reached; this one reaches out. It is the database
//! inputs' `snapshot` mode for an api — a `GET` on a timer, the whole reply
//! handed on every time — and it is a [`Reader`] rather than an input of its
//! own so that everything about *when* to read, how a failing read backs off
//! and how rows become batches stays in `poll.rs`, once.
//!
//! Three decisions are worth knowing:
//!
//! - **Snapshot only.** An api has no common spelling of "rows after this
//!   one" (a cursor parameter, a `Link` header, a page number, a `since`) and
//!   picking one would be picking an api. The case this exists for is
//!   reference data, which is small and wanted whole, and a sink that upserts
//!   by key makes the repetition harmless. [`Reader::page`] and
//!   [`Reader::newest`] are unreachable — the schedule has no `start_from` —
//!   and say so if they are ever reached.
//! - **The reply is bounded.** A snapshot holds the whole reply in memory,
//!   so it is read in chunks and given up on past [`MAX_BODY_BYTES`] rather
//!   than trusting an api not to answer with a gigabyte. Same rule as every
//!   state bucket: no unbounded spelling.
//! - **No connection kind**, for the reason the http output has none: the url
//!   is the whole of what the system is, and the credential belongs to the
//!   component (it is the output's [`Credential`], resolved the same way).

use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use kayak_core::config::HttpPollConfig;
use reqwest::header::ACCEPT;
use reqwest::{Client, Url};
use serde_json::Value;

use crate::{
    BuildCtx,
    inputs::{
        BuildInput, InputSource, ack, batch_cap,
        poll::{Fetched, Poller, Reader, Schedule},
    },
    outbound::{Credential, describe, truncate},
};

/// How long a request may take when the config doesn't say — the http
/// output's default, for the same reason.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// The largest reply a read will hold. Reference data is kilobytes; an api
/// answering with more than this is almost certainly not being asked the
/// question the config meant.
pub const MAX_BODY_BYTES: usize = 64 * 1024 * 1024;

impl BuildInput for HttpPollConfig {
    fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn InputSource>> {
        // a snapshot is handed on whatever the outputs then do with it, so
        // there is nothing `on_delivery` could hold back
        ack::require_receipt_only(ctx.ack_mode(), "http_poll")?;

        let url = Url::parse(&self.url)
            .with_context(|| format!("the http_poll input's url '{}' is not a url", self.url))?;
        anyhow::ensure!(
            matches!(url.scheme(), "http" | "https"),
            "the http_poll input's url '{}' is not an http url; the scheme is '{}'",
            self.url,
            url.scheme()
        );
        anyhow::ensure!(
            self.interval_secs > 0,
            "`interval_secs` is 0; a read that never waits is a loop against the api"
        );
        if let Some(items) = &self.items {
            anyhow::ensure!(
                items.is_empty() || items.starts_with('/'),
                "`items` is '{items}', which is not a JSON pointer; write it as a path from the \
                 root of the reply, e.g. '/data/machines'"
            );
        }

        let credential = self
            .auth
            .as_ref()
            .map(|auth| Credential::build("http_poll input", auth, ctx))
            .transpose()?;
        let timeout = self
            .timeout_seconds
            .map_or(DEFAULT_TIMEOUT, Duration::from_secs);
        let client = Client::builder()
            .timeout(timeout)
            .build()
            .context("failed to build the http_poll input's client")?;

        let described = describe(&url);
        let schedule = Schedule {
            interval: Duration::from_secs(self.interval_secs),
            start_from: None,
            // a snapshot has no pages; the poller never reads this
            page_size: 1,
            max_batch: batch_cap(self.max_batch),
            source: described.clone(),
        };
        let reader = HttpReader {
            url,
            described: described.clone(),
            items: self.items.filter(|items| !items.is_empty()),
            credential,
            client,
        };
        let envelope = ctx.envelope("http_poll", None);
        Ok(Box::new(Poller::new(
            schedule,
            Box::new(reader),
            vec![("url", Value::String(described))],
            envelope,
            ctx.pipeline_id.clone(),
            ctx.events.clone(),
        )))
    }
}

/// One `GET` per snapshot.
pub struct HttpReader {
    url: Url,
    /// [`describe`]d once: it goes into every error and every message's
    /// envelope, and never carries the url's userinfo.
    described: String,
    items: Option<String>,
    credential: Option<Credential>,
    client: Client,
}

impl HttpReader {
    /// The reply's body, whole, or an error that says why there isn't one.
    async fn fetch(&self) -> Result<Vec<u8>> {
        let mut request = self
            .client
            .get(self.url.clone())
            .header(ACCEPT, "application/json");
        if let Some(credential) = &self.credential {
            request = request.header(credential.name.clone(), credential.value.clone());
        }
        let mut response = request
            .send()
            .await
            .with_context(|| format!("failed to reach {}", self.described))?;

        let status = response.status();
        if !status.is_success() {
            let detail = response
                .text()
                .await
                .unwrap_or_else(|e| format!("<the error body could not be read: {e}>"));
            bail!(
                "{} answered {status}: {}",
                self.described,
                truncate(detail.trim())
            );
        }

        let too_large = || {
            anyhow!(
                "{} answered with more than {} MiB; a snapshot is held whole, so this input reads \
                 replies up to that size",
                self.described,
                MAX_BODY_BYTES / (1024 * 1024)
            )
        };
        if response
            .content_length()
            .is_some_and(|length| length > MAX_BODY_BYTES as u64)
        {
            return Err(too_large());
        }
        let mut body = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .with_context(|| format!("the reply from {} broke off", self.described))?
        {
            if body.len() + chunk.len() > MAX_BODY_BYTES {
                return Err(too_large());
            }
            body.extend_from_slice(&chunk);
        }
        Ok(body)
    }
}

/// The messages in one reply: what `items` points at (the reply itself when
/// absent), an array split into its elements and anything else as one.
///
/// Pure, and where the shape rules are tested.
pub fn records(reply: Value, items: Option<&str>) -> Result<Vec<Value>> {
    let mut reply = reply;
    let found = match items {
        Some(pointer) => reply
            .pointer_mut(pointer)
            .map(Value::take)
            .ok_or_else(|| anyhow!("the reply has nothing at `items` '{pointer}'"))?,
        None => reply,
    };
    Ok(match found {
        Value::Array(elements) => elements,
        other => vec![other],
    })
}

#[async_trait::async_trait]
impl Reader for HttpReader {
    async fn snapshot(&mut self) -> Result<Vec<Value>> {
        let body = self.fetch().await?;
        let reply: Value = serde_json::from_slice(&body).with_context(|| {
            format!(
                "{} did not answer with JSON: {}",
                self.described,
                truncate(String::from_utf8_lossy(&body).trim())
            )
        })?;
        records(reply, self.items.as_deref())
            .with_context(|| format!("failed to read the reply from {}", self.described))
    }

    async fn page(&mut self, _after: Option<&str>, _limit: usize) -> Result<Vec<Fetched>> {
        Err(anyhow!(
            "an http_poll input reads whole snapshots and has no pages"
        ))
    }

    async fn newest(&mut self) -> Result<Option<String>> {
        Err(anyhow!(
            "an http_poll input reads whole snapshots and has no watermark"
        ))
    }

    fn describe(&self) -> String {
        self.described.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inputs::Delivery;
    use crate::testing::MapSecretStore;
    use axum::Router;
    use axum::extract::State;
    use axum::http::{HeaderMap, StatusCode};
    use axum::routing::get;
    use kayak_core::config::{EnvelopeConfig, HttpAuthConfig, Secret};
    use serde_json::json;
    use std::collections::{HashMap, VecDeque};
    use std::sync::{Arc, Mutex, PoisonError};

    fn ok<T, E: std::fmt::Display>(result: std::result::Result<T, E>) -> T {
        result.unwrap_or_else(|e| panic!("the test could not get this far: {e}"))
    }

    fn config(url: &str) -> HttpPollConfig {
        HttpPollConfig {
            url: url.to_string(),
            interval_secs: 60,
            items: None,
            auth: None,
            timeout_seconds: None,
            max_batch: Some(100),
        }
    }

    fn build_with(
        config: HttpPollConfig,
        envelope: Option<EnvelopeConfig>,
    ) -> Result<Box<dyn InputSource>> {
        let mut pipelines = HashMap::new();
        let (events, _rx) = tokio::sync::broadcast::channel(4);
        let secrets = Arc::new(MapSecretStore::new(
            "a test store",
            &[("TOKEN", "hunter2"), ("BLANK", "")],
        ));
        let mut ctx = BuildCtx::with_secrets(&mut pipelines, "p".to_string(), events, secrets);
        ctx.envelope = envelope;
        config.build(&mut ctx)
    }

    fn build(config: HttpPollConfig) -> Result<Box<dyn InputSource>> {
        build_with(config, None)
    }

    // ---- what a reply becomes ----

    #[test]
    fn an_array_is_a_message_per_element_and_anything_else_is_one() -> Result<()> {
        assert_eq!(
            records(json!([{"id": 1}, {"id": 2}]), None)?,
            [json!({"id": 1}), json!({"id": 2})]
        );
        assert_eq!(records(json!({"id": 1}), None)?, [json!({"id": 1})]);
        assert_eq!(records(json!([]), None)?, Vec::<Value>::new());
        Ok(())
    }

    #[test]
    fn items_points_into_a_wrapped_reply() -> Result<()> {
        let reply = json!({"data": {"machines": [{"id": "m1"}, {"id": "m2"}]}, "total": 2});
        assert_eq!(
            records(reply, Some("/data/machines"))?,
            [json!({"id": "m1"}), json!({"id": "m2"})]
        );
        assert_eq!(
            records(json!({"data": {"id": "m1"}}), Some("/data"))?,
            [json!({"id": "m1"})]
        );
        let Err(e) = records(json!({"data": []}), Some("/rows")) else {
            panic!("a pointer at nothing is an error, not an empty snapshot");
        };
        assert!(format!("{e:#}").contains("/rows"), "{e:#}");
        Ok(())
    }

    // ---- what is refused at build time ----

    #[test]
    fn a_config_that_cannot_work_is_refused_at_build_time() {
        assert!(build(config("http://127.0.0.1:59999/machines")).is_ok());
        assert!(build(config("not a url")).is_err());
        assert!(build(config("ftp://example.com/machines")).is_err());
        let mut zero = config("http://127.0.0.1:59999/machines");
        zero.interval_secs = 0;
        assert!(build(zero).is_err());
        let mut pointer = config("http://127.0.0.1:59999/machines");
        pointer.items = Some("data.machines".to_string());
        let Err(e) = build(pointer) else {
            panic!("a dotted path is not a JSON pointer");
        };
        assert!(format!("{e:#}").contains("JSON pointer"), "{e:#}");
        let mut blank = config("http://127.0.0.1:59999/machines");
        blank.auth = Some(HttpAuthConfig::Bearer {
            token: Secret::from("${BLANK}".to_string()),
        });
        let Err(e) = build(blank) else {
            panic!("an empty credential is refused");
        };
        assert!(format!("{e:#}").contains("http_poll input"), "{e:#}");
    }

    // ---- against a real endpoint on a loopback port ----

    #[derive(Default)]
    struct Api {
        /// What to answer, in order; the last answer repeats.
        answers: Mutex<VecDeque<(u16, String)>>,
        authorization: Mutex<Vec<Option<String>>>,
    }

    impl Api {
        fn answer(&self, status: u16, body: &str) {
            self.answers
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push_back((status, body.to_string()));
        }
        fn requests(&self) -> Vec<Option<String>> {
            self.authorization
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone()
        }
    }

    async fn serve(State(api): State<Arc<Api>>, headers: HeaderMap) -> (StatusCode, String) {
        api.authorization
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(
                headers
                    .get("authorization")
                    .and_then(|v| v.to_str().ok())
                    .map(ToString::to_string),
            );
        let mut answers = api.answers.lock().unwrap_or_else(PoisonError::into_inner);
        let (status, body) = if answers.len() > 1 {
            answers.pop_front().unwrap_or((500, String::new()))
        } else {
            answers.front().cloned().unwrap_or((500, String::new()))
        };
        (
            StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            body,
        )
    }

    async fn api() -> (Arc<Api>, String) {
        let api = Arc::new(Api::default());
        let app = Router::new()
            .route("/machines", get(serve))
            .with_state(Arc::clone(&api));
        let listener = ok(tokio::net::TcpListener::bind("127.0.0.1:0").await);
        let addr = ok(listener.local_addr());
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (api, format!("http://{addr}/machines"))
    }

    fn ids(delivery: &Delivery) -> Vec<String> {
        delivery
            .iter()
            .filter_map(|m| m["id"].as_str().map(ToString::to_string))
            .collect()
    }

    /// The whole point: the same reply, read again every interval, handed on
    /// whole every time — with the credential presented on every request.
    #[tokio::test]
    async fn every_read_hands_on_the_whole_reply_with_the_credential() -> Result<()> {
        let (api, url) = api().await;
        api.answer(200, r#"{"data": [{"id": "m1"}, {"id": "m2"}]}"#);
        let mut config = config(&url);
        config.interval_secs = 1;
        config.items = Some("/data".to_string());
        config.auth = Some(HttpAuthConfig::Bearer {
            token: Secret::from("${TOKEN}".to_string()),
        });
        let mut input = build(config)?;

        let first = input.next().await?;
        assert_eq!(ids(&first), ["m1", "m2"]);
        let second = tokio::time::timeout(Duration::from_secs(5), input.next()).await??;
        assert_eq!(ids(&second), ["m1", "m2"]);
        assert_eq!(
            api.requests(),
            [
                Some("Bearer hunter2".to_string()),
                Some("Bearer hunter2".to_string())
            ]
        );
        Ok(())
    }

    /// A refusing api costs a wait, not the pipeline: the read is retried and
    /// the snapshot arrives once the api answers.
    #[tokio::test]
    async fn a_failing_read_is_retried_until_the_api_answers() -> Result<()> {
        let (api, url) = api().await;
        api.answer(503, "down for maintenance");
        api.answer(200, "not json");
        api.answer(200, r#"[{"id": "m1"}]"#);
        let mut input = build(config(&url))?;

        let delivery = tokio::time::timeout(Duration::from_secs(20), input.next()).await??;
        assert_eq!(ids(&delivery), ["m1"]);
        assert_eq!(api.requests().len(), 3);
        Ok(())
    }

    /// The reason a read failed is the api's own words, and the url in it
    /// carries no userinfo.
    #[tokio::test]
    async fn a_refusal_says_what_the_api_said() -> Result<()> {
        let (api, url) = api().await;
        api.answer(403, "token expired");
        let url = ok(Url::parse(&url.replace("http://", "http://kayak:hunter2@")));
        let mut reader = HttpReader {
            described: describe(&url),
            url,
            items: None,
            credential: None,
            client: Client::new(),
        };
        let Err(e) = reader.snapshot().await else {
            panic!("a 403 is a failed read");
        };
        let message = format!("{e:#}");
        assert!(message.contains("403"), "{message}");
        assert!(message.contains("token expired"), "{message}");
        assert!(!message.contains("hunter2"), "{message}");
        Ok(())
    }

    #[tokio::test]
    async fn the_envelope_carries_the_url_and_when_the_read_started() -> Result<()> {
        let (api, url) = api().await;
        api.answer(200, r#"[{"id": "m1"}]"#);
        let mut input = build_with(config(&url), Some(EnvelopeConfig::Merge { meta: None }))?;
        let delivery = input.next().await?;
        let meta = &delivery[0]["_meta"];
        assert_eq!(meta["input"], json!("http_poll"));
        assert_eq!(meta["url"], json!(url));
        assert!(meta["polled_at"].is_string());
        assert!(meta.get("connection").is_none(), "{meta}");
        Ok(())
    }
}
