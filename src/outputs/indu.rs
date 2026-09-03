//! Writes messages into Indu Cloud as streams — `POST /ingest/v1/streams`.
//!
//! The `http` output can reach the same endpoint and cannot speak to it: it
//! sends the batch as a bare array, and the ingest surface wants
//! `{"readings": [{"stream", "at", "value"}]}` under an API key, with a row
//! per *series* rather than per message. This output is that translation and
//! nothing else — the request, the gate and the failure text are the `http`
//! output's, and the body is built from Indu's documented wire format by
//! hand, because a kayak that depended on an Indu crate would make the two
//! licences meet.
//!
//! # A connection, unlike `http`
//!
//! The `http` output carries its url and credential itself because for a
//! webhook there is nothing worth naming once. Here there is: the deployment's
//! origin and its API key are *what the platform is*, every `indu` input and
//! output in the graph names the same one, and the key is a secret that
//! belongs in exactly one place.
//!
//! # What fails the batch
//!
//! Anything but a **full** acceptance. Indu answers `207` for a batch it
//! partly refused, and the refused rows are terminal — a stream the key may
//! not write to, a value that is not a number — so a `207` is reported with
//! the row errors quoted rather than counted as delivered. A message that
//! simply lacks a series' field is skipped for that series before the request
//! is built: a reducer that emits `oee` for some machines and not others is
//! not an error.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use kayak_core::config::{InduOutputConfig, InduSeries};
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderValue};
use reqwest::{Client, Url};
use serde_json::{Value, json};

use crate::{
    BuildCtx,
    backoff::Gate,
    fields,
    inputs::MessageBatch,
    outputs::{BuildOutput, OutputDestination},
};

/// Indu's stream ingest path, under the connection's ingest origin.
const STREAMS_PATH: &str = "/ingest/v1/streams";

/// The header a retry is recognised by. One key per batch, minted here, so a
/// batch kayak sends twice — a reconnect, a restart mid-flight — lands once.
const IDEMPOTENCY_HEADER: &str = "idempotency-key";

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// How much of Indu's complaint is quoted, as for the `http` output.
const MAX_DETAIL_BYTES: usize = 300;

impl BuildOutput for InduOutputConfig {
    fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn OutputDestination>> {
        anyhow::ensure!(
            !self.series.is_empty(),
            "the indu output needs at least one entry in `series`: which stream to write, and \
             which field holds its value"
        );
        for entry in &self.series {
            anyhow::ensure!(
                !entry.stream.trim().is_empty(),
                "an indu output's `series` entry needs a `stream` name"
            );
            anyhow::ensure!(
                !entry.value.trim().is_empty(),
                "the indu output's series {:?} needs a `value` field",
                entry.stream
            );
        }

        let connection = ctx.indu_connection(&self.connection)?;
        let origin = Url::parse(connection.ingest_origin()).with_context(|| {
            format!(
                "the indu connection '{}' has an origin that is not a url: {:?}",
                self.connection,
                connection.ingest_origin()
            )
        })?;
        anyhow::ensure!(
            matches!(origin.scheme(), "http" | "https"),
            "the indu connection '{}' needs an http(s) origin; got '{}'",
            self.connection,
            origin.scheme()
        );
        let url = origin
            .join(STREAMS_PATH)
            .context("joining the ingest path onto the indu origin")?;

        let resolved = ctx.resolve(&connection.api_key)?;
        anyhow::ensure!(
            !resolved.expose().is_empty(),
            "the indu connection '{}' resolved to an empty api key; check that '{resolved}' is \
             set in the secret store",
            self.connection
        );
        // The only place the key is reached. It goes into one header value,
        // marked sensitive, and is never held anywhere else.
        let credential = credential(resolved.expose())?;

        let timeout = self
            .timeout_seconds
            .map_or(DEFAULT_TIMEOUT, Duration::from_secs);
        let client = Client::builder()
            .timeout(timeout)
            .build()
            .context("failed to build the indu output's client")?;

        Ok(Box::new(InduOutput {
            described: url.to_string(),
            url,
            credential,
            series: self.series,
            at: self.at,
            client,
            gate: Gate::new(),
            sequence: 0,
        }))
    }
}

/// The `Authorization` header the key becomes: marked sensitive, so anything
/// that dumps a request's headers prints `Sensitive` rather than the key.
fn credential(key: &str) -> Result<HeaderValue> {
    let mut value = HeaderValue::try_from(format!("Bearer {key}"))
        .context("the indu api key cannot be sent as a header")?;
    value.set_sensitive(true);
    Ok(value)
}

pub struct InduOutput {
    url: Url,
    described: String,
    credential: HeaderValue,
    series: Vec<InduSeries>,
    at: Option<String>,
    client: Client,
    gate: Gate,
    /// Part of the idempotency key, so two batches built in the same
    /// nanosecond are still two batches.
    sequence: u64,
}

/// One reading, as Indu's wire format spells it.
fn reading(stream: &str, at: &str, value: f64, unit: Option<&str>) -> Value {
    let mut row = json!({ "stream": stream, "at": at, "value": value });
    if let Some(unit) = unit
        && let Some(object) = row.as_object_mut()
    {
        object.insert("unit".to_string(), Value::String(unit.to_string()));
    }
    row
}

/// Fill `{field}` placeholders from the message. `None` when a placeholder's
/// field is missing or is not a scalar — a stream named after a nested object
/// would be a stream named `[object]`.
fn render(template: &str, message: &Value) -> Option<String> {
    let mut out = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(start) = rest.find('{') {
        out.push_str(&rest[..start]);
        let after = &rest[start + 1..];
        let end = after.find('}')?;
        let field = &after[..end];
        let value = fields::get(message, field)?;
        match value {
            Value::String(s) => out.push_str(s),
            Value::Number(n) => out.push_str(&n.to_string()),
            Value::Bool(b) => out.push_str(if *b { "true" } else { "false" }),
            Value::Null | Value::Array(_) | Value::Object(_) => return None,
        }
        rest = &after[end + 1..];
    }
    out.push_str(rest);
    Some(out)
}

/// The reading's time: the configured field as RFC 3339 or epoch
/// milliseconds, else `now`.
fn timestamp(at: Option<&str>, message: &Value, now: &str) -> Option<String> {
    let Some(field) = at else {
        return Some(now.to_string());
    };
    match fields::get(message, field)? {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => n.as_i64().map(|ms| ms.to_string()),
        _ => None,
    }
}

impl InduOutput {
    /// The rows a batch becomes: one per message per series, skipping a
    /// message that lacks a series' value or a placeholder's field.
    fn rows(&self, batch: &MessageBatch, now: &str) -> Vec<Value> {
        let mut rows = Vec::with_capacity(batch.len() * self.series.len());
        for message in batch {
            let Some(at) = timestamp(self.at.as_deref(), message, now) else {
                continue;
            };
            for entry in &self.series {
                let Some(value) = fields::get(message, &entry.value).and_then(Value::as_f64) else {
                    continue;
                };
                let Some(stream) = render(&entry.stream, message) else {
                    continue;
                };
                rows.push(reading(&stream, &at, value, entry.unit.as_deref()));
            }
        }
        rows
    }

    async fn send(&self, body: String, key: &str) -> Result<()> {
        let response = self
            .client
            .post(self.url.clone())
            .header(CONTENT_TYPE, "application/json")
            .header(AUTHORIZATION, self.credential.clone())
            .header(IDEMPOTENCY_HEADER, key)
            .body(body)
            .send()
            .await
            .with_context(|| format!("failed to reach {}", self.described))?;

        let status = response.status();
        let text = response
            .text()
            .await
            .unwrap_or_else(|e| format!("<the response could not be read: {e}>"));

        // `207` is a partial acceptance, and the rows Indu refused are
        // terminal — say which, rather than counting the batch as delivered.
        if status.is_success() && status.as_u16() != 207 {
            return Ok(());
        }
        let detail = serde_json::from_str::<Value>(&text)
            .ok()
            .and_then(|receipt| {
                let rejected = receipt.get("rejected")?.as_u64()?;
                let first = receipt
                    .get("errors")
                    .and_then(Value::as_array)
                    .and_then(|errors| errors.first())
                    .map(|error| {
                        format!(
                            "row {}: {} — {}",
                            error.get("row").and_then(Value::as_u64).unwrap_or(0),
                            error.get("code").and_then(Value::as_str).unwrap_or("?"),
                            error.get("detail").and_then(Value::as_str).unwrap_or("")
                        )
                    })
                    .unwrap_or_default();
                Some(format!("{rejected} row(s) refused; first: {first}"))
            })
            .unwrap_or_else(|| truncate(text.trim()));
        Err(anyhow!(
            "{} refused the batch ({status}): {detail}",
            self.described
        ))
    }
}

fn truncate(detail: &str) -> String {
    if detail.len() <= MAX_DETAIL_BYTES {
        return detail.to_string();
    }
    let end = (0..=MAX_DETAIL_BYTES)
        .rev()
        .find(|i| detail.is_char_boundary(*i))
        .unwrap_or(0);
    format!("{}…", &detail[..end])
}

#[async_trait::async_trait]
impl OutputDestination for InduOutput {
    /// Nothing, for the `http` output's reason: there is no request to make
    /// that would not be a delivery.
    async fn init(&mut self) -> Result<()> {
        Ok(())
    }

    async fn emit(&mut self, message_batch: Arc<MessageBatch>) -> Result<()> {
        if message_batch.is_empty() {
            return Ok(());
        }
        let now = chrono::Utc::now();
        let rows = self.rows(&message_batch, &now.to_rfc3339());
        // Every message lacked every series' field: nothing to say, and an
        // empty batch would teach Indu nothing.
        if rows.is_empty() {
            return Ok(());
        }

        let instant = Instant::now();
        if !self.gate.ready(instant) {
            anyhow::bail!(
                "the indu output at {} is still failing; not retrying yet",
                self.described
            );
        }

        self.sequence = self.sequence.wrapping_add(1);
        let key = format!(
            "kayak-{}-{}",
            now.timestamp_nanos_opt().unwrap_or(0),
            self.sequence
        );
        let body = serde_json::to_string(&json!({ "readings": rows }))
            .context("failed to serialize the batch for the indu output")?;

        match self.send(body, &key).await {
            Ok(()) => {
                self.gate.record_success();
                Ok(())
            }
            Err(e) => {
                self.gate.record_failure(instant);
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::MapSecretStore;
    use axum::extract::State;
    use axum::http::{HeaderMap, StatusCode};
    use axum::routing::post;
    use axum::{Router, body::Bytes};
    use kayak_core::config::Secret;
    use kayak_core::connections::{ConnectionKind, Connections, InduConnection};
    use std::collections::HashMap;
    use std::sync::Mutex;

    fn ok<T, E: std::fmt::Display>(result: std::result::Result<T, E>) -> T {
        result.unwrap_or_else(|e| panic!("the test could not get this far: {e}"))
    }

    /// The error a call was expected to produce.
    fn failure<T>(result: Result<T>, expected: &str) -> anyhow::Error {
        match result {
            Ok(_) => panic!("{expected}, but the call succeeded"),
            Err(err) => err,
        }
    }

    fn connections(url: &str, key: &str) -> Arc<Connections> {
        Arc::new(
            [(
                "indu".to_string(),
                ConnectionKind::Indu(InduConnection {
                    url: url.to_string(),
                    ingest_url: None,
                    api_key: Secret::new(key),
                }),
            )]
            .into_iter()
            .collect(),
        )
    }

    fn build(config: InduOutputConfig, url: &str) -> Result<Box<dyn OutputDestination>> {
        let mut pipelines = HashMap::new();
        let (events, _rx) = tokio::sync::broadcast::channel(4);
        let secrets = Arc::new(MapSecretStore::new(
            "a test store",
            &[("INDU_API_KEY", "indu.ak.test.secret"), ("BLANK", "")],
        ));
        let mut ctx = BuildCtx::with_secrets(&mut pipelines, "p".to_string(), events, secrets)
            .with_connections(connections(url, "${INDU_API_KEY}"));
        config.build(&mut ctx)
    }

    fn config() -> InduOutputConfig {
        InduOutputConfig {
            connection: "indu".to_string(),
            series: vec![
                InduSeries {
                    stream: "{machine}/oee".to_string(),
                    value: "oee".to_string(),
                    unit: Some("%".to_string()),
                },
                InduSeries {
                    stream: "{machine}/availability".to_string(),
                    value: "stats.availability".to_string(),
                    unit: None,
                },
            ],
            at: Some("at".to_string()),
            timeout_seconds: None,
        }
    }

    #[test]
    fn placeholders_are_filled_from_the_message() {
        let message = json!({"machine": "press-3", "n": 7, "nested": {"a": 1}});
        assert_eq!(
            render("{machine}/oee", &message).as_deref(),
            Some("press-3/oee")
        );
        assert_eq!(render("line-{n}", &message).as_deref(), Some("line-7"));
        assert_eq!(render("plain", &message).as_deref(), Some("plain"));
        assert!(render("{missing}/oee", &message).is_none());
        assert!(
            render("{nested}/oee", &message).is_none(),
            "an object is not a name"
        );
    }

    #[test]
    fn a_batch_becomes_one_row_per_message_per_series_and_skips_what_is_missing() {
        let output = ok(build(config(), "http://127.0.0.1:59999"));
        // reach the concrete type through a second build: the trait object
        // hides `rows`, and the test wants the translation, not the request
        drop(output);
        let concrete = InduOutput {
            url: ok(Url::parse("http://127.0.0.1:59999/ingest/v1/streams")),
            described: String::new(),
            credential: HeaderValue::from_static("Bearer x"),
            series: config().series,
            at: Some("at".to_string()),
            client: Client::new(),
            gate: Gate::new(),
            sequence: 0,
        };
        let batch: MessageBatch = vec![
            Arc::new(json!({
                "machine": "press-3", "at": "2026-09-02T10:00:00Z",
                "oee": 0.83, "stats": {"availability": 0.9}
            })),
            // no availability, and oee is a string: only nothing lands
            Arc::new(json!({"machine": "press-4", "at": "2026-09-02T10:00:00Z", "oee": "n/a"})),
            // no `at`: the batch's own time is used
            Arc::new(json!({"machine": "press-5", "oee": 0.5})),
        ];
        let rows = concrete.rows(&batch, "NOW");
        assert_eq!(rows.len(), 2, "{rows:?}");
        assert_eq!(rows[0]["stream"], "press-3/oee");
        assert_eq!(rows[0]["unit"], "%");
        assert_eq!(rows[0]["value"], 0.83);
        assert_eq!(rows[1]["stream"], "press-3/availability");
        assert!(rows[1].get("unit").is_none());
        // press-5 has no `at` field, and `at` names one: skipped, because a
        // reading with a made-up time is worse than no reading
        assert!(rows.iter().all(|r| r["stream"] != "press-5/oee"));
    }

    #[test]
    fn a_missing_series_list_or_a_blank_entry_is_refused_at_build() {
        let mut empty = config();
        empty.series.clear();
        assert!(build(empty, "http://127.0.0.1:59999").is_err());

        let mut blank = config();
        blank.series[0].value = " ".to_string();
        assert!(build(blank, "http://127.0.0.1:59999").is_err());
    }

    #[test]
    fn an_empty_key_and_a_bad_origin_are_refused_at_build() {
        let mut pipelines = HashMap::new();
        let (events, _rx) = tokio::sync::broadcast::channel(4);
        let secrets = Arc::new(MapSecretStore::new("a test store", &[("BLANK", "")]));
        let mut ctx = BuildCtx::with_secrets(&mut pipelines, "p".to_string(), events, secrets)
            .with_connections(connections("http://127.0.0.1:59999", "${BLANK}"));
        assert!(config().build(&mut ctx).is_err(), "an empty key");

        assert!(build(config(), "not a url").is_err());
        assert!(build(config(), "ftp://example.com").is_err());
    }

    #[test]
    fn the_key_becomes_a_sensitive_header_and_nothing_else() {
        let header = ok(credential("indu.ak.secret"));
        assert!(header.is_sensitive());
        assert_eq!(format!("{header:?}"), "Sensitive");
        // and a key that cannot be a header value is refused rather than sent
        assert!(credential("with\nnewline").is_err());
    }

    /// What one request looked like when it arrived.
    #[derive(Clone)]
    struct Recorded {
        authorization: Option<String>,
        idempotency_key: Option<String>,
        body: Value,
    }

    #[derive(Default)]
    struct Endpoint {
        received: Mutex<Vec<Recorded>>,
        /// `(status, body)` to answer with. `None` is a 200 receipt.
        answer: Mutex<Option<(u16, Value)>>,
    }

    async fn ingest(
        State(endpoint): State<Arc<Endpoint>>,
        headers: HeaderMap,
        body: Bytes,
    ) -> (StatusCode, String) {
        let header = |name: &str| {
            headers
                .get(name)
                .and_then(|v| v.to_str().ok())
                .map(str::to_string)
        };
        ok(endpoint.received.lock()).push(Recorded {
            authorization: header("authorization"),
            idempotency_key: header(IDEMPOTENCY_HEADER),
            body: serde_json::from_slice(&body).unwrap_or(Value::Null),
        });
        match ok(endpoint.answer.lock()).clone() {
            Some((status, body)) => (
                StatusCode::from_u16(status).unwrap_or(StatusCode::OK),
                body.to_string(),
            ),
            None => (
                StatusCode::OK,
                json!({"accepted": 2, "rejected": 0}).to_string(),
            ),
        }
    }

    async fn serve() -> (String, Arc<Endpoint>) {
        let endpoint = Arc::new(Endpoint::default());
        let app = Router::new()
            .route(STREAMS_PATH, post(ingest))
            .with_state(Arc::clone(&endpoint));
        let listener = ok(tokio::net::TcpListener::bind("127.0.0.1:0").await);
        let addr = ok(listener.local_addr());
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (format!("http://{addr}"), endpoint)
    }

    #[tokio::test]
    async fn a_batch_is_posted_as_readings_with_the_key_and_an_idempotency_key() {
        let (url, endpoint) = serve().await;
        let mut output = ok(build(config(), &url));
        ok(output.init().await);

        let batch: MessageBatch = vec![Arc::new(json!({
            "machine": "press-3", "at": "2026-09-02T10:00:00Z",
            "oee": 0.83, "stats": {"availability": 0.9}
        }))];
        ok(output.emit(Arc::new(batch)).await);

        let received = ok(endpoint.received.lock()).clone();
        assert_eq!(received.len(), 1);
        let request = &received[0];
        assert_eq!(
            request.authorization.as_deref(),
            Some("Bearer indu.ak.test.secret")
        );
        assert!(
            request
                .idempotency_key
                .as_deref()
                .is_some_and(|k| k.starts_with("kayak-")),
            "{:?}",
            request.idempotency_key
        );
        let readings = ok(request.body["readings"].as_array().ok_or("no readings"));
        assert_eq!(readings.len(), 2);
        assert_eq!(readings[0]["stream"], "press-3/oee");
        assert_eq!(readings[0]["at"], "2026-09-02T10:00:00Z");
        assert_eq!(readings[1]["stream"], "press-3/availability");
    }

    #[tokio::test]
    async fn a_partial_acceptance_fails_the_batch_naming_the_refused_row() {
        let (url, endpoint) = serve().await;
        *ok(endpoint.answer.lock()) = Some((
            207,
            json!({
                "accepted": 1, "rejected": 1,
                "errors": [{"row": 1, "code": "unknown_stream", "detail": "no stream \"press-3/availability\""}]
            }),
        ));
        let mut output = ok(build(config(), &url));
        let batch: MessageBatch = vec![Arc::new(json!({
            "machine": "press-3", "at": "2026-09-02T10:00:00Z",
            "oee": 0.83, "stats": {"availability": 0.9}
        }))];
        let err = failure(output.emit(Arc::new(batch)).await, "a 207 is a failure");
        let text = format!("{err:#}");
        assert!(text.contains("1 row(s) refused"), "{text}");
        assert!(text.contains("unknown_stream"), "{text}");
        assert!(text.contains("row 1"), "{text}");
    }

    #[tokio::test]
    async fn a_refusal_is_quoted_and_the_gate_holds_the_next_batch() {
        let (url, endpoint) = serve().await;
        *ok(endpoint.answer.lock()) = Some((
            401,
            json!({"title": "Unauthenticated", "detail": "the credential was not accepted"}),
        ));
        let mut output = ok(build(config(), &url));
        let batch = Arc::new(vec![Arc::new(json!({
            "machine": "press-3", "at": "2026-09-02T10:00:00Z", "oee": 0.83
        }))]);
        let err = failure(output.emit(Arc::clone(&batch)).await, "a 401 is a failure");
        assert!(format!("{err:#}").contains("401"), "{err:#}");

        // The next batch is held back without a request.
        let err = failure(output.emit(batch).await, "held by the gate");
        assert!(format!("{err:#}").contains("still failing"), "{err:#}");
        assert_eq!(ok(endpoint.received.lock()).len(), 1);
    }

    #[tokio::test]
    async fn a_batch_with_nothing_to_write_sends_nothing() {
        let (url, endpoint) = serve().await;
        let mut output = ok(build(config(), &url));
        let batch: MessageBatch = vec![Arc::new(json!({"unrelated": true}))];
        ok(output.emit(Arc::new(batch)).await);
        assert!(ok(endpoint.received.lock()).is_empty());
    }
}
