//! Sends batches to an http endpoint — the pushing half of the `http` family.
//!
//! The three http components divide up cleanly. The **input** is reached and
//! never reaches out; the **transform** reaches out and replaces the batch with
//! what comes back; this one reaches out and is the end of the chain, so the
//! reply's body is thrown away and only its *status* is read.
//!
//! There is no connection kind behind it, and that is deliberate rather than an
//! omission. A connection holds *what a system is* against what one pipeline
//! wants from it, and for a webhook there is nothing on the first side of that
//! line: the url is the whole of it, and two pipelines posting to two webhooks
//! on the same host share nothing worth naming once. The `auth` block is the
//! same one the `http` input takes, for the same reason it lives on the
//! component there — one shared credential for every endpoint is wrong the
//! moment there are two.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow};
use kayak_core::config::{HttpAuthConfig, HttpBodyKind, HttpOutputConfig, HttpVerb};
use reqwest::header::{CONTENT_TYPE, HeaderName, HeaderValue};
use reqwest::{Client, Method, Url};

use crate::{
    BuildCtx,
    backoff::Gate,
    inputs::MessageBatch,
    outputs::{BuildOutput, OutputDestination},
};

/// How long a request may take before it is given up on, when the config
/// doesn't say. Thirty seconds is generous for a webhook and is still a bound:
/// without one, an endpoint that accepts a connection and then never answers
/// holds the pipeline's run loop for as long as it likes.
const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// How much of a rejecting endpoint's response body is quoted in the error.
///
/// The body is the only thing that says *why* a request was refused, which is
/// why it is read at all — but an html error page is megabytes, and this text
/// becomes an [`crate::history::ErrorSignature`] key as well as a line in the
/// UI. Bounded here rather than at either of those.
const MAX_DETAIL_BYTES: usize = 300;

impl BuildOutput for HttpOutputConfig {
    fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn OutputDestination>> {
        // parsed here rather than left to the first `send`: a typo in a url is
        // a config mistake, and a pipeline that starts and then fails once a
        // second says so much less clearly than one that refuses to build
        let url = Url::parse(&self.url)
            .with_context(|| format!("the http output's url '{}' is not a url", self.url))?;
        anyhow::ensure!(
            matches!(url.scheme(), "http" | "https"),
            "the http output's url '{}' is not an http url; the scheme is '{}'",
            self.url,
            url.scheme()
        );

        let verb = self.verb.unwrap_or(HttpVerb::Post);
        let method = match verb {
            HttpVerb::Post => Method::POST,
            HttpVerb::Put => Method::PUT,
            HttpVerb::Patch => Method::PATCH,
            // An output exists to send the messages somewhere. A request with
            // no body has nowhere to put them, so this would be a pipeline
            // making a round trip per batch and delivering nothing — refused
            // here rather than discovered from an endpoint that never receives
            // anything.
            HttpVerb::Get | HttpVerb::Delete => {
                anyhow::bail!(
                    "the http output cannot use {verb}: a request with no body would send none \
                     of the messages. Use POST, PUT or PATCH."
                )
            }
        };

        let credential = self
            .auth
            .as_ref()
            .map(|auth| Credential::build(auth, ctx))
            .transpose()?;

        let timeout = self
            .timeout_seconds
            .map_or(DEFAULT_TIMEOUT, Duration::from_secs);
        let client = Client::builder()
            .timeout(timeout)
            .build()
            .context("failed to build the http output's client")?;

        Ok(Box::new(HttpOutput {
            described: describe(&url),
            url,
            method,
            body: self.body.unwrap_or_default(),
            credential,
            client,
            gate: Gate::new(),
        }))
    }
}

/// How a url is named in an error and in a log.
///
/// Userinfo is stripped, because `https://kayak:hunter2@example.com/hook` is a
/// perfectly ordinary way to write a webhook url and an error message is
/// exactly the place a password should not turn up. The rest is left alone —
/// a query string is part of what someone needs to see to recognise which
/// endpoint failed.
fn describe(url: &Url) -> String {
    let mut clean = url.clone();
    if clean.password().is_some() {
        let _ = clean.set_password(None);
    }
    if !clean.username().is_empty() {
        let _ = clean.set_username("");
    }
    clean.to_string()
}

/// The header this output presents on every request, already resolved.
///
/// The value is marked sensitive, so anything that dumps a request's headers
/// prints it as `Sensitive` rather than as the token. That is the outbound
/// twin of the rule the input's [`crate::inputs::http::Credentials`] follows:
/// the credential is held in exactly one place and never travels anywhere it
/// could be written down.
struct Credential {
    name: HeaderName,
    value: HeaderValue,
}

impl Credential {
    fn build(config: &HttpAuthConfig, ctx: &BuildCtx) -> Result<Self> {
        let (name, prefix, secret) = match config {
            HttpAuthConfig::Bearer { token } => ("authorization", "Bearer ", token),
            HttpAuthConfig::Header { name, value } => {
                let trimmed = name.trim();
                anyhow::ensure!(
                    !trimmed.is_empty(),
                    "an http output's `auth` header needs a name"
                );
                (trimmed, "", value)
            }
        };
        // unlike the input's, this name is not checked against ALLOWED_HEADERS:
        // that rule exists because an input's `envelope` copies headers into the
        // messages, and nothing here reads a header at all
        let name = HeaderName::try_from(name.to_ascii_lowercase())
            .with_context(|| format!("'{name}' is not a valid http header name"))?;

        let resolved = ctx.resolve(secret)?;
        anyhow::ensure!(
            !resolved.expose().is_empty(),
            "the credential for an http output's `auth` is empty, so the requests would carry an \
             empty header; check that '{resolved}' is set in the secret store"
        );
        // `expose` is one of the few places a real secret is reached; it goes
        // straight into the header and is not held, logged or copied
        let mut value = HeaderValue::try_from(format!("{prefix}{}", resolved.expose()))
            .context("the credential for an http output's `auth` cannot be sent as a header")?;
        value.set_sensitive(true);

        Ok(Self { name, value })
    }
}

pub struct HttpOutput {
    url: Url,
    /// [`describe`]d once at build time — it goes into every error, and the
    /// stripping should not be redone per batch.
    described: String,
    method: Method,
    body: HttpBodyKind,
    credential: Option<Credential>,
    client: Client,
    /// Paces retries after a request fails.
    ///
    /// The same thing the clickhouse output's gate guards, and for the same
    /// reason: `reqwest::Client` is a stateless pool, so there is no connection
    /// to drop and rebuild — what is worth skipping is the *request*, which
    /// against a webhook that is down or refusing costs a real round trip on
    /// every batch the pipeline produces.
    gate: Gate,
}

impl HttpOutput {
    /// One request with `body` as its JSON body. `Ok(())` for a 2xx, an error
    /// carrying the status and what the endpoint said for anything else.
    async fn send(&self, body: String) -> Result<()> {
        let mut request = self
            .client
            .request(self.method.clone(), self.url.clone())
            .header(CONTENT_TYPE, "application/json")
            .body(body);
        if let Some(credential) = &self.credential {
            request = request.header(credential.name.clone(), credential.value.clone());
        }

        let response = request
            .send()
            .await
            .with_context(|| format!("failed to reach {}", self.described))?;

        let status = response.status();
        if status.is_success() {
            return Ok(());
        }
        let detail = response
            .text()
            .await
            .unwrap_or_else(|e| format!("<the error body could not be read: {e}>"));
        Err(anyhow!(
            "{} refused the request ({status}): {}",
            self.described,
            truncate(detail.trim())
        ))
    }
}

/// The first [`MAX_DETAIL_BYTES`] of an endpoint's complaint, cut on a
/// character boundary.
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
impl OutputDestination for HttpOutput {
    /// Nothing, deliberately.
    ///
    /// Every other output that talks to a server connects here, so a pipeline
    /// pointed at a broker that is down fails to start rather than at the first
    /// batch. There is no equivalent request to make: an http client opens
    /// nothing, and the only way to find out whether the endpoint is there
    /// would be to send it something — which for a webhook means delivering a
    /// request that carries no messages and means nothing. A url that is wrong
    /// is caught at build time; a url that is right and unreachable is heard
    /// about on the first batch, through the gate.
    async fn init(&mut self) -> Result<()> {
        Ok(())
    }

    async fn emit(&mut self, message_batch: Arc<MessageBatch>) -> Result<()> {
        // a filter can empty a batch, and an endpoint asked to accept `[]` —
        // or, worse, a request per nothing — learns nothing from it
        if message_batch.is_empty() {
            return Ok(());
        }

        let now = Instant::now();
        if !self.gate.ready(now) {
            anyhow::bail!(
                "the http output at {} is still failing; not retrying yet",
                self.described
            );
        }

        let result = match self.body {
            HttpBodyKind::Batch => {
                let body = serde_json::to_string(&*message_batch)
                    .context("failed to serialize the batch for the http output")?;
                self.send(body).await
            }
            HttpBodyKind::Message => {
                let mut sent = Ok(());
                for (index, message) in message_batch.iter().enumerate() {
                    let body = serde_json::to_string(message)
                        .context("failed to serialize a message for the http output")?;
                    // the first failure stops the batch: the same
                    // all-or-nothing a broker publish loop has, and continuing
                    // would report one error for a batch that was half
                    // delivered
                    sent = self.send(body).await.with_context(|| {
                        format!("message {} of {}", index + 1, message_batch.len())
                    });
                    if sent.is_err() {
                        break;
                    }
                }
                sent
            }
        };

        match result {
            Ok(()) => {
                self.gate.record_success();
                Ok(())
            }
            Err(e) => {
                self.gate.record_failure(now);
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{MapSecretStore, batch};
    use axum::extract::State;
    use axum::http::{HeaderMap, StatusCode};
    use axum::routing::any;
    use axum::{Router, body::Bytes};
    use kayak_core::config::Secret;
    use serde_json::{Value, json};
    use std::collections::HashMap;
    use std::sync::Mutex;

    /// Unwrap something the test needs in order to be a test at all. `.expect`
    /// is a clippy error in this workspace, tests included, so a failing setup
    /// says what went wrong here rather than in a lint exception.
    fn ok<T, E: std::fmt::Display>(result: std::result::Result<T, E>) -> T {
        result.unwrap_or_else(|e| panic!("the test could not get this far: {e}"))
    }

    fn build(config: HttpOutputConfig) -> Result<Box<dyn OutputDestination>> {
        let mut pipelines = HashMap::new();
        let (events, _rx) = tokio::sync::broadcast::channel(4);
        let secrets = Arc::new(MapSecretStore::new(
            "a test store",
            &[("TOKEN", "hunter2"), ("BLANK", "")],
        ));
        let mut ctx = BuildCtx::with_secrets(&mut pipelines, "p".to_string(), events, secrets);
        config.build(&mut ctx)
    }

    fn config(url: &str) -> HttpOutputConfig {
        HttpOutputConfig {
            url: url.to_string(),
            verb: None,
            body: None,
            auth: None,
            timeout_seconds: None,
        }
    }

    #[test]
    fn building_does_not_require_the_endpoint_to_exist() {
        // nothing is sent until the first batch — the same promise every other
        // output's build makes about its broker
        assert!(build(config("http://127.0.0.1:59999/hook")).is_ok());
    }

    #[test]
    fn a_url_that_is_not_a_url_is_refused() {
        assert!(build(config("not a url")).is_err());
        assert!(build(config("ftp://example.com/drop")).is_err());
    }

    #[test]
    fn a_verb_with_no_body_is_refused() {
        for verb in [HttpVerb::Get, HttpVerb::Delete] {
            let mut config = config("http://example.com/hook");
            config.verb = Some(verb);
            assert!(build(config).is_err(), "{verb} should be refused");
        }
        for verb in [HttpVerb::Post, HttpVerb::Put, HttpVerb::Patch] {
            let mut config = config("http://example.com/hook");
            config.verb = Some(verb);
            assert!(build(config).is_ok(), "{verb} should be accepted");
        }
    }

    #[test]
    fn a_credential_that_resolves_to_nothing_is_refused() {
        let mut config = config("http://example.com/hook");
        config.auth = Some(HttpAuthConfig::Bearer {
            token: Secret::new("${BLANK}"),
        });
        assert!(build(config).is_err());
    }

    #[test]
    fn a_header_name_that_is_not_a_header_name_is_refused() {
        for name in ["", "not a header"] {
            let mut config = config("http://example.com/hook");
            config.auth = Some(HttpAuthConfig::Header {
                name: name.to_string(),
                value: Secret::new("${TOKEN}"),
            });
            assert!(build(config).is_err(), "'{name}' should be refused");
        }
    }

    /// A header on the input's allow-list is refused *there* and fine here:
    /// that rule is about an `envelope` copying a credential into the messages,
    /// and an output reads no headers at all.
    #[test]
    fn the_inbound_allow_list_does_not_apply_outbound() {
        let mut config = config("http://example.com/hook");
        config.auth = Some(HttpAuthConfig::Header {
            name: "x-request-id".to_string(),
            value: Secret::new("${TOKEN}"),
        });
        assert!(build(config).is_ok());
    }

    #[test]
    fn a_url_is_named_in_errors_without_its_userinfo() {
        let url = ok(Url::parse("https://kayak:hunter2@example.com/hook?tag=a"));
        let described = describe(&url);
        assert!(!described.contains("hunter2"), "{described}");
        assert!(!described.contains("kayak"), "{described}");
        assert!(described.contains("example.com/hook?tag=a"), "{described}");
    }

    #[test]
    fn a_long_complaint_is_cut_and_marked() {
        let long = "x".repeat(MAX_DETAIL_BYTES * 2);
        let cut = truncate(&long);
        assert!(cut.len() <= MAX_DETAIL_BYTES + 4, "{}", cut.len());
        assert!(cut.ends_with('…'));
        assert_eq!(truncate("short"), "short");
    }

    /// What one request looked like when it arrived.
    #[derive(Clone)]
    struct Recorded {
        method: String,
        authorization: Option<String>,
        body: Value,
    }

    #[derive(Default)]
    struct Endpoint {
        received: Mutex<Vec<Recorded>>,
        /// What to answer with. `None` is a 200.
        refuse_with: Mutex<Option<u16>>,
    }

    impl Endpoint {
        fn received(&self) -> Vec<Recorded> {
            self.received
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        }

        fn refuse_with(&self, status: u16) {
            *self
                .refuse_with
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(status);
        }
    }

    /// A real endpoint on a loopback port, so the assertions are about what
    /// went over the wire rather than about a fake this module was written
    /// against. Nothing outside the process is touched.
    async fn endpoint() -> (Arc<Endpoint>, String) {
        let state = Arc::new(Endpoint::default());
        let app = Router::new()
            .route("/hook", any(record))
            .with_state(Arc::clone(&state));

        let listener = ok(tokio::net::TcpListener::bind("127.0.0.1:0").await);
        let addr = ok(listener.local_addr());
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (state, format!("http://{addr}/hook"))
    }

    async fn record(
        State(state): State<Arc<Endpoint>>,
        method: axum::http::Method,
        headers: HeaderMap,
        body: Bytes,
    ) -> (StatusCode, &'static str) {
        let recorded = Recorded {
            method: method.to_string(),
            authorization: headers
                .get("authorization")
                .and_then(|value| value.to_str().ok())
                .map(ToString::to_string),
            body: serde_json::from_slice(&body)
                .unwrap_or_else(|_| Value::String("<not json>".to_string())),
        };
        state
            .received
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push(recorded);

        let refuse = *state
            .refuse_with
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        match refuse {
            None => (StatusCode::OK, "ok"),
            Some(code) => (
                StatusCode::from_u16(code).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
                "no thank you",
            ),
        }
    }

    #[tokio::test]
    async fn a_batch_goes_as_one_request_carrying_a_json_array() {
        let (endpoint, url) = endpoint().await;
        let mut output = ok(build(config(&url)));
        ok(output.emit(batch(vec![json!({"a": 1}), json!({"a": 2})])).await);

        let received = endpoint.received();
        assert_eq!(received.len(), 1, "one batch is one request");
        assert_eq!(received[0].method, "POST", "POST is the default verb");
        assert_eq!(received[0].body, json!([{"a": 1}, {"a": 2}]));
    }

    #[tokio::test]
    async fn the_message_shape_sends_one_request_per_message() {
        let (endpoint, url) = endpoint().await;
        let mut config = config(&url);
        config.body = Some(HttpBodyKind::Message);
        config.verb = Some(HttpVerb::Put);
        let mut output = ok(build(config));
        ok(output.emit(batch(vec![json!({"a": 1}), json!({"a": 2})])).await);

        let received = endpoint.received();
        assert_eq!(received.len(), 2);
        assert_eq!(received[0].body, json!({"a": 1}));
        assert_eq!(received[1].body, json!({"a": 2}));
        assert!(received.iter().all(|r| r.method == "PUT"));
    }

    #[tokio::test]
    async fn a_bearer_token_is_presented_on_every_request() {
        let (endpoint, url) = endpoint().await;
        let mut config = config(&url);
        config.body = Some(HttpBodyKind::Message);
        config.auth = Some(HttpAuthConfig::Bearer {
            token: Secret::new("${TOKEN}"),
        });
        let mut output = ok(build(config));
        ok(output.emit(batch(vec![json!({"a": 1}), json!({"a": 2})])).await);

        let received = endpoint.received();
        assert_eq!(received.len(), 2);
        assert!(
            received
                .iter()
                .all(|r| r.authorization.as_deref() == Some("Bearer hunter2")),
            "every request carries the token"
        );
    }

    #[tokio::test]
    async fn a_refused_request_fails_the_batch() {
        let (endpoint, url) = endpoint().await;
        let mut output = ok(build(config(&url)));
        endpoint.refuse_with(422);

        let error = output
            .emit(batch(vec![json!({"a": 1})]))
            .await
            .err()
            .unwrap_or_else(|| panic!("a 422 is not a delivery"));
        let text = format!("{error:#}");
        assert!(text.contains("422"), "{text}");
        assert!(text.contains("no thank you"), "{text}");
    }

    /// The gate's whole point: a failing endpoint gets one attempt, not one per
    /// batch at whatever rate the pipeline produces them.
    #[tokio::test]
    async fn a_failing_endpoint_is_not_retried_on_the_next_batch() {
        let (endpoint, url) = endpoint().await;
        let mut output = ok(build(config(&url)));
        endpoint.refuse_with(500);

        for _ in 0..5 {
            assert!(output.emit(batch(vec![json!({"a": 1})])).await.is_err());
        }
        assert_eq!(
            endpoint.received().len(),
            1,
            "only the first batch should have reached the network"
        );
    }

    #[tokio::test]
    async fn an_empty_batch_sends_nothing() {
        let (endpoint, url) = endpoint().await;
        let mut output = ok(build(config(&url)));
        ok(output.emit(batch(vec![])).await);
        assert!(endpoint.received().is_empty());
    }
}
