//! What both snowflake components speak: the SQL API, one statement at a time.
//!
//! `POST /api/v2/statements` over `reqwest`, because there is no Rust driver to
//! lean on and the API is the part of Snowflake that is documented as a wire
//! format. [`Session`] is the whole of it — signing in, submitting a statement,
//! waiting for a slow one, reading a result that came back in partitions — and
//! neither component knows any of that. It is the line `outbound.rs` draws for
//! the http family: `src/inputs/snowflake.rs` knows what to ask and how a row
//! reads, `src/outputs/snowflake.rs` knows the DDL and how a batch is carried.
//!
//! Four things here are load-bearing:
//!
//! - **The output formats are on every request.** Left to itself the API sends
//!   a timestamp as seconds since the epoch with an offset biased by 1440, and
//!   its own documentation disagrees with itself about that in places. The
//!   `*_output_format` parameters make the server render RFC 3339 instead, which
//!   is the same line every other SQL component draws: the server renders, this
//!   side does not do date arithmetic. Account and user settings for these are
//!   ignored by the API, so the rendering cannot move under a pipeline.
//! - **A retry is the same request id.** A connection lost after the server took
//!   a statement says nothing about whether it ran. `requestId` with `retry=true`
//!   is the API's answer: a statement that already succeeded is not run again,
//!   which is what makes retrying an `INSERT` safe here at all.
//! - **A slow statement is a `202`, not a slow reply.** Anything past 45 seconds
//!   comes back with a handle to ask again with, so a read of a large table has
//!   to survive that. The wait is bounded by the statement's own timeout, which
//!   the connection sets: an unbounded statement is an unbounded warehouse bill,
//!   and the server's default is two days.
//! - **The statement handle is kept out of error messages.** It differs on every
//!   attempt, and an error's text is the key `history` counts it under, so one
//!   mistake would read as a thousand different ones.

use std::io::Read;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use aws_lc_rs::digest::{SHA256, digest};
use aws_lc_rs::encoding::{AsDer, PublicKeyX509Der};
use aws_lc_rs::signature::{KeyPair as _, RsaKeyPair};
use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use flate2::read::GzDecoder;
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use kayak_core::connections::{SnowflakeAuth, SnowflakeConnection};
use reqwest::header::RETRY_AFTER;
use reqwest::{Client, RequestBuilder, StatusCode};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use uuid::Uuid;

use crate::{BuildCtx, backoff::Backoff, outbound::truncate, secrets::Resolved};

/// A statement runs for this long unless the connection says otherwise.
pub const DEFAULT_STATEMENT_TIMEOUT_SECS: u64 = 300;
/// One request, as against one statement: the server answers within 45 seconds
/// whatever the statement is doing, so this only ever trips on a dead peer.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(120);
/// How many times one request is tried while the answer is a transient one.
const MAX_ATTEMPTS: u32 = 5;
/// Waited out by the longest `Retry-After` this will honour.
const MAX_RETRY_AFTER: Duration = Duration::from_secs(30);
/// The first and the longest wait between asking after a slow statement.
const FIRST_STATUS_WAIT: Duration = Duration::from_millis(200);
const MAX_STATUS_WAIT: Duration = Duration::from_secs(5);
/// How long past its own timeout a statement is waited for before giving up,
/// so the server's cancellation is what is usually reported.
const STATUS_GRACE: Duration = Duration::from_secs(60);
/// The API takes a key-pair token for at most an hour after it was issued.
const JWT_LIFETIME_SECS: i64 = 59 * 60;
/// A token this close to its end is not sent; one could expire on the way.
const JWT_RENEW_MARGIN: Duration = Duration::from_mins(5);
/// The code on a `200` whose result was too large to come back complete.
const RESULT_TOO_LARGE: &str = "391908";

/// A timestamp with an offset, as it crosses in both directions. Nine digits
/// because a watermark rounded to the default three would read the rows it was
/// taken from again: `> 12:00:00.123` matches `12:00:00.1234`, forever.
pub const ZONED_FORMAT: &str = "YYYY-MM-DD\"T\"HH24:MI:SS.FF9TZH:TZM";
/// A timestamp with no zone, for `TIMESTAMP_NTZ`, which has none to print.
pub const NAIVE_FORMAT: &str = "YYYY-MM-DD\"T\"HH24:MI:SS.FF9";
pub const DATE_FORMAT: &str = "YYYY-MM-DD";
pub const TIME_FORMAT: &str = "HH24:MI:SS.FF9";

/// The name an unquoted identifier resolves to in Snowflake.
///
/// `CREATE TABLE readings` makes a table called `READINGS`, so a config naming
/// `readings` means that table, and the same goes for its columns. It is folded
/// here and then quoted like any other identifier, which is what lets a column
/// called `value` or `order` exist. The cost is that a name someone created
/// quoted in lower case cannot be named; a `query` can still reach it on the
/// input side.
#[must_use]
pub fn fold(name: &str) -> String {
    name.to_ascii_uppercase()
}

/// One column of a result, as the server describes it.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Column {
    pub name: String,
    /// `fixed`, `text`, `timestamp_ntz`… The API's own examples spell these in
    /// both cases, so [`Column::kind`] is how they are read.
    #[serde(rename = "type")]
    pub type_name: String,
    #[serde(default)]
    pub precision: Option<u32>,
    #[serde(default)]
    pub scale: Option<u32>,
}

impl Column {
    /// The type, in the case every comparison here is written in.
    #[must_use]
    pub fn kind(&self) -> String {
        self.type_name.to_ascii_lowercase()
    }
}

/// A whole result: every partition's rows, in order. Every value is the
/// server's text, or `None` for a SQL `NULL`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ResultSet {
    pub columns: Vec<Column>,
    pub rows: Vec<Vec<Option<String>>>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Reply {
    #[serde(default)]
    code: Option<String>,
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    sql_state: Option<String>,
    #[serde(default)]
    statement_handle: Option<String>,
    #[serde(default)]
    result_set_meta_data: Option<Metadata>,
    #[serde(default)]
    data: Vec<Vec<Option<String>>>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Metadata {
    #[serde(default)]
    row_type: Vec<Column>,
    #[serde(default)]
    partition_info: Vec<Value>,
}

#[derive(Serialize)]
struct Claims<'a> {
    iss: &'a str,
    sub: &'a str,
    iat: i64,
    exp: i64,
}

/// A key the API can check, and the token last signed with it.
struct KeyPairAuth {
    key: EncodingKey,
    issuer: String,
    subject: String,
    issued: Option<(String, Instant)>,
}

enum Auth {
    KeyPair(Box<KeyPairAuth>),
    Token(Resolved),
}

impl Auth {
    fn bearer(&mut self) -> Result<String> {
        match self {
            Self::Token(token) => Ok(token.expose().to_string()),
            Self::KeyPair(pair) => {
                if let Some((token, renew_at)) = &pair.issued
                    && Instant::now() < *renew_at
                {
                    return Ok(token.clone());
                }
                let iat = chrono::Utc::now().timestamp();
                let token = jsonwebtoken::encode(
                    &Header::new(Algorithm::RS256),
                    &Claims {
                        iss: &pair.issuer,
                        sub: &pair.subject,
                        iat,
                        exp: iat + JWT_LIFETIME_SECS,
                    },
                    &pair.key,
                )
                .context("failed to sign the snowflake token")?;
                let lifetime = Duration::from_secs(JWT_LIFETIME_SECS.unsigned_abs());
                pair.issued = Some((
                    token.clone(),
                    Instant::now() + lifetime.saturating_sub(JWT_RENEW_MARGIN),
                ));
                Ok(token)
            }
        }
    }

    /// Named rather than left to the server to work out, for a key pair. A
    /// `token` is either kind the API takes, and it inspects one it isn't told
    /// about, so the same field serves a programmatic token and an OAuth one.
    fn token_type(&self) -> Option<&'static str> {
        match self {
            Self::KeyPair(_) => Some("KEYPAIR_JWT"),
            Self::Token(_) => None,
        }
    }

    /// A `401` means this token is no use, so the next attempt signs another.
    fn forget(&mut self) {
        if let Self::KeyPair(pair) = self {
            pair.issued = None;
        }
    }
}

/// The DER of a PKCS#8 PEM, however a secret store has carried its line breaks:
/// an environment variable holds them as a literal `\n`, or loses them.
fn pkcs8_der(pem: &str) -> Result<Vec<u8>> {
    let pem = pem.replace("\\n", "\n");
    let pem = pem.trim();
    if pem.contains("ENCRYPTED PRIVATE KEY") {
        bail!(
            "the private key is encrypted, and kayak has no passphrase to open it with; decrypt \
             it with `openssl pkcs8 -in key.p8 -nocrypt` and keep the result in the secret store"
        );
    }
    let body = pem
        .strip_prefix("-----BEGIN PRIVATE KEY-----")
        .and_then(|rest| rest.strip_suffix("-----END PRIVATE KEY-----"))
        .ok_or_else(|| {
            anyhow!(
                "the private key is not an unencrypted PKCS#8 PEM (it should start with \
                 '-----BEGIN PRIVATE KEY-----'); make one with `openssl pkcs8 -topk8 -nocrypt`"
            )
        })?;
    let base64: String = body.chars().filter(|c| !c.is_whitespace()).collect();
    STANDARD
        .decode(base64)
        .map_err(|_| anyhow!("the private key's PEM body is not valid base64"))
}

/// `SHA256:` and the base64 of the digest of the public key's DER, which is how
/// the token says which of a user's two registered keys signed it.
fn fingerprint(der: &[u8]) -> Result<String> {
    let pair = RsaKeyPair::from_pkcs8(der)
        .map_err(|e| anyhow!("the private key is not a usable RSA key: {e}"))?;
    let public: PublicKeyX509Der = pair
        .public_key()
        .as_der()
        .map_err(|_| anyhow!("failed to encode the public half of the private key"))?;
    Ok(format!(
        "SHA256:{}",
        STANDARD.encode(digest(&SHA256, public.as_ref()))
    ))
}

fn key_pair(account: &str, user: &str, pem: &str) -> Result<KeyPairAuth> {
    let der = pkcs8_der(pem)?;
    let fingerprint = fingerprint(&der)?;
    // rebuilt rather than handed on, since the text may have arrived with no
    // line breaks and the PEM reader wants them
    let lines: Vec<String> = STANDARD
        .encode(&der)
        .as_bytes()
        .chunks(64)
        .map(|line| String::from_utf8_lossy(line).into_owned())
        .collect();
    let canonical = format!(
        "-----BEGIN PRIVATE KEY-----\n{}\n-----END PRIVATE KEY-----\n",
        lines.join("\n")
    );
    let key = EncodingKey::from_rsa_pem(canonical.as_bytes())
        .map_err(|_| anyhow!("the private key is not a usable RSA key"))?;
    let subject = format!(
        "{}.{}",
        account.to_ascii_uppercase(),
        user.to_ascii_uppercase()
    );
    Ok(KeyPairAuth {
        key,
        issuer: format!("{subject}.{fingerprint}"),
        subject,
        issued: None,
    })
}

/// Where the API is. The account goes into a host name that credentials are
/// sent to, so it is checked as tightly as a table name is — and a period is
/// refused rather than guessed at, since the docs have it mean a region on the
/// host and something to replace in the token. `url` is the way to say the host
/// outright: a locator with a region, private link, or the China domain.
fn endpoint(server: &SnowflakeConnection, connection: &str) -> Result<String> {
    let account = &server.account;
    if account.is_empty()
        || !account
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_')
    {
        bail!(
            "connection '{connection}' has account '{account}'; write it as \
             'orgname-accountname' with letters, digits, hyphens and underscores. For an account \
             locator with a region, write the locator alone here and the full address in `url`"
        );
    }
    let Some(url) = &server.url else {
        // the docs take either spelling of an underscore in the host, and a
        // hyphen is the one a TLS client will not object to
        return Ok(format!(
            "https://{}.snowflakecomputing.com",
            account.to_ascii_lowercase().replace('_', "-")
        ));
    };
    let url = url.trim_end_matches('/').to_string();
    if url.starts_with("http://") {
        if !server.allow_http.unwrap_or(false) {
            bail!(
                "connection '{connection}' reaches snowflake over plaintext http, which would \
                 send its credentials in the clear; set \"allow_http\": true on the connection \
                 if that is what you want"
            );
        }
    } else if !url.starts_with("https://") {
        bail!("connection '{connection}' has url '{url}', which is not an http(s) url");
    }
    Ok(url)
}

/// Everything about one failed answer that belongs in an error: the server's
/// own words and their code, cut to the size a history key should be.
fn complaint(status: StatusCode, body: &[u8]) -> String {
    let text = String::from_utf8_lossy(body);
    let detail = match serde_json::from_slice::<Reply>(body) {
        Ok(Reply {
            message: Some(message),
            code,
            sql_state,
            ..
        }) => {
            let mut detail = message;
            if let Some(code) = code {
                detail = format!("{detail} (code {code})");
            }
            if let Some(state) = sql_state {
                detail = format!("{detail} (sql state {state})");
            }
            detail
        }
        _ => text.trim().to_string(),
    };
    format!("{status}: {}", truncate(&detail))
}

/// `true` for the answers the API says are worth asking again.
fn is_transient(status: StatusCode) -> bool {
    matches!(status.as_u16(), 429 | 500 | 502 | 503 | 504)
}

/// One partition after the first. It comes gzipped, and the magic bytes are
/// what is read rather than the header: what decides whether this is compressed
/// is what is in it. The docs show no body for one, so a bare list of rows and
/// the same object the first partition is are both read.
fn partition_rows(body: &[u8]) -> Result<Vec<Vec<Option<String>>>> {
    let mut plain = Vec::new();
    let json: &[u8] = if body.starts_with(&[0x1f, 0x8b]) {
        GzDecoder::new(body)
            .read_to_end(&mut plain)
            .context("a result partition could not be decompressed")?;
        &plain
    } else {
        body
    };
    if json.iter().find(|byte| !byte.is_ascii_whitespace()) == Some(&b'[') {
        return serde_json::from_slice(json).context("a result partition is not a list of rows");
    }
    let reply: Reply =
        serde_json::from_slice(json).context("a result partition is not one the API documents")?;
    Ok(reply.data)
}

struct Answer {
    status: StatusCode,
    retry_after: Option<Duration>,
    body: Vec<u8>,
}

/// One connection's worth of the SQL API. See the module docs.
pub struct Session {
    url: String,
    user: String,
    warehouse: String,
    database: String,
    schema: String,
    role: Option<String>,
    timeout_secs: u64,
    auth: Auth,
    client: Client,
}

impl Session {
    /// Reads the settings. Talks to nothing: a pipeline that builds is one whose
    /// settings parse, not one whose account happened to be reachable.
    pub fn build(ctx: &BuildCtx, connection: &str) -> Result<Self> {
        let server = ctx.snowflake_connection(connection)?;
        let url = endpoint(server, connection)?;
        let auth = match &server.auth {
            SnowflakeAuth::Token { token } => {
                Auth::Token(ctx.resolve(token).with_context(|| {
                    format!("failed to resolve secrets in the token of connection '{connection}'")
                })?)
            }
            SnowflakeAuth::KeyPair { private_key } => {
                let pem = ctx.resolve(private_key).with_context(|| {
                    format!(
                        "failed to resolve secrets in the private key of connection '{connection}'"
                    )
                })?;
                Auth::KeyPair(Box::new(
                    key_pair(&server.account, &server.user, pem.expose()).with_context(|| {
                        format!("connection '{connection}' has a private key kayak cannot use")
                    })?,
                ))
            }
        };
        let timeout_secs = server
            .statement_timeout_seconds
            .unwrap_or(DEFAULT_STATEMENT_TIMEOUT_SECS);
        // zero means the API's maximum, a week, which is nobody's idea of none
        if timeout_secs == 0 {
            bail!("connection '{connection}' has `statement_timeout_seconds` 0");
        }
        let client = Client::builder()
            .timeout(REQUEST_TIMEOUT)
            .user_agent(concat!("kayak/", env!("CARGO_PKG_VERSION")))
            .build()
            .context("failed to build the snowflake http client")?;
        Ok(Self {
            url,
            user: server.user.clone(),
            warehouse: server.warehouse.clone(),
            database: server.database.clone(),
            schema: server.schema.clone(),
            role: server.role.clone(),
            timeout_secs,
            auth,
            client,
        })
    }

    /// How this account is described in an error. No credentials in it.
    #[must_use]
    pub fn describe(&self) -> String {
        format!(
            "snowflake at {url} as '{user}' ({database}.{schema}, warehouse {warehouse})",
            url = self.url,
            user = self.user,
            database = self.database,
            schema = self.schema,
            warehouse = self.warehouse
        )
    }

    /// The request body: the statement, where it runs and how it renders.
    fn body(&self, statement: &str, texts: &[&str]) -> Value {
        let mut body = json!({
            "statement": statement,
            "timeout": self.timeout_secs,
            "warehouse": self.warehouse,
            "database": self.database,
            "schema": self.schema,
            "parameters": {
                "date_output_format": DATE_FORMAT,
                "time_output_format": TIME_FORMAT,
                "timestamp_output_format": ZONED_FORMAT,
                "timestamp_ltz_output_format": ZONED_FORMAT,
                "timestamp_tz_output_format": ZONED_FORMAT,
                "timestamp_ntz_output_format": NAIVE_FORMAT,
                // what an instant with no zone of its own is read in, so a
                // config means the same thing whatever the account is set to
                "timezone": "UTC",
            },
        });
        if let Some(role) = &self.role {
            body["role"] = json!(role);
        }
        if !texts.is_empty() {
            let bindings: serde_json::Map<String, Value> = texts
                .iter()
                .enumerate()
                .map(|(index, text)| {
                    (
                        (index + 1).to_string(),
                        json!({"type": "TEXT", "value": text}),
                    )
                })
                .collect();
            body["bindings"] = Value::Object(bindings);
        }
        body
    }

    /// One request, and whatever it was answered with. Nothing is judged here.
    async fn call(&mut self, request: RequestBuilder) -> Result<Answer> {
        // `bearer_auth` marks the header sensitive, so it can't print in a debug log
        let mut request = request.bearer_auth(self.auth.bearer()?);
        if let Some(token_type) = self.auth.token_type() {
            request = request.header("X-Snowflake-Authorization-Token-Type", token_type);
        }
        let response = request
            .header(reqwest::header::ACCEPT, "application/json")
            .send()
            .await
            // without the url, which holds this attempt's request id: this text
            // is a `history` key, and an outage should read as one failure
            .map_err(reqwest::Error::without_url)
            .with_context(|| format!("failed to reach {}", self.describe()))?;
        let status = response.status();
        if status == StatusCode::UNAUTHORIZED {
            self.auth.forget();
        }
        let retry_after = response
            .headers()
            .get(RETRY_AFTER)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.parse().ok())
            .map(|seconds: u64| Duration::from_secs(seconds).min(MAX_RETRY_AFTER));
        let body = response
            .bytes()
            .await
            .map_err(reqwest::Error::without_url)
            .with_context(|| format!("failed to read an answer from {}", self.describe()))?;
        Ok(Answer {
            status,
            retry_after,
            body: body.to_vec(),
        })
    }

    /// Asks until the answer is not a transient one, or the attempts run out.
    /// `build` is told which attempt it is making.
    async fn settled<F>(&mut self, build: F) -> Result<Answer>
    where
        F: Fn(&Client, u32) -> RequestBuilder,
    {
        let mut backoff = Backoff::new();
        let mut attempt = 0;
        loop {
            let request = build(&self.client, attempt);
            let (failure, retry_after) = match self.call(request).await {
                Ok(answer) if !is_transient(answer.status) => return Ok(answer),
                Ok(answer) => (complaint(answer.status, &answer.body), answer.retry_after),
                Err(e) => (format!("{e:#}"), None),
            };
            attempt += 1;
            if attempt >= MAX_ATTEMPTS {
                bail!("{failure} (after {MAX_ATTEMPTS} attempts)");
            }
            tokio::time::sleep(retry_after.unwrap_or_else(|| backoff.failed())).await;
        }
    }

    /// Asks after a statement that answered `202` until it has finished.
    ///
    /// A transient answer is one more reason to ask again rather than an error:
    /// the statement is running on the server whatever this request's fate was,
    /// and a `429` is documented as one of the things "still running" looks like.
    async fn finished(&mut self, handle: &str) -> Result<Vec<u8>> {
        let deadline = Instant::now() + Duration::from_secs(self.timeout_secs) + STATUS_GRACE;
        let url = format!("{}/api/v2/statements/{handle}", self.url);
        let mut wait = FIRST_STATUS_WAIT;
        loop {
            tokio::time::sleep(wait).await;
            wait = (wait * 2).min(MAX_STATUS_WAIT);
            let request = self.client.get(&url);
            let last = match self.call(request).await {
                Ok(answer) if answer.status == StatusCode::OK => return Ok(answer.body),
                Ok(answer)
                    if answer.status == StatusCode::ACCEPTED || is_transient(answer.status) =>
                {
                    complaint(answer.status, &answer.body)
                }
                Ok(answer) => {
                    return Err(anyhow!(
                        "{} refused the statement ({})",
                        self.describe(),
                        complaint(answer.status, &answer.body)
                    ));
                }
                Err(e) => format!("{e:#}"),
            };
            if Instant::now() >= deadline {
                bail!(
                    "{} had not finished a statement after its {}s timeout; the last answer was {last}",
                    self.describe(),
                    self.timeout_secs
                );
            }
        }
    }

    /// Runs one statement and reads its whole result. `texts` are the values of
    /// its `?`s, in order, each of them text the statement casts for itself.
    pub async fn execute(&mut self, statement: &str, texts: &[&str]) -> Result<ResultSet> {
        let url = format!("{}/api/v2/statements", self.url);
        let body = self.body(statement, texts);
        let request_id = Uuid::new_v4().to_string();
        let answer = self
            .settled(|client, attempt| {
                let request = client
                    .post(&url)
                    .query(&[("requestId", request_id.as_str())])
                    .json(&body);
                // only on a repeat: the docs say it costs something, and the
                // first attempt has nothing to be repeating
                if attempt > 0 {
                    request.query(&[("retry", "true")])
                } else {
                    request
                }
            })
            .await
            .with_context(|| format!("{} did not take the statement", self.describe()))?;

        let first = match answer.status {
            StatusCode::OK => answer.body,
            StatusCode::ACCEPTED => {
                let reply: Reply = serde_json::from_slice(&answer.body)
                    .context("a statement was accepted with an answer that could not be read")?;
                let handle = reply.statement_handle.ok_or_else(|| {
                    anyhow!("a statement was accepted with no handle to ask after")
                })?;
                self.finished(&handle).await?
            }
            status => {
                return Err(anyhow!(
                    "{} refused the statement ({})",
                    self.describe(),
                    complaint(status, &answer.body)
                ));
            }
        };
        self.read_result(&first).await
    }

    /// A finished statement's first answer, plus any partitions it mentions.
    async fn read_result(&mut self, first: &[u8]) -> Result<ResultSet> {
        let reply: Reply = serde_json::from_slice(first)
            .with_context(|| format!("{} sent a result that could not be read", self.describe()))?;
        if reply.code.as_deref() == Some(RESULT_TOO_LARGE) {
            bail!(
                "{} says the result is too large to come back complete; read fewer rows",
                self.describe()
            );
        }
        let metadata = reply.result_set_meta_data.unwrap_or_default();
        let mut rows = reply.data;
        if metadata.partition_info.len() > 1 {
            let handle = reply.statement_handle.ok_or_else(|| {
                anyhow!("a result in partitions came with no handle to read them")
            })?;
            let url = format!("{}/api/v2/statements/{handle}", self.url);
            for partition in 1..metadata.partition_info.len() {
                let partition = partition.to_string();
                let answer = self
                    .settled(|client, _| {
                        client.get(&url).query(&[("partition", partition.as_str())])
                    })
                    .await
                    .with_context(|| format!("failed to read partition {partition}"))?;
                if answer.status != StatusCode::OK {
                    bail!(
                        "{} refused partition {partition} ({})",
                        self.describe(),
                        complaint(answer.status, &answer.body)
                    );
                }
                rows.extend(partition_rows(&answer.body)?);
            }
        }
        Ok(ResultSet {
            columns: metadata.row_type,
            rows,
        })
    }
}

/// A Snowflake that answers what it is told to and writes down what it was
/// asked, on a real socket. Shared by this module's tests and both components'.
#[cfg(test)]
pub mod fake {
    use std::collections::{HashMap, VecDeque};
    use std::fmt::Display;
    use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

    use axum::Router;
    use axum::body::Bytes;
    use axum::extract::State;
    use axum::http::{HeaderMap, Method, StatusCode, Uri};
    use axum::response::IntoResponse;
    use kayak_core::connections::{
        ConnectionKind, Connections, SnowflakeAuth, SnowflakeConnection,
    };
    use serde_json::{Value, json};

    use crate::BuildCtx;

    /// One request as it arrived.
    #[derive(Debug, Clone)]
    pub struct Seen {
        pub method: String,
        pub uri: String,
        pub authorization: Option<String>,
        pub token_type: Option<String>,
        pub user_agent: Option<String>,
        pub body: Value,
    }

    #[derive(Default)]
    pub struct Fake {
        pub requests: Mutex<Vec<Seen>>,
        pub replies: Mutex<VecDeque<(u16, Vec<u8>)>>,
    }

    pub fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
        mutex.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub fn ok<T, E: Display>(result: Result<T, E>) -> T {
        match result {
            Ok(value) => value,
            Err(e) => panic!("{e}"),
        }
    }

    impl Fake {
        pub fn reply(&self, status: u16, body: &Value) {
            lock(&self.replies).push_back((status, body.to_string().into_bytes()));
        }

        pub fn reply_bytes(&self, status: u16, body: Vec<u8>) {
            lock(&self.replies).push_back((status, body));
        }

        pub fn seen(&self) -> Vec<Seen> {
            lock(&self.requests).clone()
        }

        /// The statements that were submitted, in order.
        pub fn statements(&self) -> Vec<String> {
            self.seen()
                .iter()
                .filter_map(|seen| seen.body["statement"].as_str().map(ToString::to_string))
                .collect()
        }
    }

    async fn answer(
        State(fake): State<Arc<Fake>>,
        method: Method,
        uri: Uri,
        headers: HeaderMap,
        body: Bytes,
    ) -> impl IntoResponse {
        let header = |name: &str| {
            headers
                .get(name)
                .and_then(|value| value.to_str().ok())
                .map(ToString::to_string)
        };
        lock(&fake.requests).push(Seen {
            method: method.to_string(),
            uri: uri.to_string(),
            authorization: header("authorization"),
            token_type: header("x-snowflake-authorization-token-type"),
            user_agent: header("user-agent"),
            body: serde_json::from_slice(&body).unwrap_or(Value::Null),
        });
        let (status, reply) = lock(&fake.replies)
            .pop_front()
            .unwrap_or((200, json!({"data": []}).to_string().into_bytes()));
        (
            StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR),
            reply,
        )
    }

    pub async fn serve() -> (Arc<Fake>, String) {
        let fake = Arc::new(Fake::default());
        let app = Router::new().fallback(answer).with_state(Arc::clone(&fake));
        let listener = ok(tokio::net::TcpListener::bind("127.0.0.1:0").await);
        let addr = ok(listener.local_addr());
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (fake, format!("http://{addr}"))
    }

    /// A connection called `warehouse` reaching `url`, signed in with a token.
    #[must_use]
    pub fn connection(url: &str) -> SnowflakeConnection {
        SnowflakeConnection {
            account: "myorg-my_account".into(),
            user: "kayak".into(),
            auth: SnowflakeAuth::Token {
                token: "sf-token".into(),
            },
            warehouse: "COMPUTE_WH".into(),
            database: "ANALYTICS".into(),
            schema: "PUBLIC".into(),
            role: None,
            url: Some(url.to_string()),
            allow_http: Some(true),
            statement_timeout_seconds: None,
        }
    }

    /// Runs `body` with a build context whose connection `warehouse` is this.
    pub fn with_ctx<T>(
        connection: SnowflakeConnection,
        body: impl FnOnce(&mut BuildCtx) -> T,
    ) -> T {
        let mut pipelines = HashMap::new();
        let (events, _rx) = tokio::sync::broadcast::channel(4);
        let connections: Connections = [(
            "warehouse".to_string(),
            ConnectionKind::Snowflake(connection),
        )]
        .into_iter()
        .collect();
        let mut ctx = BuildCtx::new(&mut pipelines, "p".to_string(), events)
            .with_connections(Arc::new(connections));
        body(&mut ctx)
    }

    /// A finished statement's answer, every column of it text.
    #[must_use]
    pub fn result(columns: &[(&str, &str)], data: &Value) -> Value {
        let row_type: Vec<Value> = columns
            .iter()
            .map(|(name, kind)| json!({"name": name, "type": kind, "nullable": true}))
            .collect();
        json!({
            "statementHandle": "01b2",
            "resultSetMetaData": {
                "numRows": data.as_array().map_or(0, Vec::len),
                "format": "jsonv2",
                "rowType": row_type,
                "partitionInfo": [{"rowCount": 0}]
            },
            "data": data
        })
    }
}

#[cfg(test)]
mod tests {
    use super::fake::{self, ok};
    use super::*;
    use flate2::Compression;
    use flate2::write::GzEncoder;
    use jsonwebtoken::{DecodingKey, Validation, decode};
    use std::io::Write;

    /// A throwaway key, generated for this test and registered nowhere.
    const PRIVATE_KEY: &str = "-----BEGIN PRIVATE KEY-----
MIIEvQIBADANBgkqhkiG9w0BAQEFAASCBKcwggSjAgEAAoIBAQChkzrggnK1qqiq
Ze5CncACnoFjYarj04X2xUs6sOwcEqQ5vXMhv2tF4dZff/dABAPHP9O3kht9CQ9Z
7qbx7D7Qh1HaLoE5wWRw1xPpD9QOgGeRlbJUjzjOkVx2OKamCtjlGsRcrGZIe6fM
9cT9axYwBFVRULctRaqfbQj4VumZLjovTFvrCLc7qybMTvcdULyR/7160kt/kNCg
GjdfVkZal7d0Kbh4L/BkZsx11L6edWUN1S2TecCubhKmLRs4vqD9qVfh0zl5tNww
mAV502yWJEuD9hdFa43P5Y4c5zH5T1PYuGXjXWqCWL5f96SP3wcw4or0T38RBbdX
Jo1Baef7AgMBAAECggEABeyaGxpA53f6CDQTyqFj+r9FnqzWBpyTr3yu97vhgM6r
SEdcfdLkitvLr85ju+xtUQnFWh91CVc4M2AddJTMU0Fcs440oOHFVxKAMzi2hJco
H6haMSgUwGoyktyqjyL/dXFA+R7n/yyy2dy6m+2oaFP4WKRPDNvXV/zGTbNm/gvy
HY95PEvw+cGMSDNEEQSMcGDE+JVnX1n9JmGNEKN+/TEObfbP5A8+54a6H50EDnbP
HS/BlqdDxigx1jiijDAByIbHThyC15VgO4HXxfuuUPAdEHcdSelxg37Rn4BrXdnq
mxwbbKnGtzGQBRq9tKQwXApqKrr2c/ir3Y0yTqvevQKBgQDPWD9yEPoYL5Bvm8NZ
mOIY7Z/GXhqcVQO1lBN9JTNYU0kqCnfhwdBGeUwGb3ihxSQGDv7tSez6D7tD2fPX
A6JB5ylJE8A3zlfOSrJGxI+0wr/QOXZ0GeADQrToM3OUgdB0Y8X1T3Z6fNsRnH9X
xdK4XNw+hoen54aOp5WgTwV3tQKBgQDHfXrPLt7YrhJv7ps5V+HJGOivTIYpWoBQ
tSYB5lDTyG8XlnwthHJxC+pd2AB3vnSdRCV/FQaK2ASlVan9BNge2VwYoT3/rTLf
spMYrzPoMrtfj0EiBaQR9xrIpN4WkcAPSGLqFrZ6BLEdadhqZqlIW7vqEaADEuLR
KRfnZz5O7wKBgGXkihk1+pEUHriaJR/Mn598dr321MDqjk4XHJStSVQoaZX9WvhY
VE+toAyxjuiAhrlreR+G6qvgOziWzNtm77/PJ1zAFf/WqRO5ZW6dfblo0vYypuFb
LdisWVaaojvip1vrBAcPK7v5y3YWIAY0Mt9i0uDvH48FmfxUopULADbZAoGBAIkQ
sSHLPXg5WheoJOToegz23WEcvg2JziIl5OBnQ9/Plmzpi5amqw77NeOWtvOPW4Z2
vSCs7JrXQ5Hp4usiue+wPrP5jaSvdwX2J1VNg+ele9JzljfvEkT5uGBlzuZI7GJT
xQq74lLCCIOCJx0sPmGmDd4MkBmsj0VQ68ilykznAoGAdNrQyUoyrRJrUPttJj/3
VICQM06l8w8LfNDqNw4qKrm1FC+brr02Xk1NrQCSvzxbsFrS9oHO0R+SocT56/xN
M0SLP5BdD3l34nkxEgK0C2xHp45QZv1YCqF+/z/PNqHGZMbDEQvfVYZNFFeMZwjG
yoHVDOFWM0SSIhzey+pUUyA=
-----END PRIVATE KEY-----
";

    const PUBLIC_KEY: &str = "-----BEGIN PUBLIC KEY-----
MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAoZM64IJytaqoqmXuQp3A
Ap6BY2Gq49OF9sVLOrDsHBKkOb1zIb9rReHWX3/3QAQDxz/Tt5IbfQkPWe6m8ew+
0IdR2i6BOcFkcNcT6Q/UDoBnkZWyVI84zpFcdjimpgrY5RrEXKxmSHunzPXE/WsW
MARVUVC3LUWqn20I+FbpmS46L0xb6wi3O6smzE73HVC8kf+9etJLf5DQoBo3X1ZG
Wpe3dCm4eC/wZGbMddS+nnVlDdUtk3nArm4Spi0bOL6g/alX4dM5ebTcMJgFedNs
liRLg/YXRWuNz+WOHOcx+U9T2Lhl411qgli+X/ekj98HMOKK9E9/EQW3VyaNQWnn
+wIDAQAB
-----END PUBLIC KEY-----
";

    /// What `openssl rsa -pubout -outform DER | openssl dgst -sha256 -binary |
    /// openssl enc -base64` says about that key, and so what `DESCRIBE USER`
    /// would report for it.
    const FINGERPRINT: &str = "SHA256:FXSg9jjOYKDsYNRbpMg4HFo/sj+QZmQ9ONk+pSVxdGw=";

    #[derive(Deserialize)]
    struct Read {
        iss: String,
        sub: String,
        iat: i64,
        exp: i64,
    }

    fn session(url: &str) -> Session {
        ok(fake::with_ctx(fake::connection(url), |ctx| {
            Session::build(ctx, "warehouse")
        }))
    }

    #[test]
    fn the_fingerprint_is_what_openssl_says_it_is() -> Result<()> {
        assert_eq!(fingerprint(&pkcs8_der(PRIVATE_KEY)?)?, FINGERPRINT);
        Ok(())
    }

    /// The token has to be one the server can check against the registered
    /// public key, and the account and user upper case in both claims.
    #[test]
    fn a_token_is_signed_and_names_the_account_user_and_key() -> Result<()> {
        let mut auth = Auth::KeyPair(Box::new(key_pair(
            "myorg-my_account",
            "kayak",
            PRIVATE_KEY,
        )?));
        let token = auth.bearer()?;
        let mut validation = Validation::new(Algorithm::RS256);
        validation.set_required_spec_claims(&["exp", "iss", "sub"]);
        let claims = decode::<Read>(
            token.as_str(),
            &DecodingKey::from_rsa_pem(PUBLIC_KEY.as_bytes())?,
            &validation,
        )?
        .claims;
        assert_eq!(claims.sub, "MYORG-MY_ACCOUNT.KAYAK");
        assert_eq!(claims.iss, format!("MYORG-MY_ACCOUNT.KAYAK.{FINGERPRINT}"));
        // the API takes an hour at the most
        assert!(claims.exp - claims.iat <= 3600);
        assert!(claims.exp > claims.iat);
        Ok(())
    }

    #[test]
    fn a_token_is_reused_until_a_refusal_says_otherwise() -> Result<()> {
        let mut auth = Auth::KeyPair(Box::new(key_pair("a", "b", PRIVATE_KEY)?));
        let first = auth.bearer()?;
        assert_eq!(auth.bearer()?, first);
        auth.forget();
        let Auth::KeyPair(pair) = &auth else {
            panic!("still a key pair");
        };
        assert!(pair.issued.is_none());
        Ok(())
    }

    /// An environment variable keeps its line breaks as `\n`, or as nothing.
    #[test]
    fn a_key_survives_what_a_secret_store_does_to_its_line_breaks() -> Result<()> {
        for pem in [
            PRIVATE_KEY.replace('\n', "\\n"),
            PRIVATE_KEY
                .replace("-----\n", "----- ")
                .replace("\n-----", " -----")
                .replace('\n', ""),
        ] {
            let mut auth = Auth::KeyPair(Box::new(key_pair("a", "b", &pem)?));
            assert_ne!(auth.bearer()?, "");
        }
        Ok(())
    }

    #[test]
    fn a_key_that_cannot_be_used_says_why_and_does_not_print_itself() {
        for (pem, expected) in [
            (
                "-----BEGIN ENCRYPTED PRIVATE KEY-----\nabc\n-----END ENCRYPTED PRIVATE KEY-----",
                "encrypted",
            ),
            (
                "-----BEGIN RSA PRIVATE KEY-----\nabc\n-----END RSA PRIVATE KEY-----",
                "PKCS#8",
            ),
            ("hunter2", "PKCS#8"),
            (
                "-----BEGIN PRIVATE KEY-----\n!!!\n-----END PRIVATE KEY-----",
                "base64",
            ),
            (
                "-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----",
                "RSA",
            ),
        ] {
            let Err(error) = key_pair("a", "b", pem) else {
                panic!("'{pem}' was accepted");
            };
            let text = format!("{error:#}");
            assert!(text.contains(expected), "{text}");
            assert!(!text.contains("hunter2") && !text.contains("!!!"), "{text}");
        }
    }

    #[test]
    fn the_host_comes_from_the_account() -> Result<()> {
        let mut server = fake::connection("x");
        server.url = None;
        assert_eq!(
            endpoint(&server, "c")?,
            "https://myorg-my-account.snowflakecomputing.com"
        );
        Ok(())
    }

    /// The account is put into the host a token is sent to.
    #[test]
    fn an_account_that_is_not_a_name_is_refused() {
        for account in [
            "",
            "evil.example/",
            "a.b",
            "xy12345.us-east-2.aws",
            "a b",
            "a@b",
        ] {
            let mut server = fake::connection("https://example.com");
            account.clone_into(&mut server.account);
            assert!(endpoint(&server, "c").is_err(), "'{account}' was accepted");
        }
    }

    #[test]
    fn a_plaintext_url_needs_the_connection_to_allow_it() {
        let mut server = fake::connection("http://localhost:1/");
        assert_eq!(ok(endpoint(&server, "c")), "http://localhost:1");
        server.allow_http = None;
        assert!(endpoint(&server, "c").is_err());
        server.url = Some("https://xy12345.us-east-2.aws.snowflakecomputing.com".into());
        assert!(endpoint(&server, "c").is_ok());
        server.url = Some("xy12345.snowflakecomputing.com".into());
        assert!(endpoint(&server, "c").is_err());
    }

    #[test]
    fn a_zero_timeout_is_refused() {
        let mut server = fake::connection("http://localhost:1");
        server.statement_timeout_seconds = Some(0);
        assert!(fake::with_ctx(server, |ctx| Session::build(ctx, "warehouse")).is_err());
    }

    #[test]
    fn asking_for_the_wrong_kind_of_connection_says_so() {
        let mut pipelines = std::collections::HashMap::new();
        let (events, _rx) = tokio::sync::broadcast::channel(4);
        let ctx = BuildCtx::new(&mut pipelines, "p".to_string(), events);
        assert!(Session::build(&ctx, "nothing").is_err());
    }

    /// The whole request, since this is the part nothing downstream can check.
    #[tokio::test]
    async fn a_statement_says_where_it_runs_and_how_to_render() -> Result<()> {
        let (fake, url) = fake::serve().await;
        fake.reply(
            200,
            &fake::result(&[("A", "text")], &json!([["x"], [null]])),
        );
        let mut session = session(&url);
        let result = session.execute("SELECT ?", &["hello"]).await?;
        assert_eq!(result.rows, vec![vec![Some("x".to_string())], vec![None]]);
        assert_eq!(result.columns[0].name, "A");

        let seen = fake.seen();
        assert_eq!(seen.len(), 1);
        assert_eq!(seen[0].method, "POST");
        assert!(
            seen[0].uri.starts_with("/api/v2/statements?requestId="),
            "{}",
            seen[0].uri
        );
        assert!(!seen[0].uri.contains("retry"), "{}", seen[0].uri);
        assert_eq!(seen[0].authorization.as_deref(), Some("Bearer sf-token"));
        // a `token` is either kind, and the API works out which
        assert_eq!(seen[0].token_type, None);
        assert!(
            seen[0]
                .user_agent
                .as_deref()
                .is_some_and(|ua| ua.starts_with("kayak/"))
        );
        let body = &seen[0].body;
        assert_eq!(body["statement"], "SELECT ?");
        assert_eq!(body["warehouse"], "COMPUTE_WH");
        assert_eq!(body["database"], "ANALYTICS");
        assert_eq!(body["schema"], "PUBLIC");
        assert_eq!(body["timeout"], DEFAULT_STATEMENT_TIMEOUT_SECS);
        assert!(body.get("role").is_none(), "{body}");
        assert_eq!(
            body["bindings"],
            json!({"1": {"type": "TEXT", "value": "hello"}})
        );
        assert_eq!(
            body["parameters"]["timestamp_tz_output_format"],
            ZONED_FORMAT
        );
        assert_eq!(
            body["parameters"]["timestamp_ltz_output_format"],
            ZONED_FORMAT
        );
        assert_eq!(
            body["parameters"]["timestamp_ntz_output_format"],
            NAIVE_FORMAT
        );
        assert_eq!(body["parameters"]["date_output_format"], DATE_FORMAT);
        assert_eq!(body["parameters"]["time_output_format"], TIME_FORMAT);
        Ok(())
    }

    #[tokio::test]
    async fn a_role_and_a_timeout_are_sent_when_the_connection_has_them() -> Result<()> {
        let (fake, url) = fake::serve().await;
        let mut server = fake::connection(&url);
        server.role = Some("LOADER".into());
        server.statement_timeout_seconds = Some(30);
        let mut session = fake::with_ctx(server, |ctx| Session::build(ctx, "warehouse"))?;
        session.execute("SELECT 1", &[]).await?;
        let seen = fake.seen();
        assert_eq!(seen[0].body["role"], "LOADER");
        assert_eq!(seen[0].body["timeout"], 30);
        assert!(seen[0].body.get("bindings").is_none());
        Ok(())
    }

    #[tokio::test]
    async fn a_key_pair_signs_in_as_a_key_pair() -> Result<()> {
        let (fake, url) = fake::serve().await;
        let mut server = fake::connection(&url);
        server.auth = SnowflakeAuth::KeyPair {
            private_key: PRIVATE_KEY.into(),
        };
        let mut session = fake::with_ctx(server, |ctx| Session::build(ctx, "warehouse"))?;
        session.execute("SELECT 1", &[]).await?;
        let seen = fake.seen();
        assert_eq!(seen[0].token_type.as_deref(), Some("KEYPAIR_JWT"));
        let authorization = seen[0].authorization.clone().unwrap_or_default();
        assert!(authorization.starts_with("Bearer ey"), "{authorization}");
        Ok(())
    }

    /// What makes retrying an insert safe: the same id, and `retry=true` so the
    /// server knows to look it up rather than run the statement again.
    #[tokio::test]
    async fn a_transient_answer_is_retried_under_the_same_request_id() -> Result<()> {
        let (fake, url) = fake::serve().await;
        fake.reply(503, &json!({"message": "busy"}));
        fake.reply(429, &json!({"message": "too many"}));
        fake.reply(200, &fake::result(&[("A", "text")], &json!([["x"]])));
        let result = session(&url).execute("SELECT 1", &[]).await?;
        assert_eq!(result.rows.len(), 1);

        let seen = fake.seen();
        assert_eq!(seen.len(), 3);
        let id_of = |uri: &str| {
            uri.split(['?', '&'])
                .find_map(|part| part.strip_prefix("requestId="))
                .map(ToString::to_string)
        };
        let first = id_of(&seen[0].uri);
        assert!(first.is_some());
        assert!(!seen[0].uri.contains("retry=true"), "{}", seen[0].uri);
        for retry in &seen[1..] {
            assert_eq!(id_of(&retry.uri), first);
            assert!(retry.uri.contains("retry=true"), "{}", retry.uri);
        }
        Ok(())
    }

    #[tokio::test]
    async fn a_new_statement_gets_a_new_request_id() -> Result<()> {
        let (fake, url) = fake::serve().await;
        let mut session = session(&url);
        session.execute("SELECT 1", &[]).await?;
        session.execute("SELECT 1", &[]).await?;
        let seen = fake.seen();
        assert_ne!(seen[0].uri, seen[1].uri);
        Ok(())
    }

    #[tokio::test]
    async fn a_failure_that_will_not_go_away_is_given_up_on() {
        let (fake, url) = fake::serve().await;
        for _ in 0..MAX_ATTEMPTS {
            fake.reply(503, &json!({"message": "still down"}));
        }
        let Err(error) = session(&url).execute("SELECT 1", &[]).await else {
            panic!("a server that kept failing was taken as an answer");
        };
        let text = format!("{error:#}");
        assert!(text.contains("still down"), "{text}");
        assert_eq!(
            fake.seen().len(),
            usize::try_from(MAX_ATTEMPTS).unwrap_or(0)
        );
    }

    /// Each statement has its own request id, and an error's text is what
    /// `history` counts it under, so the id can't be in it: one outage would
    /// read as a different failure per batch.
    #[tokio::test]
    async fn an_unreachable_server_reads_the_same_every_time() {
        let mut session = session("http://127.0.0.1:1");
        let mut texts = Vec::new();
        for _ in 0..2 {
            let Err(error) = session.execute("SELECT 1", &[]).await else {
                panic!("nothing is listening there");
            };
            texts.push(format!("{error:#}"));
        }
        assert_eq!(texts[0], texts[1]);
        assert!(!texts[0].contains("requestId"), "{}", texts[0]);
        assert!(!texts[0].contains("sf-token"), "{}", texts[0]);
    }

    /// A statement the server refused is the same refusal next time, and the
    /// handle stays out of the text because the text is a `history` key.
    #[tokio::test]
    async fn a_refused_statement_quotes_the_server_and_is_not_retried() {
        let (fake, url) = fake::serve().await;
        fake.reply(
            422,
            &json!({
                "code": "000904",
                "message": "SQL compilation error: invalid identifier 'AFAF'",
                "sqlState": "42000",
                "statementHandle": "019c0728-0000-df4f"
            }),
        );
        let Err(error) = session(&url).execute("SELECT afaf", &[]).await else {
            panic!("a refused statement was taken as an answer");
        };
        let text = format!("{error:#}");
        assert!(text.contains("invalid identifier 'AFAF'"), "{text}");
        assert!(text.contains("000904"), "{text}");
        assert!(!text.contains("019c0728"), "{text}");
        assert!(!text.contains("sf-token"), "{text}");
        assert_eq!(fake.seen().len(), 1);
    }

    #[tokio::test]
    async fn a_slow_statement_is_asked_after_until_it_finishes() -> Result<()> {
        let (fake, url) = fake::serve().await;
        fake.reply(
            202,
            &json!({"code": "333334", "message": "in progress", "statementHandle": "01ab"}),
        );
        fake.reply(202, &json!({"code": "333334", "statementHandle": "01ab"}));
        // documented as one of the things "still running" looks like
        fake.reply(429, &json!({"message": "slow down"}));
        fake.reply(200, &fake::result(&[("A", "text")], &json!([["done"]])));
        let result = session(&url).execute("SELECT 1", &[]).await?;
        assert_eq!(result.rows, vec![vec![Some("done".to_string())]]);

        let seen = fake.seen();
        assert_eq!(seen.len(), 4);
        for status in &seen[1..] {
            assert_eq!(status.method, "GET");
            assert_eq!(status.uri, "/api/v2/statements/01ab");
            assert_eq!(status.authorization.as_deref(), Some("Bearer sf-token"));
        }
        Ok(())
    }

    #[tokio::test]
    async fn a_slow_statement_that_fails_reports_the_failure() {
        let (fake, url) = fake::serve().await;
        fake.reply(202, &json!({"statementHandle": "01ab"}));
        fake.reply(
            422,
            &json!({"message": "division by zero", "code": "100051"}),
        );
        let Err(error) = session(&url).execute("SELECT 1/0", &[]).await else {
            panic!("a failed statement was taken as an answer");
        };
        assert!(
            format!("{error:#}").contains("division by zero"),
            "{error:#}"
        );
    }

    #[tokio::test]
    async fn a_statement_accepted_with_no_handle_is_an_error() {
        let (fake, url) = fake::serve().await;
        fake.reply(202, &json!({"message": "in progress"}));
        assert!(session(&url).execute("SELECT 1", &[]).await.is_err());
    }

    fn gzipped(text: &str) -> Vec<u8> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        ok(encoder.write_all(text.as_bytes()));
        ok(encoder.finish())
    }

    /// The docs describe a later partition as gzipped with no metadata and show
    /// no body, so both shapes it could have are read.
    #[tokio::test]
    async fn a_result_in_partitions_is_read_whole_and_in_order() -> Result<()> {
        let (fake, url) = fake::serve().await;
        let mut first = fake::result(&[("A", "text")], &json!([["one"]]));
        first["resultSetMetaData"]["partitionInfo"] = json!([{}, {}, {}, {}]);
        fake.reply(200, &first);
        fake.reply_bytes(200, gzipped(r#"{"data": [["two"], [null]]}"#));
        fake.reply_bytes(200, gzipped(r#"[["three"]]"#));
        fake.reply(200, &json!({"data": [["four"]]}));
        let result = session(&url).execute("SELECT 1", &[]).await?;
        let text = |t: &str| vec![Some(t.to_string())];
        assert_eq!(
            result.rows,
            vec![
                text("one"),
                text("two"),
                vec![None],
                text("three"),
                text("four")
            ]
        );
        assert_eq!(result.columns.len(), 1);

        let seen = fake.seen();
        assert_eq!(seen[1].uri, "/api/v2/statements/01b2?partition=1");
        assert_eq!(seen[2].uri, "/api/v2/statements/01b2?partition=2");
        assert_eq!(seen[3].uri, "/api/v2/statements/01b2?partition=3");
        Ok(())
    }

    #[tokio::test]
    async fn a_partition_that_cannot_be_read_fails_the_statement() {
        let (fake, url) = fake::serve().await;
        let mut first = fake::result(&[("A", "text")], &json!([["one"]]));
        first["resultSetMetaData"]["partitionInfo"] = json!([{}, {}]);
        fake.reply(200, &first);
        fake.reply(404, &json!({"message": "gone"}));
        assert!(session(&url).execute("SELECT 1", &[]).await.is_err());
    }

    /// A `200` that says it is not the whole result must not look like one.
    #[tokio::test]
    async fn a_result_the_server_says_is_incomplete_is_an_error() {
        let (fake, url) = fake::serve().await;
        let mut reply = fake::result(&[("A", "text")], &json!([["partial"]]));
        reply["code"] = json!(RESULT_TOO_LARGE);
        fake.reply(200, &reply);
        let Err(error) = session(&url).execute("SELECT 1", &[]).await else {
            panic!("half a result was taken as a whole one");
        };
        assert!(format!("{error:#}").contains("too large"), "{error:#}");
    }

    #[test]
    fn an_unquoted_name_resolves_to_upper_case() {
        assert_eq!(fold("analytics.readings"), "ANALYTICS.READINGS");
        assert_eq!(fold("Already_MIXED"), "ALREADY_MIXED");
    }
}
