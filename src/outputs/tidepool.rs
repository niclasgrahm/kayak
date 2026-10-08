//! Writes batches into a Tidepool table — `POST /v1/tables/{table}/rows`.
//!
//! The third consumer of [`crate::outputs::columns`], and the first whose
//! table is not this output's to create: Tidepool declares its tables in its
//! own project, with their types, and refuses anything that doesn't fit. So
//! where the database outputs render DDL, this one *reads* the table
//! (`GET /v1/tables/{table}`) on start and checks the mapping against it. A
//! column Tidepool doesn't have, a type it can't take, a null where it wants a
//! value: all of it fails the start, with the table's own columns named,
//! rather than every batch an hour later.
//!
//! The wire types are written out here by hand, as the `indu` output's are:
//! Tidepool's `tidepool-protocol` crate is where they're defined, and a few
//! fields of three responses are not worth a dependency on another repository.
//!
//! # A batch, and what fails it
//!
//! One batch is one request, as NDJSON: one row object per line. Tidepool
//! checks every value against its column and writes a batch whole or not at
//! all, so a `400` names every problem by row and column and nothing was
//! written; the first few are quoted in the error, which is what the card
//! shows. Such a batch is not retried — sending it again would be refused the
//! same way.
//!
//! What *is* retried, inside `emit`, is a server that is busy (`503`, after
//! its `Retry-After`) or can't be reached, for up to `retry_seconds`. That is
//! backpressure rather than failure, and the run loop drops a batch whose
//! `emit` fails, so giving up at once would lose it. Every attempt carries the
//! same `Idempotency-Key` (`{pipeline}:{run}:{batch}`), which Tidepool answers
//! with the first response for 24 hours: a request that timed out after it
//! landed is not written twice.
//!
//! # Schema drift
//!
//! Tidepool's config changes live. When a batch is refused in a way a changed
//! table explains (a `400`, a `404`, a `409` for a table that was removed), the
//! next batch first reads the table again and re-checks the mapping, so the
//! card then says what changed rather than repeating the row errors.

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use kayak_core::columns::{ColumnType, ExtraFieldPolicy, MissingColumnPolicy};
use kayak_core::config::TidepoolOutputConfig;
use rand::RngExt;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderValue, RETRY_AFTER};
use reqwest::{Client, StatusCode, Url};
use serde::Deserialize;
use tracing::{debug, warn};

use crate::{
    BuildCtx,
    backoff::{Backoff, Gate},
    inputs::MessageBatch,
    outbound::{self, DEFAULT_TIMEOUT},
    outputs::{
        BuildOutput, OutputDestination,
        columns::{ColumnPlan, Identifier, Row},
    },
};

/// How long one batch is retried while the server is busy or away, unless
/// the config says otherwise.
const DEFAULT_RETRY: Duration = Duration::from_secs(30);

/// How many of Tidepool's row problems are quoted in an error.
const QUOTED_PROBLEMS: usize = 3;

const IDEMPOTENCY_HEADER: &str = "idempotency-key";

impl BuildOutput for TidepoolOutputConfig {
    fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn OutputDestination>> {
        // a table name goes into a url path; Tidepool's are identifiers, and
        // anything else is a typo rather than something to escape
        let table = Identifier::parse(&self.table, "table name")
            .context("the tidepool output cannot be built")?;
        let layout = Layout::build(&self)?;
        let connection = ctx
            .tidepool_connection(&self.connection)
            .context("the tidepool output cannot be built")?;
        let base = outbound::parse_url("tidepool connection", &connection.url)?;

        let credential = match &connection.token {
            None => None,
            Some(token) => {
                // the token goes with every batch, so plaintext is a decision
                // the connection has to have written down — clickhouse's rule
                if base.scheme() == "http" && !connection.allows_http() {
                    bail!(
                        "connection '{}' reaches tidepool over plaintext http with a token, which \
                         would send the token in the clear; set \"allow_http\": true on the \
                         connection if that is what you want",
                        self.connection
                    );
                }
                let resolved = ctx.resolve(token)?;
                anyhow::ensure!(
                    !resolved.expose().is_empty(),
                    "the tidepool connection '{}' resolved to an empty token; check that \
                     '{resolved}' is set in the secret store",
                    self.connection
                );
                let mut value = HeaderValue::try_from(format!("Bearer {}", resolved.expose()))
                    .context("the tidepool token cannot be sent as a header")?;
                value.set_sensitive(true);
                Some(value)
            }
        };

        let table_url = base
            .join(&format!("v1/tables/{}", table.as_str()))
            .context("joining the table's path onto the tidepool url")?;
        let rows_url = base
            .join(&format!("v1/tables/{}/rows", table.as_str()))
            .context("joining the rows path onto the tidepool url")?;
        let timeout = self
            .timeout_seconds
            .map_or(DEFAULT_TIMEOUT, Duration::from_secs);
        let client = Client::builder()
            .timeout(timeout)
            .build()
            .context("failed to build the tidepool output's client")?;

        // one run of this output: a restarted pipeline mints new keys, since
        // its batch numbers start over
        let run: u64 = rand::rng().random();
        Ok(Box::new(TidepoolOutput {
            described: format!(
                "tidepool at {} (table '{}')",
                outbound::describe(&base),
                table.as_str()
            ),
            table: table.as_str().to_string(),
            table_url,
            rows_url,
            credential,
            layout,
            client,
            gate: Gate::new(),
            retry: self
                .retry_seconds
                .map_or(DEFAULT_RETRY, Duration::from_secs),
            keys: format!("{}:{run:016x}", ctx.pipeline_id),
            sequence: 0,
            stale: false,
        }))
    }
}

/// What a row is made of.
enum Layout {
    /// Each message is a row as it is.
    Whole,
    /// The mapped columns.
    Mapped(ColumnPlan),
}

impl Layout {
    fn build(config: &TidepoolOutputConfig) -> Result<Self> {
        if config.columns.is_empty() {
            if config.on_extra_fields != ExtraFieldPolicy::Ignore {
                bail!(
                    "the tidepool output for '{}' asks about extra fields but maps no columns; \
                     every field would be extra (Tidepool refuses fields it has no column for \
                     anyway)",
                    config.table
                );
            }
            return Ok(Self::Whole);
        }
        Ok(Self::Mapped(
            ColumnPlan::build(&config.columns, config.on_extra_fields).with_context(|| {
                format!("the tidepool output for '{}' cannot be built", config.table)
            })?,
        ))
    }

    /// The batch as NDJSON: one row object per line, and an empty string when
    /// nothing survived the mapping.
    fn body(&self, batch: &MessageBatch) -> Result<String> {
        let mut body = String::new();
        for message in batch {
            match self {
                Self::Whole => body.push_str(&message.to_string()),
                Self::Mapped(plan) => {
                    let Row::Values(values) = plan.row(message)? else {
                        continue;
                    };
                    body.push('{');
                    for (index, (column, value)) in plan.columns().iter().zip(&values).enumerate() {
                        if index > 0 {
                            body.push(',');
                        }
                        body.push_str(
                            &serde_json::Value::String(column.name.as_str().to_string())
                                .to_string(),
                        );
                        body.push(':');
                        match value {
                            Some(text) => body.push_str(&token(text, column.column_type)),
                            None => body.push_str("null"),
                        }
                    }
                    body.push('}');
                }
            }
            body.push('\n');
        }
        Ok(body)
    }

    /// Whether this layout can write into `table`; what's wrong if not.
    fn check(&self, table: &TableInfo) -> Result<()> {
        let Self::Mapped(plan) = self else {
            // whole messages: Tidepool checks every row, and there is nothing
            // here that knows what they hold
            return Ok(());
        };
        let names = || {
            table
                .columns
                .iter()
                .map(|c| c.name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        };
        let mut problems = Vec::new();
        for column in plan.columns() {
            let name = column.name.as_str();
            let Some(target) = table.columns.iter().find(|c| c.name == name) else {
                problems.push(format!(
                    "`{name}` is not a column of `{}` (its columns: {})",
                    table.name,
                    names()
                ));
                continue;
            };
            if !writes(column.column_type, &target.column_type) {
                problems.push(format!(
                    "`{name}` is {} in Tidepool, which a `{}` column cannot write; map it as {}",
                    target.column_type,
                    column.column_type.as_str(),
                    takes(&target.column_type)
                ));
            }
            if !target.nullable && column.on_missing == MissingColumnPolicy::Null {
                problems.push(format!(
                    "`{name}` needs a value in every row in Tidepool, and this mapping writes null \
                     when the field is missing; set \"nullable\": false (a message without it \
                     then fails) or \"on_missing\": \"skip_row\""
                ));
            }
        }
        for target in table.columns.iter().filter(|c| !c.nullable) {
            if plan.column(&target.name).is_none() {
                problems.push(format!(
                    "`{}` needs a value in every row in Tidepool, and no column writes it",
                    target.name
                ));
            }
        }
        if problems.is_empty() {
            Ok(())
        } else {
            Err(anyhow!(
                "the mapping doesn't fit Tidepool's table `{}`: {}",
                table.name,
                problems.join("; ")
            ))
        }
    }
}

/// The type's name without its parameters: `decimal(14,2)` is a `decimal`.
fn base_type(tidepool: &str) -> String {
    tidepool
        .split('(')
        .next()
        .unwrap_or_default()
        .trim()
        .to_ascii_lowercase()
}

/// Whether a column of a mapping's type can write into a Tidepool column of
/// type `tidepool`. Tidepool checks values and never coerces, so this is the
/// pairs whose values it accepts.
fn writes(kayak: ColumnType, tidepool: &str) -> bool {
    let base = base_type(tidepool);
    match kayak {
        ColumnType::Text | ColumnType::Uuid | ColumnType::Json => base == "string",
        ColumnType::Integer => matches!(base.as_str(), "int32" | "int64" | "float64"),
        ColumnType::Bigint => base == "int64",
        ColumnType::Float => base == "float64",
        ColumnType::Decimal => matches!(base.as_str(), "decimal" | "float64"),
        ColumnType::Boolean => base == "bool",
        // an instant: Tidepool's `timestamp` has no time zone, and reading an
        // instant into one would be a guess about which zone was meant
        ColumnType::Timestamp => base == "timestamptz",
        ColumnType::Date => base == "date",
    }
}

/// The mapping types that can write a Tidepool type, for the error's help.
fn takes(tidepool: &str) -> &'static str {
    match base_type(tidepool).as_str() {
        "string" => "`text`, `uuid` or `json`",
        "int32" => "`integer`",
        "int64" => "`integer` or `bigint`",
        "float64" => "`float`, `integer` or `decimal`",
        "decimal" => "`decimal`",
        "bool" => "`boolean`",
        "date" => "`date`",
        "timestamptz" => "`timestamp`",
        _ => "nothing a mapping has: leave `columns` out and send the messages as they are",
    }
}

/// One value as the JSON token it goes across as — the clickhouse output's
/// rule: the plan's text for numbers and booleans *is* JSON (a decimal keeps
/// its digits that way, and Tidepool reads them from the text), everything
/// else is a string, including a `json` column's JSON text.
fn token(text: &str, column_type: ColumnType) -> String {
    match column_type {
        ColumnType::Integer
        | ColumnType::Bigint
        | ColumnType::Float
        | ColumnType::Decimal
        | ColumnType::Boolean => text.to_string(),
        ColumnType::Text
        | ColumnType::Date
        | ColumnType::Uuid
        | ColumnType::Timestamp
        | ColumnType::Json => serde_json::Value::String(text.to_string()).to_string(),
    }
}

/// `GET /v1/tables/{table}`, the part of it read here.
#[derive(Debug, Deserialize)]
struct TableInfo {
    name: String,
    columns: Vec<ColumnInfo>,
}

#[derive(Debug, Deserialize)]
struct ColumnInfo {
    name: String,
    #[serde(rename = "type")]
    column_type: String,
    nullable: bool,
}

/// A written batch.
#[derive(Debug, Deserialize)]
struct IngestResponse {
    written: u64,
    epoch: u64,
}

/// A refused batch: nothing was written.
#[derive(Debug, Deserialize)]
struct IngestError {
    error: String,
    #[serde(default)]
    errors: Vec<RowError>,
    #[serde(default)]
    more: u64,
}

#[derive(Debug, Deserialize)]
struct RowError {
    row: Option<u64>,
    column: Option<String>,
    message: String,
}

/// Any other error Tidepool answers with.
#[derive(Debug, Deserialize)]
struct ApiError {
    error: String,
}

/// What one attempt came to.
enum Attempt {
    Written(IngestResponse),
    /// Worth trying again with the same key: the server is busy or away.
    Again(Option<Duration>, String),
    /// Refused for good; `drift` when a changed table could explain it.
    Refused {
        error: anyhow::Error,
        drift: bool,
    },
}

pub struct TidepoolOutput {
    described: String,
    table: String,
    table_url: Url,
    rows_url: Url,
    credential: Option<HeaderValue>,
    layout: Layout,
    client: Client,
    /// Paces batches after one has given up, as every output's gate does.
    gate: Gate,
    retry: Duration,
    /// `{pipeline}:{run}`, the start of every idempotency key.
    keys: String,
    sequence: u64,
    /// The table may have changed: read it again before the next batch.
    stale: bool,
}

impl TidepoolOutput {
    fn authorized(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.credential {
            Some(value) => request.header(AUTHORIZATION, value.clone()),
            None => request,
        }
    }

    /// Read the table and check the mapping against it.
    async fn check_table(&self) -> Result<()> {
        let response = self
            .authorized(self.client.get(self.table_url.clone()))
            .send()
            .await
            .with_context(|| format!("failed to reach {}", self.described))?;
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        if status == StatusCode::NOT_FOUND {
            bail!(
                "{} has no table '{}'; tables are declared in Tidepool's project",
                self.described,
                self.table
            );
        }
        if !status.is_success() {
            bail!(
                "{} refused to describe the table ({status}): {}",
                self.described,
                detail(&text)
            );
        }
        let table: TableInfo = serde_json::from_str(&text).with_context(|| {
            format!(
                "{} described the table in a shape this kayak doesn't read",
                self.described
            )
        })?;
        self.layout.check(&table)
    }

    async fn attempt(&self, body: &str, key: &str) -> Attempt {
        let request = self
            .authorized(self.client.post(self.rows_url.clone()))
            .header(CONTENT_TYPE, "application/x-ndjson")
            .header(IDEMPOTENCY_HEADER, key)
            .body(body.to_string());
        let response = match request.send().await {
            Ok(response) => response,
            // the same key makes this safe even when the request did land
            Err(e) => {
                return Attempt::Again(None, format!("failed to reach {}: {e}", self.described));
            }
        };
        let status = response.status();
        let wait = response
            .headers()
            .get(RETRY_AFTER)
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.trim().parse::<u64>().ok())
            .map(Duration::from_secs);
        let text = response.text().await.unwrap_or_default();
        if status.is_success() {
            return match serde_json::from_str::<IngestResponse>(&text) {
                Ok(written) => Attempt::Written(written),
                Err(e) => Attempt::Refused {
                    error: anyhow!(
                        "{} answered {status} with a body this kayak doesn't read: {e}",
                        self.described
                    ),
                    drift: false,
                },
            };
        }
        let why = format!(
            "{} refused the batch ({status}): {}",
            self.described,
            detail(&text)
        );
        match status {
            StatusCode::SERVICE_UNAVAILABLE => Attempt::Again(wait, why),
            // the same key still being worked on: its answer is coming
            StatusCode::CONFLICT if text.contains("still being processed") => {
                Attempt::Again(wait, why)
            }
            StatusCode::BAD_REQUEST
            | StatusCode::NOT_FOUND
            | StatusCode::CONFLICT
            | StatusCode::UNPROCESSABLE_ENTITY => Attempt::Refused {
                error: anyhow!(why),
                drift: true,
            },
            s if s.is_server_error() => Attempt::Again(wait, why),
            _ => Attempt::Refused {
                error: anyhow!(why),
                drift: false,
            },
        }
    }
}

/// What a refusal says: its row problems quoted when it lists them, else the
/// error, else the body cut short.
fn detail(text: &str) -> String {
    if let Ok(refused) = serde_json::from_str::<IngestError>(text) {
        if refused.errors.is_empty() {
            return outbound::truncate(&refused.error);
        }
        let mut quoted: Vec<String> = refused
            .errors
            .iter()
            .take(QUOTED_PROBLEMS)
            .map(|e| match (e.row, &e.column) {
                (Some(row), Some(column)) => format!("row {row}, column {column}: {}", e.message),
                (Some(row), None) => format!("row {row}: {}", e.message),
                (None, Some(column)) => format!("column {column}: {}", e.message),
                (None, None) => e.message.clone(),
            })
            .collect();
        let rest = (refused.errors.len().saturating_sub(QUOTED_PROBLEMS)) as u64 + refused.more;
        if rest > 0 {
            quoted.push(format!("and {rest} more"));
        }
        return outbound::truncate(&quoted.join("; "));
    }
    if let Ok(error) = serde_json::from_str::<ApiError>(text) {
        return outbound::truncate(&error.error);
    }
    outbound::truncate(text.trim())
}

#[async_trait::async_trait]
impl OutputDestination for TidepoolOutput {
    /// Reads the table and checks the mapping, so a config that doesn't fit
    /// fails the start (and is retried there, as any output's init is).
    async fn init(&mut self) -> Result<()> {
        self.check_table().await?;
        self.stale = false;
        Ok(())
    }

    async fn emit(&mut self, message_batch: Arc<MessageBatch>) -> Result<()> {
        let body = self
            .layout
            .body(&message_batch)
            .with_context(|| format!("failed to map a message onto '{}'", self.table))?;
        // every message was skipped by the mapping, or there were none
        if body.is_empty() {
            return Ok(());
        }
        let started = Instant::now();
        if !self.gate.ready(started) {
            bail!("{} is still failing; not retrying yet", self.described);
        }
        if self.stale {
            if let Err(e) = self.check_table().await {
                self.gate.record_failure(started);
                return Err(e);
            }
            self.stale = false;
        }

        self.sequence = self.sequence.wrapping_add(1);
        let key = format!("{}:{}", self.keys, self.sequence);
        let deadline = started + self.retry;
        let mut backoff = Backoff::new();
        loop {
            match self.attempt(&body, &key).await {
                Attempt::Written(written) => {
                    self.gate.record_success();
                    // the epoch the rows are visible from, for whoever is
                    // matching a dashboard's numbers against this pipeline
                    debug!(
                        table = self.table,
                        rows = written.written,
                        epoch = written.epoch,
                        "written to tidepool"
                    );
                    return Ok(());
                }
                Attempt::Again(after, why) => {
                    let wait = after.unwrap_or_else(|| backoff.failed());
                    if Instant::now() + wait > deadline {
                        self.gate.record_failure(started);
                        bail!(
                            "{why}; gave up after {}s (`retry_seconds`)",
                            self.retry.as_secs()
                        );
                    }
                    warn!(table = self.table, "{why}; retrying in {wait:?}");
                    tokio::time::sleep(wait).await;
                }
                Attempt::Refused { error, drift } => {
                    if drift {
                        self.stale = true;
                    } else {
                        self.gate.record_failure(started);
                    }
                    return Err(error);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{MapSecretStore, batch};
    use axum::extract::{Path, State};
    use axum::http::HeaderMap;
    use axum::response::IntoResponse;
    use axum::routing::{get, post};
    use axum::{Router, body::Bytes};
    use kayak_core::columns::ColumnMapping;
    use kayak_core::config::Secret;
    use kayak_core::connections::{ConnectionKind, Connections, TidepoolConnection};
    use serde_json::{Value, json};
    use std::collections::{HashMap, HashSet, VecDeque};
    use std::sync::Mutex;

    fn ok<T, E: std::fmt::Display>(result: std::result::Result<T, E>) -> T {
        result.unwrap_or_else(|e| panic!("the test could not get this far: {e}"))
    }

    fn failure<T>(result: Result<T>, expected: &str) -> anyhow::Error {
        match result {
            Ok(_) => panic!("{expected}, but the call succeeded"),
            Err(err) => err,
        }
    }

    /// A Tidepool that keeps what it's sent: one table, `readings`, and the
    /// rows written to it, once per idempotency key as the real one does.
    #[derive(Default)]
    struct Fake {
        table: Mutex<Value>,
        /// Answers to give before writing anything: (status, retry-after, body).
        script: Mutex<VecDeque<(u16, Option<u64>, Value)>>,
        /// Writes to make and then answer 503 anyway, as when a reply is lost
        /// on its way back.
        lost_replies: Mutex<usize>,
        posts: Mutex<Vec<(HeaderMap, String)>>,
        gets: Mutex<usize>,
        keys: Mutex<HashSet<String>>,
        rows: Mutex<Vec<Value>>,
    }

    fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
        m.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn readings() -> Value {
        json!({
            "name": "readings", "mode": "append", "primary_key": [],
            "columns": [
                {"name": "machine", "type": "string", "nullable": false, "encoding": "dict"},
                {"name": "at", "type": "timestamptz", "nullable": false, "encoding": "plain"},
                {"name": "value", "type": "decimal(10,3)", "nullable": true, "encoding": "plain"},
                {"name": "ok", "type": "bool", "nullable": true, "encoding": "dict"}
            ],
            "rows": 0, "written_total": 0, "rows_per_second": 0.0, "bytes": 0, "epoch": 1
        })
    }

    async fn describe_table(
        State(fake): State<Arc<Fake>>,
        Path(table): Path<String>,
    ) -> axum::response::Response {
        *lock(&fake.gets) += 1;
        let known = lock(&fake.table).clone();
        if known["name"] == table.as_str() {
            axum::Json(known).into_response()
        } else {
            (
                StatusCode::NOT_FOUND,
                axum::Json(json!({"error": "no table"})),
            )
                .into_response()
        }
    }

    async fn write_rows(
        State(fake): State<Arc<Fake>>,
        headers: HeaderMap,
        body: Bytes,
    ) -> axum::response::Response {
        let text = String::from_utf8_lossy(&body).to_string();
        lock(&fake.posts).push((headers.clone(), text.clone()));
        if let Some((status, after, body)) = lock(&fake.script).pop_front() {
            let status = StatusCode::from_u16(status).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR);
            let mut response = (status, axum::Json(body)).into_response();
            if let Some(after) = after {
                response
                    .headers_mut()
                    .insert(RETRY_AFTER, HeaderValue::from(after));
            }
            return response;
        }
        let key = headers
            .get(IDEMPOTENCY_HEADER)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_string();
        let lines: Vec<Value> = text
            .lines()
            .filter(|l| !l.trim().is_empty())
            .filter_map(|l| serde_json::from_str(l).ok())
            .collect();
        let written = lines.len() as u64;
        if lock(&fake.keys).insert(key) {
            lock(&fake.rows).extend(lines);
        }
        {
            let mut lost = lock(&fake.lost_replies);
            if *lost > 0 {
                *lost -= 1;
                let mut response = (
                    StatusCode::SERVICE_UNAVAILABLE,
                    axum::Json(json!({"error": "busy"})),
                )
                    .into_response();
                response
                    .headers_mut()
                    .insert(RETRY_AFTER, HeaderValue::from(0));
                return response;
            }
        }
        axum::Json(json!({"written": written, "replaced": 0, "missing": 0, "epoch": 7}))
            .into_response()
    }

    async fn tidepool() -> (Arc<Fake>, String) {
        let fake = Arc::new(Fake::default());
        *lock(&fake.table) = readings();
        let app = Router::new()
            .route("/v1/tables/{table}", get(describe_table))
            .route("/v1/tables/{table}/rows", post(write_rows))
            .with_state(Arc::clone(&fake));
        let listener = ok(tokio::net::TcpListener::bind("127.0.0.1:0").await);
        let addr = ok(listener.local_addr());
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (fake, format!("http://{addr}"))
    }

    fn column(name: &str, column_type: ColumnType) -> ColumnMapping {
        ColumnMapping {
            name: name.to_string(),
            column_type,
            field: None,
            message: false,
            nullable: None,
            on_missing: None,
        }
    }

    fn mapped() -> Vec<ColumnMapping> {
        vec![
            ColumnMapping {
                field: Some("tag".into()),
                nullable: Some(false),
                ..column("machine", ColumnType::Text)
            },
            ColumnMapping {
                nullable: Some(false),
                ..column("at", ColumnType::Timestamp)
            },
            column("value", ColumnType::Decimal),
            column("ok", ColumnType::Boolean),
        ]
    }

    fn config(columns: Vec<ColumnMapping>) -> TidepoolOutputConfig {
        TidepoolOutputConfig {
            connection: "tidepool".into(),
            table: "readings".into(),
            columns,
            on_extra_fields: ExtraFieldPolicy::Ignore,
            retry_seconds: Some(5),
            timeout_seconds: None,
        }
    }

    fn build_with(
        config: TidepoolOutputConfig,
        url: &str,
        token: Option<&str>,
        allow_http: Option<bool>,
    ) -> Result<Box<dyn OutputDestination>> {
        let mut pipelines = HashMap::new();
        let (events, _rx) = tokio::sync::broadcast::channel(4);
        let secrets = Arc::new(MapSecretStore::new(
            "a test store",
            &[("TIDEPOOL_TOKEN", "ingest-secret"), ("BLANK", "")],
        ));
        let connections: Connections = [(
            "tidepool".to_string(),
            ConnectionKind::Tidepool(TidepoolConnection {
                url: url.to_string(),
                token: token.map(Secret::new),
                allow_http,
            }),
        )]
        .into_iter()
        .collect();
        let mut ctx = BuildCtx::with_secrets(&mut pipelines, "plant".to_string(), events, secrets)
            .with_connections(Arc::new(connections));
        config.build(&mut ctx)
    }

    fn build(config: TidepoolOutputConfig, url: &str) -> Result<Box<dyn OutputDestination>> {
        build_with(config, url, None, None)
    }

    #[test]
    fn building_does_not_talk_to_the_server() {
        assert!(build(config(mapped()), "http://127.0.0.1:1").is_ok());
    }

    #[test]
    fn a_token_over_plaintext_needs_the_connection_to_allow_it() {
        let url = "http://localhost:7070";
        let token = Some("${TIDEPOOL_TOKEN}");
        assert!(build_with(config(Vec::new()), url, token, None).is_err());
        assert!(build_with(config(Vec::new()), url, token, Some(true)).is_ok());
        assert!(build_with(config(Vec::new()), "https://tidepool.example", token, None).is_ok());
        // no token, nothing to send in the clear
        assert!(build_with(config(Vec::new()), url, None, None).is_ok());
        let blank = build_with(config(Vec::new()), url, Some("${BLANK}"), Some(true));
        assert!(blank.is_err(), "an empty token is refused");
    }

    #[test]
    fn a_table_name_that_could_change_the_path_is_refused() {
        for name in ["readings/../admin", "readings?x=1", "", "a b"] {
            let mut config = config(Vec::new());
            config.table = name.to_string();
            assert!(build(config, "http://localhost:7070").is_err(), "'{name}'");
        }
    }

    #[tokio::test]
    async fn a_batch_is_one_ndjson_request_with_the_mapped_columns() {
        let (fake, url) = tidepool().await;
        let mut output = ok(build_with(
            config(mapped()),
            &url,
            Some("${TIDEPOOL_TOKEN}"),
            Some(true),
        ));
        ok(output.init().await);
        ok(output
            .emit(batch(vec![
                json!({"tag": "press-1", "at": 1_754_827_200, "value": 12.250, "ok": true}),
                json!({"tag": "press-2", "at": "2026-10-08T10:00:00Z"}),
            ]))
            .await);
        let posts = lock(&fake.posts).clone();
        assert_eq!(posts.len(), 1, "one request per batch");
        let (headers, body) = &posts[0];
        assert_eq!(headers[CONTENT_TYPE], "application/x-ndjson");
        assert_eq!(headers[AUTHORIZATION], "Bearer ingest-secret");
        let key = headers[IDEMPOTENCY_HEADER].to_str().unwrap_or_default();
        assert!(
            key.starts_with("plant:") && key.ends_with(":1"),
            "the key is pipeline:run:batch, got {key}"
        );
        assert_eq!(body.lines().count(), 2, "{body}");
        // the decimal's own digits go across as a number token
        assert!(body.contains(r#""value":12.25"#), "{body}");
        assert_eq!(
            *lock(&fake.rows),
            vec![
                json!({"machine": "press-1", "at": "2025-08-10T12:00:00+00:00", "value": 12.25, "ok": true}),
                json!({"machine": "press-2", "at": "2026-10-08T10:00:00Z", "value": null, "ok": null}),
            ]
        );
    }

    #[tokio::test]
    async fn unmapped_messages_go_across_as_they_are() {
        let (fake, url) = tidepool().await;
        let mut output = ok(build(config(Vec::new()), &url));
        ok(output.init().await);
        let row = json!({"machine": "m1", "at": "2026-10-08T10:00:00Z"});
        ok(output.emit(batch(vec![row.clone()])).await);
        assert_eq!(*lock(&fake.rows), vec![row]);
        assert!(
            !lock(&fake.posts)[0].0.contains_key(AUTHORIZATION),
            "no token, no header"
        );
    }

    #[tokio::test]
    async fn a_mapping_that_does_not_fit_the_table_fails_the_start() {
        let (_fake, url) = tidepool().await;
        let cases: Vec<(Vec<ColumnMapping>, &str)> = vec![
            (
                vec![column("colour", ColumnType::Text)],
                "`colour` is not a column of `readings` (its columns: machine, at, value, ok)",
            ),
            (
                {
                    let mut c = mapped();
                    c[2] = column("value", ColumnType::Text);
                    c
                },
                "`value` is decimal(10,3) in Tidepool, which a `text` column cannot write; map it as `decimal`",
            ),
            (
                {
                    let mut c = mapped();
                    c[0].nullable = None;
                    c
                },
                "`machine` needs a value in every row in Tidepool, and this mapping writes null",
            ),
            (
                mapped().into_iter().skip(1).collect(),
                "`machine` needs a value in every row in Tidepool, and no column writes it",
            ),
        ];
        for (columns, expected) in cases {
            let mut output = ok(build(config(columns), &url));
            let error = format!("{:#}", failure(output.init().await, expected));
            assert!(
                error.contains(expected),
                "expected `{expected}` in: {error}"
            );
        }

        let mut missing = config(Vec::new());
        missing.table = "nope".into();
        let mut output = ok(build(missing, &url));
        let error = format!("{:#}", failure(output.init().await, "no such table"));
        assert!(error.contains("has no table 'nope'"), "{error}");
    }

    /// The exit criterion: retries after a forced 503 produce no duplicates.
    /// The first answer is a 503 sent *after* the rows were written (a reply
    /// lost on the way back looks the same from here), the second a plain
    /// busy server; the retries carry the same key, so the batch lands once.
    #[tokio::test]
    async fn a_busy_server_is_retried_with_the_same_key_and_nothing_is_written_twice() {
        let (fake, url) = tidepool().await;
        let mut output = ok(build(config(mapped()), &url));
        ok(output.init().await);
        *lock(&fake.lost_replies) = 1;
        lock(&fake.script).push_back((503, Some(0), json!({"error": "busy"})));
        let messages = vec![json!({"tag": "m1", "at": "2026-10-08T10:00:00Z", "value": 1})];
        // the scripted busy answer comes first, then the write whose reply is
        // lost, then the retry that Tidepool recognises
        ok(output.emit(batch(messages.clone())).await);
        let posts = lock(&fake.posts).clone();
        assert_eq!(posts.len(), 3, "busy, lost reply, then the answer");
        let keys: HashSet<&str> = posts
            .iter()
            .map(|(h, _)| h[IDEMPOTENCY_HEADER].to_str().unwrap_or_default())
            .collect();
        assert_eq!(keys.len(), 1, "every attempt carries the same key");
        assert_eq!(lock(&fake.rows).len(), 1, "written once");

        // the next batch is a new key
        ok(output.emit(batch(messages)).await);
        assert_eq!(lock(&fake.rows).len(), 2);
    }

    #[tokio::test]
    async fn a_server_that_stays_busy_fails_the_batch_after_retry_seconds() {
        let (fake, url) = tidepool().await;
        let mut quick = config(Vec::new());
        quick.retry_seconds = Some(1);
        let mut output = ok(build(quick, &url));
        ok(output.init().await);
        lock(&fake.script).extend((0..20).map(|_| (503, Some(1), json!({"error": "busy"}))));
        let error = format!(
            "{:#}",
            failure(
                output.emit(batch(vec![json!({"machine": "m1"})])).await,
                "a server that never stops being busy"
            )
        );
        assert!(
            error.contains("503") && error.contains("gave up after 1s"),
            "{error}"
        );
        assert!(lock(&fake.rows).is_empty());
    }

    #[tokio::test]
    async fn a_refused_batch_quotes_the_problems_and_is_not_retried() {
        let (fake, url) = tidepool().await;
        let mut output = ok(build(config(Vec::new()), &url));
        ok(output.init().await);
        lock(&fake.script).push_back((
            400,
            None,
            json!({"error": "5 problems", "more": 1, "errors": [
                {"row": 0, "column": "at", "message": "a value is required"},
                {"row": 1, "column": "value", "message": "not a number: \"x\""},
                {"row": 1, "column": "colour", "message": "`colour` is not a column of `readings`"},
                {"row": 2, "message": "not an object"},
            ]}),
        ));
        let error = format!(
            "{:#}",
            failure(
                output.emit(batch(vec![json!({"machine": "m1"})])).await,
                "Tidepool refused it"
            )
        );
        assert!(
            error.contains(
                "row 0, column at: a value is required; row 1, column value: not a number: \"x\"; \
                 row 1, column colour: `colour` is not a column of `readings`; and 2 more"
            ),
            "{error}"
        );
        assert_eq!(lock(&fake.posts).len(), 1, "a refusal is not retried");
    }

    /// Tidepool's config changes live: after a refusal a changed table
    /// explains, the next batch reads the table again, and says what changed.
    #[tokio::test]
    async fn after_a_refusal_the_table_is_read_again() {
        let (fake, url) = tidepool().await;
        let mut output = ok(build(config(mapped()), &url));
        ok(output.init().await);
        assert_eq!(*lock(&fake.gets), 1);

        // `ok` is dropped from the table while the pipeline runs
        let mut changed = readings();
        if let Some(columns) = changed["columns"].as_array_mut() {
            columns.retain(|c| c["name"] != "ok");
        }
        *lock(&fake.table) = changed;
        lock(&fake.script).push_back((
            400,
            None,
            json!({"error": "1 problem", "more": 0, "errors": [
                {"row": 0, "column": "ok", "message": "`ok` is not a column of `readings`"}]}),
        ));
        let message = || {
            batch(vec![
                json!({"tag": "m1", "at": "2026-10-08T10:00:00Z", "ok": true}),
            ])
        };
        failure(output.emit(message()).await, "the first refusal");
        let error = format!(
            "{:#}",
            failure(output.emit(message()).await, "the table changed")
        );
        assert_eq!(*lock(&fake.gets), 2, "read again before the next batch");
        assert!(
            error.contains("`ok` is not a column of `readings`"),
            "{error}"
        );
        assert_eq!(
            lock(&fake.posts).len(),
            1,
            "and nothing sent that can't fit"
        );
    }

    #[tokio::test]
    async fn a_batch_the_mapping_skips_entirely_sends_nothing() {
        let (fake, url) = tidepool().await;
        let mut columns = mapped();
        columns[0].on_missing = Some(MissingColumnPolicy::SkipRow);
        let mut output = ok(build(config(columns), &url));
        ok(output.init().await);
        ok(output
            .emit(batch(vec![json!({"at": "2026-10-08T10:00:00Z"})]))
            .await);
        ok(output.emit(batch(Vec::new())).await);
        assert!(lock(&fake.posts).is_empty());
    }

    #[test]
    fn every_mapping_type_names_the_tidepool_types_it_can_write() {
        for (kayak, tidepool) in [
            (ColumnType::Text, "string"),
            (ColumnType::Integer, "int32"),
            (ColumnType::Integer, "float64"),
            (ColumnType::Bigint, "int64"),
            (ColumnType::Float, "float64"),
            (ColumnType::Decimal, "decimal(14, 2)"),
            (ColumnType::Boolean, "bool"),
            (ColumnType::Timestamp, "timestamptz"),
            (ColumnType::Date, "date"),
            (ColumnType::Json, "string"),
        ] {
            assert!(writes(kayak, tidepool), "{} → {tidepool}", kayak.as_str());
        }
        for (kayak, tidepool) in [
            (ColumnType::Bigint, "int32"),
            (ColumnType::Float, "decimal(14,2)"),
            (ColumnType::Timestamp, "timestamp"),
            (ColumnType::Text, "date"),
        ] {
            assert!(!writes(kayak, tidepool), "{} → {tidepool}", kayak.as_str());
        }
    }
}
