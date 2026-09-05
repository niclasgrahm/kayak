//! The clickhouse input: [`super::poll::Poller`] over a [`ClickhouseReader`],
//! reading over the HTTP interface the output writes through.
//!
//! The same shape as the postgres input with three things spelled `ClickHouse`'s
//! way, each found rather than chosen:
//!
//! - **The server renders the rows as `JSONEachRow`**, and two settings make
//!   that rendering the one a pipeline wants: `output_format_json_quote_64bit_integers=0`
//!   because the default quotes every `Int64` as a string — a JavaScript
//!   habit the server keeps for browsers, and one that would turn an id into
//!   text on the way in — and `date_time_output_format=iso`, so a `DateTime`
//!   is `2026-01-01T12:00:00Z` rather than a bare `2026-01-01 12:00:00` that
//!   the postgres output would refuse.
//! - **The cursor is `toString(q.field)`**, selected beside the row under a
//!   name nothing sensible calls a column and removed before the row is handed
//!   on. `toString` is unaffected by the output setting above and `CAST`
//!   reads back what it wrote, which is the round trip the watermark needs.
//! - **`maxOrNull`, not `max`.** An aggregate over an empty relation returns
//!   the type's zero here, not `NULL` — and a watermark of `1970-01-01` on an
//!   empty table would read the whole table from the epoch, which is the
//!   opposite of `start_from: newest`.
//!
//! The cursor's type comes from `DESCRIBE` of a one-column subquery, the
//! HTTP-shaped twin of preparing a statement, and the watermark goes across as
//! a query parameter — `{cursor:String}` — rather than into the SQL text.

use anyhow::{Context, Result, anyhow, bail};
use kayak_core::config::ClickhouseInputConfig;
use reqwest::Client;
use serde_json::Value;

use crate::{
    BuildCtx,
    inputs::{
        BuildInput, InputSource, ack,
        poll::{Cursor, Fetched, Plan, Poller, Reader},
    },
    outputs::clickhouse::checked_url,
    secrets::Resolved,
};

/// The column the cursor's text travels under, beside the row's own columns.
const CURSOR_COLUMN: &str = "__kayak_cursor";

/// The settings every request carries — see the module docs for each.
/// `date_time_input_format=best_effort` is the output's, so a lagged or
/// watermarked comparison parses a timestamp the same way an insert does.
const SETTINGS: [(&str, &str); 3] = [
    ("date_time_input_format", "best_effort"),
    ("output_format_json_quote_64bit_integers", "0"),
    ("date_time_output_format", "iso"),
];

impl BuildInput for ClickhouseInputConfig {
    fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn InputSource>> {
        ack::require_receipt_only(ctx.ack_mode(), "clickhouse")?;
        let plan =
            Plan::build(&self.poll, "database").context("the clickhouse input cannot be built")?;
        let server = ctx
            .clickhouse_connection(&self.connection)
            .context("the clickhouse input cannot be built")?;
        let reader = ClickhouseReader {
            url: checked_url(server, &self.connection)?,
            database: server.database.clone(),
            user: server.user.clone(),
            password: ctx.resolve(&server.password).with_context(|| {
                format!(
                    "failed to resolve secrets in the password of connection '{}'",
                    self.connection
                )
            })?,
            sql: Statements::new(&plan),
            client: None,
            cursor_type: None,
        };
        let envelope = ctx.envelope("clickhouse", Some(&self.connection));
        Ok(Box::new(Poller::new(
            plan,
            Box::new(reader),
            self.connection,
            envelope,
            ctx.pipeline_id.clone(),
            ctx.events.clone(),
        )))
    }
}

/// The SQL, rendered once from the plan. Pure, and tested without a server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Statements {
    relation: String,
    cursor: Option<Cursor>,
}

impl Statements {
    #[must_use]
    pub fn new(plan: &Plan) -> Self {
        Self {
            relation: plan.relation_sql(),
            cursor: plan.cursor.clone(),
        }
    }

    fn field(&self) -> Result<String> {
        self.cursor
            .as_ref()
            .map(|c| format!("q.{}", c.field.quoted()))
            .ok_or_else(|| anyhow!("a snapshot has no cursor field"))
    }

    fn lag(&self) -> Result<String> {
        Ok(match self.cursor.as_ref().and_then(|c| c.lag) {
            Some(lag) => format!(
                " AND {} <= now() - toIntervalSecond({})",
                self.field()?,
                lag.as_secs()
            ),
            None => String::new(),
        })
    }

    #[must_use]
    pub fn snapshot(&self) -> String {
        format!("SELECT * FROM ({}) AS q FORMAT JSONEachRow", self.relation)
    }

    /// `DESCRIBE` of the cursor column alone: its type without a row.
    pub fn probe(&self) -> Result<String> {
        Ok(format!(
            "DESCRIBE (SELECT {} FROM ({}) AS q) FORMAT JSONEachRow",
            self.field()?,
            self.relation
        ))
    }

    pub fn first_page(&self, limit: usize) -> Result<String> {
        let field = self.field()?;
        Ok(format!(
            "SELECT q.*, toString({field}) AS {CURSOR_COLUMN} FROM ({}) AS q \
             WHERE {field} IS NOT NULL{} ORDER BY {field} LIMIT {limit} FORMAT JSONEachRow",
            self.relation,
            self.lag()?
        ))
    }

    /// A page above the watermark, which travels as the `cursor` query
    /// parameter and is cast to `cursor_type` — what the probe returned.
    pub fn next_page(&self, cursor_type: &str, limit: usize) -> Result<String> {
        let field = self.field()?;
        Ok(format!(
            "SELECT q.*, toString({field}) AS {CURSOR_COLUMN} FROM ({}) AS q \
             WHERE {field} > CAST({{cursor:String}} AS {cursor_type}){} \
             ORDER BY {field} LIMIT {limit} FORMAT JSONEachRow",
            self.relation,
            self.lag()?
        ))
    }

    pub fn newest(&self) -> Result<String> {
        Ok(format!(
            "SELECT toString(maxOrNull({})) AS {CURSOR_COLUMN} FROM ({}) AS q FORMAT JSONEachRow",
            self.field()?,
            self.relation
        ))
    }
}

/// `JSONEachRow` is one JSON object per line.
fn rows_of(body: &str) -> Result<Vec<Value>> {
    body.lines()
        .filter(|line| !line.trim().is_empty())
        .map(|line| serde_json::from_str(line).with_context(|| format!("not a JSON row: {line}")))
        .collect()
}

/// Takes the cursor column off a page row, leaving the row as the table has
/// it. The value is what `toString` rendered, or `null` for... nothing: the
/// page excludes null cursors, so a missing one is a row that is not what the
/// query was asked for.
fn split_cursor(mut row: Value) -> Result<Fetched> {
    let Value::Object(map) = &mut row else {
        bail!("a row is not a JSON object: {row}");
    };
    let cursor = match map.remove(CURSOR_COLUMN) {
        Some(Value::String(text)) => Some(text),
        Some(Value::Null) | None => None,
        Some(other) => Some(other.to_string()),
    };
    Ok(Fetched { row, cursor })
}

pub struct ClickhouseReader {
    url: String,
    database: String,
    user: String,
    password: Resolved,
    sql: Statements,
    client: Option<Client>,
    cursor_type: Option<String>,
}

impl ClickhouseReader {
    fn client(&mut self) -> Result<Client> {
        if self.client.is_none() {
            self.client = Some(
                Client::builder()
                    .build()
                    .context("failed to build the clickhouse http client")?,
            );
        }
        self.client
            .clone()
            .ok_or_else(|| anyhow!("clickhouse input has no http client"))
    }

    /// Runs one statement and returns the body. `params` are query
    /// parameters — `param_cursor` for `{cursor:String}`.
    async fn execute(&mut self, sql: &str, params: &[(&str, &str)]) -> Result<String> {
        let client = self.client()?;
        let response = client
            .post(&self.url)
            .query(&[("database", self.database.as_str()), ("query", sql)])
            .query(&SETTINGS)
            .query(params)
            .header("X-ClickHouse-User", &self.user)
            .header("X-ClickHouse-Key", self.password.expose())
            // spelled out for the reason the output spells it out: a POST
            // that is neither chunked nor length-declared is a 411 here
            .header(reqwest::header::CONTENT_LENGTH, 0)
            .send()
            .await
            .with_context(|| format!("failed to reach {}", self.describe()))?;
        let status = response.status();
        let body = response
            .text()
            .await
            .unwrap_or_else(|e| format!("<the body could not be read: {e}>"));
        if status.is_success() {
            return Ok(body);
        }
        Err(anyhow!(
            "{} refused the statement ({status}): {}",
            self.describe(),
            body.trim()
        ))
    }

    async fn cursor_type(&mut self) -> Result<String> {
        if let Some(known) = &self.cursor_type {
            return Ok(known.clone());
        }
        let probe = self.sql.probe()?;
        let body = self
            .execute(&probe, &[])
            .await
            .context("failed to find the cursor column's type")?;
        let name = rows_of(&body)?
            .first()
            .and_then(|row| row["type"].as_str())
            .map(ToString::to_string)
            .ok_or_else(|| anyhow!("the cursor probe returned no column"))?;
        self.cursor_type = Some(name.clone());
        Ok(name)
    }
}

#[async_trait::async_trait]
impl Reader for ClickhouseReader {
    async fn snapshot(&mut self) -> Result<Vec<Value>> {
        let sql = self.sql.snapshot();
        let body = self.execute(&sql, &[]).await?;
        rows_of(&body)
    }

    async fn page(&mut self, after: Option<&str>, limit: usize) -> Result<Vec<Fetched>> {
        let body = match after {
            None => {
                let sql = self.sql.first_page(limit)?;
                self.execute(&sql, &[]).await?
            }
            Some(watermark) => {
                let cursor_type = self.cursor_type().await?;
                let sql = self.sql.next_page(&cursor_type, limit)?;
                self.execute(&sql, &[("param_cursor", watermark)]).await?
            }
        };
        rows_of(&body)?.into_iter().map(split_cursor).collect()
    }

    async fn newest(&mut self) -> Result<Option<String>> {
        let sql = self.sql.newest()?;
        let body = self.execute(&sql, &[]).await?;
        let rows = rows_of(&body)?;
        let Some(row) = rows.into_iter().next() else {
            return Ok(None);
        };
        Ok(split_cursor(row)?.cursor)
    }

    fn describe(&self) -> String {
        format!(
            "clickhouse at {url} as '{user}' (database '{database}')",
            url = self.url,
            user = self.user,
            database = self.database
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kayak_core::config::AckMode;
    use kayak_core::connections::{ClickhouseConnection, ConnectionKind, Connections};
    use kayak_core::sql::{PollMode, SqlPollConfig, StartFrom};
    use serde_json::json;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn poll(mode: PollMode) -> SqlPollConfig {
        SqlPollConfig {
            table: Some("readings".into()),
            query: None,
            columns: Vec::new(),
            interval_secs: 5,
            mode,
            page_size: None,
            max_batch: None,
        }
    }

    fn incremental(lag_secs: Option<u64>) -> PollMode {
        PollMode::Incremental {
            field: "received_at".into(),
            start_from: Some(StartFrom::Oldest),
            lag_secs,
        }
    }

    fn statements(poll: &SqlPollConfig) -> Result<Statements> {
        Ok(Statements::new(&Plan::build(poll, "database")?))
    }

    fn build(
        poll: SqlPollConfig,
        allow_http: Option<bool>,
        ack_mode: Option<AckMode>,
    ) -> Result<Box<dyn InputSource>> {
        let mut pipelines = HashMap::new();
        let (events, _rx) = tokio::sync::broadcast::channel(4);
        let connections: Connections = [(
            "local-clickhouse".to_string(),
            ConnectionKind::Clickhouse(ClickhouseConnection {
                url: "http://localhost:8123".into(),
                database: "kayak".into(),
                user: "kayak".into(),
                password: "hunter2".into(),
                allow_http,
            }),
        )]
        .into_iter()
        .collect();
        let mut ctx = BuildCtx::new(&mut pipelines, "p".to_string(), events)
            .with_connections(Arc::new(connections));
        ctx.ack_mode = ack_mode;
        ClickhouseInputConfig {
            connection: "local-clickhouse".into(),
            poll,
        }
        .build(&mut ctx)
    }

    #[test]
    fn a_snapshot_is_the_whole_relation_as_rows() -> Result<()> {
        let sql = statements(&poll(PollMode::Snapshot))?;
        assert_eq!(
            sql.snapshot(),
            r#"SELECT * FROM (SELECT * FROM "readings") AS q FORMAT JSONEachRow"#
        );
        Ok(())
    }

    #[test]
    fn the_pages_carry_the_cursor_beside_the_row() -> Result<()> {
        let sql = statements(&poll(incremental(None)))?;
        assert_eq!(
            sql.first_page(20)?,
            r#"SELECT q.*, toString(q."received_at") AS __kayak_cursor FROM (SELECT * FROM "readings") AS q WHERE q."received_at" IS NOT NULL ORDER BY q."received_at" LIMIT 20 FORMAT JSONEachRow"#
        );
        assert_eq!(
            sql.next_page("DateTime64(3, 'UTC')", 20)?,
            r#"SELECT q.*, toString(q."received_at") AS __kayak_cursor FROM (SELECT * FROM "readings") AS q WHERE q."received_at" > CAST({cursor:String} AS DateTime64(3, 'UTC')) ORDER BY q."received_at" LIMIT 20 FORMAT JSONEachRow"#
        );
        assert_eq!(
            sql.probe()?,
            r#"DESCRIBE (SELECT q."received_at" FROM (SELECT * FROM "readings") AS q) FORMAT JSONEachRow"#
        );
        // maxOrNull, not max — see the module docs
        assert_eq!(
            sql.newest()?,
            r#"SELECT toString(maxOrNull(q."received_at")) AS __kayak_cursor FROM (SELECT * FROM "readings") AS q FORMAT JSONEachRow"#
        );
        Ok(())
    }

    #[test]
    fn a_lag_holds_both_pages_back_from_now() -> Result<()> {
        let sql = statements(&poll(incremental(Some(45))))?;
        for page in [sql.first_page(10)?, sql.next_page("DateTime", 10)?] {
            assert!(
                page.contains(r#"AND q."received_at" <= now() - toIntervalSecond(45) ORDER BY"#),
                "{page}"
            );
        }
        Ok(())
    }

    #[test]
    fn the_cursor_column_is_taken_off_the_row() -> Result<()> {
        let fetched = split_cursor(json!({"id": 1, "__kayak_cursor": "2026-01-01 00:00:00"}))?;
        assert_eq!(fetched.row, json!({"id": 1}));
        assert_eq!(fetched.cursor.as_deref(), Some("2026-01-01 00:00:00"));
        assert_eq!(split_cursor(json!({"__kayak_cursor": null}))?.cursor, None);
        assert!(split_cursor(json!([1])).is_err());
        Ok(())
    }

    #[test]
    fn json_each_row_is_one_object_per_line() -> Result<()> {
        let rows = rows_of("{\"a\":1}\n{\"a\":2}\n\n")?;
        assert_eq!(rows, vec![json!({"a": 1}), json!({"a": 2})]);
        assert!(rows_of("{\"a\":1}\nnot json").is_err());
        Ok(())
    }

    #[test]
    fn it_builds_against_a_server_that_is_not_running() -> Result<()> {
        build(poll(incremental(None)), Some(true), None)?;
        Ok(())
    }

    /// The same rule the output follows: credentials over plaintext take a
    /// decision written on the connection.
    #[test]
    fn a_plaintext_url_is_refused_unless_the_connection_allows_it() {
        assert!(build(poll(PollMode::Snapshot), None, None).is_err());
        assert!(build(poll(PollMode::Snapshot), Some(true), None).is_ok());
    }

    #[test]
    fn on_delivery_is_refused() {
        let Err(err) = build(poll(PollMode::Snapshot), Some(true), Some(AckMode::OnDelivery))
        else {
            panic!("a clickhouse input built with `ack: on_delivery`");
        };
        assert!(format!("{err:#}").contains("clickhouse"), "{err:#}");
    }
}
