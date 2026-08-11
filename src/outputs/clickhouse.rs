//! Inserts batches into a `ClickHouse` table over its HTTP interface.
//!
//! The second consumer of [`crate::outputs::columns`], and the reason that
//! module is neutral: the mapping — which field goes in which column, what a
//! missing one does, what a value has to look like — is reused whole, and what
//! is written here is only the DDL, the wire format and the request.
//!
//! Three things differ from the postgres output, and each is `ClickHouse` being
//! itself rather than a gap:
//!
//! - **A batch is one insert.** Postgres executes one prepared statement per
//!   message; `ClickHouse` is a columnar store that merges parts in the
//!   background, and a row-at-a-time insert makes a part per row. So a batch
//!   becomes one request with one line per message, which is the shape the
//!   pipeline already hands over.
//! - **There is no surrogate key.** No auto-increment column, no unique
//!   constraint — so `order_by` names `MergeTree`'s sorting key and nothing here
//!   pretends it deduplicates. A table that names none is sorted by the
//!   `received_at` it gets for free, which is the closest honest analogue of
//!   postgres' `id`/`received_at` pair.
//! - **Values travel as JSON, not as text with a cast.** `JSONCompactEachRow`
//!   is the format, and the server parses each value against the column it
//!   lands in — the same division of labour `$n::text::NUMERIC` buys on the
//!   postgres side, spelled the way this server spells it. It keeps a decimal's
//!   own digits for the same reason: the number's text goes across as the
//!   plan produced it.

use anyhow::{Context, Result, anyhow, bail};
use kayak_core::columns::ColumnType;
use kayak_core::config::ClickhouseOutputConfig;
use reqwest::Client;
use std::sync::Arc;
use tracing::warn;

use crate::{
    BuildCtx,
    inputs::MessageBatch,
    outputs::{
        BuildOutput, OutputDestination,
        columns::{ColumnPlan, Identifier, Row, Table},
    },
    secrets::Resolved,
};

/// The timestamp a table gets when the config names no sorting key of its own.
const IMPLICIT_RECEIVED_AT: &str = "received_at";

/// The settings every request carries, and why each one is here.
///
/// `date_time_input_format=best_effort` so an ISO-8601 string with a `T` and a
/// `Z` parses — the default `basic` accepts only `YYYY-MM-DD hh:mm:ss`, and the
/// timestamps a stream carries are RFC 3339 far more often than not. Postgres
/// parses both without being asked, so this is what makes the same config work
/// against either server.
///
/// `input_format_null_as_default=0` because the default silently turns a null
/// into the column's default value. The mapping already refuses to write null
/// into a column that cannot hold one, so this should never fire — it is the
/// backstop that keeps "cannot hold a null" from quietly becoming "holds a
/// zero".
const SETTINGS: [(&str, &str); 2] = [
    ("date_time_input_format", "best_effort"),
    ("input_format_null_as_default", "0"),
];

impl BuildOutput for ClickhouseOutputConfig {
    fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn OutputDestination>> {
        // rejected at build time rather than on the first insert: a bad table
        // name should fail the pipeline that owns it, not surface an hour later
        let table = Table::parse(&self.table, "database")?;
        let layout = Layout::build(&self)?;
        let server = ctx
            .clickhouse_connection(&self.connection)
            .context("the clickhouse output cannot be built")?;

        let url = server.url.trim_end_matches('/').to_string();
        // the user and password go with every insert, so a plaintext url is a
        // decision the connection has to have written down — the same rule the
        // s3 output's `allow_http` follows, and for the same reason
        if url.starts_with("http://") {
            if !server.allows_http() {
                bail!(
                    "connection '{}' reaches clickhouse over plaintext http, which would send its \
                     credentials in the clear; set \"allow_http\": true on the connection if that \
                     is what you want",
                    self.connection
                );
            }
        } else if !url.starts_with("https://") {
            bail!(
                "connection '{}' has url '{url}', which is not an http(s) url; clickhouse is \
                 reached over its HTTP interface, e.g. http://localhost:8123",
                self.connection
            );
        }

        Ok(Box::new(ClickhouseOutput {
            url,
            database: server.database.clone(),
            user: server.user.clone(),
            password: ctx.resolve(&server.password).with_context(|| {
                format!(
                    "failed to resolve secrets in the password of connection '{}'",
                    self.connection
                )
            })?,
            create_table: self.create_table.unwrap_or(true),
            table,
            layout,
            client: None,
        }))
    }
}

/// What the table looks like, which is the one thing that differs between an
/// output with a column mapping and one without.
enum Layout {
    /// One column holding each message as JSON text, plus the arrival time the
    /// table is sorted by. What an output with no `columns` writes.
    Payload,
    /// The mapped columns, in the order the config wrote them.
    Mapped(Box<MappedTable>),
}

/// A column mapping, plus the parts of the created table that name columns.
struct MappedTable {
    plan: ColumnPlan,
    /// The sorting key, which is either what the config named or the implicit
    /// timestamp. Never empty — `MergeTree` has to be ordered by something, and
    /// `ORDER BY tuple()` is a table you cannot usefully query by time.
    order_by: Vec<Identifier>,
    /// whether the table gets a `received_at` of its own
    implicit_received_at: bool,
}

impl Layout {
    fn build(config: &ClickhouseOutputConfig) -> Result<Self> {
        if config.columns.is_empty() {
            if config.on_extra_fields != kayak_core::columns::ExtraFieldPolicy::Ignore {
                bail!(
                    "the clickhouse output for '{}' asks about extra fields but maps no columns; \
                     every field would be extra",
                    config.table
                );
            }
            if !config.order_by.is_empty() {
                bail!(
                    "the clickhouse output for '{}' names a sorting key but maps no columns",
                    config.table
                );
            }
            return Ok(Self::Payload);
        }

        let mut plan =
            ColumnPlan::build(&config.columns, config.on_extra_fields).with_context(|| {
                format!("the clickhouse output for '{}' cannot be built", config.table)
            })?;

        let mut order_by = Vec::with_capacity(config.order_by.len());
        for name in &config.order_by {
            if plan.column(name).is_none() {
                bail!("the sorting key names '{name}', which is not one of the mapped columns");
            }
            if order_by
                .iter()
                .any(|existing: &Identifier| existing.as_str() == name)
            {
                bail!("the sorting key names '{name}' twice");
            }
            // clickhouse will not sort by a Nullable column without being asked
            // to, so the mapping is brought into line here rather than left to
            // fail as a server error on the first insert
            plan.require_not_null(name)?;
            order_by.push(Identifier::parse(name, "column name")?);
        }

        if config.create_table == Some(false) && !order_by.is_empty() {
            // not refused: turning creation off for a table someone else owns is
            // a legitimate thing to do to a config that was creating it, and
            // deleting the key to say so would lose it on the way back
            warn!(
                table = config.table,
                "the clickhouse output names a sorting key but does not create its table; \
                 it describes a table it is not creating and nothing is applied"
            );
        }

        // dropped when the config names a key of its own, and dropped when a
        // mapped column claims the name: a message with its own `received_at`
        // should land in it
        let implicit_received_at =
            order_by.is_empty() && plan.column(IMPLICIT_RECEIVED_AT).is_none();
        if order_by.is_empty() {
            // either the implicit column or the mapped one exists under that
            // name by now, so the sorting key is the same either way
            order_by.push(Identifier::parse(IMPLICIT_RECEIVED_AT, "column name")?);
        }

        Ok(Self::Mapped(Box::new(MappedTable {
            plan,
            order_by,
            implicit_received_at,
        })))
    }
}

/// The `ClickHouse` type a logical column type is created as.
///
/// `Date32` rather than `Date` because `Date` stops in 2149 and starts in 1970,
/// which quietly refuses a birth date. `json` is a `String` holding the JSON
/// text: `ClickHouse`'s own `JSON` type is newer than this output is willing to
/// assume of a server, a String round-trips the value exactly, and
/// `JSONExtract` reads it.
fn sql_type(column_type: ColumnType) -> &'static str {
    match column_type {
        ColumnType::Text | ColumnType::Json => "String",
        ColumnType::Integer => "Int32",
        ColumnType::Bigint => "Int64",
        ColumnType::Float => "Float64",
        // wide enough for a currency amount and a scientific reading alike;
        // the plan carries the digits across as they were written
        ColumnType::Decimal => "Decimal(38, 9)",
        ColumnType::Boolean => "Bool",
        ColumnType::Timestamp => "DateTime64(3, 'UTC')",
        ColumnType::Date => "Date32",
        ColumnType::Uuid => "UUID",
    }
}

/// The column's declared type, wrapped for a column that accepts nulls.
fn declared_type(column_type: ColumnType, nullable: bool) -> String {
    let name = sql_type(column_type);
    if nullable {
        format!("Nullable({name})")
    } else {
        name.to_string()
    }
}

/// One value as the JSON token it goes across as.
///
/// The plan hands over text it has already checked against the column's type,
/// so this is only the question of whether that text is *already* JSON. It is
/// for the numbers and the boolean — the plan writes a number's own digits and
/// `true`/`false` — and it is not for the rest, which are strings on the wire
/// whatever they mean to the server. `json` is among them because the column is
/// a `String`: what is stored is the message's JSON *text*.
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

/// One row as a `JSONCompactEachRow` line: the values as a JSON array.
fn row_line(values: &[Option<String>], types: impl Iterator<Item = ColumnType>) -> String {
    let mut line = String::from("[");
    for (index, (value, column_type)) in values.iter().zip(types).enumerate() {
        if index > 0 {
            line.push(',');
        }
        match value {
            Some(text) => line.push_str(&token(text, column_type)),
            None => line.push_str("null"),
        }
    }
    line.push(']');
    line
}

impl MappedTable {
    fn create_table_sql(&self, table: &Table) -> String {
        let mut definitions: Vec<String> = Vec::new();
        if self.implicit_received_at {
            definitions.push(format!(
                "\"{IMPLICIT_RECEIVED_AT}\" DateTime64(3, 'UTC') DEFAULT now64(3)"
            ));
        }
        for column in self.plan.columns() {
            definitions.push(format!(
                "{} {}",
                column.name.quoted(),
                declared_type(column.column_type, column.nullable)
            ));
        }
        format!(
            "CREATE TABLE IF NOT EXISTS {table} ({definitions}) ENGINE = MergeTree ORDER BY ({order})",
            table = table.quoted(),
            definitions = definitions.join(", "),
            order = self
                .order_by
                .iter()
                .map(Identifier::quoted)
                .collect::<Vec<_>>()
                .join(", ")
        )
    }

    fn insert_sql(&self, table: &Table) -> String {
        format!(
            "INSERT INTO {table} ({names}) FORMAT JSONCompactEachRow",
            table = table.quoted(),
            names = self
                .plan
                .columns()
                .iter()
                .map(|column| column.name.quoted())
                .collect::<Vec<_>>()
                .join(", ")
        )
    }
}

impl Layout {
    fn create_table_sql(&self, table: &Table) -> String {
        match self {
            Self::Payload => format!(
                "CREATE TABLE IF NOT EXISTS {table} (\
                 \"received_at\" DateTime64(3, 'UTC') DEFAULT now64(3), \
                 \"payload\" String) \
                 ENGINE = MergeTree ORDER BY (\"received_at\")",
                table = table.quoted()
            ),
            Self::Mapped(mapped) => mapped.create_table_sql(table),
        }
    }

    fn insert_sql(&self, table: &Table) -> String {
        match self {
            Self::Payload => format!(
                "INSERT INTO {table} (\"payload\") FORMAT JSONCompactEachRow",
                table = table.quoted()
            ),
            Self::Mapped(mapped) => mapped.insert_sql(table),
        }
    }

    /// The batch as the body of one insert: one line per message, and an empty
    /// string when nothing survived the mapping.
    fn body(&self, message_batch: &MessageBatch) -> Result<String> {
        let mut body = String::new();
        for message in message_batch {
            match self {
                Self::Payload => {
                    let whole = message.to_string();
                    body.push_str(&row_line(
                        &[Some(whole)],
                        std::iter::once(ColumnType::Text),
                    ));
                }
                Self::Mapped(mapped) => {
                    let Row::Values(values) = mapped.plan.row(message)? else {
                        continue;
                    };
                    body.push_str(&row_line(
                        &values,
                        mapped.plan.columns().iter().map(|c| c.column_type),
                    ));
                }
            }
            body.push('\n');
        }
        Ok(body)
    }
}

pub struct ClickhouseOutput {
    url: String,
    database: String,
    user: String,
    password: Resolved,
    table: Table,
    layout: Layout,
    create_table: bool,
    client: Option<Client>,
}

impl ClickhouseOutput {
    /// How this server is described in an error. Everything but the password,
    /// which `Resolved` would not print anyway.
    fn describe(&self) -> String {
        format!(
            "clickhouse at {url} as '{user}' (database '{database}')",
            url = self.url,
            user = self.user,
            database = self.database
        )
    }

    /// Runs one statement, whatever it is: the DDL, the connection check, or an
    /// insert with its rows as the body.
    ///
    /// The credentials go in headers rather than in the query string — a url is
    /// the part of a request that ends up in a log, and `X-ClickHouse-Key` is
    /// what the server offers for exactly this.
    async fn execute(&self, client: &Client, sql: &str, body: String) -> Result<()> {
        let response = client
            .post(&self.url)
            .query(&[("database", self.database.as_str()), ("query", sql)])
            .query(&SETTINGS)
            .header("X-ClickHouse-User", &self.user)
            .header("X-ClickHouse-Key", self.password.expose())
            // spelled out, and not only implied by the body: an empty body
            // carries no length header of its own, and a POST that is neither
            // chunked nor length-declared is a 411 here rather than a statement
            // the server runs — which is what the DDL and the connection check
            // are, both of them bodyless
            .header(reqwest::header::CONTENT_LENGTH, body.len())
            .body(body)
            .send()
            .await
            .with_context(|| format!("failed to reach {}", self.describe()))?;

        let status = response.status();
        if status.is_success() {
            return Ok(());
        }
        // the body is the server's own message about the statement, which is
        // the only thing that says *why* — a bare 400 would send someone to the
        // server's log for what is usually a config mistake
        let detail = response
            .text()
            .await
            .unwrap_or_else(|e| format!("<the error body could not be read: {e}>"));
        Err(anyhow!(
            "{} refused the statement ({status}): {}",
            self.describe(),
            detail.trim()
        ))
    }
}

#[async_trait::async_trait]
impl OutputDestination for ClickhouseOutput {
    async fn init(&mut self) -> Result<()> {
        let client = Client::builder()
            .build()
            .context("failed to build the clickhouse http client")?;

        if self.create_table {
            // creation never *alters*: a table whose shape has moved on fails
            // the insert with the server's own error rather than being migrated
            // from a config file
            self.execute(&client, &self.layout.create_table_sql(&self.table), String::new())
                .await
                .with_context(|| format!("failed to create the table in {}", self.describe()))?;
        } else {
            // an HTTP client opens nothing, so without this a server that is
            // down or a password that is wrong would first be heard about on
            // the first batch. The postgres output finds that out by connecting;
            // this is the same news at the same moment.
            self.execute(&client, "SELECT 1", String::new())
                .await
                .with_context(|| format!("failed to reach {}", self.describe()))?;
        }

        self.client = Some(client);
        Ok(())
    }

    async fn emit(&mut self, message_batch: Arc<MessageBatch>) -> Result<()> {
        // as in the postgres output: doing nothing here would look like the
        // rows were written
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| anyhow!("clickhouse output is not connected; init() was not called"))?
            .clone();

        let body = self
            .layout
            .body(&message_batch)
            .with_context(|| format!("failed to map a message onto {}", self.table.quoted()))?;
        // every message was skipped by the mapping; an insert with no rows is a
        // round trip for nothing
        if body.is_empty() {
            return Ok(());
        }

        self.execute(&client, &self.layout.insert_sql(&self.table), body)
            .await
            .with_context(|| format!("failed to insert a batch into {}", self.describe()))
    }
}

#[cfg(test)]
mod tests {
    use super::{Layout, Table};
    use kayak_core::columns::{ColumnMapping, ColumnType, ExtraFieldPolicy, MissingColumnPolicy};
    use kayak_core::config::ClickhouseOutputConfig;
    use serde_json::{Value, json};
    use std::sync::Arc;

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

    fn config(columns: Vec<ColumnMapping>) -> ClickhouseOutputConfig {
        ClickhouseOutputConfig {
            connection: "local-clickhouse".into(),
            table: "readings".into(),
            columns,
            create_table: None,
            order_by: Vec::new(),
            on_extra_fields: ExtraFieldPolicy::Ignore,
        }
    }

    fn layout(config: &ClickhouseOutputConfig) -> anyhow::Result<(Table, Layout)> {
        let table = Table::parse(&config.table, "database")?;
        let layout = Layout::build(config)?;
        Ok((table, layout))
    }

    fn batch(messages: Vec<Value>) -> crate::inputs::MessageBatch {
        messages.into_iter().map(Arc::new).collect()
    }

    /// The whole build, through `BuildOutput` — what `Layout::build` alone
    /// cannot say is that the config the user writes reaches it.
    fn build(
        config: ClickhouseOutputConfig,
        allow_http: Option<bool>,
        url: &str,
    ) -> anyhow::Result<Box<dyn crate::outputs::OutputDestination>> {
        use crate::outputs::BuildOutput;
        let mut pipelines = std::collections::HashMap::new();
        let (events, _) = tokio::sync::broadcast::channel(4);
        let connections: kayak_core::connections::Connections = [(
            "local-clickhouse".to_string(),
            kayak_core::connections::ConnectionKind::Clickhouse(
                kayak_core::connections::ClickhouseConnection {
                    url: url.to_string(),
                    database: "kayak".to_string(),
                    user: "kayak".to_string(),
                    password: "kayak".into(),
                    allow_http,
                },
            ),
        )]
        .into_iter()
        .collect();
        let mut ctx = crate::BuildCtx::new(&mut pipelines, "p".to_string(), events)
            .with_connections(std::sync::Arc::new(connections));
        config.build(&mut ctx)
    }

    /// Without a mapping the table is one column of JSON text, sorted by the
    /// arrival time it gets for free.
    #[test]
    fn without_columns_the_message_is_stored_whole() -> anyhow::Result<()> {
        let (table, layout) = layout(&config(Vec::new()))?;
        assert_eq!(
            layout.insert_sql(&table),
            r#"INSERT INTO "readings" ("payload") FORMAT JSONCompactEachRow"#
        );
        assert_eq!(
            layout.create_table_sql(&table),
            r#"CREATE TABLE IF NOT EXISTS "readings" ("received_at" DateTime64(3, 'UTC') DEFAULT now64(3), "payload" String) ENGINE = MergeTree ORDER BY ("received_at")"#
        );
        // ...and the payload crosses the wire as a JSON *string* holding the
        // message's text, since the column is a String
        assert_eq!(
            layout.body(&batch(vec![json!({"a": 1})]))?,
            "[\"{\\\"a\\\":1}\"]\n"
        );
        Ok(())
    }

    #[test]
    fn mapped_columns_become_the_created_table() -> anyhow::Result<()> {
        let (table, layout) = layout(&config(vec![
            ColumnMapping {
                nullable: Some(false),
                ..column("sensor", ColumnType::Text)
            },
            column("value", ColumnType::Float),
            column("recorded_at", ColumnType::Timestamp),
        ]))?;
        assert_eq!(
            layout.create_table_sql(&table),
            r#"CREATE TABLE IF NOT EXISTS "readings" ("received_at" DateTime64(3, 'UTC') DEFAULT now64(3), "sensor" String, "value" Nullable(Float64), "recorded_at" Nullable(DateTime64(3, 'UTC'))) ENGINE = MergeTree ORDER BY ("received_at")"#
        );
        Ok(())
    }

    /// A database-qualified name quotes each part, and it is the one way to
    /// write into a database other than the connection's.
    #[test]
    fn a_qualified_name_quotes_each_part_separately() -> anyhow::Result<()> {
        let mut config = config(Vec::new());
        config.table = "analytics.readings".into();
        let (table, layout) = layout(&config)?;
        assert_eq!(
            layout.insert_sql(&table),
            r#"INSERT INTO "analytics"."readings" ("payload") FORMAT JSONCompactEachRow"#
        );
        Ok(())
    }

    /// The table name is interpolated into the SQL text, so this check is the
    /// only thing between `config.json` and arbitrary statements.
    #[test]
    fn a_name_that_could_carry_sql_is_rejected() {
        for name in [
            r#"readings"; drop table users; --"#,
            "readings; drop table users",
            "readings users",
            "a.b.c",
            "",
            "1readings",
        ] {
            assert!(
                Table::parse(name, "database").is_err(),
                "'{name}' should have been rejected"
            );
        }
    }

    /// Naming a sorting key says how the table is laid out, and drops the
    /// timestamp it would otherwise be sorted by.
    #[test]
    fn a_sorting_key_replaces_the_implicit_timestamp() -> anyhow::Result<()> {
        let mut config = config(vec![
            column("sensor", ColumnType::Text),
            column("recorded_at", ColumnType::Timestamp),
        ]);
        config.order_by = vec!["recorded_at".into(), "sensor".into()];
        let (table, layout) = layout(&config)?;
        let sql = layout.create_table_sql(&table);
        assert!(!sql.contains("DEFAULT now64"), "{sql}");
        assert!(sql.ends_with(r#"ORDER BY ("recorded_at", "sensor")"#), "{sql}");
        // clickhouse does not sort by a nullable column, so the mapping is
        // brought into line rather than left to fail on the first insert
        assert!(sql.contains(r#""sensor" String,"#), "{sql}");
        assert!(sql.contains(r#""recorded_at" DateTime64(3, 'UTC')"#), "{sql}");
        assert!(!sql.contains("Nullable"), "{sql}");
        Ok(())
    }

    /// A message with its own `received_at` should land in it rather than fight
    /// the implicit column for the name.
    #[test]
    fn a_mapped_column_may_claim_the_implicit_name() -> anyhow::Result<()> {
        let (table, layout) = layout(&config(vec![column("received_at", ColumnType::Timestamp)]))?;
        let sql = layout.create_table_sql(&table);
        assert!(!sql.contains("DEFAULT now64"), "{sql}");
        assert!(sql.ends_with(r#"ORDER BY ("received_at")"#), "{sql}");
        Ok(())
    }

    /// Every column type has to produce a *parseable* line — the plan gives
    /// this module checked text, and which of those texts is already JSON is an
    /// invariant of the pair rather than of either half.
    #[test]
    fn every_column_type_produces_a_json_line() -> anyhow::Result<()> {
        let (_, layout) = layout(&config(vec![
            column("a", ColumnType::Text),
            column("b", ColumnType::Integer),
            column("c", ColumnType::Bigint),
            column("d", ColumnType::Float),
            column("e", ColumnType::Decimal),
            column("f", ColumnType::Boolean),
            column("g", ColumnType::Timestamp),
            column("h", ColumnType::Date),
            column("i", ColumnType::Uuid),
            column("j", ColumnType::Json),
            column("missing", ColumnType::Text),
        ]))?;
        let body = layout.body(&batch(vec![json!({
            "a": "text\"with\"quotes",
            "b": 3,
            "c": 9_007_199_254_740_993_i64,
            "d": 1.5,
            "e": 9_007_199_254_740_993_i64,
            "f": true,
            "g": "2026-08-11T12:00:00Z",
            "h": "2026-08-11",
            "i": "0197f0e6-0000-7000-8000-000000000000",
            "j": {"nested": [1, 2]}
        })]))?;
        let parsed: Value = serde_json::from_str(body.trim())?;
        assert_eq!(
            parsed,
            json!([
                "text\"with\"quotes",
                3,
                9_007_199_254_740_993_i64,
                1.5,
                9_007_199_254_740_993_i64,
                true,
                "2026-08-11T12:00:00Z",
                "2026-08-11",
                "0197f0e6-0000-7000-8000-000000000000",
                "{\"nested\":[1,2]}",
                null
            ])
        );
        Ok(())
    }

    /// A batch is one insert, and a message the mapping skipped is simply not a
    /// line — which is what makes an all-skipped batch nothing at all.
    #[test]
    fn a_batch_is_one_body_and_a_skipped_message_is_no_line() -> anyhow::Result<()> {
        let (_, layout) = layout(&config(vec![ColumnMapping {
            on_missing: Some(MissingColumnPolicy::SkipRow),
            ..column("a", ColumnType::Text)
        }]))?;
        assert_eq!(
            layout.body(&batch(vec![json!({"a": "x"}), json!({}), json!({"a": "y"})]))?,
            "[\"x\"]\n[\"y\"]\n"
        );
        assert!(layout.body(&batch(vec![json!({})]))?.is_empty());
        Ok(())
    }

    /// Building must not talk to the server — a pipeline that starts is one
    /// whose settings parse, not one whose database happened to be up.
    #[test]
    fn it_builds_against_a_server_that_is_not_running() -> anyhow::Result<()> {
        build(
            config(vec![column("sensor", ColumnType::Text)]),
            Some(true),
            "http://127.0.0.1:1",
        )?;
        Ok(())
    }

    /// The credentials go with every insert, so plaintext is a decision the
    /// connection has to have written down.
    #[test]
    fn a_plaintext_url_needs_the_connection_to_allow_it() {
        assert!(build(config(Vec::new()), None, "http://localhost:8123").is_err());
        assert!(build(config(Vec::new()), Some(false), "http://localhost:8123").is_err());
        assert!(build(config(Vec::new()), Some(true), "http://localhost:8123").is_ok());
        // https needs no permission, and neither does it need to be asked for
        assert!(build(config(Vec::new()), None, "https://localhost:8443").is_ok());
        // ...and the native protocol's url is not something this can speak
        assert!(build(config(Vec::new()), Some(true), "localhost:9000").is_err());
    }

    #[test]
    fn a_sorting_key_naming_an_unmapped_column_is_refused() {
        let mut unmapped = config(vec![column("a", ColumnType::Text)]);
        unmapped.order_by = vec!["b".into()];
        assert!(layout(&unmapped).is_err());

        let mut duplicated = config(vec![column("a", ColumnType::Text)]);
        duplicated.order_by = vec!["a".into(), "a".into()];
        assert!(layout(&duplicated).is_err());
    }

    /// Without columns there is nothing for a sorting key or an extra-field
    /// check to be about.
    #[test]
    fn table_options_without_columns_are_refused() {
        let mut with_key = config(Vec::new());
        with_key.order_by = vec!["a".into()];
        assert!(layout(&with_key).is_err());

        let mut strict = config(Vec::new());
        strict.on_extra_fields = ExtraFieldPolicy::Error;
        assert!(layout(&strict).is_err());
    }

    /// The mapping's own build-time refusals reach the output — the message
    /// names the table, so a config with several clickhouse outputs says which.
    #[test]
    fn a_contradictory_column_fails_the_output() {
        let contradiction = config(vec![ColumnMapping {
            nullable: Some(false),
            on_missing: Some(MissingColumnPolicy::Null),
            ..column("a", ColumnType::Text)
        }]);
        let error = format!(
            "{:#}",
            layout(&contradiction).err().unwrap_or_else(|| {
                panic!("the contradiction should have been refused");
            })
        );
        assert!(error.contains("readings"), "{error}");
    }
}
