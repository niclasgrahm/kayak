//! The postgres input: [`super::poll::Poller`] over a [`PostgresReader`].
//!
//! Everything about polling is in `poll.rs`; what is here is how postgres
//! spells the four queries, and two decisions worth knowing:
//!
//! - **The server renders the rows.** Every query selects `row_to_json(q)`,
//!   so the type matrix — what a `numeric`, a `timestamptz`, a `jsonb`, a
//!   `bytea` look like as JSON — is postgres' own and not a list maintained
//!   here. It is the mirror of the output's `$n::text::NUMERIC`: there the
//!   server parses, here the server prints, and in both directions a value's
//!   digits are the server's.
//! - **The watermark travels as text and is cast by the server.** A cursor
//!   value comes back as `(q.field)::text` and goes into the next query as
//!   `($1::text)::<type>`, with the type read off a prepared statement rather
//!   than guessed from the JSON. Text in, text out is the one representation
//!   every postgres type round-trips exactly, and it means the input never
//!   has to bind a timestamp or a numeric as a Rust value.

use anyhow::{Context, Result, anyhow};
use kayak_core::config::PostgresInputConfig;
use serde_json::Value;
use tokio_postgres::NoTls;
use tracing::error;

use crate::{
    BuildCtx,
    inputs::{
        BuildInput, InputSource, ack,
        poll::{Cursor, Fetched, Plan, Poller, Reader},
    },
    secrets::Resolved,
};

const DEFAULT_PORT: u16 = 5432;

impl BuildInput for PostgresInputConfig {
    fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn InputSource>> {
        // the watermark moves when rows are handed on, so there is nothing
        // `on_delivery` could hold back — see `kayak_core::sql`
        ack::require_receipt_only(ctx.ack_mode(), "postgres")?;
        let plan = Plan::build(&self.poll, "schema").context("the postgres input cannot be built")?;
        let server = ctx
            .postgres_connection(&self.connection)
            .context("the postgres input cannot be built")?;
        let reader = PostgresReader {
            host: server.host.clone(),
            port: server.port.unwrap_or(DEFAULT_PORT),
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
        let envelope = ctx.envelope("postgres", Some(&self.connection));
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

/// The SQL, rendered once from the plan. Pure, and the part that is tested
/// without a server.
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

    /// `AND q.field <= now() - interval ...` for a lagged cursor, else nothing.
    fn lag(&self) -> Result<String> {
        Ok(match self.cursor.as_ref().and_then(|c| c.lag) {
            Some(lag) => format!(
                " AND {} <= now() - interval '{} seconds'",
                self.field()?,
                lag.as_secs()
            ),
            None => String::new(),
        })
    }

    /// Every row, as JSON text.
    #[must_use]
    pub fn snapshot(&self) -> String {
        format!(
            "SELECT row_to_json(q)::text FROM ({}) AS q",
            self.relation
        )
    }

    /// Selects the cursor column and nothing else, so its type can be read
    /// off the prepared statement without fetching a row.
    pub fn probe(&self) -> Result<String> {
        Ok(format!(
            "SELECT {} FROM ({}) AS q LIMIT 0",
            self.field()?,
            self.relation
        ))
    }

    /// The first page: every row with a cursor, from the beginning.
    pub fn first_page(&self, limit: usize) -> Result<String> {
        let field = self.field()?;
        Ok(format!(
            "SELECT row_to_json(q)::text, ({field})::text FROM ({}) AS q \
             WHERE {field} IS NOT NULL{} ORDER BY {field} LIMIT {limit}",
            self.relation,
            self.lag()?
        ))
    }

    /// A page above the watermark, which is bound as `$1` and cast to
    /// `cursor_type` — the name the probe returned.
    pub fn next_page(&self, cursor_type: &str, limit: usize) -> Result<String> {
        let field = self.field()?;
        Ok(format!(
            "SELECT row_to_json(q)::text, ({field})::text FROM ({}) AS q \
             WHERE {field} > ($1::text)::\"{cursor_type}\"{} ORDER BY {field} LIMIT {limit}",
            self.relation,
            self.lag()?
        ))
    }

    /// The highest cursor value there is, as text.
    pub fn newest(&self) -> Result<String> {
        Ok(format!(
            "SELECT max({})::text FROM ({}) AS q",
            self.field()?,
            self.relation
        ))
    }
}

pub struct PostgresReader {
    host: String,
    port: u16,
    database: String,
    user: String,
    password: Resolved,
    sql: Statements,
    client: Option<tokio_postgres::Client>,
    /// The cursor column's type name, read once per connection. Cleared with
    /// the client, since a reconnect may be to a server whose column moved.
    cursor_type: Option<String>,
}

impl PostgresReader {
    /// Built field by field rather than as a url, for the reason the output
    /// does it: a password with an `@` in it needs no escaping, and it is
    /// never concatenated into a string that might get logged.
    fn connection_config(&self) -> tokio_postgres::Config {
        let mut config = tokio_postgres::Config::new();
        config
            .host(&self.host)
            .port(self.port)
            .dbname(&self.database)
            .user(&self.user)
            .password(self.password.expose());
        config
    }

    async fn client(&mut self) -> Result<&tokio_postgres::Client> {
        if self.client.is_none() {
            let (client, connection) = self
                .connection_config()
                .connect(NoTls)
                .await
                .with_context(|| format!("failed to connect to {}", self.describe()))?;
            let described = self.describe();
            tokio::spawn(async move {
                if let Err(e) = connection.await {
                    error!("postgres connection to {described} closed: {e:?}");
                }
            });
            self.client = Some(client);
        }
        self.client
            .as_ref()
            .ok_or_else(|| anyhow!("postgres input is not connected"))
    }

    /// A failed query drops the connection, so the next call dials again
    /// rather than reusing a socket that may be the thing that failed.
    fn dropped<T>(&mut self, result: Result<T>) -> Result<T> {
        if result.is_err() {
            self.client = None;
            self.cursor_type = None;
        }
        result
    }

    async fn cursor_type(&mut self) -> Result<String> {
        if let Some(known) = &self.cursor_type {
            return Ok(known.clone());
        }
        let probe = self.sql.probe()?;
        let client = self.client().await?;
        let statement = client
            .prepare(&probe)
            .await
            .context("failed to find the cursor column's type")?;
        let name = statement
            .columns()
            .first()
            .map(|column| column.type_().name().to_string())
            .ok_or_else(|| anyhow!("the cursor probe returned no column"))?;
        self.cursor_type = Some(name.clone());
        Ok(name)
    }

    async fn page_rows(&mut self, after: Option<&str>, limit: usize) -> Result<Vec<Fetched>> {
        let rows = match after {
            None => {
                let sql = self.sql.first_page(limit)?;
                self.client().await?.query(&sql, &[]).await?
            }
            Some(watermark) => {
                let cursor_type = self.cursor_type().await?;
                let sql = self.sql.next_page(&cursor_type, limit)?;
                self.client().await?.query(&sql, &[&watermark]).await?
            }
        };
        rows.iter()
            .map(|row| {
                let text: String = row.try_get(0)?;
                let cursor: Option<String> = row.try_get(1)?;
                Ok(Fetched {
                    row: serde_json::from_str(&text)?,
                    cursor,
                })
            })
            .collect()
    }
}

#[async_trait::async_trait]
impl Reader for PostgresReader {
    async fn snapshot(&mut self) -> Result<Vec<Value>> {
        let sql = self.sql.snapshot();
        let result = async {
            let rows = self.client().await?.query(&sql, &[]).await?;
            rows.iter()
                .map(|row| {
                    let text: String = row.try_get(0)?;
                    Ok(serde_json::from_str(&text)?)
                })
                .collect::<Result<Vec<Value>>>()
        }
        .await;
        self.dropped(result)
    }

    async fn page(&mut self, after: Option<&str>, limit: usize) -> Result<Vec<Fetched>> {
        let result = self.page_rows(after, limit).await;
        self.dropped(result)
    }

    async fn newest(&mut self) -> Result<Option<String>> {
        let sql = self.sql.newest()?;
        let result = async {
            let row = self.client().await?.query_one(&sql, &[]).await?;
            Ok(row.try_get::<_, Option<String>>(0)?)
        }
        .await;
        self.dropped(result)
    }

    fn describe(&self) -> String {
        format!(
            "postgres://{user}@{host}:{port}/{database}",
            user = self.user,
            host = self.host,
            port = self.port,
            database = self.database
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kayak_core::config::AckMode;
    use kayak_core::connections::{ConnectionKind, Connections, PostgresConnection};
    use kayak_core::sql::{PollMode, SqlPollConfig, StartFrom};
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
            field: "recorded_at".into(),
            start_from: Some(StartFrom::Oldest),
            lag_secs,
        }
    }

    fn statements(poll: &SqlPollConfig) -> Result<Statements> {
        Ok(Statements::new(&Plan::build(poll, "schema")?))
    }

    fn build(poll: SqlPollConfig, ack_mode: Option<AckMode>) -> Result<Box<dyn InputSource>> {
        let mut pipelines = HashMap::new();
        let (events, _rx) = tokio::sync::broadcast::channel(4);
        let connections: Connections = [(
            "local-postgres".to_string(),
            ConnectionKind::Postgres(PostgresConnection {
                host: "localhost".into(),
                database: "kayak".into(),
                user: "kayak".into(),
                password: "hunter2".into(),
                port: None,
            }),
        )]
        .into_iter()
        .collect();
        let mut ctx = BuildCtx::new(&mut pipelines, "p".to_string(), events)
            .with_connections(Arc::new(connections));
        ctx.ack_mode = ack_mode;
        PostgresInputConfig {
            connection: "local-postgres".into(),
            poll,
        }
        .build(&mut ctx)
    }

    #[test]
    fn a_snapshot_is_the_whole_relation_as_json() -> Result<()> {
        let sql = statements(&poll(PollMode::Snapshot))?;
        assert_eq!(
            sql.snapshot(),
            r#"SELECT row_to_json(q)::text FROM (SELECT * FROM "readings") AS q"#
        );
        assert!(sql.probe().is_err(), "a snapshot has no cursor to probe");
        Ok(())
    }

    #[test]
    fn the_first_page_reads_from_the_beginning_and_the_next_from_the_watermark() -> Result<()> {
        let sql = statements(&poll(incremental(None)))?;
        assert_eq!(
            sql.first_page(500)?,
            r#"SELECT row_to_json(q)::text, (q."recorded_at")::text FROM (SELECT * FROM "readings") AS q WHERE q."recorded_at" IS NOT NULL ORDER BY q."recorded_at" LIMIT 500"#
        );
        assert_eq!(
            sql.next_page("timestamptz", 500)?,
            r#"SELECT row_to_json(q)::text, (q."recorded_at")::text FROM (SELECT * FROM "readings") AS q WHERE q."recorded_at" > ($1::text)::"timestamptz" ORDER BY q."recorded_at" LIMIT 500"#
        );
        assert_eq!(
            sql.probe()?,
            r#"SELECT q."recorded_at" FROM (SELECT * FROM "readings") AS q LIMIT 0"#
        );
        assert_eq!(
            sql.newest()?,
            r#"SELECT max(q."recorded_at")::text FROM (SELECT * FROM "readings") AS q"#
        );
        Ok(())
    }

    #[test]
    fn a_lag_holds_both_pages_back_from_now() -> Result<()> {
        let sql = statements(&poll(incremental(Some(30))))?;
        for page in [sql.first_page(10)?, sql.next_page("int8", 10)?] {
            assert!(
                page.contains(r#"AND q."recorded_at" <= now() - interval '30 seconds' ORDER BY"#),
                "{page}"
            );
        }
        Ok(())
    }

    #[test]
    fn a_query_is_wrapped_as_the_relation() -> Result<()> {
        let mut config = poll(incremental(None));
        config.table = None;
        config.query = Some("select id, recorded_at from readings where site = 'a';".into());
        let sql = statements(&config)?;
        assert!(
            sql.first_page(10)?
                .starts_with("SELECT row_to_json(q)::text, (q.\"recorded_at\")::text FROM (select id, recorded_at from readings where site = 'a') AS q"),
            "{}",
            sql.first_page(10)?
        );
        Ok(())
    }

    /// Building must not talk to the database — a pipeline that starts is
    /// one whose settings parse, not one whose server happened to be up.
    #[test]
    fn it_builds_against_a_server_that_is_not_running() -> Result<()> {
        build(poll(incremental(None)), None)?;
        build(poll(PollMode::Snapshot), Some(AckMode::OnReceipt))?;
        Ok(())
    }

    #[test]
    fn a_plan_that_cannot_work_fails_the_build() {
        let mut config = poll(PollMode::Snapshot);
        config.interval_secs = 0;
        assert!(build(config, None).is_err());
    }

    /// The watermark moves when rows are handed on, so `on_delivery` would
    /// be a promise the input cannot keep — refused, as every input without
    /// a broker-side ack refuses it.
    #[test]
    fn on_delivery_is_refused() {
        let Err(err) = build(poll(PollMode::Snapshot), Some(AckMode::OnDelivery)) else {
            panic!("a postgres input built with `ack: on_delivery`");
        };
        assert!(format!("{err:#}").contains("postgres"), "{err:#}");
    }
}
