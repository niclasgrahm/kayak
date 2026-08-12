use anyhow::{Context, Result, anyhow, bail};
use kayak_core::columns::ColumnType;
use kayak_core::config::PostgresOutputConfig;
use std::sync::Arc;
use std::time::Instant;
use tokio_postgres::NoTls;
use tokio_postgres::types::ToSql;
use tracing::{error, warn};

use crate::{
    BuildCtx,
    backoff::Gate,
    inputs::MessageBatch,
    outputs::{
        BuildOutput, OutputDestination,
        columns::{ColumnPlan, Identifier, Row, Table},
    },
    secrets::Resolved,
};

const DEFAULT_PORT: u16 = 5432;

/// The columns a mapped table gets for free when the data carries no identity
/// of its own — dropped as soon as `primary_key` says it does, and dropped
/// individually if a mapped column claims the name.
const IMPLICIT_ID: &str = "id";
const IMPLICIT_RECEIVED_AT: &str = "received_at";

/// Postgres' limit on an identifier, which is what an index name is generated
/// against — a longer one is silently truncated by the server, and two indexes
/// that truncate to the same name are a confusing failure.
const MAX_IDENTIFIER_BYTES: usize = 63;

impl BuildOutput for PostgresOutputConfig {
    fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn OutputDestination>> {
        // rejected at build time rather than on first insert: a bad table name
        // should fail the pipeline that owns it, not surface an hour later
        let table = Table::parse(&self.table, "schema")?;
        let layout = Layout::build(&self, &table)?;
        let server = ctx
            .postgres_connection(&self.connection)
            .context("the postgres output cannot be built")?;
        Ok(Box::new(PostgresOutput {
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
            create_table: self.create_table.unwrap_or(true),
            table,
            layout,
            client: None,
            gate: Gate::new(),
        }))
    }
}

/// What the table looks like, which is the one thing that differs between an
/// output with a column mapping and one without.
enum Layout {
    /// One `jsonb` column holding the whole message, plus the table's own id
    /// and arrival time. What this output has always done, and what an output
    /// with no `columns` still does — byte for byte, because a table whose
    /// shape quietly changed would break every consumer of it.
    Payload,
    /// The mapped columns, in the order the config wrote them.
    Mapped(Box<MappedTable>),
}

/// A column mapping, plus the parts of the created table that name columns.
struct MappedTable {
    plan: ColumnPlan,
    primary_key: Vec<Identifier>,
    indexes: Vec<PlannedIndex>,
    /// whether the table gets its own `id`/`received_at`
    implicit_id: bool,
    implicit_received_at: bool,
}

struct PlannedIndex {
    name: Identifier,
    columns: Vec<Identifier>,
    unique: bool,
}

impl Layout {
    fn build(config: &PostgresOutputConfig, table: &Table) -> Result<Self> {
        if config.columns.is_empty() {
            if config.on_extra_fields != kayak_core::columns::ExtraFieldPolicy::Ignore {
                bail!(
                    "the postgres output for '{}' asks about extra fields but maps no columns; every field would be extra",
                    config.table
                );
            }
            if !config.primary_key.is_empty() || !config.indexes.is_empty() {
                bail!(
                    "the postgres output for '{}' names a primary key or an index but maps no columns",
                    config.table
                );
            }
            return Ok(Self::Payload);
        }

        let mut plan = ColumnPlan::build(&config.columns, config.on_extra_fields)
            .with_context(|| format!("the postgres output for '{}' cannot be built", config.table))?;

        let mut primary_key = Vec::with_capacity(config.primary_key.len());
        for name in &config.primary_key {
            if plan.column(name).is_none() {
                bail!("the primary key names '{name}', which is not one of the mapped columns");
            }
            if primary_key
                .iter()
                .any(|existing: &Identifier| existing.as_str() == name)
            {
                bail!("the primary key names '{name}' twice");
            }
            // postgres makes a key column not-null whatever the config says, so
            // the mapping is brought into line here rather than left to fail as
            // a constraint violation on the first message that omits the field
            plan.require_not_null(name)?;
            primary_key.push(Identifier::parse(name, "column name")?);
        }

        let mut indexes = Vec::with_capacity(config.indexes.len());
        for index in &config.indexes {
            if index.columns.is_empty() {
                bail!("an index on '{}' names no columns", config.table);
            }
            let mut columns = Vec::with_capacity(index.columns.len());
            for name in &index.columns {
                if plan.column(name).is_none() {
                    bail!("an index names '{name}', which is not one of the mapped columns");
                }
                columns.push(Identifier::parse(name, "column name")?);
            }
            indexes.push(PlannedIndex {
                name: index_name(table, &columns, index.is_unique())?,
                columns,
                unique: index.is_unique(),
            });
        }

        if config.create_table == Some(false) && (!primary_key.is_empty() || !indexes.is_empty()) {
            // not refused: turning creation off for a table someone else owns
            // is a legitimate thing to do to a config that was creating it, and
            // deleting the key to say so would lose it on the way back
            warn!(
                table = config.table,
                "the postgres output names a primary key or indexes but does not create its table; \
                 they describe a table it is not creating and nothing is applied"
            );
        }

        // dropped as a pair with the key, and individually if a mapped column
        // claims the name: a message with its own `id` should land in it
        let has_key = !primary_key.is_empty();
        let implicit_id = !has_key && plan.column(IMPLICIT_ID).is_none();
        let implicit_received_at = !has_key && plan.column(IMPLICIT_RECEIVED_AT).is_none();

        Ok(Self::Mapped(Box::new(MappedTable {
            plan,
            primary_key,
            indexes,
            implicit_id,
            implicit_received_at,
        })))
    }
}

/// The generated name of an index, which has to be one identifier and has to
/// differ from any other index on the same table.
fn index_name(table: &Table, columns: &[Identifier], unique: bool) -> Result<Identifier> {
    let joined = columns
        .iter()
        .map(Identifier::as_str)
        .collect::<Vec<_>>()
        .join("_");
    let suffix = if unique { "key" } else { "idx" };
    let mut name = format!("{}_{joined}_{suffix}", table.bare());
    if name.len() > MAX_IDENTIFIER_BYTES {
        // truncating the front would drop the table, which is what makes two
        // indexes on different tables distinguishable in a `\di`
        name.truncate(MAX_IDENTIFIER_BYTES);
    }
    Identifier::parse(&name, "index name")
}

/// The SQL type a logical column type is created as.
fn sql_type(column_type: ColumnType) -> &'static str {
    match column_type {
        ColumnType::Text => "TEXT",
        ColumnType::Integer => "INTEGER",
        ColumnType::Bigint => "BIGINT",
        ColumnType::Float => "DOUBLE PRECISION",
        ColumnType::Decimal => "NUMERIC",
        ColumnType::Boolean => "BOOLEAN",
        ColumnType::Timestamp => "TIMESTAMPTZ",
        ColumnType::Date => "DATE",
        ColumnType::Uuid => "UUID",
        ColumnType::Json => "JSONB",
    }
}

/// The placeholder a column's value is written as.
///
/// Every mapped value is bound as **text** and cast in the statement, which is
/// why this is per-column rather than a bare `$n`. It is what keeps a decimal
/// exact — the digits go across as they were written — and it hands the parsing
/// of a timestamp or a uuid to the server, whose error about it is better than
/// anything reimplemented here. The value has already been checked against the
/// column's type, so the cast is a conversion and not a coercion.
fn placeholder(index: usize, column_type: ColumnType) -> String {
    match column_type {
        ColumnType::Text => format!("${index}::text"),
        other => format!("${index}::text::{}", sql_type(other)),
    }
}

impl MappedTable {
    fn create_table_sql(&self, table: &Table) -> String {
        let mut definitions: Vec<String> = Vec::new();
        if self.implicit_id {
            definitions.push(format!("\"{IMPLICIT_ID}\" BIGSERIAL PRIMARY KEY"));
        }
        if self.implicit_received_at {
            definitions.push(format!(
                "\"{IMPLICIT_RECEIVED_AT}\" TIMESTAMPTZ NOT NULL DEFAULT now()"
            ));
        }
        for column in self.plan.columns() {
            let null = if column.nullable { "" } else { " NOT NULL" };
            definitions.push(format!(
                "{} {}{null}",
                column.name.quoted(),
                sql_type(column.column_type)
            ));
        }
        if !self.primary_key.is_empty() {
            definitions.push(format!(
                "PRIMARY KEY ({})",
                self.primary_key
                    .iter()
                    .map(Identifier::quoted)
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        format!(
            "CREATE TABLE IF NOT EXISTS {table} ({definitions})",
            table = table.quoted(),
            definitions = definitions.join(", ")
        )
    }

    fn index_sql(&self, table: &Table) -> Vec<String> {
        self.indexes
            .iter()
            .map(|index| {
                format!(
                    "CREATE {unique}INDEX IF NOT EXISTS {name} ON {table} ({columns})",
                    unique = if index.unique { "UNIQUE " } else { "" },
                    name = index.name.quoted(),
                    table = table.quoted(),
                    columns = index
                        .columns
                        .iter()
                        .map(Identifier::quoted)
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            })
            .collect()
    }

    fn insert_sql(&self, table: &Table) -> String {
        let names = self
            .plan
            .columns()
            .iter()
            .map(|column| column.name.quoted())
            .collect::<Vec<_>>()
            .join(", ");
        let values = self
            .plan
            .columns()
            .iter()
            .enumerate()
            .map(|(i, column)| placeholder(i + 1, column.column_type))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "INSERT INTO {table} ({names}) VALUES ({values})",
            table = table.quoted()
        )
    }
}

impl Layout {
    fn create_table_sql(&self, table: &Table) -> String {
        match self {
            Self::Payload => format!(
                "CREATE TABLE IF NOT EXISTS {table} (\
                 id BIGSERIAL PRIMARY KEY, \
                 received_at TIMESTAMPTZ NOT NULL DEFAULT now(), \
                 payload JSONB NOT NULL)",
                table = table.quoted()
            ),
            Self::Mapped(mapped) => mapped.create_table_sql(table),
        }
    }

    fn index_sql(&self, table: &Table) -> Vec<String> {
        match self {
            Self::Payload => Vec::new(),
            Self::Mapped(mapped) => mapped.index_sql(table),
        }
    }

    fn insert_sql(&self, table: &Table) -> String {
        match self {
            Self::Payload => format!(
                "INSERT INTO {table} (payload) VALUES ($1)",
                table = table.quoted()
            ),
            Self::Mapped(mapped) => mapped.insert_sql(table),
        }
    }
}

pub struct PostgresOutput {
    host: String,
    port: u16,
    database: String,
    user: String,
    password: Resolved,
    table: Table,
    layout: Layout,
    create_table: bool,
    client: Option<tokio_postgres::Client>,
    /// Paces reconnect attempts after the connection is lost — see `emit`.
    /// `init` never consults it: a pipeline that can't reach its database at
    /// startup still fails to build, same as always.
    gate: Gate,
}

impl PostgresOutput {
    /// Built field by field rather than as a url, so a password containing `@`
    /// or `/` needs no escaping — and so the password never has to be
    /// concatenated into a string that might get logged.
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

    /// How this connection is described in an error. Everything but the
    /// password, which `Resolved` would not print anyway.
    fn describe(&self) -> String {
        format!(
            "postgres://{user}@{host}:{port}/{database}",
            user = self.user,
            host = self.host,
            port = self.port,
            database = self.database
        )
    }

    /// Connects and spawns the background task that drives the socket.
    /// Shared by `init` and by `emit`'s reconnect — DDL only runs from
    /// `init`, since a reconnect mid-run is "the same table again", not a
    /// fresh pipeline start.
    async fn connect_client(&self) -> Result<tokio_postgres::Client> {
        let (client, connection) = self
            .connection_config()
            .connect(NoTls)
            .await
            .with_context(|| format!("failed to connect to {}", self.describe()))?;

        // the connection drives the socket and has to be polled for the client
        // to work at all; it resolves when the client is dropped or the server
        // goes away, and the error it returns then surfaces on the next insert
        let described = self.describe();
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                error!("postgres connection to {described} closed: {e:?}");
            }
        });
        Ok(client)
    }
}

#[async_trait::async_trait]
impl OutputDestination for PostgresOutput {
    async fn emit(&mut self, message_batch: Arc<MessageBatch>) -> Result<()> {
        if self.client.is_none() {
            // Gated the same way the nats and kafka outputs are: a reconnect
            // is only attempted once the backoff window has passed, so a
            // downed database gets one attempt every few seconds rather than
            // one dial per batch.
            let now = Instant::now();
            if !self.gate.ready(now) {
                bail!(
                    "postgres output at {} is still unreachable; not retrying yet",
                    self.describe()
                );
            }
            match self.connect_client().await {
                Ok(client) => {
                    self.gate.record_success();
                    self.client = Some(client);
                }
                Err(e) => {
                    self.gate.record_failure(now);
                    return Err(e);
                }
            }
        }
        // as in the nats output: doing nothing here would look like the rows
        // were written
        let client = self
            .client
            .as_ref()
            .ok_or_else(|| anyhow!("postgres output is not connected; init() was not called"))?;

        let statement = match client.prepare(&self.layout.insert_sql(&self.table)).await {
            Ok(statement) => statement,
            Err(e) => {
                self.client = None;
                self.gate.record_failure(Instant::now());
                return Err(e)
                    .with_context(|| format!("failed to prepare the insert for {}", self.describe()));
            }
        };

        for msg in message_batch.iter() {
            let result = match &self.layout {
                Layout::Payload => client.execute(&statement, &[&**msg]).await,
                Layout::Mapped(mapped) => {
                    let Row::Values(values) = mapped.plan.row(msg).with_context(|| {
                        format!("failed to map a message onto {}", self.table.quoted())
                    })?
                    else {
                        continue;
                    };
                    let params: Vec<&(dyn ToSql + Sync)> = values
                        .iter()
                        .map(|value| value as &(dyn ToSql + Sync))
                        .collect();
                    client.execute(&statement, &params).await
                }
            };
            if let Err(e) = result {
                self.client = None;
                self.gate.record_failure(Instant::now());
                return Err(e).with_context(|| format!("failed to insert a row into {}", self.describe()));
            }
        }
        self.gate.record_success();
        Ok(())
    }

    async fn init(&mut self) -> Result<()> {
        let client = self.connect_client().await?;

        // creation never *alters*: a table whose shape has moved on fails the
        // insert with the server's own error rather than being migrated from a
        // config file, which is a much bigger promise than "create it if it
        // isn't there"
        if self.create_table {
            client
                .execute(&self.layout.create_table_sql(&self.table), &[])
                .await
                .with_context(|| format!("failed to create the table in {}", self.describe()))?;
            for sql in self.layout.index_sql(&self.table) {
                client.execute(&sql, &[]).await.with_context(|| {
                    format!("failed to create an index in {}", self.describe())
                })?;
            }
        }

        self.client = Some(client);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{Layout, Table};
    use kayak_core::columns::{
        ColumnMapping, ColumnType, ExtraFieldPolicy, MissingColumnPolicy, TableIndex,
    };
    use kayak_core::config::PostgresOutputConfig;

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

    fn config(columns: Vec<ColumnMapping>) -> PostgresOutputConfig {
        PostgresOutputConfig {
            connection: "local-postgres".into(),
            table: "readings".into(),
            columns,
            create_table: None,
            primary_key: Vec::new(),
            indexes: Vec::new(),
            on_extra_fields: ExtraFieldPolicy::Ignore,
        }
    }

    /// The whole build, through `BuildOutput` — what `Layout::build` alone
    /// cannot say is that the config the user writes reaches it.
    fn build(config: PostgresOutputConfig) -> anyhow::Result<Box<dyn crate::outputs::OutputDestination>> {
        use crate::outputs::BuildOutput;
        let mut pipelines = std::collections::HashMap::new();
        let (events, _) = tokio::sync::broadcast::channel(4);
        let connections: kayak_core::connections::Connections = [(
            "local-postgres".to_string(),
            kayak_core::connections::ConnectionKind::Postgres(
                kayak_core::connections::PostgresConnection {
                    host: "localhost".to_string(),
                    database: "kayak".to_string(),
                    user: "kayak".to_string(),
                    password: "kayak".into(),
                    port: None,
                },
            ),
        )]
        .into_iter()
        .collect();
        let mut ctx = crate::BuildCtx::new(&mut pipelines, "p".to_string(), events)
            .with_connections(std::sync::Arc::new(connections));
        config.build(&mut ctx)
    }

    fn layout(config: &PostgresOutputConfig) -> anyhow::Result<(Table, Layout)> {
        let table = Table::parse(&config.table, "schema")?;
        let layout = Layout::build(config, &table)?;
        Ok((table, layout))
    }

    #[test]
    fn a_plain_table_name_is_quoted() -> anyhow::Result<()> {
        let (table, layout) = layout(&config(Vec::new()))?;
        assert_eq!(
            layout.insert_sql(&table),
            r#"INSERT INTO "readings" (payload) VALUES ($1)"#
        );
        assert!(
            layout
                .create_table_sql(&table)
                .starts_with(r#"CREATE TABLE IF NOT EXISTS "readings" ("#),
            "{}",
            layout.create_table_sql(&table)
        );
        Ok(())
    }

    #[test]
    fn a_schema_qualified_name_quotes_each_part_separately() -> anyhow::Result<()> {
        let mut config = config(Vec::new());
        config.table = "analytics.readings".into();
        let (table, layout) = layout(&config)?;
        assert_eq!(
            layout.insert_sql(&table),
            r#"INSERT INTO "analytics"."readings" (payload) VALUES ($1)"#
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
            "readings--",
            "a.b.c",
            "",
            "analytics.",
            "1readings",
            "readings'",
        ] {
            assert!(
                Table::parse(name, "schema").is_err(),
                "'{name}' should have been rejected"
            );
        }
    }

    /// The compatibility promise: an output with no `columns` writes the table
    /// it always wrote, and binds nothing but the message.
    #[test]
    fn without_columns_the_insert_only_ever_binds_the_payload() -> anyhow::Result<()> {
        let (table, layout) = layout(&config(Vec::new()))?;
        let sql = layout.insert_sql(&table);
        assert!(sql.contains("(payload)"), "{sql}");
        assert_eq!(sql.matches('$').count(), 1, "{sql}");
        assert!(layout.index_sql(&table).is_empty());
        Ok(())
    }

    #[test]
    fn mapped_columns_become_the_created_table() -> anyhow::Result<()> {
        let (table, layout) = layout(&config(vec![
            ColumnMapping {
                nullable: Some(false),
                ..column("sensor_id", ColumnType::Text)
            },
            column("temperature", ColumnType::Float),
            column("recorded_at", ColumnType::Timestamp),
        ]))?;
        assert_eq!(
            layout.create_table_sql(&table),
            r#"CREATE TABLE IF NOT EXISTS "readings" ("id" BIGSERIAL PRIMARY KEY, "received_at" TIMESTAMPTZ NOT NULL DEFAULT now(), "sensor_id" TEXT NOT NULL, "temperature" DOUBLE PRECISION, "recorded_at" TIMESTAMPTZ)"#
        );
        Ok(())
    }

    /// Every mapped value is bound as text and cast in the statement — that is
    /// what keeps a decimal exact and hands timestamp parsing to the server.
    #[test]
    fn every_mapped_column_is_bound_as_text_and_cast() -> anyhow::Result<()> {
        let (table, layout) = layout(&config(vec![
            column("a", ColumnType::Text),
            column("b", ColumnType::Decimal),
            column("c", ColumnType::Json),
        ]))?;
        assert_eq!(
            layout.insert_sql(&table),
            r#"INSERT INTO "readings" ("a", "b", "c") VALUES ($1::text, $2::text::NUMERIC, $3::text::JSONB)"#
        );
        Ok(())
    }

    /// Naming a key says the data carries its own identity, so the surrogate
    /// one the table would otherwise get is dropped with it.
    #[test]
    fn a_primary_key_replaces_the_tables_own_id() -> anyhow::Result<()> {
        let mut config = config(vec![
            column("sensor_id", ColumnType::Text),
            column("recorded_at", ColumnType::Timestamp),
        ]);
        config.primary_key = vec!["sensor_id".into(), "recorded_at".into()];
        let (table, layout) = layout(&config)?;
        let sql = layout.create_table_sql(&table);
        assert!(!sql.contains("BIGSERIAL"), "{sql}");
        assert!(!sql.contains("received_at"), "{sql}");
        assert!(
            sql.ends_with(r#"PRIMARY KEY ("sensor_id", "recorded_at"))"#),
            "{sql}"
        );
        // postgres makes a key column not-null whatever the config said, so the
        // mapping is brought into line rather than left to fail per message
        assert!(sql.contains(r#""sensor_id" TEXT NOT NULL"#), "{sql}");
        Ok(())
    }

    /// A message with its own `id` should land in it rather than fight the
    /// surrogate column for the name.
    #[test]
    fn a_mapped_column_may_claim_the_implicit_names() -> anyhow::Result<()> {
        let (table, layout) = layout(&config(vec![column("id", ColumnType::Uuid)]))?;
        let sql = layout.create_table_sql(&table);
        assert!(sql.contains(r#""id" UUID"#), "{sql}");
        assert!(!sql.contains("BIGSERIAL"), "{sql}");
        assert!(sql.contains("received_at"), "{sql}");
        Ok(())
    }

    #[test]
    fn an_index_is_named_after_its_table_and_columns() -> anyhow::Result<()> {
        let mut config = config(vec![
            column("sensor_id", ColumnType::Text),
            column("recorded_at", ColumnType::Timestamp),
        ]);
        config.indexes = vec![
            TableIndex {
                columns: vec!["recorded_at".into()],
                unique: None,
            },
            TableIndex {
                columns: vec!["sensor_id".into(), "recorded_at".into()],
                unique: Some(true),
            },
        ];
        let (table, layout) = layout(&config)?;
        assert_eq!(
            layout.index_sql(&table),
            vec![
                r#"CREATE INDEX IF NOT EXISTS "readings_recorded_at_idx" ON "readings" ("recorded_at")"#.to_string(),
                r#"CREATE UNIQUE INDEX IF NOT EXISTS "readings_sensor_id_recorded_at_key" ON "readings" ("sensor_id", "recorded_at")"#.to_string(),
            ]
        );
        Ok(())
    }

    /// A table part that names nothing is a config that does not describe what
    /// the user thinks it does, so it fails the pipeline that owns it.
    #[test]
    fn a_key_or_index_naming_an_unmapped_column_is_refused() {
        let mut with_key = config(vec![column("a", ColumnType::Text)]);
        with_key.primary_key = vec!["b".into()];
        assert!(layout(&with_key).is_err());

        let mut with_index = config(vec![column("a", ColumnType::Text)]);
        with_index.indexes = vec![TableIndex {
            columns: vec!["b".into()],
            unique: None,
        }];
        assert!(layout(&with_index).is_err());

        let mut duplicated = config(vec![column("a", ColumnType::Text)]);
        duplicated.primary_key = vec!["a".into(), "a".into()];
        assert!(layout(&duplicated).is_err());
    }

    /// Without columns there is nothing for a key, an index or an extra-field
    /// check to be about — a config that says otherwise means something the
    /// output cannot do.
    #[test]
    fn table_options_without_columns_are_refused() {
        let mut with_key = config(Vec::new());
        with_key.primary_key = vec!["a".into()];
        assert!(layout(&with_key).is_err());

        let mut strict = config(Vec::new());
        strict.on_extra_fields = ExtraFieldPolicy::Error;
        assert!(layout(&strict).is_err());
    }

    /// Building must not talk to the database — a pipeline that starts is one
    /// whose settings parse, not one whose server happened to be up — and the
    /// mapping is checked on the way through.
    #[test]
    fn it_builds_against_a_server_that_is_not_running() -> anyhow::Result<()> {
        build(config(vec![column("sensor_id", ColumnType::Text)]))?;
        Ok(())
    }

    #[test]
    fn a_mapping_that_cannot_work_fails_the_build() {
        let mut config = config(vec![column("a", ColumnType::Text)]);
        config.primary_key = vec!["b".into()];
        assert!(build(config).is_err());
    }

    /// The mapping's own build-time refusals reach the output — the message
    /// names the table, so a config with several postgres outputs says which.
    #[test]
    fn a_contradictory_column_fails_the_output() {
        let contradiction = config(vec![ColumnMapping {
            nullable: Some(false),
            on_missing: Some(MissingColumnPolicy::Null),
            ..column("a", ColumnType::Text)
        }]);
        let error = format!("{:#}", layout(&contradiction).err().unwrap_or_else(|| {
            panic!("the contradiction should have been refused");
        }));
        assert!(error.contains("readings"), "{error}");
    }
}
