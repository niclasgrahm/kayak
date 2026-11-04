//! Inserts batches into a Snowflake table through the SQL API.
//!
//! The third database consumer of [`crate::outputs::columns`]: the mapping is
//! reused whole, and what is written here is only the DDL and how a batch is
//! carried. [`crate::snowflake::Session`] is the wire. Three things differ from
//! the other database outputs, and each is Snowflake being itself:
//!
//! - **A batch is one bound JSON document, flattened by the statement.** `VALUES`
//!   takes no `PARSE_JSON`, so a `VARIANT` cannot be inserted through it, and
//!   array binding is not documented for this API. So the rows go across as a
//!   single text bind, `[["a","1.5"],["b",null]]`, and `LATERAL FLATTEN` turns
//!   them back into rows. This is the same bargain as `$n::text::NUMERIC` on
//!   postgres: every value is text the plan has checked, and the statement casts
//!   it, which keeps a decimal's digits. A batch is one statement because every
//!   insert makes micro-partitions and costs warehouse time, and a row-at-a-time
//!   insert would be paying for both per message.
//! - **Names fold to upper case** ([`crate::snowflake::fold`]), and a field the
//!   mapping reads stays spelled as the message spells it.
//! - **No key of any kind.** A primary key is not enforced here, so declaring one
//!   would claim something false, and clustering costs credits and belongs to
//!   whoever owns the table. The table is its columns and `RECEIVED_AT`.

use anyhow::{Context, Result, anyhow, bail};
use kayak_core::columns::{ColumnMapping, ColumnType, ExtraFieldPolicy};
use kayak_core::config::SnowflakeOutputConfig;
use std::sync::Arc;
use std::time::Instant;

use crate::{
    BuildCtx,
    backoff::Gate,
    inputs::MessageBatch,
    outputs::{
        BuildOutput, OutputDestination,
        columns::{ColumnPlan, Row, Table},
    },
    snowflake::{Session, fold},
};

/// The arrival time a table gets for free, in the case a name folds to.
const IMPLICIT_RECEIVED_AT: &str = "RECEIVED_AT";
/// One bound document, at most; a larger batch becomes several statements. Well
/// under the 16 MB a `VARCHAR` bind and a `VARIANT` can both be relied on to
/// hold, since the second measures itself after parsing rather than before.
const MAX_DOCUMENT_BYTES: usize = 8 * 1024 * 1024;

impl BuildOutput for SnowflakeOutputConfig {
    fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn OutputDestination>> {
        // rejected at build time rather than on the first insert: a bad table
        // name should fail the pipeline that owns it, not surface an hour later
        let table = Table::parse(&fold(&self.table), "schema")?;
        let layout = Layout::build(&self)?;
        let session = Session::build(ctx, &self.connection)
            .context("the snowflake output cannot be built")?;
        Ok(Box::new(SnowflakeOutput {
            session,
            create_table: self.create_table.unwrap_or(true),
            table,
            layout,
            gate: Gate::new(),
        }))
    }
}

/// A column as Snowflake will know it: the name folded, and the field it reads
/// written out first, because that was the column's own name as the config
/// wrote it and the message still spells it that way.
fn folded(column: &ColumnMapping) -> ColumnMapping {
    ColumnMapping {
        name: fold(&column.name),
        field: (!column.message).then(|| column.source_field().to_string()),
        ..column.clone()
    }
}

/// What the table looks like, which is the one thing that differs between an
/// output with a column mapping and one without.
enum Layout {
    /// One `VARIANT` holding each message, plus its arrival time.
    Payload,
    /// The mapped columns, in the order the config wrote them.
    Mapped {
        plan: ColumnPlan,
        /// whether the table gets a `RECEIVED_AT` of its own
        implicit_received_at: bool,
    },
}

/// The type a logical column is created as. `uuid` is a `VARCHAR`, since there
/// is no such type here, and so is not checked by the server the way postgres
/// and clickhouse check one.
fn sql_type(column_type: ColumnType) -> &'static str {
    match column_type {
        ColumnType::Text => "VARCHAR",
        ColumnType::Uuid => "VARCHAR(36)",
        ColumnType::Integer => "INTEGER",
        ColumnType::Bigint => "BIGINT",
        ColumnType::Float => "FLOAT",
        // the same width the clickhouse output picks, for the same reason
        ColumnType::Decimal => "NUMBER(38, 9)",
        ColumnType::Boolean => "BOOLEAN",
        ColumnType::Timestamp => "TIMESTAMP_TZ",
        ColumnType::Date => "DATE",
        ColumnType::Json => "VARIANT",
    }
}

/// The expression reading position `index` of a row back out of the document.
///
/// Every element is a JSON string or a JSON null, and `::VARCHAR` takes the
/// string's contents and turns the null into SQL's. `json` is parsed rather than
/// cast, because a cast would store a string that holds JSON rather than JSON.
fn read_expression(index: usize, column_type: ColumnType) -> String {
    let text = format!("f.value[{index}]::VARCHAR");
    match column_type {
        ColumnType::Text | ColumnType::Uuid => text,
        ColumnType::Json => format!("PARSE_JSON({text})"),
        ColumnType::Integer
        | ColumnType::Bigint
        | ColumnType::Float
        | ColumnType::Decimal
        | ColumnType::Boolean
        | ColumnType::Timestamp
        | ColumnType::Date => format!("{text}::{}", sql_type(column_type)),
    }
}

impl Layout {
    fn build(config: &SnowflakeOutputConfig) -> Result<Self> {
        if config.columns.is_empty() {
            if config.on_extra_fields != ExtraFieldPolicy::Ignore {
                bail!(
                    "the snowflake output for '{}' asks about extra fields but maps no columns; \
                     every field would be extra",
                    config.table
                );
            }
            return Ok(Self::Payload);
        }
        let columns: Vec<ColumnMapping> = config.columns.iter().map(folded).collect();
        let plan = ColumnPlan::build(&columns, config.on_extra_fields).with_context(|| {
            format!(
                "the snowflake output for '{}' cannot be built",
                config.table
            )
        })?;
        let implicit_received_at = plan.column(IMPLICIT_RECEIVED_AT).is_none();
        Ok(Self::Mapped {
            plan,
            implicit_received_at,
        })
    }

    fn create_table_sql(&self, table: &Table) -> String {
        let received_at =
            format!("\"{IMPLICIT_RECEIVED_AT}\" TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()");
        let definitions = match self {
            Self::Payload => vec![received_at, "\"PAYLOAD\" VARIANT".to_string()],
            Self::Mapped {
                plan,
                implicit_received_at,
            } => {
                let mut definitions = Vec::with_capacity(plan.columns().len() + 1);
                if *implicit_received_at {
                    definitions.push(received_at);
                }
                for column in plan.columns() {
                    definitions.push(format!(
                        "{} {}{}",
                        column.name.quoted(),
                        sql_type(column.column_type),
                        if column.nullable { "" } else { " NOT NULL" }
                    ));
                }
                definitions
            }
        };
        format!(
            "CREATE TABLE IF NOT EXISTS {} ({})",
            table.quoted(),
            definitions.join(", ")
        )
    }

    /// The one statement every batch runs, with the document as its only bind.
    fn insert_sql(&self, table: &Table) -> String {
        let (names, reads): (Vec<String>, Vec<String>) = match self {
            Self::Payload => (
                vec!["\"PAYLOAD\"".to_string()],
                vec![read_expression(0, ColumnType::Json)],
            ),
            Self::Mapped { plan, .. } => plan
                .columns()
                .iter()
                .enumerate()
                .map(|(index, column)| {
                    (
                        column.name.quoted(),
                        read_expression(index, column.column_type),
                    )
                })
                .unzip(),
        };
        format!(
            "INSERT INTO {table} ({names}) SELECT {reads} \
             FROM (SELECT PARSE_JSON(?) AS batch) AS d, LATERAL FLATTEN(INPUT => d.batch) AS f",
            table = table.quoted(),
            names = names.join(", "),
            reads = reads.join(", ")
        )
    }

    /// The batch as JSON documents to bind, each holding its rows as arrays of
    /// text. Nothing at all when the mapping skipped every message.
    fn documents(&self, message_batch: &MessageBatch) -> Result<Vec<String>> {
        let mut documents = Vec::new();
        let mut current = String::new();
        for message in message_batch {
            let values = match self {
                Self::Payload => vec![Some(message.to_string())],
                Self::Mapped { plan, .. } => match plan.row(message)? {
                    Row::Values(values) => values,
                    Row::Skipped => continue,
                },
            };
            let row = serde_json::to_string(&values).context("failed to render a row")?;
            // a row over the limit alone still goes, by itself, for the server
            // to refuse in its own words rather than be dropped here
            if !current.is_empty() && current.len() + row.len() + 2 > MAX_DOCUMENT_BYTES {
                current.push(']');
                documents.push(std::mem::take(&mut current));
            }
            current.push(if current.is_empty() { '[' } else { ',' });
            current.push_str(&row);
        }
        if !current.is_empty() {
            current.push(']');
            documents.push(current);
        }
        Ok(documents)
    }
}

pub struct SnowflakeOutput {
    session: Session,
    table: Table,
    layout: Layout,
    create_table: bool,
    /// Paces attempts after an insert has failed for good. The session has
    /// already retried what was worth retrying by then, under one request id;
    /// what this guards is a warehouse that is really gone, which should get one
    /// attempt every few seconds rather than a round of them per batch.
    gate: Gate,
}

#[async_trait::async_trait]
impl OutputDestination for SnowflakeOutput {
    async fn init(&mut self) -> Result<()> {
        if self.create_table {
            // creation never *alters*: a table whose shape has moved on fails
            // the insert with the server's own error rather than being migrated
            // from a config file
            let sql = self.layout.create_table_sql(&self.table);
            self.session
                .execute(&sql, &[])
                .await
                .context("failed to create the table")?;
        } else {
            // the same news at the same moment: without this, a wrong key would
            // first be heard about on the first batch
            self.session
                .execute("SELECT 1", &[])
                .await
                .context("failed to check the connection")?;
        }
        Ok(())
    }

    async fn emit(&mut self, message_batch: Arc<MessageBatch>) -> Result<()> {
        let documents = self
            .layout
            .documents(&message_batch)
            .with_context(|| format!("failed to map a message onto {}", self.table.quoted()))?;
        // every message was skipped by the mapping; an insert with no rows would
        // be warehouse time for nothing
        if documents.is_empty() {
            return Ok(());
        }

        let now = Instant::now();
        if !self.gate.ready(now) {
            return Err(anyhow!(
                "{} is still unreachable; not retrying yet",
                self.session.describe()
            ));
        }
        let sql = self.layout.insert_sql(&self.table);
        for document in &documents {
            if let Err(e) = self.session.execute(&sql, &[document]).await {
                self.gate.record_failure(now);
                return Err(e).with_context(|| {
                    format!("failed to insert a batch into {}", self.table.quoted())
                });
            }
        }
        self.gate.record_success();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snowflake::fake;
    use kayak_core::columns::{ColumnMapping, MissingColumnPolicy};
    use serde_json::{Value, json};

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

    fn config(columns: Vec<ColumnMapping>) -> SnowflakeOutputConfig {
        SnowflakeOutputConfig {
            connection: "warehouse".into(),
            table: "readings".into(),
            columns,
            create_table: None,
            on_extra_fields: ExtraFieldPolicy::Ignore,
        }
    }

    fn table() -> Result<Table> {
        Table::parse("READINGS", "schema")
    }

    fn batch(messages: Vec<Value>) -> MessageBatch {
        messages.into_iter().map(Arc::new).collect()
    }

    fn built(config: SnowflakeOutputConfig, url: &str) -> Result<Box<dyn OutputDestination>> {
        fake::with_ctx(fake::connection(url), |ctx| config.build(ctx))
    }

    #[test]
    fn without_columns_each_message_is_a_variant() -> Result<()> {
        let layout = Layout::build(&config(Vec::new()))?;
        assert_eq!(
            layout.create_table_sql(&table()?),
            r#"CREATE TABLE IF NOT EXISTS "READINGS" ("RECEIVED_AT" TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(), "PAYLOAD" VARIANT)"#
        );
        assert_eq!(
            layout.insert_sql(&table()?),
            r#"INSERT INTO "READINGS" ("PAYLOAD") SELECT PARSE_JSON(f.value[0]::VARCHAR) FROM (SELECT PARSE_JSON(?) AS batch) AS d, LATERAL FLATTEN(INPUT => d.batch) AS f"#
        );
        // ...and the message crosses as a JSON string holding its own text, so
        // that the statement's `PARSE_JSON` is what makes it a value again
        let documents = layout.documents(&batch(vec![json!({"a": 1})]))?;
        assert_eq!(documents, vec![r#"[["{\"a\":1}"]]"#.to_string()]);
        Ok(())
    }

    #[test]
    fn every_column_type_has_a_ddl_type_and_a_read() -> Result<()> {
        let layout = Layout::build(&config(vec![
            ColumnMapping {
                nullable: Some(false),
                on_missing: Some(MissingColumnPolicy::Error),
                ..column("a", ColumnType::Text)
            },
            column("b", ColumnType::Integer),
            column("c", ColumnType::Bigint),
            column("d", ColumnType::Float),
            column("e", ColumnType::Decimal),
            column("f", ColumnType::Boolean),
            column("g", ColumnType::Timestamp),
            column("h", ColumnType::Date),
            column("i", ColumnType::Uuid),
            column("j", ColumnType::Json),
        ]))?;
        assert_eq!(
            layout.create_table_sql(&table()?),
            "CREATE TABLE IF NOT EXISTS \"READINGS\" (\
             \"RECEIVED_AT\" TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP(), \
             \"A\" VARCHAR NOT NULL, \"B\" INTEGER, \"C\" BIGINT, \"D\" FLOAT, \
             \"E\" NUMBER(38, 9), \"F\" BOOLEAN, \"G\" TIMESTAMP_TZ, \"H\" DATE, \
             \"I\" VARCHAR(36), \"J\" VARIANT)"
        );
        assert_eq!(
            layout.insert_sql(&table()?),
            "INSERT INTO \"READINGS\" (\"A\", \"B\", \"C\", \"D\", \"E\", \"F\", \"G\", \"H\", \"I\", \"J\") \
             SELECT f.value[0]::VARCHAR, f.value[1]::VARCHAR::INTEGER, f.value[2]::VARCHAR::BIGINT, \
             f.value[3]::VARCHAR::FLOAT, f.value[4]::VARCHAR::NUMBER(38, 9), \
             f.value[5]::VARCHAR::BOOLEAN, f.value[6]::VARCHAR::TIMESTAMP_TZ, \
             f.value[7]::VARCHAR::DATE, f.value[8]::VARCHAR, PARSE_JSON(f.value[9]::VARCHAR) \
             FROM (SELECT PARSE_JSON(?) AS batch) AS d, LATERAL FLATTEN(INPUT => d.batch) AS f"
        );
        Ok(())
    }

    /// Every value crosses as text so its digits survive, and a missing one as
    /// a JSON null, which the statement's `::VARCHAR` turns into SQL's.
    #[test]
    fn a_document_is_text_all_the_way_down() -> Result<()> {
        let layout = Layout::build(&config(vec![
            column("sensor", ColumnType::Text),
            column("big", ColumnType::Bigint),
            column("value", ColumnType::Float),
            column("ok", ColumnType::Boolean),
            column("raw", ColumnType::Json),
            column("missing", ColumnType::Text),
        ]))?;
        let documents = layout.documents(&batch(vec![json!({
            "sensor": "a\"b",
            "big": 9_007_199_254_740_993_i64,
            "value": 1.5,
            "ok": true,
            "raw": {"nested": [1]}
        })]))?;
        let parsed: Value = serde_json::from_str(&documents[0])?;
        assert_eq!(
            parsed,
            json!([[
                "a\"b",
                "9007199254740993",
                "1.5",
                "true",
                "{\"nested\":[1]}",
                null
            ]])
        );
        Ok(())
    }

    /// The config says `sensor`, the table says `SENSOR`, the message still says
    /// `sensor`: folding the name must not fold what it reads.
    #[test]
    fn a_name_folds_and_the_field_it_reads_does_not() -> Result<()> {
        let mut config = config(vec![
            column("sensor", ColumnType::Text),
            ColumnMapping {
                field: Some("reading.temp".into()),
                ..column("temperature", ColumnType::Float)
            },
            ColumnMapping {
                message: true,
                ..column("raw", ColumnType::Json)
            },
        ]);
        config.on_extra_fields = ExtraFieldPolicy::Error;
        let layout = Layout::build(&config)?;
        let sql = layout.create_table_sql(&table()?);
        assert!(sql.contains(r#""SENSOR" VARCHAR"#), "{sql}");
        assert!(sql.contains(r#""TEMPERATURE" FLOAT"#), "{sql}");
        let message = json!({"sensor": "a", "reading": {"temp": 2.5}});
        let documents = layout.documents(&batch(vec![message.clone()]))?;
        let parsed: Value = serde_json::from_str(&documents[0])?;
        assert_eq!(parsed[0][0], "a");
        assert_eq!(parsed[0][1], "2.5");
        assert_eq!(parsed[0][2], message.to_string());
        Ok(())
    }

    #[test]
    fn a_mapped_column_may_claim_the_implicit_name() -> Result<()> {
        let layout = Layout::build(&config(vec![column("received_at", ColumnType::Timestamp)]))?;
        let sql = layout.create_table_sql(&table()?);
        assert!(!sql.contains("CURRENT_TIMESTAMP"), "{sql}");
        assert_eq!(sql.matches("RECEIVED_AT").count(), 1, "{sql}");
        Ok(())
    }

    #[test]
    fn a_skipped_message_is_no_row_and_an_all_skipped_batch_is_nothing() -> Result<()> {
        let layout = Layout::build(&config(vec![ColumnMapping {
            on_missing: Some(MissingColumnPolicy::SkipRow),
            ..column("a", ColumnType::Text)
        }]))?;
        assert_eq!(
            layout.documents(&batch(vec![
                json!({"a": "x"}),
                json!({}),
                json!({"a": "y"})
            ]))?,
            vec![r#"[["x"],["y"]]"#.to_string()]
        );
        assert_eq!(layout.documents(&batch(vec![json!({})]))?, Vec::<String>::new());
        Ok(())
    }

    /// Every document stays under the limit and parses, and nothing is lost in
    /// the cut between two of them.
    #[test]
    fn a_batch_too_large_for_one_document_becomes_several() -> Result<()> {
        let layout = Layout::build(&config(vec![column("a", ColumnType::Text)]))?;
        let filler = "x".repeat(1024 * 1024);
        let messages = (0..20).map(|_| json!({"a": filler})).collect();
        let documents = layout.documents(&batch(messages))?;
        assert!(documents.len() > 1);
        let mut rows = 0;
        for document in &documents {
            assert!(document.len() <= MAX_DOCUMENT_BYTES, "{}", document.len());
            let parsed: Vec<Value> = serde_json::from_str(document)?;
            rows += parsed.len();
        }
        assert_eq!(rows, 20);
        Ok(())
    }

    /// A row larger than the limit has no way to be under it. It goes by itself
    /// for the server to answer, rather than disappearing here.
    #[test]
    fn a_row_over_the_limit_is_sent_alone() -> Result<()> {
        let layout = Layout::build(&config(vec![column("a", ColumnType::Text)]))?;
        let huge = "x".repeat(MAX_DOCUMENT_BYTES + 1);
        let documents = layout.documents(&batch(vec![
            json!({"a": "small"}),
            json!({"a": huge}),
            json!({"a": "small"}),
        ]))?;
        assert_eq!(documents.len(), 3);
        Ok(())
    }

    #[test]
    fn extra_fields_without_columns_are_refused() {
        let mut strict = config(Vec::new());
        strict.on_extra_fields = ExtraFieldPolicy::Error;
        assert!(Layout::build(&strict).is_err());
    }

    /// The mapping's own refusals reach the output, and the message says which.
    #[test]
    fn a_contradictory_column_fails_the_output() {
        let contradiction = config(vec![ColumnMapping {
            nullable: Some(false),
            on_missing: Some(MissingColumnPolicy::Null),
            ..column("a", ColumnType::Text)
        }]);
        let Err(error) = Layout::build(&contradiction) else {
            panic!("the contradiction should have been refused");
        };
        assert!(format!("{error:#}").contains("readings"), "{error:#}");
    }

    /// The table name reaches the SQL text, so this is what stands in front of
    /// arbitrary statements.
    #[test]
    fn a_name_that_could_carry_sql_is_refused() {
        let mut bad = config(Vec::new());
        bad.table = r#"readings"; drop table users; --"#.into();
        assert!(built(bad, "http://127.0.0.1:1").is_err());
        let mut three = config(Vec::new());
        three.table = "a.b.c".into();
        assert!(built(three, "http://127.0.0.1:1").is_err());
        let mut two = config(Vec::new());
        two.table = "analytics.readings".into();
        assert!(built(two, "http://127.0.0.1:1").is_ok());
    }

    /// Building must not talk to the server.
    #[test]
    fn it_builds_against_a_server_that_is_not_running() -> Result<()> {
        built(
            config(vec![column("a", ColumnType::Text)]),
            "http://127.0.0.1:1",
        )?;
        Ok(())
    }

    #[test]
    fn it_needs_a_snowflake_connection() {
        let mut other = config(Vec::new());
        other.connection = "elsewhere".into();
        assert!(built(other, "http://127.0.0.1:1").is_err());
    }

    #[tokio::test]
    async fn starting_creates_the_table_and_a_batch_is_one_statement() -> Result<()> {
        let (fake, url) = fake::serve().await;
        let mut output = built(config(vec![column("a", ColumnType::Text)]), &url)?;
        output.init().await?;
        output
            .emit(Arc::new(batch(vec![json!({"a": "x"}), json!({"a": "y"})])))
            .await?;
        // ...and an empty result costs no statement at all
        output.emit(Arc::new(batch(Vec::new()))).await?;

        let seen = fake.seen();
        assert_eq!(seen.len(), 2);
        let create = seen[0].body["statement"].as_str().unwrap_or_default();
        assert!(create.starts_with("CREATE TABLE IF NOT EXISTS"), "{create}");
        let insert = seen[1].body["statement"].as_str().unwrap_or_default();
        assert!(
            insert.starts_with(r#"INSERT INTO "READINGS" ("A")"#),
            "{insert}"
        );
        assert_eq!(
            seen[1].body["bindings"],
            json!({"1": {"type": "TEXT", "value": r#"[["x"],["y"]]"#}})
        );
        Ok(())
    }

    /// A table someone else owns is not created, and a wrong key is still
    /// heard about at startup rather than on the first batch.
    #[tokio::test]
    async fn create_table_false_only_checks_the_connection() -> Result<()> {
        let (fake, url) = fake::serve().await;
        let mut config = config(Vec::new());
        config.create_table = Some(false);
        let mut output = built(config, &url)?;
        output.init().await?;
        assert_eq!(fake.statements(), vec!["SELECT 1".to_string()]);

        fake.reply(
            401,
            &json!({"message": "JWT token is invalid", "code": "390144"}),
        );
        let Err(error) = output.init().await else {
            panic!("a refused connection was taken as a working one");
        };
        assert!(
            format!("{error:#}").contains("JWT token is invalid"),
            "{error:#}"
        );
        Ok(())
    }

    /// A batch that failed is reported, and the next one does not reach for the
    /// network until the backoff says so.
    #[tokio::test]
    async fn a_failed_insert_fails_the_batch_and_paces_the_next() -> Result<()> {
        let (fake, url) = fake::serve().await;
        let mut output = built(config(vec![column("a", ColumnType::Text)]), &url)?;
        fake.reply(422, &json!({"message": "invalid", "code": "1"}));
        let message = Arc::new(batch(vec![json!({"a": "x"})]));
        assert!(output.emit(Arc::clone(&message)).await.is_err());
        assert_eq!(fake.seen().len(), 1);

        let Err(error) = output.emit(message).await else {
            panic!("a paced attempt reported success");
        };
        assert!(
            format!("{error:#}").contains("not retrying yet"),
            "{error:#}"
        );
        assert_eq!(fake.seen().len(), 1);
        Ok(())
    }

    /// A message that does not fit its column is the message's fault and has
    /// nothing to do with whether the server is up.
    #[tokio::test]
    async fn a_message_that_does_not_fit_sends_nothing() -> Result<()> {
        let (fake, url) = fake::serve().await;
        let mut output = built(config(vec![column("a", ColumnType::Float)]), &url)?;
        let bad = Arc::new(batch(vec![json!({"a": "twelve"})]));
        assert!(output.emit(Arc::clone(&bad)).await.is_err());
        assert!(fake.seen().is_empty());
        // ...so it is not a reason to pace the batch after it
        output
            .emit(Arc::new(batch(vec![json!({"a": 1.5})])))
            .await?;
        assert_eq!(fake.seen().len(), 1);
        Ok(())
    }
}
