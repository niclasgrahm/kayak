//! The snowflake components against a real account.
//!
//! Everything else about these components is tested against a fake on a socket,
//! which can say what was sent and cannot say what Snowflake makes of it: whether
//! `LATERAL FLATTEN` over a bound `PARSE_JSON` is a legal insert, how a `VARIANT`
//! comes back, whether a nine-digit watermark reads back as itself. Compose has
//! nothing that speaks this API, so these are `#[ignore]`d out of `just ci` and
//! run by `just test-snowflake`, against whatever the environment names:
//!
//! `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_WAREHOUSE`,
//! `SNOWFLAKE_DATABASE`, `SNOWFLAKE_SCHEMA`, one of `SNOWFLAKE_TOKEN` and
//! `SNOWFLAKE_PRIVATE_KEY`, and optionally `SNOWFLAKE_ROLE` and `SNOWFLAKE_URL`.
//!
//! Each test makes a table of its own under a random name and drops it on the way
//! out. They use warehouse credits.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use kayak::BuildCtx;
use kayak::inputs::{BuildInput, InputSource};
use kayak::outputs::{BuildOutput, OutputDestination};
use kayak::snowflake::Session;
use kayak_core::columns::{ColumnMapping, ColumnType, ExtraFieldPolicy};
use kayak_core::config::{SnowflakeInputConfig, SnowflakeOutputConfig};
use kayak_core::connections::{ConnectionKind, Connections, SnowflakeAuth, SnowflakeConnection};
use kayak_core::sql::{PollMode, SqlPollConfig, StartFrom};
use serde_json::{Value, json};

const READ: Duration = Duration::from_secs(120);

fn env(name: &str) -> Result<String> {
    std::env::var(name).with_context(|| format!("{name} has to be set for this test"))
}

fn connection() -> Result<SnowflakeConnection> {
    let auth = if let Ok(key) = std::env::var("SNOWFLAKE_PRIVATE_KEY") {
        SnowflakeAuth::KeyPair {
            private_key: key.as_str().into(),
        }
    } else {
        SnowflakeAuth::Token {
            token: env("SNOWFLAKE_TOKEN")?.as_str().into(),
        }
    };
    Ok(SnowflakeConnection {
        account: env("SNOWFLAKE_ACCOUNT")?,
        user: env("SNOWFLAKE_USER")?,
        auth,
        warehouse: env("SNOWFLAKE_WAREHOUSE")?,
        database: env("SNOWFLAKE_DATABASE")?,
        schema: env("SNOWFLAKE_SCHEMA")?,
        role: std::env::var("SNOWFLAKE_ROLE").ok(),
        url: std::env::var("SNOWFLAKE_URL").ok(),
        allow_http: None,
        statement_timeout_seconds: Some(120),
    })
}

fn with_ctx<T>(body: impl FnOnce(&mut BuildCtx) -> Result<T>) -> Result<T> {
    let mut pipelines = HashMap::new();
    let (events, _rx) = tokio::sync::broadcast::channel(4);
    let connections: Connections = [(
        "warehouse".to_string(),
        ConnectionKind::Snowflake(connection()?),
    )]
    .into_iter()
    .collect();
    let mut ctx = BuildCtx::new(&mut pipelines, "live".to_string(), events)
        .with_connections(Arc::new(connections));
    body(&mut ctx)
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

fn output(table: &str, columns: Vec<ColumnMapping>) -> Result<Box<dyn OutputDestination>> {
    with_ctx(|ctx| {
        SnowflakeOutputConfig {
            connection: "warehouse".into(),
            table: table.to_string(),
            columns,
            create_table: None,
            on_extra_fields: ExtraFieldPolicy::Ignore,
        }
        .build(ctx)
    })
}

fn input(table: &str, mode: PollMode, page_size: usize) -> Result<Box<dyn InputSource>> {
    with_ctx(|ctx| {
        SnowflakeInputConfig {
            connection: "warehouse".into(),
            poll: SqlPollConfig {
                table: Some(table.to_string()),
                query: None,
                columns: Vec::new(),
                interval_secs: 1,
                mode,
                page_size: Some(page_size),
                max_batch: Some(1000),
            },
        }
        .build(ctx)
    })
}

fn batch(messages: Vec<Value>) -> Arc<kayak::inputs::MessageBatch> {
    Arc::new(messages.into_iter().map(Arc::new).collect())
}

/// Reads until `count` messages have arrived.
async fn read(input: &mut Box<dyn InputSource>, count: usize) -> Result<Vec<Value>> {
    let mut messages = Vec::new();
    while messages.len() < count {
        let delivery = tokio::time::timeout(READ, input.next())
            .await
            .context("the input did not deliver in time")??;
        messages.extend(delivery.batch.iter().map(|m| (**m).clone()));
    }
    Ok(messages)
}

async fn drop_table(table: &str) -> Result<()> {
    let mut session = with_ctx(|ctx| Session::build(ctx, "warehouse"))?;
    session
        .execute(&format!("DROP TABLE IF EXISTS \"{table}\""), &[])
        .await?;
    Ok(())
}

fn table_name(prefix: &str) -> String {
    format!("KAYAK_LIVE_{prefix}_{}", rand::random::<u32>())
}

/// Every column type there is, through both components: what the output carries
/// is what the input reads, each value as its documented JSON type.
#[tokio::test]
#[ignore = "needs a Snowflake account; run with `just test-snowflake`"]
async fn every_column_type_round_trips() -> Result<()> {
    let table = table_name("TYPES");
    let outcome = async {
        let mut writer = output(
            &table,
            vec![
                column("text_col", ColumnType::Text),
                column("int_col", ColumnType::Integer),
                column("big_col", ColumnType::Bigint),
                column("float_col", ColumnType::Float),
                column("dec_col", ColumnType::Decimal),
                column("bool_col", ColumnType::Boolean),
                column("ts_col", ColumnType::Timestamp),
                column("date_col", ColumnType::Date),
                column("uuid_col", ColumnType::Uuid),
                column("json_col", ColumnType::Json),
                column("missing_col", ColumnType::Text),
            ],
        )?;
        writer.init().await?;
        writer
            .emit(batch(vec![json!({
                "text_col": "it's \"quoted\"",
                "int_col": 3,
                "big_col": 9_007_199_254_740_993_i64,
                "float_col": 1.5,
                "dec_col": 12.25,
                "bool_col": true,
                "ts_col": "2026-08-11T12:00:00.123456Z",
                "date_col": "2026-08-11",
                "uuid_col": "0197f0e6-0000-7000-8000-000000000000",
                "json_col": {"nested": [1, 2]}
            })]))
            .await?;

        let mut reader = input(&table, PollMode::Snapshot, 100)?;
        let row = read(&mut reader, 1).await?.remove(0);
        assert_eq!(row["TEXT_COL"], "it's \"quoted\"");
        assert_eq!(row["INT_COL"], 3);
        assert_eq!(row["BIG_COL"], 9_007_199_254_740_993_i64);
        assert_eq!(row["FLOAT_COL"], 1.5);
        // NUMBER(38, 9) is wider than a JSON number can hold exactly
        assert_eq!(row["DEC_COL"], "12.250000000");
        assert_eq!(row["BOOL_COL"], true);
        assert_eq!(row["TS_COL"], "2026-08-11T12:00:00.123456000+00:00");
        assert_eq!(row["DATE_COL"], "2026-08-11");
        assert_eq!(row["UUID_COL"], "0197f0e6-0000-7000-8000-000000000000");
        assert_eq!(row["JSON_COL"], json!({"nested": [1, 2]}));
        assert_eq!(row["MISSING_COL"], Value::Null);
        assert!(row["RECEIVED_AT"].is_string(), "{row}");
        Ok::<_, anyhow::Error>(())
    }
    .await;
    drop_table(&table).await?;
    outcome
}

#[tokio::test]
#[ignore = "needs a Snowflake account; run with `just test-snowflake`"]
async fn a_message_with_no_mapping_is_stored_as_a_variant() -> Result<()> {
    let table = table_name("PAYLOAD");
    let outcome = async {
        let mut writer = output(&table, Vec::new())?;
        writer.init().await?;
        writer
            .emit(batch(vec![
                json!({"a": 1}),
                json!({"a": {"b": [true, null]}}),
            ]))
            .await?;
        let mut reader = input(&table, PollMode::Snapshot, 100)?;
        let rows = read(&mut reader, 2).await?;
        let payloads: Vec<&Value> = rows.iter().map(|row| &row["PAYLOAD"]).collect();
        assert!(payloads.contains(&&json!({"a": 1})), "{rows:?}");
        assert!(
            payloads.contains(&&json!({"a": {"b": [true, null]}})),
            "{rows:?}"
        );
        Ok::<_, anyhow::Error>(())
    }
    .await;
    drop_table(&table).await?;
    outcome
}

/// The reason the cursor is rendered with nine digits: these are a microsecond
/// apart, and a watermark rounded to milliseconds would never get past the first
/// of them. A tie across the page boundary has to come through whole as well.
#[tokio::test]
#[ignore = "needs a Snowflake account; run with `just test-snowflake`"]
async fn an_incremental_read_follows_fine_timestamps_across_pages() -> Result<()> {
    let table = table_name("CURSOR");
    let outcome = async {
        let mut writer = output(
            &table,
            vec![
                column("id", ColumnType::Bigint),
                column("ts", ColumnType::Timestamp),
            ],
        )?;
        writer.init().await?;
        writer
            .emit(batch(vec![
                json!({"id": 1, "ts": "2026-01-01T00:00:00.000001Z"}),
                json!({"id": 2, "ts": "2026-01-01T00:00:00.000002Z"}),
                json!({"id": 3, "ts": "2026-01-01T00:00:00.000002Z"}),
                json!({"id": 4, "ts": "2026-01-01T00:00:00.000003Z"}),
            ]))
            .await?;

        let mode = PollMode::Incremental {
            field: "ts".into(),
            start_from: Some(StartFrom::Oldest),
            lag_secs: None,
        };
        let mut reader = input(&table, mode, 2)?;
        let rows = read(&mut reader, 4).await?;
        let mut ids: Vec<i64> = rows.iter().filter_map(|row| row["ID"].as_i64()).collect();
        ids.sort_unstable();
        assert_eq!(ids, vec![1, 2, 3, 4]);

        // nothing new has been written, so nothing arrives
        let quiet = tokio::time::timeout(Duration::from_secs(5), reader.next()).await;
        assert!(quiet.is_err(), "the watermark read a row a second time");
        Ok::<_, anyhow::Error>(())
    }
    .await;
    drop_table(&table).await?;
    outcome
}

/// `start_from: newest` skips what is there, and an empty table does not trip it.
#[tokio::test]
#[ignore = "needs a Snowflake account; run with `just test-snowflake`"]
async fn starting_from_the_newest_skips_what_is_there() -> Result<()> {
    let table = table_name("NEWEST");
    let outcome = async {
        let mut writer = output(
            &table,
            vec![
                column("id", ColumnType::Bigint),
                column("ts", ColumnType::Timestamp),
            ],
        )?;
        writer.init().await?;
        writer
            .emit(batch(vec![json!({"id": 1, "ts": "2026-01-01T00:00:00Z"})]))
            .await?;

        let mode = PollMode::Incremental {
            field: "ts".into(),
            start_from: Some(StartFrom::Newest),
            lag_secs: None,
        };
        let mut reader = input(&table, mode, 100)?;
        let quiet = tokio::time::timeout(Duration::from_secs(10), reader.next()).await;
        assert!(quiet.is_err(), "an existing row was read");

        writer
            .emit(batch(vec![json!({"id": 2, "ts": "2026-01-02T00:00:00Z"})]))
            .await?;
        let rows = read(&mut reader, 1).await?;
        assert_eq!(rows[0]["ID"], 2);
        Ok::<_, anyhow::Error>(())
    }
    .await;
    drop_table(&table).await?;
    outcome
}

/// A result large enough that the server splits it, which is the only way to
/// find out what a later partition's body really looks like.
#[tokio::test]
#[ignore = "needs a Snowflake account; run with `just test-snowflake`"]
async fn a_result_in_partitions_comes_back_whole() -> Result<()> {
    let mut session = with_ctx(|ctx| Session::build(ctx, "warehouse"))?;
    let result = session
        .execute(
            "SELECT SEQ8() AS n, RANDSTR(200, RANDOM()) AS filler \
             FROM TABLE(GENERATOR(ROWCOUNT => 200000))",
            &[],
        )
        .await?;
    assert_eq!(result.rows.len(), 200_000);
    Ok(())
}
