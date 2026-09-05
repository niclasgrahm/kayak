//! The SQL inputs against the real servers in `docker-compose.yaml`.
//!
//! Everything in `just test` is offline, on purpose, and these are the one
//! exception: what a `row_to_json` renders, what `CAST({cursor:String} AS
//! DateTime64)` reads back, whether `q.*` is legal — none of it can be
//! answered by a double, and each was found the hard way. So they are
//! `#[ignore]`d out of `just ci` and run by `just test-live`, which needs
//! `docker compose up -d postgres clickhouse` first. Every test makes a table
//! of its own under a random name and drops it on the way out, so a run
//! leaves nothing behind and two runs at once don't share state.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use kayak::BuildCtx;
use kayak::inputs::{BuildInput, Delivery, InputSource};
use kayak_core::config::{
    ClickhouseInputConfig, EnvelopeConfig, PostgresInputConfig,
};
use kayak_core::connections::{
    ClickhouseConnection, ConnectionKind, Connections, PostgresConnection,
};
use kayak_core::sql::{PollMode, SqlPollConfig, StartFrom};
use serde_json::{Value, json};
use tokio_postgres::NoTls;

const READ: Duration = Duration::from_secs(10);

fn table_name(prefix: &str) -> String {
    format!("kayak_live_{prefix}_{}", rand::random::<u32>())
}

fn poll(table: &str, mode: PollMode) -> SqlPollConfig {
    SqlPollConfig {
        table: Some(table.to_string()),
        query: None,
        columns: Vec::new(),
        interval_secs: 1,
        mode,
        page_size: None,
        max_batch: Some(100),
    }
}

fn incremental(field: &str, start_from: StartFrom, lag_secs: Option<u64>) -> PollMode {
    PollMode::Incremental {
        field: field.to_string(),
        start_from: Some(start_from),
        lag_secs,
    }
}

async fn read(source: &mut Box<dyn InputSource>) -> Result<Delivery> {
    tokio::time::timeout(READ, source.next())
        .await
        .context("the input produced nothing in time")?
}

fn ids(delivery: &Delivery) -> Vec<i64> {
    delivery.iter().filter_map(|m| m["id"].as_i64()).collect()
}

// ---------------------------------------------------------------- postgres

async fn pg_client() -> Result<tokio_postgres::Client> {
    let (client, connection) = tokio_postgres::Config::new()
        .host("localhost")
        .port(5432)
        .dbname("kayak")
        .user("kayak")
        .password("hunter2")
        .connect(NoTls)
        .await
        .context("is postgres up? `docker compose up -d postgres`")?;
    tokio::spawn(async move {
        let _ = connection.await;
    });
    Ok(client)
}

fn pg_input(poll: SqlPollConfig, envelope: Option<EnvelopeConfig>) -> Result<Box<dyn InputSource>> {
    let mut pipelines = HashMap::new();
    let (events, _rx) = tokio::sync::broadcast::channel(4);
    let connections: Connections = [(
        "pg".to_string(),
        ConnectionKind::Postgres(PostgresConnection {
            host: "localhost".into(),
            database: "kayak".into(),
            user: "kayak".into(),
            password: "hunter2".into(),
            port: Some(5432),
        }),
    )]
    .into_iter()
    .collect();
    let mut ctx = BuildCtx::new(&mut pipelines, "live".to_string(), events)
        .with_connections(Arc::new(connections));
    ctx.envelope = envelope;
    PostgresInputConfig {
        connection: "pg".into(),
        poll,
    }
    .build(&mut ctx)
}

struct PgTable {
    client: tokio_postgres::Client,
    name: String,
}

impl PgTable {
    async fn create(prefix: &str) -> Result<Self> {
        let client = pg_client().await?;
        let name = table_name(prefix);
        client
            .batch_execute(&format!(
                "CREATE TABLE {name} (\
                 id BIGSERIAL PRIMARY KEY, \
                 sensor TEXT NOT NULL, \
                 value NUMERIC(10, 3), \
                 recorded_at TIMESTAMPTZ NOT NULL DEFAULT now(), \
                 payload JSONB)"
            ))
            .await?;
        Ok(Self { client, name })
    }

    async fn insert(&self, sensor: &str, value: &str) -> Result<()> {
        self.client
            .execute(
                &format!(
                    "INSERT INTO {} (sensor, value, payload) VALUES ($1, $2::text::numeric, '{{\"k\": [1, 2]}}')",
                    self.name
                ),
                &[&sensor, &value],
            )
            .await?;
        Ok(())
    }

    async fn insert_at(&self, sensor: &str, recorded_at: &str) -> Result<()> {
        self.client
            .execute(
                &format!(
                    "INSERT INTO {} (sensor, value, recorded_at) VALUES ($1, 1, $2::text::timestamptz)",
                    self.name
                ),
                &[&sensor, &recorded_at],
            )
            .await?;
        Ok(())
    }

    async fn drop(self) -> Result<()> {
        self.client
            .batch_execute(&format!("DROP TABLE IF EXISTS {}", self.name))
            .await?;
        Ok(())
    }
}

/// The whole of the incremental read against a real table: the server
/// renders the types, a full page is cut before its last value and the next
/// page starts above it, and rows written after the first read arrive on the
/// next tick.
#[tokio::test]
#[ignore = "needs docker compose up postgres"]
async fn postgres_follows_an_id_column_across_pages_and_ticks() -> Result<()> {
    let table = PgTable::create("ids").await?;
    for i in 1..=5 {
        table.insert(&format!("s{i}"), &format!("{i}.5")).await?;
    }
    let mut config = poll(&table.name, incremental("id", StartFrom::Oldest, None));
    config.page_size = Some(2);
    let mut input = pg_input(config, None)?;

    let mut seen = Vec::new();
    while seen.len() < 5 {
        let delivery = read(&mut input).await?;
        seen.extend(ids(&delivery));
        // the first page of two is cut to one row, so a batch is never the
        // whole page — that is what keeps the tie at a boundary from losing
        // a row
        assert!(delivery.len() <= 2, "{}", delivery.len());
        if seen.len() == 1 {
            let row = &delivery[0];
            assert_eq!(row["sensor"], json!("s1"));
            // a numeric keeps its digits and arrives as a number
            assert_eq!(row["value"], json!(1.5));
            assert_eq!(row["payload"], json!({"k": [1, 2]}));
            assert!(
                row["recorded_at"].as_str().is_some_and(|t| t.contains('T')),
                "{:?}",
                row["recorded_at"]
            );
        }
    }
    assert_eq!(seen, [1, 2, 3, 4, 5]);

    // rows written after the read are picked up on the next tick, above the
    // watermark
    table.insert("s6", "6").await?;
    table.insert("s7", "7").await?;
    let delivery = read(&mut input).await?;
    let mut later = ids(&delivery);
    if later.len() < 2 {
        later.extend(ids(&read(&mut input).await?));
    }
    assert_eq!(later, [6, 7]);
    table.drop().await
}

#[tokio::test]
#[ignore = "needs docker compose up postgres"]
async fn postgres_started_from_the_newest_sees_only_what_comes_after() -> Result<()> {
    let table = PgTable::create("newest").await?;
    for i in 1..=3 {
        table.insert(&format!("old{i}"), "1").await?;
    }
    let mut input = pg_input(
        poll(&table.name, incremental("id", StartFrom::Newest, None)),
        None,
    )?;
    // the first read finds nothing above the max; give it a tick to have
    // done so, then write
    let mut pending = Box::pin(read(&mut input));
    assert!(
        tokio::time::timeout(Duration::from_millis(500), &mut pending)
            .await
            .is_err(),
        "old rows were handed on"
    );
    table.insert("new", "1").await?;
    let delivery = pending.await?;
    assert_eq!(ids(&delivery), [4]);
    assert_eq!(delivery[0]["sensor"], json!("new"));
    table.drop().await
}

/// A timestamp cursor with ties: rows sharing a value that straddle a page
/// are read whole, and the watermark travels through `::text` and back
/// without losing a microsecond.
#[tokio::test]
#[ignore = "needs docker compose up postgres"]
async fn postgres_follows_a_timestamp_with_ties_without_losing_rows() -> Result<()> {
    let table = PgTable::create("ties").await?;
    for (sensor, at) in [
        ("a", "2026-01-01T00:00:00.000001Z"),
        ("b", "2026-01-01T00:00:00.000001Z"),
        ("c", "2026-01-01T00:00:00.000002Z"),
        ("d", "2026-01-01T00:00:00.000002Z"),
        ("e", "2026-01-01T00:00:00.000003Z"),
    ] {
        table.insert_at(sensor, at).await?;
    }
    let mut config = poll(
        &table.name,
        incremental("recorded_at", StartFrom::Oldest, None),
    );
    config.page_size = Some(3);
    let mut input = pg_input(config, None)?;
    let mut sensors: Vec<String> = Vec::new();
    while sensors.len() < 5 {
        let delivery = read(&mut input).await?;
        sensors.extend(
            delivery
                .iter()
                .filter_map(|m| m["sensor"].as_str().map(ToString::to_string)),
        );
    }
    assert_eq!(sensors, ["a", "b", "c", "d", "e"]);
    table.drop().await
}

/// A query as the source, a projection over it, and a lag: rows inside the
/// lag are held back until they are old enough.
#[tokio::test]
#[ignore = "needs docker compose up postgres"]
async fn postgres_reads_a_query_with_a_projection_and_a_lag() -> Result<()> {
    let table = PgTable::create("lag").await?;
    table.insert_at("old", "2020-01-01T00:00:00Z").await?;
    table.insert("fresh", "1").await?;
    let config = SqlPollConfig {
        table: None,
        query: Some(format!("select * from {} where sensor <> 'nobody';", table.name)),
        columns: vec!["sensor".into(), "recorded_at".into()],
        interval_secs: 1,
        mode: incremental("recorded_at", StartFrom::Oldest, Some(3600)),
        page_size: None,
        max_batch: Some(10),
    };
    let mut input = pg_input(config, None)?;
    let delivery = read(&mut input).await?;
    assert_eq!(delivery.len(), 1);
    assert_eq!(delivery[0]["sensor"], json!("old"));
    // only the projected columns
    assert!(delivery[0].get("id").is_none(), "{:?}", delivery[0]);
    assert!(delivery[0].get("recorded_at").is_some());
    table.drop().await
}

/// A snapshot returns everything every tick, and the envelope says which
/// tick each row came from.
#[tokio::test]
#[ignore = "needs docker compose up postgres"]
async fn postgres_snapshot_returns_every_row_every_tick_with_its_polled_at() -> Result<()> {
    let table = PgTable::create("snap").await?;
    table.insert("a", "1").await?;
    table.insert("b", "2").await?;
    let mut input = pg_input(
        poll(&table.name, PollMode::Snapshot),
        Some(EnvelopeConfig::Merge { meta: None }),
    )?;
    let first = read(&mut input).await?;
    assert_eq!(ids(&first), [1, 2]);
    assert_eq!(first[0]["_meta"]["connection"], json!("pg"));
    assert_eq!(first[0]["_meta"]["input"], json!("postgres"));
    let polled_at = first[0]["_meta"]["polled_at"].clone();
    assert!(polled_at.is_string());
    assert_eq!(first[1]["_meta"]["polled_at"], polled_at);

    table.insert("c", "3").await?;
    let second = read(&mut input).await?;
    assert_eq!(ids(&second), [1, 2, 3]);
    assert_ne!(second[0]["_meta"]["polled_at"], polled_at);
    table.drop().await
}

/// A wrong table is a read error reported on the card and retried, not a
/// dead pipeline — and the same input reads fine once the table exists.
#[tokio::test]
#[ignore = "needs docker compose up postgres"]
async fn postgres_survives_a_table_that_is_not_there_yet() -> Result<()> {
    let name = table_name("late");
    let mut input = pg_input(
        poll(&name, incremental("id", StartFrom::Oldest, None)),
        None,
    )?;
    let mut pending = Box::pin(read(&mut input));
    assert!(
        tokio::time::timeout(Duration::from_millis(300), &mut pending)
            .await
            .is_err()
    );
    let client = pg_client().await?;
    client
        .batch_execute(&format!(
            "CREATE TABLE {name} (id BIGSERIAL PRIMARY KEY, sensor TEXT); \
             INSERT INTO {name} (sensor) VALUES ('here')"
        ))
        .await?;
    let delivery = pending.await?;
    assert_eq!(ids(&delivery), [1]);
    client
        .batch_execute(&format!("DROP TABLE {name}"))
        .await?;
    Ok(())
}

// -------------------------------------------------------------- clickhouse

const CH_URL: &str = "http://localhost:8123";

async fn ch_execute(sql: &str) -> Result<String> {
    let response = reqwest::Client::new()
        .post(CH_URL)
        .query(&[("database", "kayak"), ("query", sql)])
        .header("X-ClickHouse-User", "kayak")
        .header("X-ClickHouse-Key", "hunter2")
        .header(reqwest::header::CONTENT_LENGTH, 0)
        .send()
        .await
        .context("is clickhouse up? `docker compose up -d clickhouse`")?;
    let status = response.status();
    let body = response.text().await?;
    if !status.is_success() {
        return Err(anyhow!("clickhouse refused `{sql}` ({status}): {body}"));
    }
    Ok(body)
}

fn ch_input(poll: SqlPollConfig) -> Result<Box<dyn InputSource>> {
    let mut pipelines = HashMap::new();
    let (events, _rx) = tokio::sync::broadcast::channel(4);
    let connections: Connections = [(
        "ch".to_string(),
        ConnectionKind::Clickhouse(ClickhouseConnection {
            url: CH_URL.into(),
            database: "kayak".into(),
            user: "kayak".into(),
            password: "hunter2".into(),
            allow_http: Some(true),
        }),
    )]
    .into_iter()
    .collect();
    let mut ctx = BuildCtx::new(&mut pipelines, "live".to_string(), events)
        .with_connections(Arc::new(connections));
    ClickhouseInputConfig {
        connection: "ch".into(),
        poll,
    }
    .build(&mut ctx)
}

struct ChTable {
    name: String,
}

impl ChTable {
    async fn create(prefix: &str) -> Result<Self> {
        let name = table_name(prefix);
        ch_execute(&format!(
            "CREATE TABLE {name} (\
             id UInt64, sensor String, big Int64, amount Decimal(10, 2), \
             ts DateTime64(3, 'UTC')) \
             ENGINE = MergeTree ORDER BY id"
        ))
        .await?;
        Ok(Self { name })
    }

    async fn insert(&self, rows: &[(u64, &str, &str)]) -> Result<()> {
        let values: Vec<String> = rows
            .iter()
            .map(|(id, sensor, ts)| {
                format!("({id}, '{sensor}', 9007199254740993, 12.34, '{ts}')")
            })
            .collect();
        ch_execute(&format!(
            "INSERT INTO {} (id, sensor, big, amount, ts) VALUES {}",
            self.name,
            values.join(", ")
        ))
        .await?;
        Ok(())
    }

    async fn drop(self) -> Result<()> {
        ch_execute(&format!("DROP TABLE IF EXISTS {}", self.name)).await?;
        Ok(())
    }
}

/// The settings do what the module docs say: an `Int64` past 2^53 arrives
/// as a number with its digits intact, a `Decimal` as a number, a
/// `DateTime64` as ISO 8601 — and the id watermark pages and follows.
#[tokio::test]
#[ignore = "needs docker compose up clickhouse"]
async fn clickhouse_follows_an_id_column_and_renders_the_types() -> Result<()> {
    let table = ChTable::create("ids").await?;
    table
        .insert(&[
            (1, "a", "2026-01-01 00:00:00.001"),
            (2, "b", "2026-01-01 00:00:00.002"),
            (3, "c", "2026-01-01 00:00:00.003"),
        ])
        .await?;
    let mut config = poll(&table.name, incremental("id", StartFrom::Oldest, None));
    config.page_size = Some(2);
    let mut input = ch_input(config)?;

    let mut seen = Vec::new();
    while seen.len() < 3 {
        let delivery = read(&mut input).await?;
        if seen.is_empty() {
            let row = &delivery[0];
            assert_eq!(row["big"], json!(9_007_199_254_740_993_i64));
            assert_eq!(row["amount"], json!(12.34));
            assert_eq!(row["ts"], json!("2026-01-01T00:00:00.001Z"));
            assert!(row.get("__kayak_cursor").is_none(), "{row}");
        }
        seen.extend(ids(&delivery));
    }
    assert_eq!(seen, [1, 2, 3]);

    table.insert(&[(4, "d", "2026-01-01 00:00:00.004")]).await?;
    assert_eq!(ids(&read(&mut input).await?), [4]);
    table.drop().await
}

/// `maxOrNull` on an empty table: a `newest` start finds nothing and reads
/// from the beginning, rather than from a watermark of zero.
#[tokio::test]
#[ignore = "needs docker compose up clickhouse"]
async fn clickhouse_started_from_the_newest_of_an_empty_table_reads_what_arrives() -> Result<()> {
    let table = ChTable::create("empty").await?;
    let mut input = ch_input(poll(&table.name, incremental("id", StartFrom::Newest, None)))?;
    let mut pending = Box::pin(read(&mut input));
    assert!(
        tokio::time::timeout(Duration::from_millis(500), &mut pending)
            .await
            .is_err()
    );
    table.insert(&[(7, "x", "2026-01-01 00:00:00")]).await?;
    assert_eq!(ids(&pending.await?), [7]);
    table.drop().await
}

/// The watermark round trip through `toString` and `CAST` on a
/// `DateTime64` cursor, with ties at a page boundary.
#[tokio::test]
#[ignore = "needs docker compose up clickhouse"]
async fn clickhouse_follows_a_datetime_with_ties() -> Result<()> {
    let table = ChTable::create("ts").await?;
    table
        .insert(&[
            (1, "a", "2026-01-01 00:00:00.001"),
            (2, "b", "2026-01-01 00:00:00.001"),
            (3, "c", "2026-01-01 00:00:00.002"),
            (4, "d", "2026-01-01 00:00:00.002"),
            (5, "e", "2026-01-01 00:00:00.003"),
        ])
        .await?;
    let mut config = poll(&table.name, incremental("ts", StartFrom::Oldest, None));
    config.page_size = Some(3);
    let mut input = ch_input(config)?;
    let mut seen = Vec::new();
    while seen.len() < 5 {
        seen.extend(ids(&read(&mut input).await?));
    }
    seen.sort_unstable();
    assert_eq!(seen, [1, 2, 3, 4, 5]);
    table.drop().await
}

#[tokio::test]
#[ignore = "needs docker compose up clickhouse"]
async fn clickhouse_snapshot_of_a_query_returns_every_row_every_tick() -> Result<()> {
    let table = ChTable::create("snap").await?;
    table
        .insert(&[(1, "a", "2026-01-01 00:00:00"), (2, "b", "2026-01-01 00:00:00")])
        .await?;
    let config = SqlPollConfig {
        table: None,
        query: Some(format!("SELECT id, sensor FROM {} WHERE sensor != 'z'", table.name)),
        columns: Vec::new(),
        interval_secs: 1,
        mode: PollMode::Snapshot,
        page_size: None,
        max_batch: Some(10),
    };
    let mut input = ch_input(config)?;
    let first = read(&mut input).await?;
    let mut first_ids = ids(&first);
    first_ids.sort_unstable();
    assert_eq!(first_ids, [1, 2]);
    assert_eq!(first[0].as_object().map(serde_json::Map::len), Some(2));
    table.insert(&[(3, "c", "2026-01-01 00:00:00")]).await?;
    let mut second_ids = ids(&read(&mut input).await?);
    second_ids.sort_unstable();
    assert_eq!(second_ids, [1, 2, 3]);
    table.drop().await
}

/// A sanity check that the helpers themselves see a value the way the
/// pipeline will: a row read here is a plain JSON object.
#[tokio::test]
#[ignore = "needs docker compose up clickhouse"]
async fn clickhouse_rows_are_plain_objects() -> Result<()> {
    let table = ChTable::create("plain").await?;
    table.insert(&[(1, "a", "2026-01-01 00:00:00")]).await?;
    let mut input = ch_input(poll(&table.name, PollMode::Snapshot))?;
    let delivery = read(&mut input).await?;
    let row: &Value = &delivery[0];
    assert!(row.is_object());
    assert_eq!(row["sensor"], json!("a"));
    table.drop().await
}
