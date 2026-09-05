//! The polling loop every SQL input runs: the schedule, the paging and the
//! watermark, with the database behind a trait.
//!
//! This is the server half of [`kayak_core::sql`] and the reason that module
//! is neutral: the postgres and clickhouse inputs implement [`Reader`] — five
//! queries, spelled the way their server spells them — and everything about
//! *when* to ask, *how much* to ask for and *where the last read got to* is
//! here, once. It is the same line `rotate.rs` draws for the file and s3
//! outputs: the destination knows its wire, the shared half knows the policy.
//!
//! [`Plan`] is the validated config. Everything contradictory is refused at
//! build time rather than left to be a strange message once per tick forever:
//! both a table and a query, neither, a zero interval, a zero page, a
//! projection that leaves out the cursor field. Same rule as the reducer's.
//!
//! [`Poller`] is the `InputSource`. Its `next()` is one loop with three
//! things it can do: hand out rows it already holds, wait until the next read
//! is due, or read. A read that fails is reported once per outage and retried
//! on the shared [`Backoff`] schedule — a database that is down for the night
//! costs the pipeline a wait, not its life, the same promise every broker
//! input makes.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, bail};
use kayak_core::Stage;
use kayak_core::sql::{PollMode, SqlPollConfig, StartFrom};
use serde_json::Value;
use tokio::sync::broadcast;
use tokio::time::Instant;

use crate::backoff::Backoff;
use crate::events::publish;
use crate::inputs::{Delivery, InputSource, MessageBatch, batch_cap, envelope::Envelope};
use crate::outputs::columns::{Identifier, Table};
use crate::state::{PipelineId, UiEvent};

/// Where the rows come from: a table by name, or a query as written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Source {
    Table(Table),
    /// The query with any trailing semicolon and whitespace removed, so it can
    /// be wrapped as a subquery. Nothing else about it is checked here — the
    /// server is the only thing that can say whether it is a valid `SELECT`,
    /// and it says so on the first read.
    Query(String),
}

/// What an incremental read follows, and where it starts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Cursor {
    pub field: Identifier,
    pub start_from: StartFrom,
    pub lag: Option<Duration>,
}

/// A [`SqlPollConfig`] that has been checked and can be read from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Plan {
    pub source: Source,
    pub columns: Vec<Identifier>,
    pub interval: Duration,
    /// `None` is a snapshot.
    pub cursor: Option<Cursor>,
    pub page_size: usize,
    pub max_batch: usize,
}

impl Plan {
    /// Validates the config. `qualifier` names the first part of a qualified
    /// table in an error — a schema in postgres, a database in clickhouse —
    /// exactly as [`Table::parse`] takes it.
    pub fn build(config: &SqlPollConfig, qualifier: &str) -> Result<Self> {
        let source = match (&config.table, &config.query) {
            (Some(_), Some(_)) => bail!("both `table` and `query` are set; it reads one or the other"),
            (None, None) => bail!("neither `table` nor `query` is set; one of them is required"),
            (Some(table), None) => Source::Table(Table::parse(table, qualifier)?),
            (None, Some(query)) => {
                let trimmed = query.trim().trim_end_matches(';').trim_end();
                if trimmed.is_empty() {
                    bail!("`query` is empty");
                }
                if trimmed.contains(';') {
                    bail!(
                        "`query` holds more than one statement; it is wrapped as a subquery, so it \
                         has to be a single SELECT"
                    );
                }
                Source::Query(trimmed.to_string())
            }
        };

        if config.interval_secs == 0 {
            bail!("`interval_secs` is 0; a read that never waits is a loop against the database");
        }

        let mut columns = Vec::with_capacity(config.columns.len());
        for name in &config.columns {
            let column = Identifier::parse(name, "column name")?;
            if columns.contains(&column) {
                bail!("`columns` names '{name}' twice");
            }
            columns.push(column);
        }

        let cursor = match &config.mode {
            PollMode::Snapshot => None,
            PollMode::Incremental {
                field,
                start_from,
                lag_secs,
            } => {
                let field = Identifier::parse(field, "cursor field")?;
                if !columns.is_empty() && !columns.contains(&field) {
                    bail!(
                        "the cursor field '{}' is not among `columns`; an incremental read has \
                         to read the field it follows",
                        field.as_str()
                    );
                }
                Some(Cursor {
                    field,
                    start_from: start_from.unwrap_or_default(),
                    lag: lag_secs.map(Duration::from_secs),
                })
            }
        };

        let page_size = config.page_size();
        if page_size == 0 {
            bail!("`page_size` is 0; a page holding no rows can never make progress");
        }

        Ok(Self {
            source,
            columns,
            interval: Duration::from_secs(config.interval_secs),
            cursor,
            page_size,
            max_batch: batch_cap(config.max_batch),
        })
    }

    /// The relation as a subquery body — `SELECT <columns> FROM <table>`, or
    /// the query as written — for a reader to wrap in parentheses. Quoting is
    /// double quotes, which both servers read as an identifier.
    #[must_use]
    pub fn relation_sql(&self) -> String {
        let projection = if self.columns.is_empty() {
            "*".to_string()
        } else {
            self.columns
                .iter()
                .map(Identifier::quoted)
                .collect::<Vec<_>>()
                .join(", ")
        };
        match &self.source {
            Source::Table(table) => format!("SELECT {projection} FROM {}", table.quoted()),
            Source::Query(query) => {
                if self.columns.is_empty() {
                    query.clone()
                } else {
                    format!("SELECT {projection} FROM ({query}) AS s")
                }
            }
        }
    }

    /// How the source is named in a log line or an error.
    #[must_use]
    pub fn describe_source(&self) -> String {
        match &self.source {
            Source::Table(table) => table.quoted(),
            Source::Query(_) => "the configured query".to_string(),
        }
    }
}

/// One row as a reader returns it: the message, and the row's cursor value
/// **as the server's own text rendering of it**, so it can be handed back in
/// the next query's condition and cast by the server to the column's type.
/// `None` for a snapshot row, which has no cursor.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fetched {
    pub row: Value,
    pub cursor: Option<String>,
}

/// The five things a database has to be able to do for the poller to read it.
///
/// A reader owns its connection and is expected to drop it on an error, so
/// the next call reconnects; the poller does the waiting in between and never
/// asks a reader to wait itself.
#[async_trait::async_trait]
pub trait Reader: Send {
    /// Every row of the relation, in one query.
    async fn snapshot(&mut self) -> Result<Vec<Value>>;
    /// Up to `limit` rows whose cursor is above `after` — or, for `None`, the
    /// first `limit` rows with a non-null cursor — ordered by the cursor.
    async fn page(&mut self, after: Option<&str>, limit: usize) -> Result<Vec<Fetched>>;
    /// The highest cursor value in the relation right now, as text, or `None`
    /// for an empty relation.
    async fn newest(&mut self) -> Result<Option<String>>;
    /// How the server is described in an error: everything but the password.
    fn describe(&self) -> String;
}

/// A full page cut before its last distinct cursor value.
///
/// Rows sharing a cursor value — a timestamp several writes landed on — can
/// straddle a page boundary, and a watermark taken from the last row would
/// exclude the rest of them from the next page. So a page that came back full
/// is cut before its last value, and those rows are read again on the next
/// page, whole. A page whose every row shares the value cannot be cut without
/// making no progress at all, so it is handed on as it is; the `bool` says
/// that happened, and the poller warns about it once.
#[must_use]
pub fn cut_at_last_value(mut page: Vec<Fetched>, limit: usize) -> (Vec<Fetched>, bool) {
    if page.len() < limit {
        return (page, false);
    }
    let Some(last) = page.last().and_then(|row| row.cursor.clone()) else {
        return (page, false);
    };
    let first_of_last = page
        .iter()
        .position(|row| row.cursor.as_deref() == Some(last.as_str()))
        .unwrap_or(0);
    if first_of_last == 0 {
        return (page, true);
    }
    page.truncate(first_of_last);
    (page, false)
}

/// The `InputSource` — see the module docs.
pub struct Poller {
    plan: Plan,
    reader: Box<dyn Reader>,
    connection_name: String,
    envelope: Envelope,
    pipeline_id: PipelineId,
    events: broadcast::Sender<UiEvent>,
    backoff: Backoff,
    /// Rows read and not yet handed on, already enveloped.
    pending: VecDeque<Value>,
    /// The highest cursor value handed on so far. Meaningless for a snapshot.
    watermark: Option<String>,
    /// Whether `start_from` has been applied — `newest` needs one query
    /// before the first read, and it has to be asked exactly once.
    started: bool,
    /// When the next read is due, once a read has finished. `None` means read
    /// now — at startup, and while a page comes back full.
    next_due: Option<Instant>,
    /// When the read in progress started, RFC 3339 — what every row of that
    /// read carries as `polled_at`.
    polled_at: String,
    /// Whether an uncuttable page has been warned about this run.
    warned_ties: bool,
}

impl Poller {
    #[must_use]
    pub fn new(
        plan: Plan,
        reader: Box<dyn Reader>,
        connection_name: String,
        envelope: Envelope,
        pipeline_id: PipelineId,
        events: broadcast::Sender<UiEvent>,
    ) -> Self {
        Self {
            plan,
            reader,
            connection_name,
            envelope,
            pipeline_id,
            events,
            backoff: Backoff::new(),
            pending: VecDeque::new(),
            watermark: None,
            started: false,
            next_due: None,
            polled_at: String::new(),
            warned_ties: false,
        }
    }

    /// The watermark as it stands — what a test asks after a read.
    #[must_use]
    pub fn watermark(&self) -> Option<&str> {
        self.watermark.as_deref()
    }

    /// Up to `max_batch` rows already read, or `None` when there are none.
    fn take_batch(&mut self) -> Option<Arc<MessageBatch>> {
        if self.pending.is_empty() {
            return None;
        }
        let count = self.pending.len().min(self.plan.max_batch);
        let batch: MessageBatch = self.pending.drain(..count).map(Arc::new).collect();
        Some(Arc::new(batch))
    }

    fn enqueue(&mut self, row: Value) {
        let own = if self.envelope.is_enabled() {
            vec![
                ("connection", Value::String(self.connection_name.clone())),
                ("polled_at", Value::String(self.polled_at.clone())),
            ]
        } else {
            Vec::new()
        };
        // a row is always an object, so the `merge` shape always has
        // somewhere to attach to; said rather than unwrapped
        if let Some(message) = self.envelope.apply(row, own) {
            self.pending.push_back(message);
        } else {
            tracing::warn!("skipping a row that could not be enveloped");
        }
    }

    /// One read: a snapshot, or one page of an incremental read. Sets
    /// `next_due` once the read is complete — after a snapshot, and after a
    /// page that came back short.
    async fn read(&mut self) -> Result<()> {
        if self.next_due.is_none() && self.pending.is_empty() {
            // the start of a read, as against the next page of one
            self.polled_at = chrono::Utc::now().to_rfc3339();
        }
        let Some(cursor) = self.plan.cursor.clone() else {
            let rows = self.reader.snapshot().await?;
            let count = rows.len();
            for row in rows {
                self.enqueue(row);
            }
            tracing::debug!(
                "snapshot of {} returned {count} rows",
                self.plan.describe_source()
            );
            self.next_due = Some(Instant::now() + self.plan.interval);
            return Ok(());
        };

        if !self.started {
            if cursor.start_from == StartFrom::Newest {
                self.watermark = self.reader.newest().await?;
                tracing::info!(
                    "incremental read of {} starts after {}",
                    self.plan.describe_source(),
                    self.watermark
                        .as_deref()
                        .map_or_else(|| "an empty relation".to_string(), |w| format!("'{w}'"))
                );
            }
            self.started = true;
        }

        let page = self
            .reader
            .page(self.watermark.as_deref(), self.plan.page_size)
            .await?;
        let fetched = page.len();
        let (rows, uncuttable) = cut_at_last_value(page, self.plan.page_size);
        if uncuttable && !self.warned_ties {
            self.warned_ties = true;
            tracing::warn!(
                "a whole page of {} rows from {} shares the cursor value '{}'; rows with that \
                 value beyond the page may be missed — raise `page_size` or follow a field \
                 with fewer ties",
                self.plan.page_size,
                self.plan.describe_source(),
                rows.last().and_then(|r| r.cursor.as_deref()).unwrap_or("")
            );
        }
        for row in rows {
            if row.cursor.is_some() {
                self.watermark = row.cursor;
            }
            self.enqueue(row.row);
        }
        if fetched < self.plan.page_size {
            self.next_due = Some(Instant::now() + self.plan.interval);
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl InputSource for Poller {
    async fn next(&mut self) -> Result<Delivery> {
        loop {
            if let Some(batch) = self.take_batch() {
                return Ok(Delivery::new(batch));
            }
            if let Some(due) = self.next_due {
                tokio::time::sleep_until(due).await;
                self.next_due = None;
            }
            match self.read().await {
                Ok(()) => {
                    if self.backoff.is_failing() {
                        tracing::info!("read of {} succeeded again", self.reader.describe());
                    }
                    self.backoff.succeeded();
                }
                Err(e) => {
                    let e = e.context(format!("failed to read from {}", self.reader.describe()));
                    if !self.backoff.is_failing() {
                        tracing::error!("{e:#}; retrying");
                        publish(&self.events, || {
                            UiEvent::error(self.pipeline_id.clone(), Stage::Input, &e)
                        });
                    }
                    tokio::time::sleep(self.backoff.failed()).await;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::Mutex;

    fn config(mode: PollMode) -> SqlPollConfig {
        SqlPollConfig {
            table: Some("readings".to_string()),
            query: None,
            columns: Vec::new(),
            interval_secs: 10,
            mode,
            page_size: Some(3),
            max_batch: None,
        }
    }

    fn incremental() -> PollMode {
        PollMode::Incremental {
            field: "id".to_string(),
            start_from: Some(StartFrom::Oldest),
            lag_secs: None,
        }
    }

    fn fetched(id: i64) -> Fetched {
        Fetched {
            row: json!({"id": id}),
            cursor: Some(id.to_string()),
        }
    }

    type ScriptedPages = Arc<Mutex<VecDeque<Result<Vec<Fetched>, String>>>>;

    /// A reader whose answers are scripted, and which records what it was
    /// asked — the double every poller test drives.
    #[derive(Clone, Default)]
    struct Scripted {
        pages: ScriptedPages,
        snapshots: Arc<Mutex<VecDeque<Vec<Value>>>>,
        newest: Option<String>,
        asked: Arc<Mutex<Vec<Option<String>>>>,
        newest_asked: Arc<Mutex<usize>>,
    }

    #[async_trait::async_trait]
    impl Reader for Scripted {
        async fn snapshot(&mut self) -> Result<Vec<Value>> {
            Ok(self
                .snapshots
                .lock()
                .map_err(|_| anyhow::anyhow!("poisoned"))?
                .pop_front()
                .unwrap_or_default())
        }
        async fn page(&mut self, after: Option<&str>, _limit: usize) -> Result<Vec<Fetched>> {
            self.asked
                .lock()
                .map_err(|_| anyhow::anyhow!("poisoned"))?
                .push(after.map(ToString::to_string));
            match self
                .pages
                .lock()
                .map_err(|_| anyhow::anyhow!("poisoned"))?
                .pop_front()
            {
                Some(Ok(page)) => Ok(page),
                Some(Err(e)) => Err(anyhow::anyhow!(e)),
                None => Ok(Vec::new()),
            }
        }
        async fn newest(&mut self) -> Result<Option<String>> {
            *self
                .newest_asked
                .lock()
                .map_err(|_| anyhow::anyhow!("poisoned"))? += 1;
            Ok(self.newest.clone())
        }
        fn describe(&self) -> String {
            "a scripted database".to_string()
        }
    }

    fn poller(plan: Plan, reader: Scripted) -> Poller {
        let (events, _rx) = broadcast::channel(4);
        Poller::new(
            plan,
            Box::new(reader),
            "db".to_string(),
            Envelope::none(),
            "p".to_string(),
            events,
        )
    }

    fn ids(delivery: &Delivery) -> Vec<i64> {
        delivery
            .iter()
            .filter_map(|m| m["id"].as_i64())
            .collect()
    }

    // ---- Plan ----

    #[test]
    fn a_table_and_a_query_are_one_or_the_other() {
        let mut both = config(PollMode::Snapshot);
        both.query = Some("select 1".into());
        assert!(Plan::build(&both, "schema").is_err());
        let mut neither = config(PollMode::Snapshot);
        neither.table = None;
        assert!(Plan::build(&neither, "schema").is_err());
    }

    #[test]
    fn a_query_loses_its_trailing_semicolon_and_may_not_hold_two_statements() -> Result<()> {
        let mut config = config(PollMode::Snapshot);
        config.table = None;
        config.query = Some("select id from t ; ".into());
        let plan = Plan::build(&config, "schema")?;
        assert_eq!(plan.source, Source::Query("select id from t".into()));
        assert_eq!(plan.relation_sql(), "select id from t");

        config.query = Some("select 1; drop table t".into());
        assert!(Plan::build(&config, "schema").is_err());
        config.query = Some(" ; ".into());
        assert!(Plan::build(&config, "schema").is_err());
        Ok(())
    }

    #[test]
    fn zero_interval_and_zero_page_are_refused() {
        let mut interval = config(PollMode::Snapshot);
        interval.interval_secs = 0;
        assert!(Plan::build(&interval, "schema").is_err());
        let mut page = config(incremental());
        page.page_size = Some(0);
        assert!(Plan::build(&page, "schema").is_err());
    }

    #[test]
    fn a_projection_has_to_include_the_cursor_field() {
        let mut config = config(incremental());
        config.columns = vec!["value".into()];
        assert!(Plan::build(&config, "schema").is_err());
        config.columns = vec!["id".into(), "value".into()];
        assert!(Plan::build(&config, "schema").is_ok());
        config.columns = vec!["id".into(), "id".into()];
        assert!(Plan::build(&config, "schema").is_err());
    }

    #[test]
    fn the_relation_is_projected_and_a_query_is_wrapped_only_when_it_has_to_be() -> Result<()> {
        let mut config = config(PollMode::Snapshot);
        assert_eq!(
            Plan::build(&config, "schema")?.relation_sql(),
            r#"SELECT * FROM "readings""#
        );
        config.columns = vec!["id".into(), "value".into()];
        assert_eq!(
            Plan::build(&config, "schema")?.relation_sql(),
            r#"SELECT "id", "value" FROM "readings""#
        );
        config.table = None;
        config.query = Some("select * from t".into());
        assert_eq!(
            Plan::build(&config, "schema")?.relation_sql(),
            r#"SELECT "id", "value" FROM (select * from t) AS s"#
        );
        Ok(())
    }

    // ---- cut_at_last_value ----

    #[test]
    fn a_short_page_is_kept_whole() {
        let (rows, warn) = cut_at_last_value(vec![fetched(1), fetched(1)], 3);
        assert_eq!(rows.len(), 2);
        assert!(!warn);
    }

    #[test]
    fn a_full_page_is_cut_before_its_last_value() {
        let page = vec![fetched(1), fetched(2), fetched(2)];
        let (rows, warn) = cut_at_last_value(page, 3);
        assert_eq!(rows, vec![fetched(1)]);
        assert!(!warn);
    }

    #[test]
    fn a_full_page_of_distinct_values_loses_only_its_last_row() {
        let page = vec![fetched(1), fetched(2), fetched(3)];
        let (rows, _) = cut_at_last_value(page, 3);
        assert_eq!(rows, vec![fetched(1), fetched(2)]);
    }

    #[test]
    fn a_page_of_one_value_cannot_be_cut_and_says_so() {
        let page = vec![fetched(7), fetched(7), fetched(7)];
        let (rows, warn) = cut_at_last_value(page, 3);
        assert_eq!(rows.len(), 3);
        assert!(warn);
    }

    // ---- Poller ----

    /// The whole of an incremental read: pages are asked for back to back
    /// while they come back full, the watermark is the last row handed on,
    /// and only a short page starts the interval.
    #[tokio::test(start_paused = true)]
    async fn an_incremental_read_pages_until_a_short_page_and_then_waits() -> Result<()> {
        let reader = Scripted::default();
        reader.pages.lock().map_err(|_| anyhow::anyhow!("poisoned"))?.extend([
            Ok(vec![fetched(1), fetched(2), fetched(3)]),
            Ok(vec![fetched(3), fetched(4)]),
            Ok(vec![fetched(5)]),
        ]);
        let mut poller = poller(Plan::build(&config(incremental()), "schema")?, reader.clone());

        // the full page is cut before its last value...
        assert_eq!(ids(&poller.next().await?), [1]);
        assert_eq!(ids(&poller.next().await?), [2]);
        assert_eq!(poller.watermark(), Some("2"));
        // ...and the next page is asked for straight away, above it
        assert_eq!(ids(&poller.next().await?), [3]);
        assert_eq!(ids(&poller.next().await?), [4]);
        assert_eq!(poller.watermark(), Some("4"));
        assert_eq!(
            *reader.asked.lock().map_err(|_| anyhow::anyhow!("poisoned"))?,
            vec![None, Some("2".to_string())]
        );

        // the page came back short, so the third read is only due after the
        // interval — a call now sits in the sleep rather than reading
        let mut third = Box::pin(poller.next());
        assert!(
            tokio::time::timeout(Duration::from_secs(9), &mut third)
                .await
                .is_err(),
            "read before the interval"
        );
        tokio::time::advance(Duration::from_secs(2)).await;
        let delivery = third.await?;
        assert_eq!(ids(&delivery), [5]);
        assert_eq!(
            reader.asked.lock().map_err(|_| anyhow::anyhow!("poisoned"))?.last(),
            Some(&Some("4".to_string()))
        );
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn starting_from_the_newest_asks_the_reader_once_and_reads_above_it() -> Result<()> {
        let reader = Scripted {
            newest: Some("41".into()),
            ..Scripted::default()
        };
        reader
            .pages
            .lock()
            .map_err(|_| anyhow::anyhow!("poisoned"))?
            .extend([Ok(vec![fetched(42)]), Ok(vec![fetched(43)])]);
        let mut config = config(PollMode::Incremental {
            field: "id".into(),
            start_from: None,
            lag_secs: None,
        });
        config.interval_secs = 1;
        let mut poller = poller(Plan::build(&config, "schema")?, reader.clone());

        assert_eq!(ids(&poller.next().await?), [42]);
        assert_eq!(ids(&poller.next().await?), [43]);
        assert_eq!(
            *reader.asked.lock().map_err(|_| anyhow::anyhow!("poisoned"))?,
            vec![Some("41".to_string()), Some("42".to_string())]
        );
        assert_eq!(
            *reader.newest_asked.lock().map_err(|_| anyhow::anyhow!("poisoned"))?,
            1
        );
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn an_empty_relation_started_from_the_newest_reads_from_the_beginning() -> Result<()> {
        let reader = Scripted::default();
        reader
            .pages
            .lock()
            .map_err(|_| anyhow::anyhow!("poisoned"))?
            .push_back(Ok(vec![fetched(1)]));
        let mut config = config(PollMode::Incremental {
            field: "id".into(),
            start_from: Some(StartFrom::Newest),
            lag_secs: None,
        });
        config.interval_secs = 1;
        let mut poller = poller(Plan::build(&config, "schema")?, reader.clone());
        assert_eq!(ids(&poller.next().await?), [1]);
        assert_eq!(
            *reader.asked.lock().map_err(|_| anyhow::anyhow!("poisoned"))?,
            vec![None]
        );
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn a_snapshot_hands_on_every_row_every_read() -> Result<()> {
        let reader = Scripted::default();
        reader
            .snapshots
            .lock()
            .map_err(|_| anyhow::anyhow!("poisoned"))?
            .extend([
                vec![json!({"id": 1}), json!({"id": 2})],
                vec![json!({"id": 1}), json!({"id": 2}), json!({"id": 3})],
            ]);
        let mut config = config(PollMode::Snapshot);
        config.max_batch = Some(10);
        let mut poller = poller(Plan::build(&config, "schema")?, reader);

        assert_eq!(ids(&poller.next().await?), [1, 2]);
        // the interval, then everything again, including what was there before
        let mut second = Box::pin(poller.next());
        assert!(
            tokio::time::timeout(Duration::from_secs(9), &mut second)
                .await
                .is_err()
        );
        tokio::time::advance(Duration::from_secs(2)).await;
        assert_eq!(ids(&second.await?), [1, 2, 3]);
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn max_batch_groups_rows_already_read_and_never_waits_for_more() -> Result<()> {
        let reader = Scripted::default();
        reader
            .pages
            .lock()
            .map_err(|_| anyhow::anyhow!("poisoned"))?
            .push_back(Ok(vec![fetched(1), fetched(2)]));
        let mut config = config(incremental());
        config.max_batch = Some(5);
        let mut poller = poller(Plan::build(&config, "schema")?, reader);
        // two rows, one batch — not a wait for three more
        assert_eq!(ids(&poller.next().await?), [1, 2]);
        Ok(())
    }

    /// A read that fails is retried on the backoff schedule, the watermark is
    /// untouched, and the next read asks the same question again.
    #[tokio::test(start_paused = true)]
    async fn a_failed_read_is_retried_without_moving_the_watermark() -> Result<()> {
        let reader = Scripted::default();
        reader.pages.lock().map_err(|_| anyhow::anyhow!("poisoned"))?.extend([
            Ok(vec![fetched(1)]),
            Err("connection reset".to_string()),
            Ok(vec![fetched(2)]),
        ]);
        let mut config = config(incremental());
        config.interval_secs = 1;
        let mut poller = poller(Plan::build(&config, "schema")?, reader.clone());
        assert_eq!(ids(&poller.next().await?), [1]);
        // the failure, the backoff, then the retry
        assert_eq!(ids(&poller.next().await?), [2]);
        assert_eq!(
            *reader.asked.lock().map_err(|_| anyhow::anyhow!("poisoned"))?,
            vec![None, Some("1".to_string()), Some("1".to_string())]
        );
        Ok(())
    }

    #[tokio::test(start_paused = true)]
    async fn the_envelope_carries_the_connection_and_when_the_read_started() -> Result<()> {
        let reader = Scripted::default();
        reader
            .pages
            .lock()
            .map_err(|_| anyhow::anyhow!("poisoned"))?
            .push_back(Ok(vec![fetched(1)]));
        let (events, _rx) = broadcast::channel(4);
        let envelope = Envelope::new(
            Some(&kayak_core::config::EnvelopeConfig::Merge { meta: None }),
            vec![("input", json!("postgres"))],
        );
        let mut poller = Poller::new(
            Plan::build(&config(incremental()), "schema")?,
            Box::new(reader),
            "warehouse".to_string(),
            envelope,
            "p".to_string(),
            events,
        );
        let delivery = poller.next().await?;
        let meta = &delivery[0]["_meta"];
        assert_eq!(meta["connection"], json!("warehouse"));
        assert_eq!(meta["input"], json!("postgres"));
        assert!(meta["polled_at"].is_string());
        assert_eq!(delivery[0]["id"], json!(1));
        Ok(())
    }
}
