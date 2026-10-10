//! Reading a database on a timer: the declaration shared by every SQL input.
//!
//! Every other input is *reached* by its messages — a broker pushes, a device
//! publishes, a client posts. A database does none of that, so an input over
//! one has to ask, and asking on a timer is the whole of this design. What
//! is declared here is what to ask for and how often; the polling loop itself
//! — the schedule, the paging, the watermark — lives in the server crate and
//! is shared by every database that gets an input, the way [`crate::columns`]
//! is shared by every database that gets an output. This module is the mirror
//! of that one: the postgres and clickhouse inputs declare no polling concepts
//! of their own, only how their server spells a query.
//!
//! # A table and a query are the same thing
//!
//! `table` and `query` are two ways of naming a *relation*, and everything
//! else is applied around whichever was given: the projection, the cursor
//! condition, the ordering and the page limit are all wrapped round it as a
//! subquery. That is what makes an incremental read of a hand-written query
//! possible without a placeholder convention — the query is the source, not
//! the whole statement, and the input owns the `WHERE` and the `ORDER BY`. A
//! raw query that had to carry its own cursor condition would silently
//! re-read the whole table every tick the moment somebody forgot it.
//!
//! # The watermark is the design
//!
//! [`PollMode::Incremental`] reads rows past a **watermark** — the highest
//! value of `field` handed on so far — and everything worth knowing about the
//! mode is a consequence of where that value lives and when it moves:
//!
//! - **It lives in memory.** A restart starts over from `start_from`, so an
//!   incremental input is *at least once* across a restart and nothing here
//!   pretends otherwise. A durable watermark is a checkpoint file with the
//!   same shape the history store's would have, and it is deliberately not
//!   built until the in-memory one has proven the mode.
//! - **It moves when rows are handed on**, not when they are delivered. The
//!   run loop acknowledges a batch whether or not its outputs succeeded — see
//!   `kayak::inputs::ack` — so tying the watermark to the acknowledgement
//!   would buy nothing today, and `ack: on_delivery` is refused rather than
//!   accepted as a promise the input cannot keep.
//! - **Ties are handled at the page boundary.** A page is cut before the last
//!   distinct cursor value it holds, so rows sharing one value that straddle
//!   two pages are read whole rather than half. A page whose every row shares
//!   the value cannot be cut and is handed on as it is, with a warning.
//! - **Rows that commit late are never seen.** A row written with a cursor
//!   value below the watermark — a long transaction, a clock behind the
//!   others — is behind the input by the time it is visible. `lag_secs` holds
//!   the input back from the current moment to give such rows time to land;
//!   it cannot make a polling input into change-data capture, and the docs
//!   say so rather than the code pretending.
//! - **Deletes are invisible and updates only as visible as `field` makes
//!   them.** A row that is deleted was already handed on; a row that is
//!   updated is read again only if the update moves its cursor.
//!
//! [`PollMode::Snapshot`] has no watermark: every tick reads the whole
//! relation and hands every row on. That is the reference-data case — a table
//! of recipes or thresholds that a `remember` transform keeps current — and it
//! reads the relation in one query, so it is for tables that fit in memory.
//!
//! # What is deliberately not here
//!
//! - **No `exclude` list.** `columns` is a projection and generates the select
//!   list; excluding would need the table's own column list to subtract from,
//!   and a `map` transform drops fields without it. The one honest case — a
//!   blob column not worth transferring — is what `columns` is for.
//! - **No per-row acknowledgement and no delete detection.** See above.
//! - **No unbounded page.** `page_size` bounds what one query returns and what
//!   the input holds, the same rule every state bucket follows.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Rows per query when the config doesn't say.
///
/// Small enough that a first read of a large table is a series of requests
/// rather than one that pulls the table into memory, and large enough that a
/// catch-up is not a query per handful of rows.
pub const DEFAULT_PAGE_SIZE: usize = 1000;

/// What to read, how often to read it, and whether to continue from the last
/// read. All SQL inputs use these fields.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SqlPollConfig {
    /// The table or view to read, as `name` or `schema.name`. Give exactly one
    /// of `table` and `query`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table: Option<String>,
    /// A `SELECT` to read in place of a table. The input puts the query in a
    /// subquery. It adds the cursor condition, the `ORDER BY` and the page
    /// limit outside the subquery. Thus, the query needs no placeholder and no
    /// `ORDER BY`.
    ///
    /// Write one statement with no semicolon at the end. You can use all SQL
    /// that the server accepts in a subquery, for example `WITH`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query: Option<String>,
    /// The columns to read, in order. Leave it empty to read all columns. For
    /// an incremental input, the list must include `field`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<String>,
    /// The time between two reads, in seconds. The time starts at the end of a
    /// read. Thus, two reads never overlap. The first read occurs when the
    /// pipeline starts.
    pub interval_secs: u64,
    /// Whether each read returns all rows (`snapshot`) or only the rows after
    /// the last read (`incremental`).
    pub mode: PollMode,
    /// The maximum number of rows that one query returns. The default is
    /// 1000. When a page is full, the input reads the next page immediately.
    /// The interval starts after a page that is not full. A `snapshot` ignores
    /// this field and reads all rows in one query.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_size: Option<usize>,
    /// The maximum number of rows in one batch. The default is 1. The input
    /// puts rows that it already read into a batch. It does not wait for more
    /// rows to fill a batch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_batch: Option<usize>,
}

/// Whether a read returns all rows or only the new rows.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PollMode {
    /// Each read returns all rows, in one query. Use it for reference data,
    /// for example a table of recipes for a `remember` transform. A snapshot
    /// has no page limit. Use it only for a table that fits in memory.
    Snapshot,
    /// Each read returns only the rows with a `field` value above the highest
    /// value that the input already sent. The input reads pages in the order
    /// of the field. The value of the field must increase, for example an id
    /// or an `updated_at`. Put an index on the field. Without an index, each
    /// read scans the full table.
    Incremental {
        /// The column that the input follows. The watermark is the highest
        /// value that the input sent. Each read asks for the rows above the
        /// watermark. The input does not read rows where the column is `null`.
        field: String,
        /// Where the first read starts. With `newest`, the input reads only
        /// the rows added after the pipeline started. With `oldest`, it reads
        /// all rows first and then follows the table. The default is `newest`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        start_from: Option<StartFrom>,
        /// For a timestamp column: the time to stay behind the current time,
        /// in seconds. A later read gets the rows that are less than this time
        /// before `now()`. This gives late transactions time to commit. Do not
        /// use it with a numeric column. The server refuses the query.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        lag_secs: Option<u64>,
    },
}

/// Where the first read of an incremental input starts.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum StartFrom {
    /// From the start. The first read returns all rows, one page at a time.
    Oldest,
    /// From now. The first read finds the highest value of the field and
    /// returns only the rows above it. This is the default.
    #[default]
    Newest,
}

impl SqlPollConfig {
    /// Rows per query, within what the config said.
    #[must_use]
    pub fn page_size(&self) -> usize {
        self.page_size.unwrap_or(DEFAULT_PAGE_SIZE)
    }

    /// Where an incremental input starts, or `None` for a snapshot.
    #[must_use]
    pub fn start_from(&self) -> Option<StartFrom> {
        match &self.mode {
            PollMode::Snapshot => None,
            PollMode::Incremental { start_from, .. } => Some(start_from.unwrap_or_default()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_snapshot_over_a_table_is_the_smallest_spelling() -> Result<(), serde_json::Error> {
        let config: SqlPollConfig = serde_json::from_value(json!({
            "table": "recipes",
            "interval_secs": 60,
            "mode": {"type": "snapshot"}
        }))?;
        assert_eq!(config.table.as_deref(), Some("recipes"));
        assert_eq!(config.mode, PollMode::Snapshot);
        assert_eq!(config.page_size(), DEFAULT_PAGE_SIZE);
        assert_eq!(config.start_from(), None);
        // and it comes back out as it went in — no nulls, no defaults written
        assert_eq!(
            serde_json::to_value(&config)?,
            json!({
                "table": "recipes",
                "interval_secs": 60,
                "mode": {"type": "snapshot"}
            })
        );
        Ok(())
    }

    #[test]
    fn an_incremental_read_defaults_to_the_newest_rows() -> Result<(), serde_json::Error> {
        let config: SqlPollConfig = serde_json::from_value(json!({
            "query": "select id, total from orders",
            "interval_secs": 5,
            "mode": {"type": "incremental", "field": "id"}
        }))?;
        assert_eq!(config.start_from(), Some(StartFrom::Newest));
        let PollMode::Incremental { lag_secs, .. } = &config.mode else {
            panic!("incremental");
        };
        assert_eq!(*lag_secs, None);
        Ok(())
    }

    #[test]
    fn the_mode_is_a_tagged_choice_not_a_flag() {
        let bare = serde_json::from_value::<SqlPollConfig>(json!({
            "table": "t",
            "interval_secs": 5,
            "incremental": true
        }));
        assert!(bare.is_err(), "a bool is not a mode");
    }
}
