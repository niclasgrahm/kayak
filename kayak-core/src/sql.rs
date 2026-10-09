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

/// What to read, how often, and whether to remember where the last read got
/// to. Shared verbatim by every SQL input — see the module docs.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct SqlPollConfig {
    /// the table or view to read, as `name` or `schema.name`. Exactly one of
    /// `table` and `query` is required.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub table: Option<String>,
    /// a `SELECT` to read instead of a table. It is the *source*, not the whole
    /// statement: the input wraps it as a subquery and adds the cursor
    /// condition, the ordering and the page limit itself, so an incremental
    /// query needs no placeholder and no `ORDER BY` of its own. One statement,
    /// no trailing semicolon; anything the server can put in a subquery
    /// (including a `WITH`) is fine.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub query: Option<String>,
    /// the columns to read, in the order they are listed. Empty reads every
    /// column the table or query has. An incremental input's `field` has to be
    /// among them.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<String>,
    /// how long to wait between reads, in seconds, counted from the end of one
    /// read to the start of the next — a read that takes longer than this
    /// never overlaps itself. The first read happens as soon as the pipeline
    /// starts.
    pub interval_secs: u64,
    /// whether every read returns the whole relation (`snapshot`) or only the
    /// rows past where the last read got to (`incremental`).
    pub mode: PollMode,
    /// most rows one query returns, and so the most an incremental read holds
    /// at once. A read that fills a page asks for the next one straight away
    /// until a page comes back short; only then does the interval start.
    /// Defaults to 1000. Ignored by `snapshot`, which reads the relation
    /// whole.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub page_size: Option<usize>,
    /// most rows to put in one batch. Defaults to 1 — one message per batch,
    /// which is what every input does unless asked otherwise. Rows already
    /// read are grouped up to this many; the input never waits for a batch to
    /// fill.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_batch: Option<usize>,
}

/// Whether a read returns everything or only what is new.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum PollMode {
    /// Every row, every read, in one query. For reference data — a table the
    /// pipeline remembers rather than a stream it follows — and for relations
    /// that fit in memory, since there is no page limit on a snapshot.
    Snapshot,
    /// Only rows whose `field` is past the highest value already handed on,
    /// read in pages ordered by that field. The field has to be one that
    /// grows — an id, an `updated_at` — and it should be indexed, or every
    /// read is a scan of the whole table.
    Incremental {
        /// the column the input follows: the watermark is the highest value
        /// of it handed on so far, and each read asks for rows above that.
        /// Rows where it is `null` are never read.
        field: String,
        /// where the first read starts: `newest` reads only rows added after
        /// the pipeline started, `oldest` reads the whole relation first and
        /// then follows it. Defaults to `newest` — replaying a whole table
        /// into a pipeline is the surprising outcome and the one to ask for.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        start_from: Option<StartFrom>,
        /// how far behind the current moment to stay, in seconds, for a
        /// timestamp cursor: rows above the watermark but within this many
        /// seconds of `now()` are left for a later read, giving a transaction
        /// that commits late time to land. Meaningless on a numeric cursor and
        /// refused by the server on one.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        lag_secs: Option<u64>,
    },
}

/// Where an incremental input's first read starts.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum StartFrom {
    /// From the beginning: the first read returns every row, page by page.
    Oldest,
    /// From now: the first read finds the highest value of the field and
    /// returns only rows above it. The default.
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
