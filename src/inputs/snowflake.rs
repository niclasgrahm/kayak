//! The snowflake input: [`super::poll::Poller`] over a [`SnowflakeReader`],
//! reading through the same [`crate::snowflake::Session`] the output writes
//! through.
//!
//! The same shape as the clickhouse input, with three things spelled the way
//! this server spells them:
//!
//! - **The row is assembled here.** Every other database renders a row as JSON
//!   and hands it over; the SQL API hands over arrays of strings and a description
//!   of the columns. [`decode`] is what turns the two back into a message, and it
//!   is deliberately the smallest thing that can: the session has already made
//!   the server render every date and time as text, so all that is left is which
//!   texts are really numbers, booleans or JSON.
//! - **A number too precise for a JSON number stays a string.** A message's
//!   numbers go through an `f64`, so `NUMBER(38, 9)` would lose digits invisibly.
//!   Whether that can happen is a property of the column, not of the value, which
//!   is why a decimal column is one type or the other for its whole life. A whole
//!   number is the exception, and has to be: the default `NUMBER(38, 0)` is what
//!   every `INTEGER` is declared as, and all of them as strings would be absurd.
//!   So a whole number is a number until it outgrows 64 bits.
//! - **The cursor is rendered with an explicit format and parsed back with the
//!   same one.** Rounded to the default three digits, a watermark would read the
//!   rows it was taken from again forever. A `FLOAT` has no format that is known
//!   to round trip, so following one is refused along with the types that do not
//!   order at all.

use anyhow::{Context, Result, anyhow, bail};
use kayak_core::config::SnowflakeInputConfig;
use kayak_core::sql::{PollMode, SqlPollConfig};
use serde_json::{Map, Number, Value};

use crate::{
    BuildCtx,
    inputs::{
        BuildInput, InputSource, ack,
        poll::{Fetched, Plan, Poller, Reader},
    },
    snowflake::{
        Column, DATE_FORMAT, NAIVE_FORMAT, ResultSet, Session, TIME_FORMAT, ZONED_FORMAT, fold,
    },
};

/// The column the cursor's text travels under, beside the row's own columns.
const CURSOR_COLUMN: &str = "__KAYAK_CURSOR";
/// Fifteen significant digits is what an `f64` holds exactly whatever they are.
const MAX_EXACT_PRECISION: u32 = 15;

impl BuildInput for SnowflakeInputConfig {
    fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn InputSource>> {
        ack::require_receipt_only(ctx.ack_mode(), "snowflake")?;
        let plan = Plan::build(&folded(self.poll), "schema")
            .context("the snowflake input cannot be built")?;
        let session =
            Session::build(ctx, &self.connection).context("the snowflake input cannot be built")?;
        let reader = SnowflakeReader {
            session,
            sql: Statements::new(&plan),
            cursor_type: None,
        };
        let envelope = ctx.envelope("snowflake", Some(&self.connection));
        Ok(Box::new(
            Poller::new(
                plan.schedule(),
                Box::new(reader),
                vec![("connection", Value::String(self.connection))],
                envelope,
                ctx.pipeline_id.clone(),
                ctx.events.clone(),
            )
            .resuming_from(ctx.restored_position.as_ref()),
        ))
    }
}

/// The config with every name in it the way Snowflake will resolve it. A `query`
/// is left alone: it is SQL, and the server reads it the way it reads any.
fn folded(mut poll: SqlPollConfig) -> SqlPollConfig {
    poll.table = poll.table.as_deref().map(fold);
    poll.columns = poll.columns.iter().map(|name| fold(name)).collect();
    if let PollMode::Incremental { field, .. } = &mut poll.mode {
        *field = fold(field);
    }
    poll
}

/// How a cursor of one type becomes text, and how that text becomes it again.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CursorType {
    /// The conversion function and the format it reads, for a type with one.
    formatted: Option<(&'static str, &'static str)>,
    /// The type a value is cast to otherwise.
    cast: String,
}

impl CursorType {
    /// What the probe found out, or why a read cannot follow that column.
    pub fn of(column: &Column) -> Result<Self> {
        let formatted = |conversion, format| Self {
            formatted: Some((conversion, format)),
            cast: String::new(),
        };
        Ok(match column.kind().as_str() {
            "fixed" => Self {
                formatted: None,
                cast: format!(
                    "NUMBER({}, {})",
                    column.precision.unwrap_or(38),
                    column.scale.unwrap_or(0)
                ),
            },
            "text" => Self {
                formatted: None,
                cast: "VARCHAR".to_string(),
            },
            "date" => formatted("TO_DATE", DATE_FORMAT),
            "time" => formatted("TO_TIME", TIME_FORMAT),
            "timestamp_ntz" => formatted("TO_TIMESTAMP_NTZ", NAIVE_FORMAT),
            "timestamp_ltz" => formatted("TO_TIMESTAMP_LTZ", ZONED_FORMAT),
            "timestamp_tz" => formatted("TO_TIMESTAMP_TZ", ZONED_FORMAT),
            other => bail!(
                "the cursor column '{}' is of type {other}, which an incremental read cannot \
                 follow; it needs a number, text, a date, a time or a timestamp",
                column.name
            ),
        })
    }

    /// The expression rendering `expression` as the watermark's text.
    fn text(&self, expression: &str) -> String {
        match self.formatted {
            Some((_, format)) => format!("TO_VARCHAR({expression}, '{format}')"),
            None => format!("TO_VARCHAR({expression})"),
        }
    }

    /// The expression reading a bound watermark back.
    fn bound(&self) -> String {
        match self.formatted {
            Some((conversion, format)) => format!("{conversion}(?, '{format}')"),
            None => format!("CAST(? AS {})", self.cast),
        }
    }
}

/// The SQL, rendered once from the plan. Pure, and tested without a server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Statements {
    relation: String,
    field: Option<String>,
    lag_secs: Option<u64>,
}

impl Statements {
    #[must_use]
    pub fn new(plan: &Plan) -> Self {
        Self {
            relation: plan.relation_sql(),
            field: plan
                .cursor
                .as_ref()
                .map(|cursor| format!("q.{}", cursor.field.quoted())),
            lag_secs: plan
                .cursor
                .as_ref()
                .and_then(|cursor| cursor.lag)
                .map(|lag| lag.as_secs()),
        }
    }

    fn field(&self) -> Result<&str> {
        self.field
            .as_deref()
            .ok_or_else(|| anyhow!("a snapshot has no cursor field"))
    }

    fn lag(&self) -> Result<String> {
        Ok(match self.lag_secs {
            Some(lag) => format!(
                " AND {} <= DATEADD(second, -{lag}, CURRENT_TIMESTAMP())",
                self.field()?
            ),
            None => String::new(),
        })
    }

    #[must_use]
    pub fn snapshot(&self) -> String {
        format!("SELECT * FROM ({}) AS q", self.relation)
    }

    /// The cursor column alone and no rows: its type, from the metadata.
    pub fn probe(&self) -> Result<String> {
        Ok(format!(
            "SELECT {} FROM ({}) AS q LIMIT 0",
            self.field()?,
            self.relation
        ))
    }

    pub fn first_page(&self, cursor: &CursorType, limit: usize) -> Result<String> {
        let field = self.field()?;
        Ok(format!(
            "SELECT q.*, {text} AS \"{CURSOR_COLUMN}\" FROM ({relation}) AS q \
             WHERE {field} IS NOT NULL{lag} ORDER BY {field} LIMIT {limit}",
            text = cursor.text(field),
            relation = self.relation,
            lag = self.lag()?
        ))
    }

    /// A page above the watermark, which is the statement's one bind.
    pub fn next_page(&self, cursor: &CursorType, limit: usize) -> Result<String> {
        let field = self.field()?;
        Ok(format!(
            "SELECT q.*, {text} AS \"{CURSOR_COLUMN}\" FROM ({relation}) AS q \
             WHERE {field} > {bound}{lag} ORDER BY {field} LIMIT {limit}",
            text = cursor.text(field),
            relation = self.relation,
            bound = cursor.bound(),
            lag = self.lag()?
        ))
    }

    pub fn newest(&self, cursor: &CursorType) -> Result<String> {
        Ok(format!(
            "SELECT {text} AS \"{CURSOR_COLUMN}\" FROM ({relation}) AS q",
            text = cursor.text(&format!("MAX({})", self.field()?)),
            relation = self.relation
        ))
    }
}

/// A number as JSON if it can be held exactly, and otherwise its own text.
fn decode_fixed(text: &str, column: &Column) -> Value {
    if column.scale.unwrap_or(0) == 0 {
        return if let Ok(number) = text.parse::<i64>() {
            Value::from(number)
        } else if let Ok(number) = text.parse::<u64>() {
            Value::from(number)
        } else {
            Value::String(text.to_string())
        };
    }
    if column.precision.unwrap_or(u32::MAX) <= MAX_EXACT_PRECISION {
        return decode_float(text);
    }
    Value::String(text.to_string())
}

/// `NaN` and the infinities are values here and have no spelling in JSON, so
/// they stay the text the server wrote.
fn decode_float(text: &str) -> Value {
    text.parse::<f64>()
        .ok()
        .and_then(Number::from_f64)
        .map_or_else(|| Value::String(text.to_string()), Value::Number)
}

/// One value of a row. A type this does not recognise reads as its text rather
/// than failing the read: that is what it is, and a new type should not stop a
/// pipeline that may not even look at it.
#[must_use]
pub fn decode(text: &str, column: &Column) -> Value {
    match column.kind().as_str() {
        "fixed" => decode_fixed(text, column),
        "real" | "float" | "double" => decode_float(text),
        "boolean" => match text {
            "true" | "TRUE" | "1" => Value::Bool(true),
            "false" | "FALSE" | "0" => Value::Bool(false),
            other => Value::String(other.to_string()),
        },
        "variant" | "object" | "array" => {
            serde_json::from_str(text).unwrap_or_else(|_| Value::String(text.to_string()))
        }
        _ => Value::String(text.to_string()),
    }
}

/// A result as messages, with the cursor column taken off each.
fn fetched(result: ResultSet) -> Vec<Fetched> {
    let ResultSet { columns, rows } = result;
    rows.into_iter()
        .map(|values| {
            let mut row = Map::new();
            let mut cursor = None;
            for (column, value) in columns.iter().zip(values) {
                if column.name == CURSOR_COLUMN {
                    cursor = value;
                } else {
                    let decoded = value.map_or(Value::Null, |text| decode(&text, column));
                    row.insert(column.name.clone(), decoded);
                }
            }
            Fetched {
                row: Value::Object(row),
                cursor,
            }
        })
        .collect()
}

pub struct SnowflakeReader {
    session: Session,
    sql: Statements,
    /// Asked once, since what a column is does not change under a running read
    /// in any way a cast could survive anyway.
    cursor_type: Option<CursorType>,
}

impl SnowflakeReader {
    async fn cursor_type(&mut self) -> Result<CursorType> {
        if let Some(known) = &self.cursor_type {
            return Ok(known.clone());
        }
        let probe = self.sql.probe()?;
        let result = self
            .session
            .execute(&probe, &[])
            .await
            .context("failed to find the cursor column's type")?;
        let column = result
            .columns
            .first()
            .ok_or_else(|| anyhow!("the cursor probe returned no column"))?;
        let cursor = CursorType::of(column)?;
        self.cursor_type = Some(cursor.clone());
        Ok(cursor)
    }
}

#[async_trait::async_trait]
impl Reader for SnowflakeReader {
    async fn snapshot(&mut self) -> Result<Vec<Value>> {
        let sql = self.sql.snapshot();
        let result = self.session.execute(&sql, &[]).await?;
        Ok(fetched(result).into_iter().map(|row| row.row).collect())
    }

    async fn page(&mut self, after: Option<&str>, limit: usize) -> Result<Vec<Fetched>> {
        // asked before the first page as well as the rest, so that a column
        // this cannot follow is said at once rather than after one good read
        let cursor = self.cursor_type().await?;
        let result = match after {
            None => {
                let sql = self.sql.first_page(&cursor, limit)?;
                self.session.execute(&sql, &[]).await?
            }
            Some(watermark) => {
                let sql = self.sql.next_page(&cursor, limit)?;
                self.session.execute(&sql, &[watermark]).await?
            }
        };
        Ok(fetched(result))
    }

    async fn newest(&mut self) -> Result<Option<String>> {
        let cursor = self.cursor_type().await?;
        let sql = self.sql.newest(&cursor)?;
        let result = self.session.execute(&sql, &[]).await?;
        Ok(result
            .rows
            .into_iter()
            .next()
            .and_then(|row| row.into_iter().next().flatten()))
    }

    fn describe(&self) -> String {
        self.session.describe()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inputs::Delivery;
    use crate::snowflake::fake;
    use kayak_core::config::AckMode;
    use kayak_core::sql::StartFrom;
    use serde_json::json;
    use std::time::Duration;

    fn poll(mode: PollMode) -> SqlPollConfig {
        SqlPollConfig {
            table: Some("readings".into()),
            query: None,
            columns: Vec::new(),
            interval_secs: 3600,
            mode,
            page_size: Some(2),
            max_batch: None,
        }
    }

    fn incremental(start_from: StartFrom, lag_secs: Option<u64>) -> PollMode {
        PollMode::Incremental {
            field: "recorded_at".into(),
            start_from: Some(start_from),
            lag_secs,
        }
    }

    fn statements(poll: SqlPollConfig) -> Result<Statements> {
        Ok(Statements::new(&Plan::build(&folded(poll), "schema")?))
    }

    fn column(kind: &str, precision: Option<u32>, scale: Option<u32>) -> Column {
        Column {
            name: "C".into(),
            type_name: kind.into(),
            precision,
            scale,
        }
    }

    fn build(poll: SqlPollConfig, url: &str) -> Result<Box<dyn InputSource>> {
        fake::with_ctx(fake::connection(url), |ctx| {
            SnowflakeInputConfig {
                connection: "warehouse".into(),
                poll,
            }
            .build(ctx)
        })
    }

    async fn next(input: &mut Box<dyn InputSource>) -> Result<Delivery> {
        tokio::time::timeout(Duration::from_secs(10), input.next()).await?
    }

    fn timestamp() -> Result<CursorType> {
        CursorType::of(&column("timestamp_ntz", None, Some(9)))
    }

    #[test]
    fn a_snapshot_is_the_whole_relation() -> Result<()> {
        assert_eq!(
            statements(poll(PollMode::Snapshot))?.snapshot(),
            r#"SELECT * FROM (SELECT * FROM "READINGS") AS q"#
        );
        Ok(())
    }

    /// `readings` means the table `CREATE TABLE readings` made, and a query is
    /// SQL the server reads for itself.
    #[test]
    fn names_fold_and_a_query_does_not() -> Result<()> {
        let mut named = poll(incremental(StartFrom::Oldest, None));
        named.columns = vec!["recorded_at".into(), "value".into()];
        let sql = statements(named)?;
        assert_eq!(
            sql.probe()?,
            r#"SELECT q."RECORDED_AT" FROM (SELECT "RECORDED_AT", "VALUE" FROM "READINGS") AS q LIMIT 0"#
        );

        let mut query = poll(PollMode::Snapshot);
        query.table = None;
        query.query = Some("select a as \"lower\" from t;".into());
        assert_eq!(
            statements(query)?.snapshot(),
            r#"SELECT * FROM (select a as "lower" from t) AS q"#
        );
        Ok(())
    }

    #[test]
    fn the_pages_carry_the_cursor_beside_the_row() -> Result<()> {
        let sql = statements(poll(incremental(StartFrom::Oldest, None)))?;
        let cursor = timestamp()?;
        assert_eq!(
            sql.first_page(&cursor, 20)?,
            r#"SELECT q.*, TO_VARCHAR(q."RECORDED_AT", 'YYYY-MM-DD"T"HH24:MI:SS.FF9') AS "__KAYAK_CURSOR" FROM (SELECT * FROM "READINGS") AS q WHERE q."RECORDED_AT" IS NOT NULL ORDER BY q."RECORDED_AT" LIMIT 20"#
        );
        assert_eq!(
            sql.next_page(&cursor, 20)?,
            r#"SELECT q.*, TO_VARCHAR(q."RECORDED_AT", 'YYYY-MM-DD"T"HH24:MI:SS.FF9') AS "__KAYAK_CURSOR" FROM (SELECT * FROM "READINGS") AS q WHERE q."RECORDED_AT" > TO_TIMESTAMP_NTZ(?, 'YYYY-MM-DD"T"HH24:MI:SS.FF9') ORDER BY q."RECORDED_AT" LIMIT 20"#
        );
        assert_eq!(
            sql.newest(&cursor)?,
            r#"SELECT TO_VARCHAR(MAX(q."RECORDED_AT"), 'YYYY-MM-DD"T"HH24:MI:SS.FF9') AS "__KAYAK_CURSOR" FROM (SELECT * FROM "READINGS") AS q"#
        );
        Ok(())
    }

    #[test]
    fn a_lag_holds_both_pages_back_from_now() -> Result<()> {
        let sql = statements(poll(incremental(StartFrom::Oldest, Some(45))))?;
        let cursor = timestamp()?;
        for page in [sql.first_page(&cursor, 10)?, sql.next_page(&cursor, 10)?] {
            assert!(
                page.contains(
                    r#"AND q."RECORDED_AT" <= DATEADD(second, -45, CURRENT_TIMESTAMP()) ORDER BY"#
                ),
                "{page}"
            );
        }
        Ok(())
    }

    /// The format the cursor is rendered with is the format it is read back
    /// with, for every type that has one — that pair is the round trip.
    #[test]
    fn a_cursor_is_read_back_the_way_it_was_written() -> Result<()> {
        for (kind, conversion, format) in [
            ("timestamp_ntz", "TO_TIMESTAMP_NTZ", NAIVE_FORMAT),
            ("TIMESTAMP_LTZ", "TO_TIMESTAMP_LTZ", ZONED_FORMAT),
            ("timestamp_tz", "TO_TIMESTAMP_TZ", ZONED_FORMAT),
            ("date", "TO_DATE", DATE_FORMAT),
            ("time", "TO_TIME", TIME_FORMAT),
        ] {
            let cursor = CursorType::of(&column(kind, None, None))?;
            assert_eq!(cursor.text("x"), format!("TO_VARCHAR(x, '{format}')"));
            assert_eq!(cursor.bound(), format!("{conversion}(?, '{format}')"));
            assert!(format.contains("FF9") || !kind.contains("time"), "{format}");
        }
        let number = CursorType::of(&column("FIXED", Some(10), Some(2)))?;
        assert_eq!(number.text("x"), "TO_VARCHAR(x)");
        assert_eq!(number.bound(), "CAST(? AS NUMBER(10, 2))");
        let whole = CursorType::of(&column("fixed", None, None))?;
        assert_eq!(whole.bound(), "CAST(? AS NUMBER(38, 0))");
        assert_eq!(
            CursorType::of(&column("text", None, None))?.bound(),
            "CAST(? AS VARCHAR)"
        );
        Ok(())
    }

    #[test]
    fn a_cursor_that_cannot_round_trip_or_order_is_refused() {
        for kind in ["real", "variant", "object", "array", "boolean", "binary"] {
            let Err(error) = CursorType::of(&column(kind, None, None)) else {
                panic!("a {kind} cursor was accepted");
            };
            assert!(format!("{error:#}").contains(kind), "{error:#}");
        }
    }

    #[test]
    fn a_value_reads_as_its_column_says() {
        let whole = column("fixed", Some(38), Some(0));
        assert_eq!(decode("42", &whole), json!(42));
        assert_eq!(decode("-42", &whole), json!(-42));
        assert_eq!(
            decode("9223372036854775808", &whole),
            json!(9_223_372_036_854_775_808_u64)
        );
        // past 64 bits, a JSON number here would be a different number
        assert_eq!(
            decode("123456789012345678901234567890", &whole),
            json!("123456789012345678901234567890")
        );

        let money = column("FIXED", Some(10), Some(2));
        assert_eq!(decode("12.50", &money), json!(12.5));
        let wide = column("fixed", Some(38), Some(9));
        assert_eq!(decode("1.123456789", &wide), json!("1.123456789"));

        let float = column("real", None, None);
        assert_eq!(decode("1.5", &float), json!(1.5));
        assert_eq!(decode("NaN", &float), json!("NaN"));
        assert_eq!(decode("inf", &float), json!("inf"));

        let boolean = column("BOOLEAN", None, None);
        assert_eq!(decode("true", &boolean), json!(true));
        assert_eq!(decode("false", &boolean), json!(false));

        for kind in ["variant", "OBJECT", "array"] {
            let structured = column(kind, None, None);
            assert_eq!(decode(r#"{"a":[1]}"#, &structured), json!({"a": [1]}));
            assert_eq!(decode("[1,2]", &structured), json!([1, 2]));
            assert_eq!(decode("not json", &structured), json!("not json"));
        }

        // the server has rendered these already, so they are text from here on
        for kind in [
            "text",
            "date",
            "time",
            "timestamp_tz",
            "timestamp_ntz",
            "binary",
            "geography",
        ] {
            assert_eq!(
                decode("2026-01-01", &column(kind, None, None)),
                json!("2026-01-01")
            );
        }
    }

    /// Whether a decimal is a number or a string depends on the column alone,
    /// so a value cannot change type on the data's say-so.
    #[test]
    fn a_decimal_column_is_one_type_for_every_value() {
        let wide = column("fixed", Some(38), Some(2));
        for text in ["1.50", "100000000000000000000.00"] {
            assert!(decode(text, &wide).is_string(), "{text}");
        }
        let narrow = column("fixed", Some(15), Some(2));
        for text in ["1.50", "9999999999999.99"] {
            assert!(decode(text, &narrow).is_number(), "{text}");
        }
    }

    #[test]
    fn the_cursor_column_is_taken_off_the_row_and_a_null_stays_one() {
        let result = ResultSet {
            columns: vec![
                column("fixed", None, None),
                Column {
                    name: CURSOR_COLUMN.into(),
                    ..column("text", None, None)
                },
            ],
            rows: vec![vec![Some("1".into()), Some("1".into())], vec![None, None]],
        };
        let rows = fetched(result);
        assert_eq!(rows[0].row, json!({"C": 1}));
        assert_eq!(rows[0].cursor.as_deref(), Some("1"));
        assert_eq!(rows[1].row, json!({"C": null}));
        assert_eq!(rows[1].cursor, None);
    }

    #[test]
    fn it_builds_against_a_server_that_is_not_running() -> Result<()> {
        build(
            poll(incremental(StartFrom::Newest, None)),
            "http://127.0.0.1:1",
        )?;
        Ok(())
    }

    #[test]
    fn on_delivery_is_refused() {
        let result = fake::with_ctx(fake::connection("http://127.0.0.1:1"), |ctx| {
            ctx.ack_mode = Some(AckMode::OnDelivery);
            SnowflakeInputConfig {
                connection: "warehouse".into(),
                poll: poll(PollMode::Snapshot),
            }
            .build(ctx)
        });
        let Err(error) = result else {
            panic!("a snowflake input built with `ack: on_delivery`");
        };
        assert!(format!("{error:#}").contains("snowflake"), "{error:#}");
    }

    #[test]
    fn it_needs_a_snowflake_connection() {
        let result = fake::with_ctx(fake::connection("http://127.0.0.1:1"), |ctx| {
            SnowflakeInputConfig {
                connection: "elsewhere".into(),
                poll: poll(PollMode::Snapshot),
            }
            .build(ctx)
        });
        assert!(result.is_err());
    }

    /// The whole loop against the wire: a snapshot is one statement's rows.
    #[tokio::test]
    async fn a_snapshot_reads_rows_into_messages() -> Result<()> {
        let (fake, url) = fake::serve().await;
        fake.reply(
            200,
            &fake::result(
                &[("ID", "fixed"), ("SENSOR", "text"), ("EXTRA", "variant")],
                &json!([["1", "a", "{\"x\":1}"], ["2", null, null]]),
            ),
        );
        let mut input = build(poll(PollMode::Snapshot), &url)?;
        let delivery = next(&mut input).await?;
        let messages: Vec<Value> = delivery.batch.iter().map(|m| (**m).clone()).collect();
        assert_eq!(
            messages[0],
            json!({"ID": 1, "SENSOR": "a", "EXTRA": {"x": 1}})
        );
        Ok(())
    }

    /// `start_from: newest` asks for the highest value once, then reads above
    /// it with the value bound rather than written into the statement.
    #[tokio::test]
    async fn an_incremental_read_starts_from_the_newest_and_binds_the_watermark() -> Result<()> {
        let (fake, url) = fake::serve().await;
        // the probe
        fake.reply(
            200,
            &fake::result(&[("RECORDED_AT", "timestamp_ntz")], &json!([])),
        );
        // newest
        fake.reply(
            200,
            &fake::result(
                &[(CURSOR_COLUMN, "text")],
                &json!([["2026-01-01T00:00:00.000000000"]]),
            ),
        );
        // one short page
        fake.reply(
            200,
            &fake::result(
                &[("ID", "fixed"), (CURSOR_COLUMN, "text")],
                &json!([["7", "2026-01-02T00:00:00.000000000"]]),
            ),
        );
        let mut input = build(poll(incremental(StartFrom::Newest, None)), &url)?;
        let delivery = next(&mut input).await?;
        assert_eq!(*delivery.batch[0], json!({"ID": 7}));

        let seen = fake.seen();
        assert_eq!(seen.len(), 3);
        assert!(
            seen[0].body["statement"]
                .as_str()
                .unwrap_or_default()
                .ends_with("LIMIT 0")
        );
        assert!(
            seen[1].body["statement"]
                .as_str()
                .unwrap_or_default()
                .contains("MAX(")
        );
        assert_eq!(
            seen[2].body["bindings"],
            json!({"1": {"type": "TEXT", "value": "2026-01-01T00:00:00.000000000"}})
        );
        Ok(())
    }

    /// An empty table has no newest row, so the read starts from the beginning
    /// of whatever arrives — and the first page has no watermark to bind.
    #[tokio::test]
    async fn an_empty_table_starts_with_no_watermark() -> Result<()> {
        let (fake, url) = fake::serve().await;
        fake.reply(
            200,
            &fake::result(&[("RECORDED_AT", "timestamp_tz")], &json!([])),
        );
        fake.reply(
            200,
            &fake::result(&[(CURSOR_COLUMN, "text")], &json!([[null]])),
        );
        fake.reply(
            200,
            &fake::result(
                &[("ID", "fixed"), (CURSOR_COLUMN, "text")],
                &json!([["1", "2026-01-02T00:00:00.000000000+00:00"]]),
            ),
        );
        let mut input = build(poll(incremental(StartFrom::Newest, None)), &url)?;
        next(&mut input).await?;
        let seen = fake.seen();
        assert!(seen[2].body.get("bindings").is_none(), "{}", seen[2].body);
        assert!(
            seen[2].body["statement"]
                .as_str()
                .unwrap_or_default()
                .contains("IS NOT NULL")
        );
        Ok(())
    }

    /// A column this cannot follow is said before any row has been handed on,
    /// rather than after one good page.
    #[tokio::test]
    async fn a_cursor_of_the_wrong_type_is_found_out_by_the_probe() -> Result<()> {
        let (fake, url) = fake::serve().await;
        let mut reader = SnowflakeReader {
            session: fake::with_ctx(fake::connection(&url), |ctx| {
                Session::build(ctx, "warehouse")
            })?,
            sql: statements(poll(incremental(StartFrom::Oldest, None)))?,
            cursor_type: None,
        };
        fake.reply(200, &fake::result(&[("RECORDED_AT", "real")], &json!([])));
        let Err(error) = reader.page(None, 10).await else {
            panic!("a float cursor was followed");
        };
        assert!(format!("{error:#}").contains("real"), "{error:#}");
        assert_eq!(fake.seen().len(), 1);
        Ok(())
    }

    /// The probe is one round trip for the life of the read.
    #[tokio::test]
    async fn the_cursor_type_is_asked_once() -> Result<()> {
        let (fake, url) = fake::serve().await;
        let mut reader = SnowflakeReader {
            session: fake::with_ctx(fake::connection(&url), |ctx| {
                Session::build(ctx, "warehouse")
            })?,
            sql: statements(poll(incremental(StartFrom::Oldest, None)))?,
            cursor_type: None,
        };
        fake.reply(
            200,
            &fake::result(&[("RECORDED_AT", "timestamp_ntz")], &json!([])),
        );
        reader.page(None, 10).await?;
        reader
            .page(Some("2026-01-01T00:00:00.000000000"), 10)
            .await?;
        let probes = fake
            .statements()
            .iter()
            .filter(|sql| sql.ends_with("LIMIT 0"))
            .count();
        assert_eq!(probes, 1);
        assert_eq!(fake.seen().len(), 3);
        Ok(())
    }
}
