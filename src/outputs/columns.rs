//! The database-neutral half of a column mapping: what a message becomes.
//!
//! [`kayak_core::columns`] declares the mapping; this turns one into a
//! [`ColumnPlan`] at build time and a plan plus a message into a row. Nothing
//! here knows any SQL — a row is a list of `Option<String>`, one per column,
//! and how those are bound and what DDL a [`ColumnType`] renders as is the
//! output's own business. That split is what lets a second database output
//! reuse the mapping whole.
//!
//! **Values are checked, never coerced.** A string `"12.5"` into a `float`
//! column fails the batch rather than arriving as a number, because a mapping
//! that accepts anything is a mapping that guarantees nothing. What *is*
//! lenient is the text the value is carried across as: every column binds as
//! text and is cast in the statement, which keeps this module free of any one
//! driver's type mapping and keeps a number's own digits — nothing here routes
//! a decimal through an f64 on the way to the server.
//!
//! Everything that would otherwise be a strange row once per message forever is
//! refused by [`ColumnPlan::build`] instead — a duplicated column, a `nullable:
//! false` column told to write null when a field is missing, an index on a
//! column nothing maps.

use anyhow::{Result, anyhow, bail};
use kayak_core::columns::{ColumnMapping, ColumnType, ExtraFieldPolicy, MissingColumnPolicy};
use serde_json::Value;
use std::collections::BTreeSet;

use crate::fields;

/// A SQL identifier that has been checked and can be interpolated into a
/// statement.
///
/// The same reason [`crate::outputs::postgres::Table`] exists: a column name
/// cannot be a bind parameter — the server only takes those where a *value*
/// goes — so it ends up in the SQL text, and anything reaching the SQL text
/// from config has to be checked first. Deliberately stricter than any server
/// is: a column named `"drop table"` is legal in postgres and not worth
/// supporting.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Identifier {
    name: String,
}

impl Identifier {
    /// Letters, digits and underscores, not starting with a digit.
    pub fn parse(name: &str, what: &str) -> Result<Self> {
        if name.is_empty() {
            bail!("invalid {what} '{name}': it is empty");
        }
        if !name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            bail!("invalid {what} '{name}': only letters, digits and underscores are allowed");
        }
        if name.starts_with(|c: char| c.is_ascii_digit()) {
            bail!("invalid {what} '{name}': it cannot start with a digit");
        }
        Ok(Self {
            name: name.to_string(),
        })
    }

    /// The identifier as it goes into a statement: quoted, so that a name
    /// colliding with a keyword still works and the case the config wrote is
    /// the case that appears.
    #[must_use]
    pub fn quoted(&self) -> String {
        format!("\"{}\"", self.name)
    }

    /// The bare name, as the config wrote it.
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.name
    }
}

/// Where a column's value comes from.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Source {
    /// A field on the message, by dotted path.
    Field(String),
    /// The whole message. The audit column, and the only source that is never
    /// missing.
    WholeMessage,
}

/// One column, ready to write into.
#[derive(Debug, Clone)]
pub struct PlannedColumn {
    pub name: Identifier,
    pub column_type: ColumnType,
    pub source: Source,
    pub nullable: bool,
    pub on_missing: MissingColumnPolicy,
}

/// What a message came to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Row {
    /// One value per planned column, in order; `None` is a SQL `NULL`.
    Values(Vec<Option<String>>),
    /// The message is not written at all — a column with `on_missing:
    /// skip_row` didn't find its field.
    Skipped,
}

/// A validated column mapping: the columns in the order they are written, plus
/// what to do about the fields none of them read.
#[derive(Debug, Clone)]
pub struct ColumnPlan {
    columns: Vec<PlannedColumn>,
    on_extra_fields: ExtraFieldPolicy,
    /// The top-level keys some column reads, for the extra-fields check. A
    /// dotted path claims its **first** segment: a column reading `sensor.id`
    /// makes `sensor` a field that is mapped, since the alternative is to call
    /// every message with a nested object an error.
    claimed: BTreeSet<String>,
    /// Whether a `message: true` column makes every field claimed.
    claims_everything: bool,
}

impl ColumnPlan {
    /// Checks a config's columns and returns the plan they describe.
    pub fn build(columns: &[ColumnMapping], on_extra_fields: ExtraFieldPolicy) -> Result<Self> {
        if columns.is_empty() {
            bail!("a column mapping needs at least one column");
        }
        let mut planned: Vec<PlannedColumn> = Vec::with_capacity(columns.len());
        let mut claimed = BTreeSet::new();
        let mut claims_everything = false;

        for column in columns {
            let name = Identifier::parse(&column.name, "column name")?;
            if planned.iter().any(|c| c.name == name) {
                bail!("column '{}' is mapped twice", column.name);
            }
            if column.message {
                if column.field.is_some() {
                    bail!(
                        "column '{}' asks for both the whole message and a field; it can have one",
                        column.name
                    );
                }
                if column.column_type != ColumnType::Json {
                    bail!(
                        "column '{}' stores the whole message, so it has to be a json column, not {}",
                        column.name,
                        column.column_type.as_str()
                    );
                }
                claims_everything = true;
            }
            let nullable = column.is_nullable();
            let on_missing = column.missing_policy();
            // a contradiction rather than a preference: null is precisely what
            // this column cannot hold, and finding that out as a constraint
            // violation an hour into a run is the outcome worth avoiding
            if !nullable && on_missing == MissingColumnPolicy::Null {
                bail!(
                    "column '{}' is not nullable, so it cannot write null for a missing field",
                    column.name
                );
            }
            let source = if column.message {
                Source::WholeMessage
            } else {
                let field = column.source_field();
                if field.is_empty() {
                    bail!("column '{}' reads a field with no name", column.name);
                }
                claimed.insert(fields::root_segment(field).to_string());
                Source::Field(field.to_string())
            };
            planned.push(PlannedColumn {
                name,
                column_type: column.column_type,
                source,
                nullable,
                on_missing,
            });
        }

        Ok(Self {
            columns: planned,
            on_extra_fields,
            claimed,
            claims_everything,
        })
    }

    /// The columns, in the order a row's values are in.
    #[must_use]
    pub fn columns(&self) -> &[PlannedColumn] {
        &self.columns
    }

    /// The column of that name, for the checks that name one — a primary key
    /// or an index.
    #[must_use]
    pub fn column(&self, name: &str) -> Option<&PlannedColumn> {
        self.columns.iter().find(|c| c.name.as_str() == name)
    }

    /// Makes a column not-null, for the parts of a table that imply it — a
    /// primary key, in every database that has one.
    ///
    /// A column left on the default `null` policy is moved to `error` with it:
    /// the server would refuse the row anyway, and the message this produces
    /// names the field and the column rather than a constraint. An explicit
    /// `on_missing` is left alone, since `skip_row` is a sensible thing to ask
    /// for on a key column and is not in contradiction with anything.
    pub fn require_not_null(&mut self, name: &str) -> Result<()> {
        let column = self
            .columns
            .iter_mut()
            .find(|c| c.name.as_str() == name)
            .ok_or_else(|| anyhow!("'{name}' is not one of the mapped columns"))?;
        column.nullable = false;
        if column.on_missing == MissingColumnPolicy::Null {
            column.on_missing = MissingColumnPolicy::Error;
        }
        Ok(())
    }

    /// One message as a row, or [`Row::Skipped`].
    pub fn row(&self, message: &Value) -> Result<Row> {
        self.check_extra_fields(message)?;
        let mut values = Vec::with_capacity(self.columns.len());
        for column in &self.columns {
            let found = match &column.source {
                Source::WholeMessage => Some(message),
                Source::Field(field) => fields::get(message, field).filter(|v| !v.is_null()),
            };
            match found {
                Some(value) => values.push(Some(encode(value, column)?)),
                // a field that is present but null counts as missing — the
                // same reading the reducer takes
                None => match column.on_missing {
                    MissingColumnPolicy::Null => values.push(None),
                    MissingColumnPolicy::SkipRow => return Ok(Row::Skipped),
                    MissingColumnPolicy::Error => {
                        return Err(anyhow!(
                            "message has no value at '{}' for column '{}'",
                            match &column.source {
                                Source::Field(field) => field.as_str(),
                                Source::WholeMessage => "the message",
                            },
                            column.name.as_str()
                        ));
                    }
                },
            }
        }
        Ok(Row::Values(values))
    }

    /// Fields nothing reads, when the config asked to hear about them.
    fn check_extra_fields(&self, message: &Value) -> Result<()> {
        if self.on_extra_fields == ExtraFieldPolicy::Ignore || self.claims_everything {
            return Ok(());
        }
        let Some(object) = message.as_object() else {
            // a non-object message has no fields to be extra; whether its
            // columns find anything is the mapping's own business
            return Ok(());
        };
        let extra: Vec<&str> = object
            .keys()
            .filter(|key| !self.claimed.contains(key.as_str()))
            .map(String::as_str)
            .collect();
        if extra.is_empty() {
            return Ok(());
        }
        bail!(
            "message carries fields no column reads: {}",
            extra.join(", ")
        )
    }
}

/// One value as the text it is bound as, checking that it is the shape the
/// column asked for.
///
/// Text rather than a driver's own type for two reasons: the number's own
/// digits survive (nothing here routes a decimal through an f64), and the
/// module stays free of any one driver's type mapping — the statement carries
/// the cast.
fn encode(value: &Value, column: &PlannedColumn) -> Result<String> {
    let mismatch = || {
        anyhow!(
            "column '{}' is {}, but the message has {} at '{}'",
            column.name.as_str(),
            column.column_type.as_str(),
            describe(value),
            match &column.source {
                Source::Field(field) => field.as_str(),
                Source::WholeMessage => "the message",
            }
        )
    };
    Ok(match column.column_type {
        // a date and a uuid are strings here too: the server parses them, and
        // its error about a malformed one is better than anything reimplemented
        // on this side
        ColumnType::Text | ColumnType::Date | ColumnType::Uuid => {
            value.as_str().ok_or_else(mismatch)?.to_string()
        }
        ColumnType::Integer => {
            let number = value.as_i64().ok_or_else(mismatch)?;
            i32::try_from(number).map_err(|_| {
                anyhow!(
                    "column '{}' is integer, but {number} does not fit in one",
                    column.name.as_str()
                )
            })?;
            number.to_string()
        }
        ColumnType::Bigint => value.as_i64().ok_or_else(mismatch)?.to_string(),
        // the JSON text of the number rather than a parsed f64, so a decimal
        // arrives with the digits it was written with
        ColumnType::Float | ColumnType::Decimal => {
            value.as_f64().ok_or_else(mismatch)?;
            value.to_string()
        }
        ColumnType::Boolean => value.as_bool().ok_or_else(mismatch)?.to_string(),
        ColumnType::Timestamp => match value {
            Value::String(text) => text.clone(),
            Value::Number(_) => epoch_seconds_as_text(value, column)?,
            _ => return Err(mismatch()),
        },
        ColumnType::Json => value.to_string(),
    })
}

/// A number in a `timestamp` column, read as seconds since the epoch.
///
/// Seconds and not milliseconds because a JSON number carries no unit and one
/// of the two has to be picked; seconds is what `to_timestamp` means everywhere
/// else, and a fractional part says the rest.
fn epoch_seconds_as_text(value: &Value, column: &PlannedColumn) -> Result<String> {
    let out_of_range = || {
        anyhow!(
            "column '{}' is a timestamp, but {value} is not a time it can hold",
            column.name.as_str()
        )
    };
    let seconds = value.as_f64().ok_or_else(out_of_range)?;
    let micros = (seconds * 1_000_000.0).round();
    // checked here so the cast below cannot truncate; the bound is far wider
    // than any date a database will accept anyway
    if !micros.is_finite() || micros.abs() > 9.0e15 {
        return Err(out_of_range());
    }
    #[allow(
        clippy::cast_possible_truncation,
        reason = "range-checked immediately above"
    )]
    let micros = micros as i64;
    Ok(chrono::DateTime::from_timestamp_micros(micros)
        .ok_or_else(out_of_range)?
        .to_rfc3339())
}

/// What a value is, for an error that has to say why it wasn't accepted.
fn describe(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "a boolean",
        Value::Number(_) => "a number",
        Value::String(_) => "a string",
        Value::Array(_) => "an array",
        Value::Object(_) => "an object",
    }
}

#[cfg(test)]
mod tests {
    use super::{ColumnPlan, Identifier, Row};
    use kayak_core::columns::{ColumnMapping, ColumnType, ExtraFieldPolicy, MissingColumnPolicy};
    use serde_json::json;

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

    fn plan(columns: &[ColumnMapping]) -> anyhow::Result<ColumnPlan> {
        ColumnPlan::build(columns, ExtraFieldPolicy::Ignore)
    }

    #[test]
    fn a_column_reads_its_own_name_and_a_field_reads_a_path() -> anyhow::Result<()> {
        let plan = plan(&[
            column("sensor_id", ColumnType::Text),
            ColumnMapping {
                field: Some("reading.temp_c".into()),
                ..column("temperature", ColumnType::Float)
            },
            ColumnMapping {
                field: Some("_meta.subject".into()),
                ..column("subject", ColumnType::Text)
            },
        ])?;
        let row = plan.row(&json!({
            "sensor_id": "a1",
            "reading": {"temp_c": 20.5},
            "_meta": {"subject": "sensors.a1"}
        }))?;
        assert_eq!(
            row,
            Row::Values(vec![
                Some("a1".into()),
                Some("20.5".into()),
                Some("sensors.a1".into())
            ])
        );
        Ok(())
    }

    /// The promise the whole mapping rests on: a value of the wrong shape is an
    /// error, not a coercion, because a silently converted column is wrong in a
    /// way nothing downstream can see.
    #[test]
    fn a_value_of_the_wrong_type_is_an_error() -> anyhow::Result<()> {
        let cases = [
            (ColumnType::Float, json!("12.5")),
            (ColumnType::Integer, json!(1.5)),
            (ColumnType::Integer, json!(i64::from(i32::MAX) + 1)),
            (ColumnType::Text, json!(3)),
            (ColumnType::Boolean, json!("true")),
            (ColumnType::Timestamp, json!(true)),
            (ColumnType::Uuid, json!(7)),
        ];
        for (column_type, value) in cases {
            let plan = plan(&[column("v", column_type)])?;
            assert!(
                plan.row(&json!({"v": value})).is_err(),
                "{value} should not have been accepted by a {} column",
                column_type.as_str()
            );
        }
        Ok(())
    }

    /// The number's own digits go across — that is what a `decimal` column is
    /// for, and routing it through an f64 on this side would defeat it. 2^53+1
    /// is the smallest whole number that says so.
    #[test]
    fn a_decimal_carries_more_than_a_float_could() -> anyhow::Result<()> {
        let plan = plan(&[column("amount", ColumnType::Decimal)])?;
        assert_eq!(
            plan.row(&json!({"amount": 9_007_199_254_740_993_u64}))?,
            Row::Values(vec![Some("9007199254740993".into())])
        );
        Ok(())
    }

    #[test]
    fn a_numeric_timestamp_is_read_as_seconds_since_the_epoch() -> anyhow::Result<()> {
        let plan = plan(&[column("at", ColumnType::Timestamp)])?;
        let Row::Values(values) = plan.row(&json!({"at": 1_754_827_200.5}))? else {
            panic!("the row should have been written");
        };
        let written = values[0].clone().unwrap_or_default();
        assert!(written.starts_with("2025-08-10T"), "{written}");
        assert!(written.contains(".5"), "{written}");
        Ok(())
    }

    /// A string goes across untouched: the server parses it, and its error is
    /// better than a re-implementation of ISO 8601 here.
    #[test]
    fn a_string_timestamp_is_left_for_the_server() -> anyhow::Result<()> {
        let plan = plan(&[column("at", ColumnType::Timestamp)])?;
        assert_eq!(
            plan.row(&json!({"at": "2026-08-10T12:00:00Z"}))?,
            Row::Values(vec![Some("2026-08-10T12:00:00Z".into())])
        );
        Ok(())
    }

    #[test]
    fn a_missing_field_writes_null_by_default() -> anyhow::Result<()> {
        let plan = plan(&[column("a", ColumnType::Text)])?;
        assert_eq!(plan.row(&json!({}))?, Row::Values(vec![None]));
        Ok(())
    }

    /// The same reading the reducer takes, and the reason it is worth pinning:
    /// an explicit null and an absent field are the same fact said two ways.
    #[test]
    fn an_explicit_null_counts_as_missing() -> anyhow::Result<()> {
        let plan = plan(&[ColumnMapping {
            nullable: Some(false),
            ..column("a", ColumnType::Text)
        }])?;
        assert!(plan.row(&json!({"a": null})).is_err());
        Ok(())
    }

    #[test]
    fn skip_row_leaves_the_whole_message_out() -> anyhow::Result<()> {
        let plan = plan(&[
            column("a", ColumnType::Text),
            ColumnMapping {
                on_missing: Some(MissingColumnPolicy::SkipRow),
                ..column("b", ColumnType::Text)
            },
        ])?;
        assert_eq!(plan.row(&json!({"a": "x"}))?, Row::Skipped);
        assert_eq!(
            plan.row(&json!({"a": "x", "b": "y"}))?,
            Row::Values(vec![Some("x".into()), Some("y".into())])
        );
        Ok(())
    }

    #[test]
    fn a_message_column_takes_the_whole_message() -> anyhow::Result<()> {
        let plan = plan(&[ColumnMapping {
            message: true,
            ..column("raw", ColumnType::Json)
        }])?;
        assert_eq!(
            plan.row(&json!({"a": 1}))?,
            Row::Values(vec![Some(r#"{"a":1}"#.into())])
        );
        Ok(())
    }

    /// Everything that would otherwise be a strange row once per message
    /// forever is refused when the output is built.
    #[test]
    fn contradictory_mappings_are_refused_at_build_time() {
        let cases: Vec<(&str, Vec<ColumnMapping>)> = vec![
            ("no columns", vec![]),
            (
                "a duplicated column",
                vec![column("a", ColumnType::Text), column("a", ColumnType::Text)],
            ),
            (
                "a name that could carry sql",
                vec![column("a\"; drop table users; --", ColumnType::Text)],
            ),
            (
                "not nullable and writing null",
                vec![ColumnMapping {
                    nullable: Some(false),
                    on_missing: Some(MissingColumnPolicy::Null),
                    ..column("a", ColumnType::Text)
                }],
            ),
            (
                "the whole message in a text column",
                vec![ColumnMapping {
                    message: true,
                    ..column("raw", ColumnType::Text)
                }],
            ),
            (
                "the whole message and a field",
                vec![ColumnMapping {
                    message: true,
                    field: Some("a".into()),
                    ..column("raw", ColumnType::Json)
                }],
            ),
        ];
        for (what, columns) in cases {
            assert!(
                ColumnPlan::build(&columns, ExtraFieldPolicy::Ignore).is_err(),
                "{what} should have been refused"
            );
        }
    }

    #[test]
    fn extra_fields_are_ignored_unless_the_config_asks() -> anyhow::Result<()> {
        let columns = vec![ColumnMapping {
            field: Some("sensor.id".into()),
            ..column("sensor_id", ColumnType::Text)
        }];
        let message = json!({"sensor": {"id": "a1"}, "surprise": 1});
        assert!(
            ColumnPlan::build(&columns, ExtraFieldPolicy::Ignore)?
                .row(&message)
                .is_ok()
        );
        let strict = ColumnPlan::build(&columns, ExtraFieldPolicy::Error)?;
        assert!(strict.row(&message).is_err());
        // the nested field's own siblings are not the top-level shape, so a
        // path claims the object it reads through
        assert!(strict.row(&json!({"sensor": {"id": "a1", "kind": "x"}})).is_ok());
        Ok(())
    }

    /// A `message: true` column reads everything, so nothing can be extra.
    #[test]
    fn a_message_column_makes_extra_fields_impossible() -> anyhow::Result<()> {
        let plan = ColumnPlan::build(
            &[ColumnMapping {
                message: true,
                ..column("raw", ColumnType::Json)
            }],
            ExtraFieldPolicy::Error,
        )?;
        assert!(plan.row(&json!({"anything": 1})).is_ok());
        Ok(())
    }

    #[test]
    fn an_identifier_is_quoted_and_checked() -> anyhow::Result<()> {
        assert_eq!(Identifier::parse("a_1", "column name")?.quoted(), "\"a_1\"");
        for bad in ["", "1a", "a b", "a\"", "a;b", "a-b"] {
            assert!(
                Identifier::parse(bad, "column name").is_err(),
                "'{bad}' should have been rejected"
            );
        }
        Ok(())
    }
}
