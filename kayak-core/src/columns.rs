//! How a message's fields become a database row.
//!
//! Shared rather than postgres-only, and that is the whole point of the module:
//! every database output answers the same two questions — which field lands in
//! which column, and what to do about a message that doesn't carry it — and
//! the answers should be spelled the same way whichever one you are writing to.
//! What differs between databases is only how a [`ColumnType`] is rendered as
//! DDL, which is each output's own business.
//!
//! So the types here are **logical**, not the SQL a particular server speaks:
//! `float` rather than `double precision`, `timestamp` rather than
//! `timestamptz`. A config that named the postgres spelling would be a config
//! that has to be rewritten to point at another database, and — since the set
//! is closed — the add-pipeline form gets a dropdown out of it for free.
//!
//! **Absent `columns` is today's behaviour**, a single column holding the whole
//! message as JSON. That is the same promise `batch_cap` and `envelope` make:
//! an output that quietly changed the shape of the table it writes into would
//! break every consumer of that table.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// The type a column holds, named the way the *config* thinks about it rather
/// than the way any one server spells it.
///
/// Values are checked against this before they are sent: a string `"12.5"` into
/// a `float` column is an error, not a coercion. Guessing is the failure mode
/// nobody sees, and a type that can be coerced from anything makes the mapping
/// worth nothing.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ColumnType {
    /// A string of any length. Only a JSON string is accepted.
    Text,
    /// A 32-bit whole number. A JSON number with a fractional part, or one
    /// outside the range, is an error rather than a rounding.
    Integer,
    /// A 64-bit whole number.
    Bigint,
    /// A double-precision floating point number.
    Float,
    /// An exact decimal. The digits are carried across as they were written, so
    /// nothing is lost to a binary float on the way.
    Decimal,
    /// True or false. Only a JSON boolean is accepted.
    Boolean,
    /// A date and time with a time zone. A JSON string is parsed by the server
    /// (ISO 8601 / RFC 3339); a JSON number is read as **seconds** since the
    /// epoch, fractions included.
    Timestamp,
    /// A calendar date, as a JSON string (`2026-08-10`).
    Date,
    /// A UUID, as a JSON string.
    Uuid,
    /// Any JSON value at all, stored as JSON.
    Json,
}

impl ColumnType {
    /// The name this type goes by in the config — used in error messages, so
    /// that what a failure names is what the user wrote.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Integer => "integer",
            Self::Bigint => "bigint",
            Self::Float => "float",
            Self::Decimal => "decimal",
            Self::Boolean => "boolean",
            Self::Timestamp => "timestamp",
            Self::Date => "date",
            Self::Uuid => "uuid",
            Self::Json => "json",
        }
    }
}

/// What to do about a message that doesn't carry a column's field.
///
/// A field that is present but `null` counts as missing — the same reading the
/// reducer's [`crate::config::MissingFieldPolicy`] takes, and the same fact
/// said two ways.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MissingColumnPolicy {
    /// Write `NULL`. The default for a nullable column: a stream where some
    /// messages carry a field and some don't is the ordinary case, and a table
    /// that says so is a table you can still query.
    Null,
    /// Fail the pipeline. The default for a column declared `"nullable": false`,
    /// since there is nothing else such a column could do.
    Error,
    /// Leave the whole message out of the table. Nothing about that row is
    /// written, including the columns that *were* present.
    SkipRow,
}

/// One message field mapped onto one column.
///
/// `field` defaults to `name`, so a message that already uses the column names
/// needs nothing but the name and the type. It is a dotted path like every
/// other field reference in kayak, so `_meta.subject` reaches whatever the
/// input's envelope attached and a literal key containing dots still wins.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[schemars(title = "column")]
pub struct ColumnMapping {
    /// the column's name in the table. Letters, digits and underscores only,
    /// since it cannot be sent as a query parameter.
    pub name: String,
    /// what the column holds. Values are checked against it rather than
    /// coerced into it.
    #[serde(rename = "type")]
    pub column_type: ColumnType,
    /// the field to read, as a dotted path. Defaults to the column's name.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    /// store the whole message in this column instead of one of its fields.
    /// Only for a `json` column, and not together with `field`.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub message: bool,
    /// whether the column accepts `NULL`. Defaults to true; `false` makes the
    /// created column `NOT NULL` and makes a missing field an error.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nullable: Option<bool>,
    /// what to do about a message that doesn't carry the field. Defaults to
    /// `null`, or to `error` for a column that is not nullable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub on_missing: Option<MissingColumnPolicy>,
}

impl ColumnMapping {
    /// The field this column reads, which is its name unless it says otherwise.
    #[must_use]
    pub fn source_field(&self) -> &str {
        self.field.as_deref().unwrap_or(&self.name)
    }

    /// Whether the column accepts `NULL`. Nullable unless it says otherwise.
    #[must_use]
    pub fn is_nullable(&self) -> bool {
        self.nullable.unwrap_or(true)
    }

    /// What a missing field does here: what the config says, or `error` for a
    /// column that cannot hold a null and `null` for one that can.
    #[must_use]
    pub fn missing_policy(&self) -> MissingColumnPolicy {
        self.on_missing.unwrap_or(if self.is_nullable() {
            MissingColumnPolicy::Null
        } else {
            MissingColumnPolicy::Error
        })
    }
}

/// An index to create alongside the table.
///
/// Only created when the table is — like the table itself it is
/// `IF NOT EXISTS`, and an index on a table someone else owns is theirs to
/// manage.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[schemars(title = "index")]
pub struct TableIndex {
    /// the columns to index, in order. Each must be one of the mapped columns.
    pub columns: Vec<String>,
    /// whether the index is unique. Defaults to false.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unique: Option<bool>,
}

impl TableIndex {
    /// Whether this index is unique. Not unless it says so.
    #[must_use]
    pub fn is_unique(&self) -> bool {
        self.unique.unwrap_or(false)
    }
}

/// What to do about a message carrying fields no column reads.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExtraFieldPolicy {
    /// Write the columns that are mapped and let the rest go. The default —
    /// mapping a subset of a wide message is the ordinary reason to map at all.
    #[default]
    Ignore,
    /// Fail the pipeline. For a stream whose shape is supposed to be fixed,
    /// where a new field appearing is news rather than noise.
    Error,
}

impl ExtraFieldPolicy {
    /// Whether this is the value serde would supply anyway — so the field can
    /// be left out of the JSON a config round-trips to.
    #[must_use]
    pub fn is_default(&self) -> bool {
        *self == Self::default()
    }
}

#[cfg(test)]
mod tests {
    use super::{ColumnMapping, ColumnType, MissingColumnPolicy};

    fn column(name: &str) -> ColumnMapping {
        ColumnMapping {
            name: name.to_string(),
            column_type: ColumnType::Text,
            field: None,
            message: false,
            nullable: None,
            on_missing: None,
        }
    }

    /// The ergonomic half of the mapping: a message that already uses the
    /// column names needs no `field` at all.
    #[test]
    fn a_column_reads_the_field_it_is_named_after() {
        assert_eq!(column("sensor_id").source_field(), "sensor_id");
        assert_eq!(
            ColumnMapping {
                field: Some("sensor.id".into()),
                ..column("sensor_id")
            }
            .source_field(),
            "sensor.id"
        );
    }

    /// A not-null column has no null to fall back on, so the default flips
    /// rather than producing a constraint violation an hour later.
    #[test]
    fn the_default_missing_policy_follows_nullability() {
        assert_eq!(column("a").missing_policy(), MissingColumnPolicy::Null);
        assert_eq!(
            ColumnMapping {
                nullable: Some(false),
                ..column("a")
            }
            .missing_policy(),
            MissingColumnPolicy::Error
        );
        assert_eq!(
            ColumnMapping {
                nullable: Some(false),
                on_missing: Some(MissingColumnPolicy::SkipRow),
                ..column("a")
            }
            .missing_policy(),
            MissingColumnPolicy::SkipRow
        );
    }
}
