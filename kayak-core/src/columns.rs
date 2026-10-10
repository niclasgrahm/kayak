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

/// The type of a column. The names are the same for all database outputs.
/// Each output changes them into the types of its server.
///
/// The output checks each value against the type before it sends the value.
/// It does not convert values. For example, the string `"12.5"` in a `float`
/// column is an error. To convert a value, use a `cast` in a `map` transform.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ColumnType {
    /// A string of any length. Only a JSON string is accepted.
    Text,
    /// A 32-bit whole number. A JSON number with a fractional part is an
    /// error. A number outside the range is an error.
    Integer,
    /// A 64-bit whole number.
    Bigint,
    /// A double-precision floating point number.
    Float,
    /// An exact decimal. The output sends the digits as they are in the
    /// message, so no precision is lost.
    Decimal,
    /// True or false. Only a JSON boolean is accepted.
    Boolean,
    /// A date and time with a time zone. The server parses a JSON string as
    /// ISO 8601 or RFC 3339. The output reads a JSON number as **seconds**
    /// since the epoch, with fractions.
    Timestamp,
    /// A calendar date, as a JSON string (`2026-08-10`).
    Date,
    /// A UUID, as a JSON string.
    Uuid,
    /// Any JSON value, stored as JSON.
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

/// What to do with a message that does not have the field of a column. A
/// field with the value `null` counts as missing.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MissingColumnPolicy {
    /// Write `NULL`. This is the default for a nullable column.
    Null,
    /// Fail the batch. This is the default for a column with
    /// `"nullable": false`.
    Error,
    /// Do not write the message. The output writes no column of that row.
    SkipRow,
}

/// One message field that the output writes to one column.
///
/// The default of `field` is `name`. When the message uses the column names,
/// give only `name` and `type`. `field` is a dotted path, for example
/// `_meta.subject`. A key that contains a dot and matches exactly has
/// priority over the path.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[schemars(title = "column")]
pub struct ColumnMapping {
    /// The name of the column in the table. Use only letters, digits and
    /// underscores.
    pub name: String,
    /// The type of the column. The output checks each value against it.
    #[serde(rename = "type")]
    pub column_type: ColumnType,
    /// The field to read, as a dotted path. The default is the name of the
    /// column.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(extend("x-message-field" = true))]
    pub field: Option<String>,
    /// Write the full message to this column. Use it only with a `json`
    /// column. Do not use it with `field`.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub message: bool,
    /// Whether the column accepts `NULL`. The default is true. With `false`,
    /// the output makes the column `NOT NULL`, and a missing field is an
    /// error.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nullable: Option<bool>,
    /// What to do with a message that does not have the field. The default is
    /// `null`, or `error` for a column that is not nullable.
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

/// An index to make with the table. The output makes the index only when it
/// makes the table, with `IF NOT EXISTS`.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[schemars(title = "index")]
pub struct TableIndex {
    /// The columns to index, in order. Each column must be a mapped column.
    pub columns: Vec<String>,
    /// Whether the index is unique. The default is false.
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

/// What to do with a message that has fields that no column reads.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ExtraFieldPolicy {
    /// Write the mapped columns and ignore the other fields. This is the
    /// default.
    #[default]
    Ignore,
    /// Fail the batch. Use it for a stream with a fixed shape, where a new
    /// field is a problem.
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
