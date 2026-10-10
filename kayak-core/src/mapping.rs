//! Field mapping: the declaration half of the `map` transform.
//!
//! One `map` is an **ordered list of mappings** over one message, and both
//! words are load-bearing. A list rather than an object keyed by target name,
//! because order is semantics — a mapping reads whatever the mappings before it
//! wrote, which is how a two-step arithmetic is expressed, and a JSON object's
//! key order is not something a config file should have to rely on. Ordered
//! rather than a set, for the same reason.
//!
//! `map` **reshapes, it does not compute.** Renaming, promoting a value out of
//! a nested object, projecting, coalescing, casting, defaulting: all of that is
//! field plumbing and all of it is expressible as data. What is deliberately
//! not here is anything that needs a parser — no nested expressions, no
//! per-field conditionals, no arithmetic deeper than one operation per mapping.
//! Chaining two [`Mapping::Arithmetic`] entries through an intermediate field
//! is as far as this goes on purpose: the point where that becomes unpleasant
//! is the point where an embedded scripting language is the honest answer, and
//! growing an expression tree in YAML to avoid admitting it would be worse than
//! either.
//!
//! The cardinality is fixed: **one message in, one message out, always.** That
//! is what keeps `map` composable and out of the territory that `filter`,
//! `splitter` and `reduce` already own — a mapping that could drop a message
//! would be a filter written in the wrong place, so [`MapMissingPolicy`] has no
//! arm for it.
//!
//! This module is the declaration only; `crate`'s consumers reflect it into
//! `/docs` and the add-pipeline form. The evaluation lives in the root crate's
//! `transforms::map`, and the split is the same one [`crate::columns`] makes.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Changes the shape of every message. A mapping can rename, move, cast or
/// remove a field, or write a constant. The transform applies the mappings in
/// order.
///
/// Each entry in `mappings` reads fields from the message and writes one
/// field. A mapping can read the fields that earlier mappings wrote. Use this
/// for intermediate values. With `keep: all`, a `drop` can remove them again.
///
/// Reads and writes use dotted paths. For example, an `as` of `sensor.id`
/// writes the value inside a `sensor` object. If the object does not exist,
/// the transform makes it.
///
/// By default, the message passes through with the mappings applied to it.
/// `keep` can change this. One message goes in and one message comes out. To
/// drop a message, use `filter`. To make many messages, use `splitter`.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[schemars(title = "map")]
pub struct MapTransformConfig {
    /// The fields to write, in order. Give one mapping or more. Two mappings
    /// must not write the same field.
    pub mappings: Vec<Mapping>,
    /// Which fields of the input message the output keeps. The default is
    /// `all`.
    #[serde(default, skip_serializing_if = "KeepPolicy::is_default")]
    pub keep: KeepPolicy,
    /// What to do with a message that does not have a field that a mapping
    /// reads. The default is `error`. A `default` on the mapping applies
    /// first. Use a `default` when you expect one field to be absent.
    #[serde(default, skip_serializing_if = "MapMissingPolicy::is_default")]
    pub on_missing: MapMissingPolicy,
}

/// Which fields of the input message a `map` keeps.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum KeepPolicy {
    /// Keep all fields, and apply the mappings to them. This is the default.
    #[default]
    All,
    /// Keep only the fields that the mappings wrote. Use it to prepare a
    /// message for an output with a fixed shape, for example a `postgres`
    /// table. It also removes intermediate fields. You cannot use `drop` with
    /// `mapped`.
    Mapped,
}

impl KeepPolicy {
    /// Whether this is the value serde would supply anyway — so the field can
    /// be left out of the JSON a config round-trips to.
    #[must_use]
    pub fn is_default(&self) -> bool {
        matches!(self, Self::All)
    }
}

/// What `map` does with a message that does not have a field that a mapping
/// reads.
///
/// No value drops the message. To drop a message, use `filter`.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MapMissingPolicy {
    /// Fail the batch. This is the default. To accept a missing field, use
    /// `omit`, or give the mapping a `default`.
    #[default]
    Error,
    /// Do not write the target field.
    Omit,
    /// Write the target field as `null`.
    Null,
}

impl MapMissingPolicy {
    /// Whether this is the value serde would supply anyway.
    #[must_use]
    pub fn is_default(&self) -> bool {
        matches!(self, Self::Error)
    }
}

/// One field that the transform writes onto the message, and the source of
/// its value. The `type` field selects the mapping.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Mapping {
    /// Reads a value from one field and writes it to another field. Use it to
    /// rename a field, or to move a field out of a nested object, for example
    /// from `_meta.subject` to `subject`.
    Copy {
        /// The field to read, as a dotted path.
        from: String,
        /// The field to write. The default is the last segment of `from`.
        #[serde(rename = "as", default, skip_serializing_if = "Option::is_none")]
        output: Option<String>,
        /// The value to write when the message does not have `from`. With it,
        /// `on_missing` does not apply.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        default: Option<Literal>,
    },
    /// Writes a fixed value, for example the name of the site.
    Constant {
        /// The value to write.
        value: Literal,
        /// The field to write the value to.
        #[serde(rename = "as")]
        output: String,
    },
    /// Writes the value of the first field in a list that the message has.
    /// Use it when two sources use different names for the same field.
    Coalesce {
        /// The fields to try, in order. Give two fields or more.
        from: Vec<String>,
        /// The field to write the first value to.
        #[serde(rename = "as")]
        output: String,
        /// The value to write when the message has none of the fields.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        default: Option<Literal>,
    },
    /// Converts a value from one JSON type to another. For example, it
    /// converts the string `"12.5"` to the number `12.5`, or epoch seconds to
    /// a timestamp. It can also parse a string that contains JSON.
    ///
    /// This is the only place in kayak that converts a value. The column
    /// mapping of the database outputs checks a value and does not convert it.
    Cast {
        /// The field to read.
        from: String,
        /// The type to convert the value to.
        to: CastType,
        /// The field to write. The default is the last segment of `from`. For
        /// example, `{"from": "value", "to": "float"}` converts `value` in place.
        #[serde(rename = "as", default, skip_serializing_if = "Option::is_none")]
        output: Option<String>,
        /// The value to write when the message does not have `from`. A value
        /// that is present and does not convert is always an error.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        default: Option<Literal>,
    },
    /// Joins fields and literal text into one string. For example, use it to
    /// write a `site/machine` key for `group_by`.
    Concat {
        /// The parts, in order. Give one part or more.
        parts: Vec<ConcatPart>,
        /// The field to write the string to.
        #[serde(rename = "as")]
        output: String,
    },
    /// One arithmetic operation on two numbers. Each number is a field or a
    /// literal.
    ///
    /// For a calculation with more steps, use more mappings with intermediate
    /// fields. For example, `(f - 32) / 1.8` is two mappings. For a long
    /// calculation, use the `script` transform.
    Arithmetic {
        /// The left operand.
        left: Operand,
        /// The operation to do.
        operator: ArithmeticOperator,
        /// The right operand.
        right: Operand,
        /// The field to write the result to.
        #[serde(rename = "as")]
        output: String,
        /// For `divide`: what to do when the right field holds zero. The
        /// default is `error`, which fails the batch.
        #[serde(default, skip_serializing_if = "OnZero::is_default")]
        on_zero: OnZero,
    },
    /// Writes the start of the calendar period that a time is in, for example
    /// the hour, the day or the shift.
    ///
    /// Use the field in `group_by`. A stateful transform that groups by it
    /// keeps one series for each period. The idle timeout of the state bucket
    /// removes the old periods.
    ///
    /// The periods use the local clock of `timezone`. Thus, a shift that
    /// starts at 06:00 starts at 06:00 in summer and in winter. On the night
    /// that the clock changes, the shift is 7 or 9 hours.
    ///
    /// The periods start at local midnight on 1 January 1970, plus
    /// `offset_seconds`. For example, `every_seconds: 28800` with
    /// `offset_seconds: 21600` gives 06:00, 14:00 and 22:00. A week starts on a
    /// Thursday. For a week that starts on a Monday, add an offset of four days.
    TimeBucket {
        /// The field that holds the time, as an RFC 3339 string or as
        /// milliseconds since the epoch.
        from: String,
        /// The length of a period, in seconds.
        every_seconds: u64,
        /// The offset of the start of the periods, in seconds. It must be
        /// less than `every_seconds`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        offset_seconds: Option<u64>,
        /// The IANA time zone of the periods, for example `Europe/Stockholm`.
        /// The default is UTC.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        timezone: Option<String>,
        /// The format of the start time. The default is `rfc3339`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        format: Option<TimeFormat>,
        /// The field to write the start time to.
        #[serde(rename = "as")]
        output: String,
    },
    /// Removes fields from the message. Use it to remove metadata fields
    /// before the output. A field that is not there is not an error.
    /// `on_missing` does not apply.
    Drop {
        /// The fields to remove. Give one field or more.
        from: Vec<String>,
    },
}

impl Mapping {
    /// The field this mapping writes, if it writes one.
    ///
    /// `None` for a `drop`, which is the one mapping that takes away rather
    /// than putting something there — which is also why it is the one that
    /// makes no sense under [`KeepPolicy::Mapped`].
    #[must_use]
    pub fn target(&self) -> Option<&str> {
        match self {
            Self::Copy { from, output, .. } | Self::Cast { from, output, .. } => {
                Some(output.as_deref().unwrap_or_else(|| leaf(from)))
            }
            Self::Constant { output, .. }
            | Self::Coalesce { output, .. }
            | Self::Concat { output, .. }
            | Self::Arithmetic { output, .. }
            | Self::TimeBucket { output, .. } => Some(output),
            Self::Drop { .. } => None,
        }
    }

    /// The name this mapping goes by in an error, so a message about the third
    /// row says which kind of row it was.
    #[must_use]
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Copy { .. } => "copy",
            Self::Constant { .. } => "constant",
            Self::Coalesce { .. } => "coalesce",
            Self::Cast { .. } => "cast",
            Self::Concat { .. } => "concat",
            Self::Arithmetic { .. } => "arithmetic",
            Self::TimeBucket { .. } => "time_bucket",
            Self::Drop { .. } => "drop",
        }
    }
}

/// A path's last segment.
///
/// The root crate's `fields::leaf` is the real one and this is its twin, kept
/// here because [`Mapping::target`] has to answer the same question and core
/// cannot reach the root crate. Both are one line and neither is worth a
/// dependency in the direction that would fix it.
fn leaf(field: &str) -> &str {
    field.rsplit('.').next().unwrap_or(field)
}

/// A literal value. A `constant` writes it, and a `default` writes it in place
/// of a missing field. The `type` field selects the type of the value.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Literal {
    /// A string.
    Text {
        /// The text.
        value: String,
    },
    /// A number.
    Number {
        /// The number.
        value: f64,
    },
    /// True or false.
    Boolean {
        /// The boolean value.
        value: bool,
    },
    /// JSON `null`. The field is present with the value `null`.
    Null,
}

impl Literal {
    /// The JSON this literal writes.
    #[must_use]
    pub fn to_value(&self) -> Value {
        match self {
            Self::Text { value } => Value::String(value.clone()),
            Self::Number { value } => serde_json::Number::from_f64(*value)
                .map_or(Value::Null, Value::Number),
            Self::Boolean { value } => Value::Bool(*value),
            Self::Null => Value::Null,
        }
    }
}

/// How a [`Mapping::TimeBucket`] writes a time.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TimeFormat {
    /// An RFC 3339 string in UTC, with milliseconds. kayak writes all times in
    /// this format.
    Rfc3339,
    /// Milliseconds since the epoch, as a number.
    Millis,
}

/// One side of an [`Mapping::Arithmetic`]: a field to read, or a fixed number.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Operand {
    /// A number from the message.
    Field {
        /// The field to read. It must hold a number.
        field: String,
    },
    /// A number in the config.
    Value {
        /// The number.
        value: f64,
    },
}

/// What an [`Mapping::Arithmetic`] does with its two operands.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ArithmeticOperator {
    /// Left + right.
    Add,
    /// Left − right.
    Subtract,
    /// Left × right.
    Multiply,
    /// Left ÷ right. A literal zero on the right is an error when the pipeline
    /// builds. For a field that holds zero, `on_zero` applies.
    Divide,
    /// The smaller of left and right. With a literal on one side, this is an
    /// upper limit.
    Min,
    /// The larger of left and right. With a literal on one side, this is a
    /// lower limit.
    Max,
}

/// What a `divide` does when the right field holds zero. For example, this
/// occurs with a ratio over an empty period.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OnZero {
    /// Fail the batch. The error names the division. This is the default.
    #[default]
    Error,
    /// Write `null` as the result.
    Null,
    /// Write a number as the result.
    Value {
        /// The number to write.
        value: f64,
    },
}

impl OnZero {
    #[must_use]
    pub fn is_default(&self) -> bool {
        matches!(self, Self::Error)
    }
}

impl ArithmeticOperator {
    /// The symbol this goes by in an error message.
    #[must_use]
    pub fn symbol(self) -> &'static str {
        match self {
            Self::Add => "+",
            Self::Subtract => "-",
            Self::Multiply => "*",
            Self::Divide => "/",
            Self::Min => "min",
            Self::Max => "max",
        }
    }
}

/// One piece of a [`Mapping::Concat`].
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ConcatPart {
    /// A value from the message. A string is used as it is. A number or a
    /// boolean is written as JSON writes it. An object or an array is an error.
    Field {
        /// The field to read.
        field: String,
    },
    /// Literal text, for example a separator, a prefix or a suffix.
    Value {
        /// The text.
        value: String,
    },
}

/// The type that a `cast` converts a value to.
///
/// These types are not the column types of the database outputs. There is no
/// `bigint` and no `decimal`. Here, `json` parses a string that contains JSON.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CastType {
    /// A string. A number or a boolean is written as JSON writes it. An object
    /// or an array is an error.
    Text,
    /// A whole number. A string is parsed. A number with a fractional part is
    /// an error. The cast does not round.
    Integer,
    /// A number. A string is parsed.
    Float,
    /// True or false. The cast accepts the strings `true` and `false` in
    /// uppercase or lowercase, and the numbers 1 and 0. Other values are an
    /// error.
    Boolean,
    /// A timestamp, written as RFC 3339. A string is parsed and written again
    /// in one format. A number is read as **seconds** since the epoch, with
    /// fractions. The column mapping reads a number in the same way.
    Timestamp,
    /// A calendar date, written as `2026-08-10`. A string can be a date or a
    /// full timestamp. From a timestamp, the cast uses the date.
    Date,
    /// A UUID, in lowercase. The cast accepts only a string in the canonical
    /// form with hyphens.
    Uuid,
    /// Parses the JSON that a string contains. The input must be a string. Use
    /// it for a payload that is encoded inside another payload.
    Json,
}

impl CastType {
    /// The name this goes by in the config, so an error names what was written.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Text => "text",
            Self::Integer => "integer",
            Self::Float => "float",
            Self::Boolean => "boolean",
            Self::Timestamp => "timestamp",
            Self::Date => "date",
            Self::Uuid => "uuid",
            Self::Json => "json",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{CastType, KeepPolicy, Literal, MapMissingPolicy, Mapping};
    use serde_json::json;

    #[test]
    fn a_copy_without_an_as_targets_the_paths_leaf() {
        let mapping = Mapping::Copy {
            from: "_meta.subject".into(),
            output: None,
            default: None,
        };
        assert_eq!(mapping.target(), Some("subject"));
    }

    #[test]
    fn an_explicit_as_wins_over_the_leaf() {
        let mapping = Mapping::Cast {
            from: "_meta.subject".into(),
            to: CastType::Text,
            output: Some("topic".into()),
            default: None,
        };
        assert_eq!(mapping.target(), Some("topic"));
    }

    /// The one mapping that writes nothing, which is what makes it the one that
    /// cannot be used with `keep: mapped`.
    #[test]
    fn a_drop_has_no_target() {
        let mapping = Mapping::Drop {
            from: vec!["_meta".into()],
        };
        assert_eq!(mapping.target(), None);
    }

    #[test]
    fn a_literal_writes_the_json_it_names() {
        assert_eq!(
            Literal::Text {
                value: "line-3".into()
            }
            .to_value(),
            json!("line-3")
        );
        assert_eq!(Literal::Number { value: 1.5 }.to_value(), json!(1.5));
        assert_eq!(Literal::Boolean { value: true }.to_value(), json!(true));
        assert_eq!(Literal::Null.to_value(), json!(null));
    }

    /// The defaults are the ones the doc comments claim, and the `is_default`
    /// helpers agree with them — those are what keep a saved config from
    /// growing fields nobody wrote.
    #[test]
    fn the_defaults_are_pass_through_and_refuse() {
        assert_eq!(KeepPolicy::default(), KeepPolicy::All);
        assert_eq!(MapMissingPolicy::default(), MapMissingPolicy::Error);
        assert!(KeepPolicy::default().is_default());
        assert!(MapMissingPolicy::default().is_default());
    }
}
