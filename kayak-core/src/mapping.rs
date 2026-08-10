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

/// Rewrites the shape of every message: renames, promotions, constants, casts
/// and projections, applied in order.
///
/// Each entry in `mappings` reads fields from the message and writes one field
/// back, and **later entries see what earlier ones wrote** — so an intermediate
/// value is just a mapping whose target a later mapping reads (and, under
/// `keep: all`, a `drop` takes away again).
///
/// Reads are dotted paths, like everywhere else. Writes are too: an `as` of
/// `sensor.id` puts the value inside a `sensor` object, creating it if it isn't
/// there.
///
/// The message is passed through unchanged, with the mappings laid over it,
/// unless `keep` says otherwise. One message always comes out — this never
/// drops one, and never makes two. Reach for `filter` or `splitter` for those.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[schemars(title = "map")]
pub struct MapTransformConfig {
    /// what to write, in the order it is written. At least one, and no two may
    /// write the same field.
    pub mappings: Vec<Mapping>,
    /// whether fields nothing mapped survive
    #[serde(default, skip_serializing_if = "KeepPolicy::is_default")]
    pub keep: KeepPolicy,
    /// what to do about a message missing a field a mapping reads. A `default`
    /// on the mapping itself is answered first, and is the better way to say
    /// that one particular field is expected to be absent.
    #[serde(default, skip_serializing_if = "MapMissingPolicy::is_default")]
    pub on_missing: MapMissingPolicy,
}

/// Whether a `map` passes through the fields it wasn't told about.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum KeepPolicy {
    /// The message is passed through and the mappings are laid over it. The
    /// default, because it is the one that doesn't quietly discard data: a map
    /// that renamed one field would otherwise throw the rest of the message
    /// away.
    #[default]
    All,
    /// Only the fields the mappings wrote come out — a projection. This is what
    /// prepares a message for an output with a shape of its own (a `postgres`
    /// table, an `s3` part), and it is also what sweeps up the intermediate
    /// fields a chained arithmetic leaves behind.
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

/// What `map` does about a message that doesn't carry a field a mapping reads.
///
/// It has its own set rather than sharing the reducer's `MissingFieldPolicy` or
/// `recall`'s `RecallMissingPolicy` for one specific reason: `skip` already
/// means two different things in those two ("leave this message out of this
/// aggregation" and "drop the message"), and a third reading of the same word
/// would make the config file unreadable. So the arm that leaves the target
/// field unwritten is called `omit`, and there is deliberately no arm that
/// drops the message — that is what `filter` is for.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MapMissingPolicy {
    /// Fail the pipeline. The default, on the reducer's argument: a mapping
    /// that silently produced nothing is wrong in a way nothing downstream can
    /// see. Say `omit`, or give that one mapping a `default`, to mean it.
    #[default]
    Error,
    /// Leave the target field unwritten, as though the mapping weren't there.
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

/// One field written onto the message, and where its value comes from.
///
/// A tagged union rather than one struct with a great many optional fields, for
/// the reason `Condition` gives: a list of these has to render as a form, and a
/// pile of boxes of which four are relevant offers no way to say which four.
/// Here the tag is picked first and the rest of the row follows from it.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Mapping {
    /// Takes a value from one field and writes it to another — a rename, or a
    /// promotion of something out of a nested object (`_meta.subject` →
    /// `subject`).
    Copy {
        /// the field to read — a dotted path, like anywhere else
        from: String,
        /// the field to write. Left out, it is `from`'s last segment, which is
        /// the reading that makes promoting a nested value the short spelling.
        #[serde(rename = "as", default, skip_serializing_if = "Option::is_none")]
        output: Option<String>,
        /// what to write when `from` isn't there, instead of applying
        /// `on_missing`
        #[serde(default, skip_serializing_if = "Option::is_none")]
        default: Option<Literal>,
    },
    /// Writes a fixed value — the environment, the site, the name of the feed.
    Constant {
        /// the value to write
        value: Literal,
        /// the field to write it to
        #[serde(rename = "as")]
        output: String,
    },
    /// Writes the first of several fields that the message actually carries.
    ///
    /// This is what merging two sources that spell one thing differently comes
    /// to, and it needs no expression language to say.
    Coalesce {
        /// the fields to try, in order. At least two — with one, this is a
        /// `copy`.
        from: Vec<String>,
        /// the field to write the first value found to
        #[serde(rename = "as")]
        output: String,
        /// what to write when none of them is there
        #[serde(default, skip_serializing_if = "Option::is_none")]
        default: Option<Literal>,
    },
    /// Converts a value from one JSON shape to another — the string `"12.5"` to
    /// the number `12.5`, an epoch second to a timestamp, a string of embedded
    /// JSON to the thing it describes.
    ///
    /// This is the one place in kayak where coercion is legal, and that is the
    /// division of labour: a `postgres` column mapping *checks* a value and
    /// never converts it, so a stream that needs converting says so once, here,
    /// rather than at each of three outputs.
    Cast {
        /// the field to read
        from: String,
        /// what to convert it to
        to: CastType,
        /// the field to write. Left out, it is `from`'s last segment — so
        /// casting a field in place is `{"from": "value", "to": "float"}`.
        #[serde(rename = "as", default, skip_serializing_if = "Option::is_none")]
        output: Option<String>,
        /// what to write when `from` isn't there. A value that *is* there and
        /// won't convert is an error either way — that is a stream that isn't
        /// what the config says it is, not a missing field.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        default: Option<Literal>,
    },
    /// Joins fields and literal text into one string.
    ///
    /// Mostly earns its place because `group_by` takes a list of fields and has
    /// no composite key: building `site/machine` as a field is the only way to
    /// group on the pair.
    Concat {
        /// the pieces, in order. At least one.
        parts: Vec<ConcatPart>,
        /// the field to write the joined string to
        #[serde(rename = "as")]
        output: String,
    },
    /// One arithmetic operation on two numbers, each of them a field or a
    /// literal.
    ///
    /// One operation, deliberately: `(f - 32) / 1.8` is two of these through an
    /// intermediate field, and the fact that three or four steps read badly is
    /// information rather than a defect — it is where this stops being
    /// configuration.
    Arithmetic {
        /// the left-hand operand
        left: Operand,
        /// what to do with them
        operator: ArithmeticOperator,
        /// the right-hand operand
        right: Operand,
        /// the field to write the answer to
        #[serde(rename = "as")]
        output: String,
    },
    /// Takes fields off the message.
    ///
    /// The counterpart of the in-band envelope: metadata that a `group_by`
    /// needed is rarely metadata an output wants, and this is what takes it
    /// back off before the message leaves. Removing a field that isn't there is
    /// not an error — `on_missing` doesn't apply.
    Drop {
        /// the fields to remove. At least one.
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
            | Self::Arithmetic { output, .. } => Some(output),
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

/// A literal value written by a `constant`, or standing in for a field that
/// isn't there.
///
/// Spelled as a tagged union rather than as a bare JSON value because an
/// untyped `Value` field reflects as a box to hand-write JSON into, and one of
/// those in a form is a field the user has to already know the answer for.
/// Tagging it means the form asks which kind of value and then offers the right
/// control.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Literal {
    /// A string.
    Text {
        /// the text
        value: String,
    },
    /// A number.
    Number {
        /// the number
        value: f64,
    },
    /// True or false.
    Boolean {
        /// the flag
        value: bool,
    },
    /// JSON null — an explicit "nothing", as against leaving the field out.
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

/// One side of an [`Mapping::Arithmetic`]: a field to read, or a fixed number.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Operand {
    /// A number read out of the message.
    Field {
        /// the field to read — it has to hold a number
        field: String,
    },
    /// A number written here in the config.
    Value {
        /// the number
        value: f64,
    },
}

/// What an [`Mapping::Arithmetic`] does with its two operands.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ArithmeticOperator {
    /// left + right
    Add,
    /// left − right
    Subtract,
    /// left × right
    Multiply,
    /// left ÷ right. A literal zero on the right is refused when the pipeline
    /// is built; a *field* that turns out to be zero fails the batch.
    Divide,
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
        }
    }
}

/// One piece of a [`Mapping::Concat`].
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ConcatPart {
    /// A value read out of the message. A string is taken as it is; a number or
    /// a boolean is written the way JSON writes it. An object or an array is an
    /// error — there is no one right way to flatten one into a key.
    Field {
        /// the field to read
        field: String,
    },
    /// Literal text — the separator, a prefix, a suffix.
    Value {
        /// the text
        value: String,
    },
}

/// What a [`Mapping::Cast`] converts a value to.
///
/// A closed set of *logical* shapes, and a deliberately smaller one than the
/// column mapping's `ColumnType` even though the two overlap. `integer` and
/// `bigint` are one thing here, because JSON has one integer; `decimal` is
/// absent, because a `serde_json` number cannot hold one distinctly from a
/// float and a cast that claimed to would be a lie. `json` means something else
/// again — in a column it is "store whatever this is", here it is "this string
/// contains JSON, parse it", which is the common case of a payload that arrived
/// double-encoded.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum CastType {
    /// A string. A number or a boolean is written the way JSON writes it; an
    /// object or an array is an error.
    Text,
    /// A whole number. A string is parsed; a number with a fractional part is
    /// an error rather than a rounding, since which way to round is not
    /// something a config file said.
    Integer,
    /// A number. A string is parsed.
    Float,
    /// True or false. The strings `true`/`false` (in any case) and the numbers
    /// 1/0 are accepted; nothing else is.
    Boolean,
    /// A timestamp, written out as RFC 3339. A string is parsed and
    /// re-rendered, so a mixture of offsets arrives downstream in one spelling;
    /// a number is read as **seconds** since the epoch, fractions included —
    /// the same reading the column mapping makes.
    Timestamp,
    /// A calendar date, written out as `2026-08-10`. A string may be a plain
    /// date or a full timestamp, of which the date is taken.
    Date,
    /// A UUID, lower-cased. Only a string in the canonical hyphenated form is
    /// accepted — this validates, it does not invent.
    Uuid,
    /// The JSON a string contains, parsed. This is the one cast whose input
    /// must be a string: it is for a payload that arrived encoded inside
    /// another one.
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
