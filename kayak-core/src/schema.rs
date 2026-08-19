//! What a handful of real messages says the data looks like.
//!
//! Everything in kayak addresses fields by name — a column's `field`, a
//! reducer's `group_by`, a filter's comparison — and until now the only way to
//! learn those names was to know them already. This module reads them back off
//! a sample of actual messages, so the "add pipeline" form can offer the
//! fields a stream really carries instead of an empty text box.
//!
//! It is deliberately **pure and dependency-light**, and it lives here rather
//! than in the server crate for the reason [`crate::columns`] and
//! [`crate::mapping`] do: the form runs in the browser, so the half that reads
//! a shape has to compile for `wasm32`. Nothing here talks to a broker; where
//! the messages came from is the caller's business.
//!
//! # The paths are `fields::get` paths, or they are not reported
//!
//! The one rule that runs through all of it: a path this module reports must
//! be a path the runtime can actually read, or the picker would offer names
//! that silently resolve to nothing. `kayak::fields::get` walks dot-separated
//! object keys and nothing else, so:
//!
//! - **an array is a leaf.** There is no array indexing in a field path, so
//!   `items.0.sku` is unaddressable and is not offered. The array itself is,
//!   since a `json` column can hold it whole.
//! - **a key that contains a dot is a leaf too.** `get` prefers an exact key,
//!   which is what makes `{"a.b": 1}` readable — but only as the *whole* path.
//!   Nothing can address the `c` inside `{"a.b": {"c": 1}}`, so that `c` is not
//!   offered and the dotted key is reported as the object it is.
//! - **a message that is not an object contributes no fields at all.** A bare
//!   number has nothing to name; [`MessageSchema::non_objects`] counts those so
//!   the UI can say so rather than showing an empty list that looks like a bug.
//!
//! # What a suggestion is worth
//!
//! [`InferredField::suggested_column`] is an opening bid for the column
//! mapper's type dropdown, and it is *advisory* — the user's choice wins,
//! because a sample is a few messages and not a schema. Two consequences of
//! that honesty are worth knowing. A whole number suggests `bigint` rather
//! than `integer`: a sample cannot bound a range, and the narrower guess is
//! the one that fails in production. And a field whose sampled values
//! disagree about their type suggests **nothing** — picking one of them would
//! be a guess presented as knowledge, and the whole point of showing real
//! messages is to stop guessing.
//!
//! Text formats ([`TextFormat`]) are recognised by **shape**, not by parsing —
//! this crate has no date library and should not grow one for a hint. A string
//! shaped like an RFC 3339 timestamp suggests `timestamp`; whether it really
//! is one is settled where it has always been settled, by the server, when the
//! row is written.

use std::collections::HashMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::columns::ColumnType;

/// How deep into a message the walk goes.
///
/// A bound rather than a limit anyone should reach: nine levels of nesting is
/// already past what a field path is pleasant to write. It exists because the
/// messages come off a broker and the walk is recursive, so the depth is the
/// sender's choice unless it is ours.
pub const MAX_DEPTH: usize = 8;

/// How many distinct paths a sample may report.
///
/// The same bound, for the same reason, in the other direction:
/// a message keyed by id (`{"sensor-1": ..., "sensor-2": ...}`) has as many
/// fields as the stream has ids, and a form offering ten thousand of them is
/// no more useful than one offering none. Past this the walk stops adding new
/// paths and says so through [`MessageSchema::truncated`].
pub const MAX_FIELDS: usize = 500;

/// How much of a sampled string is kept as an example value.
///
/// An example is a display aid — enough to recognise a field by, not enough to
/// carry a payload. Counted in characters, so a cut never lands inside one.
pub const EXAMPLE_MAX_CHARS: usize = 80;

/// What JSON said a value was.
///
/// Deliberately the JSON types and not the column types: this half reports what
/// was *seen*, and mapping that onto what a database should hold is
/// [`InferredField::suggested_column`]'s job, one step later and reversible by
/// the user.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum InferredType {
    /// An explicit `null`. Recorded rather than ignored, because a field that
    /// is sometimes null is exactly what decides whether a column is nullable.
    Null,
    /// `true` or `false`.
    Boolean,
    /// A number with no fractional part.
    Integer,
    /// A number with a fractional part.
    Float,
    /// A string.
    String,
    /// An array. A leaf: nothing inside it is addressable.
    Array,
    /// A nested object. Reported in its own right *and* descended into.
    Object,
}

impl InferredType {
    /// What this type is called in a UI list.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Null => "null",
            Self::Boolean => "boolean",
            Self::Integer => "integer",
            Self::Float => "float",
            Self::String => "string",
            Self::Array => "array",
            Self::Object => "object",
        }
    }

    /// What JSON says this value is.
    #[must_use]
    fn of(value: &Value) -> Self {
        match value {
            Value::Null => Self::Null,
            Value::Bool(_) => Self::Boolean,
            Value::Number(number) => {
                if number.is_f64() {
                    Self::Float
                } else {
                    Self::Integer
                }
            }
            Value::String(_) => Self::String,
            Value::Array(_) => Self::Array,
            Value::Object(_) => Self::Object,
        }
    }
}

/// A shape a sampled string was recognised as.
///
/// Recognised by shape and never by parsing — see the module docs. Only
/// reported when *every* string the field carried had the same shape, since a
/// column type that fits four of five messages is worse than no suggestion.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TextFormat {
    /// `2026-08-18T09:12:00Z` — a date, a separator and at least a time.
    Timestamp,
    /// `2026-08-18` — a date and nothing else.
    Date,
    /// `3f2a…` in the canonical 8-4-4-4-12 hyphenated form.
    Uuid,
}

impl TextFormat {
    /// The shape of one string, if it has one this recognises.
    #[must_use]
    pub fn of(text: &str) -> Option<Self> {
        if looks_like_timestamp(text) {
            Some(Self::Timestamp)
        } else if looks_like_date(text) {
            Some(Self::Date)
        } else if looks_like_uuid(text) {
            Some(Self::Uuid)
        } else {
            None
        }
    }
}

/// `YYYY-MM-DD`, and exactly that.
fn looks_like_date(text: &str) -> bool {
    let bytes = text.as_bytes();
    bytes.len() == 10 && date_prefix(bytes)
}

/// A date, a `T` or a space, and at least `HH:MM`.
fn looks_like_timestamp(text: &str) -> bool {
    let bytes = text.as_bytes();
    if bytes.len() < 16 || !date_prefix(bytes) {
        return false;
    }
    if bytes[10] != b'T' && bytes[10] != b' ' {
        return false;
    }
    bytes[11].is_ascii_digit()
        && bytes[12].is_ascii_digit()
        && bytes[13] == b':'
        && bytes[14].is_ascii_digit()
        && bytes[15].is_ascii_digit()
}

/// The first ten bytes read as `NNNN-NN-NN`.
fn date_prefix(bytes: &[u8]) -> bool {
    bytes.len() >= 10
        && bytes[..4].iter().all(u8::is_ascii_digit)
        && bytes[4] == b'-'
        && bytes[5..7].iter().all(u8::is_ascii_digit)
        && bytes[7] == b'-'
        && bytes[8..10].iter().all(u8::is_ascii_digit)
}

/// The canonical hyphenated form, lower or upper case.
fn looks_like_uuid(text: &str) -> bool {
    let bytes = text.as_bytes();
    if bytes.len() != 36 {
        return false;
    }
    bytes.iter().enumerate().all(|(at, byte)| match at {
        8 | 13 | 18 | 23 => *byte == b'-',
        _ => byte.is_ascii_hexdigit(),
    })
}

/// One field the sample carried, and everything the sample knows about it.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct InferredField {
    /// The dotted path that reads it, in the spelling
    /// `kayak::fields::get` understands.
    pub path: String,
    /// The JSON types seen at this path, in first-seen order and deduplicated.
    /// More than one means the sample disagreed with itself.
    pub types: Vec<InferredType>,
    /// How many sampled messages carried this path with a value that was not
    /// `null`.
    pub present: usize,
    /// How many carried it as an explicit `null`. Separate from absence
    /// because a column mapper cares that a field *exists* and is empty, even
    /// though [`crate::columns::MissingColumnPolicy`] then treats the two the
    /// same way.
    pub nulls: usize,
    /// The shape every sampled string had, if they all had one and there was
    /// at least one.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format: Option<TextFormat>,
    /// The first non-null scalar seen here, to show beside the name. Absent for
    /// objects and arrays, whose "example" would be most of the message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub example: Option<Value>,
}

impl InferredField {
    /// Whether some sampled message did not carry a value here — either
    /// because the field was absent or because it was `null`.
    ///
    /// The argument for the column mapper's `nullable`, and the reason the two
    /// counts are kept apart above: this is the question both of them answer.
    #[must_use]
    pub fn nullable(&self, messages: usize) -> bool {
        self.nulls > 0 || self.present < messages
    }

    /// The one JSON type seen here, if the sample agreed.
    #[must_use]
    pub fn settled_type(&self) -> Option<InferredType> {
        match self.types.as_slice() {
            [only] => Some(*only),
            // A field that is sometimes null and otherwise one thing has
            // agreed about what it holds; nullability is the other question.
            [InferredType::Null, other] | [other, InferredType::Null] => Some(*other),
            _ => None,
        }
    }

    /// An opening bid for this field's column type, or `None` when the sample
    /// disagreed with itself and any bid would be a guess dressed up as
    /// knowledge.
    #[must_use]
    pub fn suggested_column(&self) -> Option<ColumnType> {
        match self.settled_type()? {
            InferredType::Boolean => Some(ColumnType::Boolean),
            // A sample cannot bound a range, so the wide one. See the module
            // docs.
            InferredType::Integer => Some(ColumnType::Bigint),
            InferredType::Float => Some(ColumnType::Float),
            InferredType::String => Some(match self.format {
                Some(TextFormat::Timestamp) => ColumnType::Timestamp,
                Some(TextFormat::Date) => ColumnType::Date,
                Some(TextFormat::Uuid) => ColumnType::Uuid,
                None => ColumnType::Text,
            }),
            InferredType::Array | InferredType::Object => Some(ColumnType::Json),
            // Every message had it and every one of them had it empty. There
            // is nothing here to type.
            InferredType::Null => None,
        }
    }
}

/// What a sample of messages looks like.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct MessageSchema {
    /// How many messages were looked at.
    pub messages: usize,
    /// How many of them were not JSON objects, and so named no fields. A
    /// stream of bare numbers is a real stream; this is how the UI can say
    /// that rather than showing an empty list.
    pub non_objects: usize,
    /// The fields, in first-seen order — the same order the reducer emits
    /// groups in, and for the same reason: arrival order is the only order a
    /// sample has a claim to.
    pub fields: Vec<InferredField>,
    /// Whether [`MAX_FIELDS`] or [`MAX_DEPTH`] stopped the walk, so the list
    /// can be shown as the partial answer it is.
    pub truncated: bool,
}

impl MessageSchema {
    /// The field at a path, if the sample carried one.
    #[must_use]
    pub fn field(&self, path: &str) -> Option<&InferredField> {
        self.fields.iter().find(|field| field.path == path)
    }

    /// Just the paths, which is what a picker needs.
    #[must_use]
    pub fn paths(&self) -> Vec<&str> {
        self.fields.iter().map(|field| field.path.as_str()).collect()
    }
}

/// Reads a sample of messages back as the fields they carry.
///
/// See the module docs for which paths are reported and which deliberately are
/// not.
#[must_use]
pub fn infer(messages: &[Value]) -> MessageSchema {
    let mut walk = Walk::default();
    for message in messages {
        match message {
            Value::Object(_) => walk.descend(message, ""),
            _ => walk.non_objects += 1,
        }
    }
    MessageSchema {
        messages: messages.len(),
        non_objects: walk.non_objects,
        truncated: walk.truncated,
        fields: walk.fields,
    }
}

/// The accumulating half. Separate from [`MessageSchema`] because settling a
/// [`TextFormat`] needs a "these disagreed" state that means nothing once the
/// walk is over.
#[derive(Default)]
struct Walk {
    fields: Vec<InferredField>,
    /// path -> index into `fields`, so first-seen order survives without
    /// scanning the list per value.
    at: HashMap<String, usize>,
    /// Paths whose strings have not all had the same shape.
    formats_disagreed: Vec<bool>,
    non_objects: usize,
    truncated: bool,
}

impl Walk {
    /// Records every field of one object, and recurses where it may.
    fn descend(&mut self, object: &Value, prefix: &str) {
        let depth = if prefix.is_empty() {
            0
        } else {
            prefix.matches('.').count() + 1
        };
        let Some(map) = object.as_object() else {
            return;
        };
        for (key, value) in map {
            let path = if prefix.is_empty() {
                key.clone()
            } else {
                format!("{prefix}.{key}")
            };
            self.record(&path, value);
            // A dotted key is a leaf: `get` can reach the key itself but
            // nothing under it, so offering its children would offer names
            // that read as nothing. Same for the depth bound.
            if value.is_object() && !key.contains('.') {
                if depth + 1 < MAX_DEPTH {
                    self.descend(value, &path);
                } else {
                    self.truncated = true;
                }
            }
        }
    }

    /// Folds one observed value into the field at `path`.
    fn record(&mut self, path: &str, value: &Value) {
        let index = match self.at.get(path) {
            Some(index) => *index,
            None => {
                if self.fields.len() >= MAX_FIELDS {
                    self.truncated = true;
                    return;
                }
                self.fields.push(InferredField {
                    path: path.to_string(),
                    types: Vec::new(),
                    present: 0,
                    nulls: 0,
                    format: None,
                    example: None,
                });
                self.formats_disagreed.push(false);
                self.at.insert(path.to_string(), self.fields.len() - 1);
                self.fields.len() - 1
            }
        };
        let disagreed = self.formats_disagreed[index];
        let field = &mut self.fields[index];

        let seen = InferredType::of(value);
        // Asked before the type is recorded: a field whose *first* string this
        // is has nothing to disagree with, and a field that has held numbers
        // has nothing to say about text shapes either way.
        let first_string = !field.types.contains(&InferredType::String);
        if !field.types.contains(&seen) {
            field.types.push(seen);
        }
        if value.is_null() {
            field.nulls += 1;
        } else {
            field.present += 1;
        }

        if let Value::String(text) = value {
            let shape = TextFormat::of(text);
            if disagreed {
                field.format = None;
            } else if first_string {
                field.format = shape;
            } else if field.format != shape {
                self.formats_disagreed[index] = true;
                field.format = None;
            }
        }

        if field.example.is_none() && !value.is_null() && !value.is_object() && !value.is_array() {
            field.example = Some(example_of(value));
        }
    }
}

/// A value small enough to sit beside a field name.
fn example_of(value: &Value) -> Value {
    match value {
        Value::String(text) if text.chars().count() > EXAMPLE_MAX_CHARS => {
            let cut: String = text.chars().take(EXAMPLE_MAX_CHARS).collect();
            Value::String(format!("{cut}…"))
        }
        other => other.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn paths(schema: &MessageSchema) -> Vec<String> {
        schema.fields.iter().map(|f| f.path.clone()).collect()
    }

    #[test]
    fn a_flat_message_names_its_fields_with_their_types() {
        let schema = infer(&[json!({"id": 7, "temp": 21.5, "ok": true, "name": "a"})]);
        assert_eq!(schema.messages, 1);
        assert_eq!(paths(&schema), ["id", "name", "ok", "temp"]);
        assert_eq!(
            schema.field("id").unwrap().types,
            vec![InferredType::Integer]
        );
        assert_eq!(
            schema.field("temp").unwrap().types,
            vec![InferredType::Float]
        );
        assert_eq!(
            schema.field("ok").unwrap().types,
            vec![InferredType::Boolean]
        );
        assert_eq!(
            schema.field("name").unwrap().types,
            vec![InferredType::String]
        );
    }

    #[test]
    fn a_nested_object_is_reported_and_descended_into() {
        let schema = infer(&[json!({"sensor": {"id": "a", "cal": {"offset": 1}}})]);
        assert_eq!(paths(&schema), ["sensor", "sensor.cal", "sensor.cal.offset", "sensor.id"]);
        assert_eq!(
            schema.field("sensor").unwrap().types,
            vec![InferredType::Object]
        );
    }

    #[test]
    fn an_array_is_a_leaf_because_no_field_path_can_index_one() {
        let schema = infer(&[json!({"items": [{"sku": "x"}]})]);
        assert_eq!(paths(&schema), ["items"]);
        assert_eq!(
            schema.field("items").unwrap().types,
            vec![InferredType::Array]
        );
    }

    #[test]
    fn a_dotted_key_is_a_leaf_because_nothing_can_address_inside_it() {
        let schema = infer(&[json!({"a.b": {"c": 1}, "a": {"d": 2}})]);
        assert_eq!(paths(&schema), ["a", "a.d", "a.b"]);
        assert!(schema.field("a.b.c").is_none());
    }

    #[test]
    fn a_message_that_is_not_an_object_names_no_fields_and_is_counted() {
        let schema = infer(&[json!(3), json!({"a": 1}), json!("x")]);
        assert_eq!(schema.messages, 3);
        assert_eq!(schema.non_objects, 2);
        assert_eq!(paths(&schema), ["a"]);
    }

    #[test]
    fn a_field_missing_from_some_messages_is_nullable() {
        let schema = infer(&[json!({"a": 1, "b": 2}), json!({"a": 1})]);
        let a = schema.field("a").unwrap();
        let b = schema.field("b").unwrap();
        assert!(!a.nullable(schema.messages));
        assert!(b.nullable(schema.messages));
        assert_eq!(b.present, 1);
    }

    #[test]
    fn an_explicit_null_is_counted_apart_from_absence() {
        let schema = infer(&[json!({"a": null}), json!({"a": 1})]);
        let a = schema.field("a").unwrap();
        assert_eq!((a.present, a.nulls), (1, 1));
        assert!(a.nullable(schema.messages));
        assert_eq!(a.settled_type(), Some(InferredType::Integer));
        assert_eq!(a.suggested_column(), Some(ColumnType::Bigint));
    }

    #[test]
    fn fields_are_in_first_seen_order() {
        let schema = infer(&[json!({"z": 1}), json!({"a": 1})]);
        assert_eq!(paths(&schema), ["z", "a"]);
    }

    #[test]
    fn a_sample_that_disagrees_about_a_type_suggests_nothing() {
        let schema = infer(&[json!({"a": 1}), json!({"a": "x"})]);
        let a = schema.field("a").unwrap();
        assert_eq!(a.types, vec![InferredType::Integer, InferredType::String]);
        assert_eq!(a.settled_type(), None);
        assert_eq!(a.suggested_column(), None);
    }

    #[test]
    fn a_whole_number_suggests_the_wide_column_because_a_sample_cannot_bound_it() {
        let schema = infer(&[json!({"a": 1})]);
        assert_eq!(
            schema.field("a").unwrap().suggested_column(),
            Some(ColumnType::Bigint)
        );
    }

    #[test]
    fn text_shapes_become_column_suggestions() {
        let schema = infer(&[json!({
            "at": "2026-08-18T09:12:00Z",
            "day": "2026-08-18",
            "id": "3f2a1b4c-5d6e-4f70-8a9b-0c1d2e3f4a5b",
            "plain": "hello",
        })]);
        assert_eq!(
            schema.field("at").unwrap().suggested_column(),
            Some(ColumnType::Timestamp)
        );
        assert_eq!(
            schema.field("day").unwrap().suggested_column(),
            Some(ColumnType::Date)
        );
        assert_eq!(
            schema.field("id").unwrap().suggested_column(),
            Some(ColumnType::Uuid)
        );
        assert_eq!(
            schema.field("plain").unwrap().suggested_column(),
            Some(ColumnType::Text)
        );
    }

    #[test]
    fn one_string_of_another_shape_settles_the_field_as_plain_text() {
        let schema = infer(&[json!({"a": "2026-08-18"}), json!({"a": "later"})]);
        let a = schema.field("a").unwrap();
        assert_eq!(a.format, None);
        assert_eq!(a.suggested_column(), Some(ColumnType::Text));
    }

    #[test]
    fn a_shape_stays_settled_once_it_has_disagreed() {
        // the third message agrees with the first, and must not undo the second
        let schema = infer(&[
            json!({"a": "2026-08-18"}),
            json!({"a": "later"}),
            json!({"a": "2026-08-19"}),
        ]);
        assert_eq!(schema.field("a").unwrap().format, None);
    }

    #[test]
    fn a_field_that_is_always_null_suggests_nothing() {
        let schema = infer(&[json!({"a": null})]);
        let a = schema.field("a").unwrap();
        assert_eq!(a.settled_type(), Some(InferredType::Null));
        assert_eq!(a.suggested_column(), None);
    }

    #[test]
    fn an_object_or_an_array_suggests_json() {
        let schema = infer(&[json!({"o": {"x": 1}, "a": [1]})]);
        assert_eq!(
            schema.field("o").unwrap().suggested_column(),
            Some(ColumnType::Json)
        );
        assert_eq!(
            schema.field("a").unwrap().suggested_column(),
            Some(ColumnType::Json)
        );
    }

    #[test]
    fn an_example_is_kept_for_a_scalar_and_not_for_a_container() {
        let schema = infer(&[json!({"a": 1, "o": {"x": 1}, "arr": [1]})]);
        assert_eq!(schema.field("a").unwrap().example, Some(json!(1)));
        assert_eq!(schema.field("o").unwrap().example, None);
        assert_eq!(schema.field("arr").unwrap().example, None);
    }

    #[test]
    fn an_example_is_the_first_non_null_value() {
        let schema = infer(&[json!({"a": null}), json!({"a": 2}), json!({"a": 3})]);
        assert_eq!(schema.field("a").unwrap().example, Some(json!(2)));
    }

    #[test]
    fn a_long_example_string_is_cut() {
        let long = "é".repeat(EXAMPLE_MAX_CHARS + 10);
        let schema = infer(&[json!({ "a": long })]);
        let example = schema.field("a").unwrap().example.clone().unwrap();
        let text = example.as_str().unwrap();
        assert_eq!(text.chars().count(), EXAMPLE_MAX_CHARS + 1);
        assert!(text.ends_with('…'));
    }

    #[test]
    fn the_field_list_is_bounded_and_says_when_it_bit() {
        let mut message = serde_json::Map::new();
        for n in 0..(MAX_FIELDS + 10) {
            message.insert(format!("f{n}"), json!(n));
        }
        let schema = infer(&[Value::Object(message)]);
        assert_eq!(schema.fields.len(), MAX_FIELDS);
        assert!(schema.truncated);
    }

    #[test]
    fn the_walk_is_bounded_in_depth_and_says_when_it_bit() {
        let mut message = json!(1);
        for _ in 0..(MAX_DEPTH + 4) {
            message = json!({ "n": message });
        }
        let schema = infer(&[message]);
        assert!(schema.truncated);
        let deepest = schema.fields.last().unwrap();
        assert_eq!(deepest.path.matches('.').count(), MAX_DEPTH - 1);
    }

    #[test]
    fn paths_are_what_a_picker_offers() {
        let schema = infer(&[json!({"a": {"b": 1}})]);
        assert_eq!(schema.paths(), ["a", "a.b"]);
    }

    #[test]
    fn an_empty_sample_is_an_empty_schema() {
        let schema = infer(&[]);
        assert_eq!(schema, MessageSchema::default());
    }
}
