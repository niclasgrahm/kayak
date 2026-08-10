//! How a transform finds the field it was configured with.
//!
//! Everything that addresses a field by name goes through here — `filter`'s two
//! comparisons and `reduce`'s `group_by` and aggregations — so a path works
//! everywhere or nowhere, rather than in whichever transform someone
//! remembered.
//!
//! **An exact key wins over a path.** `get(message, "a.b")` returns the value
//! under the literal key `"a.b"` if the message has one, and only otherwise
//! reads it as "the `b` inside the `a`". That ordering is what makes paths a
//! compatible addition: a source whose field names contain dots — and they
//! exist — keeps working exactly as it did, and no config has to learn an
//! escaping rule to say what it already said.
//!
//! Paths are the plain thing and no more: dot-separated object keys. There is
//! no array indexing and no wildcard, because the moment those exist the
//! question of what a transform does with several matches follows them, and
//! that is a different feature.
//!
//! # Writing
//!
//! [`set`] and [`remove`] are the write side, added for `map` — the first
//! transform that puts a value at a name the config chose. The read rule has an
//! obvious meaning ("both readings exist, prefer the exact one") and the write
//! rule does not, so it is spelled out here and everything that writes has to
//! follow it:
//!
//! 1. If the message already has the **literal key**, overwrite it. That is
//!    what makes a write round-trip a read: `copy` from `a.b` to `a.b` puts the
//!    value back where it found it, whichever of the two shapes that was.
//! 2. Otherwise write through the path, creating the objects on the way.
//! 3. If the path runs through something that is **not** an object, that is an
//!    error and not an overwrite. Replacing a scalar with an object to make
//!    room for a field is the kind of data loss nothing downstream can see.

use anyhow::{Result, bail};
use serde_json::{Map, Value};

/// The value at `field`, by exact key first and then as a dotted path.
///
/// `None` for a field that isn't there *and* for one that is explicitly `null`
/// is deliberately **not** decided here — that reading belongs to the caller,
/// since it is right for an aggregation and wrong for a filter that wants to
/// know a field exists. This returns what is there.
#[must_use]
pub fn get<'a>(message: &'a Value, field: &str) -> Option<&'a Value> {
    if let Some(value) = message.get(field) {
        return Some(value);
    }
    if !field.contains('.') {
        return None;
    }
    let mut current = message;
    for segment in field.split('.') {
        current = current.get(segment)?;
    }
    Some(current)
}

/// The name a path is known by once it is out of the message: the last segment.
///
/// A `reduce` grouping by `_meta.subject` writes the group's value into the
/// message it emits, and `subject` is the field downstream wants to see —
/// carrying the whole path through as a literal key would make every consumer
/// of that message spell the *upstream's* shape. The collisions this can cause
/// (`a.id` and `b.id` both landing on `id`) are refused when the reducer is
/// built rather than resolved here.
#[must_use]
pub fn leaf(field: &str) -> &str {
    field.rsplit('.').next().unwrap_or(field)
}

/// The top-level key a path reads through: its first segment.
///
/// The counterpart of [`leaf`], and what a column mapping's "which fields does
/// something read" question comes to — a column reading `sensor.id` makes
/// `sensor` a mapped field, since the alternative is to call every message with
/// a nested object unmapped.
#[must_use]
pub fn root_segment(field: &str) -> &str {
    field.split('.').next().unwrap_or(field)
}

/// Write `value` at `field`, by the rule in this module's docs.
///
/// The message has to be an object — there is nowhere to put a named field on a
/// number, and a caller that might be holding one is expected to have said so
/// before getting here.
pub fn set(message: &mut Value, field: &str, value: Value) -> Result<()> {
    let Some(object) = message.as_object_mut() else {
        bail!("cannot write '{field}': the message is not an object");
    };
    // rule 1: an existing literal key is the thing being written to, whatever
    // dots it contains. Checked with `contains_key` rather than by inserting,
    // so a path write isn't turned into a literal one by accident.
    if object.contains_key(field) || !field.contains('.') {
        object.insert(field.to_string(), value);
        return Ok(());
    }

    let mut current = object;
    let mut segments = field.split('.').peekable();
    while let Some(segment) = segments.next() {
        if segments.peek().is_none() {
            current.insert(segment.to_string(), value);
            return Ok(());
        }
        // rule 2: make the object if it isn't there. `or_insert_with` would
        // leave an existing non-object in place, which rule 3 then rejects.
        let entry = current
            .entry(segment)
            .or_insert_with(|| Value::Object(Map::new()));
        let Some(next) = entry.as_object_mut() else {
            bail!(
                "cannot write '{field}': '{segment}' is already there and is not an object, \
                 so there is nowhere to put the rest of the path"
            );
        };
        current = next;
    }
    // unreachable in practice: `split` always yields at least one segment, and
    // that one is handled as the last. Spelled out rather than unwrapped.
    bail!("cannot write '{field}': it names no field")
}

/// Take `field` off the message, by the same rule [`set`] follows: the literal
/// key first, then the path.
///
/// Returns whether anything was there. Removing a field that isn't there is not
/// an error anywhere this is used — `drop` is a statement about the shape the
/// message should leave with, not about the one it arrived in.
pub fn remove(message: &mut Value, field: &str) -> bool {
    let Some(object) = message.as_object_mut() else {
        return false;
    };
    if object.remove(field).is_some() {
        return true;
    }
    if !field.contains('.') {
        return false;
    }
    let mut current = object;
    let mut segments = field.split('.').peekable();
    while let Some(segment) = segments.next() {
        if segments.peek().is_none() {
            return current.remove(segment).is_some();
        }
        match current.get_mut(segment).and_then(Value::as_object_mut) {
            Some(next) => current = next,
            None => return false,
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::{get, leaf, remove, root_segment, set};
    use serde_json::json;

    #[test]
    fn a_plain_field_is_read_as_it_always_was() {
        let message = json!({ "value": 3 });
        assert_eq!(get(&message, "value"), Some(&json!(3)));
        assert_eq!(get(&message, "missing"), None);
    }

    #[test]
    fn a_dotted_field_reads_into_the_nested_object() {
        let message = json!({ "_meta": { "subject": "m1.temperature" } });
        assert_eq!(get(&message, "_meta.subject"), Some(&json!("m1.temperature")));
    }

    #[test]
    fn a_path_can_be_deeper_than_two() {
        let message = json!({ "a": { "b": { "c": 1 } } });
        assert_eq!(get(&message, "a.b.c"), Some(&json!(1)));
        assert_eq!(get(&message, "a.b.d"), None);
        assert_eq!(get(&message, "a.x.c"), None);
    }

    /// The compatibility rule: a source with a dot in a field name keeps
    /// working, and doesn't have to learn an escape to say so.
    #[test]
    fn a_literal_key_wins_over_the_path_reading() {
        let message = json!({
            "a.b": "literal",
            "a": { "b": "nested" },
        });
        assert_eq!(get(&message, "a.b"), Some(&json!("literal")));
    }

    /// Only when there is no literal key does the path get its turn, so both
    /// shapes are reachable from the same message.
    #[test]
    fn the_path_reading_applies_when_there_is_no_literal_key() {
        let message = json!({ "a": { "b": "nested" } });
        assert_eq!(get(&message, "a.b"), Some(&json!("nested")));
    }

    #[test]
    fn a_path_through_a_non_object_is_not_found() {
        let message = json!({ "a": 5 });
        assert_eq!(get(&message, "a.b"), None);
    }

    #[test]
    fn a_null_is_returned_rather_than_read_as_missing() {
        let message = json!({ "a": null });
        assert_eq!(get(&message, "a"), Some(&json!(null)));
    }

    #[test]
    fn a_paths_leaf_is_its_last_segment() {
        assert_eq!(leaf("_meta.subject"), "subject");
        assert_eq!(leaf("value"), "value");
        assert_eq!(leaf("a.b.c"), "c");
    }

    #[test]
    fn a_paths_root_is_its_first_segment() {
        assert_eq!(root_segment("_meta.subject"), "_meta");
        assert_eq!(root_segment("value"), "value");
        assert_eq!(root_segment("a.b.c"), "a");
    }

    #[test]
    fn a_plain_field_is_written_as_a_plain_field() -> anyhow::Result<()> {
        let mut message = json!({ "a": 1 });
        set(&mut message, "b", json!(2))?;
        assert_eq!(message, json!({ "a": 1, "b": 2 }));
        Ok(())
    }

    #[test]
    fn a_dotted_target_makes_the_objects_it_needs() -> anyhow::Result<()> {
        let mut message = json!({});
        set(&mut message, "a.b.c", json!(1))?;
        assert_eq!(message, json!({ "a": { "b": { "c": 1 } } }));
        Ok(())
    }

    #[test]
    fn a_dotted_target_writes_into_an_object_that_is_already_there() -> anyhow::Result<()> {
        let mut message = json!({ "a": { "x": 0 } });
        set(&mut message, "a.b", json!(1))?;
        assert_eq!(message, json!({ "a": { "x": 0, "b": 1 } }));
        Ok(())
    }

    /// Rule 1, and the reason it exists: a value read out of a literal key goes
    /// back into that same key, so writing a field to itself is a no-op rather
    /// than a change of shape.
    #[test]
    fn an_existing_literal_key_is_what_gets_written() -> anyhow::Result<()> {
        let mut message = json!({ "a.b": "literal", "a": { "b": "nested" } });
        let read = get(&message, "a.b").cloned().unwrap_or(json!(null));
        set(&mut message, "a.b", read)?;
        assert_eq!(message, json!({ "a.b": "literal", "a": { "b": "nested" } }));
        Ok(())
    }

    /// Rule 3. Overwriting the `5` with an object to make room for `a.b` would
    /// throw a value away that nothing downstream could tell had been there.
    #[test]
    fn a_path_through_a_scalar_is_refused_rather_than_overwriting_it() {
        let mut message = json!({ "a": 5 });
        let Err(error) = set(&mut message, "a.b", json!(1)) else {
            panic!("writing through a scalar should be refused");
        };
        assert!(error.to_string().contains("not an object"), "{error}");
        assert_eq!(message, json!({ "a": 5 }), "the message is left alone");
    }

    #[test]
    fn a_non_object_message_has_nowhere_to_write() {
        let mut message = json!(7);
        assert!(set(&mut message, "a", json!(1)).is_err());
    }

    #[test]
    fn removing_takes_the_literal_key_first() {
        let mut message = json!({ "a.b": 1, "a": { "b": 2 } });
        assert!(remove(&mut message, "a.b"));
        assert_eq!(message, json!({ "a": { "b": 2 } }));
        // and now that the literal key is gone, the same name reaches the path
        assert!(remove(&mut message, "a.b"));
        assert_eq!(message, json!({ "a": {} }));
    }

    #[test]
    fn removing_something_that_is_not_there_says_so_rather_than_failing() {
        let mut message = json!({ "a": { "b": 1 } });
        assert!(!remove(&mut message, "c"));
        assert!(!remove(&mut message, "a.c"));
        assert!(!remove(&mut message, "a.b.c"));
        assert_eq!(message, json!({ "a": { "b": 1 } }));
    }
}
