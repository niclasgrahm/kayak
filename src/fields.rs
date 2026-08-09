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

use serde_json::Value;

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

#[cfg(test)]
mod tests {
    use super::{get, leaf};
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
}
