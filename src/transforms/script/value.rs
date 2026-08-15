//! Moving messages between `serde_json::Value` and rhai's `Dynamic`.
//!
//! This is hand-written rather than going through `rhai::serde`, and the reason
//! is the one thing that decides whether a scripted transform is affordable at
//! all: a serde round trip builds an intermediate representation on the way in
//! *and* on the way out, so a script that reads two fields of a fifty-field
//! message pays for all fifty, twice. Walking the two trees directly pays once
//! per direction and allocates nothing that isn't in the result.
//!
//! Two asymmetries are deliberate:
//!
//! - **Numbers narrow to an integer when they are one.** JSON has one number
//!   type and rhai has two, so `1` arriving as a float would make `msg.count ==
//!   1` false and `msg.count % 2` a type error. An integer that fits an `i64`
//!   becomes one; everything else becomes a float. The way back is
//!   [`serde_json::Number`]'s own rules, so a round trip of an untouched value
//!   is exact.
//! - **The way back can fail; the way in cannot.** Every JSON value has a rhai
//!   spelling, but the reverse isn't true — a script can put a timestamp, a
//!   function pointer or a custom type into a map, and none of those are
//!   messages. That is an error naming the type rather than a silent null,
//!   because a field that quietly became `null` is the kind of wrong nothing
//!   downstream can see.

use rhai::{Dynamic, Map};
use serde_json::{Map as JsonMap, Number, Value};

/// A message on its way into a script.
///
/// Infallible: every JSON value has a rhai spelling.
pub fn to_dynamic(value: &Value) -> Dynamic {
    match value {
        Value::Null => Dynamic::UNIT,
        Value::Bool(b) => Dynamic::from_bool(*b),
        Value::Number(n) => number_to_dynamic(n),
        Value::String(s) => Dynamic::from(rhai::ImmutableString::from(s.as_str())),
        Value::Array(items) => {
            let array: rhai::Array = items.iter().map(to_dynamic).collect();
            Dynamic::from_array(array)
        }
        Value::Object(fields) => {
            let map: Map = fields
                .iter()
                .map(|(key, value)| (key.as_str().into(), to_dynamic(value)))
                .collect();
            Dynamic::from_map(map)
        }
    }
}

/// An integer when the JSON number is one and it fits, a float otherwise.
///
/// See the module docs: this is what keeps `msg.count == 1` true for a message
/// that arrived carrying `1`.
fn number_to_dynamic(n: &Number) -> Dynamic {
    if let Some(i) = n.as_i64() {
        Dynamic::from_int(i)
    } else if n.as_u64().is_some() {
        // Above `i64::MAX`. There is no lossless rhai integer for it, and a
        // float at least keeps the magnitude — the precision loss is the point
        // of the branch rather than an oversight.
        Dynamic::from_float(n.as_f64().unwrap_or(f64::NAN))
    } else {
        Dynamic::from_float(n.as_f64().unwrap_or(f64::NAN))
    }
}

/// A message on its way back out of a script.
///
/// Fails on anything rhai can hold that a message cannot — see the module docs
/// on why that is an error rather than a null.
pub fn from_dynamic(value: &Dynamic) -> Result<Value, UnrepresentableValue> {
    if value.is_unit() {
        return Ok(Value::Null);
    }
    if let Some(b) = value.clone().try_cast::<bool>() {
        return Ok(Value::Bool(b));
    }
    if let Some(i) = value.clone().try_cast::<i64>() {
        return Ok(Value::Number(i.into()));
    }
    if let Some(f) = value.clone().try_cast::<f64>() {
        // A NaN or an infinity has no JSON spelling at all. Naming it is worth
        // more than writing `null` into the field it came from.
        return Number::from_f64(f)
            .map(Value::Number)
            .ok_or_else(|| UnrepresentableValue::new("a number that is not finite"));
    }
    if let Some(c) = value.clone().try_cast::<char>() {
        return Ok(Value::String(c.to_string()));
    }
    if let Some(s) = value.clone().try_cast::<rhai::ImmutableString>() {
        return Ok(Value::String(s.to_string()));
    }
    if let Some(items) = value.clone().try_cast::<rhai::Array>() {
        let mut out = Vec::with_capacity(items.len());
        for item in &items {
            out.push(from_dynamic(item)?);
        }
        return Ok(Value::Array(out));
    }
    if let Some(fields) = value.clone().try_cast::<Map>() {
        let mut out = JsonMap::with_capacity(fields.len());
        for (key, value) in &fields {
            out.insert(key.to_string(), from_dynamic(value)?);
        }
        return Ok(Value::Object(out));
    }
    Err(UnrepresentableValue::new(value.type_name()))
}

/// Something a script produced that is not a message.
///
/// Carries the rhai type name rather than the value: the value may be large,
/// and the type is what says how to fix the script.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnrepresentableValue {
    type_name: String,
}

impl UnrepresentableValue {
    fn new(type_name: impl Into<String>) -> Self {
        Self {
            type_name: type_name.into(),
        }
    }
}

impl std::fmt::Display for UnrepresentableValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "a script produced {}, which is not something a message can hold",
            self.type_name
        )
    }
}

impl std::error::Error for UnrepresentableValue {}

#[cfg(test)]
mod tests {
    use super::*;

    /// The property the whole bridge rests on: a message a script does not
    /// touch comes out of it byte-for-byte the message that went in. Every
    /// shape a payload can take, including the nesting that makes the recursion
    /// worth testing.
    #[test]
    fn an_untouched_message_round_trips_exactly() -> Result<(), UnrepresentableValue> {
        let message = serde_json::json!({
            "id": "sensor-1",
            "count": 42,
            "ratio": 0.5,
            "ok": true,
            "missing": null,
            "tags": ["a", "b"],
            "nested": { "deep": { "deeper": [1, 2, {"x": false}] } },
            "empty_object": {},
            "empty_array": [],
        });
        assert_eq!(from_dynamic(&to_dynamic(&message))?, message);
        Ok(())
    }

    /// A JSON number is one type and rhai's are two, so an integer that arrived
    /// as an integer has to *be* one inside the script — otherwise `msg.count
    /// == 1` is false and `msg.count % 2` does not compile. See the module
    /// docs.
    ///
    /// The last two assertions are the edge worth knowing about: the split
    /// follows what the *source* wrote, not what the value happens to equal, so
    /// a stream sending `42.0` gets a float in the script. Narrowing it would
    /// be the wrong trade — it would make a genuine `1.0` come back out of an
    /// untouched message as `1`, and the round trip is the more important
    /// promise.
    #[test]
    fn a_whole_number_arrives_as_an_integer_and_a_fractional_one_as_a_float() {
        assert!(to_dynamic(&serde_json::json!(42)).is_int());
        assert!(to_dynamic(&serde_json::json!(-42)).is_int());
        assert!(to_dynamic(&serde_json::json!(0)).is_int());
        assert!(to_dynamic(&serde_json::json!(0.5)).is_float());
        assert!(to_dynamic(&serde_json::json!(42.0)).is_float());
    }

    /// A number too big for an `i64` still has to arrive as *something*
    /// numeric — a script comparing it against a threshold is the case, and an
    /// error there would fail a batch over a field the script never read.
    #[test]
    fn a_number_beyond_an_integer_arrives_as_a_float() {
        let huge = serde_json::json!(u64::MAX);
        assert!(to_dynamic(&huge).is_float());
    }

    /// The way out is fallible on purpose. A `null` here would put the failure
    /// in the data instead of in the log — see the module docs.
    #[test]
    fn a_value_a_message_cannot_hold_is_an_error_naming_its_type() {
        let not_finite = Dynamic::from_float(f64::INFINITY);
        let Err(err) = from_dynamic(&not_finite) else {
            panic!("an infinity is not a JSON number");
        };
        assert!(
            err.to_string().contains("not finite"),
            "the error should say what was wrong: {err}"
        );

        // rhai spells `Instant` "timestamp", and that is the name the script
        // author knows it by — the error carries rhai's word, not Rust's.
        let timestamp = Dynamic::from(std::time::Instant::now());
        let Err(err) = from_dynamic(&timestamp) else {
            panic!("an Instant is not a message field");
        };
        assert!(
            err.to_string().contains("timestamp"),
            "the error should name the type: {err}"
        );
    }

    /// A nested unrepresentable value has to fail the whole message rather than
    /// being dropped out of the object it sat in.
    #[test]
    fn an_unrepresentable_value_nested_in_a_message_fails_the_message() {
        let mut map = Map::new();
        map.insert("ok".into(), Dynamic::from_int(1));
        map.insert("bad".into(), Dynamic::from_float(f64::NAN));
        assert!(from_dynamic(&Dynamic::from_map(map)).is_err());

        let array = rhai::Array::from([Dynamic::from_int(1), Dynamic::from_float(f64::NAN)]);
        assert!(from_dynamic(&Dynamic::from_array(array)).is_err());
    }

    /// rhai has a `char` and JSON does not. Indexing a string yields one, so a
    /// script assembling a field out of characters is an ordinary thing to
    /// write and must not fail on the way out.
    #[test]
    fn a_character_comes_back_as_a_string() -> Result<(), UnrepresentableValue> {
        assert_eq!(
            from_dynamic(&Dynamic::from('x'))?,
            Value::String("x".to_string())
        );
        Ok(())
    }
}
