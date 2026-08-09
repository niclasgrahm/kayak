//! Attaching what an input knows about a message to the message.
//!
//! The metadata is **in band** — ordinary fields on the JSON, not a sidecar
//! travelling beside it — and that is the whole design decision. The alternative
//! (a `Message { value, meta }` the way Benthos does it) forces every transform
//! that changes cardinality to answer a question with no good general answer:
//! `reduce` collapses five hundred messages into one, `splitter` divides one
//! into several, the `http` transform replaces the batch with a service's
//! reply — whose metadata comes out? In band there is no such question. Metadata
//! is data, so `"group_by": ["_meta.subject"]` is a `group_by` like any other, and
//! reaching it needs no syntax that doesn't already exist.
//!
//! What it costs, said plainly: metadata reaches the outputs (a nats publish or
//! an ndjson file carries `_meta` unless something removes it), and the key can
//! collide with one the payload already uses. Both are the user's to decide,
//! which is why the whole thing is opt-in — see [`EnvelopeConfig`].
//!
//! Each input builds its own, because only the input knows the interesting
//! half: [`BuildCtx::envelope`] supplies the fields every input shares and the
//! input adds the subject, the topic and offset, the method it was posted with.

use kayak_core::config::EnvelopeConfig;
use serde_json::{Map, Value};

/// The metadata one message arrived with, as pairs, before it is written
/// anywhere. A `Vec` rather than a map because it is built once per message and
/// read once: the ordering is the declaration's, and nothing looks a key up.
pub type Meta = Vec<(&'static str, Value)>;

/// An input's answer to "attach metadata, and how".
///
/// [`Envelope::none`] is the default and passes messages through untouched —
/// the same promise `batch_cap` makes about batching. An input that quietly
/// changed the shape of its messages would break every field reference
/// downstream, so it takes asking.
pub struct Envelope {
    shape: Option<Shape>,
    /// What every message from this input carries whatever else happens to it:
    /// the pipeline, the kind, the connection where there is one.
    statics: Meta,
}

struct Shape {
    meta_field: String,
    /// `None` is the `merge` shape — the payload stays where it is.
    payload_field: Option<String>,
}

impl Envelope {
    /// No metadata: `wrap` hands every message straight back.
    #[must_use]
    pub fn none() -> Self {
        Self {
            shape: None,
            statics: Vec::new(),
        }
    }

    /// From what the config asked for, plus the fields this input shares with
    /// every other one.
    #[must_use]
    pub fn new(config: Option<&EnvelopeConfig>, statics: Meta) -> Self {
        let Some(config) = config else {
            return Self::none();
        };
        Self {
            shape: Some(Shape {
                meta_field: config.meta_field().to_string(),
                payload_field: config.payload_field().map(ToString::to_string),
            }),
            statics,
        }
    }

    /// Whether anything is attached at all. Inputs read this to skip building
    /// per-message metadata they know will be thrown away — the common case, and
    /// the one that has to stay free.
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        self.shape.is_some()
    }

    /// The message as it should be handed on, given what this input knows about
    /// it.
    ///
    /// `None` means the message cannot be enveloped and should be skipped: the
    /// `merge` shape has nowhere to put a field on a payload that isn't an
    /// object. That is reported the way a non-JSON payload already is — a
    /// warning and the next message — rather than by failing the pipeline,
    /// because one odd message on a subject is not the pipeline being
    /// misconfigured. `wrap` never returns `None`.
    #[must_use]
    pub fn apply(&self, payload: Value, own: Meta) -> Option<Value> {
        let Some(shape) = &self.shape else {
            return Some(payload);
        };

        let mut meta = Map::new();
        for (name, value) in self.statics.iter().cloned().chain(own) {
            meta.insert(name.to_string(), value);
        }
        meta.insert(
            "received_at".to_string(),
            Value::String(chrono::Utc::now().to_rfc3339()),
        );
        let meta = Value::Object(meta);

        match &shape.payload_field {
            Some(payload_field) => {
                let mut out = Map::new();
                out.insert(payload_field.clone(), payload);
                out.insert(shape.meta_field.clone(), meta);
                Some(Value::Object(out))
            }
            None => match payload {
                Value::Object(mut out) => {
                    out.insert(shape.meta_field.clone(), meta);
                    Some(Value::Object(out))
                }
                _ => None,
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{Envelope, Meta};
    use kayak_core::config::EnvelopeConfig;
    use serde_json::{Value, json};

    fn statics() -> Meta {
        vec![
            ("pipeline", json!("p1")),
            ("input", json!("nats")),
        ]
    }

    fn merge() -> EnvelopeConfig {
        EnvelopeConfig::Merge { meta: None }
    }

    fn wrap() -> EnvelopeConfig {
        EnvelopeConfig::Wrap {
            payload: None,
            meta: None,
        }
    }

    /// Strip the one field a test can't predict.
    fn without_time(value: &Value, meta_field: &str) -> Value {
        let mut value = value.clone();
        if let Some(meta) = value.get_mut(meta_field).and_then(Value::as_object_mut) {
            assert!(meta.remove("received_at").is_some(), "no received_at");
        }
        value
    }

    /// The default, and the thing every existing config relies on.
    #[test]
    fn without_a_config_the_message_is_untouched() {
        let envelope = Envelope::new(None, statics());
        assert!(!envelope.is_enabled());
        assert_eq!(
            envelope.apply(json!({ "value": 1 }), vec![]),
            Some(json!({ "value": 1 }))
        );
    }

    #[test]
    fn merge_adds_a_field_and_leaves_the_others_alone() {
        let envelope = Envelope::new(Some(&merge()), statics());
        let Some(out) = envelope.apply(json!({ "value": 1 }), vec![("subject", json!("m1.temp"))])
        else {
            panic!("an object payload can be merged into")
        };

        assert_eq!(
            without_time(&out, "_meta"),
            json!({
                "value": 1,
                "_meta": { "pipeline": "p1", "input": "nats", "subject": "m1.temp" },
            })
        );
    }

    /// The reason `wrap` exists: an OPC-style reading is a bare number, and
    /// `merge` has nowhere to put a field on one.
    #[test]
    fn merge_skips_a_payload_that_is_not_an_object() {
        let envelope = Envelope::new(Some(&merge()), statics());
        assert_eq!(envelope.apply(json!(1), vec![]), None);
        assert_eq!(envelope.apply(json!("recipe-a"), vec![]), None);
        assert_eq!(envelope.apply(json!([1, 2]), vec![]), None);
    }

    #[test]
    fn wrap_moves_the_whole_payload_whatever_it_is() {
        let envelope = Envelope::new(Some(&wrap()), statics());
        let Some(out) = envelope.apply(json!(1), vec![("subject", json!("m1.temperature"))])
        else {
            panic!("wrap always has somewhere to put the payload")
        };

        assert_eq!(
            without_time(&out, "_meta"),
            json!({
                "value": 1,
                "_meta": { "pipeline": "p1", "input": "nats", "subject": "m1.temperature" },
            })
        );
        assert!(
            envelope.apply(json!({ "a": 1 }), vec![]).is_some(),
            "wrap works on an object payload too"
        );
    }

    #[test]
    fn the_field_names_can_be_chosen() {
        let envelope = Envelope::new(
            Some(&EnvelopeConfig::Wrap {
                payload: Some("reading".to_string()),
                meta: Some("provenance".to_string()),
            }),
            vec![("input", json!("nats"))],
        );
        let Some(out) = envelope.apply(json!(7), vec![]) else {
            panic!("wrap")
        };
        assert_eq!(
            without_time(&out, "provenance"),
            json!({ "reading": 7, "provenance": { "input": "nats" } })
        );
    }

    /// An input's own fields are more specific than the shared ones, so they
    /// win — and the timestamp is always there.
    #[test]
    fn an_inputs_own_metadata_overrides_a_shared_field_of_the_same_name() {
        let envelope = Envelope::new(Some(&merge()), statics());
        let Some(out) = envelope.apply(json!({}), vec![("input", json!("something else"))]) else {
            panic!("an object payload can be merged into")
        };
        assert_eq!(out["_meta"]["input"], json!("something else"));
        assert!(out["_meta"]["received_at"].is_string());
    }
}
