//! Turning a component's config into inspector rows.
//!
//! The cards used to show `serde_json::to_string_pretty` of the whole `Config`.
//! This flattens it instead: one [`Section`] per component, each a kind (the
//! `type` tag) plus a flat list of `name: value` [`Property`] rows.
//!
//! It works off `serde_json::Value` rather than matching on the config enums on
//! purpose — a new component kind, or a new field on an existing one, shows up
//! in the UI without touching this file. The cost is that field *names* are the
//! wire names, which is also what the docs and the API use.
//!
//! Like `graph`, this is pure and unit-tested; `app.rs` only renders what it
//! returns.

use kayak_core::config::Config;
use serde_json::Value;

/// Shown when a config doesn't carry a `type` tag. Every current one does; this
/// is only here so a malformed config renders as a row rather than vanishing.
const UNKNOWN_KIND: &str = "unknown";
/// Placeholder for a null value, which reads better than the word "null".
const NO_VALUE: &str = "—";

/// One `name: value` row in the inspector.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Property {
    pub name: String,
    pub value: String,
}

/// One component: what kind it is, and how it's configured.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Section {
    pub kind: String,
    pub properties: Vec<Property>,
}

/// An inspector tab's label: the stage and how many components it holds.
///
/// Every tab is counted, including the ones that can only hold one today — a
/// count that appears on some tabs and not others reads as a property of the
/// pipeline rather than of the tab.
#[must_use]
pub fn tab_label(stage: &str, count: usize) -> String {
    format!("{stage} ({count})")
}

/// One section per input. Order is the configured order, which says nothing
/// about the order batches arrive in — inputs are merged, not chained — but is
/// at least stable between renders.
#[must_use]
pub fn input_sections(config: &Config) -> Vec<Section> {
    config
        .inputs
        .iter()
        .map(|i| section_of(serde_json::to_value(i)))
        .collect()
}

/// One section per output. Every output receives every batch, so this order is
/// presentational too.
#[must_use]
pub fn output_sections(config: &Config) -> Vec<Section> {
    config
        .outputs
        .iter()
        .map(|o| section_of(serde_json::to_value(o)))
        .collect()
}

/// One section per transform, in the order they run — which is the order the
/// tab shows them in, since that's the whole point of a transform chain.
#[must_use]
pub fn transform_sections(config: &Config) -> Vec<Section> {
    config
        .transforms
        .iter()
        .map(|t| section_of(serde_json::to_value(t)))
        .collect()
}

/// A config that can't be serialised has nothing to show, but it shouldn't take
/// the card down with it.
fn section_of(serialized: serde_json::Result<Value>) -> Section {
    section_from_json(&serialized.unwrap_or(Value::Null))
}

fn section_from_json(json: &Value) -> Section {
    let Some(fields) = json.as_object() else {
        return Section {
            kind: UNKNOWN_KIND.to_string(),
            properties: Vec::new(),
        };
    };

    let mut properties = Vec::new();
    for (name, value) in fields {
        // the tag is the section's heading, not one of its settings
        if name == "type" {
            continue;
        }
        push_properties(name, value, &mut properties);
    }

    Section {
        kind: fields
            .get("type")
            .and_then(Value::as_str)
            .unwrap_or(UNKNOWN_KIND)
            .to_string(),
        properties,
    }
}

/// Flatten a value into rows. Nested objects — an input's buffer, a filter's
/// operands — become dotted names rather than a blob of JSON, so everything
/// stays a two-column list however deep the config nests.
fn push_properties(name: &str, value: &Value, out: &mut Vec<Property>) {
    match value {
        Value::Object(fields) => {
            for (key, nested) in fields {
                // a nested tag names the thing it tags: `buffer` = "tumbling",
                // not `buffer.type` = "tumbling"
                let name = if key == "type" {
                    name.to_string()
                } else {
                    format!("{name}.{key}")
                };
                push_properties(&name, nested, out);
            }
        }
        Value::Array(items) => {
            for (i, item) in items.iter().enumerate() {
                push_properties(&format!("{name}[{i}]"), item, out);
            }
        }
        _ => out.push(Property {
            name: name.to_string(),
            value: render(value),
        }),
    }
}

/// Values are for reading, not for round-tripping: a string loses its quotes.
fn render(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        Value::Null => NO_VALUE.to_string(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kayak_core::config::{
        BufferConfig, BufferTransformConfig, DummyConfig, InputConfig, InputKind, NatsOutputConfig,
        OutputConfig, OutputKind, ReduceFnKind, ReduceTransformConfig, SplitterTransformConfig,
        StdoutOutputConfig, TransformConfig, TransformKind,
    };

    fn config(input: InputConfig, transforms: Vec<TransformKind>, output: OutputKind) -> Config {
        Config {
            id: None,
            inputs: vec![input],
            transforms: transforms
                .into_iter()
                .map(|kind| TransformConfig { kind })
                .collect(),
            outputs: vec![OutputConfig { kind: output }],
        }
    }

    /// Most of these tests build a config with exactly one input and one
    /// output and are about what that one section looks like.
    fn only(sections: Vec<Section>) -> Section {
        match <[Section; 1]>::try_from(sections) {
            Ok([section]) => section,
            Err(sections) => panic!("expected exactly one section, got {}", sections.len()),
        }
    }

    fn input_section(config: &Config) -> Section {
        only(input_sections(config))
    }

    fn output_section(config: &Config) -> Section {
        only(output_sections(config))
    }

    fn dummy_input() -> InputConfig {
        InputConfig {
            kind: InputKind::Dummy(DummyConfig { duration: 5 }),
            buffer: None,
        }
    }

    /// The three tabs are read side by side, so they have to be counted the
    /// same way — including an empty chain, which is a fact about the pipeline
    /// and not a reason to drop the count.
    #[test]
    fn every_tab_label_carries_its_count() {
        assert_eq!(tab_label("inputs", 1), "inputs (1)");
        assert_eq!(tab_label("outputs", 1), "outputs (1)");
        assert_eq!(tab_label("transforms", 2), "transforms (2)");
        assert_eq!(tab_label("transforms", 0), "transforms (0)");
    }

    /// A stage with several components shows one section each, in configured
    /// order — the card is the only place you can see that a pipeline reads
    /// from two places or writes to two.
    #[test]
    fn every_input_and_output_gets_its_own_section() {
        let config = Config {
            id: None,
            inputs: vec![
                dummy_input(),
                InputConfig {
                    kind: InputKind::Dummy(DummyConfig { duration: 9 }),
                    buffer: None,
                },
            ],
            transforms: vec![],
            outputs: vec![
                OutputConfig {
                    kind: OutputKind::Stdout(StdoutOutputConfig {}),
                },
                OutputConfig {
                    kind: OutputKind::Nats(NatsOutputConfig {
                        urls: "nats://localhost:4222".into(),
                        subject: "out".to_string(),
                    }),
                },
            ],
        };

        let inputs = input_sections(&config);
        assert_eq!(inputs.len(), 2);
        assert_eq!(value_of(&inputs[0], "duration"), "5");
        assert_eq!(value_of(&inputs[1], "duration"), "9");

        let outputs = output_sections(&config);
        assert_eq!(
            outputs.iter().map(|s| s.kind.as_str()).collect::<Vec<_>>(),
            vec!["stdout", "nats"]
        );
    }

    /// An output-less pipeline is legal — it exists to feed its children — and
    /// the tab has to be able to say so.
    #[test]
    fn a_stage_with_nothing_in_it_has_no_sections() {
        let config = Config {
            id: None,
            inputs: vec![dummy_input()],
            transforms: vec![],
            outputs: vec![],
        };
        assert!(output_sections(&config).is_empty());
        assert_eq!(
            tab_label("outputs", output_sections(&config).len()),
            "outputs (0)"
        );
    }

    fn value_of(section: &Section, name: &str) -> String {
        match section.properties.iter().find(|p| p.name == name) {
            Some(p) => p.value.clone(),
            None => panic!("no '{name}' row; got {:?}", section.properties),
        }
    }

    #[test]
    fn an_input_section_is_its_kind_plus_its_settings() {
        let section = input_section(&config(
            dummy_input(),
            vec![],
            OutputKind::Stdout(StdoutOutputConfig {}),
        ));

        assert_eq!(section.kind, "dummy");
        assert_eq!(
            section.properties,
            vec![Property {
                name: "duration".to_string(),
                value: "5".to_string(),
            }]
        );
    }

    /// The tag is the heading; showing it again as a `type: dummy` row would be
    /// the raw JSON all over again.
    #[test]
    fn the_type_tag_is_never_a_row() {
        let section = input_section(&config(
            dummy_input(),
            vec![],
            OutputKind::Stdout(StdoutOutputConfig {}),
        ));
        assert!(section.properties.iter().all(|p| p.name != "type"));
    }

    /// A buffer is a nested object on the input. It has to read as rows of the
    /// input's own section rather than as JSON.
    #[test]
    fn a_nested_buffer_becomes_dotted_rows() {
        let input = InputConfig {
            buffer: Some(BufferConfig::Tumbling { window_seconds: 10 }),
            ..dummy_input()
        };
        let section = input_section(&config(
            input,
            vec![],
            OutputKind::Stdout(StdoutOutputConfig {}),
        ));

        // the nested tag names the buffer itself
        assert_eq!(value_of(&section, "buffer"), "tumbling");
        assert_eq!(value_of(&section, "buffer.window_seconds"), "10");
    }

    /// Transforms run in order, so the inspector has to list them in order.
    #[test]
    fn transform_sections_keep_the_configured_order() {
        let sections = transform_sections(&config(
            dummy_input(),
            vec![
                TransformKind::Buffer(BufferTransformConfig { size: 100 }),
                TransformKind::Splitter(SplitterTransformConfig { out_size: 3 }),
                TransformKind::Reducer(ReduceTransformConfig {
                    function: ReduceFnKind::Avg,
                    field: "temperature".to_string(),
                }),
            ],
            OutputKind::Stdout(StdoutOutputConfig {}),
        ));

        let kinds: Vec<&str> = sections.iter().map(|s| s.kind.as_str()).collect();
        assert_eq!(kinds, ["buffer", "splitter", "reducer"]);
        assert_eq!(value_of(&sections[2], "field"), "temperature");
    }

    /// Several components have no settings at all; they still need a kind, or
    /// the tab would look broken.
    #[test]
    fn a_component_without_settings_still_has_a_kind() {
        let section = output_section(&config(
            dummy_input(),
            vec![],
            OutputKind::Stdout(StdoutOutputConfig {}),
        ));
        assert_eq!(section.kind, "stdout");
        assert!(section.properties.is_empty());
    }

    /// Quotes around every string would be noise in a two-column table.
    #[test]
    fn string_values_are_rendered_unquoted() {
        let section = output_section(&config(
            dummy_input(),
            vec![],
            OutputKind::Nats(NatsOutputConfig {
                urls: "nats://localhost:4222".into(),
                subject: "test.subject".to_string(),
            }),
        ));

        assert_eq!(value_of(&section, "urls"), "nats://localhost:4222");
        assert_eq!(value_of(&section, "subject"), "test.subject");
    }
}
