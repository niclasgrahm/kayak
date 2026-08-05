//! Wire-format tests for the config types.
//!
//! The JSON shape here is the project's public API: it's what the UI posts,
//! what `config.json` contains and what `/docs` advertises. `#[serde(flatten)]`
//! plus internally-tagged enums make it easy to change that shape by accident,
//! so every component kind gets a round-trip sample.
//!
//! `every_component_kind_has_a_wire_format_sample` is the guard rail for
//! scaling out: adding a variant to `InputKind`/`TransformKind`/`OutputKind`
//! fails this file until a sample is added below.

use std::collections::BTreeSet;

use schemars::schema_for;
use serde_json::{Value, json};
use streamer_core::config::{Config, InputConfig, InputKind, OutputKind, TransformKind};

/// These samples all declare exactly one input; reach for it without a panic
/// path at every call site.
fn only_input(config: &Config) -> &InputConfig {
    match config.inputs.as_slice() {
        [input] => input,
        other => panic!("expected exactly one input, got {}", other.len()),
    }
}

fn input_samples() -> Vec<(&'static str, Value)> {
    vec![
        ("dummy", json!({"type": "dummy", "duration": 5})),
        (
            "nats",
            json!({"type": "nats", "urls": "nats://localhost:4222", "subject": "test.subject"}),
        ),
        ("streamer", json!({"type": "streamer", "upstream": "p1"})),
        (
            "kafka",
            json!({
                "type": "kafka",
                "brokers": "localhost:9092",
                "topic": "test.events",
                "group": "kayak",
                "start_at": "latest"
            }),
        ),
    ]
}

fn transform_samples() -> Vec<(&'static str, Value)> {
    vec![
        ("buffer", json!({"type": "buffer", "size": 10})),
        (
            "http",
            json!({"type": "http", "url": "http://localhost/x", "verb": "POST"}),
        ),
        ("splitter", json!({"type": "splitter", "out_size": 2})),
        (
            "reducer",
            json!({"type": "reducer", "function": "sum", "field": "value"}),
        ),
        (
            "filter",
            json!({
                "type": "filter",
                "Numeric": {"field": "value", "operator": "GreaterThan", "value": 10.0}
            }),
        ),
    ]
}

fn output_samples() -> Vec<(&'static str, Value)> {
    vec![
        ("stdout", json!({"type": "stdout"})),
        ("file", json!({"type": "file"})),
        (
            "nats",
            json!({"type": "nats", "urls": "nats://localhost:4222", "subject": "out.subject"}),
        ),
        (
            "kafka",
            json!({"type": "kafka", "brokers": "localhost:9092", "topic": "out.events"}),
        ),
        (
            "postgres",
            json!({
                "type": "postgres",
                "host": "localhost",
                "port": 5432,
                "database": "kayak",
                "user": "kayak",
                "password": "${POSTGRES_PASSWORD}",
                "table": "readings"
            }),
        ),
    ]
}

/// The `type` tags a schema accepts, read out of the generated JSON schema.
fn tags_in_schema(schema: &Value) -> BTreeSet<String> {
    schema["oneOf"]
        .as_array()
        .map(|variants| {
            variants
                .iter()
                .filter_map(|v| v["properties"]["type"]["const"].as_str())
                .map(ToString::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn sample_tags(samples: &[(&'static str, Value)]) -> BTreeSet<String> {
    samples.iter().map(|(tag, _)| (*tag).to_string()).collect()
}

/// Adding a component means adding a config variant. If that variant has no
/// sample here, nothing else in this file covers it — so fail loudly.
#[test]
fn every_component_kind_has_a_wire_format_sample() -> anyhow::Result<()> {
    let cases = [
        ("InputKind", serde_json::to_value(schema_for!(InputKind))?, sample_tags(&input_samples())),
        (
            "TransformKind",
            serde_json::to_value(schema_for!(TransformKind))?,
            sample_tags(&transform_samples()),
        ),
        (
            "OutputKind",
            serde_json::to_value(schema_for!(OutputKind))?,
            sample_tags(&output_samples()),
        ),
    ];

    for (name, schema, samples) in cases {
        let declared = tags_in_schema(&schema);
        assert!(!declared.is_empty(), "{name}: could not read any type tags out of the schema");
        assert_eq!(
            declared, samples,
            "{name}: the variants and the samples in tests/config.rs have drifted apart — \
             add a sample for any new component"
        );
    }
    Ok(())
}

/// Each sample must parse into a `Config` and serialise back to exactly the
/// same JSON — that's what keeps the flatten/tag layout stable.
#[test]
fn every_component_sample_round_trips_unchanged() -> anyhow::Result<()> {
    for (tag, input) in input_samples() {
        let config = json!({"id": "x", "inputs": [input], "transforms": [], "outputs": [{"type": "stdout"}]});
        let parsed: Config = serde_json::from_value(config.clone())?;
        assert_eq!(serde_json::to_value(&parsed)?, config, "input '{tag}' changed shape");
    }
    for (tag, transform) in transform_samples() {
        let config = json!({
            "id": "x",
            "inputs": [{"type": "dummy", "duration": 1}],
            "transforms": [transform],
            "outputs": [{"type": "stdout"}]
        });
        let parsed: Config = serde_json::from_value(config.clone())?;
        assert_eq!(serde_json::to_value(&parsed)?, config, "transform '{tag}' changed shape");
    }
    for (tag, output) in output_samples() {
        let config = json!({
            "id": "x",
            "inputs": [{"type": "dummy", "duration": 1}],
            "transforms": [],
            "outputs": [output]
        });
        let parsed: Config = serde_json::from_value(config.clone())?;
        assert_eq!(serde_json::to_value(&parsed)?, config, "output '{tag}' changed shape");
    }
    Ok(())
}

/// `buffer` on an input is a decorator that sits alongside the input's own
/// fields — a shape that flatten makes easy to break.
#[test]
fn an_input_buffer_parses_alongside_the_input_fields() -> anyhow::Result<()> {
    let config: Config = serde_json::from_value(json!({
        "id": "x",
        "inputs": [{
            "type": "streamer",
            "upstream": "p1",
            "buffer": { "type": "tumbling", "window_seconds": 60 }
        }],
        "transforms": [],
        "outputs": [{"type": "stdout"}]
    }))?;

    let input = only_input(&config);
    assert!(input.buffer.is_some(), "buffer config was dropped");
    assert!(matches!(input.kind, InputKind::Streamer(_)));
    Ok(())
}

#[test]
fn an_input_without_a_buffer_is_valid() -> anyhow::Result<()> {
    let config: Config = serde_json::from_value(json!({
        "id": "x",
        "inputs": [{"type": "dummy", "duration": 1}],
        "transforms": [],
        "outputs": [{"type": "stdout"}]
    }))?;
    assert!(only_input(&config).buffer.is_none());
    Ok(())
}

/// An unknown `type` must be rejected rather than silently ignored — otherwise
/// a typo in a pipeline config would start a pipeline that does the wrong thing.
#[test]
fn unknown_component_types_are_rejected() {
    let cases = [
        json!({"id": "x", "inputs": [{"type": "kafka"}], "transforms": [], "outputs": [{"type": "stdout"}]}),
        json!({"id": "x", "inputs": [{"type": "dummy", "duration": 1}], "transforms": [{"type": "map"}], "outputs": [{"type": "stdout"}]}),
        json!({"id": "x", "inputs": [{"type": "dummy", "duration": 1}], "transforms": [], "outputs": [{"type": "s3"}]}),
    ];
    for case in cases {
        assert!(
            serde_json::from_value::<Config>(case.clone()).is_err(),
            "expected {case} to be rejected"
        );
    }
}

/// The config file that ships with the repo has to stay loadable — it's the
/// example every new pipeline gets copied from, and the Dockerfile bakes it in.
#[test]
fn the_repository_config_file_parses() -> anyhow::Result<()> {
    let raw = std::fs::read_to_string("config.json")?;
    let configs: Vec<Config> = serde_json::from_str(&raw)?;
    assert!(!configs.is_empty(), "config.json should describe at least one pipeline");
    Ok(())
}
