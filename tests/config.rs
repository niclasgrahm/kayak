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

use kayak_core::config::{Config, DummyPayload, InputConfig, InputKind, OutputKind, TransformKind};
use kayak_core::connections::{ConnectionKind, Connections};
use schemars::schema_for;
use serde_json::{Value, json};

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
        (
            "dummy",
            json!({
                "type": "dummy",
                "duration": 5,
                "payload": "number",
                "amplitude": 10.0,
                "period": 30.0
            }),
        ),
        ("http", json!({"type": "http", "capacity": 256})),
        (
            "nats",
            json!({"type": "nats", "connection": "local-nats", "subject": "test.subject"}),
        ),
        ("pipeline", json!({"type": "pipeline", "upstream": "p1"})),
        (
            "kafka",
            json!({
                "type": "kafka",
                "connection": "local-kafka",
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
            json!({
                "type": "reducer",
                "aggregations": [
                    {"function": "sum", "as": "total", "field": "value"},
                    {"function": "count", "as": "readings"}
                ],
                "group_by": ["sensor"],
                "on_missing": "skip"
            }),
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
        (
            "file",
            json!({
                "type": "file",
                "connection": "local-files",
                "path": "orders",
                "format": "ndjson",
                "rotate": {"max_rows": 100_000, "interval_secs": 3600}
            }),
        ),
        (
            "s3",
            json!({
                "type": "s3",
                "connection": "local-s3",
                "prefix": "orders",
                "format": "ndjson",
                "rotate": {"max_rows": 100_000, "interval_secs": 3600}
            }),
        ),
        (
            "nats",
            json!({"type": "nats", "connection": "local-nats", "subject": "out.subject"}),
        ),
        (
            "kafka",
            json!({"type": "kafka", "connection": "local-kafka", "topic": "out.events"}),
        ),
        (
            "postgres",
            json!({
                "type": "postgres",
                "connection": "local-postgres",
                "table": "readings"
            }),
        ),
    ]
}

/// One sample per connection kind, for the same reason the components have
/// them: the connections file is a wire format someone commits, and the
/// flatten/tag layout is as easy to break here as anywhere.
fn connection_samples() -> Vec<(&'static str, Value)> {
    vec![
        (
            "kafka",
            json!({"type": "kafka", "brokers": "localhost:9092"}),
        ),
        (
            "nats",
            json!({"type": "nats", "urls": "nats://localhost:4222"}),
        ),
        (
            "postgres",
            json!({
                "type": "postgres",
                "host": "localhost",
                "database": "kayak",
                "user": "kayak",
                "password": "${POSTGRES_PASSWORD}",
                "port": 5432
            }),
        ),
        ("file", json!({"type": "file", "root": "./out/events"})),
        (
            "s3",
            json!({
                "type": "s3",
                "bucket": "events",
                "access_key_id": "${S3_ACCESS_KEY_ID}",
                "secret_access_key": "${S3_SECRET_ACCESS_KEY}",
                "endpoint": "http://localhost:9000",
                "region": "us-east-1",
                "allow_http": true
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
        (
            "InputKind",
            serde_json::to_value(schema_for!(InputKind))?,
            sample_tags(&input_samples()),
        ),
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
        (
            "ConnectionKind",
            serde_json::to_value(schema_for!(ConnectionKind))?,
            sample_tags(&connection_samples()),
        ),
    ];

    for (name, schema, samples) in cases {
        let declared = tags_in_schema(&schema);
        assert!(
            !declared.is_empty(),
            "{name}: could not read any type tags out of the schema"
        );
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
        assert_eq!(
            serde_json::to_value(&parsed)?,
            config,
            "input '{tag}' changed shape"
        );
    }
    for (tag, transform) in transform_samples() {
        let config = json!({
            "id": "x",
            "inputs": [{"type": "dummy", "duration": 1}],
            "transforms": [transform],
            "outputs": [{"type": "stdout"}]
        });
        let parsed: Config = serde_json::from_value(config.clone())?;
        assert_eq!(
            serde_json::to_value(&parsed)?,
            config,
            "transform '{tag}' changed shape"
        );
    }
    for (tag, output) in output_samples() {
        let config = json!({
            "id": "x",
            "inputs": [{"type": "dummy", "duration": 1}],
            "transforms": [],
            "outputs": [output]
        });
        let parsed: Config = serde_json::from_value(config.clone())?;
        assert_eq!(
            serde_json::to_value(&parsed)?,
            config,
            "output '{tag}' changed shape"
        );
    }
    Ok(())
}

/// A connection round-trips the same way a component does, and the file it
/// lives in is a map of name to connection — that map *is* the wire format,
/// both on disk and out of `GET /api/connections`.
#[test]
fn every_connection_sample_round_trips_unchanged() -> anyhow::Result<()> {
    for (tag, connection) in connection_samples() {
        let file = json!({"the-name": connection});
        let parsed: Connections = serde_json::from_value(file.clone())?;
        assert_eq!(
            parsed.get("the-name").map(ConnectionKind::type_name),
            Some(tag)
        );
        assert_eq!(
            serde_json::to_value(&parsed)?,
            file,
            "connection '{tag}' changed shape"
        );
    }
    Ok(())
}

/// A component names a connection and carries nothing of its own about the
/// system: that split is the whole feature, and putting the brokers back on the
/// input would silently make the connection optional again.
#[test]
fn a_component_refers_to_a_connection_rather_than_describing_one() {
    for (name, sample) in input_samples().into_iter().chain(output_samples()) {
        let Some(object) = sample.as_object() else {
            panic!("'{name}' is not an object");
        };
        for moved in ["brokers", "urls", "host", "user", "password"] {
            assert!(
                !object.contains_key(moved),
                "'{name}' still describes the connection itself ('{moved}')"
            );
        }
    }
}

/// A component that names a connection must say so — leaving it out would
/// deserialize into a config that can never be built.
#[test]
fn a_component_without_its_connection_is_rejected() {
    let cases = [
        json!({"type": "nats", "subject": "x"}),
        json!({"type": "kafka", "topic": "x", "group": "g"}),
    ];
    for case in cases {
        let config = json!({"id": "x", "inputs": [case.clone()], "transforms": [], "outputs": []});
        assert!(
            serde_json::from_value::<Config>(config).is_err(),
            "expected {case} to be rejected"
        );
    }
}

/// The connections the repository's own config file names have to exist in the
/// connections file beside it, or the sample everything is copied from does not
/// start.
#[test]
fn the_repository_config_names_only_connections_that_are_configured() -> anyhow::Result<()> {
    let configs: Vec<Config> =
        serde_json::from_str(&std::fs::read_to_string("example_config/config.json")?)?;
    let connections: Connections = serde_json::from_str(&std::fs::read_to_string(
        "example_config/config.connections.json",
    )?)?;
    let named: Vec<&String> = configs.iter().flat_map(Config::connections).collect();
    assert!(!named.is_empty(), "the sample should exercise connections");
    for id in named {
        assert!(
            connections.contains(id),
            "config.json names '{id}', which config.connections.json does not declare"
        );
    }
    Ok(())
}

/// The YAML sample has to describe the same connections as the JSON one, for
/// the same reason the two config files are kept in step.
#[test]
fn the_repository_yaml_connections_are_the_same_connections() -> anyhow::Result<()> {
    let as_json: Connections = serde_json::from_str(&std::fs::read_to_string(
        "example_config/config.connections.json",
    )?)?;
    let as_yaml: Connections = serde_norway::from_str(&std::fs::read_to_string(
        "example_config/config.connections.yaml",
    )?)?;
    assert_eq!(as_json, as_yaml, "the two connection samples have drifted");
    Ok(())
}

/// `buffer` on an input is a decorator that sits alongside the input's own
/// fields — a shape that flatten makes easy to break.
#[test]
fn an_input_buffer_parses_alongside_the_input_fields() -> anyhow::Result<()> {
    let config: Config = serde_json::from_value(json!({
        "id": "x",
        "inputs": [{
            "type": "pipeline",
            "upstream": "p1",
            "buffer": { "type": "tumbling", "window_seconds": 60 }
        }],
        "transforms": [],
        "outputs": [{"type": "stdout"}]
    }))?;

    let input = only_input(&config);
    assert!(input.buffer.is_some(), "buffer config was dropped");
    assert!(matches!(input.kind, InputKind::Pipeline(_)));
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

/// The http input's only field is optional, so `{"type": "http"}` on its own is
/// a whole input — and it has to come back out that way, without a `"capacity":
/// null` a hand-written config never had.
#[test]
fn an_http_input_without_a_capacity_round_trips_bare() -> anyhow::Result<()> {
    let bare = json!({"type": "http"});
    let kind: InputKind = serde_json::from_value(bare.clone())?;
    assert!(matches!(kind, InputKind::Http(ref c) if c.capacity.is_none()));
    assert_eq!(serde_json::to_value(&kind)?, bare);
    Ok(())
}

/// Every dummy field but `duration` is optional, and the configs written before
/// they existed only have that one — so the bare form has to keep parsing, and
/// keep coming back out bare rather than gaining three nulls.
#[test]
fn a_dummy_input_without_a_payload_round_trips_bare() -> anyhow::Result<()> {
    let bare = json!({"type": "dummy", "duration": 1});
    let kind: InputKind = serde_json::from_value(bare.clone())?;
    assert!(
        matches!(kind, InputKind::Dummy(ref c)
            if c.payload.is_none() && c.amplitude.is_none() && c.period.is_none())
    );
    assert_eq!(serde_json::to_value(&kind)?, bare);
    Ok(())
}

/// The two payload spellings are the wire format the UI's dropdown posts.
#[test]
fn dummy_payloads_are_snake_case() -> anyhow::Result<()> {
    for (spelling, expected) in [("number", DummyPayload::Number), ("text", DummyPayload::Text)] {
        let kind: InputKind =
            serde_json::from_value(json!({"type": "dummy", "duration": 1, "payload": spelling}))?;
        assert!(matches!(kind, InputKind::Dummy(ref c) if c.payload == Some(expected)));
    }
    assert!(
        serde_json::from_value::<InputKind>(
            json!({"type": "dummy", "duration": 1, "payload": "Number"})
        )
        .is_err(),
        "payload spellings are snake_case only"
    );
    Ok(())
}

/// An unknown `type` must be rejected rather than silently ignored — otherwise
/// a typo in a pipeline config would start a pipeline that does the wrong thing.
#[test]
fn unknown_component_types_are_rejected() {
    let cases = [
        json!({"id": "x", "inputs": [{"type": "kafka"}], "transforms": [], "outputs": [{"type": "stdout"}]}),
        json!({"id": "x", "inputs": [{"type": "dummy", "duration": 1}], "transforms": [], "outputs": [{"type": "postgres", "connection": "c"}]}),
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
    let raw = std::fs::read_to_string("example_config/config.json")?;
    let configs: Vec<Config> = serde_json::from_str(&raw)?;
    assert!(
        !configs.is_empty(),
        "config.json should describe at least one pipeline"
    );
    Ok(())
}

/// `config.yaml` is the same graph as `config.json`, spelled the other way — a
/// file to point `--config` at when you want to exercise the YAML path by hand.
///
/// Kept honest here because a sample that drifts is worse than none: a
/// component added to `config.json` and not to `config.yaml` would leave the
/// YAML example quietly describing a graph the repo no longer has. It is
/// compared as *pipelines*, not as text — the two files order their fields
/// differently, and only the parsed result has to agree.
///
/// Both sides go through `persist::ordered` first, for the same reason: one of
/// the files was written by a save and the other by hand, so they declare the
/// same pipelines in a different order. That is not drift — the order a config
/// file may be written in is exactly what `ordered` defines — and comparing the
/// sequences raw would fail on it while missing nothing.
#[test]
fn the_repository_yaml_config_describes_the_same_graph() -> anyhow::Result<()> {
    let as_json: Vec<Config> =
        serde_json::from_str(&std::fs::read_to_string("example_config/config.json")?)?;
    let as_yaml: Vec<Config> =
        serde_norway::from_str(&std::fs::read_to_string("example_config/config.yaml")?)?;
    assert_eq!(
        serde_json::to_value(kayak::persist::ordered(as_yaml))?,
        serde_json::to_value(kayak::persist::ordered(as_json))?,
        "config.yaml has drifted from config.json; regenerate it"
    );
    Ok(())
}
