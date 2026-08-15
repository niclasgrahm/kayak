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

use kayak_core::config::{
    BufferConfig, Config, DummyPayload, EnvelopeConfig, InputConfig, InputKind, OutputKind,
    TransformKind,
};
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
        (
            "http",
            json!({
                "type": "http",
                "capacity": 256,
                "auth": {"type": "bearer", "token": "${INGEST_TOKEN}"}
            }),
        ),
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
        (
            "mqtt",
            json!({
                "type": "mqtt",
                "connection": "local-mqtt",
                "topic": "sensors/+/temperature",
                "qos": "at_least_once"
            }),
        ),
        (
            "redis",
            json!({"type": "redis", "connection": "local-redis", "channel": "test.channel"}),
        ),
        (
            "opcua",
            json!({
                "type": "opcua",
                "connection": "local-opcua",
                "nodes": [
                    {"node_id": "ns=3;s=FastUInt1", "name": "line_counter"},
                    {"node_id": "ns=3;s=SlowUInt1"}
                ],
                "browse": {"root": "ns=3;s=Anomaly", "depth": 2},
                "publish_interval_ms": 500,
                "sampling_interval_ms": 250,
                "queue_size": 10,
                "deadband": 0.5
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
        (
            "remember",
            json!({
                "type": "remember",
                "when": [
                    {"type": "string", "field": "_meta.signal", "operator": "EqualTo",
                     "value": "unit_id"}
                ],
                "remember": [{"field": "value", "as": "unit_id"}]
            }),
        ),
        (
            "recall",
            json!({
                "type": "recall",
                "recall": ["unit_id", "recipe"],
                "on_missing": "null"
            }),
        ),
        // deliberately one of every mapping kind: `mappings` is a list of a
        // tagged union, which is the most intricate shape in the config, and a
        // sample that only covered `copy` would let the other six drift.
        (
            "map",
            json!({
                "type": "map",
                "mappings": [
                    {"type": "copy", "from": "_meta.subject"},
                    {"type": "copy", "from": "id", "as": "sensor.id",
                     "default": {"type": "text", "value": "unknown"}},
                    {"type": "constant", "value": {"type": "text", "value": "line-3"},
                     "as": "line"},
                    {"type": "coalesce", "from": ["temp_c", "readings.celsius"],
                     "as": "celsius_in", "default": {"type": "null"}},
                    {"type": "cast", "from": "recorded_at", "to": "timestamp"},
                    {"type": "concat", "as": "asset", "parts": [
                        {"type": "field", "field": "site"},
                        {"type": "value", "value": "/"},
                        {"type": "field", "field": "machine"}
                    ]},
                    {"type": "arithmetic", "as": "_offset", "operator": "subtract",
                     "left": {"type": "field", "field": "fahrenheit"},
                     "right": {"type": "value", "value": 32.0}},
                    {"type": "arithmetic", "as": "celsius", "operator": "divide",
                     "left": {"type": "field", "field": "_offset"},
                     "right": {"type": "value", "value": 1.8}},
                    {"type": "drop", "from": ["_offset", "_meta"]}
                ],
                "on_missing": "omit"
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
                "table": "readings",
                "columns": [
                    {"name": "sensor_id", "type": "text", "field": "sensor.id", "nullable": false},
                    {"name": "temperature", "type": "float", "on_missing": "skip_row"},
                    {"name": "raw", "type": "json", "message": true}
                ],
                "create_table": true,
                "primary_key": ["sensor_id"],
                "indexes": [{"columns": ["temperature"], "unique": false}],
                // `ignore` is the default and is dropped on the way out, so the
                // sample names the other one — a round trip has to carry it
                "on_extra_fields": "error"
            }),
        ),
        (
            "clickhouse",
            json!({
                "type": "clickhouse",
                "connection": "local-clickhouse",
                "table": "readings",
                "columns": [
                    {"name": "sensor_id", "type": "text", "field": "sensor.id", "nullable": false},
                    {"name": "temperature", "type": "float", "on_missing": "skip_row"},
                    {"name": "raw", "type": "json", "message": true}
                ],
                "create_table": true,
                // the sorting key, not a primary key — clickhouse has no unique
                // constraint, and the name says which of the two this is
                "order_by": ["sensor_id"],
                "on_extra_fields": "error"
            }),
        ),
        (
            "mqtt",
            json!({
                "type": "mqtt",
                "connection": "local-mqtt",
                "topic": "out.events",
                "qos": "at_least_once",
                "retain": false
            }),
        ),
        (
            "redis",
            json!({"type": "redis", "connection": "local-redis", "channel": "out.channel"}),
        ),
        (
            "http",
            json!({
                "type": "http",
                "url": "https://example.com/hooks/readings",
                // POST and `batch` are the defaults, so the sample names the
                // other spellings — a round trip has to carry what was written
                "verb": "PUT",
                "body": "message",
                "auth": {"type": "bearer", "token": "${WEBHOOK_TOKEN}"},
                "timeout_seconds": 5
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
        (
            "clickhouse",
            json!({
                "type": "clickhouse",
                "url": "http://localhost:8123",
                "database": "kayak",
                "user": "kayak",
                "password": "${CLICKHOUSE_PASSWORD}",
                "allow_http": true
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
        (
            "mqtt",
            json!({
                "type": "mqtt",
                "host": "localhost",
                "port": 1883,
                "username": "kayak",
                "password": "${MQTT_PASSWORD}"
            }),
        ),
        (
            "redis",
            json!({"type": "redis", "url": "redis://localhost:6379"}),
        ),
        (
            "opcua",
            json!({
                "type": "opcua",
                "endpoint": "opc.tcp://localhost:50000",
                "username": "kayak",
                "password": "${OPCUA_PASSWORD}"
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
        kayak::persist::read(std::path::Path::new("example_config/config.json"))?.pipelines;
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

/// The sample covers every component kind on purpose, and the same goes for
/// both envelope shapes — it is what the UI is inspected against and what
/// anyone reads to find out how a feature is written down.
#[test]
fn the_repository_config_exercises_both_envelope_shapes() -> anyhow::Result<()> {
    let configs: Vec<Config> =
        kayak::persist::read(std::path::Path::new("example_config/config.json"))?.pipelines;
    let envelopes: Vec<&EnvelopeConfig> = configs
        .iter()
        .flat_map(|c| &c.inputs)
        .filter_map(|i| i.envelope.as_ref())
        .collect();

    assert!(
        envelopes
            .iter()
            .any(|e| matches!(e, EnvelopeConfig::Merge { .. })),
        "no input in the sample attaches metadata with a `merge` envelope"
    );
    assert!(
        envelopes
            .iter()
            .any(|e| matches!(e, EnvelopeConfig::Wrap { .. })),
        "no input in the sample attaches metadata with a `wrap` envelope"
    );
    Ok(())
}

/// `sensors_10s_avg` groups by `_meta.subject`, which only exists because the
/// `sensors` input attaches it. The two are a pair: dropping that envelope
/// would leave a reducer grouping on a field no message carries, and
/// `on_missing: skip` means it would emit nothing rather than fail loudly.
#[test]
fn a_sample_reducer_grouping_on_metadata_has_an_input_that_attaches_it() -> anyhow::Result<()> {
    let configs: Vec<Config> =
        kayak::persist::read(std::path::Path::new("example_config/config.json"))?.pipelines;

    let mut checked = 0;
    for config in &configs {
        let groups_on_metadata = config.transforms.iter().any(|t| match &t.kind {
            TransformKind::Reducer(r) => r.group_by.iter().any(|g| g.starts_with("_meta.")),
            _ => false,
        });
        if !groups_on_metadata {
            continue;
        }
        checked += 1;
        for upstream in config.upstreams() {
            let Some(parent) = configs.iter().find(|c| c.id.as_ref() == Some(upstream)) else {
                panic!("'{upstream}' is not in the sample")
            };
            assert!(
                parent.inputs.iter().any(|i| i.envelope.is_some()),
                "pipeline '{:?}' groups by a metadata field, but its upstream '{upstream}' \
                 attaches none",
                config.id
            );
        }
    }
    assert!(checked > 0, "the sample should demonstrate a metadata group_by");
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

/// Every shape of buffer has to survive the round trip, and the two older ones
/// have to parse exactly as they always did — a config file written before
/// `batch` existed is not allowed to change meaning or stop loading.
#[test]
fn every_buffer_shape_parses_and_round_trips() -> anyhow::Result<()> {
    for (wire, expected) in [
        (
            json!({"type": "static", "size": 10}),
            BufferConfig::Static { size: 10 },
        ),
        (
            json!({"type": "tumbling", "window_seconds": 30}),
            BufferConfig::Tumbling { window_seconds: 30 },
        ),
        (
            json!({"type": "batch", "size": 10, "window_seconds": 30}),
            BufferConfig::Batch {
                size: 10,
                window_seconds: 30,
            },
        ),
    ] {
        let parsed: BufferConfig = serde_json::from_value(wire.clone())?;
        assert_eq!(
            format!("{parsed:?}"),
            format!("{expected:?}"),
            "parsed {wire} as the wrong buffer"
        );
        assert_eq!(serde_json::to_value(&parsed)?, wire, "{wire} did not survive");
    }
    Ok(())
}

/// `batch` needs both limits — half of one is one of the other two, and serde
/// filling in a zero would silently mean "close the window immediately".
#[test]
fn a_batch_buffer_missing_a_limit_is_refused() {
    assert!(
        serde_json::from_value::<BufferConfig>(json!({"type": "batch", "size": 10})).is_err(),
        "a batch buffer with no window should not parse"
    );
    assert!(
        serde_json::from_value::<BufferConfig>(json!({"type": "batch", "window_seconds": 30}))
            .is_err(),
        "a batch buffer with no size should not parse"
    );
}

/// `envelope` is the other decorator on `InputConfig`, and shares the flatten
/// hazard with `buffer`: both sit alongside the input's own fields.
#[test]
fn an_input_envelope_parses_alongside_the_input_fields() -> anyhow::Result<()> {
    let config: Config = serde_json::from_value(json!({
        "id": "x",
        "inputs": [{
            "type": "nats",
            "connection": "local-nats",
            "subject": "*.temperature",
            "envelope": { "type": "wrap", "payload": "reading" }
        }],
        "transforms": [],
        "outputs": [{"type": "stdout"}]
    }))?;

    let input = only_input(&config);
    let Some(envelope) = input.envelope.as_ref() else {
        panic!("envelope config was dropped")
    };
    assert_eq!(envelope.payload_field(), Some("reading"));
    // not given, so the default rather than nothing
    assert_eq!(envelope.meta_field(), "_meta");
    assert!(matches!(input.kind, InputKind::Nats(_)));
    Ok(())
}

/// Both fields of both shapes are optional, so the tag alone is a whole
/// envelope — and has to round-trip as one rather than growing nulls.
#[test]
fn an_envelope_round_trips_without_its_optional_fields() -> anyhow::Result<()> {
    let sample = json!({
        "type": "http",
        "envelope": { "type": "merge" }
    });
    let input: InputConfig = serde_json::from_value(sample.clone())?;
    let Some(envelope) = input.envelope.as_ref() else {
        panic!("envelope config was dropped")
    };
    assert_eq!(envelope.meta_field(), "_meta");
    assert_eq!(envelope.payload_field(), None, "merge moves no payload");
    assert_eq!(serde_json::to_value(&input)?, sample);
    Ok(())
}

#[test]
fn an_input_without_an_envelope_is_valid_and_stays_absent() -> anyhow::Result<()> {
    let sample = json!({"type": "dummy", "duration": 1});
    let input: InputConfig = serde_json::from_value(sample.clone())?;
    assert!(input.envelope.is_none());
    assert_eq!(
        serde_json::to_value(&input)?,
        sample,
        "an absent envelope must not serialize as null"
    );
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
    let file = kayak::persist::read(std::path::Path::new("example_config/config.json"))?;
    assert!(
        !file.pipelines.is_empty(),
        "config.json should describe at least one pipeline"
    );
    // the sample declares buckets, so it is also what pins the *document*
    // spelling of a config file as a thing that really parses
    assert!(
        !file.state.is_empty(),
        "config.json should declare at least one state bucket"
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
    let as_json = kayak::persist::read(std::path::Path::new("example_config/config.json"))?;
    let as_yaml = kayak::persist::read(std::path::Path::new("example_config/config.yaml"))?;
    assert_eq!(
        serde_json::to_value(kayak::persist::ordered(as_yaml.pipelines))?,
        serde_json::to_value(kayak::persist::ordered(as_json.pipelines))?,
        "config.yaml has drifted from config.json; regenerate it"
    );
    Ok(())
}
