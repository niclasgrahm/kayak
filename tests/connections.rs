//! Named connections, end to end: the file, the HTTP surface, and what a
//! pipeline does with the name it was given.
//!
//! The point of the feature is that the settings for a system live in one place
//! and every pipeline that talks to it refers to them, so most of these are
//! about the *seam* — a component that names a connection that isn't there, a
//! connection someone tries to delete out from under a pipeline, a save that
//! writes one file and not the other.
//!
//! Nothing here contacts a broker: the nats and kafka components connect on
//! first read, so building them exercises the lookup without a server.

use std::path::Path;
use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use http_body_util::BodyExt;
use kayak::api_router;
use kayak::state::{AppState, PipelineError};
use serde_json::{Value, json};
use tower::ServiceExt;

fn nats_connection() -> Value {
    json!({"type": "nats", "urls": "nats://localhost:4222"})
}

fn kafka_connection() -> Value {
    json!({"type": "kafka", "brokers": "localhost:9092"})
}

/// A pipeline reading a nats subject over the named connection.
fn nats_config(id: &str, connection: &str) -> Value {
    json!({
        "id": id,
        "inputs": [{"type": "nats", "connection": connection, "subject": "test.subject"}],
        "transforms": [],
        "outputs": [{"type": "stdout"}]
    })
}

fn app_from(path: &Path) -> anyhow::Result<Router> {
    Ok(api_router(Arc::new(AppState::from_config(path)?)))
}

async fn send(app: &Router, req: Request<Body>) -> anyhow::Result<(StatusCode, Value)> {
    let res = app.clone().oneshot(req).await?;
    let status = res.status();
    let bytes = res.into_body().collect().await?.to_bytes();
    let body = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned()))
    };
    Ok((status, body))
}

async fn post(app: &Router, uri: &str, body: &Value) -> anyhow::Result<(StatusCode, Value)> {
    let req = Request::builder()
        .method("POST")
        .uri(uri)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(body)?))?;
    send(app, req).await
}

async fn get(app: &Router, uri: &str) -> anyhow::Result<(StatusCode, Value)> {
    send(app, Request::builder().uri(uri).body(Body::empty())?).await
}

async fn delete(app: &Router, uri: &str) -> anyhow::Result<(StatusCode, Value)> {
    send(
        app,
        Request::builder()
            .method("DELETE")
            .uri(uri)
            .body(Body::empty())?,
    )
    .await
}

/// Two pipelines on one cluster, which is the case the whole feature exists
/// for: the brokers are written once and named twice.
#[tokio::test]
async fn several_pipelines_share_one_connection() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let config = dir.path().join("config.json");
    std::fs::write(
        &config,
        serde_json::to_string(&json!([
            {
                "id": "orders",
                "inputs": [{"type": "kafka", "connection": "cluster", "topic": "orders", "group": "kayak"}],
                "transforms": [],
                "outputs": []
            },
            {
                "id": "payments",
                "inputs": [{"type": "kafka", "connection": "cluster", "topic": "payments", "group": "kayak"}],
                "transforms": [],
                "outputs": []
            }
        ]))?,
    )?;
    std::fs::write(
        dir.path().join("config.connections.json"),
        serde_json::to_string(&json!({"cluster": kafka_connection()}))?,
    )?;

    let app = app_from(&config)?;
    let (status, pipelines) = get(&app, "/api/pipelines").await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(pipelines.as_array().map(Vec::len), Some(2));
    Ok(())
}

/// The file's name is derived from the config's, so the pair travels together
/// and a server started with `--config` alone still finds it.
#[tokio::test]
async fn the_connections_file_is_found_beside_the_config() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let config = dir.path().join("pipelines.yaml");
    std::fs::write(
        &config,
        serde_norway::to_string(&json!([nats_config("sensors", "broker")]))?,
    )?;
    // ...in the config's format, because this one is hand-written too
    std::fs::write(
        dir.path().join("pipelines.connections.yaml"),
        serde_norway::to_string(&json!({"broker": nats_connection()}))?,
    )?;

    let app = app_from(&config)?;
    let (_, connections) = get(&app, "/api/connections").await?;
    assert_eq!(connections["broker"]["type"], json!("nats"));
    Ok(())
}

/// A component naming a connection that isn't configured cannot be built, and
/// the error has to say so where someone will read it — with the names that
/// *are* configured, since the usual cause is a typo.
#[tokio::test]
async fn a_pipeline_naming_an_unknown_connection_is_rejected() -> anyhow::Result<()> {
    let state = AppState::new();
    state.create_connection(
        "broker".to_string(),
        serde_json::from_value(nats_connection())?,
    )?;

    let config = serde_json::from_value(nats_config("typo", "brokre"))?;
    let err = match state.create_pipeline(config) {
        Err(PipelineError::InvalidConfig(e)) => format!("{e:#}"),
        Err(e) => panic!("expected InvalidConfig, got: {e}"),
        Ok(_) => panic!("a pipeline built on a connection that does not exist"),
    };
    assert!(err.contains("brokre"), "{err}");
    assert!(
        err.contains("broker"),
        "the known names should be listed: {err}"
    );
    assert!(state.get_pipeline_ids().is_empty());
    Ok(())
}

/// The kind is checked, not just the name: a nats url is no use to a kafka
/// consumer, and passing one through as a broker list would fail much later
/// with a much worse message.
#[tokio::test]
async fn a_connection_of_the_wrong_kind_is_rejected() -> anyhow::Result<()> {
    let state = AppState::new();
    state.create_connection(
        "broker".to_string(),
        serde_json::from_value(nats_connection())?,
    )?;

    let config = serde_json::from_value(json!({
        "id": "mixed-up",
        "inputs": [{"type": "kafka", "connection": "broker", "topic": "t", "group": "g"}],
        "transforms": [],
        "outputs": []
    }))?;
    let err = match state.create_pipeline(config) {
        Err(PipelineError::InvalidConfig(e)) => format!("{e:#}"),
        Err(e) => panic!("expected InvalidConfig, got: {e}"),
        Ok(_) => panic!("a nats connection was accepted by a kafka input"),
    };
    assert!(
        err.contains("is a nats connection"),
        "the error should name both kinds: {err}"
    );
    Ok(())
}

/// Adding one changes what the *next* pipeline can name and nothing else — it
/// is not a write, and it does not touch anything already running.
#[tokio::test]
async fn a_connection_can_be_added_and_used_over_http() -> anyhow::Result<()> {
    let app = api_router(Arc::new(AppState::new()));

    let (status, _) = post(
        &app,
        "/api/connections",
        &json!({"id": "broker", "type": "nats", "urls": "nats://localhost:4222"}),
    )
    .await?;
    assert_eq!(status, StatusCode::CREATED);

    let (status, _) = post(&app, "/api/pipelines", &nats_config("sensors", "broker")).await?;
    assert_eq!(status, StatusCode::CREATED);

    let (_, connections) = get(&app, "/api/connections").await?;
    assert_eq!(
        connections["broker"]["urls"],
        json!("nats://localhost:4222")
    );
    Ok(())
}

/// The name is the identity — two connections called the same thing would make
/// every reference to it ambiguous.
#[tokio::test]
async fn a_duplicate_connection_name_is_refused() -> anyhow::Result<()> {
    let app = api_router(Arc::new(AppState::new()));
    let body = json!({"id": "broker", "type": "nats", "urls": "nats://localhost:4222"});
    assert_eq!(
        post(&app, "/api/connections", &body).await?.0,
        StatusCode::CREATED
    );
    assert_eq!(
        post(&app, "/api/connections", &body).await?.0,
        StatusCode::CONFLICT
    );
    Ok(())
}

/// Deleting one out from under a running pipeline is refused, and the refusal
/// names the pipelines — which is exactly the list of things to deal with
/// first.
#[tokio::test]
async fn a_connection_in_use_cannot_be_deleted() -> anyhow::Result<()> {
    let app = api_router(Arc::new(AppState::new()));
    post(
        &app,
        "/api/connections",
        &json!({"id": "broker", "type": "nats", "urls": "nats://localhost:4222"}),
    )
    .await?;
    post(&app, "/api/pipelines", &nats_config("sensors", "broker")).await?;

    let (status, body) = delete(&app, "/api/connections/broker").await?;
    assert_eq!(status, StatusCode::CONFLICT);
    let message = body["error"].as_str().unwrap_or_default().to_string();
    assert!(message.contains("sensors"), "{message}");

    // ...and once the pipeline is gone, so can the connection be
    assert_eq!(
        delete(&app, "/api/pipelines/sensors").await?.0,
        StatusCode::NO_CONTENT
    );
    assert_eq!(
        delete(&app, "/api/connections/broker").await?.0,
        StatusCode::NO_CONTENT
    );
    let (_, connections) = get(&app, "/api/connections").await?;
    assert_eq!(connections, json!({}));
    Ok(())
}

#[tokio::test]
async fn deleting_an_unknown_connection_returns_404() -> anyhow::Result<()> {
    let app = api_router(Arc::new(AppState::new()));
    assert_eq!(
        delete(&app, "/api/connections/nowhere").await?.0,
        StatusCode::NOT_FOUND
    );
    Ok(())
}

/// A connection added in the UI is an unsaved change like any other: it is
/// something the running server can build against, and a restart without a save
/// would lose it. The save writes *both* files, because a config saved without
/// the connections it names would not start.
#[tokio::test]
async fn adding_a_connection_is_an_unsaved_change_until_the_save_writes_it() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let config = dir.path().join("config.json");
    std::fs::write(&config, "[]")?;
    let app = app_from(&config)?;

    let (_, settings) = get(&app, "/api/settings").await?;
    assert_eq!(settings["unsaved_changes"], json!(false));

    post(
        &app,
        "/api/connections",
        &json!({"id": "broker", "type": "nats", "urls": "nats://app:${NATS_PASSWORD}@b:4222"}),
    )
    .await?;
    let (_, settings) = get(&app, "/api/settings").await?;
    assert_eq!(
        settings["unsaved_changes"],
        json!(true),
        "a new connection is a change to what the server can build"
    );

    let connections_file = dir.path().join("config.connections.json");
    assert!(
        !connections_file.exists(),
        "adding a connection wrote to disk; only a save may do that"
    );

    let (status, _) = post(&app, "/api/config/save", &json!({"name": "config.json"})).await?;
    assert_eq!(status, StatusCode::OK);

    let written = std::fs::read_to_string(&connections_file)?;
    assert!(
        written.contains("${NATS_PASSWORD}"),
        "the file should hold the reference, never a value: {written}"
    );
    let (_, settings) = get(&app, "/api/settings").await?;
    assert_eq!(settings["unsaved_changes"], json!(false));
    Ok(())
}

/// A server with no config file at all acquires both files from one save — the
/// connections land beside the config that has just come into existence.
#[tokio::test]
async fn a_save_on_a_bare_server_creates_the_connections_file_too() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let app = api_router(Arc::new(AppState::new_in(dir.path().to_path_buf())));

    post(
        &app,
        "/api/connections",
        &json!({"id": "broker", "type": "nats", "urls": "nats://localhost:4222"}),
    )
    .await?;
    post(&app, "/api/pipelines", &nats_config("sensors", "broker")).await?;

    let (status, _) = post(&app, "/api/config/save", &json!({"name": "config.json"})).await?;
    assert_eq!(status, StatusCode::OK);

    assert!(dir.path().join("config.json").exists());
    let connections = std::fs::read_to_string(dir.path().join("config.connections.json"))?;
    assert!(connections.contains("broker"), "{connections}");
    Ok(())
}

/// Revert is "go back to what is on disk", and that is both files. The
/// connections have to be reloaded *before* the pipelines, since the pipelines
/// about to be rebuilt name them.
#[tokio::test]
async fn a_revert_reloads_the_connections_as_well_as_the_pipelines() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let config = dir.path().join("config.json");
    std::fs::write(
        &config,
        serde_json::to_string(&json!([nats_config("sensors", "broker")]))?,
    )?;
    std::fs::write(
        dir.path().join("config.connections.json"),
        serde_json::to_string(&json!({"broker": nats_connection()}))?,
    )?;
    let app = app_from(&config)?;

    // a session's work: one more connection, and a pipeline using it
    post(
        &app,
        "/api/connections",
        &json!({"id": "other", "type": "nats", "urls": "nats://elsewhere:4222"}),
    )
    .await?;
    post(&app, "/api/pipelines", &nats_config("extra", "other")).await?;

    let (status, _) = post(&app, "/api/config/revert", &json!({})).await?;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (_, connections) = get(&app, "/api/connections").await?;
    assert_eq!(
        connections,
        json!({"broker": nats_connection()}),
        "the unsaved connection should be gone with the unsaved pipeline"
    );
    let (_, pipelines) = get(&app, "/api/pipelines").await?;
    assert_eq!(pipelines.as_array().map(Vec::len), Some(1));
    Ok(())
}

/// The pipelines have to be rebuilt *after* the connections they name, or a
/// revert would fail to start every pipeline in the file.
#[tokio::test]
async fn a_revert_rebuilds_pipelines_on_the_reloaded_connections() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let config = dir.path().join("config.json");
    std::fs::write(
        &config,
        serde_json::to_string(&json!([nats_config("sensors", "broker")]))?,
    )?;
    std::fs::write(
        dir.path().join("config.connections.json"),
        serde_json::to_string(&json!({"broker": nats_connection()}))?,
    )?;
    let app = app_from(&config)?;

    assert_eq!(
        post(&app, "/api/config/revert", &json!({})).await?.0,
        StatusCode::NO_CONTENT
    );
    let (_, pipelines) = get(&app, "/api/pipelines").await?;
    assert_eq!(
        pipelines.as_array().map(Vec::len),
        Some(1),
        "the pipeline should have been rebuilt on the reloaded connection"
    );
    Ok(())
}

/// One connections file, two configs — the reason the flag exists at all.
#[tokio::test]
async fn a_named_connections_file_is_used_instead_of_the_derived_one() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let shared = dir.path().join("shared.connections.json");
    std::fs::write(
        &shared,
        serde_json::to_string(&json!({"broker": nats_connection()}))?,
    )?;

    for name in ["one.json", "two.json"] {
        let config = dir.path().join(name);
        std::fs::write(
            &config,
            serde_json::to_string(&json!([nats_config("sensors", "broker")]))?,
        )?;
        let state = AppState::from_config_with(
            &config,
            Arc::new(kayak::secrets::EnvStore),
            Some(&shared),
            None,
        )?;
        assert_eq!(state.get_pipeline_ids(), ["sensors"], "{name}");
        assert!(
            !dir.path()
                .join(name.replace(".json", ".connections.json"))
                .exists(),
            "{name}: the derived file should not have been created"
        );
    }
    Ok(())
}

/// A file the operator named has to exist. Starting with no connections would
/// fail later — as every pipeline in the config refuses to build — and much
/// further from the cause.
#[tokio::test]
async fn a_named_connections_file_that_is_missing_fails_to_start() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let config = dir.path().join("config.json");
    std::fs::write(&config, "[]")?;
    let missing = dir.path().join("nowhere.json");

    let result = AppState::from_config_with(
        &config,
        Arc::new(kayak::secrets::EnvStore),
        Some(&missing),
        None,
    );
    assert!(result.is_err(), "a missing --connections file was accepted");
    Ok(())
}

/// A config that names no connections needs no connections file, which is the
/// state of every graph built out of dummies and pipelines.
#[tokio::test]
async fn a_config_with_no_connections_needs_no_file() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let config = dir.path().join("config.json");
    std::fs::write(
        &config,
        serde_json::to_string(&json!([{
            "id": "heartbeat",
            "inputs": [{"type": "dummy", "duration": 3600}],
            "transforms": [],
            "outputs": [{"type": "stdout"}]
        }]))?,
    )?;

    let app = app_from(&config)?;
    let (_, connections) = get(&app, "/api/connections").await?;
    assert_eq!(connections, json!({}));

    // ...and a save doesn't invent one
    post(&app, "/api/config/save", &json!({"name": "config.json"})).await?;
    assert!(
        !dir.path().join("config.connections.json").exists(),
        "an empty connections file was written where none is needed"
    );
    Ok(())
}
