//! HTTP-surface tests.
//!
//! They call the real router through `tower::ServiceExt::oneshot`, so there is
//! no socket, no port to collide on and no server to start or stop. Every
//! pipeline used here is a `dummy` input with a long interval and a `stdout`
//! output, so nothing external is contacted.

use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use streamer::api_router;
use streamer::state::AppState;
use tower::ServiceExt;

fn app() -> Router {
    api_router(Arc::new(AppState::new()))
}

/// A pipeline that will sit idle: the dummy input only ticks once an hour.
fn idle_config(id: &str) -> Value {
    json!({
        "id": id,
        "inputs": [{ "type": "dummy", "duration": 3600 }],
        "transforms": [],
        "outputs": [{ "type": "stdout" }]
    })
}

async fn send(app: &Router, req: Request<Body>) -> anyhow::Result<(StatusCode, Value)> {
    let res = app.clone().oneshot(req).await?;
    let status = res.status();
    let bytes = res.into_body().collect().await?.to_bytes();
    // 204s have no body, and axum's own extractor rejections are plain text —
    // normalise both so callers can just look at the value
    let body = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned()))
    };
    Ok((status, body))
}

async fn post_stream(app: &Router, config: &Value) -> anyhow::Result<(StatusCode, Value)> {
    let req = Request::builder()
        .method("POST")
        .uri("/api/streams")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(config)?))?;
    send(app, req).await
}

async fn get_streams(app: &Router) -> anyhow::Result<(StatusCode, Value)> {
    let req = Request::builder()
        .uri("/api/streams")
        .body(Body::empty())?;
    send(app, req).await
}

async fn delete_stream(app: &Router, id: &str) -> anyhow::Result<(StatusCode, Value)> {
    let req = Request::builder()
        .method("DELETE")
        .uri(format!("/api/streams/{id}"))
        .body(Body::empty())?;
    send(app, req).await
}

#[tokio::test]
async fn creating_a_stream_returns_201_and_the_created_streamer() -> anyhow::Result<()> {
    let app = app();
    let (status, body) = post_stream(&app, &idle_config("p1")).await?;

    assert_eq!(status, StatusCode::CREATED);
    assert_eq!(body["id"], json!("p1"));
    assert_eq!(body["config"]["inputs"][0]["type"], json!("dummy"));
    Ok(())
}

/// Omitting the id is allowed — the server generates a petname.
#[tokio::test]
async fn a_stream_without_an_id_gets_a_generated_one() -> anyhow::Result<()> {
    let app = app();
    let mut config = idle_config("ignored");
    config["id"] = Value::Null;

    let (status, body) = post_stream(&app, &config).await?;
    assert_eq!(status, StatusCode::CREATED);
    let Some(id) = body["id"].as_str() else {
        panic!("no id in response: {body}");
    };
    assert!(!id.is_empty(), "generated id should not be empty");
    Ok(())
}

#[tokio::test]
async fn a_created_stream_shows_up_in_the_listing() -> anyhow::Result<()> {
    let app = app();
    post_stream(&app, &idle_config("p1")).await?;
    post_stream(&app, &idle_config("p2")).await?;

    let (status, body) = get_streams(&app).await?;
    assert_eq!(status, StatusCode::OK);
    let Some(items) = body.as_array() else {
        panic!("expected an array, got {body}");
    };
    let mut ids: Vec<&str> = items.iter().filter_map(|s| s["id"].as_str()).collect();
    ids.sort_unstable();
    assert_eq!(ids, vec!["p1", "p2"]);
    Ok(())
}

#[tokio::test]
async fn an_empty_server_lists_no_streams() -> anyhow::Result<()> {
    let (status, body) = get_streams(&app()).await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!([]));
    Ok(())
}

/// Ids are the graph's primary key, so a collision has to be rejected rather
/// than silently replacing a running pipeline.
#[tokio::test]
async fn a_duplicate_id_is_rejected_with_409() -> anyhow::Result<()> {
    let app = app();
    post_stream(&app, &idle_config("p1")).await?;
    let (status, body) = post_stream(&app, &idle_config("p1")).await?;

    assert_eq!(status, StatusCode::CONFLICT);
    assert!(
        body["error"].as_str().unwrap_or_default().contains("p1"),
        "error should name the conflicting id: {body}"
    );
    Ok(())
}

/// A config that parses but can't be built — here, an unknown upstream — is the
/// caller's mistake, so 422 rather than 500.
#[tokio::test]
async fn an_unbuildable_config_is_rejected_with_422() -> anyhow::Result<()> {
    let config = json!({
        "id": "downstream",
        "inputs": [{ "type": "streamer", "upstream": "does-not-exist" }],
        "transforms": [],
        "outputs": [{ "type": "stdout" }]
    });

    let (status, body) = post_stream(&app(), &config).await?;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert!(
        body["error"]
            .as_str()
            .unwrap_or_default()
            .contains("does-not-exist"),
        "error should name the missing upstream: {body}"
    );
    Ok(())
}

/// Malformed JSON is rejected by axum's extractor before it reaches us; this
/// pins that a bad body can't reach `create_streamer` at all.
#[tokio::test]
async fn a_body_that_is_not_a_valid_config_is_rejected() -> anyhow::Result<()> {
    let app = app();
    let cases = [
        json!({ "nonsense": true }),
        json!({ "id": "x", "inputs": [{ "type": "no-such-input" }], "transforms": [], "outputs": [{ "type": "stdout" }] }),
        json!({ "id": "x", "inputs": [{ "type": "dummy", "duration": 1 }], "transforms": [] }),
    ];
    for case in cases {
        let (status, _) = post_stream(&app, &case).await?;
        assert!(
            status.is_client_error(),
            "expected a 4xx for {case}, got {status}"
        );
    }
    // nothing was created
    let (_, body) = get_streams(&app).await?;
    assert_eq!(body, json!([]));
    Ok(())
}

#[tokio::test]
async fn deleting_a_stream_returns_204_and_removes_it() -> anyhow::Result<()> {
    let app = app();
    post_stream(&app, &idle_config("p1")).await?;

    let (status, _) = delete_stream(&app, "p1").await?;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (_, body) = get_streams(&app).await?;
    assert_eq!(body, json!([]));
    Ok(())
}

#[tokio::test]
async fn deleting_an_unknown_stream_returns_404() -> anyhow::Result<()> {
    let (status, body) = delete_stream(&app(), "nope").await?;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(
        body["error"].as_str().unwrap_or_default().contains("nope"),
        "error should name the id: {body}"
    );
    Ok(())
}

/// Deleting frees the id again, which is what makes "edit a pipeline" work as
/// delete-then-create in the UI.
#[tokio::test]
async fn an_id_can_be_reused_after_deletion() -> anyhow::Result<()> {
    let app = app();
    post_stream(&app, &idle_config("p1")).await?;
    delete_stream(&app, "p1").await?;

    let (status, _) = post_stream(&app, &idle_config("p1")).await?;
    assert_eq!(status, StatusCode::CREATED);
    Ok(())
}

/// The component reference is generated by reflecting over the config schemas,
/// so it breaks the moment a component's config stops deriving `JsonSchema`.
/// What each component documents is covered by unit tests in `streamer-core`;
/// this pins the HTTP contract.
#[tokio::test]
async fn the_docs_endpoint_serves_every_component_kind() -> anyhow::Result<()> {
    let req = Request::builder().uri("/api/docs").body(Body::empty())?;
    let res = app().oneshot(req).await?;
    assert_eq!(res.status(), StatusCode::OK);

    let bytes = res.into_body().collect().await?.to_bytes();
    let docs: Vec<Value> = serde_json::from_slice(&bytes)?;
    let kinds: Vec<&str> = docs.iter().filter_map(|d| d["kind"].as_str()).collect();
    for component in ["dummy", "nats", "streamer", "filter", "reducer", "stdout"] {
        assert!(
            kinds.contains(&component),
            "/api/docs should document '{component}', got {kinds:?}"
        );
    }

    // every entry is a component someone can actually write in a config: it has
    // a family, and a description that came from a doc comment
    for doc in &docs {
        let kind = doc["kind"].as_str().unwrap_or("<none>");
        assert!(
            ["input", "transform", "output"].contains(&doc["family"].as_str().unwrap_or_default()),
            "'{kind}' has no family"
        );
        assert!(doc["description"].is_string(), "'{kind}' has no description");
    }
    Ok(())
}
