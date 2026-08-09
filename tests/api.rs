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
use kayak::api_router;
use kayak::state::AppState;
use serde_json::{Value, json};
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
        .uri("/api/pipelines")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(config)?))?;
    send(app, req).await
}

fn get(uri: &str) -> Request<Body> {
    // a builder that can't fail on a static uri; unwrapping is not allowed here
    // and there is nothing to recover from either way
    Request::builder()
        .uri(uri)
        .body(Body::empty())
        .unwrap_or_else(|_| Request::new(Body::empty()))
}

fn post(uri: &str, body: &Value) -> Request<Body> {
    Request::builder()
        .method("POST")
        .uri(uri)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(body).unwrap_or_default()))
        .unwrap_or_else(|_| Request::new(Body::empty()))
}

async fn get_pipelines(app: &Router) -> anyhow::Result<(StatusCode, Value)> {
    let req = Request::builder()
        .uri("/api/pipelines")
        .body(Body::empty())?;
    send(app, req).await
}

async fn delete_pipeline(app: &Router, id: &str) -> anyhow::Result<(StatusCode, Value)> {
    let req = Request::builder()
        .method("DELETE")
        .uri(format!("/api/pipelines/{id}"))
        .body(Body::empty())?;
    send(app, req).await
}

#[tokio::test]
async fn creating_a_stream_returns_201_and_the_created_pipeline() -> anyhow::Result<()> {
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

    let (status, body) = get_pipelines(&app).await?;
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
    let (status, body) = get_pipelines(&app()).await?;
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
        "inputs": [{ "type": "pipeline", "upstream": "does-not-exist" }],
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
/// pins that a bad body can't reach `create_pipeline` at all.
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
    let (_, body) = get_pipelines(&app).await?;
    assert_eq!(body, json!([]));
    Ok(())
}

#[tokio::test]
async fn deleting_a_stream_returns_204_and_removes_it() -> anyhow::Result<()> {
    let app = app();
    post_stream(&app, &idle_config("p1")).await?;

    let (status, _) = delete_pipeline(&app, "p1").await?;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let (_, body) = get_pipelines(&app).await?;
    assert_eq!(body, json!([]));
    Ok(())
}

#[tokio::test]
async fn deleting_an_unknown_stream_returns_404() -> anyhow::Result<()> {
    let (status, body) = delete_pipeline(&app(), "nope").await?;
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
    delete_pipeline(&app, "p1").await?;

    let (status, _) = post_stream(&app, &idle_config("p1")).await?;
    assert_eq!(status, StatusCode::CREATED);
    Ok(())
}

/// The component reference is generated by reflecting over the config schemas,
/// so it breaks the moment a component's config stops deriving `JsonSchema`.
/// What each component documents is covered by unit tests in `kayak-core`;
/// this pins the HTTP contract.
#[tokio::test]
async fn the_docs_endpoint_serves_every_component_kind() -> anyhow::Result<()> {
    let req = Request::builder().uri("/api/docs").body(Body::empty())?;
    let res = app().oneshot(req).await?;
    assert_eq!(res.status(), StatusCode::OK);

    let bytes = res.into_body().collect().await?.to_bytes();
    let docs: Vec<Value> = serde_json::from_slice(&bytes)?;
    let kinds: Vec<&str> = docs.iter().filter_map(|d| d["kind"].as_str()).collect();
    for component in ["dummy", "nats", "pipeline", "filter", "reducer", "stdout"] {
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
            ["input", "transform", "output", "connection"]
                .contains(&doc["family"].as_str().unwrap_or_default()),
            "'{kind}' has no family"
        );
        assert!(
            doc["description"].is_string(),
            "'{kind}' has no description"
        );
    }
    Ok(())
}

/// Every documented endpoint is actually routed, at the method it is documented
/// under.
///
/// The router is built by folding over the same table, so a documented route
/// that isn't served would take a bug in `endpoints::handler_for` — but the
/// *method* is worth pinning: a 405 here means the table and the handler
/// disagree about what kind of request this is, which is the one mismatch
/// folding can't rule out.
///
/// A 404 is the awkward case, because two different things produce one: the
/// router, for a path it doesn't serve, and `delete_pipeline`, for an id that
/// isn't running. They are told apart by the body — the router's is empty,
/// `AppError`'s is the JSON error object. Anything else counts as routed; an
/// extractor rejecting `{}` with a 422 is still an endpoint answering.
#[tokio::test]
async fn every_documented_endpoint_is_routed_at_its_documented_method() -> anyhow::Result<()> {
    let app = app();
    for endpoint in kayak_core::api_docs::endpoints() {
        // a placeholder needs *some* value; a 404 from the handler is still a
        // routed endpoint, and this test only rejects a 404 from the router
        let path = endpoint.path.replace("{pipeline_id}", "nope").replace(
            "{connection_id}",
            "nope",
        );
        let method = match endpoint.method {
            kayak_core::api_docs::Method::Get => "GET",
            kayak_core::api_docs::Method::Post => "POST",
            kayak_core::api_docs::Method::Put => "PUT",
            kayak_core::api_docs::Method::Delete => "DELETE",
        };
        let req = Request::builder()
            .method(method)
            .uri(&path)
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::from("{}"))?;
        let res = app.clone().oneshot(req).await?;
        let status = res.status();

        assert_ne!(
            status,
            StatusCode::METHOD_NOT_ALLOWED,
            "{method} {path} is documented but routed under a different method"
        );
        // the body is only read on a 404, and deliberately so: `/events` is an
        // event stream that stays open, so collecting every response here would
        // hang on it forever
        if status == StatusCode::NOT_FOUND {
            let bytes = res.into_body().collect().await?.to_bytes();
            let body: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
            assert!(
                body["error"].is_string(),
                "{method} {path} is documented but not routed"
            );
        }
    }
    Ok(())
}

/// The spec is served, is 3.1, and describes the server serving it.
#[tokio::test]
async fn the_openapi_endpoint_serves_a_document_covering_every_endpoint() -> anyhow::Result<()> {
    let req = Request::builder()
        .uri("/api/openapi.json")
        .body(Body::empty())?;
    let res = app().oneshot(req).await?;
    assert_eq!(res.status(), StatusCode::OK);

    let bytes = res.into_body().collect().await?.to_bytes();
    let document: Value = serde_json::from_slice(&bytes)?;
    assert_eq!(document["openapi"], "3.1.0");

    for endpoint in kayak_core::api_docs::endpoints() {
        assert_eq!(
            document["paths"][endpoint.path][endpoint.method.key()]["operationId"],
            endpoint.operation_id(),
            "{} {} is missing from the served document",
            endpoint.method.label(),
            endpoint.path
        );
    }
    Ok(())
}

/// The reference page is served as HTML and points at this server's spec.
#[tokio::test]
async fn the_reference_page_is_served_as_html() -> anyhow::Result<()> {
    let req = Request::builder()
        .uri("/api/reference")
        .body(Body::empty())?;
    let res = app().oneshot(req).await?;
    assert_eq!(res.status(), StatusCode::OK);
    assert!(
        res.headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .unwrap_or_default()
            .starts_with("text/html"),
        "the reference page should be served as html"
    );

    let bytes = res.into_body().collect().await?.to_bytes();
    let page = String::from_utf8(bytes.to_vec())?;
    assert!(page.contains("/api/openapi.json"));
    Ok(())
}

/// The documented error body has to be the one `AppError` actually produces.
///
/// `ApiError` is a Rust type in `kayak_core::api_docs` precisely so the spec's
/// error schema is generated rather than written; this is what stops the two
/// from drifting, since nothing else connects them.
#[tokio::test]
async fn an_error_body_matches_the_documented_shape() -> anyhow::Result<()> {
    let (status, body) = delete_pipeline(&app(), "nope").await?;
    assert_eq!(status, StatusCode::NOT_FOUND);

    let parsed: kayak_core::api_docs::ApiError = serde_json::from_value(body)?;
    assert!(
        parsed.error.contains("nope"),
        "the error should name the id: {}",
        parsed.error
    );
    Ok(())
}

/// A pipeline whose only input is its own endpoint, and whose output goes
/// nowhere external.
fn posted_config(id: &str) -> Value {
    json!({
        "id": id,
        "inputs": [{ "type": "http" }],
        "transforms": [],
        "outputs": [{ "type": "stdout" }]
    })
}

async fn post_messages(app: &Router, id: &str, body: &Value) -> anyhow::Result<(StatusCode, Value)> {
    let req = Request::builder()
        .method("POST")
        .uri(format!("/api/pipelines/{id}/messages"))
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(body)?))?;
    send(app, req).await
}

#[tokio::test]
async fn a_single_posted_message_is_accepted() -> anyhow::Result<()> {
    let app = app();
    post_stream(&app, &posted_config("ingest")).await?;

    let (status, body) = post_messages(&app, "ingest", &json!({"n": 1})).await?;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(body, json!({"accepted": 1}));
    Ok(())
}

/// An array is *one batch*, not a message that happens to be an array — which
/// is what the untagged `IngestRequest` arm order is there to guarantee.
#[tokio::test]
async fn an_array_is_accepted_as_that_many_messages() -> anyhow::Result<()> {
    let app = app();
    post_stream(&app, &posted_config("ingest")).await?;

    let (status, body) = post_messages(&app, "ingest", &json!([{"n": 1}, {"n": 2}, {"n": 3}])).await?;
    assert_eq!(status, StatusCode::ACCEPTED);
    assert_eq!(body, json!({"accepted": 3}));
    Ok(())
}

#[tokio::test]
async fn posting_to_an_unknown_pipeline_is_a_404() -> anyhow::Result<()> {
    let (status, body) = post_messages(&app(), "nobody", &json!({"n": 1})).await?;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(
        body["error"].as_str().is_some_and(|e| e.contains("nobody")),
        "the error should name the id: {body}"
    );
    Ok(())
}

/// A running pipeline with no `http` input has no endpoint either, and says so
/// rather than pretending the pipeline isn't there.
#[tokio::test]
async fn posting_to_a_pipeline_without_an_http_input_is_a_404() -> anyhow::Result<()> {
    let app = app();
    post_stream(&app, &idle_config("p1")).await?;

    let (status, body) = post_messages(&app, "p1", &json!({"n": 1})).await?;
    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(
        body["error"]
            .as_str()
            .is_some_and(|e| e.contains("no http input")),
        "the error should say what is missing: {body}"
    );
    Ok(())
}

/// Deleting the pipeline takes its endpoint with it, and immediately — the run
/// loop's task may not have finished dying yet.
#[tokio::test]
async fn a_deleted_pipeline_stops_accepting_posts() -> anyhow::Result<()> {
    let app = app();
    post_stream(&app, &posted_config("ingest")).await?;
    delete_pipeline(&app, "ingest").await?;

    let (status, _) = post_messages(&app, "ingest", &json!({"n": 1})).await?;
    assert_eq!(status, StatusCode::NOT_FOUND);
    Ok(())
}

/// Two http inputs would share one endpoint, so the pipeline doesn't build.
#[tokio::test]
async fn a_pipeline_with_two_http_inputs_is_rejected() -> anyhow::Result<()> {
    let app = app();
    let mut config = posted_config("ingest");
    config["inputs"] = json!([{ "type": "http" }, { "type": "http" }]);

    let (status, body) = post_stream(&app, &config).await?;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert!(
        body["error"]
            .as_str()
            .is_some_and(|e| e.contains("already has an http input")),
        "the error should say why: {body}"
    );
    // and the failed build left nothing claiming the endpoint: the same id
    // builds cleanly with one input
    let (status, _) = post_stream(&app, &posted_config("ingest")).await?;
    assert_eq!(status, StatusCode::CREATED);
    Ok(())
}

/// A pipeline that is behind is a 503 rather than a 404 or a silent drop: the
/// data is still wanted, just not now. The queue level is unit-tested in
/// `inputs::http`; what's pinned here is the status the mapping gives it.
#[test]
fn a_backlogged_pipeline_maps_onto_a_503() {
    use axum::response::IntoResponse;
    use kayak::handlers::error::AppError;
    use kayak::state::PipelineError;

    let err: AppError = anyhow::Error::from(PipelineError::Backpressure("ingest".to_string())).into();
    assert_eq!(
        err.into_response().status(),
        StatusCode::SERVICE_UNAVAILABLE
    );
}

/// A server with no buckets declared — which is every config that hasn't asked
/// for them — reports none rather than failing.
#[tokio::test]
async fn a_server_with_no_state_buckets_lists_none() -> anyhow::Result<()> {
    let app = app();
    let (status, body) = send(&app, get("/api/state")).await?;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body, json!([]));
    Ok(())
}

/// Asking for a bucket nobody declared is a 404, and a JSON one — the router's
/// own 404 has an empty body, and telling them apart is what the route-coverage
/// test leans on.
#[tokio::test]
async fn an_undeclared_state_bucket_is_a_json_404() -> anyhow::Result<()> {
    let app = app();
    let (status, body) = send(&app, get("/api/state/nope")).await?;

    assert_eq!(status, StatusCode::NOT_FOUND);
    assert!(
        body.get("error").is_some(),
        "expected an ApiError body, got {body}"
    );
    Ok(())
}

/// The buckets a config declares are reported with the bounds they were
/// declared with, and fill up as pipelines remember things.
#[tokio::test]
async fn a_declared_bucket_is_listed_and_fills_as_it_is_written_to() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("config.json");
    std::fs::write(
        &path,
        serde_json::to_string(&json!({
            "state": { "machines": { "max_keys": 50, "idle_timeout_secs": 900 } },
            "pipelines": [{
                "id": "feeder",
                "state": { "bucket": "machines", "key": "machine_id" },
                "inputs": [{ "type": "http" }],
                "transforms": [{
                    "type": "remember",
                    "remember": [{ "field": "unit", "as": "unit_id" }]
                }],
                "outputs": []
            }]
        }))?,
    )?;
    let app = api_router(Arc::new(AppState::from_config(&path)?));

    let (status, body) = send(&app, get("/api/state")).await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body[0]["name"], json!("machines"));
    assert_eq!(body[0]["keys"], json!(0), "nothing has been remembered yet");
    assert_eq!(body[0]["max_keys"], json!(50));
    assert_eq!(body[0]["idle_timeout_secs"], json!(900));

    // post something for the pipeline to remember
    let (status, _) = send(
        &app,
        post(
            "/api/pipelines/feeder/messages",
            &json!([{ "machine_id": "m1", "unit": "u-7" }]),
        ),
    )
    .await?;
    assert_eq!(status, StatusCode::ACCEPTED);

    // the post is accepted before the run loop has processed it, so wait for
    // the bucket rather than assuming
    let mut contents = Value::Null;
    for _ in 0..50 {
        tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        let (status, body) = send(&app, get("/api/state/machines")).await?;
        assert_eq!(status, StatusCode::OK);
        if body["entries"].as_array().is_some_and(|e| !e.is_empty()) {
            contents = body;
            break;
        }
    }

    assert_eq!(contents["name"], json!("machines"));
    assert_eq!(contents["keys"], json!(1));
    assert_eq!(contents["truncated"], json!(false));
    assert_eq!(contents["entries"][0]["key"], json!("m1"));
    assert_eq!(contents["entries"][0]["values"]["unit_id"], json!("u-7"));
    assert!(contents["entries"][0]["updated_at"].is_string());
    Ok(())
}
