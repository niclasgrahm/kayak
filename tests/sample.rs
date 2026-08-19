//! `POST /api/inputs/sample` — fetching a few real messages from an input
//! that isn't a pipeline yet.
//!
//! Offline, like `tests/api.rs`: everything here is a `dummy` input, an input
//! that is refused before anything is built, or one that names a connection
//! that doesn't exist and so fails at build. Nothing contacts a broker.

use std::sync::Arc;
use std::time::Duration;

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

async fn sample(app: &Router, body: &Value) -> anyhow::Result<(StatusCode, Value)> {
    let req = Request::builder()
        .method("POST")
        .uri("/api/inputs/sample")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(body)?))?;
    let res = app.clone().oneshot(req).await?;
    let status = res.status();
    let bytes = res.into_body().collect().await?.to_bytes();
    let value = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned()));
    Ok((status, value))
}

/// The happy path, and the thing the whole feature is for: real messages, from
/// a real input, with no pipeline created.
#[tokio::test]
async fn a_dummy_input_can_be_sampled_without_creating_a_pipeline() -> anyhow::Result<()> {
    let app = app();
    let (status, body) = sample(
        &app,
        &json!({
            "input": {"type": "dummy", "duration": 1},
            "max_messages": 2,
            "timeout_ms": 4000,
        }),
    )
    .await?;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["outcome"], "sampled");
    let Some(messages) = body["messages"].as_array() else {
        panic!("messages is not a list: {body:?}");
    };
    assert_eq!(messages.len(), 2);
    assert!(
        messages[0].get("value").is_some(),
        "a dummy message carries a value: {:?}",
        messages[0]
    );
    // nothing was created on the way
    let res = app
        .clone()
        .oneshot(Request::builder().uri("/api/pipelines").body(Body::empty())?)
        .await?;
    let bytes = res.into_body().collect().await?.to_bytes();
    let pipelines: Value = serde_json::from_slice(&bytes)?;
    assert_eq!(pipelines.as_array().map(Vec::len), Some(0));
    Ok(())
}

/// The envelope is part of the input, and a sample that dropped it would show
/// a message shaped unlike the one the pipeline will see — which is exactly
/// the mistake a column mapping made from the sample would then inherit.
#[tokio::test]
async fn a_sample_carries_the_envelope_the_pipeline_would_attach() -> anyhow::Result<()> {
    let (status, body) = sample(
        &app(),
        &json!({
            "input": {
                "type": "dummy",
                "duration": 1,
                "envelope": {"type": "merge", "field": "_meta"},
            },
            "max_messages": 1,
        }),
    )
    .await?;

    assert_eq!(status, StatusCode::OK);
    let message = &body["messages"][0];
    assert_eq!(message["_meta"]["input"], "dummy");
    Ok(())
}

/// A buffer's job is to make the pipeline wait, which is the opposite of what
/// a sample is for — so it is ignored, and the response says so rather than
/// leaving the user to wonder why 100 messages arrived as 1.
#[tokio::test]
async fn a_buffer_is_ignored_and_the_response_says_so() -> anyhow::Result<()> {
    let (status, body) = sample(
        &app(),
        &json!({
            "input": {
                "type": "dummy",
                "duration": 1,
                "buffer": {"type": "static", "size": 500},
            },
            "max_messages": 1,
            "timeout_ms": 4000,
        }),
    )
    .await?;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["messages"].as_array().map(Vec::len), Some(1));
    let Some(notes) = body["notes"].as_array() else {
        panic!("no notes at all: {body:?}");
    };
    assert!(
        notes.iter().any(|n| n.as_str().is_some_and(|n| n.contains("buffer"))),
        "notes did not mention the buffer: {notes:?}"
    );
    Ok(())
}

/// Nothing arriving is an answer rather than a failure — a subject nobody
/// publishes to is a real state of the world, and one worth being shown.
#[tokio::test]
async fn a_stream_with_nothing_on_it_samples_empty_rather_than_failing() -> anyhow::Result<()> {
    let (status, body) = sample(
        &app(),
        &json!({
            "input": {"type": "dummy", "duration": 3600},
            "max_messages": 1,
            "timeout_ms": 150,
        }),
    )
    .await?;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["outcome"], "sampled");
    assert_eq!(body["messages"].as_array().map(Vec::len), Some(0));
    Ok(())
}

/// An input that cannot be sampled at all is refused before anything is built,
/// and the refusal says what to do instead.
#[tokio::test]
async fn an_http_input_is_refused_with_the_reason() -> anyhow::Result<()> {
    let (status, body) = sample(&app(), &json!({"input": {"type": "http"}})).await?;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    let error = body["error"].as_str().unwrap_or_default();
    assert!(
        error.contains("posted to"),
        "the refusal doesn't say what to do instead: {error}"
    );
    Ok(())
}

/// A build failure is the *sample's* answer, not the request's: the request
/// was fine and the server carried it out completely — what it found is that
/// this input cannot be built, which is the same thing creating the pipeline
/// would have said and is worth showing in the same panel as the messages.
#[tokio::test]
async fn an_input_that_cannot_be_built_reports_where_it_failed() -> anyhow::Result<()> {
    let (status, body) = sample(
        &app(),
        &json!({
            "input": {
                "type": "nats",
                "connection": "nothing-declares-this",
                "subject": "test.subject",
            },
        }),
    )
    .await?;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["outcome"], "failed");
    assert_eq!(body["stage"], "build");
    assert!(
        body["message"]
            .as_str()
            .is_some_and(|m| m.contains("nothing-declares-this")),
        "the failure doesn't name the connection: {:?}",
        body["message"]
    );
    Ok(())
}

/// The bound is the server's, not the caller's: it holds a request open and a
/// connection to somebody's broker for as long as it runs.
#[tokio::test]
async fn a_sample_is_bounded_however_much_the_request_asks_for() -> anyhow::Result<()> {
    let started = std::time::Instant::now();
    let (status, body) = sample(
        &app(),
        &json!({
            "input": {"type": "dummy", "duration": 3600},
            "max_messages": 1_000_000,
            "timeout_ms": 200,
        }),
    )
    .await?;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["messages"].as_array().map(Vec::len), Some(0));
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "the sample ran past its own timeout"
    );
    Ok(())
}

/// Sampling a `pipeline` input taps a running pipeline's output, which is the
/// case the fan-out pruning in the run loop exists for: the subscription is
/// gone the moment the sample is answered.
#[tokio::test]
async fn a_pipeline_input_samples_what_its_upstream_emits() -> anyhow::Result<()> {
    let app = app();
    let req = Request::builder()
        .method("POST")
        .uri("/api/pipelines")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&json!({
            "id": "ticker",
            "inputs": [{"type": "dummy", "duration": 1}],
            "transforms": [],
            "outputs": [],
        }))?))?;
    let created = app.clone().oneshot(req).await?;
    assert_eq!(created.status(), StatusCode::CREATED);

    let (status, body) = sample(
        &app,
        &json!({
            "input": {"type": "pipeline", "upstream": "ticker"},
            "max_messages": 1,
            "timeout_ms": 4000,
        }),
    )
    .await?;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["outcome"], "sampled");
    assert_eq!(body["messages"].as_array().map(Vec::len), Some(1));
    Ok(())
}
