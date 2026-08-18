//! `POST /api/pipelines/dry-run` — putting messages through a draft's
//! transforms without creating a pipeline.
//!
//! Offline like `tests/sample.rs`: every transform here is one that touches
//! nothing outside the process.

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

async fn dry_run(body: &Value) -> anyhow::Result<(StatusCode, Value)> {
    let req = Request::builder()
        .method("POST")
        .uri("/api/pipelines/dry-run")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(body)?))?;
    let res = app().oneshot(req).await?;
    let status = res.status();
    let bytes = res.into_body().collect().await?.to_bytes();
    let value = serde_json::from_slice(&bytes)
        .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned()));
    Ok((status, value))
}

/// The point of the whole endpoint: each stage says what it handed on, so a
/// chain can be read one transform at a time.
#[tokio::test]
async fn each_stage_reports_what_it_handed_on() -> anyhow::Result<()> {
    let (status, body) = dry_run(&json!({
        "messages": [{"value": 5}, {"value": 20}, {"value": 30}],
        "transforms": [
            {"type": "filter", "Numeric": {"field": "value", "operator": "greater_than", "value": 10.0}},
            {"type": "map", "mappings": [
                {"type": "constant", "value": {"type": "text", "value": "line-3"}, "as": "line"}
            ]},
        ],
    }))
    .await?;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["outcome"], "ran");
    let stages = &body["stages"];
    assert_eq!(stages[0]["kind"], "filter");
    // one batch in, one out — with the message under the threshold gone
    assert_eq!(stages[0]["batches"][0], json!([{"value": 20}, {"value": 30}]));
    assert_eq!(stages[1]["kind"], "map");
    assert_eq!(stages[1]["batches"][0][0]["line"], "line-3");
    Ok(())
}

/// A transform that changes the *number* of batches is exactly what a
/// per-stage report is for.
#[tokio::test]
async fn a_splitter_reports_several_batches() -> anyhow::Result<()> {
    let (status, body) = dry_run(&json!({
        "messages": [{"n": 1}, {"n": 2}, {"n": 3}, {"n": 4}],
        "transforms": [{"type": "splitter", "out_size": 2}],
    }))
    .await?;

    assert_eq!(status, StatusCode::OK);
    let batches = &body["stages"][0]["batches"];
    assert_eq!(batches.as_array().map(Vec::len), Some(2));
    assert_eq!(batches[0], json!([{"n": 1}, {"n": 2}]));
    Ok(())
}

/// Nothing handed on is an answer, and a common one — a filter that matched
/// nothing looks exactly like this and is the reason someone is asking.
#[tokio::test]
async fn a_filter_that_drops_everything_hands_on_no_batches() -> anyhow::Result<()> {
    let (status, body) = dry_run(&json!({
        "messages": [{"value": 1}],
        "transforms": [
            {"type": "filter", "Numeric": {"field": "value", "operator": "greater_than", "value": 10.0}}
        ],
    }))
    .await?;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["stages"][0]["batches"].as_array().map(Vec::len), Some(0));
    Ok(())
}

/// A transform that is still holding its messages hands on nothing, and the
/// chain says so rather than pretending they came through.
///
/// This is what the pipeline would do at that instant too: a buffer waiting
/// for its window releases on a tick, and a dry run has no tick to give it —
/// the drain asks every transform whether it will hand on what it holds *now*,
/// and a window with 30 seconds left says no. Truthfully empty beats a
/// convenient lie here, because the lie would be "this buffer passes
/// everything straight through".
#[tokio::test]
async fn a_transform_still_holding_its_messages_hands_on_nothing() -> anyhow::Result<()> {
    let (status, body) = dry_run(&json!({
        "messages": [{"n": 1}, {"n": 2}],
        "transforms": [
            {"type": "buffer", "seconds": 30, "max_messages": 100},
            {"type": "splitter", "out_size": 1},
        ],
    }))
    .await?;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["outcome"], "ran");
    let held = &body["stages"][0];
    assert_eq!(held["kind"], "buffer");
    assert_eq!(held["batches"].as_array().map(Vec::len), Some(0));
    assert_eq!(held["on_flush"].as_array().map(Vec::len), None, "nothing was released");
    // and the stage behind it saw nothing, which is the honest answer
    assert_eq!(body["stages"][1]["batches"].as_array().map(Vec::len), Some(0));
    Ok(())
}

/// A transform that cannot be built is the same failure creating the pipeline
/// would have given, which is most of the value of building the real one.
#[tokio::test]
async fn a_transform_that_cannot_be_built_says_which_one_and_why() -> anyhow::Result<()> {
    let (status, body) = dry_run(&json!({
        "messages": [{"n": 1}],
        "transforms": [
            {"type": "splitter", "out_size": 2},
            // no aggregations at all: refused at build time, not per message
            {"type": "reducer", "aggregations": [], "group_by": []},
        ],
    }))
    .await?;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["outcome"], "failed");
    assert_eq!(body["phase"], "build");
    assert_eq!(body["at"], 1);
    assert_eq!(body["kind"], "reducer");
    Ok(())
}

/// How far the messages got is half of what says *why* the failing transform
/// failed, so the stages that completed come back with the failure.
#[tokio::test]
async fn a_failure_on_a_message_keeps_the_stages_that_ran_before_it() -> anyhow::Result<()> {
    let (status, body) = dry_run(&json!({
        "messages": [{"value": 20}],
        "transforms": [
            {"type": "filter", "Numeric": {"field": "value", "operator": "greater_than", "value": 10.0}},
            // `on_missing` defaults to `error`, and nothing here carries `missing`
            {"type": "reducer", "aggregations": [{"function": "sum", "as": "total", "field": "missing"}], "group_by": []},
        ],
    }))
    .await?;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["outcome"], "failed");
    assert_eq!(body["phase"], "apply");
    assert_eq!(body["at"], 1);
    // the filter still shows what it passed on
    assert_eq!(body["stages"][0]["batches"][0], json!([{"value": 20}]));
    Ok(())
}

/// State is private to the request: seeded from the body, returned in the
/// response, and never the server's own.
#[tokio::test]
async fn state_is_seeded_from_the_request_and_comes_back_with_it() -> anyhow::Result<()> {
    let (status, body) = dry_run(&json!({
        "messages": [{"machine": "m1", "value": 7}],
        "state": {"bucket": "scratch", "key": "machine"},
        "buckets": {"m1": {"recipe": "rye"}},
        "transforms": [
            {"type": "recall", "recall": ["recipe"], "on_missing": "skip"},
            {"type": "remember", "remember": [{"field": "value", "as": "last_value"}]},
        ],
    }))
    .await?;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["outcome"], "ran");
    // the recall put the remembered field on the message
    assert_eq!(body["stages"][0]["batches"][0][0]["recipe"], "rye");
    // and what the remember wrote is visible rather than a side effect nobody
    // can see
    assert_eq!(body["buckets"]["m1"]["last_value"], 7);
    Ok(())
}

/// An empty chain is a legal question with a dull answer, and must not be a
/// failure: it is what the form asks while there are no transforms yet.
#[tokio::test]
async fn a_chain_with_no_transforms_runs_and_reports_no_stages() -> anyhow::Result<()> {
    let (status, body) = dry_run(&json!({"messages": [{"n": 1}], "transforms": []})).await?;

    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["outcome"], "ran");
    assert_eq!(body["stages"].as_array().map(Vec::len), Some(0));
    Ok(())
}
