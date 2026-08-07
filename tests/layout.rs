//! The canvas layout file, end to end.
//!
//! Where the cards sit is not configuration: it changes nothing the server
//! runs, it lives in its own file beside the config, and — unlike every other
//! edit the UI makes — it is written the moment it changes rather than waiting
//! for a save. These tests pin all three of those, because each of them is a
//! deliberate difference from how `--config` is treated (see `tests/persist.rs`)
//! and so each of them is a thing a well-meaning change could quietly undo.
//!
//! Through the real router with `tower::oneshot`, same as `tests/api.rs`.

use std::path::Path;
use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use http_body_util::BodyExt;
use kayak::api_router;
use kayak::state::AppState;
use serde_json::{Value, json};
use tower::ServiceExt;

/// A pipeline that will sit idle: the dummy input only ticks once an hour.
fn idle_config(id: &str) -> Value {
    json!({
        "id": id,
        "inputs": [{ "type": "dummy", "duration": 3600 }],
        "transforms": [],
        "outputs": [{ "type": "stdout" }]
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

async fn get_layout(app: &Router) -> anyhow::Result<(StatusCode, Value)> {
    send(
        app,
        Request::builder().uri("/api/layout").body(Body::empty())?,
    )
    .await
}

async fn put_layout(app: &Router, layout: &Value) -> anyhow::Result<(StatusCode, Value)> {
    let req = Request::builder()
        .method("PUT")
        .uri("/api/layout")
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(layout)?))?;
    send(app, req).await
}

async fn settings(app: &Router) -> anyhow::Result<Value> {
    Ok(send(
        app,
        Request::builder()
            .uri("/api/settings")
            .body(Body::empty())?,
    )
    .await?
    .1)
}

fn placed(x: f64, y: f64) -> Value {
    json!({ "x": x, "y": y, "width": 360.0 })
}

/// Writes a config file and returns the directory it lives in, so the layout
/// file's own path can be asserted against it.
fn config_dir(pipelines: &[Value]) -> anyhow::Result<(tempfile::TempDir, std::path::PathBuf)> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("config.json");
    std::fs::write(&path, serde_json::to_string_pretty(&pipelines)?)?;
    Ok((dir, path))
}

/// A graph nobody has arranged has no layout file and an empty layout — the
/// canvas lays that out itself, so the absence must not be an error.
#[tokio::test]
async fn a_graph_that_has_never_been_arranged_has_an_empty_layout() -> anyhow::Result<()> {
    let (dir, path) = config_dir(&[idle_config("a")])?;
    let app = app_from(&path)?;

    let (status, body) = get_layout(&app).await?;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(body["pipelines"], json!({}));
    assert!(
        !dir.path().join("config.layout.json").exists(),
        "a layout file was written by merely starting up"
    );
    Ok(())
}

/// The other half: an arrangement lands on disk as soon as it is sent, without
/// a save. Moving a card is not a change to the system, so making someone
/// commit to it would be ceremony over a cosmetic act.
#[tokio::test]
async fn an_arrangement_is_written_immediately_and_read_back() -> anyhow::Result<()> {
    let (dir, path) = config_dir(&[idle_config("a"), idle_config("b")])?;
    let app = app_from(&path)?;

    let arrangement = json!({
        "version": 1,
        "pipelines": { "a": placed(0.0, 0.0), "b": placed(400.0, 300.0) }
    });
    let (status, _) = put_layout(&app, &arrangement).await?;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let layout_file = dir.path().join("config.layout.json");
    assert!(layout_file.exists(), "nothing was written");
    let on_disk: Value = serde_json::from_str(&std::fs::read_to_string(&layout_file)?)?;
    assert_eq!(on_disk["pipelines"]["b"]["x"], json!(400.0));

    // and the next start picks it up
    let restarted = app_from(&path)?;
    assert_eq!(
        get_layout(&restarted).await?.1["pipelines"]["b"]["y"],
        json!(300.0)
    );
    Ok(())
}

/// The layout is not configuration, so arranging the canvas must not make the
/// navbar claim there is unsaved work. Getting this wrong would train people to
/// ignore the one warning standing between an afternoon's edits and a restart.
#[tokio::test]
async fn arranging_the_canvas_is_not_an_unsaved_change() -> anyhow::Result<()> {
    let (_dir, path) = config_dir(&[idle_config("a")])?;
    let app = app_from(&path)?;
    assert_eq!(settings(&app).await?["unsaved_changes"], json!(false));

    put_layout(&app, &json!({ "pipelines": { "a": placed(600.0, 600.0) } })).await?;
    assert_eq!(
        settings(&app).await?["unsaved_changes"],
        json!(false),
        "moving a card was reported as an unsaved change to the pipelines"
    );
    Ok(())
}

/// A full replacement, not a patch — which is what makes "put it all back to
/// automatic" an ordinary send of a smaller map rather than its own endpoint.
#[tokio::test]
async fn sending_a_smaller_arrangement_drops_what_is_missing() -> anyhow::Result<()> {
    let (_dir, path) = config_dir(&[idle_config("a"), idle_config("b")])?;
    let app = app_from(&path)?;

    put_layout(
        &app,
        &json!({ "pipelines": { "a": placed(0.0, 0.0), "b": placed(400.0, 0.0) } }),
    )
    .await?;
    put_layout(&app, &json!({ "pipelines": { "a": placed(0.0, 0.0) } })).await?;

    let (_, body) = get_layout(&app).await?;
    assert!(
        body["pipelines"]["b"].is_null(),
        "'b' was not unpinned: {body}"
    );
    assert!(!body["pipelines"]["a"].is_null());
    Ok(())
}

/// The arrangement covers the lines as well as the cards: an edge whose middle
/// channel has been dragged out of the way of another one is an adjustment
/// worth keeping, and it round trips the same way a position does.
#[tokio::test]
async fn an_adjusted_edge_is_written_and_read_back() -> anyhow::Result<()> {
    let (dir, path) = config_dir(&[idle_config("a")])?;
    let app = app_from(&path)?;

    let (status, _) = put_layout(
        &app,
        &json!({
            "pipelines": {},
            "edges": [{ "from": "a", "to": "b", "offset": -60.0 }]
        }),
    )
    .await?;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let on_disk: Value = serde_json::from_str(&std::fs::read_to_string(
        dir.path().join("config.layout.json"),
    )?)?;
    assert_eq!(on_disk["edges"][0]["offset"], json!(-60.0));

    let restarted = app_from(&path)?;
    let (_, body) = get_layout(&restarted).await?;
    assert_eq!(body["edges"][0]["from"], json!("a"));
    assert_eq!(body["edges"][0]["offset"], json!(-60.0));
    Ok(())
}

/// Where a line attaches to a card is stored with the *face* it was measured
/// on, because the number means nothing without it. Both survive the round trip
/// with the face written as a name — the file is read by people.
#[tokio::test]
async fn a_pinned_connection_point_keeps_the_face_it_was_measured_on() -> anyhow::Result<()> {
    let (dir, path) = config_dir(&[idle_config("a")])?;
    let app = app_from(&path)?;

    put_layout(
        &app,
        &json!({
            "pipelines": {},
            "edges": [{
                "from": "a",
                "to": "b",
                "from_port": { "side": "bottom", "along": 260.0 },
                "to_port": { "side": "left", "along": 40.0 }
            }]
        }),
    )
    .await?;

    let written = std::fs::read_to_string(dir.path().join("config.layout.json"))?;
    assert!(written.contains(r#""side": "bottom""#), "got: {written}");

    let restarted = app_from(&path)?;
    let (_, body) = get_layout(&restarted).await?;
    assert_eq!(body["edges"][0]["from_port"]["along"], json!(260.0));
    assert_eq!(body["edges"][0]["to_port"]["side"], json!("left"));
    // nothing was invented: an unadjusted channel stays out of the file
    assert!(body["edges"][0]["offset"].is_null(), "got: {body}");
    Ok(())
}

/// An arrangement with nothing adjusted writes no `edges` key at all, so a
/// graph laid out entirely automatically has a file with nothing in it rather
/// than an empty list to explain.
#[tokio::test]
async fn an_arrangement_with_no_adjusted_edges_writes_none() -> anyhow::Result<()> {
    let (dir, path) = config_dir(&[idle_config("a")])?;
    let app = app_from(&path)?;

    put_layout(&app, &json!({ "pipelines": { "a": placed(40.0, 40.0) } })).await?;

    let written = std::fs::read_to_string(dir.path().join("config.layout.json"))?;
    assert!(!written.contains("edges"), "got: {written}");
    Ok(())
}

/// A position for a pipeline that no longer exists is harmless — the canvas
/// simply has nothing to apply it to — and dropping it would lose the
/// arrangement of a pipeline someone is about to re-create.
#[tokio::test]
async fn a_position_for_an_unknown_pipeline_is_kept() -> anyhow::Result<()> {
    let (_dir, path) = config_dir(&[idle_config("a")])?;
    let app = app_from(&path)?;

    let (status, _) = put_layout(
        &app,
        &json!({ "pipelines": { "ghost": placed(20.0, 20.0) } }),
    )
    .await?;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert_eq!(
        get_layout(&app).await?.1["pipelines"]["ghost"]["x"],
        json!(20.0)
    );
    Ok(())
}

/// Without a `--config` there is nowhere to put the file. The arrangement still
/// works for the life of the process, because refusing to let someone tidy the
/// canvas would be a worse answer than not remembering it.
#[tokio::test]
async fn an_arrangement_without_a_config_file_is_kept_in_memory() -> anyhow::Result<()> {
    let app = api_router(Arc::new(AppState::new()));

    let (status, _) =
        put_layout(&app, &json!({ "pipelines": { "a": placed(80.0, 80.0) } })).await?;
    assert_eq!(status, StatusCode::NO_CONTENT);
    assert_eq!(
        get_layout(&app).await?.1["pipelines"]["a"]["x"],
        json!(80.0)
    );
    Ok(())
}

/// Reverting is "go back to what is on disk", and the arrangement is on disk
/// too — so it goes back as well.
#[tokio::test]
async fn reverting_reloads_the_arrangement_from_disk() -> anyhow::Result<()> {
    let (dir, path) = config_dir(&[idle_config("a")])?;
    std::fs::write(
        dir.path().join("config.layout.json"),
        json!({ "pipelines": { "a": placed(100.0, 100.0) } }).to_string(),
    )?;
    let app = app_from(&path)?;

    put_layout(&app, &json!({ "pipelines": { "a": placed(999.0, 999.0) } })).await?;
    assert_eq!(
        get_layout(&app).await?.1["pipelines"]["a"]["x"],
        json!(999.0)
    );

    // the file is the thing being reverted *to*, so change it underneath
    std::fs::write(
        dir.path().join("config.layout.json"),
        json!({ "pipelines": { "a": placed(40.0, 40.0) } }).to_string(),
    )?;
    let req = Request::builder()
        .method("POST")
        .uri("/api/config/revert")
        .body(Body::empty())?;
    assert_eq!(send(&app, req).await?.0, StatusCode::NO_CONTENT);

    assert_eq!(
        get_layout(&app).await?.1["pipelines"]["a"]["x"],
        json!(40.0),
        "reverting kept the arrangement that was only in memory"
    );
    Ok(())
}
