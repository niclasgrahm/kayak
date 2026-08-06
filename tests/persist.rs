//! The `--config` file as a load source and a save target.
//!
//! The file is never a mirror of what the server is running. Editing the graph
//! changes the runtime immediately and leaves the file alone; only an explicit
//! `POST /api/config/save` writes anything. These tests pin both halves of
//! that — that edits *don't* reach disk, and that a save produces a file the
//! next start can build.
//!
//! They go through the real HTTP surface with `tower::oneshot`, same as
//! `tests/api.rs`, because the round trip is the thing being tested. Every
//! pipeline is a `dummy` input on a long interval with a `stdout` output, so
//! nothing external is contacted.

use std::path::Path;
use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use streamer::api_router;
use streamer::state::AppState;
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

/// A pipeline fed by another one, which is the case the file's *order* exists
/// for: it can only be built once `upstream` is there.
fn downstream_config(id: &str, upstream: &str) -> Value {
    json!({
        "id": id,
        "inputs": [{ "type": "streamer", "upstream": upstream }],
        "transforms": [],
        "outputs": []
    })
}

/// A server started from `path`, exactly as `main` does with `--config`.
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

async fn post_stream(app: &Router, config: &Value) -> anyhow::Result<StatusCode> {
    Ok(post(app, "/api/streams", config).await?.0)
}

async fn save_as(app: &Router, name: &str) -> anyhow::Result<(StatusCode, Value)> {
    post(app, "/api/config/save", &json!({ "name": name })).await
}

/// A save that says which format it wants, as the UI's picker does.
async fn save_as_format(
    app: &Router,
    name: &str,
    format: &str,
) -> anyhow::Result<(StatusCode, Value)> {
    post(
        app,
        "/api/config/save",
        &json!({ "name": name, "format": format }),
    )
    .await
}

async fn revert(app: &Router) -> anyhow::Result<StatusCode> {
    Ok(post(app, "/api/config/revert", &json!({})).await?.0)
}

async fn delete_stream(app: &Router, id: &str) -> anyhow::Result<StatusCode> {
    let req = Request::builder()
        .method("DELETE")
        .uri(format!("/api/streams/{id}"))
        .body(Body::empty())?;
    Ok(send(app, req).await?.0)
}

async fn settings(app: &Router) -> anyhow::Result<Value> {
    let req = Request::builder().uri("/api/settings").body(Body::empty())?;
    Ok(send(app, req).await?.1)
}

async fn listed_ids(app: &Router) -> anyhow::Result<Vec<String>> {
    let req = Request::builder().uri("/api/streams").body(Body::empty())?;
    let (_, body) = send(app, req).await?;
    let mut ids: Vec<String> = body
        .as_array()
        .map(|list| {
            list.iter()
                .filter_map(|s| s["id"].as_str().map(ToString::to_string))
                .collect()
        })
        .unwrap_or_default();
    // the server holds them in a HashMap; only the file has a defined order
    ids.sort();
    Ok(ids)
}

/// The pipelines a file declares, in the order it declares them — read the way
/// a restart would read it, so a `.yaml` file is checked as YAML.
fn ids_in(path: &Path) -> anyhow::Result<Vec<String>> {
    Ok(streamer::persist::read(path)?
        .into_iter()
        .map(|c| c.id.unwrap_or_default())
        .collect())
}

/// A file to start a server from.
fn seeded(dir: &tempfile::TempDir, configs: &[Value]) -> anyhow::Result<std::path::PathBuf> {
    let path = dir.path().join("config.json");
    std::fs::write(&path, serde_json::to_string_pretty(configs)?)?;
    Ok(path)
}

// ---------------------------------------------------------------------------
// editing does not touch the file
// ---------------------------------------------------------------------------

/// The core of the design: the graph is edited live, the file is not touched.
/// Anything else and a live view of a running system is one stray click away
/// from a committed change.
#[tokio::test]
async fn editing_the_graph_leaves_the_config_file_alone() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = seeded(&dir, &[idle_config("seeded"), idle_config("doomed")])?;
    let app = app_from(&path)?;
    let before = std::fs::read_to_string(&path)?;

    assert_eq!(post_stream(&app, &idle_config("added")).await?, StatusCode::CREATED);
    assert_eq!(delete_stream(&app, "doomed").await?, StatusCode::NO_CONTENT);

    // the runtime changed...
    assert_eq!(listed_ids(&app).await?, ["added", "seeded"]);
    // ...and the file did not
    assert_eq!(std::fs::read_to_string(&path)?, before);
    Ok(())
}

/// Starting the server is not an edit either. A file the server has only read
/// comes back byte-identical, hand-written layout and all.
#[tokio::test]
async fn loading_a_config_file_does_not_rewrite_it() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("config.json");
    // deliberately in an order and a layout the writer would not choose
    let hand_written = concat!(
        "[{\"id\": \"z-root\", \"inputs\": [{\"type\": \"dummy\", \"duration\": 3600}],\n",
        "  \"transforms\": [], \"outputs\": []},\n",
        " {\"id\": \"a-child\", \"inputs\": [{\"type\": \"streamer\", \"upstream\": \"z-root\"}],\n",
        "  \"transforms\": [], \"outputs\": []}]"
    );
    std::fs::write(&path, hand_written)?;

    let _app = app_from(&path)?;

    assert_eq!(std::fs::read_to_string(&path)?, hand_written);
    Ok(())
}

// ---------------------------------------------------------------------------
// unsaved changes
// ---------------------------------------------------------------------------

/// Edits are live and the file is untouched, so divergence is invisible unless
/// the server says so — and a restart would silently drop the work.
#[tokio::test]
async fn the_server_reports_when_the_graph_has_diverged_from_the_file() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = seeded(&dir, &[idle_config("seeded")])?;
    let app = app_from(&path)?;

    let fresh = settings(&app).await?;
    assert_eq!(fresh["config_file"], json!("config.json"));
    assert_eq!(fresh["unsaved_changes"], json!(false), "nothing has happened yet");

    post_stream(&app, &idle_config("added")).await?;
    assert_eq!(settings(&app).await?["unsaved_changes"], json!(true));

    save_as(&app, "config.json").await?;
    assert_eq!(
        settings(&app).await?["unsaved_changes"],
        json!(false),
        "saving is what makes the two agree again"
    );
    Ok(())
}

/// Add and remove the same pipeline and you are back where you started. The
/// dirty check compares rendered graphs, not a counter of how many things
/// happened, so it can tell.
#[tokio::test]
async fn an_edit_that_cancels_itself_out_is_not_an_unsaved_change() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = seeded(&dir, &[idle_config("seeded")])?;
    let app = app_from(&path)?;

    post_stream(&app, &idle_config("temporary")).await?;
    assert_eq!(settings(&app).await?["unsaved_changes"], json!(true));
    delete_stream(&app, "temporary").await?;

    assert_eq!(settings(&app).await?["unsaved_changes"], json!(false));
    Ok(())
}

/// With nowhere to save to there is nothing to be out of sync with, and the UI
/// should not nag about it.
#[tokio::test]
async fn a_server_without_a_config_file_has_nothing_to_save() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let app = api_router(Arc::new(AppState::new()));

    post_stream(&app, &idle_config("ephemeral")).await?;
    let settings = settings(&app).await?;
    assert_eq!(settings["config_file"], Value::Null);
    assert_eq!(settings["unsaved_changes"], json!(false));

    // and a save has nowhere to go, rather than picking somewhere
    let (status, _) = save_as(&app, "anywhere.json").await?;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert_eq!(
        std::fs::read_dir(dir.path())?.count(),
        0,
        "an in-memory server wrote something"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// saving
// ---------------------------------------------------------------------------

#[tokio::test]
async fn saving_under_a_new_name_leaves_the_original_untouched() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = seeded(&dir, &[idle_config("seeded")])?;
    let app = app_from(&path)?;
    let original = std::fs::read_to_string(&path)?;
    post_stream(&app, &idle_config("added")).await?;

    let (status, body) = save_as(&app, "next.json").await?;
    assert_eq!(status, StatusCode::OK);

    let written = dir.path().join("next.json");
    assert_eq!(body["path"], json!(written.display().to_string()));
    assert_eq!(ids_in(&written)?, ["added", "seeded"]);
    assert_eq!(std::fs::read_to_string(&path)?, original);
    Ok(())
}

#[tokio::test]
async fn saving_under_the_loaded_name_overwrites_it() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = seeded(&dir, &[idle_config("seeded")])?;
    let app = app_from(&path)?;
    post_stream(&app, &idle_config("added")).await?;

    assert_eq!(save_as(&app, "config.json").await?.0, StatusCode::OK);

    assert_eq!(ids_in(&path)?, ["added", "seeded"]);
    Ok(())
}

/// The whole point of saving: a server restarted from the file runs the same
/// graph. Building a second `AppState` from it is what the next
/// `cargo run -- --config` does.
#[tokio::test]
async fn a_saved_file_starts_the_same_graph_again() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = seeded(&dir, &[idle_config("root")])?;
    let app = app_from(&path)?;
    post_stream(&app, &downstream_config("child", "root")).await?;
    save_as(&app, "config.json").await?;

    let restarted = app_from(&path)?;

    assert_eq!(listed_ids(&restarted).await?, ["child", "root"]);
    assert_eq!(
        settings(&restarted).await?["unsaved_changes"],
        json!(false),
        "a graph straight off disk is in sync with disk"
    );
    Ok(())
}

/// A downstream can only be built after its upstream, so the file has to
/// declare it later — whatever order the pipelines were created in.
#[tokio::test]
async fn a_downstream_is_saved_after_its_upstream() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    // 'a-child' sorts first, so only the graph can be putting 'z-root' first
    let path = seeded(&dir, &[idle_config("z-root")])?;
    let app = app_from(&path)?;
    post_stream(&app, &downstream_config("a-child", "z-root")).await?;
    save_as(&app, "config.json").await?;

    assert_eq!(ids_in(&path)?, ["z-root", "a-child"]);
    Ok(())
}

/// A pipeline posted without an `id` gets a generated petname. That name is
/// what a downstream would have to reference, so it's the name that has to be
/// saved — a config with no id would come back as a *different* pipeline.
#[tokio::test]
async fn a_generated_id_is_saved() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = seeded(&dir, &[])?;
    let app = app_from(&path)?;
    post_stream(
        &app,
        &json!({
            "inputs": [{ "type": "dummy", "duration": 3600 }],
            "transforms": [],
            "outputs": [{ "type": "stdout" }]
        }),
    )
    .await?;
    save_as(&app, "config.json").await?;

    match ids_in(&path)?.as_slice() {
        [id] => assert!(!id.is_empty(), "the generated id was not saved"),
        other => panic!("expected exactly one pipeline, got {}", other.len()),
    }
    Ok(())
}

/// Two saves of the same graph produce the same bytes, or every save shows up
/// as a diff of the whole file.
#[tokio::test]
async fn saving_an_unchanged_graph_twice_produces_an_identical_file() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = seeded(&dir, &[idle_config("a"), idle_config("b"), idle_config("c")])?;
    let app = app_from(&path)?;

    save_as(&app, "one.json").await?;
    // add and remove: the graph ends up where it started
    post_stream(&app, &idle_config("d")).await?;
    delete_stream(&app, "d").await?;
    save_as(&app, "two.json").await?;

    assert_eq!(
        std::fs::read_to_string(dir.path().join("one.json"))?,
        std::fs::read_to_string(dir.path().join("two.json"))?
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// yaml
//
// The format is a property of the file and nothing else: the same graph, the
// same rules, spelled differently. These pin that it really is only spelling.
// ---------------------------------------------------------------------------

/// The reason to support YAML at all: a hand-written `.yaml` file starts the
/// server. Written in block style with a comment, because that is what someone
/// would actually write and none of it may reach the runtime.
#[tokio::test]
async fn a_server_can_be_started_from_a_yaml_config_file() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("config.yaml");
    std::fs::write(
        &path,
        concat!(
            "# the root, ticking once an hour\n",
            "- id: z-root\n",
            "  inputs:\n",
            "    - type: dummy\n",
            "      duration: 3600\n",
            "  transforms: []\n",
            "  outputs:\n",
            "    - type: stdout\n",
            "- id: a-child\n",
            "  inputs:\n",
            "    - type: streamer\n",
            "      upstream: z-root\n",
            "  transforms: []\n",
            "  outputs: []\n",
        ),
    )?;

    let app = app_from(&path)?;

    assert_eq!(listed_ids(&app).await?, ["a-child", "z-root"]);
    assert_eq!(settings(&app).await?["config_file"], json!("config.yaml"));
    assert_eq!(
        settings(&app).await?["unsaved_changes"],
        json!(false),
        "a graph straight off disk is in sync with disk, whatever it is written in"
    );
    Ok(())
}

/// Loading is not an edit in either format.
#[tokio::test]
async fn loading_a_yaml_config_file_does_not_rewrite_it() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("config.yml");
    let hand_written =
        "- id: seeded\n  inputs: [{type: dummy, duration: 3600}]\n  transforms: []\n  outputs: []\n";
    std::fs::write(&path, hand_written)?;

    let _app = app_from(&path)?;

    assert_eq!(std::fs::read_to_string(&path)?, hand_written);
    Ok(())
}

/// The round trip that matters: save as YAML, restart from it, same graph —
/// including the ordering that a `streamer` input depends on.
#[tokio::test]
async fn a_graph_saved_as_yaml_starts_again_from_the_yaml_file() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = seeded(&dir, &[idle_config("z-root")])?;
    let app = app_from(&path)?;
    post_stream(&app, &downstream_config("a-child", "z-root")).await?;

    let (status, body) = save_as_format(&app, "config.yaml", "yaml").await?;
    assert_eq!(status, StatusCode::OK);

    let written = dir.path().join("config.yaml");
    assert_eq!(body["path"], json!(written.display().to_string()));
    let contents = std::fs::read_to_string(&written)?;
    assert!(
        !contents.trim_start().starts_with('['),
        "asked for yaml, got json: {contents}"
    );
    assert_eq!(ids_in(&written)?, ["z-root", "a-child"]);

    let restarted = app_from(&written)?;
    assert_eq!(listed_ids(&restarted).await?, ["a-child", "z-root"]);
    Ok(())
}

/// Saving is what makes "unsaved changes" go away — the dirty check compares
/// graphs, so the format it was written in doesn't come into it.
#[tokio::test]
async fn saving_as_yaml_clears_the_unsaved_marker() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = seeded(&dir, &[idle_config("seeded")])?;
    let app = app_from(&path)?;
    post_stream(&app, &idle_config("added")).await?;
    assert_eq!(settings(&app).await?["unsaved_changes"], json!(true));

    save_as_format(&app, "config.yaml", "yaml").await?;

    assert_eq!(settings(&app).await?["unsaved_changes"], json!(false));
    Ok(())
}

/// A request that doesn't mention a format gets the one its name implies, so a
/// client that predates the choice still writes a `.yaml` file that will load.
#[tokio::test]
async fn a_save_without_a_format_follows_the_file_name() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = seeded(&dir, &[idle_config("seeded")])?;
    let app = app_from(&path)?;

    assert_eq!(save_as(&app, "inferred.yaml").await?.0, StatusCode::OK);

    let written = dir.path().join("inferred.yaml");
    let contents = std::fs::read_to_string(&written)?;
    assert!(
        contents.starts_with("- id: seeded"),
        "the extension did not pick the format: {contents}"
    );
    assert_eq!(ids_in(&written)?, ["seeded"]);
    Ok(())
}

/// Both formats describe the same pipelines, down to the component fields —
/// otherwise "save as yaml" would be a quiet way to change what runs next.
#[tokio::test]
async fn the_same_graph_saves_to_the_same_pipelines_in_either_format() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = seeded(&dir, &[idle_config("root")])?;
    let app = app_from(&path)?;
    post_stream(&app, &downstream_config("child", "root")).await?;

    save_as_format(&app, "both.json", "json").await?;
    save_as_format(&app, "both.yaml", "yaml").await?;

    let as_json = streamer::persist::read(&dir.path().join("both.json"))?;
    let as_yaml = streamer::persist::read(&dir.path().join("both.yaml"))?;
    assert_eq!(
        serde_json::to_value(&as_json)?,
        serde_json::to_value(&as_yaml)?
    );
    Ok(())
}

/// The directory rule is about the path, not the contents, so a `.yaml` name
/// gets exactly the same refusal.
#[tokio::test]
async fn a_yaml_save_cannot_leave_the_config_directory_either() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = seeded(&dir, &[idle_config("seeded")])?;
    let app = app_from(&path)?;

    let (status, _) = save_as_format(&app, "../stolen.yaml", "yaml").await?;
    assert_eq!(status, StatusCode::UNPROCESSABLE_ENTITY);
    assert!(!dir.path().parent().unwrap_or(dir.path()).join("stolen.yaml").exists());
    Ok(())
}

/// A `.yaml` file that isn't YAML is an error, not a fallback to JSON: the
/// extension is the whole rule, and guessing would hide the typo.
#[tokio::test]
async fn a_yaml_file_that_does_not_parse_fails_to_start() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = dir.path().join("config.yaml");
    std::fs::write(&path, "- id: broken\n  inputs: [{type: dummy,\n")?;

    let Err(err) = app_from(&path) else {
        panic!("a broken yaml file started a server");
    };

    let message = format!("{err:#}");
    assert!(
        message.contains("config.yaml") && message.contains("as yaml"),
        "the error should name the file and the format: {message}"
    );
    Ok(())
}

/// A save is a write to the server's disk driven by a request. The directory is
/// not the caller's to choose, and a refusal must not write anything anywhere.
#[tokio::test]
async fn a_save_cannot_be_talked_into_leaving_the_config_directory() -> anyhow::Result<()> {
    let outside = tempfile::tempdir()?;
    let dir = tempfile::tempdir()?;
    let path = seeded(&dir, &[idle_config("seeded")])?;
    let app = app_from(&path)?;

    let escape = outside.path().join("stolen.json");
    for name in [
        "../stolen.json",
        "sub/stolen.json",
        escape.display().to_string().as_str(),
        "..",
        "",
    ] {
        let (status, _) = save_as(&app, name).await?;
        assert_eq!(
            status,
            StatusCode::UNPROCESSABLE_ENTITY,
            "'{name}' should have been refused"
        );
    }

    assert!(!escape.exists(), "a save escaped the config directory");
    assert_eq!(
        std::fs::read_dir(dir.path())?.count(),
        1,
        "a refused save left something behind"
    );
    Ok(())
}

// ---------------------------------------------------------------------------
// reverting
// ---------------------------------------------------------------------------

/// The undo that a read-only file otherwise takes away: edits are live, so
/// reloading the file is the only way back to a known state.
#[tokio::test]
async fn reverting_restores_the_graph_the_file_describes() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = seeded(&dir, &[idle_config("keep"), idle_config("doomed")])?;
    let app = app_from(&path)?;
    post_stream(&app, &idle_config("added")).await?;
    delete_stream(&app, "doomed").await?;
    assert_eq!(listed_ids(&app).await?, ["added", "keep"]);

    assert_eq!(revert(&app).await?, StatusCode::NO_CONTENT);

    assert_eq!(listed_ids(&app).await?, ["doomed", "keep"]);
    assert_eq!(
        settings(&app).await?["unsaved_changes"],
        json!(false),
        "a reverted graph is the file's graph"
    );
    Ok(())
}

/// Reverting to a file that has since been broken by hand must not leave the
/// server with nothing running: the parse happens before the teardown.
#[tokio::test]
async fn a_revert_to_an_unparseable_file_keeps_the_running_graph() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = seeded(&dir, &[idle_config("running")])?;
    let app = app_from(&path)?;
    std::fs::write(&path, "{ this is not a config")?;

    let (status, _) = post(&app, "/api/config/revert", &json!({})).await?;
    assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);

    assert_eq!(
        listed_ids(&app).await?,
        ["running"],
        "the graph was torn down for a file that could never have loaded"
    );
    Ok(())
}

/// Reverting a graph with upstreams must not report the teardown as failures.
///
/// The regression: cancelling every streamer and then dropping the upstreams
/// left each downstream woken with both its cancellation and an "upstream
/// streamer 'x' is gone" ready at once, and a random pick reported the latter.
/// Those errors reached the UI, where they landed on the cards of the *newly
/// built* streamers that had just taken the same ids — so a clean revert looked
/// like it had produced a broken graph.
#[tokio::test]
async fn reverting_a_graph_with_upstreams_reports_no_errors() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = seeded(
        &dir,
        &[
            idle_config("source"),
            downstream_config("reader-a", "source"),
            downstream_config("reader-b", "source"),
        ],
    )?;
    let state = Arc::new(AppState::from_config(&path)?);
    // subscribed before the revert, so nothing published during it is missed
    let mut events = state.subscribe_events();
    let app = api_router(Arc::clone(&state));

    // reverted repeatedly because the bug was a `select!` coin toss: one revert
    // reproduced it only sometimes, and a regression test that fails a third of
    // the time is not a regression test
    for _ in 0..8 {
        post_stream(&app, &idle_config("scratch")).await?;
        assert_eq!(revert(&app).await?, StatusCode::NO_CONTENT);
    }

    let mut errors = Vec::new();
    while let Ok(event) = events.try_recv() {
        if let streamer_core::EventPayload::Error(message) = event.payload {
            errors.push(format!("[{}] {}", event.streamer_id, message));
        }
    }
    assert!(
        errors.is_empty(),
        "reverting reported its own teardown as pipeline failures: {errors:?}"
    );
    assert_eq!(listed_ids(&app).await?, ["reader-a", "reader-b", "source"]);
    Ok(())
}

/// A revert waits for the old run loops to stop before building the new ones.
/// Two run loops for one pipeline would share a kafka consumer group or a nats
/// subscription and double up on every output, so the overlap is not merely
/// untidy.
#[tokio::test]
async fn reverting_stops_the_old_pipelines_before_starting_the_new_ones() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = seeded(&dir, &[idle_config("a"), downstream_config("b", "a")])?;
    let state = Arc::new(AppState::from_config(&path)?);
    let before: Vec<_> = state.get_streamer_ids();
    let app = api_router(Arc::clone(&state));

    revert(&app).await?;

    // the ids are the same, but every streamer behind them is a new one — the
    // old handles are gone from the map and their loops have been awaited
    assert_eq!(before.len(), 2);
    assert_eq!(listed_ids(&app).await?, ["a", "b"]);
    assert!(
        !state.has_unsaved_changes(),
        "a reverted graph should match the file it came from"
    );
    Ok(())
}

/// Reverting is a reload, so a pipeline added to the file since startup shows
/// up — which is what makes it useful for hand-editing the file too.
#[tokio::test]
async fn reverting_picks_up_changes_made_to_the_file() -> anyhow::Result<()> {
    let dir = tempfile::tempdir()?;
    let path = seeded(&dir, &[idle_config("original")])?;
    let app = app_from(&path)?;
    std::fs::write(
        &path,
        serde_json::to_string_pretty(&json!([idle_config("original"), idle_config("hand-added")]))?,
    )?;

    revert(&app).await?;

    assert_eq!(listed_ids(&app).await?, ["hand-added", "original"]);
    Ok(())
}
