//! Changing a running pipeline in place: `AppState::replace_pipeline`, which
//! `PUT /api/pipelines/{id}` is a thin handler over.
//!
//! What these pin is what a delete and a create could not do: the pipelines
//! under the replaced one stay attached and keep running, the mistakes that
//! can be caught before anything stops leave the old pipeline untouched, and a
//! config that fails to build puts the old one back.

use std::sync::Arc;
use std::time::Duration;

use kayak::inputs::http::{Credentials, PostMeta};
use kayak::pipeline::Pipeline;
use kayak::state::{AppState, PipelineError};
use kayak_core::RunStatus;
use kayak_core::config::Config;
use serde_json::{Value, json};
use tokio::sync::mpsc;

fn config(value: Value) -> anyhow::Result<Config> {
    Ok(serde_json::from_value(value)?)
}

/// A pipeline that sits idle — the dummy input only ticks once an hour.
fn idle(id: &str) -> anyhow::Result<Config> {
    config(json!({
        "id": id,
        "inputs": [{ "type": "dummy", "duration": 3600 }],
        "outputs": [{ "type": "stdout" }]
    }))
}

/// A pipeline that ticks constantly and stamps every message with `version`,
/// so a downstream can tell which config produced what it received.
fn stamped(id: &str, version: &str) -> anyhow::Result<Config> {
    config(json!({
        "id": id,
        "inputs": [{ "type": "dummy", "duration": 0 }],
        "transforms": [{
            "type": "map",
            "mappings": [{
                "type": "constant",
                "value": { "type": "text", "value": version },
                "as": "version"
            }]
        }],
        "outputs": [{ "type": "stdout" }]
    }))
}

fn downstream_of(id: &str, upstream: &str) -> anyhow::Result<Config> {
    config(json!({
        "id": id,
        "inputs": [{ "type": "pipeline", "upstream": upstream }],
        "outputs": [{ "type": "stdout" }]
    }))
}

fn running(state: &AppState, id: &str) -> anyhow::Result<Arc<Pipeline>> {
    state
        .pipeline_handles()
        .into_iter()
        .find(|pipeline| pipeline.id == id)
        .ok_or_else(|| anyhow::anyhow!("pipeline '{id}' is not running"))
}

/// Waits for a batch carrying `version`, skipping any from before it. `None`
/// when the channel closed or nothing came within five seconds.
async fn a_batch_stamped(
    rx: &mut mpsc::Receiver<Arc<kayak_core::MessageBatch>>,
    version: &str,
) -> Option<()> {
    let wanted = json!(version);
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let batch = rx.recv().await?;
            if batch.iter().any(|message| message["version"] == wanted) {
                return Some(());
            }
        }
    })
    .await
    .ok()
    .flatten()
}

/// Asserts that a refused replace stopped nothing: the same pipeline is still
/// registered, still running its old config, and was never cancelled.
fn untouched(state: &AppState, before: &Arc<Pipeline>) -> anyhow::Result<()> {
    let now = running(state, &before.id)?;
    assert!(Arc::ptr_eq(before, &now), "the pipeline was rebuilt");
    assert!(
        !before.cancellation_token.is_cancelled(),
        "the pipeline was stopped"
    );
    Ok(())
}

#[tokio::test]
async fn a_replace_runs_the_new_config_under_the_same_id() -> anyhow::Result<()> {
    let state = AppState::new();
    let old = state.create_pipeline(idle("p1")?)?;

    let new = state.replace_pipeline("p1", stamped("p1", "v2")?).await?;

    assert_eq!(new.id, "p1");
    assert_eq!(new.config.transforms.len(), 1);
    assert!(old.cancellation_token.is_cancelled(), "the old run loop was left running");
    assert!(Arc::ptr_eq(&new, &running(&state, "p1")?));
    assert_eq!(state.get_pipeline_ids(), vec!["p1".to_string()]);
    Ok(())
}

/// The body may leave the id out; the path is what names the pipeline.
#[tokio::test]
async fn a_replace_without_an_id_takes_the_one_it_replaces() -> anyhow::Result<()> {
    let state = AppState::new();
    state.create_pipeline(idle("p1")?)?;
    let mut body = stamped("ignored", "v2")?;
    body.id = None;

    let new = state.replace_pipeline("p1", body).await?;
    assert_eq!(new.id, "p1");
    assert_eq!(new.config.id.as_deref(), Some("p1"));
    Ok(())
}

/// The reason the endpoint exists. A delete and a create would end the
/// downstream's only input and stop it; a replace hands it to the new config,
/// so it carries on — the same pipeline, never rebuilt — and receives what the
/// new config produces.
#[tokio::test]
async fn the_pipelines_below_a_replaced_one_stay_attached() -> anyhow::Result<()> {
    let state = AppState::new();
    state.create_pipeline(stamped("source", "v1")?)?;
    let child = state.create_pipeline(downstream_of("child", "source")?)?;
    let (tx, mut rx) = mpsc::channel(64);
    child.subscribe(tx);
    assert!(a_batch_stamped(&mut rx, "v1").await.is_some(), "the graph never ran");

    state.replace_pipeline("source", stamped("source", "v2")?).await?;

    assert!(
        a_batch_stamped(&mut rx, "v2").await.is_some(),
        "the downstream never received what the new config produced"
    );
    untouched(&state, &child)?;
    assert_eq!(running(&state, "source")?.downstream_count(), 1);
    Ok(())
}

#[tokio::test]
async fn replacing_an_unknown_pipeline_is_not_found() -> anyhow::Result<()> {
    let state = AppState::new();
    let err = state.replace_pipeline("nope", idle("nope")?).await;
    assert!(
        matches!(err, Err(PipelineError::NotFound(ref id)) if id == "nope"),
        "expected NotFound, got {:?}",
        err.map(|p| p.id.clone())
    );
    assert!(state.get_pipeline_ids().is_empty(), "a replace created a pipeline");
    Ok(())
}

/// Renaming would orphan every downstream that names the old id, so it is a
/// delete and a create, said out loud — not something a replace does quietly.
#[tokio::test]
async fn a_body_naming_another_id_is_refused_before_anything_stops() -> anyhow::Result<()> {
    let state = AppState::new();
    let before = state.create_pipeline(idle("p1")?)?;

    let err = state.replace_pipeline("p1", idle("p2")?).await;
    let Err(PipelineError::InvalidConfig(err)) = err else {
        panic!("expected InvalidConfig, got {:?}", err.map(|p| p.id.clone()));
    };
    assert!(format!("{err:#}").contains("'p2'"), "{err:#}");
    untouched(&state, &before)?;
    assert_eq!(state.get_pipeline_ids(), vec!["p1".to_string()]);
    Ok(())
}

#[tokio::test]
async fn a_pipeline_cannot_be_made_its_own_upstream() -> anyhow::Result<()> {
    let state = AppState::new();
    let before = state.create_pipeline(idle("p1")?)?;

    let err = state.replace_pipeline("p1", downstream_of("p1", "p1")?).await;
    assert!(
        matches!(err, Err(PipelineError::InvalidConfig(_))),
        "expected InvalidConfig"
    );
    untouched(&state, &before)?;
    Ok(())
}

/// A create can never close a cycle — the upstream has to exist first — but a
/// replace can point a pipeline at one of its own descendants. `a → b → c`,
/// and `a` re-pointed at `c`, is a loop its batches would go round forever.
#[tokio::test]
async fn an_upstream_that_would_close_a_cycle_is_refused() -> anyhow::Result<()> {
    let state = AppState::new();
    let a = state.create_pipeline(idle("a")?)?;
    let b = state.create_pipeline(downstream_of("b", "a")?)?;
    let c = state.create_pipeline(downstream_of("c", "b")?)?;

    let err = state.replace_pipeline("a", downstream_of("a", "c")?).await;
    let Err(PipelineError::InvalidConfig(err)) = err else {
        panic!("expected InvalidConfig, got {:?}", err.map(|p| p.id.clone()));
    };
    assert!(format!("{err:#}").contains("cycle"), "{err:#}");
    for pipeline in [&a, &b, &c] {
        untouched(&state, pipeline)?;
    }
    Ok(())
}

/// A pipeline's siblings are not its descendants: re-pointing `b` from `a` to
/// `c`, where both hang off `a`, is an ordinary edit.
#[tokio::test]
async fn re_pointing_at_a_sibling_is_not_a_cycle() -> anyhow::Result<()> {
    let state = AppState::new();
    state.create_pipeline(idle("a")?)?;
    state.create_pipeline(downstream_of("b", "a")?)?;
    state.create_pipeline(downstream_of("c", "a")?)?;

    let b = state.replace_pipeline("b", downstream_of("b", "c")?).await?;
    let upstreams: Vec<&String> = b.config.upstreams();
    assert_eq!(upstreams, ["c"]);
    Ok(())
}

#[tokio::test]
async fn an_unknown_upstream_is_refused_before_anything_stops() -> anyhow::Result<()> {
    let state = AppState::new();
    let before = state.create_pipeline(idle("p1")?)?;

    let err = state.replace_pipeline("p1", downstream_of("p1", "missing")?).await;
    assert!(
        matches!(err, Err(PipelineError::InvalidConfig(_))),
        "expected InvalidConfig"
    );
    untouched(&state, &before)?;
    Ok(())
}

#[tokio::test]
async fn an_unknown_connection_is_refused_before_anything_stops() -> anyhow::Result<()> {
    let state = AppState::new();
    let before = state.create_pipeline(idle("p1")?)?;
    let body = config(json!({
        "id": "p1",
        "inputs": [{ "type": "nats", "connection": "nowhere", "subject": "x" }],
        "outputs": [{ "type": "stdout" }]
    }))?;

    let err = state.replace_pipeline("p1", body).await;
    let Err(PipelineError::InvalidConfig(err)) = err else {
        panic!("expected InvalidConfig, got {:?}", err.map(|p| p.id.clone()));
    };
    assert!(format!("{err:#}").contains("'nowhere'"), "{err:#}");
    untouched(&state, &before)?;
    Ok(())
}

/// What the checks can't see into: a component that refuses its own config.
/// The old pipeline has been stopped by then, so it is built again — and its
/// downstream, which was handed back to it, keeps receiving.
#[tokio::test]
async fn a_config_that_fails_to_build_puts_the_old_one_back() -> anyhow::Result<()> {
    let state = AppState::new();
    let old = state.create_pipeline(stamped("source", "v1")?)?;
    let child = state.create_pipeline(downstream_of("child", "source")?)?;
    let (tx, mut rx) = mpsc::channel(64);
    child.subscribe(tx);
    // a reducer with nothing to compute is refused by its own build
    let broken = config(json!({
        "id": "source",
        "inputs": [{ "type": "dummy", "duration": 0 }],
        "transforms": [{ "type": "reducer", "aggregations": [] }],
        "outputs": [{ "type": "stdout" }]
    }))?;

    let err = state.replace_pipeline("source", broken).await;
    let Err(PipelineError::InvalidConfig(err)) = err else {
        panic!("expected InvalidConfig, got {:?}", err.map(|p| p.id.clone()));
    };
    let message = format!("{err:#}");
    assert!(message.contains("old config"), "{message}");
    assert!(message.contains("aggregation"), "the cause was lost: {message}");

    let restored = running(&state, "source")?;
    assert_eq!(
        serde_json::to_value(&restored.config)?,
        serde_json::to_value(&old.config)?,
        "the old config is not what runs"
    );
    // drain what the old run loop sent before it stopped, then expect more
    while rx.try_recv().is_ok() {}
    assert!(
        a_batch_stamped(&mut rx, "v1").await.is_some(),
        "the downstream lost its upstream when the replace failed"
    );
    untouched(&state, &child)?;
    Ok(())
}

/// A `PUT` of the very config that is running still rebuilds it, which is how
/// a pipeline that has stopped is started again. Here the downstream stops
/// because its upstream was deleted, and comes back once there is one again.
#[tokio::test]
async fn replacing_a_stopped_pipeline_starts_it_again() -> anyhow::Result<()> {
    let state = AppState::new();
    state.create_pipeline(stamped("source", "v1")?)?;
    let child = state.create_pipeline(downstream_of("child", "source")?)?;
    state.delete_pipeline("source")?;
    tokio::time::timeout(Duration::from_secs(5), async {
        while matches!(child.status(), RunStatus::Starting | RunStatus::Running) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;

    state.create_pipeline(stamped("source", "v2")?)?;
    let again = state
        .replace_pipeline("child", downstream_of("child", "source")?)
        .await?;
    let (tx, mut rx) = mpsc::channel(64);
    again.subscribe(tx);
    assert!(
        a_batch_stamped(&mut rx, "v2").await.is_some(),
        "the restarted pipeline never received anything"
    );
    Ok(())
}

/// While the id is out of the map nothing else may take it or change it. The
/// window is made long enough to look into by an output that never gets an
/// answer: the old run loop is stuck inside `emit` and the replace waits for
/// it, up to its grace period.
#[tokio::test]
async fn the_id_is_reserved_while_it_is_replaced() -> anyhow::Result<()> {
    // accepts and never answers, so the request hangs until its own timeout
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    let silent = tokio::spawn(async move {
        let mut held = Vec::new();
        while let Ok((socket, _)) = listener.accept().await {
            held.push(socket);
        }
    });

    let state = Arc::new(AppState::new());
    let stuck = config(json!({
        "id": "p1",
        "inputs": [{ "type": "dummy", "duration": 0 }],
        "outputs": [{ "type": "http", "url": format!("http://{addr}/") }]
    }))?;
    let old = state.create_pipeline(stuck)?;
    // let the first batch reach the output and hang there
    tokio::time::sleep(Duration::from_millis(300)).await;

    let replacing = {
        let state = Arc::clone(&state);
        tokio::spawn(async move { state.replace_pipeline("p1", idle("p1")?).await.map(|p| p.id.clone()).map_err(anyhow::Error::from) })
    };
    tokio::time::timeout(Duration::from_secs(2), async {
        while !old.cancellation_token.is_cancelled() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;

    assert!(
        matches!(state.create_pipeline(idle("p1")?), Err(PipelineError::Replacing(_))),
        "a create took an id that was being replaced"
    );
    assert!(
        matches!(state.delete_pipeline("p1"), Err(PipelineError::Replacing(_))),
        "a delete reached a pipeline that was being replaced"
    );
    assert!(
        matches!(
            state.replace_pipeline("p1", idle("p1")?).await,
            Err(PipelineError::Replacing(_))
        ),
        "two replaces of one pipeline ran at once"
    );
    // a device posting in the gap is told to come back, not that the
    // pipeline it posts to has gone for good
    assert!(
        matches!(
            state.ingest("p1", vec![json!({})], PostMeta::default(), &Credentials::none()),
            Err(PipelineError::Restarting(_))
        ),
        "a post during the replace was not a 503"
    );

    assert_eq!(replacing.await??, "p1");
    // and released afterwards
    state.delete_pipeline("p1")?;
    silent.abort();
    Ok(())
}
