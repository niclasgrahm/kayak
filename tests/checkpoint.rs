//! Checkpoints: what survives a crash, what a restart replays, and what it
//! costs the pipelines that are not crashing. See `docs/durability.md`.
//!
//! A crash is simulated by aborting run-loop tasks where they stand — no
//! savepoint, no `finish`, held acknowledgements dropped unfired — and then
//! opening the state directory afresh, exactly as a new process would. The
//! inputs are a [`ReplayLog`] (a rewindable source standing in for a kafka
//! topic) and, end to end, the `http` input's write-ahead log.

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use kayak::buckets::Buckets;
use kayak::checkpoint::{Checkpoints, PipelineSnapshot, Settings};
use kayak::inputs::ack::{Ack, Delivery};
use kayak::inputs::http::{Credentials, PostMeta};
use kayak::inputs::{InputSource, MessageBatch};
use kayak::outputs::OutputDestination;
use kayak::pipeline::{Edge, Pipeline, PipelineRuntime};
use kayak::state::AppState;
use kayak::testing::{CollectingOutput, Counting, Emitted, ReplayLog, batch, stub_config};
use kayak::transforms::Transform;
use kayak_core::RunStatus;
use kayak_core::config::Config;
use serde_json::{Value, json};
use tokio::sync::{broadcast, mpsc};

const WAIT: Duration = Duration::from_secs(10);

fn settings(dir: &Path) -> Settings {
    Settings {
        dir: dir.to_path_buf(),
        // triggered by hand, so each test decides exactly when a cut happens
        interval: None,
        timeout: Duration::from_secs(5),
    }
}

fn open(dir: &Path) -> Result<Arc<Checkpoints>> {
    Checkpoints::open(settings(dir))
}

/// Open the directory as a *new process* would: once the last one has let go
/// of it. A crashed process is gone at once; an aborted one in this test
/// process may still be finishing a commit on a blocking thread, and the
/// directory's lock is what makes the next one wait for it rather than
/// trample it.
async fn reopen(settings: Settings) -> Result<Arc<Checkpoints>> {
    let deadline = tokio::time::Instant::now() + WAIT;
    loop {
        match Checkpoints::open(settings.clone()) {
            Ok(checkpoints) => return Ok(checkpoints),
            Err(e) if tokio::time::Instant::now() < deadline && format!("{e:#}").contains("in use") => {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            Err(e) => return Err(e),
        }
    }
}

fn shared(config: Config) -> Result<Arc<Pipeline>> {
    Ok(Arc::new(Pipeline::new(config)?))
}

fn config(value: Value) -> Result<Config> {
    Ok(serde_json::from_value(value)?)
}

/// A runtime taking part in `checkpoints`, restored from what they hold for
/// it — what `Pipeline::start` does, for parts built by hand.
fn checkpointed(
    checkpoints: &Arc<Checkpoints>,
    shared: Arc<Pipeline>,
    inputs: Vec<Box<dyn InputSource>>,
    transforms: Vec<Box<dyn Transform>>,
    outputs: Vec<Box<dyn OutputDestination>>,
    buckets: Arc<Buckets>,
    positions: Vec<Option<Value>>,
) -> Result<PipelineRuntime> {
    let (events, _) = broadcast::channel(16);
    let id = shared.id.clone();
    let mut runtime = PipelineRuntime::from_parts(inputs, transforms, outputs, shared, events)?;
    if let Some(snapshot) = checkpoints.restored(&id) {
        runtime.restore(&snapshot);
    }
    Ok(runtime.with_checkpoints(checkpoints, buckets, positions))
}

/// Where input `index` had got to in a pipeline's committed checkpoint.
fn position(snapshot: Option<&PipelineSnapshot>, index: usize) -> Option<Value> {
    snapshot?.inputs.get(index)?.state.clone()
}

fn flat(emitted: &Emitted) -> Vec<Value> {
    emitted.values().into_iter().flatten().collect()
}

fn field(messages: &[Value], name: &str) -> Vec<i64> {
    messages.iter().filter_map(|m| m[name].as_i64()).collect()
}

/// The core promise. A crash after a checkpoint replays exactly what came
/// after it — the outputs see those messages twice (at-least-once), and the
/// state counts every message once (exactly-once): the restarted pipeline's
/// numbering carries on from the checkpoint rather than from zero, and not
/// from where the crashed one had got to either.
#[tokio::test]
async fn a_crash_replays_what_followed_the_checkpoint_and_the_state_counts_it_once() -> Result<()>
{
    let dir = tempfile::tempdir()?;
    let log = ReplayLog::new();
    for n in 0..10 {
        log.push(json!({ "n": n }));
    }

    // the first process: read ten, checkpoint, read five more, die
    let first_output = CollectingOutput::new();
    let first = first_output.emitted();
    {
        let checkpoints = open(dir.path())?;
        let runtime = checkpointed(
            &checkpoints,
            shared(stub_config("p"))?,
            vec![Box::new(log.input(3, None))],
            vec![Box::new(Counting::new())],
            vec![Box::new(first_output)],
            Arc::new(Buckets::new()),
            Vec::new(),
        )?;
        let task = tokio::spawn(runtime.run());
        anyhow::ensure!(first.wait_for(4, WAIT).await, "the first run never read the log");
        let epoch = checkpoints.trigger();
        anyhow::ensure!(
            checkpoints.wait_for("p", epoch, WAIT).await,
            "the checkpoint never committed"
        );
        for n in 10..15 {
            log.push(json!({ "n": n }));
        }
        let deadline = tokio::time::Instant::now() + WAIT;
        while flat(&first).len() < 15 && tokio::time::Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        task.abort();
        let _ = task.await;
    }
    assert_eq!(field(&flat(&first), "seq"), (1..=15).collect::<Vec<_>>());

    // the second process
    let checkpoints = reopen(settings(dir.path())).await?;
    let restored = checkpoints.restored("p");
    assert_eq!(
        position(restored.as_ref(), 0),
        Some(json!({ "offset": 10 })),
        "the checkpoint should hold the position after the first ten"
    );
    let second_output = CollectingOutput::new();
    let second = second_output.emitted();
    let runtime = checkpointed(
        &checkpoints,
        shared(stub_config("p"))?,
        vec![Box::new(log.input(3, position(restored.as_ref(), 0).as_ref()))],
        vec![Box::new(Counting::new())],
        vec![Box::new(second_output)],
        Arc::new(Buckets::new()),
        vec![position(restored.as_ref(), 0)],
    )?;
    let task = tokio::spawn(runtime.run());
    let deadline = tokio::time::Instant::now() + WAIT;
    while flat(&second).len() < 5 && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    task.abort();

    let replayed = flat(&second);
    assert_eq!(field(&replayed, "n"), (10..15).collect::<Vec<_>>(), "exactly what followed the cut");
    assert_eq!(
        field(&replayed, "seq"),
        (11..=15).collect::<Vec<_>>(),
        "the state carried on from the checkpoint: each message counted once"
    );
    // and together the two runs delivered everything
    let all: BTreeSet<i64> = field(&flat(&first), "n").into_iter().chain(field(&replayed, "n")).collect();
    assert_eq!(all, (0..15).collect());
    Ok(())
}

/// An acknowledgement that waits for a checkpoint, counting its firings.
struct Deferring(Arc<AtomicUsize>);

impl Ack for Deferring {
    fn ack(&self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
    fn defers_to_checkpoint(&self) -> bool {
        true
    }
}

/// One delivery whose ack defers, then nothing.
struct OneDeferred(Option<Arc<AtomicUsize>>);

#[async_trait::async_trait]
impl InputSource for OneDeferred {
    async fn next(&mut self) -> Result<Delivery> {
        match self.0.take() {
            Some(count) => Ok(Delivery::with_ack(
                batch(vec![json!({ "n": 1 })]),
                Box::new(Deferring(count)),
            )),
            None => std::future::pending().await,
        }
    }
}

/// Under checkpointing an ack that stands for a position does not fire when
/// its pass ends — the broker would then be ahead of the state — but only once
/// a checkpoint covering it has committed.
#[tokio::test]
async fn an_ack_fires_only_once_a_checkpoint_covering_it_has_committed() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let checkpoints = open(dir.path())?;
    let fired = Arc::new(AtomicUsize::new(0));
    let output = CollectingOutput::new();
    let emitted = output.emitted();
    let runtime = checkpointed(
        &checkpoints,
        shared(stub_config("p"))?,
        vec![Box::new(OneDeferred(Some(Arc::clone(&fired))))],
        vec![],
        vec![Box::new(output)],
        Arc::new(Buckets::new()),
        Vec::new(),
    )?;
    let task = tokio::spawn(runtime.run());
    anyhow::ensure!(emitted.wait_for(1, WAIT).await, "nothing was delivered");
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(fired.load(Ordering::SeqCst), 0, "fired before any checkpoint");

    let epoch = checkpoints.trigger();
    anyhow::ensure!(checkpoints.wait_for("p", epoch, WAIT).await, "never committed");
    assert_eq!(fired.load(Ordering::SeqCst), 1);
    task.abort();
    Ok(())
}

/// An output that cannot flush keeps the checkpoint from completing, and its
/// acks wait — and fire with the next checkpoint that does complete, rather
/// than being lost.
#[tokio::test]
async fn an_output_that_cannot_flush_holds_the_checkpoint_and_the_next_one_covers_it() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let checkpoints = open(dir.path())?;
    let fired = Arc::new(AtomicUsize::new(0));
    let output = CollectingOutput::new();
    let emitted = output.emitted();
    *output.failing_checkpoints().lock().map_err(|_| anyhow!("poisoned"))? = 1;
    let runtime = checkpointed(
        &checkpoints,
        shared(stub_config("p"))?,
        vec![Box::new(OneDeferred(Some(Arc::clone(&fired))))],
        vec![],
        vec![Box::new(output)],
        Arc::new(Buckets::new()),
        Vec::new(),
    )?;
    let task = tokio::spawn(runtime.run());
    anyhow::ensure!(emitted.wait_for(1, WAIT).await, "nothing was delivered");

    let failed = checkpoints.trigger();
    assert!(
        !checkpoints.wait_for("p", failed, Duration::from_millis(300)).await,
        "a checkpoint whose output could not flush must not commit"
    );
    assert_eq!(fired.load(Ordering::SeqCst), 0);

    let next = checkpoints.trigger();
    anyhow::ensure!(checkpoints.wait_for("p", next, WAIT).await, "the next one never committed");
    assert_eq!(fired.load(Ordering::SeqCst), 1, "the held ack fired with the next checkpoint");
    task.abort();
    Ok(())
}

/// Under checkpointing an output that fails holds the pipeline and is retried
/// until it takes the batch — dropping it and then completing a checkpoint
/// past it would be exactly the loss checkpoints exist to prevent. Only the
/// failing output is retried; a sibling that took the batch gets it once.
#[tokio::test]
async fn a_failing_output_is_retried_rather_than_dropped() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let checkpoints = open(dir.path())?;
    let log = ReplayLog::new();
    log.push(json!({ "n": 1 }));
    let flaky = CollectingOutput::failing_emits(2);
    let flaky_emitted = flaky.emitted();
    let steady = CollectingOutput::new();
    let steady_emitted = steady.emitted();
    let runtime = checkpointed(
        &checkpoints,
        shared(stub_config("p"))?,
        vec![Box::new(log.input(1, None))],
        vec![],
        vec![Box::new(steady), Box::new(flaky)],
        Arc::new(Buckets::new()),
        Vec::new(),
    )?;
    let task = tokio::spawn(runtime.run());
    anyhow::ensure!(
        flaky_emitted.wait_for(1, WAIT).await,
        "the failing output never got the batch"
    );
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(flaky_emitted.values(), vec![vec![json!({ "n": 1 })]]);
    assert_eq!(steady_emitted.len(), 1, "the sibling that took it got it once");
    task.abort();
    Ok(())
}

/// The http input's write-ahead log makes a `202` a promise that survives a
/// crash, end to end through the real server: posts acknowledged after the
/// checkpoint are replayed by the next process, the bucket a script counts in
/// is restored to the checkpoint, and so every post is counted exactly once.
#[tokio::test]
async fn posts_acknowledged_before_a_crash_are_replayed_and_counted_once() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = write_counter_config(dir.path())?;
    let state_dir = dir.path().join("state");

    // the first process: five posts, a checkpoint, three more, a crash
    let first = boot(&path, &state_dir).await?;
    let mut seen = subscribe(&first, "ingest")?;
    for n in 1..=5 {
        post(&first, n).await?;
    }
    assert_eq!(collect(&mut seen, 5).await?, (1..=5).collect::<Vec<_>>());
    let checkpoints = first.checkpoints().context("checkpointing is on")?.clone();
    let epoch = checkpoints.trigger();
    anyhow::ensure!(checkpoints.wait_for("ingest", epoch, WAIT).await, "never committed");
    for n in 6..=8 {
        post(&first, n).await?;
    }
    assert_eq!(collect(&mut seen, 3).await?, vec![6, 7, 8]);
    first.abort_pipelines().await;
    drop(first);
    drop(checkpoints);

    // the second process replays the three posts the checkpoint did not
    // cover, counting on from five — and then takes new posts as usual
    let second = boot(&path, &state_dir).await?;
    let mut seen = subscribe(&second, "ingest")?;
    let replayed = collect_messages(&mut seen, 3).await?;
    assert_eq!(field(&replayed, "n"), vec![6, 7, 8]);
    assert_eq!(field(&replayed, "seq"), vec![6, 7, 8]);
    post(&second, 9).await?;
    assert_eq!(collect(&mut seen, 1).await?, vec![9]);
    second.shutdown().await;
    Ok(())
}

/// A graceful stop is a savepoint: the graph stops at a consistent cut, the
/// checkpoint commits, and the next start replays nothing at all — the count
/// simply carries on.
#[tokio::test]
async fn a_shutdown_is_a_savepoint_and_a_restart_replays_nothing() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = write_counter_config(dir.path())?;
    let state_dir = dir.path().join("state");

    let first = boot(&path, &state_dir).await?;
    let mut seen = subscribe(&first, "ingest")?;
    for n in 1..=4 {
        post(&first, n).await?;
    }
    assert_eq!(collect(&mut seen, 4).await?, vec![1, 2, 3, 4]);
    first.shutdown().await;
    drop(first);

    let second = boot(&path, &state_dir).await?;
    let mut seen = subscribe(&second, "ingest")?;
    post(&second, 5).await?;
    let messages = collect_messages(&mut seen, 1).await?;
    assert_eq!(field(&messages, "n"), vec![5], "nothing was replayed ahead of the new post");
    assert_eq!(field(&messages, "seq"), vec![5], "the count carried on");
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(seen.try_recv().is_err(), "a savepoint restart replayed something");
    second.shutdown().await;
    Ok(())
}

/// A revert under checkpointing stops the graph at a savepoint and rebuilds it
/// from that checkpoint, so state survives the rebuild exactly — no message
/// lost to the teardown and none counted twice.
#[tokio::test]
async fn a_revert_carries_state_across_the_rebuild() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = write_counter_config(dir.path())?;
    let state = boot(&path, &dir.path().join("state")).await?;
    let mut seen = subscribe(&state, "ingest")?;
    for n in 1..=3 {
        post(&state, n).await?;
    }
    assert_eq!(collect(&mut seen, 3).await?, vec![1, 2, 3]);
    state.revert().await?;
    let mut seen = subscribe(&state, "ingest")?;
    post(&state, 4).await?;
    let messages = collect_messages(&mut seen, 1).await?;
    assert_eq!(field(&messages, "n"), vec![4]);
    assert_eq!(field(&messages, "seq"), vec![4]);
    state.shutdown().await;
    Ok(())
}

/// A replace checkpoints first, and the new config's unchanged components —
/// here the `http` input and the counting script, at the same positions —
/// restore from that checkpoint. So the replace replays nothing and counts
/// nothing twice: the next post is the fourth message, counted fourth, and
/// carries what the added transform stamps on it. Without the checkpoint the
/// input would replay its logged posts into the bucket a second time.
#[tokio::test]
async fn a_replace_resumes_the_unchanged_components_exactly() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = write_counter_config(dir.path())?;
    let state = boot(&path, &dir.path().join("state")).await?;
    let mut seen = subscribe(&state, "ingest")?;
    for n in 1..=3 {
        post(&state, n).await?;
    }
    assert_eq!(collect(&mut seen, 3).await?, vec![1, 2, 3]);

    let mut config = serde_json::to_value(
        &state
            .pipeline_handles()
            .into_iter()
            .find(|p| p.id == "ingest")
            .context("no pipeline 'ingest'")?
            .config,
    )?;
    config["transforms"]
        .as_array_mut()
        .context("transforms is a list")?
        .push(json!({
            "type": "map",
            "mappings": [{
                "type": "constant",
                "value": { "type": "text", "value": "v2" },
                "as": "version"
            }]
        }));
    state.replace_pipeline("ingest", serde_json::from_value(config)?).await?;

    let mut seen = subscribe(&state, "ingest")?;
    post(&state, 4).await?;
    let messages = collect_messages(&mut seen, 1).await?;
    assert_eq!(field(&messages, "n"), vec![4], "the replace replayed: {messages:?}");
    assert_eq!(field(&messages, "seq"), vec![4], "counted twice or lost: {messages:?}");
    assert_eq!(messages[0]["version"], json!("v2"), "the new config is not what runs");
    state.shutdown().await;
    Ok(())
}

/// The same promise with posts arriving *while* the pipeline is replaced. The
/// old config stops at a checkpoint barrier rather than being cancelled after
/// one, so there is no window in which it processes what the new config then
/// replays: every message comes out exactly once, numbered as the n-th — and
/// to the same subscriber, which the replace handed from one to the other.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn posts_during_a_replace_are_counted_exactly_once() -> Result<()> {
    const POSTS: i64 = 300;
    let dir = tempfile::tempdir()?;
    let path = write_counter_config(dir.path())?;
    let state = Arc::new(boot(&path, &dir.path().join("state")).await?);
    let mut seen = subscribe(&state, "ingest")?;

    let poster = {
        let state = Arc::clone(&state);
        tokio::spawn(async move {
            for n in 0..POSTS {
                // a 503 in the gap is the endpoint saying "again"
                while state
                    .ingest_durably("ingest", vec![json!({ "n": n })], PostMeta::default(), &Credentials::none())
                    .await
                    .is_err()
                {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            }
        })
    };
    // mid-stream: let some through first
    let first = collect_messages(&mut seen, 50).await?;
    let mut config = serde_json::to_value(
        &state
            .pipeline_handles()
            .into_iter()
            .find(|p| p.id == "ingest")
            .context("no pipeline 'ingest'")?
            .config,
    )?;
    config["transforms"]
        .as_array_mut()
        .context("transforms is a list")?
        .push(json!({
            "type": "map",
            "mappings": [{ "type": "constant", "value": { "type": "text", "value": "v2" }, "as": "version" }]
        }));
    state.replace_pipeline("ingest", serde_json::from_value(config)?).await?;
    poster.await?;

    let mut messages = first;
    while i64::try_from(messages.len())? < POSTS {
        messages.extend(collect_messages(&mut seen, 1).await?);
    }
    assert_counted_once(&messages, "across the replace");
    let ns: Vec<i64> = field(&messages, "n");
    let distinct: BTreeSet<i64> = ns.iter().copied().collect();
    assert_eq!(ns.len(), distinct.len(), "a message came out twice");
    assert_eq!(distinct, (0..POSTS).collect(), "a message was lost");
    state.shutdown().await;
    Ok(())
}

/// Deleting a pipeline takes its checkpoint and its logged posts with it: a
/// pipeline created later under the same id starts from nothing rather than
/// from a stranger's count.
#[tokio::test]
async fn a_deleted_pipeline_leaves_nothing_for_its_successor() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = write_counter_config(dir.path())?;
    let state = boot(&path, &dir.path().join("state")).await?;
    let mut seen = subscribe(&state, "ingest")?;
    post(&state, 1).await?;
    assert_eq!(collect(&mut seen, 1).await?, vec![1]);
    let checkpoints = state.checkpoints().context("checkpointing is on")?.clone();
    let epoch = checkpoints.trigger();
    anyhow::ensure!(checkpoints.wait_for("ingest", epoch, WAIT).await, "never committed");

    state.delete_pipeline("ingest")?;
    // the forget is committed by the writer like any other change
    let deadline = tokio::time::Instant::now() + WAIT;
    while checkpoints.restored("ingest").is_some() && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(checkpoints.restored("ingest").is_none(), "the checkpoint outlived the pipeline");
    assert!(
        !kayak::inputs::wal::dir_for(checkpoints.dir(), "ingest").exists(),
        "the logged posts outlived the pipeline"
    );
    state.shutdown().await;
    Ok(())
}

/// A bucket the config no longer declares is reported as undeclared — not as
/// one whose declaration changed, which is a different fact and the only one
/// worth a warning.
#[tokio::test]
async fn an_undeclared_bucket_is_not_reported_as_a_changed_one() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = write_counter_config(dir.path())?;
    let state = boot(&path, &dir.path().join("state")).await?;
    let mut seen = subscribe(&state, "ingest")?;
    post(&state, 1).await?;
    assert_eq!(collect(&mut seen, 1).await?, vec![1]);
    let checkpoints = state.checkpoints().context("checkpointing is on")?.clone();
    let epoch = checkpoints.trigger();
    anyhow::ensure!(checkpoints.wait_for("ingest", epoch, WAIT).await, "never committed");
    anyhow::ensure!(checkpoints.manifest().buckets.contains_key("totals"), "the bucket was not checkpointed");

    let report = checkpoints.restore_buckets(&Buckets::new());
    assert_eq!(report.undeclared, vec!["totals".to_string()]);
    assert!(report.changed.is_empty(), "{report:?}");

    let changed: kayak_core::state::StateBuckets =
        serde_json::from_value(json!({ "totals": { "max_keys": 5 } }))?;
    let report = checkpoints.restore_buckets(&Buckets::from_config(&changed));
    assert_eq!(report.changed, vec!["totals".to_string()]);
    assert!(report.undeclared.is_empty(), "{report:?}");
    state.shutdown().await;
    Ok(())
}

/// Loading a config drops the checkpoint of every pipeline and bucket it no
/// longer names — entries and files both — and keeps the ones it does.
#[tokio::test]
async fn a_config_load_drops_the_checkpoints_of_what_it_no_longer_names() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = write_counter_config(dir.path())?;
    let state = boot(&path, &dir.path().join("state")).await?;
    let mut seen = subscribe(&state, "ingest")?;
    post(&state, 1).await?;
    assert_eq!(collect(&mut seen, 1).await?, vec![1]);
    let checkpoints = state.checkpoints().context("checkpointing is on")?.clone();
    let epoch = checkpoints.trigger();
    anyhow::ensure!(checkpoints.wait_for("ingest", epoch, WAIT).await, "never committed");

    // the pipeline and its bucket are renamed, and a stateless one stays
    let config = json!({
        "state": { "sums": { "max_keys": 10 } },
        "pipelines": [{
            "id": "renamed",
            "inputs": [{ "type": "http" }],
            "transforms": [{ "type": "script", "source": { "type": "inline", "code": "emit(msg);" } }],
            "state": { "bucket": "sums" }
        }]
    });
    std::fs::write(&path, serde_json::to_vec_pretty(&config)?)?;
    state.revert().await?;

    let gone = |m: &kayak::checkpoint::Manifest| {
        !m.pipelines.contains_key("ingest") && !m.buckets.contains_key("totals")
    };
    let deadline = tokio::time::Instant::now() + WAIT;
    while !gone(&checkpoints.manifest()) && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    let manifest = checkpoints.manifest();
    assert!(gone(&manifest), "stale entries outlived the config: {manifest:?}");

    // and the files went with them: what is on disk is what the manifest names
    let epoch = checkpoints.trigger();
    anyhow::ensure!(checkpoints.wait_for("renamed", epoch, WAIT).await, "never committed");
    let manifest = checkpoints.manifest();
    assert!(manifest.pipelines.contains_key("renamed"), "{manifest:?}");
    for (sub, entries) in [("pipelines", &manifest.pipelines), ("buckets", &manifest.buckets)] {
        let named: BTreeSet<String> = entries.values().filter_map(|e| e.file.clone()).collect();
        let on_disk: BTreeSet<String> = std::fs::read_dir(dir.path().join("state").join(sub))?
            .filter_map(|e| e.ok()?.file_name().into_string().ok())
            .collect();
        assert_eq!(on_disk, named, "{sub}/ holds files the manifest does not name");
    }
    state.shutdown().await;
    Ok(())
}

/// A pipeline fed by two others aligns their barriers: what one upstream sent
/// after its barrier waits until the other's has arrived too, so the
/// downstream's snapshot holds exactly what both upstreams counted as sent —
/// and its region commits only once all three have submitted.
#[tokio::test]
async fn a_pipeline_fed_by_two_others_commits_with_them() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let checkpoints = open(dir.path())?;
    let (log_a, log_b) = (ReplayLog::new(), ReplayLog::new());
    log_a.push(json!({ "from": "a" }));
    log_b.push(json!({ "from": "b" }));

    let a = shared(stub_config("a"))?;
    let b = shared(stub_config("b"))?;
    let c = shared(config(json!({
        "id": "c",
        "inputs": [
            { "type": "pipeline", "upstream": "a" },
            { "type": "pipeline", "upstream": "b" }
        ],
        "outputs": [{ "type": "stdout" }]
    }))?)?;
    let edge = |upstream: &Arc<Pipeline>| -> Box<dyn InputSource> {
        let (tx, rx) = mpsc::channel::<Edge>(16);
        upstream.subscribe_edges(tx);
        Box::new(kayak::inputs::pipeline::PipelineInput {
            upstream: upstream.id.clone(),
            envelope: kayak::inputs::envelope::Envelope::none(),
            rx,
        })
    };
    let c_inputs = vec![edge(&a), edge(&b)];
    let output = CollectingOutput::new();
    let emitted = output.emitted();
    let buckets = Arc::new(Buckets::new());
    let tasks = vec![
        tokio::spawn(
            checkpointed(&checkpoints, Arc::clone(&a), vec![Box::new(log_a.input(1, None))], vec![], vec![], Arc::clone(&buckets), Vec::new())?.run(),
        ),
        tokio::spawn(
            checkpointed(&checkpoints, Arc::clone(&b), vec![Box::new(log_b.input(1, None))], vec![], vec![], Arc::clone(&buckets), Vec::new())?.run(),
        ),
        tokio::spawn(
            checkpointed(&checkpoints, Arc::clone(&c), c_inputs, vec![Box::new(Counting::new())], vec![Box::new(output)], Arc::clone(&buckets), Vec::new())?.run(),
        ),
    ];
    anyhow::ensure!(emitted.wait_for(2, WAIT).await, "c never heard from both upstreams");

    let epoch = checkpoints.trigger();
    for id in ["a", "b", "c"] {
        anyhow::ensure!(checkpoints.wait_for(id, epoch, WAIT).await, "{id} never committed");
    }
    let snapshot = checkpoints.restored("c").context("c has a checkpoint")?;
    assert_eq!(
        snapshot.transforms.first().and_then(|t| t.state.clone()),
        Some(json!({ "count": 2 })),
        "c's snapshot holds both messages its upstreams counted as sent"
    );
    for task in tasks {
        task.abort();
    }
    Ok(())
}

/// Two pipelines bound to one bucket meet at the barrier before it is copied,
/// so neither writes past the cut while the other is being snapshotted — and
/// the wait does not deadlock when one writer is downstream of the other.
#[tokio::test]
async fn writers_of_one_bucket_meet_at_the_barrier_without_deadlocking() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let checkpoints = open(dir.path())?;
    let declared: kayak_core::state::StateBuckets =
        serde_json::from_value(json!({ "shared": {} }))?;
    let buckets = Arc::new(Buckets::from_config(&declared));
    buckets.remember("shared", "k", vec![("v".to_string(), json!(1))]);

    let upstream = shared(config(json!({
        "id": "up",
        "inputs": [{ "type": "dummy", "duration": 1 }],
        "state": { "bucket": "shared" }
    }))?)?;
    let downstream = shared(config(json!({
        "id": "down",
        "inputs": [{ "type": "pipeline", "upstream": "up" }],
        "state": { "bucket": "shared" }
    }))?)?;
    let (tx, rx) = mpsc::channel::<Edge>(16);
    upstream.subscribe_edges(tx);
    let log = ReplayLog::new();
    log.push(json!({ "n": 1 }));
    let tasks = vec![
        tokio::spawn(
            checkpointed(&checkpoints, Arc::clone(&upstream), vec![Box::new(log.input(1, None))], vec![], vec![], Arc::clone(&buckets), Vec::new())?.run(),
        ),
        tokio::spawn(
            checkpointed(
                &checkpoints,
                Arc::clone(&downstream),
                vec![Box::new(kayak::inputs::pipeline::PipelineInput {
                    upstream: "up".into(),
                    envelope: kayak::inputs::envelope::Envelope::none(),
                    rx,
                })],
                vec![],
                vec![],
                Arc::clone(&buckets),
                Vec::new(),
            )?
            .run(),
        ),
    ];

    let epoch = checkpoints.trigger();
    for id in ["up", "down"] {
        anyhow::ensure!(checkpoints.wait_for(id, epoch, WAIT).await, "{id} never committed");
    }
    // the bucket went into the checkpoint, and restores into a fresh set
    let fresh = Buckets::from_config(&declared);
    checkpoints.restore_buckets(&fresh);
    assert_eq!(fresh.recall("shared", "k", &["v".to_string()]), vec![Some(json!(1))]);
    for task in tasks {
        task.abort();
    }
    Ok(())
}

/// A savepoint's barrier stops the run loop once it has passed it, with each
/// output finished exactly once — before the snapshot, so a part is uploaded
/// rather than both snapshotted and uploaded — and the pipeline reported as
/// stopped, not failed.
#[tokio::test]
async fn a_savepoint_stops_the_run_loop_with_its_outputs_finished_once() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let checkpoints = open(dir.path())?;
    let log = ReplayLog::new();
    let output = CollectingOutput::new();
    let finished = output.finish_calls();
    let pipeline = shared(stub_config("p"))?;
    let runtime = checkpointed(
        &checkpoints,
        Arc::clone(&pipeline),
        vec![Box::new(log.input(1, None))],
        vec![],
        vec![Box::new(output)],
        Arc::new(Buckets::new()),
        Vec::new(),
    )?;
    let task = tokio::spawn(runtime.run());
    // until the run loop is running, a savepoint would be answered from init
    let deadline = tokio::time::Instant::now() + WAIT;
    while pipeline.status() != RunStatus::Running && tokio::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert!(checkpoints.savepoint(WAIT).await, "the savepoint never completed");
    tokio::time::timeout(WAIT, task).await??.context("the run loop failed")?;
    assert_eq!(*finished.lock().map_err(|_| anyhow!("poisoned"))?, 1);
    assert_eq!(pipeline.status(), RunStatus::Stopped);
    Ok(())
}

/// A pipeline still waiting for an output to come up has read nothing, and
/// still answers its checkpoints — with the state it was restored with — so a
/// down database neither stalls its region's checkpoints nor makes a shutdown
/// wait out the whole grace period on it.
#[tokio::test]
async fn a_pipeline_waiting_for_its_output_still_takes_its_checkpoints() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let checkpoints = open(dir.path())?;
    let log = ReplayLog::new();
    let runtime = checkpointed(
        &checkpoints,
        shared(stub_config("p"))?,
        vec![Box::new(log.input(1, None))],
        vec![],
        vec![Box::new(CollectingOutput::failing_init(usize::MAX))],
        Arc::new(Buckets::new()),
        vec![Some(json!({ "offset": 7 }))],
    )?;
    let task = tokio::spawn(runtime.run());
    tokio::time::sleep(Duration::from_millis(20)).await;
    let epoch = checkpoints.trigger();
    anyhow::ensure!(checkpoints.wait_for("p", epoch, WAIT).await, "never committed");
    assert_eq!(
        position(checkpoints.restored("p").as_ref(), 0),
        Some(json!({ "offset": 7 })),
        "the restored position is carried, not dropped"
    );
    task.abort();
    Ok(())
}

// ---- the end-to-end setup ----

/// A config with one pipeline: an `http` input, a script that numbers every
/// message in a bucket, no outputs (the test subscribes to the batches).
fn write_counter_config(dir: &Path) -> Result<std::path::PathBuf> {
    let path = dir.join("config.json");
    let code = "let s = recall(\"total\");\n\
                let n = if s == () { 0 } else { s.n };\n\
                n += 1;\n\
                remember(\"total\", #{ n: n });\n\
                msg.seq = n;\n\
                emit(msg);";
    let config = json!({
        "state": { "totals": { "max_keys": 10 } },
        "pipelines": [{
            "id": "ingest",
            "inputs": [{ "type": "http" }],
            "transforms": [{ "type": "script", "source": { "type": "inline", "code": code } }],
            "state": { "bucket": "totals" }
        }]
    });
    std::fs::write(&path, serde_json::to_vec_pretty(&config)?)?;
    Ok(path)
}

async fn boot(path: &Path, state_dir: &Path) -> Result<AppState> {
    AppState::from_config_with_checkpoints(
        path,
        Arc::new(kayak::secrets::EnvStore),
        None,
        None,
        Arc::new(kayak::history::History::disabled()),
        Some(reopen(settings(state_dir)).await?),
    )
}

fn subscribe(state: &AppState, id: &str) -> Result<mpsc::Receiver<Arc<MessageBatch>>> {
    let (tx, rx) = mpsc::channel(64);
    let pipelines = state.pipeline_handles();
    let pipeline = pipelines
        .iter()
        .find(|p| p.id == id)
        .ok_or_else(|| anyhow!("no pipeline '{id}'"))?;
    pipeline.subscribe(tx);
    Ok(rx)
}

async fn post(state: &AppState, n: i64) -> Result<()> {
    state
        .ingest_durably("ingest", vec![json!({ "n": n })], PostMeta::default(), &Credentials::none())
        .await?;
    Ok(())
}

async fn collect_messages(rx: &mut mpsc::Receiver<Arc<MessageBatch>>, count: usize) -> Result<Vec<Value>> {
    let mut out = Vec::new();
    while out.len() < count {
        let batch = tokio::time::timeout(WAIT, rx.recv())
            .await
            .context("timed out waiting for messages")?
            .ok_or_else(|| anyhow!("the pipeline stopped"))?;
        out.extend(batch.iter().map(|m| (**m).clone()));
    }
    Ok(out)
}

/// The `seq` numbers of the next `count` messages.
async fn collect(rx: &mut mpsc::Receiver<Arc<MessageBatch>>, count: usize) -> Result<Vec<i64>> {
    Ok(field(&collect_messages(rx, count).await?, "seq"))
}

// ---- chaos: crash at arbitrary points, many times ----

/// A cheap deterministic sequence, so a failing chaos run can be reproduced.
struct Lcg(u64);

impl Lcg {
    fn next(&mut self, bound: u64) -> u64 {
        self.0 = self.0.wrapping_mul(6_364_136_223_846_793_005).wrapping_add(1_442_695_040_888_963_407);
        (self.0 >> 33) % bound.max(1)
    }
}

/// The invariant that catches both failure modes at once. The log's order is
/// fixed, so with exactly-once state message `n` is *always* the `n+1`th one
/// counted — in the first run, in a replay, after any number of crashes. A
/// lost message shows up as a gap in what was delivered; a message counted
/// twice shifts every `seq` after it.
fn assert_counted_once(messages: &[Value], stage: &str) {
    for message in messages {
        let (Some(n), Some(seq)) = (message["n"].as_i64(), message["seq"].as_i64()) else {
            panic!("{stage}: a message without n or seq: {message}");
        };
        assert_eq!(seq, n + 1, "{stage}: message {n} was counted as the {seq}th");
    }
}

/// Thirty crashes at arbitrary moments — mid-batch, mid-checkpoint, between a
/// commit and its acks — against a graph of two pipelines with a timer
/// checkpointing every few milliseconds. After each crash a fresh coordinator
/// opens the directory as a new process would. Every message is delivered,
/// and every count, upstream and downstream, is exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn thirty_crashes_lose_nothing_and_count_nothing_twice() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let log = ReplayLog::new();
    let mut rng = Lcg(0x5eed);
    let mut pushed = 0i64;
    let mut delivered: BTreeSet<i64> = BTreeSet::new();
    let total = 600;

    let upstream_config = stub_config("a");
    let downstream_config = config(json!({
        "id": "b",
        "inputs": [{ "type": "pipeline", "upstream": "a" }],
        "outputs": [{ "type": "stdout" }]
    }))?;

    for round in 0..31 {
        // a new process
        let checkpoints = reopen(Settings {
            dir: dir.path().to_path_buf(),
            interval: Some(Duration::from_millis(3)),
            timeout: Duration::from_secs(5),
        })
        .await?;
        let a = shared(upstream_config.clone())?;
        let b = shared(downstream_config.clone())?;
        let (tx, rx) = mpsc::channel::<Edge>(8);
        a.subscribe_edges(tx);
        let restored = checkpoints.restored("a");
        let start = position(restored.as_ref(), 0);
        let output = CollectingOutput::new();
        let emitted = output.emitted();
        let buckets = Arc::new(Buckets::new());
        let batch_size = 1 + usize::try_from(rng.next(5)).unwrap_or(0);
        // both join before either runs, as `AppState` builds them: a pipeline
        // joins before it subscribes to anything
        let a_runtime = checkpointed(
            &checkpoints,
            Arc::clone(&a),
            vec![Box::new(log.input(batch_size, start.as_ref()))],
            vec![Box::new(Counting::new())],
            vec![],
            Arc::clone(&buckets),
            vec![start.clone()],
        )?;
        let b_runtime = checkpointed(
            &checkpoints,
            Arc::clone(&b),
            vec![Box::new(kayak::inputs::pipeline::PipelineInput {
                upstream: "a".into(),
                envelope: kayak::inputs::envelope::Envelope::none(),
                rx,
            })],
            vec![Box::new(RenameSeq), Box::new(Counting::new())],
            vec![Box::new(output)],
            Arc::clone(&buckets),
            Vec::new(),
        )?;
        let tasks = vec![tokio::spawn(a_runtime.run()), tokio::spawn(b_runtime.run())];

        if round == 30 {
            // the last process runs until everything has come through
            while pushed < total {
                log.push(json!({ "n": pushed }));
                pushed += 1;
            }
            let deadline = tokio::time::Instant::now() + WAIT;
            loop {
                let seen: BTreeSet<i64> = delivered
                    .iter()
                    .copied()
                    .chain(field(&flat(&emitted), "n"))
                    .collect();
                if seen.len() == usize::try_from(total).unwrap_or(0) || tokio::time::Instant::now() > deadline {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        } else {
            // push some, let it run a little, crash
            for _ in 0..rng.next(40) {
                if pushed < total {
                    log.push(json!({ "n": pushed }));
                    pushed += 1;
                }
            }
            tokio::time::sleep(Duration::from_millis(rng.next(25))).await;
        }
        for task in &tasks {
            task.abort();
        }
        for task in tasks {
            let _ = task.await;
        }
        drop(checkpoints);

        let messages = flat(&emitted);
        assert_counted_once(&messages, &format!("round {round}, downstream"));
        // upstream's count, carried through as `up`
        for message in &messages {
            assert_eq!(
                message["up"].as_i64(),
                message["n"].as_i64().map(|n| n + 1),
                "round {round}: upstream counted {message} wrongly"
            );
        }
        delivered.extend(field(&messages, "n"));
    }
    assert_eq!(delivered, (0..total).collect::<BTreeSet<_>>(), "something was never delivered");
    Ok(())
}

/// Moves the upstream's `seq` aside to `up`, so the downstream's own count
/// can be checked beside it.
struct RenameSeq;

#[async_trait::async_trait]
impl Transform for RenameSeq {
    async fn apply(&mut self, message_batch: Arc<MessageBatch>) -> Result<Vec<Arc<MessageBatch>>> {
        Ok(vec![Arc::new(
            message_batch
                .iter()
                .map(|m| {
                    let mut m = (**m).clone();
                    if let Some(object) = m.as_object_mut()
                        && let Some(seq) = object.remove("seq")
                    {
                        object.insert("up".to_string(), seq);
                    }
                    Arc::new(m)
                })
                .collect(),
        )])
    }
}

/// The same, end to end through the server: every post acknowledged before
/// any of ten crashes is counted exactly once by a script keeping its count in
/// a bucket — the write-ahead log, the bucket's checkpoint and the replay all
/// agreeing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn acknowledged_posts_survive_ten_crashes_counted_once() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let path = write_counter_config(dir.path())?;
    let state_dir = dir.path().join("state");
    let mut rng = Lcg(0xfeed);
    let mut acknowledged = 0i64;
    let mut seen: BTreeSet<i64> = BTreeSet::new();

    for round in 0..11 {
        let state = boot(&path, &state_dir).await?;
        let checkpoints = state.checkpoints().context("checkpointing is on")?.clone();
        let mut rx = subscribe(&state, "ingest")?;
        let posts = if round == 10 { 0 } else { 1 + rng.next(15) };
        let checkpoint_after = rng.next(posts + 1);
        for i in 0..posts {
            acknowledged += 1;
            post(&state, acknowledged).await?;
            if i == checkpoint_after {
                checkpoints.trigger();
            }
        }
        tokio::time::sleep(Duration::from_millis(rng.next(20))).await;
        if round == 10 {
            // the last process: wait until every acknowledged post is through
            let deadline = tokio::time::Instant::now() + WAIT;
            while seen.len() < usize::try_from(acknowledged).unwrap_or(0)
                && tokio::time::Instant::now() < deadline
            {
                while let Ok(batch) = rx.try_recv() {
                    for message in batch.iter() {
                        assert_eq!(message["seq"], message["n"], "final round: {message}");
                        seen.extend(message["n"].as_i64());
                    }
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            state.shutdown().await;
        } else {
            state.abort_pipelines().await;
        }
        while let Ok(batch) = rx.try_recv() {
            for message in batch.iter() {
                assert_eq!(message["seq"], message["n"], "round {round}: {message}");
                seen.extend(message["n"].as_i64());
            }
        }
        drop(state);
        drop(checkpoints);
    }
    assert_eq!(seen, (1..=acknowledged).collect::<BTreeSet<_>>(), "an acknowledged post was lost");
    Ok(())
}

// ---- barriers through the input layers ----

fn edge_input(id: &str) -> (mpsc::Sender<Edge>, Box<dyn InputSource>) {
    let (tx, rx) = mpsc::channel::<Edge>(16);
    (
        tx,
        Box::new(kayak::inputs::pipeline::PipelineInput {
            upstream: id.to_string(),
            envelope: kayak::inputs::envelope::Envelope::none(),
            rx,
        }),
    )
}

fn barrier(epoch: u64) -> Edge {
    Edge::Barrier(kayak::inputs::ack::Barrier { epoch, stop: false })
}

/// What one delivery was, for comparing sequences.
fn describe(delivery: &Delivery) -> String {
    match delivery.barrier() {
        Some(b) => format!("barrier {}", b.epoch),
        None => delivery
            .iter()
            .map(|m| m["from"].as_str().unwrap_or("?").to_string())
            .collect::<Vec<_>>()
            .join(","),
    }
}

/// Alignment: once a barrier has come through one upstream, nothing that
/// upstream sent after it reaches the run loop until the barrier has come
/// through the other too — so the barrier the run loop sees has everything
/// both upstreams sent before theirs ahead of it, and nothing after.
#[tokio::test]
async fn a_merged_input_aligns_barriers_from_its_upstreams() -> Result<()> {
    let (a_tx, a) = edge_input("a");
    let (b_tx, b) = edge_input("b");
    a_tx.send(barrier(1)).await?;
    a_tx.send(Edge::Batch(batch(vec![json!({ "from": "a-after" })]))).await?;
    b_tx.send(Edge::Batch(batch(vec![json!({ "from": "b-before" })]))).await?;
    b_tx.send(barrier(1)).await?;

    let (events, _) = broadcast::channel(4);
    let mut merged = kayak::inputs::merge(vec![a, b], "c".into(), events)?;
    assert!(merged.carries_barriers());
    let mut seen = Vec::new();
    for _ in 0..3 {
        seen.push(describe(&tokio::time::timeout(WAIT, merged.next()).await??));
    }
    assert_eq!(seen, vec!["b-before", "barrier 1", "a-after"]);
    Ok(())
}

/// An upstream that skipped an epoch never delivers it, so a later barrier
/// abandons the one being aligned rather than waiting on it forever.
#[tokio::test]
async fn a_later_barrier_abandons_an_earlier_one_instead_of_deadlocking() -> Result<()> {
    let (a_tx, a) = edge_input("a");
    let (b_tx, b) = edge_input("b");
    a_tx.send(barrier(1)).await?;
    a_tx.send(barrier(2)).await?;
    b_tx.send(barrier(2)).await?;
    let (events, _) = broadcast::channel(4);
    let mut merged = kayak::inputs::merge(vec![a, b], "c".into(), events)?;
    let first = tokio::time::timeout(WAIT, merged.next()).await??;
    assert_eq!(describe(&first), "barrier 2");
    Ok(())
}

/// An input buffer holding messages when a barrier arrives hands them on
/// first: the upstream counted them as delivered when it sent them, so they
/// belong inside the snapshot the barrier asks for.
#[tokio::test]
async fn an_input_buffer_hands_on_what_it_holds_ahead_of_a_barrier() -> Result<()> {
    let (tx, input) = edge_input("a");
    tx.send(Edge::Batch(batch(vec![json!({ "from": "1" })]))).await?;
    tx.send(Edge::Batch(batch(vec![json!({ "from": "2" })]))).await?;
    tx.send(barrier(1)).await?;
    let mut buffered = kayak::inputs::Buffered::new(input, kayak::inputs::BufferKind::Static { size: 10 });
    assert!(buffered.carries_barriers());
    let held = tokio::time::timeout(WAIT, buffered.next()).await??;
    assert_eq!(describe(&held), "1,2");
    let next = tokio::time::timeout(WAIT, buffered.next()).await??;
    assert_eq!(describe(&next), "barrier 1");
    Ok(())
}

/// The run loop drops the input's `next()` whenever something else wins its
/// `select!` — a checkpoint trigger, a transform waking. An input buffer used
/// to keep what it was gathering in that future's locals, so a message it had
/// taken but not yet handed on was simply gone. It is kept on the buffer now.
#[tokio::test]
async fn an_input_buffer_loses_nothing_when_its_next_is_cancelled() -> Result<()> {
    let (tx, input) = edge_input("a");
    let mut buffered = kayak::inputs::Buffered::new(input, kayak::inputs::BufferKind::Static { size: 2 });
    tx.send(Edge::Batch(batch(vec![json!({ "from": "1" })]))).await?;
    // taken off the channel, then the call is abandoned before the batch fills
    assert!(
        tokio::time::timeout(Duration::from_millis(50), buffered.next()).await.is_err(),
        "a batch of one came out of a buffer of two"
    );
    tx.send(Edge::Batch(batch(vec![json!({ "from": "2" })]))).await?;
    let out = tokio::time::timeout(WAIT, buffered.next()).await??;
    assert_eq!(describe(&out), "1,2", "the message held across the cancelled call survived");
    Ok(())
}

// ---- the coordinator's rules ----

/// A new process carries the epochs on from the last one rather than starting
/// again at one: snapshot files are named by epoch, and reusing a number would
/// let a new snapshot overwrite one the committed manifest still names.
#[tokio::test]
async fn a_new_process_never_reuses_an_epoch() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let committed = {
        let checkpoints = open(dir.path())?;
        let runtime = checkpointed(
            &checkpoints,
            shared(stub_config("p"))?,
            vec![Box::new(ReplayLog::new().input(1, None))],
            vec![Box::new(Counting::new())],
            vec![],
            Arc::new(Buckets::new()),
            Vec::new(),
        )?;
        let task = tokio::spawn(runtime.run());
        let mut epoch = 0;
        for _ in 0..3 {
            epoch = checkpoints.trigger();
            anyhow::ensure!(checkpoints.wait_for("p", epoch, WAIT).await, "never committed");
        }
        task.abort();
        let _ = task.await;
        epoch
    };
    let checkpoints = reopen(settings(dir.path())).await?;
    assert!(checkpoints.trigger() > committed);
    Ok(())
}

/// A pipeline that stops without a savepoint — crashed, or its input died —
/// holds its region's checkpoints, as a failed task holds a Flink job: its
/// upstream committing past batches it never snapshotted would lose them on
/// the next restart. Rebuilt under its id, it lets them go again.
#[tokio::test]
async fn a_dead_pipeline_holds_its_regions_checkpoints_until_it_is_rebuilt() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let checkpoints = open(dir.path())?;
    let up = shared(stub_config("up"))?;
    let down_config = config(json!({
        "id": "down",
        "inputs": [{ "type": "pipeline", "upstream": "up" }]
    }))?;
    let buckets = Arc::new(Buckets::new());
    let (tx, rx) = mpsc::channel::<Edge>(16);
    up.subscribe_edges(tx);
    let up_runtime = checkpointed(&checkpoints, Arc::clone(&up), vec![Box::new(ReplayLog::new().input(1, None))], vec![], vec![], Arc::clone(&buckets), Vec::new())?;
    let down_runtime = checkpointed(
        &checkpoints,
        shared(down_config.clone())?,
        vec![Box::new(kayak::inputs::pipeline::PipelineInput {
            upstream: "up".into(),
            envelope: kayak::inputs::envelope::Envelope::none(),
            rx,
        })],
        vec![],
        vec![],
        Arc::clone(&buckets),
        Vec::new(),
    )?;
    let up_task = tokio::spawn(up_runtime.run());
    let down_task = tokio::spawn(down_runtime.run());
    let epoch = checkpoints.trigger();
    anyhow::ensure!(checkpoints.wait_for("up", epoch, WAIT).await, "never committed");

    // the downstream dies
    down_task.abort();
    let _ = down_task.await;
    let held = checkpoints.trigger();
    assert!(
        !checkpoints.wait_for("up", held, Duration::from_millis(300)).await,
        "the upstream committed past a dead downstream"
    );

    // rebuilt, the region commits again
    let (tx, rx) = mpsc::channel::<Edge>(16);
    up.subscribe_edges(tx);
    let rebuilt = checkpointed(
        &checkpoints,
        shared(down_config)?,
        vec![Box::new(kayak::inputs::pipeline::PipelineInput {
            upstream: "up".into(),
            envelope: kayak::inputs::envelope::Envelope::none(),
            rx,
        })],
        vec![],
        vec![],
        Arc::clone(&buckets),
        Vec::new(),
    )?;
    let down_task = tokio::spawn(rebuilt.run());
    let next = checkpoints.trigger();
    anyhow::ensure!(checkpoints.wait_for("up", next, WAIT).await, "the region never recovered");
    up_task.abort();
    down_task.abort();
    Ok(())
}

/// Checkpoints that cannot complete are abandoned after the timeout rather
/// than piling up, and the acks they held are not lost — they fire with the
/// next checkpoint that does complete.
#[tokio::test]
async fn an_abandoned_checkpoint_hands_its_acks_to_the_next_one() -> Result<()> {
    let dir = tempfile::tempdir()?;
    let checkpoints = Checkpoints::open(Settings {
        dir: dir.path().to_path_buf(),
        interval: None,
        timeout: Duration::from_millis(100),
    })?;
    let fired = Arc::new(AtomicUsize::new(0));
    let output = CollectingOutput::new();
    let emitted = output.emitted();
    *output.failing_checkpoints().lock().map_err(|_| anyhow!("poisoned"))? = 1;
    let runtime = checkpointed(
        &checkpoints,
        shared(stub_config("p"))?,
        vec![Box::new(OneDeferred(Some(Arc::clone(&fired))))],
        vec![],
        vec![Box::new(output)],
        Arc::new(Buckets::new()),
        Vec::new(),
    )?;
    let task = tokio::spawn(runtime.run());
    anyhow::ensure!(emitted.wait_for(1, WAIT).await, "nothing was delivered");
    let failed = checkpoints.trigger();
    assert!(!checkpoints.wait_for("p", failed, Duration::from_millis(200)).await);
    // past the timeout, the next trigger abandons it
    let next = checkpoints.trigger();
    anyhow::ensure!(checkpoints.wait_for("p", next, WAIT).await, "never committed");
    assert_eq!(fired.load(Ordering::SeqCst), 1);
    assert!(checkpoints.failures() >= 1, "the abandoned checkpoint was not counted");
    task.abort();
    Ok(())
}
