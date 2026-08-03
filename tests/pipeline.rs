//! Tests for the streamer run loop itself: transform chaining, the
//! error-tolerance rules, downstream fan-out, cancellation and UI events.
//!
//! These drive `StreamerRuntime::from_parts` with test doubles, so they touch
//! no network, no filesystem and (where it matters) no real clock.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde_json::json;
use streamer::BuildCtx;
use streamer::config::BuildTransformConfig;
use streamer::inputs::{BufferKind, Buffered, InputSource, MessageBatch};
use streamer::state::UiEvent;
use streamer::streamer::{Streamer, StreamerRuntime};
use streamer::testing::{
    CollectingOutput, Emitted, FailOnNth, ScriptedInput, WhenExhausted, batch, stub_config,
};
use streamer::transforms::Transform;
use streamer_core::config::{
    ReduceFnKind, ReduceTransformConfig, SplitterTransformConfig, TransformConfig, TransformKind,
};
use tokio::sync::{broadcast, mpsc};

fn streamer(id: &str) -> Arc<Streamer> {
    match Streamer::new(stub_config(id)) {
        Ok(s) => Arc::new(s),
        Err(e) => panic!("building streamer '{id}': {e:#}"),
    }
}

/// Build a transform the way the server does — through the config layer — but
/// without a live streamer map, which no transform needs.
fn transform_from_config(kind: TransformKind) -> anyhow::Result<Box<dyn Transform>> {
    let mut streamers = HashMap::new();
    let (events, _rx) = broadcast::channel(16);
    let mut ctx = BuildCtx::new(&mut streamers, events);
    TransformConfig { kind }.build(&mut ctx)
}

/// Run a streamer over a finite script and return what the output collected.
/// The script fails once exhausted, so `run()` returns on its own.
async fn run_to_completion(
    input: Vec<Arc<MessageBatch>>,
    transforms: Vec<Box<dyn Transform>>,
    output: CollectingOutput,
) -> Emitted {
    let emitted = output.emitted();
    let (events, _rx) = broadcast::channel(16);
    let runtime = StreamerRuntime::from_parts(
        Box::new(ScriptedInput::new(input, WhenExhausted::Fail)),
        transforms,
        Box::new(output),
        streamer("test"),
        events,
    );
    // an exhausted input is a clean stop here, not a failure of run()
    let _ = runtime.run().await;
    emitted
}

#[tokio::test]
async fn a_pipeline_without_transforms_passes_batches_through() {
    let emitted = run_to_completion(
        vec![batch(vec![json!({"n": 1})]), batch(vec![json!({"n": 2})])],
        vec![],
        CollectingOutput::new(),
    )
    .await;

    assert_eq!(
        emitted.values(),
        vec![vec![json!({"n": 1})], vec![json!({"n": 2})]]
    );
}

/// Transforms are applied in configured order and each one's output feeds the
/// next — including the one-in-N-out case, where the splitter's two batches
/// each get reduced separately.
#[tokio::test]
async fn transforms_are_chained_in_order_and_fan_out_within_the_chain() -> anyhow::Result<()> {
    let split = transform_from_config(TransformKind::Splitter(SplitterTransformConfig {
        out_size: 2,
    }))?;
    let reduce = transform_from_config(TransformKind::Reducer(ReduceTransformConfig {
        function: ReduceFnKind::Sum,
        field: "n".to_string(),
    }))?;

    let emitted = run_to_completion(
        vec![batch(vec![
            json!({"n": 1}),
            json!({"n": 2}),
            json!({"n": 3}),
            json!({"n": 4}),
        ])],
        vec![split, reduce],
        CollectingOutput::new(),
    )
    .await;

    // 4 messages -> 2 batches of 2 -> one sum each
    assert_eq!(
        emitted.values(),
        vec![
            vec![json!({"original_field": "n", "reduced_value": 3.0})],
            vec![json!({"original_field": "n", "reduced_value": 7.0})],
        ]
    );
    Ok(())
}

/// A failing transform drops that batch only — the pipeline stays up and later
/// batches still reach the output.
#[tokio::test]
async fn a_transform_error_drops_one_batch_but_keeps_the_pipeline_running() {
    let emitted = run_to_completion(
        vec![
            batch(vec![json!({"n": 1})]),
            batch(vec![json!({"n": 2})]),
            batch(vec![json!({"n": 3})]),
        ],
        vec![Box::new(FailOnNth::new(1))],
        CollectingOutput::new(),
    )
    .await;

    assert_eq!(
        emitted.values(),
        vec![vec![json!({"n": 1})], vec![json!({"n": 3})]],
        "only the second batch should have been dropped"
    );
}

/// A broken output must not tear the pipeline down — downstream streamers are
/// still fed, same as we do for transform errors.
#[tokio::test]
async fn an_output_error_does_not_stop_downstream_delivery() {
    let shared = streamer("upstream");
    let (tx, mut rx) = mpsc::channel(8);
    shared.subscribe(tx);

    let (events, _events_rx) = broadcast::channel(16);
    let runtime = StreamerRuntime::from_parts(
        Box::new(ScriptedInput::new(
            vec![batch(vec![json!({"n": 1})]), batch(vec![json!({"n": 2})])],
            WhenExhausted::Fail,
        )),
        vec![],
        Box::new(CollectingOutput::failing()),
        Arc::clone(&shared),
        events,
    );
    let _ = runtime.run().await;

    let mut received = Vec::new();
    while let Ok(b) = rx.try_recv() {
        received.push(b.iter().map(|m| (**m).clone()).collect::<Vec<_>>());
    }
    assert_eq!(
        received,
        vec![vec![json!({"n": 1})], vec![json!({"n": 2})]],
        "both batches should have reached the downstream despite the failing output"
    );
}

/// Every batch reaches both the output and every subscribed downstream — this
/// is what lets one pipeline feed several others.
#[tokio::test]
async fn a_batch_reaches_the_output_and_all_downstreams() {
    let shared = streamer("upstream");
    let (tx_a, mut rx_a) = mpsc::channel(8);
    let (tx_b, mut rx_b) = mpsc::channel(8);
    shared.subscribe(tx_a);
    shared.subscribe(tx_b);

    let output = CollectingOutput::new();
    let emitted = output.emitted();
    let (events, _events_rx) = broadcast::channel(16);
    let runtime = StreamerRuntime::from_parts(
        Box::new(ScriptedInput::new(
            vec![batch(vec![json!({"n": 1})])],
            WhenExhausted::Fail,
        )),
        vec![],
        Box::new(output),
        Arc::clone(&shared),
        events,
    );
    let _ = runtime.run().await;

    assert_eq!(emitted.values(), vec![vec![json!({"n": 1})]]);
    assert!(rx_a.try_recv().is_ok(), "downstream a got nothing");
    assert!(rx_b.try_recv().is_ok(), "downstream b got nothing");
}

/// Cancelling the token stops a run loop parked on its input — this is what
/// `DELETE /api/streams/{id}` relies on.
#[tokio::test]
async fn cancelling_the_token_stops_a_running_pipeline() {
    let shared = streamer("cancel-me");
    let (events, _events_rx) = broadcast::channel(16);
    let runtime = StreamerRuntime::from_parts(
        // never resolves again after the first batch
        Box::new(ScriptedInput::new(
            vec![batch(vec![json!({"n": 1})])],
            WhenExhausted::Pend,
        )),
        vec![],
        Box::new(CollectingOutput::new()),
        Arc::clone(&shared),
        events,
    );
    let handle = tokio::spawn(runtime.run());

    shared.cancellation_token.cancel();
    let stopped = tokio::time::timeout(Duration::from_secs(5), handle).await;
    assert!(stopped.is_ok(), "run loop did not exit after cancellation");
}

/// The SSE feed sees one `input` event and one `output` event per batch.
#[tokio::test]
async fn ui_events_are_published_for_the_input_and_output_stages() {
    let (events, mut rx) = broadcast::channel::<UiEvent>(16);
    let runtime = StreamerRuntime::from_parts(
        Box::new(ScriptedInput::new(
            vec![batch(vec![json!({"n": 1})])],
            WhenExhausted::Fail,
        )),
        vec![],
        Box::new(CollectingOutput::new()),
        streamer("events"),
        events,
    );
    let _ = runtime.run().await;

    let mut stages = Vec::new();
    while let Ok(ev) = rx.try_recv() {
        assert_eq!(ev.streamer_id, "events");
        stages.push(ev.stage);
    }
    assert_eq!(stages, vec!["input".to_string(), "output".to_string()]);
}

/// `init()` runs once, before anything is emitted — an output that emitted
/// before connecting would silently drop messages.
#[tokio::test]
async fn the_output_is_initialised_once_before_the_first_emit() {
    let output = CollectingOutput::new();
    let init_calls = output.init_calls();
    let _ = run_to_completion(vec![batch(vec![json!({"n": 1})])], vec![], output).await;

    let calls = *init_calls
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner);
    assert_eq!(calls, 1);
}

/// An input error ends this streamer's loop. Downstream streamers detect it
/// through their channel closing — see `graph.rs`.
#[tokio::test]
async fn an_input_error_ends_the_run_loop() {
    let (events, _rx) = broadcast::channel(16);
    let runtime = StreamerRuntime::from_parts(
        Box::new(ScriptedInput::new(vec![], WhenExhausted::Fail)),
        vec![],
        Box::new(CollectingOutput::new()),
        streamer("dies"),
        events,
    );
    let finished = tokio::time::timeout(Duration::from_secs(5), runtime.run()).await;
    assert!(finished.is_ok(), "run loop hung on a failing input");
}

/// The `Buffered` input decorator collects `size` upstream batches into one.
#[tokio::test]
async fn a_static_buffer_merges_upstream_batches() -> anyhow::Result<()> {
    let mut buffered = Buffered::new(
        Box::new(ScriptedInput::new(
            vec![
                batch(vec![json!({"n": 1})]),
                batch(vec![json!({"n": 2})]),
                batch(vec![json!({"n": 3})]),
            ],
            WhenExhausted::Pend,
        )),
        BufferKind::Static { size: 3 },
    );

    let out = tokio::time::timeout(Duration::from_secs(5), buffered.next()).await??;
    let values: Vec<_> = out.iter().map(|m| (**m).clone()).collect();
    assert_eq!(
        values,
        vec![json!({"n": 1}), json!({"n": 2}), json!({"n": 3})]
    );
    Ok(())
}

/// A tumbling window closes on time even when the upstream goes quiet, and
/// emits whatever it collected. Runs on a paused clock, so the 10s window costs
/// no wall time.
#[tokio::test(start_paused = true)]
async fn a_tumbling_buffer_closes_when_the_window_elapses() -> anyhow::Result<()> {
    let mut buffered = Buffered::new(
        Box::new(ScriptedInput::new(
            vec![batch(vec![json!({"n": 1})])],
            WhenExhausted::Pend,
        )),
        BufferKind::Tumbling { window_seconds: 10 },
    );

    let out = buffered.next().await?;
    let values: Vec<_> = out.iter().map(|m| (**m).clone()).collect();
    assert_eq!(values, vec![json!({"n": 1})]);
    Ok(())
}
