//! Tests for the pipeline run loop itself: transform chaining, the
//! error-tolerance rules, downstream fan-out, cancellation and UI events.
//!
//! These drive `PipelineRuntime::from_parts` with test doubles, so they touch
//! no network, no filesystem and (where it matters) no real clock.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use kayak::BuildCtx;
use kayak::config::BuildTransformConfig;
use kayak::inputs::{BufferKind, Buffered, InputSource, MessageBatch};
use kayak::outputs::OutputDestination;
use kayak::pipeline::{Pipeline, PipelineRuntime};
use kayak::state::UiEvent;
use kayak::testing::{
    AckingInput, CollectingOutput, CountingAck, DropEverything, Emitted, FailOnNth, ScriptedInput,
    Ticking, WhenExhausted, batch, stub_config,
};
use kayak::transforms::Transform;
use kayak_core::{EventPayload, Stage};
use kayak_core::config::{
    Aggregation, MissingFieldPolicy, ReduceFnKind, ReduceTransformConfig, SplitterTransformConfig,
    TransformConfig, TransformKind,
};
use serde_json::json;
use tokio::sync::{broadcast, mpsc};

/// `PipelineRuntime::from_parts` only fails when a pipeline has no inputs at
/// all, which is its own test — everything else here wires up at least one.
fn runtime(
    inputs: Vec<Box<dyn InputSource>>,
    transforms: Vec<Box<dyn Transform>>,
    outputs: Vec<Box<dyn OutputDestination>>,
    shared: Arc<Pipeline>,
    events: broadcast::Sender<UiEvent>,
) -> PipelineRuntime {
    match PipelineRuntime::from_parts(inputs, transforms, outputs, shared, events) {
        // Every test in this file is about what the feed *says* — that one
        // pass's events share a sequence number, that a failure names its
        // component — and none of them about how often it says it. The feed is
        // sampled in production; `throttling_the_ui_feed` below is what covers
        // that, and it builds its runtime without this.
        Ok(r) => r.reporting_every_pass(),
        Err(e) => panic!("building the runtime: {e:#}"),
    }
}

fn pipeline(id: &str) -> Arc<Pipeline> {
    match Pipeline::new(stub_config(id)) {
        Ok(s) => Arc::new(s),
        Err(e) => panic!("building pipeline '{id}': {e:#}"),
    }
}

/// Build a transform the way the server does — through the config layer — but
/// without a live pipeline map, which no transform needs.
fn transform_from_config(kind: TransformKind) -> anyhow::Result<Box<dyn Transform>> {
    let mut pipelines = HashMap::new();
    let (events, _rx) = broadcast::channel(16);
    let mut ctx = BuildCtx::new(&mut pipelines, "test".to_string(), events);
    TransformConfig { kind }.build(&mut ctx)
}

/// Run a pipeline over a finite script and return what the output collected.
/// The script fails once exhausted, so `run()` returns on its own.
async fn run_to_completion(
    input: Vec<Arc<MessageBatch>>,
    transforms: Vec<Box<dyn Transform>>,
    output: CollectingOutput,
) -> Emitted {
    let emitted = output.emitted();
    let (events, _rx) = broadcast::channel(16);
    let runtime = runtime(
        vec![Box::new(ScriptedInput::new(input, WhenExhausted::Fail))],
        transforms,
        vec![Box::new(output)],
        pipeline("test"),
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
        aggregations: vec![Aggregation {
            function: ReduceFnKind::Sum,
            output: "total".to_string(),
            field: Some("n".to_string()),
        }],
        group_by: Vec::new(),
        on_missing: MissingFieldPolicy::Error,
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
            vec![json!({"total": 3.0})],
            vec![json!({"total": 7.0})],
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

/// A broken output must not tear the pipeline down — downstream pipelines are
/// still fed, same as we do for transform errors.
#[tokio::test]
async fn an_output_error_does_not_stop_downstream_delivery() {
    let shared = pipeline("upstream");
    let (tx, mut rx) = mpsc::channel(8);
    shared.subscribe(tx);

    let (events, _events_rx) = broadcast::channel(16);
    let runtime = runtime(
        vec![Box::new(ScriptedInput::new(
            vec![batch(vec![json!({"n": 1})]), batch(vec![json!({"n": 2})])],
            WhenExhausted::Fail,
        ))],
        vec![],
        vec![Box::new(CollectingOutput::failing())],
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
    let shared = pipeline("upstream");
    let (tx_a, mut rx_a) = mpsc::channel(8);
    let (tx_b, mut rx_b) = mpsc::channel(8);
    shared.subscribe(tx_a);
    shared.subscribe(tx_b);

    let output = CollectingOutput::new();
    let emitted = output.emitted();
    let (events, _events_rx) = broadcast::channel(16);
    let runtime = runtime(
        vec![Box::new(ScriptedInput::new(
            vec![batch(vec![json!({"n": 1})])],
            WhenExhausted::Fail,
        ))],
        vec![],
        vec![Box::new(output)],
        Arc::clone(&shared),
        events,
    );
    let _ = runtime.run().await;

    assert_eq!(emitted.values(), vec![vec![json!({"n": 1})]]);
    assert!(rx_a.try_recv().is_ok(), "downstream a got nothing");
    assert!(rx_b.try_recv().is_ok(), "downstream b got nothing");
}

/// Cancelling the token stops a run loop parked on its input — this is what
/// `DELETE /api/pipelines/{id}` relies on.
#[tokio::test]
async fn cancelling_the_token_stops_a_running_pipeline() {
    let shared = pipeline("cancel-me");
    let (events, _events_rx) = broadcast::channel(16);
    let runtime = runtime(
        vec![
            // never resolves again after the first batch
            Box::new(ScriptedInput::new(
                vec![batch(vec![json!({"n": 1})])],
                WhenExhausted::Pend,
            )),
        ],
        vec![],
        vec![Box::new(CollectingOutput::new())],
        Arc::clone(&shared),
        events,
    );
    let handle = tokio::spawn(runtime.run());

    shared.cancellation_token.cancel();
    let stopped = tokio::time::timeout(Duration::from_secs(5), handle).await;
    assert!(stopped.is_ok(), "run loop did not exit after cancellation");
}

/// An output holding an unfinished part is told the run is over, whichever way
/// it ended.
///
/// Both cases matter and they arrive down different arms of the run loop's
/// `select!`: a cancelled pipeline breaks out of the top, an input that dies
/// breaks out of the error arm. The s3 output loses a whole object if either one
/// skips this, and the file output leaves an unterminated `json_array` behind.
#[tokio::test]
async fn an_output_is_finished_when_the_run_loop_ends() {
    for when_exhausted in [WhenExhausted::Pend, WhenExhausted::Fail] {
        let shared = pipeline("finish-me");
        let (events, _events_rx) = broadcast::channel(16);
        let output = CollectingOutput::new();
        let finish_calls = output.finish_calls();
        let runtime = runtime(
            vec![Box::new(ScriptedInput::new(
                vec![batch(vec![json!({"n": 1})])],
                when_exhausted,
            ))],
            vec![],
            vec![Box::new(output)],
            Arc::clone(&shared),
            events,
        );
        let handle = tokio::spawn(runtime.run());
        if when_exhausted == WhenExhausted::Pend {
            shared.cancellation_token.cancel();
        }
        let stopped = tokio::time::timeout(Duration::from_secs(5), handle).await;
        assert!(stopped.is_ok(), "{when_exhausted:?}: run loop did not exit");

        let calls = *finish_calls
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // exactly once: an output that has already flushed its part must not be
        // asked to do it again and upload an empty one
        assert_eq!(calls, 1, "{when_exhausted:?}: finish was called {calls} times");
    }
}

/// The SSE feed sees one `input` event and one `output` event per batch.
#[tokio::test]
async fn ui_events_are_published_for_the_input_and_output_stages() {
    let (events, mut rx) = broadcast::channel::<UiEvent>(16);
    let runtime = runtime(
        vec![Box::new(ScriptedInput::new(
            vec![batch(vec![json!({"n": 1})])],
            WhenExhausted::Fail,
        ))],
        vec![],
        vec![Box::new(CollectingOutput::new())],
        pipeline("events"),
        events,
    );
    let _ = runtime.run().await;

    let mut stages = Vec::new();
    while let Ok(ev) = rx.try_recv() {
        assert_eq!(ev.pipeline_id, "events");
        if matches!(ev.payload, EventPayload::Batch(_)) {
            stages.push(ev.stage);
        }
    }
    assert_eq!(stages, vec![Stage::Input, Stage::Output]);
}

/// One batch in, its transforms, and everything that left are one pass — and
/// the frontend groups the log by exactly this number. Without it the log can
/// only guess where one batch's journey ends and the next begins.
#[tokio::test]
async fn every_event_of_one_pass_shares_a_sequence_number() {
    let (events, mut rx) = broadcast::channel::<UiEvent>(32);
    let runtime = runtime(
        vec![Box::new(ScriptedInput::new(
            vec![
                batch(vec![json!({"n": 1})]),
                batch(vec![json!({"n": 2})]),
                batch(vec![json!({"n": 3})]),
            ],
            WhenExhausted::Fail,
        ))],
        vec![],
        vec![Box::new(CollectingOutput::new())],
        pipeline("events"),
        events,
    );
    let _ = runtime.run().await;

    let mut passes = Vec::new();
    while let Ok(ev) = rx.try_recv() {
        if matches!(ev.payload, EventPayload::Batch(_)) {
            passes.push((ev.seq, ev.stage));
        }
    }

    assert_eq!(
        passes,
        vec![
            (Some(1), Stage::Input),
            (Some(1), Stage::Output),
            (Some(2), Stage::Input),
            (Some(2), Stage::Output),
            (Some(3), Stage::Input),
            (Some(3), Stage::Output),
        ]
    );
}

/// An input dying happens in its own task while the loop waits for it, so it
/// belongs to no pass. `None` is the honest answer, and the log shows it as an
/// event of its own rather than folding it into whichever pass ran last.
#[tokio::test]
async fn an_event_outside_a_pass_carries_no_sequence_number() {
    let (events, mut rx) = broadcast::channel::<UiEvent>(16);
    let runtime = runtime(
        vec![Box::new(ScriptedInput::new(vec![], WhenExhausted::Fail))],
        vec![],
        vec![Box::new(CollectingOutput::new())],
        pipeline("events"),
        events,
    );
    let _ = runtime.run().await;

    let errors: Vec<_> = std::iter::from_fn(|| rx.try_recv().ok())
        .filter(|ev| matches!(ev.payload, EventPayload::Error(_)))
        .collect();
    let first = errors
        .first()
        .unwrap_or_else(|| panic!("no error was published"));
    assert_eq!(first.stage, Stage::Input);
    assert_eq!(first.seq, None);
}

/// With two outputs, "output failed" is not enough to act on — the card has to
/// be able to say *which* one, and only the run loop knows the index.
#[tokio::test]
async fn a_failing_output_names_which_of_them_failed() {
    let (events, mut rx) = broadcast::channel::<UiEvent>(16);
    let runtime = runtime(
        vec![Box::new(ScriptedInput::new(
            vec![batch(vec![json!({"n": 1})])],
            WhenExhausted::Fail,
        ))],
        vec![],
        vec![
            Box::new(CollectingOutput::new()),
            Box::new(CollectingOutput::failing()),
        ],
        pipeline("events"),
        events,
    );
    let _ = runtime.run().await;

    let failure = std::iter::from_fn(|| rx.try_recv().ok())
        .find(|ev| ev.stage == Stage::Output && matches!(ev.payload, EventPayload::Error(_)))
        .unwrap_or_else(|| panic!("no output error was published"));

    assert_eq!(failure.component, Some(1), "the second output is the one");
    assert_eq!(failure.seq, Some(1));
}

/// Same for a chain of transforms: which one dropped the batch is the question
/// the card is being asked.
#[tokio::test]
async fn a_failing_transform_names_its_place_in_the_chain() {
    let (events, mut rx) = broadcast::channel::<UiEvent>(16);
    let runtime = runtime(
        vec![Box::new(ScriptedInput::new(
            vec![batch(vec![json!({"n": 1})])],
            WhenExhausted::Fail,
        ))],
        // one that never fails, then one that fails on the first batch
        vec![
            Box::new(FailOnNth::new(usize::MAX)),
            Box::new(FailOnNth::new(0)),
        ],
        vec![Box::new(CollectingOutput::new())],
        pipeline("events"),
        events,
    );
    let _ = runtime.run().await;

    let failure = std::iter::from_fn(|| rx.try_recv().ok())
        .find(|ev| ev.stage == Stage::Transform)
        .unwrap_or_else(|| panic!("no transform error was published"));

    assert_eq!(failure.component, Some(1), "the second transform is the one");
    assert_eq!(failure.seq, Some(1));
}

/// Errors are the reason a card stops updating, so the UI feed has to carry
/// them — the server log is not visible from the canvas.
#[tokio::test]
async fn a_failing_transform_publishes_an_error_event_for_that_stage() {
    let (events, mut rx) = broadcast::channel::<UiEvent>(16);
    let runtime = runtime(
        vec![Box::new(ScriptedInput::new(
            vec![batch(vec![json!({"n": 1})])],
            WhenExhausted::Fail,
        ))],
        vec![Box::new(FailOnNth::new(0))],
        vec![Box::new(CollectingOutput::new())],
        pipeline("events"),
        events,
    );
    let _ = runtime.run().await;

    // the trailing error is the script running out, which the run loop reports
    // as an input failure like any other
    let errors = collect_errors(&mut rx);
    let first = errors
        .first()
        .unwrap_or_else(|| panic!("no error was published"));
    assert_eq!(first.0, Stage::Transform);
    assert!(
        first.1.contains("transform failed on batch 0"),
        "the event should carry the cause: {}",
        first.1
    );
}

/// Same for an output that can't emit — a NATS connection that dropped, or the
/// http transform's url being wrong.
#[tokio::test]
async fn a_failing_output_publishes_an_error_event_for_that_stage() {
    let (events, mut rx) = broadcast::channel::<UiEvent>(16);
    let runtime = runtime(
        vec![Box::new(ScriptedInput::new(
            vec![batch(vec![json!({"n": 1})])],
            WhenExhausted::Fail,
        ))],
        vec![],
        vec![Box::new(CollectingOutput::failing())],
        pipeline("events"),
        events,
    );
    let _ = runtime.run().await;

    let errors = collect_errors(&mut rx);
    let first = errors
        .first()
        .unwrap_or_else(|| panic!("no error was published"));
    assert_eq!(first.0, Stage::Output);
    assert!(
        first.1.contains("collecting output was told to fail"),
        "the event should carry the cause: {}",
        first.1
    );
}

/// An input that dies takes the pipeline with it, which is the case where the
/// card most needs to say why it went quiet.
#[tokio::test]
async fn a_failing_input_publishes_an_error_event_before_the_loop_ends() {
    let (events, mut rx) = broadcast::channel::<UiEvent>(16);
    let runtime = runtime(
        vec![Box::new(ScriptedInput::new(vec![], WhenExhausted::Fail))],
        vec![],
        vec![Box::new(CollectingOutput::new())],
        pipeline("events"),
        events,
    );
    let _ = runtime.run().await;

    let errors = collect_errors(&mut rx);
    assert_eq!(errors.len(), 1, "expected one input error, got {errors:?}");
    assert_eq!(errors[0].0, Stage::Input);
    assert!(
        errors[0].1.contains("scripted input exhausted"),
        "the event should carry the cause: {}",
        errors[0].1
    );
}

/// Drain the feed down to the error events, as `(stage, message)`.
fn collect_errors(rx: &mut broadcast::Receiver<UiEvent>) -> Vec<(Stage, String)> {
    let mut errors = Vec::new();
    while let Ok(ev) = rx.try_recv() {
        if let EventPayload::Error(message) = ev.payload {
            errors.push((ev.stage, message));
        }
    }
    errors
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

/// An input error ends this pipeline's loop. Downstream pipelines detect it
/// through their channel closing — see `graph.rs`.
#[tokio::test]
async fn an_input_error_ends_the_run_loop() {
    let (events, _rx) = broadcast::channel(16);
    let runtime = runtime(
        vec![Box::new(ScriptedInput::new(vec![], WhenExhausted::Fail))],
        vec![],
        vec![Box::new(CollectingOutput::new())],
        pipeline("dies"),
        events,
    );
    let finished = tokio::time::timeout(Duration::from_secs(5), runtime.run()).await;
    assert!(finished.is_ok(), "run loop hung on a failing input");
}

/// An input that fails *because we cancelled the pipeline* is not a pipeline
/// failure and must not be reported as one.
///
/// This is the shape of a real bug: tearing the graph down cancels every
/// pipeline and then drops the upstreams, so a downstream wakes with both its
/// cancellation and an "upstream is gone" ready at once. Reporting the latter
/// put a red error on a card — and on a revert, on the card of the *new*
/// pipeline that had just inherited the id, which read as the fresh pipeline
/// being broken.
/// Repeated because the bug was a coin toss: `select!` chooses randomly between
/// two ready branches, so one run reproduced it only about a third of the time.
/// Twenty makes a regression essentially certain to be caught rather than
/// noticed three CI runs later.
#[tokio::test]
async fn a_cancelled_pipeline_does_not_report_its_input_dying() {
    for attempt in 0..20 {
        let (events, mut rx) = broadcast::channel(16);
        let shared = pipeline("going-away");
        // cancelled before it ever runs, so the very first iteration has both
        // the cancellation and the input failure ready — the exact tie
        shared.cancellation_token.cancel();

        let runtime = runtime(
            vec![Box::new(ScriptedInput::new(vec![], WhenExhausted::Fail))],
            vec![],
            vec![Box::new(CollectingOutput::new())],
            Arc::clone(&shared),
            events,
        );
        let finished = tokio::time::timeout(Duration::from_secs(5), runtime.run()).await;
        assert!(finished.is_ok(), "a cancelled run loop should exit");

        let errors = collect_errors(&mut rx);
        assert!(
            errors.is_empty(),
            "attempt {attempt}: shutting a pipeline down reported itself as a failure: {errors:?}"
        );
    }
}

/// The other half of the rule: an input that dies on its own, while the
/// pipeline is very much alive, still has to be reported. The check above is
/// "was *I* cancelled", not "did something go quiet".
#[tokio::test]
async fn a_running_pipeline_still_reports_its_input_dying() {
    let (events, mut rx) = broadcast::channel(16);
    let runtime = runtime(
        vec![Box::new(ScriptedInput::new(vec![], WhenExhausted::Fail))],
        vec![],
        vec![Box::new(CollectingOutput::new())],
        pipeline("still-here"),
        events,
    );
    let _ = tokio::time::timeout(Duration::from_secs(5), runtime.run()).await;

    let errors = collect_errors(&mut rx);
    assert_eq!(
        errors.len(),
        1,
        "expected the failure to be reported: {errors:?}"
    );
    assert_eq!(errors[0].0, Stage::Input);
}

/// The `Buffered` input decorator collects `size` *messages* into one batch.
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

/// `size` counts messages, not upstream batches. An input doing its own
/// batching (`max_batch` on kafka and nats) hands the buffer several messages at
/// a time, and counting the arrivals would make `size: 100` mean anything
/// between 100 and 100×`max_batch`.
#[tokio::test]
async fn a_static_buffer_counts_messages_rather_than_upstream_batches() -> anyhow::Result<()> {
    let mut buffered = Buffered::new(
        Box::new(ScriptedInput::new(
            vec![batch(vec![json!({"n": 1}), json!({"n": 2}), json!({"n": 3})])],
            WhenExhausted::Pend,
        )),
        BufferKind::Static { size: 3 },
    );

    let out = tokio::time::timeout(Duration::from_secs(5), buffered.next()).await??;
    assert_eq!(out.len(), 3, "one batch of three messages fills a size of 3");
    Ok(())
}

/// A batch is never split, so `size` is a floor rather than a ceiling — the
/// same rule a file output's `max_rows` follows.
#[tokio::test]
async fn a_static_buffer_overshoots_rather_than_splitting_an_upstream_batch() -> anyhow::Result<()>
{
    let mut buffered = Buffered::new(
        Box::new(ScriptedInput::new(
            vec![batch(vec![json!({"n": 1}), json!({"n": 2}), json!({"n": 3})])],
            WhenExhausted::Pend,
        )),
        BufferKind::Static { size: 2 },
    );

    let out = tokio::time::timeout(Duration::from_secs(5), buffered.next()).await??;
    assert_eq!(out.len(), 3, "the arriving batch should not have been cut");
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

/// The window opens when the first message of the batch arrives, not when the
/// buffer was asked for one. A ticker at 4s under a 10s window is therefore
/// read at t=4, 8 and 12 and closes at t=14 — clocking from the call would have
/// closed at t=10 with two.
#[tokio::test(start_paused = true)]
async fn a_tumbling_window_opens_at_the_first_message_not_at_the_call() -> anyhow::Result<()> {
    let mut buffered = Buffered::new(
        Box::new(Ticking::new(Duration::from_secs(4), json!({"n": 1}))),
        BufferKind::Tumbling { window_seconds: 10 },
    );

    let out = buffered.next().await?;
    assert_eq!(out.len(), 3, "the window should run from t=4 to t=14");
    Ok(())
}

/// The point of the whole change: an input that never speaks must never produce
/// a batch. The old tumbling buffer woke every window and handed the transforms
/// an empty batch to chew on.
#[tokio::test(start_paused = true)]
async fn a_quiet_input_never_produces_an_empty_batch() {
    let mut buffered = Buffered::new(
        Box::new(ScriptedInput::new(vec![], WhenExhausted::Pend)),
        BufferKind::Tumbling { window_seconds: 1 },
    );

    // an hour of windows on a paused clock; not one of them may close
    let result = tokio::time::timeout(Duration::from_hours(1), buffered.next()).await;
    assert!(
        result.is_err(),
        "a buffer over a silent input emitted a batch"
    );
}

/// An empty batch from upstream is not a message: it neither fills the buffer
/// nor starts its clock, so it can't turn into an emitted empty batch either.
#[tokio::test(start_paused = true)]
async fn an_empty_upstream_batch_neither_fills_the_buffer_nor_starts_its_clock()
-> anyhow::Result<()> {
    let mut buffered = Buffered::new(
        Box::new(ScriptedInput::new(
            vec![batch(vec![]), batch(vec![json!({"n": 1})])],
            WhenExhausted::Pend,
        )),
        BufferKind::Tumbling { window_seconds: 10 },
    );

    let started = tokio::time::Instant::now();
    let out = buffered.next().await?;
    let values: Vec<_> = out.iter().map(|m| (**m).clone()).collect();
    assert_eq!(values, vec![json!({"n": 1})]);
    // both batches arrive at once, so a window started by the empty one would
    // have closed at the same moment — what says it didn't is that the batch
    // waited the full window rather than being cut short by it
    assert!(
        started.elapsed() >= Duration::from_secs(10),
        "the window closed early: {:?}",
        started.elapsed()
    );
    Ok(())
}

/// The combined buffer: the count ends the batch when the input is busy...
#[tokio::test(start_paused = true)]
async fn a_batch_buffer_closes_on_the_count_when_the_messages_are_there() -> anyhow::Result<()> {
    let mut buffered = Buffered::new(
        Box::new(ScriptedInput::new(
            vec![
                batch(vec![json!({"n": 1})]),
                batch(vec![json!({"n": 2})]),
                batch(vec![json!({"n": 3})]),
            ],
            WhenExhausted::Pend,
        )),
        BufferKind::Batch {
            size: 3,
            window_seconds: 600,
        },
    );

    let started = tokio::time::Instant::now();
    let out = buffered.next().await?;
    assert_eq!(out.len(), 3);
    assert!(
        started.elapsed() < Duration::from_mins(10),
        "it waited out the window instead of closing on the count"
    );
    Ok(())
}

/// ...and the window ends it when they aren't, without waiting for a count that
/// may never be reached.
#[tokio::test(start_paused = true)]
async fn a_batch_buffer_closes_on_the_window_when_the_count_is_out_of_reach() -> anyhow::Result<()>
{
    let mut buffered = Buffered::new(
        Box::new(Ticking::new(Duration::from_secs(4), json!({"n": 1}))),
        BufferKind::Batch {
            size: 1_000,
            window_seconds: 10,
        },
    );

    let out = buffered.next().await?;
    assert_eq!(out.len(), 3, "the window should run from t=4 to t=14");
    Ok(())
}

/// A zero size can only mean "don't batch", the same reading `batch_cap` gives
/// it. Taking it literally would mean a full batch before a single message
/// arrived — an empty one, forever.
#[tokio::test]
async fn a_size_of_zero_reads_as_one_rather_than_emitting_nothing() -> anyhow::Result<()> {
    let mut buffered = Buffered::new(
        Box::new(ScriptedInput::new(
            vec![batch(vec![json!({"n": 1})])],
            WhenExhausted::Pend,
        )),
        BufferKind::Static { size: 0 },
    );

    let out = tokio::time::timeout(Duration::from_secs(5), buffered.next()).await??;
    assert_eq!(out.len(), 1);
    Ok(())
}

/// An upstream that fails while the buffer is part-way through a batch reports
/// the failure rather than returning what it has — the run loop needs to hear
/// that the input is gone, and a partial batch says nothing about it.
#[tokio::test]
async fn a_buffer_reports_its_upstream_failing_rather_than_emitting_short() {
    let mut buffered = Buffered::new(
        Box::new(ScriptedInput::new(
            vec![batch(vec![json!({"n": 1})])],
            WhenExhausted::Fail,
        )),
        BufferKind::Static { size: 100 },
    );

    let result = tokio::time::timeout(Duration::from_secs(5), buffered.next()).await;
    assert!(
        matches!(result, Ok(Err(_))),
        "expected the upstream's error to come through"
    );
}

/// Several inputs are merged into one stream: the pipeline sees every batch
/// from every input, whichever produced it.
#[tokio::test]
async fn every_input_feeds_the_same_transform_chain_and_output() {
    let output = CollectingOutput::new();
    let emitted = output.emitted();
    let (events, _events_rx) = broadcast::channel(16);
    let runtime = runtime(
        vec![
            // both scripts end, so the run loop stops on its own once the
            // second one runs out
            Box::new(ScriptedInput::new(
                vec![batch(vec![json!({"from": "a"})])],
                WhenExhausted::Fail,
            )),
            Box::new(ScriptedInput::new(
                vec![batch(vec![json!({"from": "b"})])],
                WhenExhausted::Fail,
            )),
        ],
        vec![],
        vec![Box::new(output)],
        pipeline("merged"),
        events,
    );
    let _ = tokio::time::timeout(Duration::from_secs(5), runtime.run()).await;

    // the interleaving is a race, so assert on the set rather than the order
    let mut seen: Vec<_> = emitted.values().into_iter().flatten().collect();
    seen.sort_by_key(ToString::to_string);
    assert_eq!(seen, vec![json!({"from": "a"}), json!({"from": "b"})]);
}

/// A slow input must not be starved by a fast one. The dummy-style input here
/// only yields after a delay; a merge that cancelled and restarted its pending
/// `next()` on every sibling batch would never let it through.
#[tokio::test(start_paused = true)]
async fn a_slow_input_is_not_starved_by_a_chatty_one() {
    let output = CollectingOutput::new();
    let emitted = output.emitted();
    let (events, _events_rx) = broadcast::channel(16);
    let runtime = runtime(
        vec![
            Box::new(Ticking::new(Duration::from_mins(1), json!({"slow": true}))),
            Box::new(Ticking::new(
                Duration::from_millis(1),
                json!({"slow": false}),
            )),
        ],
        vec![],
        vec![Box::new(output)],
        pipeline("starved"),
        events,
    );
    let handle = tokio::spawn(runtime.run());

    // long enough for the slow input's first tick to be due many times over
    tokio::time::sleep(Duration::from_mins(10)).await;
    handle.abort();

    let saw_slow = emitted
        .values()
        .into_iter()
        .flatten()
        .any(|m| m == json!({"slow": true}));
    assert!(
        saw_slow,
        "the one-minute input never got through in ten minutes"
    );
}

/// Every output receives every batch — that's the whole point of allowing more
/// than one, and it's what makes "archive to postgres *and* watch on stdout"
/// a single pipeline rather than two.
#[tokio::test]
async fn every_output_receives_every_batch() {
    let (first, second) = (CollectingOutput::new(), CollectingOutput::new());
    let (a, b) = (first.emitted(), second.emitted());
    let (events, _events_rx) = broadcast::channel(16);
    let runtime = runtime(
        vec![Box::new(ScriptedInput::new(
            vec![batch(vec![json!({"n": 1})]), batch(vec![json!({"n": 2})])],
            WhenExhausted::Fail,
        ))],
        vec![],
        vec![Box::new(first), Box::new(second)],
        pipeline("tee"),
        events,
    );
    let _ = runtime.run().await;

    let expected = vec![vec![json!({"n": 1})], vec![json!({"n": 2})]];
    assert_eq!(a.values(), expected);
    assert_eq!(b.values(), expected, "the second output was skipped");
}

/// One broken output must not cost the others their batches — the same rule
/// that already keeps a broken output from stopping downstream delivery.
#[tokio::test]
async fn a_failing_output_does_not_stop_its_siblings() {
    let healthy = CollectingOutput::new();
    let seen = healthy.emitted();
    let (events, _events_rx) = broadcast::channel(16);
    let runtime = runtime(
        vec![Box::new(ScriptedInput::new(
            vec![batch(vec![json!({"n": 1})])],
            WhenExhausted::Fail,
        ))],
        vec![],
        // the broken one first, so a short-circuit would be visible
        vec![Box::new(CollectingOutput::failing()), Box::new(healthy)],
        pipeline("half-broken"),
        events,
    );
    let _ = runtime.run().await;

    assert_eq!(seen.values(), vec![vec![json!({"n": 1})]]);
}

/// The whole point of the ack machinery: a delivery is acknowledged exactly
/// once, and only after it has reached every output *and* every downstream
/// pipeline this one feeds — not before, and not per output.
#[tokio::test]
async fn a_delivery_is_acknowledged_once_it_has_reached_every_output_and_downstream() {
    let shared = pipeline("acked");
    let (tx_downstream, mut rx_downstream) = mpsc::channel(8);
    shared.subscribe(tx_downstream);

    let ack = CountingAck::new();
    let (events, _events_rx) = broadcast::channel(16);
    let runtime = runtime(
        vec![Box::new(AckingInput::new(
            batch(vec![json!({"n": 1})]),
            ack.clone(),
        ))],
        vec![],
        vec![Box::new(CollectingOutput::new()), Box::new(CollectingOutput::new())],
        Arc::clone(&shared),
        events,
    );
    let _ = runtime.run().await;

    assert!(rx_downstream.try_recv().is_ok(), "downstream got nothing");
    assert_eq!(ack.count(), 1, "acknowledged {} times, want exactly 1", ack.count());
}

/// A durability guarantee stronger than "this pipeline attempted every send"
/// is a deliberate non-goal for now (see the `ack` module docs) — so a
/// failing output does not hold up the acknowledgement of a delivery that
/// reached it.
#[tokio::test]
async fn a_failing_output_does_not_withhold_the_acknowledgement() {
    let ack = CountingAck::new();
    let (events, _events_rx) = broadcast::channel(16);
    let runtime = runtime(
        vec![Box::new(AckingInput::new(
            batch(vec![json!({"n": 1})]),
            ack.clone(),
        ))],
        vec![],
        vec![Box::new(CollectingOutput::failing())],
        pipeline("acked-despite-failure"),
        events,
    );
    let _ = runtime.run().await;

    assert_eq!(ack.count(), 1);
}

/// A message a `filter` or a reducer's `group_by` legitimately drops was
/// still correctly processed — there is nothing left to wait for, so the
/// delivery it arrived in is acknowledged even though no output ever saw it.
#[tokio::test]
async fn a_delivery_whose_batch_is_filtered_away_entirely_is_still_acknowledged() {
    let ack = CountingAck::new();
    let output = CollectingOutput::new();
    let seen = output.emitted();
    let (events, _events_rx) = broadcast::channel(16);
    let runtime = runtime(
        vec![Box::new(AckingInput::new(
            batch(vec![json!({"n": 1})]),
            ack.clone(),
        ))],
        vec![Box::new(DropEverything)],
        vec![Box::new(output)],
        pipeline("acked-despite-drop"),
        events,
    );
    let _ = runtime.run().await;

    assert!(seen.values().is_empty(), "the output should never have been called");
    assert_eq!(ack.count(), 1);
}

/// A pipeline with no inputs can never produce anything, so it's a config
/// error rather than a pipeline that sits there looking healthy.
#[tokio::test]
async fn a_pipeline_with_no_inputs_is_rejected() {
    let (events, _events_rx) = broadcast::channel(16);
    let built = PipelineRuntime::from_parts(
        vec![],
        vec![],
        vec![Box::new(CollectingOutput::new())],
        pipeline("empty"),
        events,
    );
    let err = match built {
        Ok(_) => panic!("a runtime with no inputs should not build"),
        Err(e) => format!("{e:#}"),
    };
    assert!(err.contains("at least one input"), "unhelpful error: {err}");
}

/// No outputs is legal, not an error: such a pipeline exists to feed the ones
/// below it, and must still run and fan out.
#[tokio::test]
async fn a_pipeline_with_no_outputs_still_feeds_its_downstreams() {
    let shared = pipeline("relay");
    let (tx, mut rx) = mpsc::channel(8);
    shared.subscribe(tx);

    let (events, _events_rx) = broadcast::channel(16);
    let runtime = runtime(
        vec![Box::new(ScriptedInput::new(
            vec![batch(vec![json!({"n": 1})])],
            WhenExhausted::Fail,
        ))],
        vec![],
        vec![],
        Arc::clone(&shared),
        events,
    );
    let _ = runtime.run().await;

    assert!(rx.try_recv().is_ok(), "downstream got nothing");
}

/// One input dying must not take the pipeline with it — the others are still
/// feeding it. The failure is reported, and only the *last* input's failure
/// ends the run loop.
#[tokio::test(start_paused = true)]
async fn one_input_failing_does_not_stop_the_others() {
    let output = CollectingOutput::new();
    let emitted = output.emitted();
    let (events, mut rx) = broadcast::channel::<UiEvent>(64);
    let runtime = runtime(
        vec![
            // fails immediately
            Box::new(ScriptedInput::new(vec![], WhenExhausted::Fail)),
            Box::new(Ticking::new(Duration::from_secs(1), json!({"alive": true}))),
        ],
        vec![],
        vec![Box::new(output)],
        pipeline("survivor"),
        events,
    );
    let handle = tokio::spawn(runtime.run());

    tokio::time::sleep(Duration::from_secs(5)).await;
    let still_running = !handle.is_finished();
    handle.abort();

    assert!(
        still_running,
        "the run loop stopped when only one of two inputs failed"
    );
    assert!(
        !emitted.values().is_empty(),
        "the surviving input never reached the output"
    );

    let errors = collect_errors(&mut rx);
    assert_eq!(
        errors.len(),
        1,
        "expected the dead input to be reported once"
    );
    assert_eq!(errors[0].0, Stage::Input);
    assert!(
        errors[0].1.contains("scripted input exhausted"),
        "the event should carry the cause: {}",
        errors[0].1
    );
}

/// The feed is a **sample**, not a record.
///
/// Publishing every pass halved the server's throughput with one browser
/// attached, and bought nothing with it — the broadcast channel dropped
/// millions of events a second to deliver a few hundred. These pin the two
/// halves of the deal: the feed stays quiet under load, and nothing it drops is
/// lost from the number the card reports.
mod throttling_the_ui_feed {
    use super::{Arc, Duration, Ticking, pipeline, ScriptedInput, WhenExhausted, batch};
    use kayak::inputs::InputSource;
    use kayak::pipeline::PipelineRuntime;
    use kayak::state::UiEvent;
    use kayak_core::{EventPayload, Stage};
    use serde_json::json;
    use tokio::sync::broadcast;

    /// A hundred passes arriving back to back inside one throttle window must
    /// not become a hundred events.
    #[tokio::test]
    async fn a_burst_of_passes_is_reported_as_only_a_few() {
        let (events, mut rx) = broadcast::channel::<UiEvent>(1024);
        let script: Vec<_> = (0..100).map(|n| batch(vec![json!({ "n": n })])).collect();
        let inputs: Vec<Box<dyn InputSource>> =
            vec![Box::new(ScriptedInput::new(script, WhenExhausted::Fail))];

        let Ok(runtime) = PipelineRuntime::from_parts(
            inputs,
            vec![],
            vec![],
            pipeline("throttled"),
            events.clone(),
        ) else {
            panic!("building the runtime");
        };
        let _ = runtime.run().await;

        let batches = std::iter::from_fn(|| rx.try_recv().ok())
            .filter(|ev| matches!(ev.payload, EventPayload::Batch(_)))
            .count();

        assert!(
            batches < 100,
            "the feed reported every pass ({batches}) instead of sampling them"
        );
    }

    /// The other half of the deal, and the one that would be easy to lose: a
    /// pipeline that isn't flooding must still be reported in full.
    ///
    /// Almost every real pipeline is this one — a message every second or two —
    /// and a throttle that sampled *those* would have turned a live log into a
    /// lossy one to fix a problem they never had.
    #[tokio::test]
    async fn a_pipeline_slower_than_the_budget_has_every_pass_reported() {
        let (events, mut rx) = broadcast::channel::<UiEvent>(1024);
        let inputs: Vec<Box<dyn InputSource>> = vec![Box::new(Ticking::new(
            // comfortably slower than UI_PASS_INTERVAL, so every pass is due
            // when it arrives. A pipeline *faster* than the budget does lose
            // lines, on purpose — see the burst test above, and note the
            // frontend draws the resulting `seq` gaps as "not shown" rather
            // than running the survivors together.
            Duration::from_millis(150),
            json!({"n": 1}),
        ))];

        let shared = pipeline("slow");
        let Ok(runtime) =
            PipelineRuntime::from_parts(inputs, vec![], vec![], Arc::clone(&shared), events.clone())
        else {
            panic!("building the runtime");
        };
        let handle = tokio::spawn(runtime.run());
        tokio::time::sleep(Duration::from_millis(800)).await;
        shared.cancellation_token.cancel();
        let _ = handle.await;

        let mut seqs = Vec::new();
        while let Ok(ev) = rx.try_recv() {
            if ev.stage == Stage::Input && matches!(ev.payload, EventPayload::Batch(_)) {
                seqs.push(ev.seq);
            }
        }

        assert!(seqs.len() >= 4, "expected several passes, got {seqs:?}");
        let expected: Vec<_> = (1..=seqs.len() as u64).map(Some).collect();
        assert_eq!(
            seqs, expected,
            "a pipeline inside its budget must have every pass reported, with no gaps"
        );
    }

    /// What the throttle drops still has to be *counted*, or the card's
    /// throughput readout reports a fraction of what the pipeline is doing.
    ///
    /// The accounting is "carried on the next reported event", which means the
    /// messages of the final unreported window are never carried out — a
    /// pipeline that stops mid-window takes up to one window's count with it.
    /// That is deliberate rather than a gap: the alternative is publishing a
    /// synthetic batch event at shutdown, which would put a row holding no
    /// messages into every card's log every time a pipeline ends. The readout is
    /// a ten-second rolling average, so a bounded 100 ms tail is invisible in
    /// it — what must not happen is the *steady state* under-reporting, which is
    /// what this pins.
    #[tokio::test]
    async fn the_messages_of_skipped_passes_are_carried_on_the_next_event() {
        let (events, mut rx) = broadcast::channel::<UiEvent>(1024);
        // long enough to span several throttle windows, so there is a "next
        // reported event" for the skipped counts to ride out on
        let script: Vec<_> = (0..100).map(|n| batch(vec![json!({ "n": n })])).collect();
        let inputs: Vec<Box<dyn InputSource>> = vec![Box::new(Ticking::new(
            Duration::from_millis(5),
            json!({"n": 1}),
        ))];
        drop(script);

        let shared = pipeline("counted");
        let Ok(runtime) =
            PipelineRuntime::from_parts(inputs, vec![], vec![], Arc::clone(&shared), events.clone())
        else {
            panic!("building the runtime");
        };
        let handle = tokio::spawn(runtime.run());
        // ~5 windows of a 5 ms ticker: ~100 passes, ~5 of them reported
        tokio::time::sleep(Duration::from_millis(500)).await;
        shared.cancellation_token.cancel();
        let _ = handle.await;

        let mut reported_events = 0u64;
        let mut counted = 0u64;
        while let Ok(ev) = rx.try_recv() {
            if ev.stage != Stage::Input {
                continue;
            }
            if let EventPayload::Batch(preview) = &ev.payload {
                reported_events += 1;
                counted += preview.counted() as u64;
            }
        }

        assert!(
            reported_events > 0,
            "the feed reported nothing at all over half a second"
        );
        assert!(
            counted > reported_events,
            "the skipped passes were not counted: {reported_events} events accounted for only \
             {counted} messages, so the readout would report a fraction of the real rate"
        );
    }
}
