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
    CollectingOutput, Emitted, FailOnNth, ScriptedInput, Ticking, WhenExhausted, batch, stub_config,
};
use kayak::transforms::Transform;
use kayak_core::{EventPayload, Stage};
use kayak_core::config::{
    ReduceFnKind, ReduceTransformConfig, SplitterTransformConfig, TransformConfig, TransformKind,
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
        Ok(r) => r,
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
