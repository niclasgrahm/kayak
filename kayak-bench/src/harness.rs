//! Building a [`Scenario`]'s graph and running it for a fixed wall-clock
//! duration.
//!
//! Everything here goes through the same seams the integration tests use —
//! `PipelineRuntime::from_parts` for the run loop, `BuildCtx` for the
//! transforms and the `pipeline` input — so what is measured is the product's
//! code path and not a second copy of it living in a bench.
//!
//! Measurement is the counters and nothing else. `Pipeline::counters` is three
//! relaxed atomics the run loop adds to unconditionally, outside the feed's
//! `receiver_count()` gate, so reading them at the start and the end of a run
//! and differencing is a complete count of what happened — no sampler, no
//! history store, no subscriber, and so nothing that changes the number by
//! being asked for it.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use kayak::BuildCtx;
use kayak::config::BuildTransformConfig;
use kayak::inputs::InputSource;
use kayak::outputs::OutputDestination;
use kayak::pipeline::{Pipeline, PipelineRuntime};
use kayak::state::{PipelineHandle, UiEvent};
use kayak::testing::{LoadInput, NullOutput, stub_config};
use kayak::transforms::Transform;
use kayak_core::config::{
    FilterKind, FilterTransformConfig, InputConfig, InputKind, NumericFilterOperatorKind,
    PipelineConfig, TransformConfig, TransformKind,
};
use kayak_core::mapping::{MapTransformConfig, Mapping};
use tokio::sync::broadcast;
use tokio::task::JoinSet;

use crate::scenario::{Chain, Scenario};

/// How large the `/events` broadcast channel is. The same order as the server's
/// so a watched run backs up — or doesn't — the way a real one would.
const EVENT_CHANNEL: usize = 1024;

/// What one run of a scenario counted.
#[derive(Clone, Copy, Debug, Default)]
pub struct Counted {
    /// Messages that entered the **root** pipelines: what the graph ingested.
    pub ingested: u64,
    /// Messages that entered *any* pipeline, roots and hops alike: what the
    /// server moved. Larger than `ingested` exactly when a scenario has depth,
    /// which is what makes the cost of a hop a division rather than a story.
    pub handled: u64,
    /// Messages that left a transform chain anywhere in the graph.
    pub emitted: u64,
    /// Failures counted anywhere in the graph. A bench run that reports any at
    /// all is a bench run whose numbers mean something else.
    pub errors: u64,
}

/// One built graph, held so it can be counted and then cancelled.
struct Graph {
    roots: Vec<Arc<Pipeline>>,
    all: Vec<Arc<Pipeline>>,
    tasks: JoinSet<()>,
    /// Kept alive for the duration: dropping the last receiver is what closes
    /// the feed's gate, and a watched run that dropped it would measure an
    /// unwatched one.
    watcher: Option<tokio::task::JoinHandle<()>>,
}

impl Graph {
    fn count(&self) -> Counted {
        let sum = |ps: &[Arc<Pipeline>]| -> (u64, u64, u64) {
            ps.iter().fold((0, 0, 0), |(i, o, e), p| {
                (
                    i + p.counters.inbound(),
                    o + p.counters.outbound(),
                    e + p.counters.errors(),
                )
            })
        };
        let (ingested, _, _) = sum(&self.roots);
        let (handled, emitted, errors) = sum(&self.all);
        Counted {
            ingested,
            handled,
            emitted,
            errors,
        }
    }

    /// Cancel every run loop and wait for it. Waiting matters between
    /// scenarios: a thousand pipelines still shutting down while the next
    /// scenario starts would be measured as part of it.
    async fn stop(mut self) {
        for p in &self.all {
            p.cancellation_token.cancel();
        }
        if let Some(w) = self.watcher.take() {
            w.abort();
        }
        while self.tasks.join_next().await.is_some() {}
    }
}

/// Build the scenario's transform chain, through the config layer so that what
/// is measured is the transform the server would have built.
fn transforms(chain: Chain) -> Result<Vec<Box<dyn Transform>>> {
    let kinds: Vec<TransformKind> = match chain {
        Chain::None => Vec::new(),
        // `value` is 21.5 in every generated message and the comparison is
        // "> 0", so every message passes — see `Chain::Filter`.
        Chain::Filter(n) => std::iter::repeat_n(
            TransformKind::Filter(FilterTransformConfig {
                filter: FilterKind::Numeric {
                    field: "value".to_string(),
                    operator: NumericFilterOperatorKind::GreaterThan,
                    value: 0.0,
                },
            }),
            n,
        )
        .collect(),
        Chain::Map => vec![TransformKind::Map(MapTransformConfig {
            mappings: vec![Mapping::Copy {
                from: "reading.unit".to_string(),
                output: Some("unit".to_string()),
                default: None,
            }],
            keep: kayak_core::mapping::KeepPolicy::All,
            on_missing: kayak_core::mapping::MapMissingPolicy::Error,
        })],
    };
    let mut pipelines = HashMap::new();
    let (events, _rx) = broadcast::channel(EVENT_CHANNEL);
    let mut built = Vec::with_capacity(kinds.len());
    for kind in kinds {
        let mut ctx = BuildCtx::new(&mut pipelines, "bench".to_string(), events.clone());
        built.push(
            TransformConfig { kind }
                .build(&mut ctx)
                .context("building a bench transform")?,
        );
    }
    Ok(built)
}

/// Build and spawn the whole graph, ready to be counted.
fn build(scenario: &Scenario, events: &broadcast::Sender<UiEvent>) -> Result<Graph> {
    let mut tasks = JoinSet::new();
    let mut roots = Vec::with_capacity(scenario.pipelines);
    let mut all = Vec::with_capacity(scenario.total_pipelines());
    // The map a `pipeline` input looks its upstream up in. It has to outlive
    // the building of the hop below, which is why the handles are kept here
    // rather than dropped as each pipeline is spawned.
    let mut handles: HashMap<String, PipelineHandle> = HashMap::new();

    for root in 0..scenario.pipelines {
        let mut upstream: Option<String> = None;
        for hop in 0..scenario.depth {
            let id = format!("bench-{root}-{hop}");
            let shared = Arc::new(
                Pipeline::new(stub_config(&id)).with_context(|| format!("building '{id}'"))?,
            );
            // The root generates; every hop below it reads what the one above
            // sent. Two different inputs, which is the whole point of `depth`.
            let input: Box<dyn InputSource> = match &upstream {
                None => Box::new(LoadInput::new(scenario.batch_size)),
                Some(up) => {
                    let mut ctx =
                        BuildCtx::new(&mut handles, id.clone(), events.clone());
                    kayak::config::BuildInputConfig::build(
                        InputConfig {
                            kind: InputKind::Pipeline(PipelineConfig {
                                upstream: up.clone(),
                            }),
                            buffer: None,
                            envelope: None,
                            ack: None,
                        },
                        &mut ctx,
                    )
                    .with_context(|| format!("building the pipeline input of '{id}'"))?
                }
            };
            let outputs: Vec<Box<dyn OutputDestination>> = vec![Box::new(NullOutput)];
            let runtime = PipelineRuntime::from_parts(
                vec![input],
                transforms(scenario.chain)?,
                outputs,
                Arc::clone(&shared),
                events.clone(),
            )
            .with_context(|| format!("assembling the runtime of '{id}'"))?;
            let join = tokio::spawn(async move {
                // A bench run that fails is reported through the error
                // counters, which the report already refuses to publish a
                // number for. Nothing here needs the `Result`.
                let _ = runtime.run().await;
            });
            handles.insert(
                id.clone(),
                PipelineHandle {
                    join_handle: join,
                    shared: Arc::clone(&shared),
                },
            );
            if hop == 0 {
                roots.push(Arc::clone(&shared));
            }
            all.push(shared);
            upstream = Some(id);
        }
    }
    // The join handles live on the `PipelineHandle`s a `pipeline` input was
    // built against; move them into the set now that nothing more will look
    // one up.
    for (_, handle) in handles {
        tasks.spawn(async move {
            let _ = handle.join_handle.await;
        });
    }
    Ok(Graph {
        roots,
        all,
        tasks,
        watcher: None,
    })
}

/// Run one scenario for `duration` and return what it counted, plus the wall
/// clock it actually ran for.
///
/// The graph is built, given `warmup` to reach a steady state, and only *then*
/// are the counters read — so the cost of spawning a thousand tasks lands
/// outside the window rather than being averaged into its throughput.
pub async fn run(scenario: &Scenario, warmup: Duration, duration: Duration) -> Result<(Counted, Duration)> {
    let (events, rx) = broadcast::channel(EVENT_CHANNEL);
    let mut graph = build(scenario, &events)?;
    if scenario.watched {
        // A receiver that keeps up, which is what a browser is: the run loop
        // only pays the reporting cost while `receiver_count() > 0`, and a
        // receiver nobody drains would still hold that gate open but would
        // also fill the channel and start lagging, which is a different
        // measurement.
        graph.watcher = Some(tokio::spawn(async move {
            let mut rx = rx;
            while rx.recv().await.is_ok() {}
        }));
    } else {
        // Dropping it is what closes the gate. Held until here so that
        // `build`'s events sender has somewhere to go during startup.
        drop(rx);
    }

    tokio::time::sleep(warmup).await;
    let before = graph.count();
    let started = Instant::now();
    tokio::time::sleep(duration).await;
    let elapsed = started.elapsed();
    let after = graph.count();
    graph.stop().await;

    Ok((
        Counted {
            ingested: after.ingested.saturating_sub(before.ingested),
            handled: after.handled.saturating_sub(before.handled),
            emitted: after.emitted.saturating_sub(before.emitted),
            errors: after.errors.saturating_sub(before.errors),
        },
        elapsed,
    ))
}

#[cfg(test)]
mod tests {
    use super::run;
    use crate::scenario::{Chain, Scenario, suite};
    use std::time::Duration;

    /// Short enough that the whole module stays inside `just test`'s budget,
    /// long enough that a debug build moves a few thousand messages.
    const WARMUP: Duration = Duration::from_millis(30);
    const WINDOW: Duration = Duration::from_millis(150);

    fn scenario(name: &'static str) -> Scenario {
        suite()
            .into_iter()
            .find(|s| s.name == name)
            .unwrap_or_else(|| panic!("the suite has no '{name}'"))
    }

    /// The harness works at all: a graph is built, it moves messages, and the
    /// counters say how many. Without this the whole crate could report zeroes
    /// forever and every baseline would agree with every other one.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_run_counts_the_messages_it_moved() {
        let (counted, elapsed) = match run(&scenario("batch100"), WARMUP, WINDOW).await {
            Ok(r) => r,
            Err(e) => panic!("running the reference scenario: {e:#}"),
        };
        assert_eq!(counted.errors, 0, "the reference scenario failed");
        assert!(counted.ingested > 0, "no messages were ingested");
        assert_eq!(
            counted.ingested, counted.handled,
            "a one-deep scenario handles exactly what it ingests"
        );
        assert!(elapsed >= WINDOW, "the window was shorter than asked for");
    }

    /// Batch size is a knob on the *messages*, not on the passes — the row
    /// that separates per-batch cost from per-message cost is worthless if
    /// both sizes move the same number of messages.
    #[tokio::test(flavor = "multi_thread")]
    async fn batch_size_changes_how_many_messages_a_pass_carries() {
        let mut small = scenario("batch100");
        small.batch_size = 1;
        let mut large = small.clone();
        large.batch_size = 500;
        let (small, _) = match run(&small, WARMUP, WINDOW).await {
            Ok(r) => r,
            Err(e) => panic!("running the small-batch scenario: {e:#}"),
        };
        let (large, _) = match run(&large, WARMUP, WINDOW).await {
            Ok(r) => r,
            Err(e) => panic!("running the large-batch scenario: {e:#}"),
        };
        assert!(
            large.ingested > small.ingested,
            "batches of 500 moved {} messages against {} for batches of 1",
            large.ingested,
            small.ingested
        );
    }

    /// Every chain has to *build* against the message `LoadInput` generates
    /// and then pass it through. A `map` whose mapping read a field that isn't
    /// there would fail every batch — and `on_missing` defaults to `error`, so
    /// it would fail loudly, which is exactly what this asserts it doesn't.
    #[tokio::test(flavor = "multi_thread")]
    async fn every_chain_runs_against_the_generated_message() {
        for chain in [Chain::Filter(1), Chain::Filter(5), Chain::Map] {
            let mut s = scenario("batch100");
            s.chain = chain;
            let (counted, _) = match run(&s, WARMUP, WINDOW).await {
                Ok(r) => r,
                Err(e) => panic!("running the {chain:?} chain: {e:#}"),
            };
            assert_eq!(counted.errors, 0, "the {chain:?} chain failed");
            assert!(counted.ingested > 0, "the {chain:?} chain moved nothing");
            // Every one of these chains passes every message through, so the
            // two counts are the same number — but not at the same *instant*:
            // a pass counts inbound before it runs the transforms and outbound
            // after, and the window's two readings each land wherever they
            // land. At most one pass is in flight at each end, so the slack is
            // two batches and asserting equality is asking for a flake on a
            // loaded machine.
            let slack = 2 * s.batch_size as u64;
            assert!(
                counted.emitted > 0 && counted.ingested.abs_diff(counted.emitted) <= slack,
                "the {chain:?} chain is meant to pass every message through, but ingested {} \
                 and emitted {}",
                counted.ingested,
                counted.emitted,
            );
        }
    }

    /// `depth` is the one scenario knob that makes `handled` differ from
    /// `ingested` — if the hops weren't wired up, the two would stay equal and
    /// the row would silently be measuring a single pipeline.
    #[tokio::test(flavor = "multi_thread")]
    async fn depth_puts_pipeline_hops_below_the_root() {
        let (counted, _) = match run(&scenario("depth3"), WARMUP, WINDOW).await {
            Ok(r) => r,
            Err(e) => panic!("running the deep scenario: {e:#}"),
        };
        assert_eq!(counted.errors, 0, "the deep scenario failed");
        assert!(counted.ingested > 0, "the root ingested nothing");
        assert!(
            counted.handled > counted.ingested,
            "three pipelines handled {} messages for {} ingested — the hops aren't wired up",
            counted.handled,
            counted.ingested
        );
    }

    /// Several roots at once all have to run. A worker starved by a
    /// never-yielding neighbour is the failure this catches — see
    /// `LoadInput`'s docs for why that is a real possibility rather than a
    /// theoretical one.
    #[tokio::test(flavor = "multi_thread")]
    async fn every_root_of_a_multi_pipeline_scenario_runs() {
        let mut s = scenario("pipelines10");
        s.pipelines = 8;
        let (counted, _) = match run(&s, WARMUP, WINDOW).await {
            Ok(r) => r,
            Err(e) => panic!("running the multi-pipeline scenario: {e:#}"),
        };
        assert_eq!(counted.errors, 0, "the multi-pipeline scenario failed");
        assert!(
            counted.ingested >= 8,
            "eight pipelines moved {} messages between them",
            counted.ingested
        );
    }

    /// The watched row is only worth reading if a subscriber is genuinely
    /// attached for the run — the run loop's reporting is gated on
    /// `receiver_count() > 0`, so a watcher that was never spawned would make
    /// the row a duplicate of the reference.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_watched_run_still_moves_messages() {
        let (counted, _) = match run(&scenario("watched"), WARMUP, WINDOW).await {
            Ok(r) => r,
            Err(e) => panic!("running the watched scenario: {e:#}"),
        };
        assert_eq!(counted.errors, 0, "the watched scenario failed");
        assert!(counted.ingested > 0, "the watched scenario moved nothing");
    }
}
