use crate::BuildCtx;
use crate::backoff::Backoff;
use crate::checkpoint::{Checkpoints, ComponentState, Participant, PipelineSnapshot};
use crate::config::BuildInputConfig;
use crate::events::{Watchers, publish};
use crate::history::History;
use crate::config::BuildOutputConfig;
use crate::config::BuildTransformConfig;
use crate::inputs::InputSource;
use crate::inputs::MessageBatch;
use crate::inputs::ack::{Barrier, Delivery};
use crate::outputs::OutputDestination;
use crate::state::PipelineId;
use crate::state::UiEvent;
use crate::transforms::Transform;
use anyhow::Context;
use futures_util::stream::{FuturesUnordered, StreamExt};
use anyhow::Result;
use kayak_core::config::Config;
use kayak_core::{RunStatus, Stage};
use serde::Serialize;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::select;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tracing::debug;
use tracing::error;

#[derive(Serialize)]
pub struct Pipeline {
    pub id: PipelineId,
    pub config: Config,
    #[serde(skip)]
    pub cancellation_token: tokio_util::sync::CancellationToken,
    #[serde(skip)]
    downstream_senders: Mutex<Vec<Downstream>>,
    /// What this pipeline has done since it started, for
    /// [`crate::history`]. Three atomics the run loop adds to unconditionally
    /// — deliberately *not* behind the `receiver_count()` gate the UI feed
    /// uses, because the whole point of history is to have an answer for the
    /// hours when nobody was watching.
    #[serde(skip)]
    pub counters: crate::history::Counters,
    /// Where the run loop has got to — see [`RunStatus`], and
    /// [`PipelineRuntime::init_outputs`] for the state that made this worth
    /// having.
    ///
    /// A `Mutex` rather than an atomic despite living beside three of them:
    /// this is written three times in a pipeline's whole life and read only
    /// when the API is asked, so the `u8` encoding an atomic would need is
    /// cost with no reader to pay it back. It is a leaf lock, never held
    /// across an `.await`.
    #[serde(skip)]
    status: Mutex<RunStatus>,
    /// The source each transform was compiled from, by position in the chain —
    /// `None` for every transform that holds no code. Set once, when the run
    /// loop is built, and read by `GET /api/pipelines/{id}/transforms/{index}/
    /// script`: the transforms themselves move into the run loop's task, and
    /// what a card shows of a running script has to be what it was built from.
    #[serde(skip)]
    scripts: std::sync::OnceLock<Vec<Option<Arc<kayak_core::script::LoadedScript>>>>,
    /// Stop at the next checkpoint barrier, as if it were a savepoint — but
    /// for this pipeline alone. What a replace under checkpointing sets, so
    /// the old config stops *exactly* where its last snapshot is and the new
    /// one resumes there; see [`Pipeline::stop_at_next_barrier`]. Read only
    /// when a barrier arrives, never on an ordinary pass.
    #[serde(skip)]
    stop_at_barrier: std::sync::atomic::AtomicBool,
}

/// What travels along an edge between two pipelines: batches, and — when
/// checkpointing is on — the barriers between them. See `docs/durability.md`.
#[derive(Clone, Debug)]
pub enum Edge {
    Batch(Arc<MessageBatch>),
    Barrier(Barrier),
}

/// Someone this pipeline hands its output to.
///
/// Two kinds because only one of them understands barriers: a downstream
/// pipeline's input does, while a plain subscriber — a sample of a `pipeline`
/// input, a test — only ever wanted the batches, and is never sent a barrier.
#[derive(Clone)]
enum Downstream {
    Batches(mpsc::Sender<Arc<MessageBatch>>),
    Edges(mpsc::Sender<Edge>),
}

/// The downstreams of a pipeline that is being replaced, on their way from
/// the old pipeline to the new one. Opaque on purpose: nothing but
/// [`Pipeline::adopt_subscribers`] should do anything with them, and dropping
/// them is what tells the downstreams their upstream is gone.
#[must_use]
pub struct Subscribers(Vec<Downstream>);

impl Subscribers {
    /// How many there are, for a log line.
    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

impl Downstream {
    /// `false` when the receiver has gone.
    async fn send_batch(&self, batch: &Arc<MessageBatch>) -> bool {
        match self {
            Self::Batches(tx) => tx.send(Arc::clone(batch)).await.is_ok(),
            Self::Edges(tx) => tx.send(Edge::Batch(Arc::clone(batch))).await.is_ok(),
        }
    }

    async fn send_barrier(&self, barrier: Barrier) -> bool {
        match self {
            Self::Batches(_) => true,
            Self::Edges(tx) => tx.send(Edge::Barrier(barrier)).await.is_ok(),
        }
    }

    fn is_closed(&self) -> bool {
        match self {
            Self::Batches(tx) => tx.is_closed(),
            Self::Edges(tx) => tx.is_closed(),
        }
    }
}

/// The borrowed spelling of [`kayak_core::PipelineDto`] — what
/// `GET /api/pipelines` serializes. The two shapes are the same on the wire
/// and `the_pipeline_view_is_the_documented_dto` in `tests/api.rs` is what
/// says so.
#[derive(Serialize)]
pub struct PipelineView<'a> {
    id: &'a PipelineId,
    config: &'a Config,
    status: RunStatus,
}

async fn next_input_message(input: &mut Box<dyn InputSource>) -> Result<Delivery> {
    input.next().await
}

/// Resolves when some transform in the chain wants to hand something on
/// without a batch arriving, and says which one.
///
/// This is the run loop's second source of work and the only tick a transform
/// gets — see [`Transform::wakeup`] for why one is needed at all. A free
/// function taking the slice for the borrow checker's sake, the same reason
/// [`remember_failure`] is one: it is called in a `select!` beside
/// `next_input_message(&mut self.input)`, and only disjoint field borrows make
/// that legal.
///
/// The futures are built fresh here and the losers dropped on every pass,
/// which is why a `wakeup` has to be cancel-safe. Building them costs an
/// allocation per transform per pass, and that is the price of the feature: it
/// is paid only by pipelines that have a transform which can wake, because
/// every other `wakeup` is the default `pending()` and `FuturesUnordered`
/// polls each exactly once before parking.
async fn next_wakeup(transforms: &mut [Box<dyn Transform>]) -> usize {
    let mut waiting: FuturesUnordered<_> = transforms
        .iter_mut()
        .enumerate()
        .map(|(index, transform)| async move {
            transform.wakeup().await;
            index
        })
        .collect();
    match waiting.next().await {
        Some(index) => index,
        // An empty chain has nothing that could ever ask, and a `select!` arm
        // that returns immediately would spin the loop.
        None => std::future::pending().await,
    }
}

/// The next barrier the coordinator triggers, for a root pipeline — never, for
/// everything else. A free function for [`next_wakeup`]'s reason: it sits in
/// the run loop's `select!` beside borrows of other fields. A poll rather than
/// an `async fn`, for the size of the future — see
/// [`Participant::poll_trigger`].
fn poll_trigger(
    checkpoint: &mut Option<Participant>,
    cx: &mut std::task::Context<'_>,
) -> std::task::Poll<Barrier> {
    match checkpoint {
        Some(participant) => participant.poll_trigger(cx),
        None => std::task::Poll::Pending,
    }
}

/// Sleep for `delay`, or less if `token` is cancelled first; `false` if it
/// was.
async fn wait_unless_cancelled(
    token: &tokio_util::sync::CancellationToken,
    delay: Duration,
) -> bool {
    select! {
        biased;
        () = token.cancelled() => false,
        () = tokio::time::sleep(delay) => true,
    }
}

/// What woke the run loop.
enum Woken {
    /// The input produced a batch — the ordinary pass.
    Delivered(Delivery),
    /// The transform at this index asked to be looked at.
    Flush(usize),
    /// A checkpoint barrier: from the coordinator for a root, down an edge
    /// for everything else. See `docs/durability.md`.
    Barrier(Barrier),
}

/// Each component's config as JSON, by position — what a snapshot is keyed
/// by, so state is only ever restored into the component it came from.
#[derive(Default)]
struct ComponentConfigs {
    inputs: Vec<serde_json::Value>,
    transforms: Vec<serde_json::Value>,
    outputs: Vec<serde_json::Value>,
}

impl ComponentConfigs {
    fn of(config: &Config) -> Self {
        fn render<T: Serialize>(items: &[T]) -> Vec<serde_json::Value> {
            items
                .iter()
                .map(|item| serde_json::to_value(item).unwrap_or(serde_json::Value::Null))
                .collect()
        }
        Self {
            inputs: render(&config.inputs),
            transforms: render(&config.transforms),
            outputs: render(&config.outputs),
        }
    }
}

/// Record a failure in the history store, as `count` occurrences of it.
///
/// A function rather than five call sites of `record_error` because the
/// *rendering* has to be identical at each of them: `{:#}` puts the whole
/// context chain on one line, and the store keys a signature by that text, so a
/// call site that spelled it `{}` would file the same failure under a second
/// entry.
///
/// Free rather than a method on [`PipelineRuntime`] for the borrow checker's
/// sake: two of the call sites are inside a loop already holding `&mut
/// self.transforms` or `&mut self.outputs`, and taking the two fields it needs
/// by reference is what keeps those borrows disjoint.
fn remember_failure(
    history: &History,
    id: &PipelineId,
    stage: Stage,
    component: Option<usize>,
    err: &anyhow::Error,
    count: u64,
) {
    history.record_error(
        id,
        stage,
        component,
        &format!("{err:#}"),
        count,
        crate::events::now_millis(),
    );
}

/// Report a transform failure: count it, and — if the throttle allows — log
/// it, record it and put it on the feed.
///
/// A free function for [`remember_failure`]'s reason, one step further: both
/// call sites hold a borrow of `self.transforms` (one iterating the chain, one
/// flushing a single element), so a `&mut self` method is not available to
/// either. Shared rather than duplicated because the two paths must report
/// identically — a flush failure and an apply failure on the same transform
/// are the same failure to anyone reading the card.
fn report_transform_error(
    shared: &Pipeline,
    history: &History,
    events: &broadcast::Sender<UiEvent>,
    throttle: &mut UiThrottle,
    index: usize,
    pass: u64,
    e: &anyhow::Error,
) {
    // Counted before anything decides whether to *say* it: the throttle
    // governs the feed and the log, never the tally history keeps.
    shared.counters.add_error();
    // Logged only when the throttle also lets it through the UI: a transform
    // failing on every message of a fast pipeline would otherwise write a line
    // to the log for each one, same reasoning as the output error.
    if let Some(count) = throttle.report_error(Stage::Transform, Some(index), Instant::now()) {
        error!("[{}]\t transform error: {:?}", shared.id, e);
        remember_failure(history, &shared.id, Stage::Transform, Some(index), e, count);
        publish(events, || {
            UiEvent::error(shared.id.clone(), Stage::Transform, e)
                .seq(pass)
                .component(index)
        });
    } else {
        debug!("[{}]\t transform error (suppressed): {:?}", shared.id, e);
    }
}

/// The shortest gap between two reported passes on one pipeline. Ten a second
/// is more than a person can read and far less than a pipeline can produce.
pub const UI_PASS_INTERVAL: Duration = Duration::from_millis(100);

/// The shortest gap between two reported failures on one pipeline. Separate
/// from the pass budget on purpose — see [`UiThrottle`].
pub const UI_ERROR_INTERVAL: Duration = Duration::from_millis(250);

/// How many passes a fast pipeline may take between clock readings — see
/// [`UiThrottle::report_pass`]. A power of two so the modulo is a mask.
const CLOCK_CHECK_STRIDE: u64 = 256;

/// How much of the feed a run loop is allowed to produce.
///
/// **The feed is a sample, not a record.** Publishing every pass was measured
/// at roughly half the server's throughput with a single browser attached, and
/// the events bought with it were not even useful: at full tilt a subscriber
/// serialized 341 events a second while 8.8 *million* a second were dropped on
/// the floor by the broadcast channel. The UI was being shown noise, and the
/// pipeline was paying for the privilege.
///
/// So a pass is either reported or it isn't, and the decision is made **once
/// per pass** rather than per event: an input event whose matching output event
/// was dropped would draw a pass that never finished. What the skipped passes
/// carried is not lost — it is added to the next reported event's
/// `skipped_messages`, which is what the throughput readout is computed from.
///
/// Failures keep their own budget. A pipeline emitting batches faster than the
/// pass budget must never be able to starve its own error reporting, which one
/// shared timer would let it do.
pub struct UiThrottle {
    /// How long a reported pass suppresses the next one. Zero means "report
    /// everything", which is what the tests that are about the *content* of the
    /// feed rather than its rate run with.
    pass_interval: Duration,
    last_pass: Option<Instant>,
    /// Passes since the last reported one, used to keep the clock out of the
    /// hot path — see [`UiThrottle::report_pass`].
    passes_since_report: u64,
    /// Per stage *and component*, because two different outputs failing on one
    /// batch are two facts, not a repeat — see [`UiThrottle::report_error`].
    last_error: HashMap<(Stage, Option<usize>), Instant>,
    /// Failures suppressed since the last reported one, by the same key.
    ///
    /// The UI only ever shows the reported ones, but [`crate::history`] keeps a
    /// *count* per distinct failure, and a count that omitted the suppressed
    /// repeats would say "failed 4 times" about a pipeline that failed four
    /// thousand. Same accounting `skipped_in`/`skipped_out` do for messages.
    suppressed_errors: HashMap<(Stage, Option<usize>), u64>,
    /// Messages that passed the input stage in passes that were not reported.
    skipped_in: u64,
    /// The same for what left the transforms.
    skipped_out: u64,
}

impl UiThrottle {
    #[must_use]
    pub fn new(pass_interval: Duration) -> Self {
        Self {
            pass_interval,
            last_pass: None,
            passes_since_report: 0,
            last_error: HashMap::new(),
            suppressed_errors: HashMap::new(),
            skipped_in: 0,
            skipped_out: 0,
        }
    }

    /// Whether this pass is reported. Call once per pass, before anything is
    /// published for it.
    ///
    /// Reads the clock at most once every [`CLOCK_CHECK_STRIDE`] passes once a
    /// pipeline is running fast enough for that to matter. `Instant::now()` is
    /// only tens of nanoseconds, but a run loop that turns eight million times a
    /// second does not have tens of nanoseconds to spare — calling it on every
    /// pass measured about a tenth of the whole throughput.
    ///
    /// A pipeline slower than the stride consults the clock every pass, so the
    /// budget is exact where it is observable. A faster one may let up to
    /// `CLOCK_CHECK_STRIDE - 1` extra passes by before reporting, which at those
    /// rates is a rounding error on the hundreds of thousands of passes a window
    /// already holds — and every one of them is still *counted*, because the
    /// skip accounting doesn't go through here.
    fn report_pass(&mut self) -> bool {
        self.passes_since_report = self.passes_since_report.saturating_add(1);
        if self.passes_since_report >= CLOCK_CHECK_STRIDE
            && !self.passes_since_report.is_multiple_of(CLOCK_CHECK_STRIDE)
        {
            return false;
        }
        let now = Instant::now();
        let due = self
            .last_pass
            .is_none_or(|last| now.duration_since(last) >= self.pass_interval);
        if due {
            self.last_pass = Some(now);
            self.passes_since_report = 0;
        }
        due
    }

    /// Whether this failure is reported.
    ///
    /// Budgeted per stage and component rather than per pipeline. A single
    /// output failing once per batch is one broken connection repeating itself
    /// and is worth suppressing; the *second* of two outputs failing on the same
    /// batch is a different component with a different cause, and a shared timer
    /// would silently swallow it. The frontend coalesces what does get through,
    /// so a suppressed repeat costs nothing a reader would have seen.
    /// `Some(count)` when it is reported, where `count` is this failure plus
    /// everything suppressed since the last reported one — what
    /// [`crate::history`] adds to the signature's tally. `None` when it is
    /// suppressed, in which case the occurrence has been counted for next time.
    fn report_error(
        &mut self,
        stage: Stage,
        component: Option<usize>,
        now: Instant,
    ) -> Option<u64> {
        let key = (stage, component);
        let due = self
            .last_error
            .get(&key)
            .is_none_or(|last| now.duration_since(*last) >= UI_ERROR_INTERVAL);
        if !due {
            *self.suppressed_errors.entry(key).or_default() += 1;
            return None;
        }
        self.last_error.insert(key, now);
        Some(self.suppressed_errors.remove(&key).unwrap_or(0) + 1)
    }

    /// Take the skipped count for a stage, resetting it — what the next
    /// reported event carries.
    fn take_skipped(&mut self, stage: Stage) -> u64 {
        let counter = match stage {
            Stage::Input => &mut self.skipped_in,
            _ => &mut self.skipped_out,
        };
        std::mem::take(counter)
    }

    /// Record a batch that went unreported.
    fn skip(&mut self, stage: Stage, messages: usize) {
        let count = messages as u64;
        match stage {
            Stage::Input => self.skipped_in = self.skipped_in.saturating_add(count),
            _ => self.skipped_out = self.skipped_out.saturating_add(count),
        }
    }
}

pub struct PipelineRuntime {
    /// Every configured input, merged into one — see [`crate::inputs::merge`].
    input: Box<dyn InputSource>,
    transforms: Vec<Box<dyn Transform>>,
    outputs: Vec<Box<dyn OutputDestination>>,
    shared: Arc<Pipeline>,
    events: broadcast::Sender<UiEvent>,
    /// How much of the feed this run loop may produce. See [`UiThrottle`].
    pass_interval: Duration,
    /// Where failures are recorded for the UI to show after the fact. The
    /// *counts* go through [`Pipeline::counters`] instead — this is only ever
    /// touched behind the throttle, so a pipeline failing on every batch takes
    /// its lock a few times a second rather than a few million.
    history: Arc<History>,
    /// How the run loop asks whether anyone is attached to `/events`.
    ///
    /// Not `self.events.receiver_count()`, which is the obvious way to ask and
    /// is a mutex on a channel every pipeline in the process shares — see
    /// [`Watchers`] for what that cost.
    watchers: Watchers,
    /// This run loop's part in checkpointing, when the server checkpoints.
    /// `None` is the default and changes nothing: acks fire at the end of
    /// their pass, an output failure drops its batch. See
    /// `docs/durability.md`.
    checkpoint: Option<Participant>,
    /// What a snapshot is keyed by. Empty unless checkpointing.
    components: ComponentConfigs,
    /// How many outputs have initialised — a checkpoint taken while the rest
    /// are still being waited on only flushes these.
    initialised: usize,
}

impl PipelineRuntime {
    /// Assemble a runtime from already-built components, bypassing the config
    /// layer. This is the seam integration tests use to drive the run loop with
    /// scripted inputs and collecting outputs; production code goes through
    /// [`Pipeline::start`].
    pub fn from_parts(
        inputs: Vec<Box<dyn InputSource>>,
        transforms: Vec<Box<dyn Transform>>,
        outputs: Vec<Box<dyn OutputDestination>>,
        shared: Arc<Pipeline>,
        events: broadcast::Sender<UiEvent>,
    ) -> Result<Self> {
        Ok(Self {
            input: crate::inputs::merge(inputs, shared.id.clone(), events.clone())?,
            transforms,
            outputs,
            shared,
            events,
            pass_interval: UI_PASS_INTERVAL,
            history: Arc::new(History::disabled()),
            // Attached rather than empty, which is the opposite of `history`'s
            // "does nothing" default and deliberately so: every caller of this
            // seam holds a live receiver and expects the feed to work, so this
            // is what preserves that. See [`Watchers::attached`] for the
            // general rule. A caller that wants the headless path — the bench
            // does — says so with [`PipelineRuntime::with_watchers`].
            watchers: Watchers::attached(),
            checkpoint: None,
            components: ComponentConfigs::default(),
            initialised: 0,
        })
    }

    /// Take part in `checkpoints`: this run loop takes barriers, defers its
    /// acks to the checkpoints that cover them, and retries a failing output
    /// rather than dropping the batch. `positions` are the inputs' restored
    /// positions, carried into every snapshot until they move.
    #[must_use]
    pub fn with_checkpoints(
        self,
        checkpoints: &Arc<Checkpoints>,
        buckets: Arc<crate::buckets::Buckets>,
        positions: Vec<Option<serde_json::Value>>,
    ) -> Self {
        let participant = join(&self.shared, checkpoints, buckets, positions);
        self.with_participant(participant)
    }

    /// Take part in checkpoints through a participant already joined — see
    /// [`Pipeline::create_runtime`] for why joining comes first.
    #[must_use]
    pub fn with_participant(mut self, participant: Participant) -> Self {
        self.components = ComponentConfigs::of(&self.shared.config);
        self.checkpoint = Some(participant);
        self
    }

    /// Put a checkpoint's state back into the components built for it: each
    /// transform's and output's, where its config is unchanged. Inputs take
    /// their positions at build time instead — see `BuildCtx::restored_position`.
    pub fn restore(&mut self, snapshot: &PipelineSnapshot) {
        // a component the config does not describe (a test driving the run
        // loop through `from_parts`) is keyed by null — what `snapshot` writes
        // for it too
        let configs = ComponentConfigs::of(&self.shared.config);
        let null = serde_json::Value::Null;
        for (index, transform) in self.transforms.iter_mut().enumerate() {
            let config = configs.transforms.get(index).unwrap_or(&null);
            if let Some(state) = snapshot.transform(index, config)
                && let Err(e) = transform.restore(state)
            {
                error!(
                    "[{}]\t transform {index} could not take back its checkpoint, starting empty: {e:#}",
                    self.shared.id
                );
            }
        }
        for (index, output) in self.outputs.iter_mut().enumerate() {
            let config = configs.outputs.get(index).unwrap_or(&null);
            if let Some(state) = snapshot.output(index, config)
                && let Err(e) = output.restore(state)
            {
                error!(
                    "[{}]\t output {index} could not take back its checkpoint, starting empty: {e:#}",
                    self.shared.id
                );
            }
        }
    }

    /// What every component holds right now, for a checkpoint.
    fn snapshot(&self, positions: &[Option<serde_json::Value>]) -> PipelineSnapshot {
        let config = |configs: &[serde_json::Value], index: usize| {
            configs.get(index).cloned().unwrap_or(serde_json::Value::Null)
        };
        let inputs = (0..self.components.inputs.len().max(positions.len()))
            .map(|index| ComponentState {
                config: config(&self.components.inputs, index),
                state: positions.get(index).cloned().flatten(),
            })
            .collect();
        PipelineSnapshot {
            inputs,
            transforms: self
                .transforms
                .iter()
                .enumerate()
                .map(|(index, t)| ComponentState {
                    config: config(&self.components.transforms, index),
                    state: t.snapshot(),
                })
                .collect(),
            outputs: self
                .outputs
                .iter()
                .enumerate()
                .map(|(index, o)| ComponentState {
                    config: config(&self.components.outputs, index),
                    state: o.snapshot(),
                })
                .collect(),
        }
    }

    /// Take the checkpoint `barrier` asks for, and say whether the run loop
    /// should then stop (a savepoint).
    ///
    /// The order is the design: outputs durable first (or finished, at a
    /// savepoint — so a part is uploaded rather than snapshotted *and*
    /// uploaded), then the snapshot and the held acks to the coordinator,
    /// then the barrier on to every downstream, and only then the wait for
    /// the other writers of this pipeline's bucket. Forwarding before waiting
    /// is what keeps a writer downstream of another writer from deadlocking
    /// on it. The disk write itself happens on the coordinator's writer task;
    /// the run loop does not wait for it.
    async fn take_checkpoint(&mut self, barrier: Barrier) -> bool {
        // a savepoint for this pipeline alone, if a replace asked for one —
        // see `Pipeline::stop_at_next_barrier`. Only this pipeline's own half
        // of the checkpoint sees it: `barrier` itself is forwarded as it came.
        let stop = barrier.stop
            || self
                .shared
                .stop_at_barrier
                .load(std::sync::atomic::Ordering::Relaxed);
        let Some(mut participant) = self.checkpoint.take() else {
            return stop;
        };
        let mut durable = true;
        for index in 0..self.initialised.min(self.outputs.len()) {
            let result = if stop {
                self.outputs[index].finish().await
            } else {
                self.outputs[index].checkpoint().await
            };
            if let Err(e) = result {
                durable = false;
                error!(
                    "[{}]\t output {index} could not be made durable for checkpoint {}: {e:?}",
                    self.shared.id, barrier.epoch
                );
                self.shared.counters.add_error();
                remember_failure(&self.history, &self.shared.id, Stage::Output, Some(index), &e, 1);
                publish(&self.events, || {
                    UiEvent::error(self.shared.id.clone(), Stage::Output, &e).component(index)
                });
            }
        }
        let snapshot = durable.then(|| self.snapshot(&participant.positions()));
        participant.submit(Barrier { stop, ..barrier }, snapshot);

        let mut gone = false;
        for tx in &self.shared.downstream_senders() {
            gone |= !tx.send_barrier(barrier).await;
        }
        if gone {
            self.shared.prune_downstream();
        }
        let bucket = self.shared.config.state.as_ref().map(|s| s.bucket.clone());
        participant
            .bucket_barrier(bucket.as_deref(), barrier.epoch, &self.shared.cancellation_token)
            .await;
        self.checkpoint = Some(participant);
        stop
    }

    /// Ask this shared count whether anyone is watching, rather than assuming
    /// somebody is. What [`Pipeline::start`] threads through from
    /// [`crate::state::AppState`], and the only way to get the headless path.
    #[must_use]
    pub fn with_watchers(mut self, watchers: Watchers) -> Self {
        self.watchers = watchers;
        self
    }

    /// Record what this run loop does into the server's history store. Without
    /// it a runtime keeps none, which is what every test that isn't about
    /// history wants.
    #[must_use]
    pub fn with_history(mut self, history: Arc<History>) -> Self {
        self.history = history;
        self
    }

    /// Report **every** pass rather than sampling the feed.
    ///
    /// The seam the tests that are about what the feed *says* use — that the
    /// events of one pass share a sequence number, that a failure names its
    /// component — none of which are about how often it says it. Production
    /// never calls this; the rate is not a thing a config should be able to
    /// turn off, because a pipeline that can flood the browser is one that can
    /// halve its own throughput.
    #[must_use]
    pub fn reporting_every_pass(mut self) -> Self {
        self.pass_interval = Duration::ZERO;
        self
    }

    /// Initialise every output, waiting out whatever is stopping one.
    ///
    /// The invariant is unchanged and is the reason this runs before the loop
    /// rather than inside it: an output that has not initialised never sees a
    /// batch, because there is no pass until every one of them is ready. What
    /// changed is what happens *while* one is unreachable. This used to
    /// return the error, which ended `run` before the loop was ever entered —
    /// and since nothing removes the handle when a run loop exits, a
    /// `postgres` output pointed at a database that simply wasn't up yet left
    /// a pipeline registered, dead, and with nothing in the process that
    /// would ever bring it back. Restarting the server was the only cure.
    ///
    /// So a failed `init` is now retried on the same [`Backoff`] every input
    /// and every output already reconnects on, and the pipeline comes up on
    /// its own the moment the far end answers. Not reading the input in the
    /// meantime is the right backpressure: there is nowhere to put a message
    /// yet.
    ///
    /// **Retrying is deliberately not conditional on the kind of failure.** A
    /// wrong password and a downed host are the same `Err` to most drivers,
    /// and guessing wrong is expensive in one direction only — a
    /// misclassified outage is a pipeline that never comes back, while a
    /// misclassified config error costs one connect attempt every thirty
    /// seconds. A permanent failure is legible without the runtime having to
    /// rule on it: it arrives as a single [`crate::history::ErrorSignature`]
    /// whose count climbs, and "password authentication failed, 240 times
    /// since 02:14" reads as fatal to anyone looking at the card.
    ///
    /// Returns `false` if the pipeline was cancelled while waiting.
    async fn init_outputs(&mut self) -> bool {
        let mut backoff = Backoff::new();
        // Resumed at, never restarted from: the outputs before this one are
        // connected already, and calling `init` on them again would open a
        // second connection and leak the first.
        let mut index = 0;
        while index < self.outputs.len() {
            self.initialised = index;
            match self.outputs[index].init().await {
                Ok(()) => {
                    if backoff.is_failing() {
                        debug!(
                            "[{}]\t output {index} initialised after {} failed attempts",
                            self.shared.id,
                            backoff.attempts()
                        );
                        backoff.succeeded();
                    }
                    index += 1;
                    continue;
                }
                Err(e) => {
                    error!(
                        "[{}]\t output {index} failed to initialise, retrying: {e:?}",
                        self.shared.id
                    );
                    self.shared.counters.add_error();
                    remember_failure(
                        &self.history,
                        &self.shared.id,
                        Stage::Output,
                        Some(index),
                        &e,
                        1,
                    );
                    // no `seq`: this is before the first pass, not part of one
                    publish(&self.events, || {
                        UiEvent::error(self.shared.id.clone(), Stage::Output, &e).component(index)
                    });
                }
            }
            // Paced by the backoff rather than by [`UiThrottle`]: here the
            // attempts *are* the events, and they are already at most one
            // every 250ms climbing to one every 30s. Cancellable, because
            // waiting out a database that is never coming back must not hold
            // up a delete or a shutdown for thirty seconds.
            //
            // A root still takes its checkpoints meanwhile: it has read
            // nothing, so its snapshot is what it was restored with, and
            // answering keeps its region's checkpoints moving — and a
            // savepoint from waiting out the whole grace period on it.
            let delay = backoff.failed();
            let deadline = tokio::time::Instant::now() + delay;
            loop {
                let barrier = select! {
                    () = self.shared.cancellation_token.cancelled() => return false,
                    () = tokio::time::sleep_until(deadline) => break,
                    barrier = std::future::poll_fn(|cx| poll_trigger(&mut self.checkpoint, cx)) => barrier,
                };
                if Box::pin(self.take_checkpoint(barrier)).await {
                    return false;
                }
            }
        }
        self.initialised = self.outputs.len();
        true
    }

    /// Give every output its one chance to land an unfinished part.
    ///
    /// Called after the loop ends, however it ended. Errors are reported like
    /// an `emit` error rather than returned: the pipeline has already stopped,
    /// and failing the run *now* would report the shutdown itself as the
    /// pipeline's failure.
    async fn finish_outputs(&mut self) {
        for index in 0..self.outputs.len() {
            let Err(e) = self.outputs[index].finish().await else {
                continue;
            };
            error!("[{}]\t output failed to finish: {:?}", self.shared.id, e);
            self.shared.counters.add_error();
            remember_failure(&self.history, &self.shared.id, Stage::Output, Some(index), &e, 1);
            publish(&self.events, || {
                UiEvent::error(self.shared.id.clone(), Stage::Output, &e).component(index)
            });
        }
    }

    /// Put one batch through the transform chain, in order, and return whatever
    /// came out the far end — which may be several batches, or none.
    ///
    /// A transform that fails drops *that* batch and the chain carries on with
    /// the rest: one bad message must not stop the pipeline.
    /// `from` is where in the chain to start, which is 0 for an arriving batch
    /// and one past the flushing transform for a [`Woken::Flush`] pass — what
    /// a buffer hands on has already been through everything in front of it.
    async fn apply_transforms(
        &mut self,
        batch: Arc<MessageBatch>,
        from: usize,
        pass: u64,
        throttle: &mut UiThrottle,
    ) -> Vec<Arc<MessageBatch>> {
        let mut batches = vec![batch];
        for (offset, t) in self.transforms[from..].iter_mut().enumerate() {
            let index = from + offset;
            let mut next = Vec::new();
            for b in batches {
                match t.apply(b).await {
                    Ok(b) => next.extend(b),
                    Err(e) => report_transform_error(
                        &self.shared,
                        &self.history,
                        &self.events,
                        throttle,
                        index,
                        pass,
                        &e,
                    ),
                }
            }
            batches = next;
        }
        batches
    }

    /// Take what one transform wants to hand on, and put it through the rest
    /// of the chain.
    ///
    /// The transform decides whether it actually has anything — a wakeup is
    /// "look at me", not a promise — so an empty answer here is ordinary and
    /// produces a pass with no output events, which is what a gate that was
    /// woken by a write that didn't open it looks like.
    async fn flush_transform(
        &mut self,
        index: usize,
        pass: u64,
        throttle: &mut UiThrottle,
    ) -> Vec<Arc<MessageBatch>> {
        let flushed = match self.transforms[index].flush().await {
            Ok(batches) => batches,
            Err(e) => {
                report_transform_error(
                    &self.shared,
                    &self.history,
                    &self.events,
                    throttle,
                    index,
                    pass,
                    &e,
                );
                return Vec::new();
            }
        };
        let mut out = Vec::new();
        for batch in flushed {
            out.extend(
                self.apply_transforms(batch, index + 1, pass, throttle)
                    .await,
            );
        }
        out
    }

    /// Everything a pass does with what came out of the transform chain:
    /// count it, report it, hand it to every output and every downstream.
    ///
    /// Split out of [`PipelineRuntime::run`] because both kinds of pass — a
    /// batch that arrived and a transform that flushed — end here, and because
    /// what is left in `run` is then the shape of the loop rather than the
    /// shape of the loop plus the shape of a delivery.
    ///
    /// Returns `false` when the pipeline was cancelled while an output was
    /// being retried — the pass did not finish, and must not be acknowledged.
    async fn deliver(
        &mut self,
        batches: &[Arc<MessageBatch>],
        pass: u64,
        reported: bool,
        watching: bool,
        throttle: &mut UiThrottle,
    ) -> bool {
        // Under checkpointing a failed emit is retried rather than dropped:
        // dropping it and then completing a checkpoint past it is exactly the
        // loss checkpoints exist to prevent. See `docs/durability.md`.
        let retry = self.checkpoint.is_some();
        // What the transforms produced, reported before the outputs are
        // given it: the log is a record of what passed through this
        // pipeline, which is true whether or not an output then took it.
        for b in batches {
            self.shared.counters.add_outbound(b.len());
            if reported {
                let skipped = throttle.take_skipped(Stage::Output);
                publish(&self.events, || {
                    UiEvent::batch(self.shared.id.clone(), Stage::Output, b, skipped).seq(pass)
                });
            } else if watching {
                throttle.skip(Stage::Output, b.len());
            }
        }

        for b in batches {
            // every output gets every batch. a failing one shouldn't tear
            // the pipeline down — its siblings and the downstream pipelines
            // are still fed, same as we do for transform errors
            for (index, output) in self.outputs.iter_mut().enumerate() {
                let mut backoff: Option<Backoff> = None;
                while let Err(e) = output.emit(b.clone()).await {
                    // Logged only when the throttle also lets it through
                    // the UI. An output whose broker is down now fails
                    // fast on its own backoff gate (see `outputs::*`),
                    // but that still leaves one failed `emit` per batch —
                    // without this, a fast pipeline against a downed
                    // broker writes a log line for every one of them,
                    // which is the "went crazy" a reconnect storm looks
                    // like even after the reconnect itself is tamed.
                    self.shared.counters.add_error();
                    if let Some(count) =
                        throttle.report_error(Stage::Output, Some(index), Instant::now())
                    {
                        error!("[{}]\t output error: {:?}", self.shared.id, e);
                        remember_failure(
                            &self.history,
                            &self.shared.id,
                            Stage::Output,
                            Some(index),
                            &e,
                            count,
                        );
                        publish(&self.events, || {
                            UiEvent::error(self.shared.id.clone(), Stage::Output, &e)
                                .seq(pass)
                                .component(index)
                        });
                    } else {
                        debug!("[{}]\t output error (suppressed): {:?}", self.shared.id, e);
                    }
                    if !retry {
                        break;
                    }
                    // only this output is retried: its siblings already have
                    // the batch, and giving it to them twice is a duplicate
                    // nothing made necessary
                    let delay = backoff.get_or_insert_with(Backoff::new).failed();
                    // boxed so the wait's timer is not part of every pass's
                    // state: this is the failure path, and the pass is the
                    // hot one
                    if !Box::pin(wait_unless_cancelled(&self.shared.cancellation_token, delay)).await {
                        return false;
                    }
                }
            }
            let senders = self.shared.downstream_senders();
            let mut gone = false;
            for tx in &senders {
                if !tx.send_batch(b).await {
                    debug!(
                        "[{}]\t dropping batch for a downstream that went away",
                        self.shared.id
                    );
                    gone = true;
                }
            }
            // A receiver that has been dropped never comes back, so keeping
            // its sender means failing to send to it once per batch for the
            // life of the pipeline. Pruned here, on the failure, rather than
            // by asking every sender whether it is closed on every pass —
            // that question costs something on a hot path where the answer is
            // almost always no.
            if gone {
                self.shared.prune_downstream();
            }
        }
        // Every output and every downstream handoff for this pass has now
        // been *attempted* — not necessarily succeeded, and that's the
        // deliberate line: a failing output does not withhold the
        // acknowledgement, because "delivered" here means "this pipeline
        // finished handling the batch", not "every sink has it". See the
        // `ack` module docs for the reasoning and the current scope.
        true
    }

    /// A barrier the coordinator triggered since the last pass, for a root —
    /// taken between two passes, so the snapshot is exactly "everything
    /// delivered so far". Asked as an atomic load rather than as the first arm
    /// of the run loop's `select!`: there, polling it would register and drop
    /// a waiter on every pass, which measured at a quarter of a busy
    /// pipeline's throughput.
    fn due_barrier(&mut self) -> Option<Barrier> {
        self.checkpoint.as_mut().and_then(Participant::trigger_ready)
    }

    /// Count an arriving batch and, if this pass is reported, put it on the
    /// feed. Returns whether the pass is reported — decided once, here, and
    /// used for every batch event the pass produces: reporting the input of a
    /// pass whose output was dropped would draw a pass that never finished.
    /// See [`UiThrottle`].
    ///
    /// A plain function for the run loop's length, and deliberately not an
    /// `async` one — this is the hot path, and only a sync helper is inlined
    /// for free.
    fn report_inbound(
        &self,
        delivery: &Delivery,
        pass: u64,
        watching: bool,
        throttle: &mut UiThrottle,
    ) -> bool {
        let reported = watching && throttle.report_pass();
        // Counted before the `watching` gate and deliberately outside it: the
        // feed is a sample of what is happening now and costs nothing when
        // nobody is attached, while this is the record of what happened,
        // which is worth exactly as much at 3am. Three relaxed atomic adds a
        // pass is what that costs.
        self.shared.counters.add_inbound(delivery.len());
        // The batch as it arrived, before any transform has touched it — one
        // half of what a card's log shows, the other being what leaves at the
        // outputs.
        if reported {
            let skipped = throttle.take_skipped(Stage::Input);
            publish(&self.events, || {
                UiEvent::batch(self.shared.id.clone(), Stage::Input, delivery, skipped).seq(pass)
            });
        } else if watching {
            throttle.skip(Stage::Input, delivery.len());
        }
        reported
    }

    /// The last input has died: say so everywhere a failure is said.
    fn input_failed(&self, e: &anyhow::Error) {
        error!(
            "[{}]\t input error, stopping pipeline: {:?}",
            self.shared.id, e
        );
        self.shared.counters.add_error();
        remember_failure(&self.history, &self.shared.id, Stage::Input, None, e, 1);
        publish(&self.events, || {
            UiEvent::error(self.shared.id.clone(), Stage::Input, e)
        });
    }

    /// A pass is done with its delivery: acknowledge it — or, under
    /// checkpointing, hold an ack that stands for a position until the
    /// coordinator fires it, once a checkpoint covering it is durable.
    fn settle(&mut self, ack: Box<dyn crate::inputs::Ack>) {
        match self.checkpoint.as_mut() {
            Some(participant) => participant.defer(ack),
            None => ack.ack(),
        }
    }

    /// Run until the input errors or the pipeline is cancelled.
    pub async fn run(mut self) -> anyhow::Result<()> {
        if !self.init_outputs().await {
            // Cancelled before the loop was ever entered, so there is nothing
            // to finish: no batch was emitted, and no output opens a part
            // before its first one — see `outputs::file`'s `init`.
            self.shared.set_status(RunStatus::Stopped);
            return Ok(());
        }
        self.shared.set_status(RunStatus::Running);
        // Which pass through the loop we are on. Every event a pass produces
        // carries it, which is what lets the UI show one batch's journey — in,
        // transforms, out — as one thing rather than as unrelated lines that
        // happened to arrive together.
        let mut pass = 0u64;
        let mut throttle = UiThrottle::new(self.pass_interval);
        // A savepoint's barrier already finished the outputs before it took
        // the snapshot; finishing them again after the loop would be a second
        // `finish` the trait promises never happens.
        let mut stopped_at_savepoint = false;
        loop {
            // A root's barrier that is due is taken before anything else,
            // between two passes — see `due_barrier`.
            let next = if let Some(barrier) = self.due_barrier() {
                Ok(Woken::Barrier(barrier))
            } else {
                // Inline rather than behind a helper `async fn`: the extra
                // state-machine layer measured at a few percent of the
                // per-pass floor.
                select! {
                    // `biased` so cancellation always wins a tie. Tearing the
                    // graph down cancels every pipeline and *then* drops the
                    // upstreams, so a downstream is woken with both its
                    // cancellation and an "upstream is gone" ready at once —
                    // and a random pick would report our own shutdown as a
                    // pipeline failure half the time.
                    biased;
                    () = self.shared.cancellation_token.cancelled() => break,
                    msg = next_input_message(&mut self.input) => msg.map(|delivery| {
                        match delivery.barrier() {
                            Some(barrier) => Woken::Barrier(barrier),
                            None => Woken::Delivered(delivery),
                        }
                    }),
                    // The other source of work, and the only one that isn't a
                    // message: a transform that holds messages back — a
                    // `buffer` waiting on a window or on a state bucket —
                    // asking to be looked at. `FuturesUnordered` parks
                    // immediately when every transform in the chain is one
                    // that can never wake, which is the overwhelmingly common
                    // case.
                    index = next_wakeup(&mut self.transforms) => Ok(Woken::Flush(index)),
                    // A root's barrier, awaited only once its input has gone
                    // quiet; a busy root takes them from `due_barrier` above.
                    barrier = std::future::poll_fn(|cx| poll_trigger(&mut self.checkpoint, cx)) => {
                        Ok(Woken::Barrier(barrier))
                    }
                }
            };
            let woken = match next {
                Ok(woken) => woken,
                Err(e) => {
                    // Same reasoning one step later: the error may have been
                    // produced before we were cancelled but read after. An
                    // input dying because we asked it to is not news, and
                    // reporting it would put a red line on a card that is on
                    // its way out — or, after a revert, on the card of the
                    // freshly built pipeline that inherited its id.
                    if self.shared.cancellation_token.is_cancelled() {
                        debug!(
                            "[{}]\t input stopped while shutting down: {:#}",
                            self.shared.id, e
                        );
                        break;
                    }
                    self.input_failed(&e);
                    break;
                }
            };
            // Nobody watching means no clock read and no accounting: reading
            // the clock once per pass is small but it is not free, and a
            // headless run measured about 12% slower when it did it anyway.
            // This is the same gate `publish` applies, hoisted to cover the
            // throttle as well.
            //
            // Asked of [`Watchers`] rather than of the channel. They answer the
            // same question, but `receiver_count()` answers it by taking a
            // mutex on a channel every pipeline shares, which capped the whole
            // process at ~6.5M passes a second however many cores it had.
            //
            // Nothing is accumulated while unwatched either, so a browser
            // attaching to a pipeline that has been running for a week doesn't
            // get a week's worth of skipped messages on its first event.
            let watching = self.watchers.any();

            // A flush pass has no inbound half: nothing arrived, a transform
            // simply decided it had waited long enough. So it counts no
            // inbound messages, publishes no input event and has nothing to
            // acknowledge — the messages it hands on were counted and
            // acknowledged on the passes they arrived.
            //
            // It is also the one kind of pass that can turn out not to be one.
            // A wakeup is "look at me": a `buffer` gated on a bucket is woken
            // by every write to it and opens on almost none of them, so the
            // empty answer is the common one and it is dealt with *before* the
            // throttle is asked anything. Spending the pipeline's pass budget
            // on a pass with nothing to show would drop a real one in its
            // place, and `pass` itself is left alone so the sequence numbers
            // keep meaning what the UI draws them as.
            let (batches, ack, reported) = match woken {
                Woken::Barrier(barrier) => {
                    if Box::pin(self.take_checkpoint(barrier)).await {
                        stopped_at_savepoint = true;
                        break;
                    }
                    continue;
                }
                Woken::Flush(index) => {
                    let batches = self.flush_transform(index, pass + 1, &mut throttle).await;
                    if batches.is_empty() {
                        continue;
                    }
                    pass += 1;
                    (batches, None, watching && throttle.report_pass())
                }
                Woken::Delivered(delivery) => {
                    pass += 1;
                    let reported = self.report_inbound(&delivery, pass, watching, &mut throttle);
                    // Taken apart here rather than carried whole: the transforms
                    // and outputs below only ever want the messages, and holding
                    // onto `ack` by name is what lets it be acknowledged once,
                    // after all of them, regardless of how this pass turns out —
                    // see the `ack` module docs for what "delivered" means and
                    // its current scope.
                    let Delivery { batch, ack } = delivery;
                    (
                        self.apply_transforms(batch, 0, pass, &mut throttle).await,
                        Some(ack),
                        reported,
                    )
                }
            };

            if !self
                .deliver(&batches, pass, reported, watching, &mut throttle)
                .await
            {
                break;
            }

            // Firing whatever the pass produced (a filtered-out message
            // produces zero `batches`, so the loop above never ran) is
            // deliberate too: a message a `filter` or a reducer's `group_by`
            // legitimately dropped was still correctly processed, and there is
            // nothing left to wait for.
            //
            // A flush pass has no `ack` at all — nothing arrived on it. Note
            // what that means for `on_delivery` in front of a `buffer`: a held
            // message is acknowledged on the pass it *arrived*, not on the one
            // that hands it on, so a crash while a buffer is holding loses it.
            // That is the buffer's existing behaviour rather than something
            // the wait triggers introduced, and it is on the roadmap with the
            // rest of the ack story.
            if let Some(ack) = ack {
                self.settle(ack);
            }
        }
        // Which of the two ways out this was, decided by the same question
        // the error arm above asks — "was *I* cancelled". A loop that ended
        // any other way ended because its last input died, and that is the
        // state worth being able to see from outside: the pipeline is over,
        // and nothing in the process will restart it.
        let ended = if stopped_at_savepoint || self.shared.cancellation_token.is_cancelled() {
            RunStatus::Stopped
        } else {
            RunStatus::Failed
        };
        if !stopped_at_savepoint {
            self.finish_outputs().await;
        }
        // After `finish_outputs`, so the status changes when the run loop is
        // actually done rather than while an output is still landing a part.
        self.shared.set_status(ended);
        Ok(())
    }
}

/// Join `checkpoints` as `shared`'s run loop. Whether it is a root — takes its
/// barriers from the coordinator rather than down an edge — is read off the
/// config: a pipeline with no `pipeline` input has nowhere else to get them.
fn join(
    shared: &Arc<Pipeline>,
    checkpoints: &Arc<Checkpoints>,
    buckets: Arc<crate::buckets::Buckets>,
    positions: Vec<Option<serde_json::Value>>,
) -> Participant {
    let config = &shared.config;
    let upstreams: Vec<PipelineId> = config.upstreams().into_iter().cloned().collect();
    let root = upstreams.is_empty();
    checkpoints.join(
        shared.id.clone(),
        upstreams,
        config.state.as_ref().map(|state| state.bucket.clone()),
        root,
        buckets,
        positions,
    )
}

impl Pipeline {
    pub fn new(config: Config) -> Result<Self> {
        let id = match config.id.clone() {
            Some(id) => id,
            None => petname::petname(3, "-").context("failed to generate a random pipeline id")?,
        };
        let cancellation_token = tokio_util::sync::CancellationToken::new();
        Ok(Self {
            id,
            cancellation_token,
            config,
            downstream_senders: Mutex::new(Vec::new()),
            counters: crate::history::Counters::default(),
            // Built, not yet spawned — and `Starting` rather than `Running`
            // because the run loop is what earns the promotion, in `run()`,
            // once every output has initialised.
            status: Mutex::new(RunStatus::Starting),
            scripts: std::sync::OnceLock::new(),
            stop_at_barrier: std::sync::atomic::AtomicBool::new(false),
        })
    }

    /// Stop this pipeline's run loop at the next checkpoint barrier it takes,
    /// the way a savepoint stops the whole graph: its outputs are finished
    /// rather than checkpointed, its snapshot is submitted, and it leaves the
    /// coordinator cleanly. The barrier it forwards is left as it was, so the
    /// pipelines below it take an ordinary checkpoint and keep running.
    ///
    /// Nothing happens without checkpointing — there are no barriers — which
    /// is why a caller pairs this with a trigger and a timeout.
    pub(crate) fn stop_at_next_barrier(&self) {
        self.stop_at_barrier
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    /// What the transform at `index` was compiled from, if it is one that
    /// holds code and the pipeline has been built.
    #[must_use]
    pub fn loaded_script(&self, index: usize) -> Option<Arc<kayak_core::script::LoadedScript>> {
        self.scripts.get()?.get(index)?.clone()
    }

    /// Where this pipeline's run loop has got to.
    #[must_use]
    pub fn status(&self) -> RunStatus {
        *self.lock_status()
    }

    /// Move it. Only the run loop calls this — the status is a report of what
    /// that task is doing, and something else writing it would be a claim
    /// rather than an observation.
    fn set_status(&self, status: RunStatus) {
        *self.lock_status() = status;
    }

    /// A poisoned lock here only means a task panicked while reading or
    /// writing one enum; recover rather than propagate, exactly as
    /// [`Pipeline::lock_senders`] does.
    fn lock_status(&self) -> std::sync::MutexGuard<'_, RunStatus> {
        self.status.lock().unwrap_or_else(|poisoned| {
            tracing::warn!("[{}]\t status lock was poisoned; recovering", self.id);
            poisoned.into_inner()
        })
    }

    fn create_runtime(self: &Arc<Self>, mut ctx: BuildCtx) -> Result<PipelineRuntime> {
        // what the last committed checkpoint holds for this pipeline, if the
        // server checkpoints and it has one
        let restored = ctx
            .checkpoints
            .as_ref()
            .and_then(|checkpoints| checkpoints.restored(&self.id));
        let configs = ComponentConfigs::of(&self.config);
        let mut transforms = Vec::with_capacity(self.config.transforms.len());
        for t in self.config.transforms.iter().cloned() {
            transforms.push(t.build(&mut ctx)?);
        }
        // Ignored when already set: a `Pipeline` is built into one run loop,
        // and a second build of the same one would be of the same config.
        let _ = self
            .scripts
            .set(transforms.iter().map(|t| t.loaded_script()).collect());
        // inputs first: a `pipeline` input registers itself on its upstream as
        // it builds, and an output that fails to build shouldn't leave half a
        // subscription behind — building it last keeps that window as small as
        // the old single-input code had it
        // a position is the one piece of state an input takes at build time
        // rather than afterwards: an input `buffer` moves the input into a
        // task of its own the moment it is built
        let positions: Vec<Option<serde_json::Value>> = (0..self.config.inputs.len())
            .map(|index| {
                restored.as_ref().and_then(|snapshot| {
                    snapshot.input(index, configs.inputs.get(index).unwrap_or(&serde_json::Value::Null))
                })
            })
            .collect();
        // Joined *before* the inputs are built, because building a `pipeline`
        // input subscribes it to its upstream, which is running: from that
        // moment the upstream may hand it batches and then a barrier. Were
        // this pipeline not yet a member, the upstream would find itself
        // alone in its region, commit, and fire its acks for batches nothing
        // here has snapshotted — a loss window every time a pipeline is
        // created under a running one. Dropped (leaving again) if the build
        // fails.
        let participant = ctx.checkpoints.as_ref().map(|checkpoints| {
            join(self, checkpoints, Arc::clone(&ctx.buckets), positions.clone())
        });
        let mut inputs = Vec::with_capacity(self.config.inputs.len());
        for (index, i) in self.config.inputs.iter().cloned().enumerate() {
            ctx.restored_position = positions.get(index).cloned().flatten();
            let built = i.build(&mut ctx);
            ctx.restored_position = None;
            inputs.push(built?);
        }
        let mut outputs = Vec::with_capacity(self.config.outputs.len());
        for o in self.config.outputs.iter().cloned() {
            outputs.push(o.build(&mut ctx)?);
        }
        let history = Arc::clone(&ctx.history);
        let watchers = ctx.watchers.clone();
        let mut runtime = PipelineRuntime::from_parts(
            inputs,
            transforms,
            outputs,
            Arc::clone(self),
            ctx.events.clone(),
        )?
        .with_history(history)
        .with_watchers(watchers);
        if let Some(participant) = participant {
            if let Some(snapshot) = &restored {
                runtime.restore(snapshot);
                tracing::info!("[{}]\t restored from its checkpoint", self.id);
            }
            runtime = runtime.with_participant(participant);
        }
        Ok(runtime)
    }
    /// Build the run loop and throw it away: everything [`Pipeline::start`]
    /// checks, and nothing it starts. See [`crate::check`].
    pub(crate) fn build_unstarted(self: &Arc<Self>, ctx: BuildCtx) -> anyhow::Result<()> {
        self.create_runtime(ctx).map(drop)
    }
    pub fn start(self: &Arc<Self>, ctx: BuildCtx) -> anyhow::Result<tokio::task::JoinHandle<()>> {
        let runtime = self.create_runtime(ctx)?;
        Ok(tokio::task::spawn(async move {
            let shared = Arc::clone(&runtime.shared);
            match runtime.run().await {
                Ok(()) => debug!("pipeline {} exited successfully", shared.id),
                Err(e) => error!("pipeline {} exited with error: {:?}", shared.id, e),
            }
        }))
    }
    /// A poisoned lock only means some other task panicked while pushing or
    /// cloning this vec; the vec itself can't be left inconsistent, so we
    /// recover rather than propagate a panic into every downstream send.
    fn lock_senders(&self) -> std::sync::MutexGuard<'_, Vec<Downstream>> {
        self.downstream_senders.lock().unwrap_or_else(|poisoned| {
            tracing::warn!(
                "[{}]\t downstream senders lock was poisoned; recovering",
                self.id
            );
            poisoned.into_inner()
        })
    }

    fn downstream_senders(&self) -> Vec<Downstream> {
        self.lock_senders().clone()
    }

    /// Hand every batch this pipeline produces to `tx` as well. Barriers are
    /// not sent here — see [`Pipeline::subscribe_edges`] for the subscriber
    /// that wants them.
    pub fn subscribe(&self, tx: mpsc::Sender<Arc<MessageBatch>>) {
        self.lock_senders().push(Downstream::Batches(tx));
    }

    /// The same, for a downstream pipeline: batches *and* the checkpoint
    /// barriers between them, in the order they were produced.
    pub fn subscribe_edges(&self, tx: mpsc::Sender<Edge>) {
        self.lock_senders().push(Downstream::Edges(tx));
    }

    /// Take every downstream away from this pipeline, for a replacement to
    /// [`Pipeline::adopt_subscribers`]. What lets `PUT /api/pipelines/{id}`
    /// change a pipeline in the middle of a graph without the pipelines under
    /// it noticing: their inputs keep the receiving end of the channel, so as
    /// long as somebody holds the sending end they are not told the upstream
    /// has gone — see [`crate::state::AppState::replace_pipeline`].
    ///
    /// A pass already underway holds its own clone of the list and may still
    /// hand one batch on; the replacement waits for this pipeline's run loop
    /// to end before it starts the new one, so the two never interleave.
    pub(crate) fn take_subscribers(&self) -> Subscribers {
        Subscribers(std::mem::take(&mut *self.lock_senders()))
    }

    /// Hand every batch this pipeline produces to the downstreams another one
    /// had — the other half of [`Pipeline::take_subscribers`]. Ones whose
    /// receiver went while they were in between are left behind.
    pub(crate) fn adopt_subscribers(&self, subscribers: Subscribers) {
        let mut senders = self.lock_senders();
        senders.extend(subscribers.0.into_iter().filter(|tx| !tx.is_closed()));
    }

    /// How many downstreams this pipeline is currently handing batches to.
    #[must_use]
    pub fn downstream_count(&self) -> usize {
        self.lock_senders().len()
    }

    /// Forgets the downstreams whose receivers are gone.
    ///
    /// Called from the run loop when a send actually fails — a deleted
    /// pipeline, or a sample of a `pipeline` input that has finished reading —
    /// so nothing here is paid for on a pass where every downstream is alive.
    fn prune_downstream(&self) {
        self.lock_senders().retain(|tx| !tx.is_closed());
    }
    pub fn view(&self) -> PipelineView<'_> {
        PipelineView {
            id: &self.id,
            config: &self.config,
            status: self.status(),
        }
    }
}
