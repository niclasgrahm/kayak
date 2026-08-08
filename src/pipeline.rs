use crate::BuildCtx;
use crate::config::BuildInputConfig;
use crate::events::publish;
use crate::config::BuildOutputConfig;
use crate::config::BuildTransformConfig;
use crate::inputs::InputSource;
use crate::inputs::MessageBatch;
use crate::outputs::OutputDestination;
use crate::state::PipelineId;
use crate::state::UiEvent;
use crate::transforms::Transform;
use anyhow::Context;
use anyhow::Result;
use kayak_core::config::Config;
use kayak_core::Stage;
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
    downstream_senders: Mutex<Vec<mpsc::Sender<Arc<MessageBatch>>>>,
}

// impl Pipeline {
//     pub fn to_dto(&self) -> anyhow::Result<PipelineDto> {
//             Ok(PipelineDto {
//                 id: self.id.clone(),
//                 config: self.config.clone(),
//             })
//         }
//     }

#[derive(Serialize)]
pub struct PipelineView<'a> {
    id: &'a PipelineId,
    config: &'a Config,
}

async fn next_input_message(input: &mut Box<dyn InputSource>) -> Result<Arc<MessageBatch>> {
    input.next().await
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
    fn report_error(&mut self, stage: Stage, component: Option<usize>, now: Instant) -> bool {
        let due = self
            .last_error
            .get(&(stage, component))
            .is_none_or(|last| now.duration_since(*last) >= UI_ERROR_INTERVAL);
        if due {
            self.last_error.insert((stage, component), now);
        }
        due
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
        })
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

    /// Put one batch through the transform chain, in order, and return whatever
    /// came out the far end — which may be several batches, or none.
    ///
    /// A transform that fails drops *that* batch and the chain carries on with
    /// the rest: one bad message must not stop the pipeline.
    async fn apply_transforms(
        &mut self,
        batch: Arc<MessageBatch>,
        pass: u64,
        throttle: &mut UiThrottle,
    ) -> Vec<Arc<MessageBatch>> {
        let mut batches = vec![batch];
        for (index, t) in self.transforms.iter_mut().enumerate() {
            let mut next = Vec::new();
            for b in batches {
                match t.apply(b).await {
                    Ok(b) => next.extend(b),
                    Err(e) => {
                        error!("[{}]\t transform error: {:?}", self.shared.id, e);
                        if throttle.report_error(Stage::Transform, Some(index), Instant::now()) {
                            publish(&self.events, || {
                                UiEvent::error(self.shared.id.clone(), Stage::Transform, &e)
                                    .seq(pass)
                                    .component(index)
                            });
                        }
                    }
                }
            }
            batches = next;
        }
        batches
    }

    /// Run until the input errors or the pipeline is cancelled.
    pub async fn run(mut self) -> anyhow::Result<()> {
        // an output that can't be initialised is fatal: it would never accept a
        // batch, and a pipeline half-writing its outputs is worse than one that
        // says why it didn't start
        for (index, output) in self.outputs.iter_mut().enumerate() {
            if let Err(e) = output.init().await {
                // no `seq`: this is before the first pass, not part of one
                publish(&self.events, || {
                    UiEvent::error(self.shared.id.clone(), Stage::Output, &e).component(index)
                });
                return Err(e);
            }
        }
        // Which pass through the loop we are on. Every event a pass produces
        // carries it, which is what lets the UI show one batch's journey — in,
        // transforms, out — as one thing rather than as unrelated lines that
        // happened to arrive together.
        let mut pass = 0u64;
        let mut throttle = UiThrottle::new(self.pass_interval);
        loop {
            let next_msg = match select! {
                // `biased` so cancellation always wins a tie. Tearing the graph
                // down cancels every pipeline and *then* drops the upstreams,
                // so a downstream is woken with both its cancellation and an
                // "upstream is gone" ready at once — and a random pick would
                // report our own shutdown as a pipeline failure half the time.
                biased;
                () = self.shared.cancellation_token.cancelled() => break,
                msg = next_input_message(&mut self.input) => msg,
            } {
                Ok(msg) => msg,
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
                    error!(
                        "[{}]\t input error, stopping pipeline: {:?}",
                        self.shared.id, e
                    );
                    publish(&self.events, || {
                        UiEvent::error(self.shared.id.clone(), Stage::Input, &e)
                    });
                    break;
                }
            };
            pass += 1;
            // Nobody watching means no clock read and no accounting: reading
            // the clock once per pass is small but it is not free, and a
            // headless run measured about 12% slower when it did it anyway.
            // This is the same gate `publish` applies, hoisted to cover the
            // throttle as well.
            //
            // Nothing is accumulated while unwatched either, so a browser
            // attaching to a pipeline that has been running for a week doesn't
            // get a week's worth of skipped messages on its first event.
            let watching = self.events.receiver_count() > 0;
            // Decided once, here, and used for every batch event this pass
            // produces: reporting the input of a pass whose output was dropped
            // would draw a pass that never finished. See `UiThrottle`.
            let reported = watching && throttle.report_pass();
            // The batch as it arrived, before any transform has touched it —
            // one half of what a card's log shows, the other being what leaves
            // at the outputs below.
            if reported {
                let skipped = throttle.take_skipped(Stage::Input);
                publish(&self.events, || {
                    UiEvent::batch(self.shared.id.clone(), Stage::Input, &next_msg, skipped)
                        .seq(pass)
                });
            } else if watching {
                throttle.skip(Stage::Input, next_msg.len());
            }
            let batches = self.apply_transforms(next_msg, pass, &mut throttle).await;

            // What the transforms produced, reported before the outputs are
            // given it: the log is a record of what passed through this
            // pipeline, which is true whether or not an output then took it.
            for b in &batches {
                if reported {
                    let skipped = throttle.take_skipped(Stage::Output);
                    publish(&self.events, || {
                        UiEvent::batch(self.shared.id.clone(), Stage::Output, b, skipped).seq(pass)
                    });
                } else if watching {
                    throttle.skip(Stage::Output, b.len());
                }
            }

            for b in &batches {
                // every output gets every batch. a failing one shouldn't tear
                // the pipeline down — its siblings and the downstream pipelines
                // are still fed, same as we do for transform errors
                for (index, output) in self.outputs.iter_mut().enumerate() {
                    if let Err(e) = output.emit(b.clone()).await {
                        error!("[{}]\t output error: {:?}", self.shared.id, e);
                        if throttle.report_error(Stage::Output, Some(index), Instant::now()) {
                            publish(&self.events, || {
                                UiEvent::error(self.shared.id.clone(), Stage::Output, &e)
                                    .seq(pass)
                                    .component(index)
                            });
                        }
                    }
                }
                let senders = self.shared.downstream_senders();
                for tx in &senders {
                    if let Err(e) = tx.send(Arc::clone(b)).await {
                        debug!(
                            "[{}]\t dropping batch for a downstream that went away: {}",
                            self.shared.id, e
                        );
                    }
                }
            }
        }
        // However we got out of the loop — cancelled, or an input that died —
        // an output holding an unfinished part gets its one chance to land it.
        // Errors are reported like an `emit` error rather than returned: the
        // pipeline has already stopped, and failing the run *now* would report
        // the shutdown itself as the pipeline's failure.
        for (index, output) in self.outputs.iter_mut().enumerate() {
            if let Err(e) = output.finish().await {
                error!("[{}]\t output failed to finish: {:?}", self.shared.id, e);
                publish(&self.events, || {
                    UiEvent::error(self.shared.id.clone(), Stage::Output, &e).component(index)
                });
            }
        }
        Ok(())
    }
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
        })
    }

    fn create_runtime(self: &Arc<Self>, mut ctx: BuildCtx) -> Result<PipelineRuntime> {
        let mut transforms = Vec::with_capacity(self.config.transforms.len());
        for t in self.config.transforms.iter().cloned() {
            transforms.push(t.build(&mut ctx)?);
        }
        // inputs first: a `pipeline` input registers itself on its upstream as
        // it builds, and an output that fails to build shouldn't leave half a
        // subscription behind — building it last keeps that window as small as
        // the old single-input code had it
        let mut inputs = Vec::with_capacity(self.config.inputs.len());
        for i in self.config.inputs.iter().cloned() {
            inputs.push(i.build(&mut ctx)?);
        }
        let mut outputs = Vec::with_capacity(self.config.outputs.len());
        for o in self.config.outputs.iter().cloned() {
            outputs.push(o.build(&mut ctx)?);
        }
        PipelineRuntime::from_parts(
            inputs,
            transforms,
            outputs,
            Arc::clone(self),
            ctx.events.clone(),
        )
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
    fn lock_senders(&self) -> std::sync::MutexGuard<'_, Vec<mpsc::Sender<Arc<MessageBatch>>>> {
        self.downstream_senders.lock().unwrap_or_else(|poisoned| {
            tracing::warn!(
                "[{}]\t downstream senders lock was poisoned; recovering",
                self.id
            );
            poisoned.into_inner()
        })
    }

    fn downstream_senders(&self) -> Vec<mpsc::Sender<Arc<MessageBatch>>> {
        self.lock_senders().clone()
    }

    pub fn subscribe(&self, tx: mpsc::Sender<Arc<MessageBatch>>) {
        self.lock_senders().push(tx);
    }
    pub fn view(&self) -> PipelineView<'_> {
        PipelineView {
            id: &self.id,
            config: &self.config,
        }
    }
}
