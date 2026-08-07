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
use std::sync::{Arc, Mutex};
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

pub struct PipelineRuntime {
    /// Every configured input, merged into one — see [`crate::inputs::merge`].
    input: Box<dyn InputSource>,
    transforms: Vec<Box<dyn Transform>>,
    outputs: Vec<Box<dyn OutputDestination>>,
    shared: Arc<Pipeline>,
    events: broadcast::Sender<UiEvent>,
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
        })
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
            // The batch as it arrived, before any transform has touched it —
            // one half of what a card's log shows, the other being what leaves
            // at the outputs below.
            publish(&self.events, || {
                UiEvent::batch(self.shared.id.clone(), Stage::Input, Arc::clone(&next_msg))
                    .seq(pass)
            });
            let mut batches = vec![next_msg];
            for (index, t) in self.transforms.iter_mut().enumerate() {
                let mut next = Vec::new();
                for b in batches {
                    match t.apply(b).await {
                        Ok(b) => next.extend(b),
                        // the batch is dropped and the loop moves on to the
                        // next one — one bad batch must not stop the pipeline
                        Err(e) => {
                            error!("[{}]\t transform error: {:?}", self.shared.id, e);
                            publish(&self.events, || {
                                UiEvent::error(self.shared.id.clone(), Stage::Transform, &e)
                                    .seq(pass)
                                    .component(index)
                            });
                        }
                    }
                }
                batches = next;
            }

            // What the transforms produced, reported before the outputs are
            // given it: the log is a record of what passed through this
            // pipeline, which is true whether or not an output then took it.
            for b in &batches {
                publish(&self.events, || {
                    UiEvent::batch(self.shared.id.clone(), Stage::Output, Arc::clone(b)).seq(pass)
                });
            }

            for b in &batches {
                // every output gets every batch. a failing one shouldn't tear
                // the pipeline down — its siblings and the downstream pipelines
                // are still fed, same as we do for transform errors
                for (index, output) in self.outputs.iter_mut().enumerate() {
                    if let Err(e) = output.emit(b.clone()).await {
                        error!("[{}]\t output error: {:?}", self.shared.id, e);
                        publish(&self.events, || {
                            UiEvent::error(self.shared.id.clone(), Stage::Output, &e)
                                .seq(pass)
                                .component(index)
                        });
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
