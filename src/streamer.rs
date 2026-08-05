use crate::BuildCtx;
use streamer_core::config::Config;
use crate::inputs::InputSource;
use crate::inputs::MessageBatch;
use crate::outputs::OutputDestination;
use crate::state::StreamerId;
use crate::state::UiEvent;
use streamer_core::stage;
use crate::transforms::Transform;
use anyhow::Context;
use anyhow::Result;
use serde::Serialize;
use std::sync::{Arc, Mutex};
use tokio::select;
use tokio::sync::broadcast;
use tokio::sync::mpsc;
use tracing::debug;
use tracing::error;
use crate::config::BuildInputConfig;
use crate::config::BuildTransformConfig;
use crate::config::BuildOutputConfig;

#[derive(Serialize)]
pub struct Streamer {
    pub id: StreamerId,
    pub config: Config,
    #[serde(skip)]
    pub cancellation_token: tokio_util::sync::CancellationToken,
    #[serde(skip)]
    downstream_senders: Mutex<Vec<mpsc::Sender<Arc<MessageBatch>>>>,
}

// impl Streamer {
//     pub fn to_dto(&self) -> anyhow::Result<StreamerDto> {
//             Ok(StreamerDto {
//                 id: self.id.clone(),
//                 config: self.config.clone(),
//             })
//         }
//     }

#[derive(Serialize)]
pub struct StreamerView<'a> {
    id: &'a StreamerId,
    config: &'a Config,
}

async fn next_input_message(input: &mut Box<dyn InputSource>) -> Result<Arc<MessageBatch>> {
    input.next().await
}

/// Publish to the UI feed. Nothing is built or sent when no one is watching —
/// errors reach the server log either way, and this keeps a headless run from
/// paying to describe them. A free function rather than a method so it can be
/// called from inside a loop that already holds `&mut self.transforms`.
fn publish(events: &broadcast::Sender<UiEvent>, event: impl FnOnce() -> UiEvent) {
    if events.receiver_count() > 0 {
        let _ = events.send(event());
    }
}

pub struct StreamerRuntime {
    /// Every configured input, merged into one — see [`crate::inputs::merge`].
    input: Box<dyn InputSource>,
    transforms: Vec<Box<dyn Transform>>,
    outputs: Vec<Box<dyn OutputDestination>>,
    shared: Arc<Streamer>,
    events: broadcast::Sender<UiEvent>,
}

impl StreamerRuntime {
    /// Assemble a runtime from already-built components, bypassing the config
    /// layer. This is the seam integration tests use to drive the run loop with
    /// scripted inputs and collecting outputs; production code goes through
    /// [`Streamer::start`].
    pub fn from_parts(
        inputs: Vec<Box<dyn InputSource>>,
        transforms: Vec<Box<dyn Transform>>,
        outputs: Vec<Box<dyn OutputDestination>>,
        shared: Arc<Streamer>,
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

    /// Run until the input errors or the streamer is cancelled.
    pub async fn run(mut self) -> anyhow::Result<()> {
        // an output that can't be initialised is fatal: it would never accept a
        // batch, and a pipeline half-writing its outputs is worse than one that
        // says why it didn't start
        for output in &mut self.outputs {
            if let Err(e) = output.init().await {
                publish(&self.events, || {
                    UiEvent::error(self.shared.id.clone(), stage::OUTPUT, &e)
                });
                return Err(e);
            }
        }
        loop {
            let next_msg = match select! {
                // `biased` so cancellation always wins a tie. Tearing the graph
                // down cancels every streamer and *then* drops the upstreams,
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
                    // freshly built streamer that inherited its id.
                    if self.shared.cancellation_token.is_cancelled() {
                        debug!(
                            "[{}]\t input stopped while shutting down: {:#}",
                            self.shared.id, e
                        );
                        break;
                    }
                    error!("[{}]\t input error, stopping streamer: {:?}", self.shared.id, e);
                    publish(&self.events, || UiEvent::error(self.shared.id.clone(), stage::INPUT, &e));
                    break;
                }
            };
            // NOTE this is temporary! Send input to web client
            publish(&self.events, || {
                UiEvent::batch(
                    self.shared.id.clone(),
                    stage::INPUT,
                    Arc::clone(&next_msg),
                )
            });
            // END NOTE this is temporary! Send input to web client
            let mut batches = vec![next_msg];
            for t in &mut self.transforms {
                let mut next = Vec::new();
                for b in batches {
                    match t.apply(b).await {
                        Ok(b) => next.extend(b),
                        // the batch is dropped and the loop moves on to the
                        // next one — one bad batch must not stop the pipeline
                        Err(e) => {
                            error!("[{}]\t transform error: {:?}", self.shared.id, e);
                            publish(&self.events, || {
                                UiEvent::error(self.shared.id.clone(), stage::TRANSFORM, &e)
                            });
                        }
                    }
                }
                batches = next;
            }

            // NOTE this is temporary! Send output to web client
            for b in &batches {
                publish(&self.events, || {
                    UiEvent::batch(self.shared.id.clone(), stage::OUTPUT, Arc::clone(b))
                });
            }
            // END NOTE this is temporary! Send output to web client

            for b in &batches {
                // every output gets every batch. a failing one shouldn't tear
                // the pipeline down — its siblings and the downstream streamers
                // are still fed, same as we do for transform errors
                for output in &mut self.outputs {
                    if let Err(e) = output.emit(b.clone()).await {
                        error!("[{}]\t output error: {:?}", self.shared.id, e);
                        publish(&self.events, || {
                            UiEvent::error(self.shared.id.clone(), stage::OUTPUT, &e)
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

impl Streamer {
    pub fn new(config: Config) -> Result<Self> {
        let id = match config.id.clone() {
            Some(id) => id,
            None => petname::petname(3, "-").context("failed to generate a random streamer id")?,
        };
        let cancellation_token = tokio_util::sync::CancellationToken::new();
        Ok(Self {
            id,
            cancellation_token,
            config,
            downstream_senders: Mutex::new(Vec::new()),
        })
    }

    fn create_runtime(self: &Arc<Self>, mut ctx: BuildCtx) -> Result<StreamerRuntime> {
        let mut transforms = Vec::with_capacity(self.config.transforms.len());
        for t in self.config.transforms.iter().cloned() {
            transforms.push(t.build(&mut ctx)?);
        }
        // inputs first: a `streamer` input registers itself on its upstream as
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
        StreamerRuntime::from_parts(
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
                Ok(()) => debug!("streamer {} exited successfully", shared.id),
                Err(e) => error!("streamer {} exited with error: {:?}", shared.id, e),
            }
        }))
    }
    /// A poisoned lock only means some other task panicked while pushing or
    /// cloning this vec; the vec itself can't be left inconsistent, so we
    /// recover rather than propagate a panic into every downstream send.
    fn lock_senders(&self) -> std::sync::MutexGuard<'_, Vec<mpsc::Sender<Arc<MessageBatch>>>> {
        self.downstream_senders.lock().unwrap_or_else(|poisoned| {
            tracing::warn!("[{}]\t downstream senders lock was poisoned; recovering", self.id);
            poisoned.into_inner()
        })
    }

    fn downstream_senders(&self) -> Vec<mpsc::Sender<Arc<MessageBatch>>> {
        self.lock_senders().clone()
    }

    pub fn subscribe(&self, tx: mpsc::Sender<Arc<MessageBatch>>) {
        self.lock_senders().push(tx);
    }
    pub fn view(&self) -> StreamerView<'_> {
        StreamerView {
            id: &self.id,
            config: &self.config,
        }
    }
}
