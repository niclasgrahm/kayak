use crate::BuildCtx;
use streamer_core::config::Config;
use crate::inputs::InputSource;
use crate::inputs::MessageBatch;
use crate::outputs::OutputDestination;
use crate::state::StreamerId;
use crate::state::UiEvent;
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

pub struct StreamerRuntime {
    input: Box<dyn InputSource>,
    transforms: Vec<Box<dyn Transform>>,
    output: Box<dyn OutputDestination>,
    shared: Arc<Streamer>,
    events: broadcast::Sender<UiEvent>,
}

impl StreamerRuntime {
    /// Assemble a runtime from already-built components, bypassing the config
    /// layer. This is the seam integration tests use to drive the run loop with
    /// scripted inputs and collecting outputs; production code goes through
    /// [`Streamer::start`].
    pub fn from_parts(
        input: Box<dyn InputSource>,
        transforms: Vec<Box<dyn Transform>>,
        output: Box<dyn OutputDestination>,
        shared: Arc<Streamer>,
        events: broadcast::Sender<UiEvent>,
    ) -> Self {
        Self {
            input,
            transforms,
            output,
            shared,
            events,
        }
    }

    /// Run until the input errors or the streamer is cancelled.
    pub async fn run(mut self) -> anyhow::Result<()> {
        self.output.init().await?;
        loop {
            let next_msg = match select! {
                () = self.shared.cancellation_token.cancelled() => break,
                msg = next_input_message(&mut self.input) => msg,
            } {
                Ok(msg) => msg,
                Err(e) => {
                    error!("[{}]\t input error, stopping streamer: {:?}", self.shared.id, e);
                    break;
                }
            };
            // NOTE this is temporary! Send input to web client
            if self.events.receiver_count() > 0 {
                let _ = self.events.send(UiEvent {
                    streamer_id: self.shared.id.clone(),
                    stage: "input".to_string(),
                    batch: Arc::clone(&next_msg),
                });
            }
            // END NOTE this is temporary! Send input to web client
            let mut batches = vec![next_msg];
            for t in &mut self.transforms {
                let mut next = Vec::new();
                for b in batches {
                    match t.apply(b).await {
                        Ok(b) => next.extend(b),
                        // the batch is dropped and the loop moves on to the
                        // next one — one bad batch must not stop the pipeline
                        Err(e) => error!("[{}]\t transform error: {:?}", self.shared.id, e),
                    }
                }
                batches = next;
            }

            // NOTE this is temporary! Send output to web client
            if self.events.receiver_count() > 0 {
                for b in &batches {
                    let _ = self.events.send(UiEvent {
                        streamer_id: self.shared.id.clone(),
                        stage: "output".to_string(),
                        batch: Arc::clone(b),
                    });
                }
            }
            // END NOTE this is temporary! Send output to web client

            for b in &batches {
                // a failing output shouldn't tear the pipeline down — downstream
                // streamers are still fed, same as we do for transform errors
                if let Err(e) = self.output.emit(b.clone()).await {
                    error!("[{}]\t output error: {:?}", self.shared.id, e);
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
        Ok(StreamerRuntime {
            input: self.config.input.clone().build(&mut ctx)?,
            transforms,
            output: self.config.output.clone().build(&mut ctx)?,
            shared: Arc::clone(self),
            events: ctx.events.clone(),
        })
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
