use crate::BuildCtx;
use streamer_core::config::Config;
use crate::inputs::InputSource;
use crate::inputs::MessageBatch;
use crate::outputs::OutputDestination;
use crate::state::StreamerId;
use crate::state::UiEvent;
use crate::transforms::Transform;
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
    async fn run(mut self) -> anyhow::Result<()> {
        debug!("[{}]\t inside StreamerRuntime::run()", self.shared.id);
        self.output.init().await?;
        loop {
            let next_msg = match select! {
                () = self.shared.cancellation_token.cancelled() => break,
                msg = next_input_message(&mut self.input) => msg,
            } {
                Ok(msg) => msg,
                Err(e) => {
                    eprintln!("[{}]\t input error: {:?}", self.shared.id, e);
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
                    next.extend(t.apply(b).await?);
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
                self.output.emit(b.clone()).await?;
                let senders = self.shared.downstream_senders.lock().unwrap().clone();
                for tx in &senders {
                    let _ = tx.send(Arc::clone(b)).await;
                }
            }
        }
        Ok(())
    }
}

impl Streamer {
    pub fn new(config: Config) -> Self {
        let id = match config.id.clone() {
            Some(id) => id,
            None => petname::petname(3, "-").unwrap(),
        };
        let cancellation_token = tokio_util::sync::CancellationToken::new();
        Self {
            id,
            cancellation_token,
            config,
            downstream_senders: Mutex::new(Vec::new()),
        }
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
    pub fn start(self: &Arc<Self>, ctx: BuildCtx) -> tokio::task::JoinHandle<()> {
        let runtime = self.create_runtime(ctx).unwrap();
        tokio::task::spawn(async move {
            let shared = Arc::clone(&runtime.shared);
            match runtime.run().await {
                Ok(_) => debug!("streamer {} exited successfully", shared.id),
                Err(e) => error!("streamer {} exited with error: {:?}", shared.id, e),
            }
        })
    }
    pub fn subscribe(&self, tx: mpsc::Sender<Arc<MessageBatch>>) -> Result<()> {
        let mut senders = self.downstream_senders.lock().unwrap();
        senders.push(tx);
        Ok(())
    }
    pub fn view(&self) -> StreamerView<'_> {
        StreamerView {
            id: &self.id,
            config: &self.config,
        }
    }
}
