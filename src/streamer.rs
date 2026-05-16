use crate::BuildCtx;
use crate::config::Config;
use crate::inputs::Input;
use crate::outputs::Output;
use crate::state::StreamerId;
use crate::transforms::Transform;
use anyhow::Result;
use serde::Serialize;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::select;
use tokio::sync::mpsc;

#[derive(Serialize)]
pub struct Streamer {
    pub id: StreamerId,
    pub config: Config,
    #[serde(skip)]
    pub cancellation_token: tokio_util::sync::CancellationToken,
    #[serde(skip)]
    downstream_senders: Mutex<Vec<mpsc::Sender<Arc<serde_json::Value>>>>,
}

#[derive(Serialize)]
pub struct StreamerView<'a> {
    id: &'a StreamerId,
    config: &'a Config,
}

async fn next_input_message(input: &mut Input) -> Arc<serde_json::Value> {
    match input {
        Input::Dummy => {
            tokio::time::sleep(Duration::from_secs(1)).await;
            Arc::new(serde_json::json!({"hello": "streamer"}))
        }
        Input::Streamer(rx) => rx.recv().await.expect("upstream channel closed"),
        Input::Nats(_) => todo!(),
    }
}

pub struct StreamerRuntime {
    input: Input,
    transforms: Vec<Transform>,
    output: Output,
    shared: Arc<Streamer>,
}

impl StreamerRuntime {
    async fn run(mut self) {
        println!("[{}]\t inside StreamerRuntime::run()", self.shared.id);
        loop {
            let next_msg = select! {
                _ = self.shared.cancellation_token.cancelled() => {
                    println!("[{}]\t cancellation received, shutting down", self.shared.id);
                    break;
                }
                msg = next_input_message(&mut self.input) => msg,
            };

            // transform; skip for now
            match self.output {
                Output::Stdout => {
                    println!("[{}]\t {:?}", self.shared.id, next_msg.to_string());
                }
            }

            // send to downstream subscribers
            let senders = self.shared.downstream_senders.lock().unwrap().clone();
            for tx in &senders {
                let _ = tx.send(Arc::clone(&next_msg)).await;
            }
        }
    }
}

impl Streamer {
    pub fn new(config: Config) -> Self {
        let id = petname::petname(3, "-").unwrap();
        let cancellation_token = tokio_util::sync::CancellationToken::new();
        Self {
            id,
            cancellation_token,
            config,
            downstream_senders: Mutex::new(Vec::new()),
        }
    }

    fn create_runtime(self: &Arc<Self>, mut ctx: BuildCtx) -> Result<StreamerRuntime> {
        Ok(StreamerRuntime {
            input: self.config.input.clone().build(&mut ctx)?,
            transforms: Vec::new(),
            output: Output::Stdout,
            shared: Arc::clone(self),
        })
    }
    pub fn start(self: &Arc<Self>, ctx: BuildCtx) -> tokio::task::JoinHandle<()> {
        let runtime = self.create_runtime(ctx).unwrap();
        tokio::task::spawn(async move {
            runtime.run().await;
        })
    }
    pub fn subscribe(&self, tx: mpsc::Sender<Arc<serde_json::Value>>) -> Result<()> {
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
