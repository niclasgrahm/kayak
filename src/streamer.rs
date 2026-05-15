use crate::BuildCtx;
use crate::config::Config;
use crate::inputs::Input;
use crate::outputs::Output;
use crate::state::StreamerId;
use crate::transforms::Transform;

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use std::time;
use tokio::sync::mpsc;

#[derive(Serialize)]
pub struct Streamer {
    pub id: StreamerId,
    pub config: Config,
    #[serde(skip)]
    downstream_senders: Mutex<Vec<mpsc::Sender<Arc<serde_json::Value>>>>,
}

#[derive(Serialize)]
pub struct StreamerView<'a> {
    id: &'a StreamerId,
    config: &'a Config,
}

pub struct StreamerRuntime {
    input: Input,
    transforms: Vec<Transform>,
    output: Output,
    shared: Arc<Streamer>,
}

impl StreamerRuntime {
    async fn run(&self) {
        println!("[{}]\t inside StreamerRuntime::run()", self.shared.id);
        loop {
            let next_msg = match self.input {
                Input::Dummy => {
                    tokio::time::sleep(time::Duration::from_secs(1)).await;
                    Arc::new(serde_json::json!({"hello": "streamer"}))
                }
                _ => {
                    todo!()
                }
            };

            // transform; skip for now
            match self.output {
                Output::Stdout => {
                    println!("[{}]\t {:?}", self.shared.id, next_msg.to_string());
                }
            }
        }
    }
}

impl Streamer {
    pub fn new(config: Config) -> Self {
        let id = petname::petname(3, "-").unwrap();
        Self {
            id,
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
    pub fn start(self: &Arc<Self>, mut ctx: BuildCtx) -> tokio::task::JoinHandle<()> {
        let runtime = self.create_runtime(ctx).unwrap();
        tokio::task::spawn(async move {
            runtime.run().await;
        })
    }
    fn subscribe(&self, tx: mpsc::Sender<Arc<serde_json::Value>>) -> Result<()> {
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
