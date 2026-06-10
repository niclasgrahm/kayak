use serde::Serialize;
use tokio::sync::broadcast;

use crate::BuildCtx;
use crate::config::Config;
use crate::inputs::MessageBatch;
use crate::streamer::Streamer;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

pub type StreamerId = String;

#[derive(Serialize)]
pub struct StreamerHandle {
    #[serde(skip)]
    pub join_handle: tokio::task::JoinHandle<()>,
    pub shared: Arc<Streamer>,
}

pub struct AppState {
    streamers: Mutex<HashMap<StreamerId, StreamerHandle>>,
    events: broadcast::Sender<UiEvent>,
}

impl AppState {
    pub fn new() -> Self {
        tracing::debug!("Initializing empty server state...");
        let (events, _) = broadcast::channel(1024);
        Self {
            streamers: Mutex::new(HashMap::new()),
            events,
        }
    }

    pub fn subscribe_events(&self) -> broadcast::Receiver<UiEvent> {
        self.events.subscribe()
    }

    pub fn from_config(path: &PathBuf) -> anyhow::Result<Self> {
        let new_state = AppState::new();
        tracing::debug!("Loading initial configuration from {:?}...", path);
        let file = std::fs::File::open(path)?;
        let configs: Vec<Config> = serde_json::from_reader(file)?;
        for c in configs {
            let _ = new_state.create_streamer(c)?;
        }

        Ok(new_state)
    }

    pub fn get_streamer_ids(&self) -> anyhow::Result<Vec<StreamerId>> {
        match self.streamers.lock() {
            Ok(app) => Ok(app.keys().cloned().collect()),
            Err(err) => Err(anyhow::anyhow!(
                "Failed to acquire lock on streamers: {}",
                err
            )),
        }
    }

    pub fn get_streamers(&self) -> anyhow::Result<serde_json::Value> {
        match self.streamers.lock() {
            Ok(app) => {
                let views: Vec<_> = app.values().map(|h| h.shared.view()).collect();
                Ok(serde_json::to_value(views)?)
            }
            Err(err) => Err(anyhow::anyhow!(
                "failed to acquire lock on streamers: {}",
                err
            )),
        }
    }

    pub fn create_streamer(&self, config: Config) -> anyhow::Result<Arc<Streamer>> {
        let streamer = Arc::new(Streamer::new(config));
        match self.streamers.lock() {
            Ok(mut app) => {
                let id = streamer.id.clone();
                // we require unique ids, so if this id already exists we should error out
                if app.contains_key(id.as_str()) {
                    return Err(anyhow::anyhow!("Streamer with id {} already exists", id));
                }
                let ctx = BuildCtx::new(&mut app, self.events.clone());
                let join_handle = streamer.start(ctx);

                let streamer_handle = StreamerHandle {
                    join_handle,
                    shared: Arc::clone(&streamer),
                };
                app.insert(id, streamer_handle);
                let body = serde_json::to_value(streamer.view())?;
                tracing::debug!("streamer created, config: {}", &body);
                Ok(streamer)
            }
            Err(err) => Err(anyhow::anyhow!(
                "Failed to acquire lock on streamers: {}",
                err
            )),
        }
    }

    pub fn delete_streamer(&self, id: StreamerId) -> anyhow::Result<()> {
        let mut app = self.streamers.lock().unwrap();
        if let Some(streamer) = app.get(id.as_str()) {
            // signal cancellation here
            streamer.shared.cancellation_token.cancel();
            app.remove(id.clone().as_str());
            tracing::debug!("streamer deleted: {}", id);
            Ok(())
        } else {
            tracing::debug!("failed to delete streamer: {}", id);
            Err(anyhow::anyhow!("Streamer with id {} not found", id))
        }
    }
}

#[derive(Clone, Serialize)]
pub struct UiEvent {
    pub streamer_id: StreamerId,
    pub stage: String, //let's see if we like this
    pub batch: Arc<MessageBatch>,
}
