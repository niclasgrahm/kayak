use serde::Serialize;

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
    pub streamers: Mutex<HashMap<StreamerId, StreamerHandle>>,
}

impl AppState {
    pub fn new() -> Self {
        tracing::debug!("Initializing empty server state...");
        Self {
            streamers: Mutex::new(HashMap::new()),
        }
    }
    pub fn from_config(path: &PathBuf) -> Self {
        tracing::debug!("Loading initial configuration from {:?}...", path);
        Self {
            streamers: Mutex::new(HashMap::new()),
        }
    }

    pub fn create_streamer(&self) -> anyhow::Result<()> {
        // i guess this takes a config object
        // parses it
        // builds and starts the streamer
        // adds it to the hashmap
        // returns the streamer
        todo!()
    }

    pub fn delete_streamer(&self, id: StreamerId) -> anyhow::Result<()> {
        todo!()
    }
}
