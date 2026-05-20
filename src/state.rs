use serde::Serialize;

use crate::streamer::Streamer;
use std::collections::HashMap;
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
