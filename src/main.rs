use anyhow::Context;
use axum::{
    Router,
    routing::{delete, get, post},
};
use std::sync::Arc;
use std::{collections::HashMap, path::PathBuf};
use tokio::sync::broadcast;
use tracing::Level;

mod config;
mod handlers;
mod inputs;
mod outputs;
mod state;
mod streamer;
mod transforms;
use crate::handlers::{
    rest::streamer::{create_stream, delete_stream, get_streams},
    ui::{docs::get_docs, ui::events_handler},
};
use crate::state::{AppState, StreamerHandle, StreamerId};
use crate::{handlers::ui::ui::index_handler, state::UiEvent};
use clap::Parser;
macro_rules! hello {
    () => {
        println!("Hello, world!");
    };
}

#[derive(Parser)]
struct Args {
    #[arg(long)]
    debug: bool,
    #[arg(long)]
    config: Option<PathBuf>,
    #[arg(long, default_value_t = 6767)]
    port: u16,
}

struct BuildCtx<'a> {
    streamers: &'a mut HashMap<StreamerId, StreamerHandle>,
    events: broadcast::Sender<UiEvent>,
}

impl<'a> BuildCtx<'a> {
    fn new(
        streamers: &'a mut HashMap<StreamerId, StreamerHandle>,
        events: broadcast::Sender<UiEvent>,
    ) -> Self {
        Self { streamers, events }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let level = if args.debug {
        Level::DEBUG
    } else {
        Level::INFO
    };

    tracing_subscriber::fmt()
        .with_env_filter(match level {
            Level::DEBUG => "info,streamer=debug",
            _ => "info",
        })
        .init();
    let addr = format!("0.0.0.0:{}", args.port);
    tracing::info!("Starting server on {}", addr);

    let state = match &args.config {
        Some(path) => {
            AppState::from_config(path).context("failed to initialize app state from config")?
        }
        None => AppState::new(),
    };

    let app = Router::new()
        .route("/ui", get(index_handler))
        .route("/docs", get(get_docs))
        .route("/events", get(events_handler))
        .route("/", post(create_stream))
        .route("/", get(get_streams))
        .route("/{stream_id}", delete(delete_stream))
        .with_state(Arc::new(state));
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .context("failed to bind to address")?;
    let _ = axum::serve(listener, app).await;
    hello!();
    Ok(())
}
