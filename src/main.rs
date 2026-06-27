use anyhow::Context;
use axum::{
    Router,
    routing::{delete, get, post},
};
use frontend::app::{App, shell};
use leptos::config::get_configuration;
use leptos_axum::{LeptosRoutes, generate_route_list};
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

    let conf = get_configuration(None)?;
    let leptos_options = conf.leptos_options;
    let routes = generate_route_list(App);

    let api = Router::new()
        .route("/docs", get(get_docs))
        .route("/events", get(events_handler))
        // .route("/ui", get(index_handler))
        .route("/api/streams", post(create_stream))
        .route("/api/streams", get(get_streams))
        .route("/api/streams/{stream_id}", delete(delete_stream))
        .with_state(Arc::new(state));

    let leptos = Router::new()
        .leptos_routes(&leptos_options, routes, {
            let opts = leptos_options.clone();
            move || shell(opts.clone())
        })
        .fallback(leptos_axum::file_and_error_handler(shell))
        .with_state(leptos_options.clone());
    let app = api.merge(leptos);
    let listener = tokio::net::TcpListener::bind(&leptos_options.site_addr).await?;
    let _ = axum::serve(listener, app.into_make_service()).await;
    hello!();
    Ok(())
}
