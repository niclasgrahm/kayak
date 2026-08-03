use anyhow::Context;
use axum::Router;
use frontend::app::{App, shell};
use leptos::config::get_configuration;
use leptos_axum::{LeptosRoutes, generate_route_list};
use std::path::PathBuf;
use std::sync::Arc;
use tracing::Level;

use clap::Parser;
use streamer::api_router;
use streamer::state::AppState;

#[derive(Parser)]
struct Args {
    #[arg(long)]
    debug: bool,
    #[arg(long)]
    config: Option<PathBuf>,
    #[arg(long, default_value_t = 6767)]
    port: u16,
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

    let api = api_router(Arc::new(state));

    let leptos = Router::new()
        .leptos_routes(&leptos_options, routes, {
            let opts = leptos_options.clone();
            move || shell(opts.clone())
        })
        .fallback(leptos_axum::file_and_error_handler(shell))
        .with_state(leptos_options.clone());
    let app = api.merge(leptos);
    let listener = tokio::net::TcpListener::bind(&leptos_options.site_addr)
        .await
        .with_context(|| format!("failed to bind {}", leptos_options.site_addr))?;
    axum::serve(listener, app.into_make_service())
        .await
        .context("server error")?;
    Ok(())
}
