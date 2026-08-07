// The server renders the same Leptos tree the frontend does, so it hits the
// same wall: the nested view types on the canvas page overflow rustc's default
// type-layout query depth. `frontend/src/lib.rs` raises it for the hydrate
// build; this is the SSR half of the same problem.
#![recursion_limit = "512"]

use anyhow::Context;
use axum::Router;
use frontend::app::{App, shell};
use leptos::config::get_configuration;
use leptos_axum::{LeptosRoutes, generate_route_list};
use std::path::PathBuf;
use std::sync::Arc;
use tracing::Level;

use clap::Parser;
use kayak::api_router;
use kayak::secrets::{ChainStore, EnvStore, FileStore, SecretStore};
use kayak::state::AppState;

#[derive(Parser)]
struct Args {
    #[arg(long)]
    debug: bool,
    /// The pipelines to start with, as JSON or YAML. The extension decides
    /// which: `.yaml`/`.yml` is read as YAML, anything else as JSON.
    #[arg(long)]
    config: Option<PathBuf>,
    /// JSON file of `"NAME": "value"` pairs that `${NAME}` references in the
    /// config resolve against. Keep it out of version control; mount it at
    /// deploy time. Environment variables are always consulted first, so a
    /// single secret can be overridden for one run without editing the file.
    #[arg(long)]
    secrets: Option<PathBuf>,
    #[arg(long, default_value_t = 6767)]
    port: u16,
}

/// The environment ahead of the secrets file, so an env var wins on a name
/// collision.
fn secret_store(path: Option<&PathBuf>) -> anyhow::Result<Arc<dyn SecretStore>> {
    let mut stores: Vec<Box<dyn SecretStore>> = vec![Box::new(EnvStore)];
    if let Some(path) = path {
        tracing::info!("Loading secrets from {}", path.display());
        stores.push(Box::new(FileStore::from_path(path)?));
    }
    Ok(Arc::new(ChainStore::new(stores)))
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
            Level::DEBUG => "info,pipeline=debug",
            _ => "info",
        })
        .init();
    let addr = format!("0.0.0.0:{}", args.port);
    tracing::info!("Starting server on {}", addr);

    let secrets = secret_store(args.secrets.as_ref()).context("failed to load secrets")?;
    let state = match &args.config {
        Some(path) => AppState::from_config_with_secrets(path, secrets)
            .context("failed to initialize app state from config")?,
        None => AppState::with_secrets(secrets),
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
