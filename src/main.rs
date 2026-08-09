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
    /// The systems the pipelines connect to, as JSON or YAML — a kafka cluster
    /// or a nats server declared once under a name, which a component's
    /// `connection` field then refers to. Defaults to `<config>.connections.<ext>`
    /// beside the config file; name one here to share a single file between
    /// several configs. Holds `${NAME}` secret references, not secrets.
    #[arg(long)]
    connections: Option<PathBuf>,
    /// The one directory tree file outputs may write pipeline data into,
    /// created if it is not there. **Without this flag file outputs are turned
    /// off** — a component that writes whatever a pipeline carries onto disk is
    /// not something a deployment should get by default, so enabling it is an
    /// explicit act. A `file` connection's root has to resolve inside this
    /// directory, which is what keeps a config posted over HTTP from naming an
    /// arbitrary path. Separate from where `--config` is saved, on purpose.
    #[arg(long)]
    data_dir: Option<PathBuf>,
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
    let connections = args.connections.as_deref();
    let data_dir = args.data_dir.clone();
    let state = match &args.config {
        Some(path) => AppState::from_config_with(path, secrets, connections, data_dir)
            .context("failed to initialize app state from config")?,
        None => AppState::with_secrets_and_connections(secrets, connections)
            .context("failed to load connections")?
            .with_data_dir(data_dir)
            .context("failed to prepare the data directory")?,
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
    // with_connect_info rather than the plain make service: it is what puts the
    // peer address in the request extensions, which is where the `http` input's
    // `remote_addr` metadata is read from. Nothing fails without it — the
    // address is simply absent, as it is in the tests that drive the router
    // directly — so this is the only place it has to be asked for.
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
        .await
        .context("server error")?;
    Ok(())
}
