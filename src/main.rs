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
use kayak::auth::Auth;
use kayak::banner;
use kayak::listen;
use kayak::secrets::{ChainStore, EnvStore, FileStore, SecretStore};
use kayak::state::AppState;
use kayak::history::History;
use kayak_core::server_config::ServerConfig;
use std::net::SocketAddr;

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
    /// How the server itself is run, as JSON or YAML — who is allowed to reach
    /// it, and whatever else belongs to the deployment rather than to the
    /// graph. Not derived from the config's name, unlike the connections file:
    /// it belongs to the process, so two configs served by one server share it.
    ///
    /// **Without this flag the server authenticates nobody**, which is what
    /// makes a local `just dev` work and what keeps an existing deployment
    /// running unchanged. See `kayak_core::server_config` for the file's shape.
    #[arg(long)]
    server_config: Option<PathBuf>,
    /// The address and port to bind, as one value — `127.0.0.1:6767`,
    /// `0.0.0.0:6767`, `[::]:6767`. One argument rather than a host beside a
    /// port because that is what binding takes, and because reassembling the
    /// two gets IPv6 wrong.
    ///
    /// Without this flag the address is whatever the leptos options already
    /// say: `LEPTOS_SITE_ADDR` if it is set — which is how `cargo leptos
    /// watch` and the container image both speak — and otherwise the
    /// `site-addr` in `Cargo.toml`. See `kayak::listen` for why this is not
    /// defaulted here.
    ///
    /// Loopback is reachable only from this machine; anything else is
    /// reachable from wherever that interface is, which on an unauthenticated
    /// server means anyone who can reach the port can rewrite the config.
    #[arg(long)]
    listen: Option<SocketAddr>,
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

/// Say so, once, when an unauthenticated server is reachable from off the
/// machine. The decision is `listen::is_open_to_the_network`, which is where
/// the reasoning and the tests are; this is only the line.
fn warn_if_open_to_the_network(config: &ServerConfig, addr: SocketAddr) {
    if !listen::is_open_to_the_network(config, addr) {
        return;
    }
    tracing::warn!(
        "no authentication is configured and {addr} is not loopback: anyone who can reach \
         this port can create and delete pipelines, edit connections and rewrite the config \
         file. Pass --server-config with an 'auth' section to require a login."
    );
}

/// One dependency silenced by name, because what it says is not true here.
///
/// The OPC UA client loads an *application instance certificate* from a pki
/// directory when a session is built, and logs at ERROR when there is none.
/// kayak has none on purpose — every session it opens is
/// `SecurityPolicy::None`, so there is nothing to sign with and nothing to sign
/// (see `OpcuaConnection`) — which made two ERROR lines about a missing
/// certificate appear on every connect of a pipeline that was working
/// perfectly.
///
/// Only the module whose whole job is reading those files is turned off, and
/// only until the connection grows a security policy: at that point a
/// certificate that cannot be read *is* the error it claims to be, and this
/// comes back out. Everything else the client logs, including the rest of the
/// crypto and the secure channel, is left alone.
const QUIET: &str = "opcua_crypto::certificate_store=off";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    // before the subscriber is built, so no line of it is prefixed with a
    // timestamp and a level — see `kayak::banner`.
    println!("{}", banner::banner(banner::version()));
    let level = if args.debug {
        Level::DEBUG
    } else {
        Level::INFO
    };

    tracing_subscriber::fmt()
        .with_env_filter(match level {
            Level::DEBUG => format!("info,pipeline=debug,{QUIET}"),
            _ => format!("info,{QUIET}"),
        })
        .init();

    let secrets = secret_store(args.secrets.as_ref()).context("failed to load secrets")?;
    // the accounts resolve against the same store the pipelines do — one place
    // a `${NAME}` can come from, whether it is a broker's password or a login
    let secrets_for_auth = Arc::clone(&secrets);
    let server_config = match &args.server_config {
        Some(path) => {
            tracing::info!("Loading server config from {}", path.display());
            kayak::server_config::read_required(path)?
        }
        None => ServerConfig::default(),
    };
    let connections = args.connections.as_deref();
    let data_dir = args.data_dir.clone();
    // Built before the state because loading a config *starts* its pipelines,
    // and each one captures the store as it builds — see
    // `AppState::from_config_with`.
    let history = Arc::new(History::new(server_config.history.clone()));
    let state = match &args.config {
        Some(path) => {
            AppState::from_config_with(path, secrets, connections, data_dir, Arc::clone(&history))
                .context("failed to initialize app state from config")?
        }
        None => AppState::with_secrets_and_connections(secrets, connections)
            .context("failed to load connections")?
            .with_data_dir(data_dir)
            .context("failed to prepare the data directory")?
            .with_history(Arc::clone(&history)),
    };

    let auth = Arc::new(
        Auth::from_config(&server_config, secrets_for_auth.as_ref())
            .context("failed to load the accounts")?,
    );
    // the jwt scheme's startup fetch, and a no-op for every other scheme. It
    // fails the server on purpose: a jwt server with no keys is a server
    // nobody can enter, and a crash loop names the problem where a mysterious
    // wall of 401s would not.
    auth.prime()
        .await
        .context("failed to load the identity provider's signing keys")?;
    let state = state.with_auth(auth);

    let conf = get_configuration(None)?;
    let leptos_options = conf.leptos_options;
    let routes = generate_route_list(App);

    let state = Arc::new(state);
    // One wake-up every five seconds for the life of the process, and nothing
    // at all when history is turned off. See `kayak::history::sampler`.
    tokio::spawn(kayak::history::sampler(Arc::clone(&state)));

    let api = api_router(Arc::clone(&state));

    let leptos = Router::new()
        .leptos_routes(&leptos_options, routes, {
            let opts = leptos_options.clone();
            move || shell(opts.clone())
        })
        // The embedded site first, then leptos' own handler for everything
        // that is not a file — see `kayak::site`. Without the `embed-assets`
        // feature this *is* leptos' handler, so a dev build is unchanged.
        .fallback(kayak::site::fallback(shell))
        .with_state(leptos_options.clone());
    let app = api.merge(leptos);
    // the flag if one was given, otherwise whatever LEPTOS_SITE_ADDR or the
    // Cargo.toml `site-addr` already settled on, and `listen::DEFAULT_ADDR`
    // when nothing did — and the address that is logged is the one that is
    // bound, which is the whole point of resolving it in one place.
    //
    // Reading the variable rather than comparing the address is the whole of
    // the rule: leptos' own default is indistinguishable from someone asking
    // for that address, and only one of those should be overridden. This is
    // the one place it is read; `listen::resolve` stays pure.
    let site_addr_was_set = std::env::var_os("LEPTOS_SITE_ADDR").is_some();
    let addr = listen::resolve(args.listen, leptos_options.site_addr, site_addr_was_set);
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("failed to bind {addr}"))?;
    tracing::info!("Listening on {addr}");
    if !kayak::site::is_embedded() {
        // A blank canvas with a 404 for the WASM bundle is the failure this
        // line exists to name, and the browser's network tab is a bad place to
        // find it. See `kayak::site`.
        tracing::info!(
            "serving the frontend from {} (this binary was built without the 'embed-assets' \
             feature, so the site directory has to be beside it)",
            leptos_options.site_root
        );
    }
    warn_if_open_to_the_network(&server_config, addr);
    // with_connect_info rather than the plain make service: it is what puts the
    // peer address in the request extensions, which is where the `http` input's
    // `remote_addr` metadata is read from. Nothing fails without it — the
    // address is simply absent, as it is in the tests that drive the router
    // directly — so this is the only place it has to be asked for.
    // Everything from here down is the shutdown path, and the order is the
    // whole of it — see `kayak::shutdown`. The signal comes first, then the
    // token that ends the `/events` streams, and only then does axum drain: a
    // drain that started first would wait on an SSE response that never ends.
    let shutdown = Arc::clone(&state);
    let server = axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .with_graceful_shutdown(async move {
        kayak::shutdown::requested().await;
        shutdown.begin_shutdown();
    });

    // axum's graceful shutdown has no timeout and waits for every connection
    // still open, so one wedged client would hold the process here until a
    // second signal killed it — which is the behaviour being removed. The
    // deadline starts when the shutdown does, not when the server did.
    let deadline = {
        let token = state.shutdown_token();
        async move {
            token.cancelled().await;
            tokio::time::sleep(kayak::shutdown::DRAIN_GRACE).await;
        }
    };
    tokio::select! {
        result = server => result.context("server error")?,
        () = deadline => tracing::warn!(
            "connections were still open after {}s; stopping the pipelines anyway",
            kayak::shutdown::DRAIN_GRACE.as_secs()
        ),
    }

    // Only now are the run loops stopped, which is what gives every output its
    // `finish` — the `file` output's closing bracket and the `s3` output's
    // buffered part, which exists nowhere else.
    state.shutdown().await;
    Ok(())
}
