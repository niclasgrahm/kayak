//! What the process does when it is asked to stop.
//!
//! There was nothing here before, and the absence was the bug: `axum::serve`
//! was awaited bare, so the only thing that ever ended the process was the
//! kernel's default action for a signal it had no handler for. Nothing ran on
//! the way out — which is worse than untidy, because [`OutputDestination::
//! finish`] is where a `file` output closes its `json_array` and where the `s3`
//! output uploads the part it has been accumulating **in memory**. A `^C` threw
//! the second one away entirely.
//!
//! It is worse still under docker. The image's `ENTRYPOINT` is the binary, so
//! kayak is pid 1, and pid 1 has no default disposition for anything: a signal
//! with no handler installed is *ignored*. `docker stop` sent a SIGTERM into a
//! void and then killed it with SIGKILL ten seconds later, every time.
//!
//! [`requested`] is the half of the answer that watches for the signal. The
//! other half is [`crate::state::AppState::shutdown`], which stops the graph;
//! `main` is what joins them, in the order the whole thing turns on:
//!
//! 1. a signal arrives,
//! 2. the shutdown token is cancelled, which is what ends the `/events`
//!    streams — see [`crate::state::AppState::shutdown_token`],
//! 3. axum stops accepting and drains the connections that are left,
//! 4. the pipelines are cancelled and awaited, so every output gets its
//!    `finish`.
//!
//! Steps 2 and 3 are one step in the wrong order: draining first would wait
//! forever on an SSE stream that by design never ends.
//!
//! [`OutputDestination::finish`]: crate::outputs::OutputDestination::finish

use std::time::Duration;

/// How long the connections left over from step 3 get before the process stops
/// waiting for them.
///
/// It bounds the one thing that can't be interrupted: a request whose handler
/// is inside something slow. `axum`'s graceful shutdown has no timeout of its
/// own and waits for every open connection, so without this a single wedged
/// client would keep the process alive until someone sent it a second signal —
/// which is precisely the behaviour this module exists to remove.
pub const DRAIN_GRACE: Duration = Duration::from_secs(10);

/// Resolves when the operator asks the process to stop.
///
/// SIGINT and SIGTERM both, and deliberately: a `^C` at a terminal and a
/// `docker stop` are the same request, and handling only the first would leave
/// the container case — the one that matters in production — exactly as broken
/// as it was.
///
/// Untested, and that is not an oversight: the only honest test raises a real
/// signal at the test process, which every other test in the binary shares.
/// The body is two lines with no branching precisely so that there is nothing
/// here for a test to catch; what *can* go wrong is the wiring around it, and
/// `AppState::shutdown` and the `/events` stream are both tested directly.
pub async fn requested() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        // Failing to install a handler is not survivable: carrying on would
        // mean a process that looks like it shuts down cleanly and doesn't.
        let mut terminate = match signal(SignalKind::terminate()) {
            Ok(stream) => stream,
            Err(err) => {
                tracing::error!("could not listen for SIGTERM ({err}); waiting for SIGINT only");
                let _ = tokio::signal::ctrl_c().await;
                return;
            }
        };
        tokio::select! {
            _ = tokio::signal::ctrl_c() => tracing::info!("received SIGINT, shutting down"),
            _ = terminate.recv() => tracing::info!("received SIGTERM, shutting down"),
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
        tracing::info!("received an interrupt, shutting down");
    }
}
