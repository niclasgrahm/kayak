# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

"kayak" — graph-based stream processing: an Axum server that runs configurable `input → transforms → output` pipelines, with a Leptos web UI.

## Commands

```bash
cargo leptos watch                  # dev server w/ hot reload on 127.0.0.1:6767 (builds WASM + server)
cargo leptos build --release        # production build (server binary + target/site assets)
cargo check                         # fast type check of the whole workspace
just ci                             # lint + test — what GitHub Actions runs
just test                           # cargo test --all-targets (offline: no NATS, no server)
just lint                           # cargo clippy --all-targets -- -D warnings
cargo run -- --config config.json --debug --port 6767   # run server binary directly (no WASM rebuild)

docker compose up                   # NATS on :4222 + a publisher spamming test.subject once/sec
just test-http                      # hurl --test hurl/tests/*.hurl (needs the server running)
just start-baseline                 # hurl hurl/create_baseline.hurl — creates a sample pipeline
```

## Definition of done

These two rules are not negotiable and apply to every change, however small:

1. **New code ships with tests.** Any new or changed behaviour — a component, a handler, a config field, a bug fix — needs a test that fails without the change. A bug fix without a regression test is not a fix. If something genuinely can't be tested offline (a real NATS connection, say), say so explicitly and explain why rather than skipping quietly.
2. **`just ci` must be green before a task is called done.** That's `just lint` (clippy `-D warnings`) plus `just test`. Not "compiles", not "the new test passes" — the whole suite. If tests fail, report the failure and the output; never describe a task as complete with a red suite, and never disable, `#[ignore]` or weaken an existing test to get to green. A test that turns out to encode the wrong behaviour is a conversation to have first, not something to edit away.

Testing is documented in `readme.md` under "testing" — read that before adding tests. In short: the runtime lives in `src/lib.rs` (not `main.rs`) so `tests/` can reach it; `src/testing.rs` holds the test doubles; `StreamerRuntime::from_parts` drives a run loop without a config; `api_router()` is called through `tower::oneshot` so HTTP tests need no socket. Adding a component config variant fails `tests/config.rs` until a wire-format sample is added — that's intentional.

Lints are strict by design: clippy `pedantic` plus `unwrap_used`/`expect_used` as warnings, and `clippy.toml` makes those apply in tests too. Removing remaining `.unwrap()`s is active work — flag new ones in review.

## Architecture

Three workspace crates:

- **`streamer-core/`** — shared, dependency-light types. All config structs/enums (`config.rs`), plus `StreamerId`, `MessageBatch = Vec<Arc<serde_json::Value>>`, `UiEvent`, `StreamerDto`. Compiles for both native and `wasm32`, which is why it exists: the frontend needs the same config types as the server. It has no async/network deps and no real `main.rs`.
- **`/` (root `streamer` crate)** — the Axum server and the whole stream-processing runtime. It is a **lib + bin**: everything lives in `src/lib.rs` and its modules so integration tests can import it; `src/main.rs` is only clap args, tracing setup and the Leptos router wiring. `api_router()` in `lib.rs` builds the JSON/SSE routes for both.
- **`frontend/`** — Leptos 0.8 SSR + hydrate crate. `cdylib`+`rlib` with `ssr`/`hydrate` features; the root binary depends on it with `ssr` and mounts it via `leptos_axum`.

### The pipeline model

A **Streamer** is one pipeline: `input → [transforms] → output`. Streamers are identified by `id` (from config, or a random `petname` if omitted) and form a **graph**: the `streamer` input kind subscribes to another streamer's output, so one pipeline can fan out to several downstream ones. `config.json` is the worked example and deliberately covers every component kind bar the `file` output: two roots (a NATS source and a dummy ticker), a fan-out of five under the source, and one node at depth 3. Keep it that way when adding a component — it's what the UI is inspected against, and `tests/graph.rs` builds the whole file.

Data flowing through is always `Arc<MessageBatch>` — a batch of `Arc<serde_json::Value>`. There is no typed schema; everything is untyped JSON, and transforms address fields by name.

Three object-safe traits define the plugin points, all in the root crate:

- `inputs::InputSource` — `async fn next() -> Result<Arc<MessageBatch>>`
- `transforms::Transform` — `async fn apply(batch) -> Result<Vec<Arc<MessageBatch>>>` (one batch in, N batches out — that's how `splitter` works)
- `outputs::OutputDestination` — `async fn init()` + `async fn emit(batch)`

### Config → runtime wiring (the part that spans files)

Config types live in `streamer-core::config` and are pure data. The *building* of runtime objects from them lives in the root crate, `src/config.rs`, via three local traits (`BuildInputConfig`, `BuildTransformConfig`, `BuildOutputConfig`) implemented **on the core config enums** — this is how the orphan rule is worked around while keeping core wasm-friendly. Each enum variant delegates to a per-component `BuildInput`/`BuildTransform`/`BuildOutput` impl in `src/inputs/*.rs`, `src/transforms/*.rs`, `src/outputs/*.rs`.

`BuildCtx` (defined in `src/lib.rs`) is threaded through every `build()` call. It carries `&mut HashMap<StreamerId, StreamerHandle>` — needed so a `streamer` input can look up its upstream and register an mpsc sender on it — and the `broadcast::Sender<UiEvent>`.

**Adding a component** therefore touches five places: the config struct + enum variant in `streamer-core/src/config.rs`, the `build()` dispatch arm in `src/config.rs`, the impl module, and a wire-format sample in `tests/config.rs` (which fails until you add it).

The config enums use `#[serde(tag = "type", rename_all = "snake_case")]` with `#[serde(flatten)]` wrappers, so JSON looks like `{"type": "nats", "urls": ..., "subject": ...}`. They also derive `schemars::JsonSchema` with `#[schemars(title = "...")]` — `/docs` generates component documentation by reflecting over `schema_for!(InputKind)` etc., so the title/doc-comments on config fields *are* the docs.

Buffering is an input decorator, not a transform: `InputConfig.buffer` wraps any `InputSource` in `inputs::Buffered` (static N-message or tumbling time window). There is *also* a `buffer` transform — different thing, different place.

### Runtime & state

`AppState` (`src/state.rs`) holds `Mutex<HashMap<StreamerId, StreamerHandle>>` and the UI event broadcast channel. Creating a streamer builds a `StreamerRuntime` and `tokio::spawn`s its `run()` loop; each `Streamer` owns a `CancellationToken` that `delete_streamer` cancels, and the run loop `select!`s on it against the next input message. Downstream fan-out is a `Mutex<Vec<mpsc::Sender>>` on `Streamer`, populated by `subscribe()`.

Note the concurrency shape: `std::sync::Mutex` guards, held across map lookups but never across `.await` — the lock is dropped/cloned out before awaiting sends. Worth preserving.

### HTTP surface

`src/main.rs` builds two routers and merges them: an `api` router with `Arc<AppState>` state (`POST/GET /api/streams`, `DELETE /api/streams/{id}`, `GET /events` SSE, `GET /docs`), and the Leptos router with `LeptosOptions` state plus `file_and_error_handler` fallback.

`/events` is an SSE stream over the `UiEvent` broadcast; the frontend consumes it with `leptos_use::use_event_source` + `codee` JSON. Streamer run loops only send events when `receiver_count() > 0`. This is explicitly marked temporary in `src/streamer.rs`.

The frontend (`frontend/src/app.rs`) is a pannable/zoomable canvas of streamer "cards" fed by `ApiClient::list_streams()` plus the live event signal. The older Askama templates (`templates/index.html`, `docs.html`, `src/handlers/ui/`) are the pre-Leptos UI and are slated for removal — `/ui` is already commented out, but `/docs` still uses Askama.

## Notes

- `readme.md` holds the current TODO list — check it for what's in flight before proposing work.
- Leptos config lives in the root `Cargo.toml` under `[[workspace.metadata.leptos]]`; `site-addr` there (6767) is what the binary actually binds, not the `--port` arg.
- `Dockerfile` is a two-stage cargo-leptos build; the runtime image bakes in `config.json` and the `LEPTOS_SITE_*` env vars.
