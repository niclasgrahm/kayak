# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

"kayak" — graph-based stream processing: an Axum server that runs configurable `input → transforms → output` pipelines, with a Leptos web UI.

## Commands

```bash
just dev                            # dev server on :6767 against example_config/ (hot reload; makes secrets.json if absent)
cargo leptos watch                  # the same without a config — hot reload on 127.0.0.1:6767 (builds WASM + server)
cargo leptos build --release        # production build (server binary + target/site assets)
cargo check                         # fast type check of the whole workspace
just ci                             # lint + test — what GitHub Actions runs
just test                           # cargo test --all-targets (offline: no NATS, no server)
just lint                           # cargo clippy --all-targets -- -D warnings
cargo run -- --config example_config/config.json --secrets example_config/secrets.json --debug
                                    # run the server binary directly (no WASM rebuild). Note it binds
                                    # :3000 and serves no frontend unless LEPTOS_SITE_ADDR/_ROOT are set —
                                    # `just dev` is the one that works. --connections <path> is optional:
                                    # without it the file is derived from the config's name.

docker compose up                   # NATS :4222 + publisher on test.subject, kafka :9092 + publisher on test.events, postgres :5432
just test-http                      # hurl --test hurl/tests/*.hurl (needs the server running)
just start-baseline                 # hurl hurl/create_baseline.hurl — creates a sample pipeline
```

## Definition of done

These two rules are not negotiable and apply to every change, however small:

1. **New code ships with tests.** Any new or changed behaviour — a component, a handler, a config field, a bug fix — needs a test that fails without the change. A bug fix without a regression test is not a fix. If something genuinely can't be tested offline (a real NATS connection, say), say so explicitly and explain why rather than skipping quietly.
2. **`just ci` must be green before a task is called done.** That's `just lint` (clippy `-D warnings`) plus `just test`. Not "compiles", not "the new test passes" — the whole suite. If tests fail, report the failure and the output; never describe a task as complete with a red suite, and never disable, `#[ignore]` or weaken an existing test to get to green. A test that turns out to encode the wrong behaviour is a conversation to have first, not something to edit away.

Testing is documented in `readme.md` under "testing" — read that before adding tests. In short: the runtime lives in `src/lib.rs` (not `main.rs`) so `tests/` can reach it; `src/testing.rs` holds the test doubles; `PipelineRuntime::from_parts` drives a run loop without a config; `api_router()` is called through `tower::oneshot` so HTTP tests need no socket. Adding a component config variant fails `tests/config.rs` until a wire-format sample is added — that's intentional.

Lints are strict by design: clippy `pedantic` plus `unwrap_used`/`expect_used` as warnings, and `clippy.toml` makes those apply in tests too. Removing remaining `.unwrap()`s is active work — flag new ones in review.

## Architecture

Three workspace crates:

- **`kayak-core/`** — shared, dependency-light types. All config structs/enums (`config.rs`), plus `PipelineId`, `MessageBatch = Vec<Arc<serde_json::Value>>`, `UiEvent`, `PipelineDto`, the canvas layout types (`layout.rs`), and the endpoint table the HTTP surface is built and documented from (`api_docs.rs`). Compiles for both native and `wasm32`, which is why it exists: the frontend needs the same config types as the server. It has no async/network deps and no real `main.rs`.
- **`/` (root `kayak` crate)** — the Axum server and the whole stream-processing runtime. It is a **lib + bin**: everything lives in `src/lib.rs` and its modules so integration tests can import it; `src/main.rs` is only clap args, tracing setup and the Leptos router wiring. `api_router()` — re-exported from `lib.rs`, defined in `src/endpoints.rs` — builds the JSON/SSE routes for both.
- **`frontend/`** — Leptos 0.8 SSR + hydrate crate. `cdylib`+`rlib` with `ssr`/`hydrate` features; the root binary depends on it with `ssr` and mounts it via `leptos_axum`.

### The pipeline model

A **pipeline** is one `inputs → [transforms] → outputs` chain. A pipeline may have several inputs (merged into one stream) and several outputs (each gets every batch); `inputs` and `outputs` are JSON arrays, and there is no singular form. Pipelines are identified by `id` (from config, or a random `petname` if omitted) and form a **graph**: the `pipeline` input kind subscribes to another pipeline's output, so one pipeline can fan out to several downstream ones. `example_config/config.json` (with `config.connections.json` beside it) is the worked example and deliberately covers every component kind and every connection kind: two roots (a NATS source and a dummy ticker), a fan-out of seven under the source, one pipeline (`everything`) fed by three inputs — two upstreams and a nats subject another pipeline publishes to — one pipeline with two outputs of different kinds, and one pipeline at depth 3. Keep it that way when adding a component — it's what the UI is inspected against, and `tests/graph.rs` builds the whole file.

Data flowing through is always `Arc<MessageBatch>` — a batch of `Arc<serde_json::Value>`. There is no typed schema; everything is untyped JSON, and transforms address fields by name.

Three object-safe traits define the plugin points, all in the root crate:

- `inputs::InputSource` — `async fn next() -> Result<Arc<MessageBatch>>`. Several of them are merged by `inputs::merge` into an `inputs::Merged`, which runs a pump task per input rather than `select!`ing over them — selecting would cancel a losing `next()` and starve any input that waits on a timer. One input failing is reported and survived; the run loop only stops when the last one is gone.
- `transforms::Transform` — `async fn apply(batch) -> Result<Vec<Arc<MessageBatch>>>` (one batch in, N batches out — that's how `splitter` works)
- `outputs::OutputDestination` — `async fn init()` + `async fn emit(batch)`

### Config → runtime wiring (the part that spans files)

Config types live in `kayak-core::config` and are pure data. The *building* of runtime objects from them lives in the root crate, `src/config.rs`, via three local traits (`BuildInputConfig`, `BuildTransformConfig`, `BuildOutputConfig`) implemented **on the core config enums** — this is how the orphan rule is worked around while keeping core wasm-friendly. Each enum variant delegates to a per-component `BuildInput`/`BuildTransform`/`BuildOutput` impl in `src/inputs/*.rs`, `src/transforms/*.rs`, `src/outputs/*.rs`.

`BuildCtx` (defined in `src/lib.rs`) is threaded through every `build()` call. It carries `&mut HashMap<PipelineId, PipelineHandle>` — needed so a `pipeline` input can look up its upstream and register an mpsc sender on it — the `broadcast::Sender<UiEvent>`, the `Arc<dyn SecretStore>` that `${NAME}` references resolve against, and the `Arc<Connections>` a component's `connection` field is looked up in.

### The http input

Every other input reaches out; this one is reached. `src/inputs/http.rs` holds
both halves: the `InputSource` and the `Inboxes` registry the handler finds it
through. Building the input claims the pipeline's id in the registry and
dropping it gives the claim up, so the endpoint's lifetime *is* the run loop's
and nothing has to remember to unregister. The registry rides on `BuildCtx`
exactly as the connections do (`AppState` owns it), which is what keeps the axum
layer from ever holding an `InputSource`.

The path is derived from the pipeline id (`POST /api/pipelines/{id}/messages`)
rather than configured — one pipeline is one endpoint, so a second `http` input
on a pipeline fails to build. Three things are load-bearing. Registrations carry
a **token** and `Drop` only removes a matching one: a revert can build the new
pipeline before the old one's task has finished dying, and an unconditional
remove would tear down the successor's endpoint. `delete_pipeline` **evicts by
name** under the pipelines lock (and `revert` clears the map) so an endpoint goes
down with the request that removed it rather than whenever the task gets round to
it — by-name eviction is safe *because* of that lock. And posting is a
`try_send`: a full queue is a 503, never an awaited send, because holding an HTTP
request open until a pipeline catches up just moves the timeout somewhere less
visible. Lock order is the existing one — pipelines before inboxes; `ingest`
classifies its 404 after the inbox lock is already released.

`PipelineError` grew `NotAccepting` (a running pipeline with no `http` input) and
`Backpressure` (503). `NotAccepting` is a 404 like `NotFound` and is deliberately
a separate variant: one is fixed by creating the pipeline, the other by giving it
the input.

### Connections

The systems pipelines talk to are declared once under a name, in a third file
beside the config, and components refer to them: `{"type": "kafka", "connection":
"prod-kafka", "topic": "orders", "group": "kayak"}`. The split is **what the
system needs (brokers, urls, credentials) against what this pipeline wants from
it (topic, group, subject, table)** — there is no inline form, a component names
a connection or it does not build. Types are in `kayak-core/src/connections.rs`
(`ConnectionKind`, `Connections` — a `BTreeMap` newtype, so iteration order is
the name order and the file is deterministic); file IO is `src/connections.rs`,
mirroring `persist` rather than `layout`: JSON or YAML by extension, atomic
write, and **written only by an explicit save**, because adding a connection
changes what the server can build. The same save writes both files — a config
saved without the connections it names would not start — and `revert` reloads
the connections *first*, since the pipelines being rebuilt name them.

One kind serves both directions (a `kafka` connection feeds a kafka input and a
kafka output). The kind is checked as well as the name: `Connections::kafka/nats/
postgres` return a `ConnectionError` that says which kind it actually is, or —
for an unknown name — lists the ones that exist. `BuildCtx` carries an
`Arc<Connections>` snapshot, so a component reads its settings once at build
time and editing a connection afterwards reaches only new and rebuilt pipelines.
Deleting one a running pipeline names is refused (409, `PipelineError::
ConnectionInUse`, listing them).

The path is `--connections <path>` — fixed for the process, which is what lets
two configs share one file — or, without the flag, derived from the config's
name *and format* (`config.json` → `config.connections.json`, `pipelines.yaml` →
`pipelines.connections.yaml`; unlike the layout file, which is always JSON,
because this one is hand-written). A derived file that is missing means "no
connections"; one named with the flag has to exist.

`Config::connections()` is the counterpart of `Config::upstreams()` — spelled
out per kind on purpose, so a new component that talks to a configured system
has to be added there and the compiler is what says so.

**Adding a connection kind** touches: the struct + `ConnectionKind` variant in
`kayak-core/src/connections.rs`, the typed accessor beside it, the `BuildCtx`
helper in `src/lib.rs`, and a wire-format sample in `tests/config.rs`. The docs
page, the `/api/docs` output and the "add connection" form all come from the
schema reflection and need nothing.

The `file` connection is the odd one out — a directory, no host, no credentials
— and it is a connection anyway because it holds the same thing the others do:
*what the system is*, as against what one pipeline wants from it. A file output
names a `path` under its root exactly as a kafka output names a topic on those
brokers, and the object-store connection that comes later swaps the root for a
bucket without any component changing.

### The file output sandbox

`--data-dir` is a **second** directory boundary, and it is not `AppState`'s
`save_dir`: that one bounds where the server writes *configs* on request, this
one bounds where pipelines write *data*. Don't conflate them because both are
directories fixed at startup — pointing a data firehose at the directory holding
the config someone is editing is not a default anyone would choose.

Two layers, and the reason is `persist::save_path`'s reason: a connection's
`root` arrives from `POST /api/connections` like anything else, so it is checked
against `--data-dir` rather than trusted, and without the flag file outputs
refuse to build at all. That closed default is the design, not a stub. Paths are
**refused, never normalised** (`Root::relative_path`), and the landing directory
is canonicalized and re-checked afterwards — that second check is what catches a
symlink planted inside the root, and the component-wise check alone does not.
All of it happens at build time, and the build creates the directory.

`src/outputs/rotate.rs` is split from `src/outputs/file.rs` on purpose and the
line matters: rotate.rs decides *when* a part is finished, what it is called and
how messages are laid out inside it, and touches no filesystem — the object
store output will take it whole. file.rs is only the destination. Keep new
format or rotation work on the rotate.rs side of that line.

Two properties there are load-bearing. Rotation is checked *after* a batch is
written, so a batch is never split across two parts (`max_rows` is a floor, not
a ceiling). And `Rotation::is_full` returns false at zero rows — without that, an
interval trigger on an idle pipeline closes and reopens a part every time it is
asked, filling the directory with empty files.

The sample's file output is `heartbeat_to_disk`, and its upstream is deliberate:
`heartbeat` is a dummy input, so it is the one pipeline in `example_config/` that
writes real output without `docker compose up`. Its cost is that the sample no
longer loads on a server with no `--data-dir` — so `just dev` and
`tests/graph.rs` both pass `--data-dir dev_data`, and the connection's root
(`dev_data/events`) is relative, resolving against the working directory in
both. Change one and change the other. The container image doesn't pass it
(nothing is baked in there), so running the sample out of the image takes the
flag on the command line — that's the readme's deployment section. `dev_data` is
gitignored; the build creates it.

### Secrets

Config fields that can hold credentials are typed `Secret` (`kayak-core::config`), not `String`. They all live on *connections* now rather than on components. `Secret` only ever holds the *unresolved* `${NAME}` template, which is what makes it safe to serialize back out of `GET /api/pipelines` and to compile for wasm. Resolution happens at build time via `ctx.resolve()` and yields a `secrets::Resolved`, whose `Display`/`Debug` print the template rather than the value — so error contexts can name a connection without leaking it. Reaching the real value takes `.expose()`; flag new call sites in review, and never put a `Resolved` into anything `Serialize`. Stores (`EnvStore`, `FileStore`, `ChainStore`) live in `src/secrets.rs`; `main.rs` chains env ahead of `--secrets <file>`. `src/testing.rs` has `MapSecretStore` for tests. See "secrets" in `readme.md`.

Note that `$defs` in the generated schema now holds non-component types (`Secret`), so anything reflecting over the schema has to distinguish those from components — see the docs section below.

**Adding a component** therefore touches five places: the config struct + enum variant in `kayak-core/src/config.rs`, the `build()` dispatch arm in `src/config.rs`, the impl module, and a wire-format sample in `tests/config.rs` (which fails until you add it). The config struct also needs a doc comment, and its fields want one — that's what `/docs` shows, and a missing one fails a test in `kayak-core/src/docs.rs`.

The config enums use `#[serde(tag = "type", rename_all = "snake_case")]` with `#[serde(flatten)]` wrappers, so JSON looks like `{"type": "nats", "urls": ..., "subject": ...}`. They also derive `schemars::JsonSchema` with `#[schemars(title = "...")]` — `/docs` generates component documentation by reflecting over `schema_for!(InputKind)` etc., so the title/doc-comments on config fields *are* the docs.

Buffering is an input decorator, not a transform: `InputConfig.buffer` wraps any `InputSource` in `inputs::Buffered` (static N-message or tumbling time window). There is *also* a `buffer` transform — different thing, different place.

### Runtime & state

`AppState` (`src/state.rs`) holds `Mutex<HashMap<PipelineId, PipelineHandle>>`, the connections (plus their own saved-snapshot), and the UI event broadcast channel. Creating a pipeline builds a `PipelineRuntime` and `tokio::spawn`s its `run()` loop; each `Pipeline` owns a `CancellationToken` that `delete_pipeline` cancels, and the run loop `select!`s on it against the next input message. Downstream fan-out is a `Mutex<Vec<mpsc::Sender>>` on `Pipeline`, populated by `subscribe()`.

Lock order, worth preserving: the pipelines lock is taken *before* the connections lock and never the other way round — `delete_connection` asks `pipelines_using` first and lets that guard go before it touches the map.

Note the concurrency shape: `std::sync::Mutex` guards, held across map lookups but never across `.await` — the lock is dropped/cloned out before awaiting sends. Worth preserving. `revert` obeys it the awkward way round: it cancels and takes the join handles out under the guard, drops the guard, *then* awaits them.

The run loop's `select!` is `biased` on purpose, and the cancellation check in its error arm is not redundant. Teardown cancels every pipeline and then drops the upstreams, so a downstream wakes with both its cancellation and an "upstream is gone" ready; an unbiased `select!` reported our own shutdown as a pipeline failure about a third of the time, and those errors surfaced on the UI cards of the *new* pipelines that had just taken the same ids. The rule is "was **I** cancelled" — an input dying under a pipeline that is still running must still be reported. `tests/pipeline.rs` pins both halves, and repeats the assertion 20× because a coin-toss regression test isn't one.

`AppState` also holds an optional `config_path`, a fixed `save_dir` and a `saved` snapshot. The path is optional *and* mutable (`Mutex<Option<PathBuf>>`) because a server started without `--config` can still be asked to write one: `save_config_as` calls `adopt`, and from that save on the created file is what `revert` reloads, `has_unsaved_changes` compares against and the layout is written beside. Adoption only ever goes from none to some — a save-as on a server that already has a file leaves the loaded one alone. `save_dir` is the boundary saves are confined to and is *not* derived from `config_path`: it's the config file's directory, or the process's working directory when there is no config file. **The config file is a load source and a save target, never a mirror**: `create_pipeline`/`delete_pipeline` never write, and an earlier design where they did is what turned "load a file" into "rewrite a file". Writing happens only in `save_config_as`, via `src/persist.rs` — deterministic order (topological, ties by id) and an atomic temp-then-rename, because the file is meant to be committed.

The file can be JSON or YAML (`kayak_core::ConfigFormat`), decided by the extension and nowhere else: `persist::read`/`persist::write` are the only two places a format exists, and everything past the parser has `Config`s that don't remember which it was. `POST /api/config/save` takes an optional `format`; without one the name decides. The `saved` snapshot behind `has_unsaved_changes` is *always* rendered as JSON — it's a fingerprint of the graph, not of the file.

Three things to preserve. `persist::save_path` rejects anything but a bare file name and is a security boundary, not a nicety — the path comes from an HTTP request, so an unconstrained one is an arbitrary write; refuse, never normalise. `has_unsaved_changes` compares `render(current)` against the `saved` snapshot, which is exact *because* `render` is deterministic. And `revert` parses the file before tearing the runtime down, so a file broken by hand doesn't cost you the running graph.

### The canvas layout file

Card positions are **not configuration** and are deliberately kept out of the config file. They live in `<config-stem>.layout.json` beside it (`config.json` → `config.layout.json`, always JSON), whose path is *derived* in `layout::layout_path` rather than configured. `kayak_core::layout` holds the types (`LayoutFile`, `PipelineLayout`), `src/layout.rs` the file IO — deterministic (`BTreeMap`) and atomic, same as `persist`, because the file is committed.

The write rule is the **opposite** of the config file's, on purpose: `PUT /api/layout` writes to disk immediately, and arranging the canvas never counts as an unsaved change — moving a card changes nothing the server runs, so there is nothing worth reviewing before it lands. It is a full replacement rather than a patch, which is what makes "reset everything to automatic" a send of a smaller map. Only pipelines someone has actually moved appear; `height` is absent unless the card was resized (a card is normally as tall as its content). `edges` holds adjusted lines (channel offset and either end's port), is omitted when empty, and `adjust_edge` drops an entry once *every* adjustment on it is back to default — an undone adjustment must not leave a no-op in a committed file, and dropping the entry when only one of the three is undone would silently lose the others. It's a `Vec` sorted by `(from, to)` rather than a map so no id character has to be escaped. `Side` lives in core rather than `graph.rs` because a stored port position is meaningless without the face it was measured on. Entries for pipelines that no longer exist are kept, not pruned. Without a config file there is nowhere to write, so the arrangement lives in memory until a save creates one — `adopt` writes it out at that point, which is the one time the layout file is written by something other than `PUT /api/layout`.

`tests/layout.rs` pins all of that, and each assertion there is a deliberate difference from `tests/persist.rs` — don't "fix" one to match the other.

### Canvas geometry: the grid

`graph::GRID` (20px) is the unit for everything on the canvas, not decoration: card width is 18 cells, `layout` snaps positions and sizes to it, ports sit on its lines, and edge channels run along them. The `.pipelines` background-size is set inline from the same constant — if you change one, change both.

Measured card heights are rounded **up** (`snap_up`): up so content still fits, and idempotent because the height feeds back through measure → lay out → render and would otherwise oscillate. A pinned height (from a resize) wins over the measured one, and the card's content scrolls — that's the `.card.pinned` CSS.

Edges are orthogonal: `sides_between` picks the faces (only ones with `CLEARANCE` in front of them), `port` places an edge on a face — edges sharing one fan out along it — and `route` produces the corners, always axis-aligned, through a grid-snapped channel between the cards. `rounded_path` renders it.

**Channels are separated automatically** (`channel_offsets`, `Segment`, `candidate_offsets`), which is the third pass in `edge_paths` and the reason it has three. Every route between the same two rows wants the same half-way line, so a fan-out would be drawn as one thick line; each edge instead takes the nearest line to half way — 0, ±`GRID`, ±2·`GRID` … — whose stretch no other channel already covers. Two rules earn their keep. Hand-placed offsets are laid down *first*, because they can't move and an automatic channel is the one that should give way. And the automatic ones are then placed left to right, which is the greedy order that packs intervals onto the fewest lines. Nearest-first matters as much as the separation: an edge with room stays on the half-way line, so a sparse graph is drawn exactly as it was before any of this existed. Only middle segments are considered — stubs are already fanned out by `slots_on_faces`.

That makes `EdgeAdjustment.offset` an `Option<f64>` rather than a number defaulting to zero, and `EdgePath` carry the `channel_offset` it was *drawn* at: `None` means "you place it", `Some(0.0)` is a deliberate "on the half-way line, whatever else is there", and a drag has to start from where the line actually is or the first pixel of it snaps the line back to the middle. The way back to automatic is the double-click, which stores `None`.

**Vertical wins whenever it's available** in `sides_between`, and that ordering is the point, not an accident: the graph is a flow, so a child a row below reads as fed-from-above however far to the side it sits. An earlier version tie-broke on centre distance and turned every wide fan-out into lines arriving sideways. Side faces are therefore what you get when the cards are *level* — no room to route between them — which is when a sideways line is right.

**Three parts of a route are draggable**, all stored per edge in the layout file and all reset by double-click:

- *The channel* (`Route::channel`, `Channel`, `dragged_channel`) — an *offset* from the half-way line rather than a coordinate, so it survives either card moving; `route` clamps it to the gap, since past either end the route doubles back. A route with no middle segment — straight, or L-shaped between perpendicular faces — reports `channel: None` and gets no handle; one that did nothing would be worse than none.
- *The two ends* (`PortHandle`, `port_at`, `auto_along`, `dragged_port`). The *face* stays automatic — that answer is nearly always right and has to keep up as cards move — but where along it the edge attaches is the user's. Stored as a distance from the face's start, not a fraction: "a card's width in from the corner" should stay put when the card is made taller.

Two rules in there earn their keep. A stored port carries the `Side` it was measured on, and is **ignored when the router picks a different face** — the number means nothing on the new face, and dropping it is self-healing with no cleanup pass. And a pinned end **still occupies its slot in `slots_on_faces`** even though the slot's position is discarded for it: excluding it would re-spread the rest, so nudging one line would shift its siblings. `pinning_one_port_does_not_move_the_others` pins that; it failed on the first attempt, which is how the rule got found.

One known gap: a channel can still pass through a third card. That's now a drag away from fixed rather than needing an obstacle-aware router.

Pinned pipelines **keep their slot in the automatic flow** (`layout` overwrites the auto answer rather than removing the pipeline from it), so dragging one card doesn't rearrange the rest. Cards can then overlap, which is the user's business.

A card can also be **maximized** to fill the canvas, from the button in its title bar (`canvas.maximized`, at most one at a time, available in read-only too — filling the screen with a card is a way of reading it). Its geometry comes from `graph::maximized_geom(camera, viewport)` rather than from the layout, so it stays inside the transformed surface and needs no second coordinate system; the width is divided by the zoom the surface is scaled by. Three things it deliberately does *not* do: it never reaches the layout file (it's a way of looking at a card, not a change to where the card lives, so it doesn't survive a reload), it doesn't report its window-sized height into `measured` (that would push every row below it apart and pull them back on restore), and it leaves its laid-out position alone underneath — which is why restoring it is exact and the edges, still routed against that position, come back to a card that never moved. A focus request clears it, since being shown a pipeline means the canvas.

### HTTP surface

`src/main.rs` builds two routers and merges them: the `api_router` with `Arc<AppState>` state, and the Leptos router with `LeptosOptions` state plus `file_and_error_handler` fallback.

**The API router is not a list of `.route()` calls.** `src/endpoints.rs` folds it over `kayak_core::api_docs::endpoints()`, so that table isn't a description of the routes — it *is* the routes, and an endpoint missing from it is never registered. `handler_for` matches on the `Operation` enum, so a table entry with no handler doesn't compile; `route_of` takes the method from the table, so an entry documented `PUT` and wired to `post(...)` isn't expressible. Adding an endpoint therefore touches three places (an `Operation` variant, an `ApiDoc` entry, a handler arm) and the compiler names two of them.

`/events` is an SSE stream over the `UiEvent` broadcast; the frontend consumes it with `leptos_use::use_event_source` + `codee` JSON. Pipeline run loops only send events when `receiver_count() > 0`. This is explicitly marked temporary in `src/pipeline.rs`.

The frontend has two routes behind `leptos_router` (`frontend/src/app.rs`): `/` is the pannable/zoomable canvas of pipeline "cards" fed by `ApiClient::list_pipelines()` plus the live event signal, and `/docs` is the generated reference — two tabs, components and HTTP API. `Navbar` is shared and reads `AppState` through `use_context` rather than `expect_context`, because only the canvas provides it.

Of the older Askama templates, only `templates/index.html` and the dead `/ui` `index_handler` are left; both are slated for removal, and Askama goes with them.

### The component reference (`/docs`)

Generated, never hand-written. `kayak-core/src/docs.rs` reflects over `schema_for!(InputKind)` etc. and produces `ComponentDoc`s — kind, family, description, fields (name, type, required) and, for enum-shaped configs like `filter`, variants. **The doc comments on the config structs are the docs**, and a component with no doc comment fails a unit test. Two consumers: the Leptos `/docs` page renders it, `GET /api/docs` serves it as JSON.

Nothing in there knows the name of any component — keep it that way. Notes for anyone touching it: walk `oneOf` (which pairs a `type` tag with a config struct), never `$defs` (which also holds shared field types like `Secret`); field order is `required` order then alphabetical; `Option<T>` arrives as `anyOf: [T, null]` when the inner type is a `$ref` and as `"type": ["integer", "null"]` when it isn't — `scalar_type_of` handles the second spelling.

A `FieldDoc` carries `field_type` (`FieldType`) beside the human-readable `type_name`. That's the same reflection serving a second consumer: the "add pipeline" modal generates its form from it, so a new component gets working controls and validation for free. `FieldType::Json` is the honest fallback for anything with a shape of its own (a tagged union like `buffer`) — it renders as a JSON box rather than a control that can't work.

`FieldType::Connection(kind)` works the same way and for the same reason, one step further: a `connection` field carries `#[schemars(extend("x-connection" = "kafka"))]`, and the marker holds the *kind* — "any connection" is the wrong set to offer, since a kafka input can only use a kafka connection. `Family::Connection` is a fourth family, so a connection kind documents itself on `/docs` and generates its own form through the same machinery a component does.

`FieldType::PipelineId` is the one field type the schema alone can't derive: a pipeline id is a `String` like any other, so the field says so where it's declared — `#[schemars(extend("x-pipeline-id" = true))]` on `PipelineConfig.upstream` — and `docs.rs` looks for the marker, not for the field's name. The rule about not knowing component names covers their field names too, so any component that grows a reference to another pipeline gets the dropdown by adding that attribute. The options can't come from the schema either: they are the running graph, so `AddPipelineModal` derives them from the pipeline list and passes them down to `FieldEditor`. That control is the only one in the modal that reads its value back — the list can arrive after the modal opened, and a rebuild must re-mark what was already chosen rather than drop it.

### The HTTP API reference (`/api/openapi.json`, `/api/reference`, the `/docs` tab)

Three consumers off the one table in `kayak-core/src/api_docs.rs` — the same table `api_router` is folded over, so none of them can describe a server that doesn't exist.

Unlike `docs.rs` this table is **written, not reflected**, and has to be: a Rust doc comment on an axum handler isn't readable at runtime. So the convention inverts — **the prose lives in the `ApiDoc` entry** and handlers carry a one-line `///` pointing at it. Don't "fix" that by moving descriptions back onto the handlers; nothing would read them. Bodies are the exception: they name a schema and `api_docs::schemas()` generates those with `schema_for!`, so request/response shapes can't drift from the Rust types.

`src/openapi.rs` renders the table as OpenAPI 3.1. The only real work is hoisting each generated root schema's `$defs` into one `components/schemas` and rewriting the `$ref`s — schemars 1.x emits JSON Schema 2020-12, which 3.1 embeds unchanged, so there's no translation beyond that. Only the *value of a `$ref` key* is rewritten; a description mentioning `#/$defs/` is prose and is left alone.

Four things to preserve:

- **`ApiError` is a Rust type** (`api_docs::ApiError`) so the spec's error schema is generated rather than written. `an_error_body_matches_the_documented_shape` in `tests/api.rs` deserializes a real failure into it — that test is the only thing connecting `AppError` to what the spec claims.
- **`/events` is described as far as OpenAPI can go and no further.** `Body::EventStream` renders as a string body plus prose, because 3.1 can name the media type but not the events in it. That's AsyncAPI's job; describing it as a JSON body would make clients try to parse the stream in one piece.
- **The renderer is vendored**, `assets/scalar.js` (3.5 MB, committed) with `assets-dir = "assets"` in the leptos metadata. The page loads it and the spec by relative URL, so an offline `just dev` serves a working reference. `the_reference_page_loads_the_spec_and_the_bundle_from_this_server` fails if a CDN link creeps back in.
- **The route-coverage test reads the body, not just the status.** `every_documented_endpoint_is_routed_at_its_documented_method` has to tell the router's 404 from `delete_pipeline`'s — the router's is empty, `AppError`'s is JSON. And it only collects the body on a 404, because collecting `/events` would hang forever.

`frontend/src/api_docs.rs` is the pure half of the `/docs` tab, mirroring `frontend/src/docs.rs`; the two tabs keep separate search queries and separate `selected` signals, because a search for "nats" and a search for "409" aren't the same search. Endpoint prose is rendered through `docs::rendered_description` rather than a copy of it.

### Editing the graph from the UI

The canvas has a `Mode` (`frontend/src/app.rs`) that starts at `ReadOnly`; edit affordances are `<Show>`n, not disabled, so read-only really is read-only. That includes moving cards: the title bar is a drag handle and the corner a resize handle only in edit mode, and double-clicking the title bar puts a card back under the automatic layout. A `<Show>`'s children must be `Fn` *and* `Send + Sync`, which is why `Card`'s drag handler keeps its id in a `StoredValue` — that makes the closure `Copy` and usable in both places.

The same applies to the edge handles: `ChannelGrip` and `PortGrip` are each two `<line>`s, a fat transparent one that catches the pointer (`.edges` sets `pointer-events: none`, so the hit line turns it back on for itself) and a visible grip. Note the label is an `aria-label` and not an SVG `<title>` child — `leptos_meta` claims `<title>` for the document's, and the browser tab ends up named after whichever edge rendered last. Their `.vertical` classes mean *opposite* things (a channel's is the route's direction, a port's is the face's), which is why the cursor rules are per-class rather than shared.

Drags are tracked with window-level listeners rather than on the card (a fast pointer leaves the card behind, and a `mouseup` outside it would never arrive). The delta is divided by the zoom, applied to the geometry captured at press time rather than accumulated, and written into `arrangement` live so the edges follow; the `PUT` happens once, on release. It's a browser-tab property — the API accepts writes either way, which is fine for a dev tool but shouldn't be mistaken for enforcement. Edits apply to the runtime immediately, so `revert` (reload the file) is the only undo, and `unsaved changes` in the navbar is the only thing between a session's work and a restart.

The sidebar has two tabs (`SidebarTab`), pipelines and connections, each with its own `+` and armed delete. The `+` in the pipelines tab opens `AddPipelineModal` (`frontend/src/app.rs`), whose pure half is `frontend/src/form.rs` — drafts in, `POST /api/pipelines` body or a list of `FormError`s out, unit tested like `graph.rs`/`inspector.rs`/`docs.rs`. One non-obvious constraint shapes the component: the field boxes are **uncontrolled** (`value=` once, `on:input` writes, never reads back), because the field list is rebuilt when the kind or variant changes and a rebuild on every keystroke would destroy the `<input>` being typed into. `DraftSignals` exists for the same reason — per-part signals so typing doesn't invalidate the list.

The pipelines tab has two arrangements and a search box, and which rows that comes to is `frontend/src/sidebar.rs` — pure, unit-tested, fed by `graph::pipelines_from` so the sidebar and the canvas derive from one description of the graph. `Flat` sorts by id *here* rather than trusting the server, which walks a `HashMap`. `Tree` has to answer the DAG: a pipeline with several upstreams is listed under each, in full under the **deepest** parent (ties by id) — the one the canvas draws the card below — and as a `repeat` row under the rest. A repeat doesn't recurse (it would draw a subtree twice, and would not terminate in a cycle) and gets no delete, since the armed state is keyed by id and two `×`s arming together read as two pipelines. Anything the walk can't reach — a cycle — is appended as a root rather than dropped. Search keeps the *ancestors* of a match and not its descendants; the rows are pre-order, which is what makes that a single backwards pass. The list is rebuilt wholesale rather than `<For>`-keyed because an id isn't unique in tree mode, and the search `<input>` sits outside that closure for the same reason the modal's fields are uncontrolled. The mode lives in `AppState` (the tab strip unmounts the list) and the query doesn't (a filter is transient).

`frontend/src/docs.rs` holds the page's pure logic (search filtering, grouping, anchors, doc-comment rendering) with unit tests, same convention as `graph.rs`/`inspector.rs`. One trap worth remembering: the docs lists are rebuilt with plain closures rather than `<For>`, because keying groups by family leaves stale components on screen when a filter changes a group's contents without changing its key.

## Notes

- `readme.md` holds the current TODO list — check it for what's in flight before proposing work.
- Leptos config lives in the root `Cargo.toml` under `[[workspace.metadata.leptos]]`; `site-addr` there (6767) is what the binary actually binds, not the `--port` arg.
- `Dockerfile` is a two-stage cargo-leptos build, documented in the readme's "deployment" section. The runtime image is the *runtime and nothing else*: binary, site directory, `LEPTOS_SITE_*` env vars, uid 10001, `ENTRYPOINT` = the binary so container args are server flags. **No config is baked in** — bare it serves an empty graph, and a deployment mounts one into `/kayak` (the WORKDIR, owned by the run user because saving writes there). The sample is carried at `/usr/share/kayak/example` for a tour, connections and layout file beside it under the same stem or they stop being found. The builder installs `cmake` for `rdkafka-sys`; nothing else, since TLS is rustls and zlib is vendored.
- **`example_config/` is the sample everything is tried against**, and it is one directory because the set travels together: the connections and layout files are *derived* from the config's path, so they only find each other side by side. `tests/config.rs` and `tests/graph.rs` read the files from there by relative path, so moving or renaming them breaks those tests — which is the point, the sample is not allowed to rot. `secrets.json` is gitignored anywhere in the tree; `just dev` creates the sample's from `secrets.example.json`.
