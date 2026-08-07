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
cargo run -- --config config.json --secrets ./secrets.json --debug   # run server binary directly (no WASM rebuild)

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

- **`kayak-core/`** — shared, dependency-light types. All config structs/enums (`config.rs`), plus `PipelineId`, `MessageBatch = Vec<Arc<serde_json::Value>>`, `UiEvent`, `PipelineDto`, and the canvas layout types (`layout.rs`). Compiles for both native and `wasm32`, which is why it exists: the frontend needs the same config types as the server. It has no async/network deps and no real `main.rs`.
- **`/` (root `kayak` crate)** — the Axum server and the whole stream-processing runtime. It is a **lib + bin**: everything lives in `src/lib.rs` and its modules so integration tests can import it; `src/main.rs` is only clap args, tracing setup and the Leptos router wiring. `api_router()` in `lib.rs` builds the JSON/SSE routes for both.
- **`frontend/`** — Leptos 0.8 SSR + hydrate crate. `cdylib`+`rlib` with `ssr`/`hydrate` features; the root binary depends on it with `ssr` and mounts it via `leptos_axum`.

### The pipeline model

A **pipeline** is one `inputs → [transforms] → outputs` chain. A pipeline may have several inputs (merged into one stream) and several outputs (each gets every batch); `inputs` and `outputs` are JSON arrays, and there is no singular form. Pipelines are identified by `id` (from config, or a random `petname` if omitted) and form a **graph**: the `pipeline` input kind subscribes to another pipeline's output, so one pipeline can fan out to several downstream ones. `config.json` is the worked example and deliberately covers every component kind bar the `file` output: two roots (a NATS source and a dummy ticker), a fan-out of seven under the source, one pipeline (`everything`) fed by three inputs — two upstreams and a nats subject another pipeline publishes to — one pipeline with two outputs of different kinds, and one pipeline at depth 3. Keep it that way when adding a component — it's what the UI is inspected against, and `tests/graph.rs` builds the whole file.

Data flowing through is always `Arc<MessageBatch>` — a batch of `Arc<serde_json::Value>`. There is no typed schema; everything is untyped JSON, and transforms address fields by name.

Three object-safe traits define the plugin points, all in the root crate:

- `inputs::InputSource` — `async fn next() -> Result<Arc<MessageBatch>>`. Several of them are merged by `inputs::merge` into an `inputs::Merged`, which runs a pump task per input rather than `select!`ing over them — selecting would cancel a losing `next()` and starve any input that waits on a timer. One input failing is reported and survived; the run loop only stops when the last one is gone.
- `transforms::Transform` — `async fn apply(batch) -> Result<Vec<Arc<MessageBatch>>>` (one batch in, N batches out — that's how `splitter` works)
- `outputs::OutputDestination` — `async fn init()` + `async fn emit(batch)`

### Config → runtime wiring (the part that spans files)

Config types live in `kayak-core::config` and are pure data. The *building* of runtime objects from them lives in the root crate, `src/config.rs`, via three local traits (`BuildInputConfig`, `BuildTransformConfig`, `BuildOutputConfig`) implemented **on the core config enums** — this is how the orphan rule is worked around while keeping core wasm-friendly. Each enum variant delegates to a per-component `BuildInput`/`BuildTransform`/`BuildOutput` impl in `src/inputs/*.rs`, `src/transforms/*.rs`, `src/outputs/*.rs`.

`BuildCtx` (defined in `src/lib.rs`) is threaded through every `build()` call. It carries `&mut HashMap<PipelineId, PipelineHandle>` — needed so a `pipeline` input can look up its upstream and register an mpsc sender on it — the `broadcast::Sender<UiEvent>`, and the `Arc<dyn SecretStore>` that `${NAME}` references resolve against.

### Secrets

Config fields that can hold credentials are typed `Secret` (`kayak-core::config`), not `String`. `Secret` only ever holds the *unresolved* `${NAME}` template, which is what makes it safe to serialize back out of `GET /api/pipelines` and to compile for wasm. Resolution happens at build time via `ctx.resolve()` and yields a `secrets::Resolved`, whose `Display`/`Debug` print the template rather than the value — so error contexts can name a connection without leaking it. Reaching the real value takes `.expose()`; flag new call sites in review, and never put a `Resolved` into anything `Serialize`. Stores (`EnvStore`, `FileStore`, `ChainStore`) live in `src/secrets.rs`; `main.rs` chains env ahead of `--secrets <file>`. `src/testing.rs` has `MapSecretStore` for tests. See "secrets" in `readme.md`.

Note that `$defs` in the generated schema now holds non-component types (`Secret`), so anything reflecting over the schema has to distinguish those from components — see the docs section below.

**Adding a component** therefore touches five places: the config struct + enum variant in `kayak-core/src/config.rs`, the `build()` dispatch arm in `src/config.rs`, the impl module, and a wire-format sample in `tests/config.rs` (which fails until you add it). The config struct also needs a doc comment, and its fields want one — that's what `/docs` shows, and a missing one fails a test in `kayak-core/src/docs.rs`.

The config enums use `#[serde(tag = "type", rename_all = "snake_case")]` with `#[serde(flatten)]` wrappers, so JSON looks like `{"type": "nats", "urls": ..., "subject": ...}`. They also derive `schemars::JsonSchema` with `#[schemars(title = "...")]` — `/docs` generates component documentation by reflecting over `schema_for!(InputKind)` etc., so the title/doc-comments on config fields *are* the docs.

Buffering is an input decorator, not a transform: `InputConfig.buffer` wraps any `InputSource` in `inputs::Buffered` (static N-message or tumbling time window). There is *also* a `buffer` transform — different thing, different place.

### Runtime & state

`AppState` (`src/state.rs`) holds `Mutex<HashMap<PipelineId, PipelineHandle>>` and the UI event broadcast channel. Creating a pipeline builds a `PipelineRuntime` and `tokio::spawn`s its `run()` loop; each `Pipeline` owns a `CancellationToken` that `delete_pipeline` cancels, and the run loop `select!`s on it against the next input message. Downstream fan-out is a `Mutex<Vec<mpsc::Sender>>` on `Pipeline`, populated by `subscribe()`.

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

**Vertical wins whenever it's available** in `sides_between`, and that ordering is the point, not an accident: the graph is a flow, so a child a row below reads as fed-from-above however far to the side it sits. An earlier version tie-broke on centre distance and turned every wide fan-out into lines arriving sideways. Side faces are therefore what you get when the cards are *level* — no room to route between them — which is when a sideways line is right.

**Three parts of a route are draggable**, all stored per edge in the layout file and all reset by double-click:

- *The channel* (`Route::channel`, `Channel`, `dragged_channel`) — the answer to a dozen edges between the same two rows all picking the same half-way line. An *offset* from that line rather than a coordinate, so it survives either card moving; `route` clamps it to the gap, since past either end the route doubles back. A route with no middle segment — straight, or L-shaped between perpendicular faces — reports `channel: None` and gets no handle; one that did nothing would be worse than none.
- *The two ends* (`PortHandle`, `port_at`, `auto_along`, `dragged_port`). The *face* stays automatic — that answer is nearly always right and has to keep up as cards move — but where along it the edge attaches is the user's. Stored as a distance from the face's start, not a fraction: "a card's width in from the corner" should stay put when the card is made taller.

Two rules in there earn their keep. A stored port carries the `Side` it was measured on, and is **ignored when the router picks a different face** — the number means nothing on the new face, and dropping it is self-healing with no cleanup pass. And a pinned end **still occupies its slot in `slots_on_faces`** even though the slot's position is discarded for it: excluding it would re-spread the rest, so nudging one line would shift its siblings. `pinning_one_port_does_not_move_the_others` pins that; it failed on the first attempt, which is how the rule got found.

One known gap: a channel can still pass through a third card. That's now a drag away from fixed rather than needing an obstacle-aware router.

Pinned pipelines **keep their slot in the automatic flow** (`layout` overwrites the auto answer rather than removing the pipeline from it), so dragging one card doesn't rearrange the rest. Cards can then overlap, which is the user's business.

### HTTP surface

`src/main.rs` builds two routers and merges them: an `api` router with `Arc<AppState>` state (`POST/GET /api/pipelines`, `DELETE /api/pipelines/{id}`, `GET /events` SSE, `GET /api/docs`, `GET/PUT /api/layout`), and the Leptos router with `LeptosOptions` state plus `file_and_error_handler` fallback.

`/events` is an SSE stream over the `UiEvent` broadcast; the frontend consumes it with `leptos_use::use_event_source` + `codee` JSON. Pipeline run loops only send events when `receiver_count() > 0`. This is explicitly marked temporary in `src/pipeline.rs`.

The frontend has two routes behind `leptos_router` (`frontend/src/app.rs`): `/` is the pannable/zoomable canvas of pipeline "cards" fed by `ApiClient::list_pipelines()` plus the live event signal, and `/docs` is the generated component reference. `Navbar` is shared and reads `AppState` through `use_context` rather than `expect_context`, because only the canvas provides it.

Of the older Askama templates, only `templates/index.html` and the dead `/ui` `index_handler` are left; both are slated for removal, and Askama goes with them.

### The component reference (`/docs`)

Generated, never hand-written. `kayak-core/src/docs.rs` reflects over `schema_for!(InputKind)` etc. and produces `ComponentDoc`s — kind, family, description, fields (name, type, required) and, for enum-shaped configs like `filter`, variants. **The doc comments on the config structs are the docs**, and a component with no doc comment fails a unit test. Two consumers: the Leptos `/docs` page renders it, `GET /api/docs` serves it as JSON.

Nothing in there knows the name of any component — keep it that way. Notes for anyone touching it: walk `oneOf` (which pairs a `type` tag with a config struct), never `$defs` (which also holds shared field types like `Secret`); field order is `required` order then alphabetical; `Option<T>` arrives as `anyOf: [T, null]` when the inner type is a `$ref` and as `"type": ["integer", "null"]` when it isn't — `scalar_type_of` handles the second spelling.

A `FieldDoc` carries `field_type` (`FieldType`) beside the human-readable `type_name`. That's the same reflection serving a second consumer: the "add pipeline" modal generates its form from it, so a new component gets working controls and validation for free. `FieldType::Json` is the honest fallback for anything with a shape of its own (a tagged union like `buffer`) — it renders as a JSON box rather than a control that can't work.

`FieldType::PipelineId` is the one field type the schema alone can't derive: a pipeline id is a `String` like any other, so the field says so where it's declared — `#[schemars(extend("x-pipeline-id" = true))]` on `PipelineConfig.upstream` — and `docs.rs` looks for the marker, not for the field's name. The rule about not knowing component names covers their field names too, so any component that grows a reference to another pipeline gets the dropdown by adding that attribute. The options can't come from the schema either: they are the running graph, so `AddPipelineModal` derives them from the pipeline list and passes them down to `FieldEditor`. That control is the only one in the modal that reads its value back — the list can arrive after the modal opened, and a rebuild must re-mark what was already chosen rather than drop it.

### Editing the graph from the UI

The canvas has a `Mode` (`frontend/src/app.rs`) that starts at `ReadOnly`; edit affordances are `<Show>`n, not disabled, so read-only really is read-only. That includes moving cards: the title bar is a drag handle and the corner a resize handle only in edit mode, and double-clicking the title bar puts a card back under the automatic layout. A `<Show>`'s children must be `Fn` *and* `Send + Sync`, which is why `Card`'s drag handler keeps its id in a `StoredValue` — that makes the closure `Copy` and usable in both places.

The same applies to the edge handles: `ChannelGrip` and `PortGrip` are each two `<line>`s, a fat transparent one that catches the pointer (`.edges` sets `pointer-events: none`, so the hit line turns it back on for itself) and a visible grip. Note the label is an `aria-label` and not an SVG `<title>` child — `leptos_meta` claims `<title>` for the document's, and the browser tab ends up named after whichever edge rendered last. Their `.vertical` classes mean *opposite* things (a channel's is the route's direction, a port's is the face's), which is why the cursor rules are per-class rather than shared.

Drags are tracked with window-level listeners rather than on the card (a fast pointer leaves the card behind, and a `mouseup` outside it would never arrive). The delta is divided by the zoom, applied to the geometry captured at press time rather than accumulated, and written into `arrangement` live so the edges follow; the `PUT` happens once, on release. It's a browser-tab property — the API accepts writes either way, which is fine for a dev tool but shouldn't be mistaken for enforcement. Edits apply to the runtime immediately, so `revert` (reload the file) is the only undo, and `unsaved changes` in the navbar is the only thing between a session's work and a restart.

The `+` in the sidebar opens `AddPipelineModal` (`frontend/src/app.rs`), whose pure half is `frontend/src/form.rs` — drafts in, `POST /api/pipelines` body or a list of `FormError`s out, unit tested like `graph.rs`/`inspector.rs`/`docs.rs`. One non-obvious constraint shapes the component: the field boxes are **uncontrolled** (`value=` once, `on:input` writes, never reads back), because the field list is rebuilt when the kind or variant changes and a rebuild on every keystroke would destroy the `<input>` being typed into. `DraftSignals` exists for the same reason — per-part signals so typing doesn't invalidate the list.

`frontend/src/docs.rs` holds the page's pure logic (search filtering, grouping, anchors, doc-comment rendering) with unit tests, same convention as `graph.rs`/`inspector.rs`. One trap worth remembering: the docs lists are rebuilt with plain closures rather than `<For>`, because keying groups by family leaves stale components on screen when a filter changes a group's contents without changing its key.

## Notes

- `readme.md` holds the current TODO list — check it for what's in flight before proposing work.
- Leptos config lives in the root `Cargo.toml` under `[[workspace.metadata.leptos]]`; `site-addr` there (6767) is what the binary actually binds, not the `--port` arg.
- `Dockerfile` is a two-stage cargo-leptos build; the runtime image bakes in `config.json` and the `LEPTOS_SITE_*` env vars.
