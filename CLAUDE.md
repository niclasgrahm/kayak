# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

"kayak" — graph-based stream processing: an Axum server that runs configurable `input → transforms → output` pipelines, with a Leptos web UI.

## Commands

```bash
just dev                            # dev server on :6767 against example_config/ (hot reload; makes secrets.json if absent)
cargo leptos watch                  # the same without a config — hot reload on 127.0.0.1:6767 (builds WASM + server)
just build                          # production build — `cargo leptos build --release
                                    # --bin-features embed-assets`, i.e. one binary with the
                                    # frontend compiled into it. See "the static assets" below.
just test-embed                     # the embed's own tests; needs a `just build` first
cargo check                         # fast type check of the whole workspace
just ci                             # lint + test — what GitHub Actions runs
just test                           # cargo test --all-targets (offline: no NATS, no server)
just lint                           # cargo clippy --all-targets -- -D warnings
cargo run -- --config example_config/config.json --secrets example_config/secrets.json --debug
                                    # run the server binary directly (no WASM rebuild). Note it binds
                                    # :3000 and serves no frontend unless LEPTOS_SITE_ADDR/_ROOT are set —
                                    # `just dev` is the one that works. --connections <path> is optional:
                                    # without it the file is derived from the config's name.

docker compose up                   # NATS :4222 + publisher on test.subject, kafka :9092 + publisher on test.events, mqtt :1883 + publisher on test.mqtt, redis :6379 + publisher on test.redis, opc ua (opc-plc) :50000, postgres :5432, clickhouse :8123, rustfs (s3) :9000 with the `events` bucket
just test-http                      # hurl --test hurl/tests/*.hurl (needs the server running)
just start-baseline                 # hurl hurl/create_baseline.hurl — creates a sample pipeline
```

## Definition of done

These two rules are not negotiable and apply to every change, however small:

1. **New code ships with tests.** Any new or changed behaviour — a component, a handler, a config field, a bug fix — needs a test that fails without the change. A bug fix without a regression test is not a fix. If something genuinely can't be tested offline (a real NATS connection, say), say so explicitly and explain why rather than skipping quietly.
2. **`just ci` must be green before a task is called done.** That's `just lint` (clippy `-D warnings`) plus `just test`. Not "compiles", not "the new test passes" — the whole suite. If tests fail, report the failure and the output; never describe a task as complete with a red suite, and never disable, `#[ignore]` or weaken an existing test to get to green. A test that turns out to encode the wrong behaviour is a conversation to have first, not something to edit away.

Testing is documented in `website/contributing/testing.md` — read that before adding tests. In short: the runtime lives in `src/lib.rs` (not `main.rs`) so `tests/` can reach it; `src/testing.rs` holds the test doubles; `PipelineRuntime::from_parts` drives a run loop without a config; `api_router()` is called through `tower::oneshot` so HTTP tests need no socket. Adding a component config variant fails `tests/config.rs` until a wire-format sample is added — that's intentional.

Lints are strict by design: clippy `pedantic` plus `unwrap_used`/`expect_used` as warnings, and `clippy.toml` makes those apply in tests too. Removing remaining `.unwrap()`s is active work — flag new ones in review.

## Architecture

Four workspace crates:

- **`kayak-core/`** — shared, dependency-light types. All config structs/enums (`config.rs`), plus `PipelineId`, `MessageBatch = Vec<Arc<serde_json::Value>>`, `UiEvent`, `PipelineDto`, the canvas layout types (`layout.rs`), and the endpoint table the HTTP surface is built and documented from (`api_docs.rs`). Compiles for both native and `wasm32`, which is why it exists: the frontend needs the same config types as the server. It has no async/network deps and no real `main.rs`.
- **`/` (root `kayak` crate)** — the Axum server and the whole stream-processing runtime. It is a **lib + bin**: everything lives in `src/lib.rs` and its modules so integration tests can import it; `src/main.rs` is only clap args, tracing setup and the Leptos router wiring. `api_router()` — re-exported from `lib.rs`, defined in `src/endpoints.rs` — builds the JSON/SSE routes for both.
- **`frontend/`** — Leptos 0.8 SSR + hydrate crate. `cdylib`+`rlib` with `ssr`/`hydrate` features; the root binary depends on it with `ssr` and mounts it via `leptos_axum`.
- **`kayak-bench/`** — the throughput harness. A `bin` that drives the runtime in process and prints what the run loop costs; not part of the server and not part of `just ci`. See "the throughput harness" below.
- **`docsgen/`** (`kayak-docsgen`) — writes the doc site's reference out of the same reflection the UI renders. A lib + bin, and the only crate that depends on *both* core and the server crate — the one thing it needs from the server is `openapi::document()`. See "the doc site" below.

### The pipeline model

A **pipeline** is one `inputs → [transforms] → outputs` chain. A pipeline may have several inputs (merged into one stream) and several outputs (each gets every batch); `inputs` and `outputs` are JSON arrays, and there is no singular form. Pipelines are identified by `id` (from config, or a random `petname` if omitted) and form a **graph**: the `pipeline` input kind subscribes to another pipeline's output, so one pipeline can fan out to several downstream ones. `example_config/config.json` (with `config.connections.json` beside it) is the worked example and deliberately covers every component kind and every connection kind: two roots (a NATS source and a dummy ticker), a wide fan-out under the source, one pipeline (`everything`) fed by three inputs — two upstreams and a nats subject another pipeline publishes to — one pipeline with two outputs of different kinds, and one pipeline at depth 3. Keep it that way when adding a component — it's what the UI is inspected against, and `tests/graph.rs` builds the whole file. It also carries four deliberately **broken** pipelines (`broken_*`, all fed by `heartbeat` so they fail once a second with nothing running): a card's failure history and a chart with holes in it don't exist until something is actually broken, so a sample where everything works exercises half the UI. `broken_intermittently` is the one that matters — it fails in bursts rather than constantly, which is the shape a real outage has. They must keep failing at *runtime* rather than at build time: `tests/graph.rs` asserts every declared pipeline builds and registers.

Data flowing through is always `Arc<MessageBatch>` — a batch of `Arc<serde_json::Value>`. There is no typed schema; everything is untyped JSON, and transforms address fields by name.

Three object-safe traits define the plugin points, all in the root crate:

- `inputs::InputSource` — `async fn next() -> Result<Arc<MessageBatch>>`. Several of them are merged by `inputs::merge` into an `inputs::Merged`, which runs a pump task per input rather than `select!`ing over them — selecting would cancel a losing `next()` and starve any input that waits on a timer. One input failing is reported and survived; the run loop only stops when the last one is gone.
- `transforms::Transform` — `async fn apply(batch) -> Result<Vec<Arc<MessageBatch>>>` (one batch in, N batches out — that's how `splitter` works)
- `outputs::OutputDestination` — `async fn init()` + `async fn emit(batch)` + `async fn finish()`. `finish` defaults to a no-op and is called once by the run loop *after* the loop ends, however it ended (cancelled, or the input died). It exists for the two outputs that hold a *part*: `file` has a `json_array` to close, `s3` has a buffered object that has not been uploaded at all. Its errors are published like an `emit` error rather than returned — the pipeline has already stopped, and failing the run there would report the shutdown as the pipeline's failure. `tests/pipeline.rs` pins that it's called exactly once down *both* `select!` arms.

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

`PipelineError` grew `NotAccepting` (a running pipeline with no `http` input),
`Backpressure` (503) and `Unauthorized` (401). `NotAccepting` is a 404 like
`NotFound` and is deliberately a separate variant: one is fixed by creating the
pipeline, the other by giving it the input.

**`auth` is the endpoint's own credential and is not the server's sign-in.**
That separation is the design, not an omission: the ingest path stays
`Access::Public` in the api_docs table however the server is configured, because
a device posting readings is not an operator and one shared credential for every
publisher is wrong the moment there are two. `HttpAuthConfig`
(`kayak-core::config`) has a `bearer` and a `header` variant; `Requirement`
(`src/inputs/http.rs`) is the live half. Optional, and absent is byte-for-byte
the old behaviour — the same promise `batch_cap` and `envelope` make.

It rides on the **`Inboxes` registration**, not on the input, so the
requirement's lifetime is exactly the endpoint's and deleting a pipeline takes
the credential down with the path it guarded. Four rules there are load-bearing.
`Credentials` is a **separate type from `PostMeta`** and unfiltered, which is
what makes it impossible for a credential to take the metadata's path into an
object store — never make it `Serialize`, and its `Debug` prints header names
only. A header on `ALLOWED_HEADERS` is **refused at build time** rather than
filtered later, as is a credential that resolves to empty. The check happens
before the `try_send`, so someone without the token can't fill the queue and
turn the holder's 202 into a 503. And `check` (the empty-post path) is
authenticated too — `[]` to every id in turn is otherwise a free enumeration.

Known and accepted: the status code says whether a pipeline exists and whether
it is guarded (401 / 202 / 404). Unavoidable while the credential is
per-pipeline. There is no rate limiting and no `WWW-Authenticate` header — the
latter for the reason `auth::refuse` gives. HMAC over the body is the sketched
third variant and is in `docs/roadmap.md`; it needs the raw body, which the
handler hands straight to the JSON extractor.

### The opcua input

The industrial one, and the first input whose messages are *assembled* rather
than parsed: an OPC UA server publishes a value, not a document, so
`src/inputs/opcua.rs` builds the JSON itself. Subscription only — a monitored
item per node, one message per value change. Polling is the other half of the
component and is deliberately not built (`docs/roadmap.md`).

Four decisions are load-bearing:

- **The tag is part of the message, not of the envelope.** Every other input
  puts what it knows behind the opt-in envelope because the message means
  something without it; a reading does not — `21.5` with no node and no name is
  not data. So `node`, `name`, `value`, `status`, `source_timestamp` and
  `server_timestamp` are always the message, and `metadata.rs` declares only
  `connection` for this kind. That is also what makes everything downstream
  ordinary: per-tag aggregation is `group_by: ["name"]` and needs nothing new.
- **`status` is always present and an absent one reads as `Good`.** A failed
  instrument does not go quiet — it reports `BadDeviceFailure` with no value
  once and then nothing — so those are passed on with `value: null` rather than
  dropped, and a `filter` decides. The encoding leaves a `Good` status *off the
  wire*, which is why `status_name(None)` is `Good` and not "unknown"; get that
  backwards and every `status == "Good"` filter drops the whole stream.
- **The library owns reconnects, this module owns outages.** The client is built
  with `session_retry_limit(-1)` and `recreate_subscriptions(true)`, so a blip
  is healed underneath with the monitored items put back — much better than
  tearing down and browsing again. What is left here is what cannot be healed:
  a connect that never completes (hence `CONNECT_TIMEOUT`, because
  `wait_for_connection` never returns while the event loop is retrying) and an
  event loop that *ends*, which is watched for in `next`'s `select!` because a
  dead session neither publishes nor closes the queue.
- **The queue between the callback and the run loop is bounded and overflow is
  counted.** `DataChangeCallback` is synchronous — it runs inside the client's
  publish handling — so it cannot wait for a slow pipeline, and there is nothing
  to push back on in any case: the server publishes regardless. So `try_send`,
  a counter, and one warning per run of drops.

Two smaller things found the hard way. The endpoint is dialled **directly**
(`connect_to_endpoint_directly`, no `GetEndpoints` first): a server behind
docker or NAT advertises the hostname it knows itself by, which is regularly not
one the client can resolve — the compose simulator does exactly this. And the
client's default message limits (5 chunks, ~320 kB) are too small for a browse
of any real address space; the first one attempted came back
`BadResponseTooLarge`, hence `MAX_MESSAGE_BYTES`/`MAX_CHUNK_COUNT`.

`json_of` is the pure half and is where the value types are decided: scalars map
across, an `f32` goes through its own **shortest decimal form** (`f64::from(0.1f32)`
is `0.10000000149011612`, which reads as nonsense in a log line), and the
structured leftovers — extension objects, nested data values, diagnostic info —
return `None` so the reading is skipped with a warning rather than rendered as a
`Debug` string.

`browse` is the convenience with a cost that belongs in any review of it: what
the pipeline reads is then the address space *at the moment it started*.
`depth` defaults to 3 and 0 is refused — there is no spelling for "all of them",
because a plant server's whole address space is thousands of nodes.

Security is anonymous or username/password over `SecurityPolicy::None`, and the
connection says so rather than pretending. The visible consequence is two ERROR
lines from the client about a missing application instance certificate on every
connect; `main.rs`'s `QUIET` silences the one module that is *only* about
reading those files, and the other two are left alone because their modules log
real failures too.

### Message metadata (the envelope)

`InputConfig.envelope` sits beside `buffer` — available on every input kind,
declared by none of them — and attaches what the input knows about a message to
the message. **In band**, as ordinary JSON fields, and that is the decision the
whole thing turns on.

The argument is the transforms that change cardinality. Out of band (a
`Message { value, meta }`, which is what Benthos does) `reduce`, `splitter`,
`buffer` and the `http` transform each have to answer "whose metadata comes
out?", and there is no good general answer — Benthos picks the first message's,
arbitrarily. In band the question doesn't arise: metadata is data, so
`group_by: ["_meta.subject"]` is a `group_by` and nothing in the reducer knows
metadata exists. It also costs zero type changes — `MessageBatch =
Vec<Arc<Value>>` is in all three traits, `testing.rs`, `BatchPreview` and every
test — and shows up in the UI log for free. What it costs is that metadata
reaches the outputs and can collide with a payload key; both are the user's
call, which is why the field names are configurable and the whole thing is
opt-in.

Two shapes because a payload is not always an object: `merge` adds a field (and
**skips a non-object payload with a warning**, like a non-JSON one), `wrap`
moves the payload under a field of its own. Absent is byte-for-byte today's
behaviour and that is a promise, the same one `batch_cap` makes — an input that
quietly re-shaped its messages would break every field reference downstream.

The split of responsibility is the one non-obvious part of the implementation.
`envelope` is the *wrapper's* field but only the input knows the interesting
half (a subject, an offset), so `BuildInputConfig for InputConfig` puts the
config on `BuildCtx` around the kind's build and takes it straight off again;
each input calls `ctx.envelope(kind, connection)` and adds its own per-message
fields at `apply` time. An input that forgets to call it attaches nothing — the
compiler can't catch that, which is why the metadata each input attaches is
*declared* in `kayak-core/src/metadata.rs` and
`every_input_declares_its_metadata` fails without an arm. `/docs` renders that
declaration; nothing is written by hand.

The `http` input is the one that needed plumbing: the request is gone by the
time the run loop reads the messages, so `PostMeta` travels down the inbox
channel with the batch. Its headers are an **allow-list**
(`inputs::http::ALLOWED_HEADERS`) and must stay one — a deny-list or an `x-`
prefix rule passes `x-api-key` through, and a credential written into an object
store outlives the request by years. `main.rs` serves with
`into_make_service_with_connect_info` so `remote_addr` exists; nothing fails
without it (the router tests have no peer), which is why it's read out of the
request extensions rather than taken as a `ConnectInfo` extractor.

**Field paths** (`src/fields.rs`) are what make metadata reachable, and they are
the *only* way any transform addresses a field — `filter`'s two comparisons and
`reduce`'s `present()` all go through `fields::get`, so a path works everywhere
or nowhere. **An exact key wins over a path**, which is what makes them a
compatible addition rather than a breaking one: a source with dots in its field
names keeps working and needs no escaping rule. A reducer writes a grouped path
out under its **leaf** (`fields::leaf`) and refuses two paths sharing one at
build time.

### State buckets

Named, **global**, declared at the top of the config file and referred to by the
pipelines that use them — deliberately the shape connections already have. The
argument for global over per-pipeline is the reference-data case: one pipeline
remembers the current recipe per machine and six unrelated ones stamp it onto
their output, which per-pipeline state can only answer with six copies and six
edges that carry nothing else.

**The unenforced rule is the important one and belongs in any review of this
area**: two pipelines sharing a bucket are two run loops with no ordering
between them, so ordering-sensitive correlation must live in *one* pipeline and
sharing is only for state that doesn't change on the timescale of a message.
Documented in `kayak_core::state`'s module docs and `website/pipelines/state.md`; not preventable.

`kayak-core/src/state.rs` holds the declaration (`StateBuckets`,
`StateBucketConfig`, `PipelineState`) plus the API DTOs, since `api_docs` needs
`schema_for!`. `src/buckets.rs` is the live store. Three properties there:

- **In memory, and that is a decision rather than a stage.** The store is
  touched per message (no network round trip), and durability without
  checkpointed *input positions* would be worse than none — a core nats
  subscription has no replay, so restoring a half-finished piece of work whose
  remaining messages were never delivered yields an answer that is wrong
  invisibly. A durable backend starts at the input, not here.
- **Every bucket is bounded and there is no unbounded spelling.** Enforced in
  the store rather than by the transforms, so a new stateful component can't
  forget.
- **Eviction is lazy — no sweeper task.** Expiry is applied when a bucket is
  touched, which is what makes it work at all: transforms are only driven by
  arriving batches and get no tick. This is the same missing tick behind the
  "idle file output holds its part open" issue; solving that one properly would
  let this be exact.

`Buckets::rebuilt` is the revert rule: contents survive a reload, **unless that
bucket's declaration changed** (its contents may not satisfy the new limits).
That is what makes state survivable without being unaccountable — a revert
rebuilds every pipeline, so emptying the buckets would make an edit to an
unrelated pipeline cost an hour of accumulated state.

`src/transforms/state.rs` is `remember` and `recall`. Two transforms rather than
one because *chain order is the semantics*. `remember` is a **tap** — it passes
its batch on unchanged. `recall` writes to the top level (so a downstream
`group_by` needs no prefix) and defaults `on_missing` to `skip`, which is the
opposite of the reducer's default and deliberately so: every stateful pipeline
has a warm-up in which nothing is remembered, and `error` would fail them all at
startup. Both warn **once per transform, not per message** about a key the
stream doesn't carry — that's a config mistake, not an event, and a line per
message buries the log.

`Condition` is a new internally-tagged enum rather than reusing `FilterKind`
(which is externally tagged): a `Vec<FilterKind>` would reflect as
`FieldType::Json` and fail `no_component_field_needs_raw_json`. Several
conditions mean *all of them*; there is no `or` and no nesting, because that is
the point where this becomes an expression language.

**The config file now has two spellings** (`kayak_core::state::ConfigFile`): a
bare array of pipelines, or a document with `state` and `pipelines`. Both are
permanent — `render_as_document` picks the array whenever there are no buckets,
so no existing file changes by a byte when saved. `persist::read`/`write` are
still the only places a *format* exists, and now also the only place the two
spellings are told apart.

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
`kayak-core/src/connections.rs`, the typed accessor beside it, the `type_name`
arm and the kind const, the `BuildCtx` helper in `src/lib.rs`, a wire-format
sample in `tests/config.rs`, and the expected list in
`kayak-core/src/docs.rs::connections_are_documented_as_their_own_family`. The docs
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
how messages are laid out inside it, and touches no filesystem — `src/outputs/
s3.rs` takes it whole. file.rs is only the destination. Keep new
format or rotation work on the rotate.rs side of that line.

Two properties there are load-bearing. Rotation is checked *after* a batch is
written, so a batch is never split across two parts (`max_rows` is a floor, not
a ceiling). And `Rotation::is_full` returns false at zero rows — without that, an
interval trigger on an idle pipeline closes and reopens a part every time it is
asked, filling the directory with empty files.

### The s3 output

A **separate component** from `file`, not a mode of it, and the reason is one
property that runs deep: **an object store has no append.** `file` opens a file
and writes each batch into it, so a part is readable while it fills; a bucket has
no such state, and `PUT` writes an object whole. So `src/outputs/s3.rs`
accumulates a part in memory and uploads it on rotation — which makes `rotate`
**required** here (a `RotationConfig`, not an `Option<RotationConfig>`, plus a
`Rotation::rotates()` check at build time) and optional on `file`. Without a
trigger the pipeline would hold its whole run in RAM; it refuses to build instead.
Everything else is shared verbatim through `rotate.rs`, which is what that split
was for.

There is **no `--data-dir` analogue and there cannot be**: the local sandbox
works because the server can ask the filesystem where a path really landed, and
nothing equivalent exists for a remote namespace. The boundary is the
credentials on the connection. The one guard rail on this side is `allow_http` —
plaintext credentials are refused unless the connection asks, which is what the
local rustfs does. Don't add a `Root`-shaped check here and don't reach for one;
a `prefix` is not a path and cannot escape anything.

`object_store` is the client, chosen over `aws-sdk-s3` for what comes next: Azure
Blob and GCS are the same crate behind feature flags, so each is a connection
kind, a destination module and a `FieldType::Connection` marker — not new
machinery. Note the crate's `Path::child` is deprecated in 0.14 (use `join`), and
`ObjectStoreExt` is the trait `put` lives on.

Multipart upload is deliberately *not* used: S3's 5 MiB minimum per non-final
part doesn't fit the batch sizes a pipeline produces, and doing it properly means
this same accumulation with a flush threshold on top.

The sample's file output is `heartbeat_to_disk`, and its upstream is deliberate:
`heartbeat` is a dummy input, so it is the one pipeline in `example_config/` that
writes real output without `docker compose up`. Its cost is that the sample no
longer loads on a server with no `--data-dir` — so `just dev` and
`tests/graph.rs` both pass `--data-dir dev_data`, and the connection's root
(`dev_data/events`) is relative, resolving against the working directory in
both. Change one and change the other. The container image doesn't pass it
(nothing is baked in there), so running the sample out of the image takes the
flag on the command line — see `website/operating/deployment.md`. `dev_data` is
gitignored; the build creates it.

### Column mapping (the database outputs)

`columns` on the postgres output maps message fields onto real columns and
types. It lives in **`kayak-core/src/columns.rs`**, not in the postgres config,
and that placement is the design: every database output asks the same two
questions — which field goes in which column, and what to do about a message
that doesn't carry it — so the next one reuses the mapping whole and only
renders the DDL differently. The clickhouse output is that claim collected:
it declares no mapping types of its own.

Which is why the types are **logical** (`float`, `timestamp`, `json`) rather than
one server's spelling. A config naming `double precision` would have to be
rewritten to point elsewhere, and a closed set reflects as a dropdown in the
add-pipeline form for free.

The split of work mirrors that: `src/outputs/columns.rs` is the neutral half —
`ColumnPlan::build` validates, `plan.row(message)` produces `Vec<Option<String>>`
or `Row::Skipped`, `Identifier` and `Table` check everything that reaches the SQL
text, and nothing in it knows any SQL. `src/outputs/postgres.rs` is only names,
DDL and placeholders. Keep new mapping work on the neutral side of that line —
`Table` moved there when clickhouse needed it, which is the shape that question
takes.

Four properties are load-bearing:

- **Absent `columns` is byte-for-byte the old table** (`id`/`received_at`/
  `payload` jsonb), the same promise `batch_cap` and `envelope` make. `Layout`
  has the two arms for exactly this; `sensors_archive` in `example_config/` is
  the one that exercises it and must stay unmapped.
- **Values are checked, never coerced** — `"12.5"` into a `float` is an error.
  But every value *travels* as text and is cast in the statement
  (`$2::text::NUMERIC`), which is what keeps a number's own digits and hands
  timestamp/uuid parsing to the server. Don't "fix" that into driver-native
  binding: it would pull a type mapping into the neutral module and route
  decimals through f64.
- **Everything contradictory is refused at build time** — a not-null column told
  to write null, a `message: true` column that isn't `json`, a key or index
  naming an unmapped column. Same rule as the reducer's.
- **Creation never alters.** `IF NOT EXISTS` and nothing else; a table whose
  shape has moved on fails the insert with postgres' error. Migrating a live
  table from a config file is a much bigger promise and is deliberately not
  made. Naming a `primary_key` drops the implicit `id`/`received_at` and makes
  those columns not-null (`ColumnPlan::require_not_null`), because postgres
  would anyway.

`fields::root_segment` is the counterpart to `leaf`, added for the
`on_extra_fields` check: a column reading `sensor.id` claims `sensor`.

### The clickhouse output

The second consumer of the mapping, and the thing that keeps `columns.rs`
honest: it adds no `ColumnType`, no policy and no mapping concept — only DDL, a
wire format and a request. Three differences from postgres, each of them
ClickHouse being itself rather than a gap, and none of them worth erasing:

- **`order_by`, not `primary_key`.** No auto-increment column and no unique
  constraint exist here, so there is no surrogate key to fall back on and
  `order_by` is `MergeTree`'s *sorting* key — an index and a layout, not a
  uniqueness claim. The name is the honest one and should stay. Naming none gets
  the implicit `received_at` and sorts by it; named columns go through
  `require_not_null` for the same reason postgres' key columns do. There is
  deliberately **no `indexes` field**: the sorting key is the index.
- **A batch is one insert**, not a statement per message — a columnar store that
  merges parts in the background makes a part per row otherwise. This is why an
  input `buffer` matters more in front of this output than anywhere else, and
  why `Layout::body` builds the whole body before anything is sent.
- **Values travel as `JSONCompactEachRow`**, which is the same division of labour
  `$n::text::NUMERIC` buys on the postgres side, spelled the way this server
  spells it. `token()` decides which of the plan's checked texts is *already*
  JSON — that is an invariant of the **pair** of modules rather than of either
  half, which is why `every_column_type_produces_a_json_line` pins it by parsing
  the line back.

Three smaller things that were each found the hard way and are load-bearing.
`Content-Length` is set explicitly, including on the bodyless statements: a POST
that is neither chunked nor length-declared is a 411 here, not a statement. The
DDL and the connection check go down the same `execute` as an insert, which is
what makes `create_table: false` still fail at startup rather than on the first
batch — an HTTP client opens nothing, so there is no "connect" to find it out.
And the two settings on every request are not tuning: `date_time_input_format=
best_effort` is what makes an RFC 3339 timestamp parse at all, and
`input_format_null_as_default=0` is the backstop that keeps a column that cannot
hold a null from quietly holding a zero.

`json` is a `String` holding the JSON text (`JSONExtract` reads it) and `date` a
`Date32`; the connection is the HTTP interface with `allow_http` following the
s3 connection's rule, since the credentials go with every insert.

### The http output

The pushing half of the http family, and the one output with **no connection
behind it**. That is the decision worth knowing: a connection holds *what a
system is* against what one pipeline wants from it, and for a webhook the url
is the whole of the first half — two pipelines posting to two endpoints on one
host share nothing worth naming once. So `url` sits on the component, and so
does `auth`, for the reason the `http` input's does: one shared credential for
every endpoint is wrong the moment there are two.

`auth` is literally the input's `HttpAuthConfig`, read the other way round —
the input compares what arrived against it, the output sets it. Only the input
carries the `ALLOWED_HEADERS` refusal, because only an `envelope` can write a
header into the messages. The value is marked `sensitive` on the `HeaderValue`,
and `describe()` strips userinfo out of the url before it reaches an error
message.

Four properties are load-bearing:

- **Anything but a 2xx fails the batch**, with the endpoint's own body quoted
  (cut at `MAX_DETAIL_BYTES`, because that text becomes a `history`
  `ErrorSignature` key as well as a UI line). A webhook rejecting the data has
  to show up on the card rather than being written off as delivered. The reply
  is otherwise discarded — a service whose answer the pipeline should carry on
  with is the `http` *transform*.
- **`init` does nothing, deliberately.** Every other output that talks to a
  server connects there so a bad config fails at startup; there is no request
  to make here that would not itself be a delivery. A wrong url is caught at
  build time (parsed, and the scheme checked) and an unreachable one at the
  first batch, through the same `Gate` the clickhouse output uses for the same
  reason — `reqwest::Client` is a stateless pool, so what is worth skipping is
  the *request*.
- **`verb` is honoured and `GET`/`DELETE` are refused at build time.** A
  request with no body would send none of the messages. Note this is the
  opposite of the `http` transform, which accepts `verb` and ignores it — a
  known issue, and this is now the argument for settling it.
- **`body` picks `batch` (one request, a JSON array) or `message` (one request
  each).** The receiving API's shape, not a tuning knob. Under `message` the
  first failure stops the batch, the same all-or-nothing a broker publish loop
  has. An empty batch sends nothing at all.

`heartbeat_to_webhook` in `example_config/` points at the server's own `ingest`
endpoint on `127.0.0.1:6767`, so it is the one http output that does real work
under `just dev` with nothing else running — `heartbeat_to_disk`'s trick. It
hardcodes the port `Cargo.toml` binds; change one and change the other.

### The map transform

`map` is the only transform that **writes** a field the config named, which is
why `src/fields.rs` grew `set`/`remove` beside `get`. The write rule is spelled
out in that module's docs and everything that writes has to follow it: an
existing **literal key** wins (so a write round-trips a read), otherwise the
path is created, and a path running through a non-object is **refused, not
overwritten** — replacing a scalar with an object loses data invisibly.

The split is `kayak-core/src/mapping.rs` (declaration) against
`src/transforms/map.rs` (evaluation), the same line `columns.rs` draws and for
the same reason: the declaration has to compile for wasm so the form can render
it, the evaluation needs chrono and a JSON parser.

Five properties are load-bearing:

- **One message in, one message out, always.** That is what keeps `map` out of
  `filter`/`splitter`/`reduce` territory, and it is why `MapMissingPolicy` has
  no "drop the message" arm. Don't add one.
- **`mappings` is an ordered list and the order is the semantics.** A mapping
  reads what earlier ones wrote — that is how a two-step arithmetic works. A map
  keyed by target name is unavailable for two independent reasons: JSON object
  key order isn't guaranteed, and a `HashMap` reflects as `FieldType::Json` and
  fails `no_component_field_needs_raw_json`.
- **`map` reshapes; it does not compute.** One arithmetic operation per mapping,
  no nesting, no conditionals. The point where chaining reads badly is the
  boundary where a scripting language is the honest answer, and it is meant to
  be visible. Don't grow `Operand` into an expression tree.
- **`cast` is the one place in kayak that coerces**, against the column
  mapping's "check, never convert". `CastType` is deliberately *not*
  `ColumnType`: `integer`/`bigint` are one thing in JSON, `decimal` can't be
  held distinctly by `serde_json`, and `json` means "parse this string" here
  rather than "store whatever this is".
- **A present value that won't convert is an error whatever `on_missing` says.**
  `on_missing` is about a sparse stream; a `"twelve"` in a float field is a
  stream that isn't what the config claims, and folding the two together hides
  it forever.

There is deliberately **no** build-time check that a mapping reads a field a
later mapping writes — the message may already carry it, so the check has false
positives and a false refusal is worse than the warning.

`Literal` (a tagged union of text/number/boolean/null) exists because a bare
`serde_json::Value` field would reflect as a hand-write-the-JSON box. Reach for
it rather than `Value` anywhere a component config needs a literal.

Note this is what pushed `docs.rs::MAX_NESTING` from 4 to 6:
`mappings[].concat.parts[].value` is a list of a union containing a list of a
union, at depth five. That constant is a stack guard against self-referential
schemas, not a statement about how deep config should nest — raise it when
something legitimate reaches it.

### Secrets

Config fields that can hold credentials are typed `Secret` (`kayak-core::config`), not `String`. They all live on *connections* now rather than on components. `Secret` only ever holds the *unresolved* `${NAME}` template, which is what makes it safe to serialize back out of `GET /api/pipelines` and to compile for wasm. Resolution happens at build time via `ctx.resolve()` and yields a `secrets::Resolved`, whose `Display`/`Debug` print the template rather than the value — so error contexts can name a connection without leaking it. Reaching the real value takes `.expose()`; flag new call sites in review, and never put a `Resolved` into anything `Serialize`. Stores (`EnvStore`, `FileStore`, `ChainStore`) live in `src/secrets.rs`; `main.rs` chains env ahead of `--secrets <file>`. `src/testing.rs` has `MapSecretStore` for tests. See `website/io/secrets.md`.

Note that `$defs` in the generated schema now holds non-component types (`Secret`), so anything reflecting over the schema has to distinguish those from components — see the docs section below.

**Adding an input** additionally needs an arm in `kayak-core/src/metadata.rs`
(`every_input_declares_its_metadata` fails without one) and a
`ctx.envelope(...)` call in its `build`, applied per message in `next`.

**Adding a component** therefore touches five places: the config struct + enum variant in `kayak-core/src/config.rs`, the `build()` dispatch arm in `src/config.rs`, the impl module, and a wire-format sample in `tests/config.rs` (which fails until you add it). The config struct also needs a doc comment, and its fields want one — that's what `/docs` shows, and a missing one fails a test in `kayak-core/src/docs.rs`.

The config enums use `#[serde(tag = "type", rename_all = "snake_case")]` with `#[serde(flatten)]` wrappers, so JSON looks like `{"type": "nats", "urls": ..., "subject": ...}`. They also derive `schemars::JsonSchema` with `#[schemars(title = "...")]` — `/docs` generates component documentation by reflecting over `schema_for!(InputKind)` etc., so the title/doc-comments on config fields *are* the docs.

### The reducer

One `reducer` is a **list of aggregations over a list of grouping fields**, not
one function over one field — `{"function": "sum", "field": "value", "as":
"total"}` × N, plus `group_by`, plus `on_missing`. Three properties are
load-bearing and none of them can be had by chaining reducers:

- **Several answers in one message.** A chain can't do it, because each reducer
  throws away the fields the next one needs. That's also why the output message
  is *assembled* (group fields, then each `as`) rather than being one fixed
  shape — the old `{original_field, reduced_value}` was the shape that made a
  second reducer downstream useless.
- **`group_by` changes the cardinality**: one message per distinct key, emitted
  in **first-seen** order. First-seen rather than sorted because a reducer sits
  in a stream, and arrival order is the only order it has a claim to. Groups are
  found by a linear scan of the keys — a batch holds a handful of distinct
  values, and hashing would mean giving `serde_json::Value` a `Hash` it doesn't
  have.
- **`on_missing` defaults to `error`**, which is the behaviour that was there
  before. A sum over "whichever messages happened to carry the field" is wrong
  in a way nothing downstream can see, so `skip` has to be asked for. A field
  present but `null` counts as missing — the same fact said two ways.

Everything that would otherwise be a strange message once per batch forever is
refused at `build()` instead: no aggregations, a function other than `count`
with no `field`, a blank or duplicated `as`, an `as` that would overwrite a
`group_by` field. `count` is the one function with no field, and both readings
of it are useful — messages in the group, or messages that carried the field.
`min`/`max` compare numbers as numbers and strings alphabetically, which is what
makes `max` over an ISO timestamp the latest one; mixed types are an error
rather than a guess.

Buffering is an input decorator, not a transform: `InputConfig.buffer` wraps any
`InputSource` in `inputs::Buffered`. Three spellings — `static` (a count),
`tumbling` (a window), `batch` (both, whichever first) — but one behaviour:
`BufferKind::limits` flattens them to a pair of `Option`s and `next()` has one
loop, because the combined case is not a third thing, it is the other two at
once. Two rules are load-bearing. **A buffer never emits an empty batch**, and
the way that is achieved is that the window opens at the *first message of the
batch* rather than at the call — so windows are not wall-clock aligned, and what
a buffer promises is a latency bound rather than a cadence. And `size` counts
**messages, not arriving batches**, overshooting rather than splitting one, the
same floor-not-ceiling rule `max_rows` follows. There is *also* a `buffer`
transform — different thing, different place — and `max_batch` on kafka/nats is a
third thing again: it never waits.

**`max_batch` on the kafka and nats inputs is a third thing again, and its
default is a promise.** `inputs::batch_cap` is where that promise lives: absent
means 1, one message per batch, which is what those inputs have always done. A
pipeline that must see its messages one at a time is a real case, and an input
that quietly grouped them would be wrong in a way that is nearly invisible from
the outside — so batching is opt-in and stays opt-in. Where it differs from
`buffer` is that it never *waits*: it takes one message, then drains whatever has
**already arrived** with `now_or_never`, so a quiet topic yields batches of one
however high the cap is and only a catch-up ever fills one. That is what makes it
safe to raise, and raising it is the cheapest fix there is for a consumer
replaying a backlog — it divides the run loop's per-batch work, the transforms,
the fan-out and the feed all at once.

### Runtime & state

`AppState` (`src/state.rs`) holds `Mutex<HashMap<PipelineId, PipelineHandle>>`, the connections (plus their own saved-snapshot), and the UI event broadcast channel. Creating a pipeline builds a `PipelineRuntime` and `tokio::spawn`s its `run()` loop; each `Pipeline` owns a `CancellationToken` that `delete_pipeline` cancels, and the run loop `select!`s on it against the next input message. Downstream fan-out is a `Mutex<Vec<mpsc::Sender>>` on `Pipeline`, populated by `subscribe()`.

Lock order, worth preserving: the pipelines lock is taken *before* the connections lock and never the other way round — `delete_connection` asks `pipelines_using` first and lets that guard go before it touches the map.

Note the concurrency shape: `std::sync::Mutex` guards, held across map lookups but never across `.await` — the lock is dropped/cloned out before awaiting sends. Worth preserving. `revert` obeys it the awkward way round: it cancels and takes the join handles out under the guard, drops the guard, *then* awaits them.

The run loop's `select!` is `biased` on purpose, and the cancellation check in its error arm is not redundant. Teardown cancels every pipeline and then drops the upstreams, so a downstream wakes with both its cancellation and an "upstream is gone" ready; an unbiased `select!` reported our own shutdown as a pipeline failure about a third of the time, and those errors surfaced on the UI cards of the *new* pipelines that had just taken the same ids. The rule is "was **I** cancelled" — an input dying under a pipeline that is still running must still be reported. `tests/pipeline.rs` pins both halves, and repeats the assertion 20× because a coin-toss regression test isn't one.

`AppState` also holds an optional `config_path`, a fixed `save_dir` and a `saved` snapshot. The path is optional *and* mutable (`Mutex<Option<PathBuf>>`) because a server started without `--config` can still be asked to write one: `save_config_as` calls `adopt`, and from that save on the created file is what `revert` reloads, `has_unsaved_changes` compares against and the layout is written beside. Adoption only ever goes from none to some — a save-as on a server that already has a file leaves the loaded one alone. `save_dir` is the boundary saves are confined to and is *not* derived from `config_path`: it's the config file's directory, or the process's working directory when there is no config file. **The config file is a load source and a save target, never a mirror**: `create_pipeline`/`delete_pipeline` never write, and an earlier design where they did is what turned "load a file" into "rewrite a file". Writing happens only in `save_config_as`, via `src/persist.rs` — deterministic order (topological, ties by id) and an atomic temp-then-rename, because the file is meant to be committed.

The file can be JSON or YAML (`kayak_core::ConfigFormat`), decided by the extension and nowhere else: `persist::read`/`persist::write` are the only two places a format exists, and everything past the parser has `Config`s that don't remember which it was. `POST /api/config/save` takes an optional `format`; without one the name decides. The `saved` snapshot behind `has_unsaved_changes` is *always* rendered as JSON — it's a fingerprint of the graph, not of the file.

Three things to preserve. `persist::save_path` rejects anything but a bare file name and is a security boundary, not a nicety — the path comes from an HTTP request, so an unconstrained one is an arbitrary write; refuse, never normalise. `has_unsaved_changes` compares `render(current)` against the `saved` snapshot, which is exact *because* `render` is deterministic. And `revert` parses the file before tearing the runtime down, so a file broken by hand doesn't cost you the running graph.

### The static assets (`src/site.rs`)

The frontend's files — the WASM bundle, the stylesheet, the vendored
API-reference renderer — are compiled **into the binary** in a production
build, so a release is one artifact rather than a binary plus a `target/site`
directory that `LEPTOS_SITE_ROOT` points at. A binary moved without that
directory serves a page whose bundle 404s, which reads as a blank canvas rather
than as a missing file; that is the failure this removes.

Four things are load-bearing:

- **It is a cargo feature, `embed-assets`, and it is off by default.**
  `target/site` is a *build output*, so embedding it makes the root crate
  uncompilable until the frontend has been built — on by default would put a
  WASM toolchain in front of `cargo check`, `cargo test` and `just ci`. `just
  build` and the `Dockerfile` pass `--bin-features embed-assets`; nothing in
  the dev loop does, which is also what keeps `cargo leptos watch`'s hot reload
  working. That one command is safe because cargo-leptos builds the client
  before the server.
- **The fallback is layered, not replaced.** `site::fallback` looks in the
  embed and hands everything else to `leptos_axum::file_and_error_handler`,
  which is what renders the shell with a 404 for a path that is neither a route
  nor a file. Without the feature the fallback *is* leptos', so a dev build
  behaves exactly as the server did before this existed. Don't reimplement that
  arm — it would mean owning a copy of leptos' SSR response builder.
- **The serving half is generic over where the bytes come from.** `Assets` is a
  trait and `respond` takes `&dyn Assets`, which is what makes content types,
  `ETag`, `304` and encoding negotiation testable against an in-memory map
  under a plain `cargo test`. The alternative was a module whose only test
  needed a WASM build first — i.e. one with no test that ever ran in CI. The
  handful of tests that need the *real* embed are `#[cfg(feature =
  "embed-assets")]` and run under `just test-embed`, not `just ci`.
- **Paths are refused, never normalised** (`key_for`) — the same rule
  `persist::save_path` follows, and for the same reason. Nothing could escape
  an exact key map, but the check outlives whatever backs `Assets` next.

`.wasm` must stay `application/wasm` (a browser only streams-compiles a bundle
served as that), and `cache-control` is `no-cache` because asset names are
stable across releases — anything longer-lived serves last release's bundle
after a deploy, and the `ETag` is what makes the revalidation a 304 rather than
a download. `br`/`gzip` variants are served if a build produced them
(`--precompress`); no build here does, since it costs three copies of the
bundle in the binary. Request paths are **not** percent-decoded — nothing
cargo-leptos or `assets-dir` emits needs it.

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

### The UI feed is a sample, not a record

The single most important thing to know about `/events`: **a run loop does not
report every pass, and it does not send whole batches.** Both were measured
problems rather than theoretical ones, on a graph of nine pipelines:

- publishing every pass cost **46% of the server's throughput** the moment one
  browser attached — and bought nothing, because the broadcast channel then
  dropped 8.8 million events a second to deliver 341;
- the browser was blocked **63% of the time in freezes approaching a second**,
  and the cost tracked the *event* rate, not the data: 50,000 messages a second
  in 900 fat events was smooth, while 500 messages a second in 9,000 thin ones
  was not.

Three pieces, and they only work together:

- **`pipeline::UiThrottle`** decides **once per pass** whether that pass is
  reported — once per pass rather than per event, because an input event whose
  matching output event was dropped draws a pass that never finished. Failures
  get their own budget, keyed by *stage and component*: one output failing every
  batch is a repeat worth suppressing, but the second of two outputs failing is a
  different fact and one shared timer would swallow it. `report_pass` reads the
  clock only every `CLOCK_CHECK_STRIDE` passes once a pipeline is fast enough for
  that to matter — `Instant::now()` per pass was itself worth a tenth of the
  throughput. All of it is gated on `receiver_count() > 0`, so a headless run
  pays nothing and a browser attaching to a week-old pipeline doesn't get a
  week's skipped messages on its first event.
- **`kayak_core::BatchPreview`** is what a batch crosses the wire as: at most
  `MESSAGES_PER_BATCH` messages, each cut to `MAX_MESSAGE_BYTES`, **rendered on
  the server**. The truncation used to happen in the browser, i.e. after the
  whole batch had been serialized, sent and parsed.
- **`skipped_messages`** is what keeps the readout honest. The feed is sampled,
  so counting only what arrives would report a fraction of the real rate; a
  reported event carries the messages of the passes that were dropped to reach
  it. The tail of the final unreported window is *not* carried — that is
  deliberate, since the alternative is a synthetic batch event at every shutdown,
  and the readout is a ten-second average that a bounded 100 ms tail can't move.

**How the gate is read is as load-bearing as the gate itself.**
`events::Watchers` is an `AtomicUsize` shared by `AppState` and every run loop,
and the per-pass question is a relaxed load on it. It is *not*
`broadcast::Sender::receiver_count()`, which answers the same question by taking
`shared.tail.lock()` — one mutex on the one channel every pipeline shares, which
capped the whole process at ~6.5M passes/sec however many cores it had. Don't go
back to asking the channel; `a_run_loop_asks_the_watcher_count_and_not_the_channel`
in `tests/pipeline.rs` fails if you do.

Three rules around it. `AppState::subscribe_events` is the **only** place a
receiver is handed out, which is what makes the count trustworthy enough to read
instead of the channel — it returns a `(WatchGuard, Receiver)` pair and the guard
must outlive the receiver's use (the SSE handler carries it *inside* the stream's
closure, since `BroadcastStream` takes the receiver by value). The guard is
incremented **before** the receiver exists, because a receiver that isn't yet
counted is a window where the run loops think nobody is watching. And the default
where one isn't threaded through — `BuildCtx`, `PipelineRuntime::from_parts` — is
`Watchers::attached()` rather than empty, because of which mistake it makes: a
component that assumes nobody is watching goes silently dark (a correctness bug),
one that assumes somebody is pays for reporting it didn't need (a performance
one). `publish()` in `src/events.rs` still asks the channel, deliberately: it only
runs on error paths and on passes the throttle already let through, so it is a few
times a second rather than millions.

Consequence worth knowing before you "fix" it: a pipeline faster than
`UI_PASS_INTERVAL` (10 passes a second) **does** lose log lines. The `seq` gaps
are what say so, and the frontend already draws them as passes not shown.
`tests/pipeline.rs::throttling_the_ui_feed` pins both directions — a burst is
sampled, a pipeline inside its budget is reported in full with no gaps. Tests
that are about what the feed *says* rather than how often opt out with
`PipelineRuntime::reporting_every_pass()`, which production never calls.

### History: what a card shows about last night

`/events` answers "what is happening", and it is deliberately bad at answering
"what happened": it is gated on `receiver_count() > 0`, it drops passes under
load, and it keeps nothing. So a pipeline that broke at 02:14 has nothing to
show at 08:00 — nobody was subscribed, so there was no feed. History is the
second, much cheaper path that answers that question, and the **one rule that
must not be broken is that it does not ride on the event feed**: a persistent
subscriber would hold that gate open forever and make every headless server pay
the browser-attached cost of a UI nobody has opened.

Three pieces, split the way `state` is — declaration in
`kayak-core/src/history.rs`, live store in `src/history.rs`, knob in
`server_config`:

- **`Counters`** on `Pipeline` — three `AtomicU64`s the run loop `fetch_add`s
  **unconditionally**, outside the `watching` gate. That is the whole cost on
  the hot path, and it is what makes the record complete rather than sampled.
  Errors are counted here on *every* failure, including the ones the UI throttle
  suppresses, which is why a bucket's error count is the true one.
- **`History::sample`** differences the counters on a tick (`sampler`, every
  `FINE_BUCKET_SECS`) and folds the delta into two rings. Cost is O(pipelines)
  per five seconds and does not scale with throughput — that is the property the
  counters buy.
- **`History::record_error`** keeps one `ErrorSignature` per distinct
  (stage, component, message), with `first_seen`/`last_seen`/`count`. Called
  from the run loop's error arms, which are already behind
  `UiThrottle::report_error`, so it takes its lock a few times a second however
  fast a pipeline is failing. `report_error` now returns `Some(count)` rather
  than a bool: the tally has to include the repeats the throttle swallowed, or
  it would say "failed 4 times" about a pipeline that failed four thousand.

Five properties are load-bearing:

- **Two resolutions, and the capacity of both is *derived from a duration*.**
  `history.retention_secs` is the only knob (a day by default, `0` is the off
  switch — no `enabled` bool beside a duration, for the reason `AuthConfig` is
  an enum rather than a bool beside a map). Fine is 5s buckets over half an hour
  and is **not** configurable: it is sized by what a card can display, which is
  not a deployment's business. A day costs about 58 kB per pipeline, flat in
  throughput, because a bucket holds counts and never messages.
- **Buckets are stored dense.** A gap and a run of zeroes mean different things
  — "the server wasn't asked" against "the pipeline stopped" — and the second is
  the whole point, so it is written down rather than inferred from a missing key
  at render time. `Ring::advance_to` caps its gap fill at the capacity, so a
  server asleep for a week doesn't walk a week of empties.
- **The error map is bounded, and this is the bound that is easy to miss.**
  Failures look self-limiting — "however many things are broken" — but an error
  text carrying a message id or an offset makes every failure distinct, so it is
  an unbounded map fed at the failure rate. `MAX_ERROR_SIGNATURES` with
  stalest-first eviction, and `dropped_signatures` says when it bit.
- **Records outlive their pipelines.** A revert rebuilds every pipeline in the
  graph, so dropping history there would make an edit to one card cost the
  overnight record of all of them — the same argument `Buckets::rebuilt` makes
  about state, one step further. `sample` prunes a record once it is both dead
  and past the retention, and a deleted pipeline's ring is **not** advanced:
  filling it with zeroes would push what it did off the tail long before the
  retention was up.
- **In memory, and that is a decision rather than a stage** — the same one
  `src/buckets.rs` makes. A durable backend is a later swap behind
  `History::get`, and is the point at which a real metrics store is the honest
  answer instead.

`GET /api/pipelines/{id}/history?resolution=fine|coarse` serves it. Two
deliberate differences from the other pipeline endpoints: an unknown id is an
empty history and a **200, not a 404** (history outlives its pipeline, so "no
such pipeline" is not this endpoint's question), and an unreadable `resolution`
falls back to the default rather than 400ing — it picks between two views of one
record, and a chart that refuses to draw over a typo is the worse outcome. This
is also the API's first query parameter, which is why `ApiDoc` grew a `query`
list; they are always optional, so a bare request to any documented path is a
working request.

On the frontend, `Stats::backfill` seeds the chart so an opened card starts full
instead of drawing itself over the next two minutes. Two rules: it refuses a
chart that is **already counting** (the server's counters and the browser's
events count the same messages, so merging would double them, and finding the
overlap means trusting two clocks), and it refuses history **coarser than the
unit** — buckets sum upwards but cannot be cut downwards, the same rule
`set_unit` follows. `Unit::resolution` is what picks the ring. `FailureHistory`
renders the signatures under the chart and is **silent when empty**: history can
be turned off, and a reassuring "no failures" that looks identical either way
would be a lie in the direction that matters.

### How the feed reaches the cards (`Feed`, `frontend/src/app.rs`)

The browser half of the same problem, and the backstop for what the server can't
bound (many pipelines, each inside its own budget). Two rules:

- **The feed writes to the cards; cards do not watch the feed.** Every card used
  to hold an `Effect` on one global `Signal<Option<UiEvent>>` and wake on every
  event to compare one string, so an event cost O(cards). `Feed` is one
  dispatcher with a map, and cards `register` for the life of the component.
  Registrations carry a **token** and cleanup only removes a matching one — the
  card list is rebuilt wholesale, so a card for an id can be created before the
  old one is cleaned up, exactly as with the http inbox registry.
- **Delivery is once per animation frame, not once per event.** A card's log is a
  200-row keyed `<For>`, and appending one row re-runs the whole reconciliation;
  all of a frame's events for one card land in a *single* signal write. The frame
  loop pauses itself when the queue is empty, so an idle graph costs no frames.
  `Edges` drives its blink off `Feed::frame` for the same reason.

A **paused** log must not notify. `Log::skip` still records the rate and the
error badge, but `update` marks a signal dirty whether or not the value moved, so
notifying regardless made pausing a busy card cost *as much as leaving it
running* — measured worse, in fact, because the work arrived in one 9.9-second
task. So a paused card uses `update_untracked` unless a failure arrived, which is
the only thing it shows that can change. Following the tail is deferred to the
next frame for a related reason: reading `scroll_height` forces a synchronous
layout of a 200-row pane, and doing that inline on every update was worth about a
quarter of the blocked time.

### A card's three sections, and the throughput chart

A card is `config` / `stats` / `logs`, each a `CardSection` whose body is behind
a `<Show>` — genuinely unmounted, not hidden, which is the only version of
collapsing worth the code. Open state is three `RwSignal<bool>` on the card and
goes nowhere else: like `maximized` and unlike `arrangement`, it is a way of
looking at a card, so the layout file never hears about it. Logs start shut.

**A shut section is not fed, and the two shut differently** — `CardSink` carries
the flags and `Feed::deliver` reads them. The log still takes `Log::skip`
*untracked*, so the rate window is right the moment it is opened and nothing
renders. The chart takes nothing **and is emptied on the way down**, because an
unfed chart draws the gap as an idle pipeline; "since you opened it" is the only
honest reading, and it is a promise the clearing effect in `Card` keeps. Don't
"optimise" that into keeping the old bars.

`frontend/src/stats.rs` is the pure half, tested like `log.rs`. Three things
there are load-bearing. A bar counts `BatchPreview::counted()`, not `total` —
the same reason the log's rate does, and a chart of the sampling rate would be
worse than none. `bars(now)` is given the clock rather than reading the newest
bucket, which is what makes the window *roll* when a pipeline stops; it ends at
whichever of the two clocks is further on, since the timestamps are the server's
and `now` is the browser's. And `outbound` sums every output component, so
fan-out shows as more leaving than arriving — deliberate, and the thing that
makes one output dying legible.

**Failures are a third series on a strip of their own, with their own scale**
(`Bucket::errors`, `Stats::error_peak`, `error_path`), never a third bar beside
the other two. Two reasons, and the first is the one that matters: it is not the
same quantity, so on a shared scale three failures beside fifty thousand
messages is a bar a pixel high — and three failures is precisely what the strip
is for. The second is that a third bar in each slot is 90 bars in a card
eighteen cells wide, which is the fan of hairlines `BARS` was cut to 30 to
avoid. Note the live count is a **floor**, not the truth: `UI_ERROR_INTERVAL`
suppresses repeats, so the strip says *when* and the `FailureHistory` tally
underneath (which comes from the counters) says how many. Backfilled buckets
carry the true count, since those come from the counters too.

**The y axis is HTML laid over the plot, and that is forced rather than
chosen**: the `<svg>` is `preserveAspectRatio: none`, so an SVG `<text>` would
be stretched horizontally by whatever width the card happens to be. Bars are
scaled against `axis_ceiling` — the first of 1, 2 or 5 × 10ⁿ that reaches the
peak — rather than against the peak itself, which costs a little height at the
top and is what lets the gridlines land on numbers someone can read. `axis_marks`
gives two of them, the ceiling and its half, and **never one at zero**: the
plot's bottom border already means zero and always does, scale or no scale. The
exact peak is not lost, it moved to the axis label's `title`.

`BARS` is 30 because a card is 18 grid cells wide; 60 turned each pair into a
fan of hairlines, which loses the one comparison the chart is for. The plot is
**two `<path>` elements** in a fixed 100×100 viewBox at `preserveAspectRatio:
none` — a frame is two attribute writes rather than sixty elements reconciled,
which is what lets every card redraw once a second. Keep it that way; a rect per
bar is the version that doesn't scale. The `aria-label` on the `<svg>` is not a
`<title>` child for the reason the edge grips give: `leptos_meta` owns `<title>`.

### Expanding a log row

A row is a batch collapsed to one line; the arrow on its left edge opens it into
a box of every message the feed carried, pretty-printed and coloured, and
**opening one pauses the log** — a row that scrolls away while it is being read
is the thing the box exists to fix. Collapsing leaves it paused: pause is a
state someone asked for, and the bar's button is where it is given back.

Three things are load-bearing:

- **The layout happens in `<Show>`'s children, never in `LogRow`.** `LogRow` is
  built for every visible row on every update, so pretty-printing there would
  put per-message work back on exactly the path the sampled feed exists to keep
  clear. The row holds its `EntryKind` in a `StoredValue` and does nothing with
  it until the box opens.
- **`frontend/src/pretty.rs` re-indents the text; it does not parse to a `Value`
  and print that back.** `serde_json::Map` is a `BTreeMap` here (`preserve_order`
  is off, and turning it on is a workspace-wide feature change that would reach
  the config renderer), so a round trip would sort the keys of every payload on
  screen, and re-serializing would restyle its numbers. A payload is shown to be
  *read*. Scanning also hands the highlighter its token kinds and degrades
  gracefully: a message the feed cut at `MAX_MESSAGE_BYTES` isn't valid JSON, so
  it comes back as `Rendered::Plain` and is shown as it stands. Errors take that
  same arm — a row truncates an error's text and there is nowhere else to read
  it.
- **Which rows are open lives on `MessageLog`, not on the row**, in a set
  *separate* from the open-passes set: the rows are rebuilt wholesale on every
  update and on the flat/grouped switch, and closing a pass over a row someone
  was reading shouldn't throw it away. `clear` empties it, since the ids it
  holds are about entries that are gone.

The frontend has two routes behind `leptos_router` (`frontend/src/app.rs`): `/` is the pannable/zoomable canvas of pipeline "cards" fed by `ApiClient::list_pipelines()` plus the live event signal, and `/docs` is the generated reference — two tabs, components and HTTP API. `Navbar` is shared and reads `AppState` through `use_context` rather than `expect_context`, because only the canvas provides it.

Of the older Askama templates, only `templates/index.html` and the dead `/ui` `index_handler` are left; both are slated for removal, and Askama goes with them.

### The component reference (`/docs`)

Generated, never hand-written. `kayak-core/src/docs.rs` reflects over `schema_for!(InputKind)` etc. and produces `ComponentDoc`s — kind, family, description, fields (name, type, required) and, for enum-shaped configs like `filter`, variants. **The doc comments on the config structs are the docs**, and a component with no doc comment fails a unit test. Two consumers: the Leptos `/docs` page renders it, `GET /api/docs` serves it as JSON.

Nothing in there knows the name of any component — keep it that way. Notes for anyone touching it: walk `oneOf` (which pairs a `type` tag with a config struct), never `$defs` (which also holds shared field types like `Secret`); field order is `required` order then alphabetical; `Option<T>` arrives as `anyOf: [T, null]` when the inner type is a `$ref` and as `"type": ["integer", "null"]` when it isn't — `scalar_type_of` handles the second spelling.

A closed set of values has two spellings too, and both have to be read
(`string_values_of`): a plain unit-variant enum is `enum: ["a", "b"]`, but one
variant with a doc comment on it switches schemars to `oneOf: [{const: "a",
description}, ...]`. They mean the same thing, and recognising only the first is
what once made *documenting* a variant silently downgrade its dropdown to a JSON
box. The `oneOf` walk is the same one the tagged unions use, so the rule is
every branch is a bare string `const` or it is not a closed set.

A `FieldDoc` carries `field_type` (`FieldType`) beside the human-readable `type_name`. That's the same reflection serving a second consumer: the "add pipeline" modal generates its form from it, so a new component gets working controls and validation for free. A field with a shape of its own is described rather than surrendered to a JSON box: `FieldType::Object` carries its fields, `FieldType::Union` carries a tag and the variants it selects between. The union is the **conditional** case — a `buffer` is `{"type": "static", "size": 10}` or `{"type": "tumbling", "window_seconds": 30}`, so which boxes exist depends on an answer given in another box — and it is the field-level twin of `ComponentDoc::variants`, which does the same thing for a component's own shape (`filter`). Only the internally tagged spelling is read; the externally tagged one is a component's shape and is `variants_of`'s job. `FieldType::Json` remains as the fallback for anything neither walk understands, and `no_component_field_needs_raw_json` fails if a component ever lands on it — the point being that a JSON box is a field the user has to hand-write.

The walk is bounded (`MAX_NESTING`) because it follows `$ref`s and a config type that referred to itself would otherwise recurse until the stack ran out.

On the form side that nesting is flat: draft values and error keys are **dotted paths** (`buffer.type`, `buffer.size`, `rotate.max_rows`), which is what lets one `HashMap<String, String>`, one error list and one `FieldEditor` serve any depth. `FieldEditor` renders itself for those, so it returns `AnyView` — a component containing itself can't have a return type defined in terms of its own. The union's tag dropdown is the **one control in the modal that reads its value back**, for the same reason the others don't: rebuilding the fields is exactly what it is for. Its signal holds only the tag, so a keystroke in a nested box can't reach it.

`FieldType::List` is the one field with **no fixed number of boxes** — a
reducer's `aggregations`. It carries the element as a whole `FieldDoc` (name
empty, always required), so nothing renders rows without knowing what they are
rows *of*, and a list whose element the reflection can't render degrades to
`Json` whole rather than to rows of JSON boxes. On the form side a row's
**position is its name**: `aggregations.0.function`, in the same flat map as
everything else. The list's *own* path holds the row **count** (`aggregations` =
`"3"`) rather than a value, which is what lets an empty row exist — counting the
keys that happen to be filled in would make a freshly added row vanish until
something was typed into it. Removing a row therefore has to shift the ones
after it down (`form::remove_list_element`) and drop that list's messages, since
they are about boxes that have moved. Like the union's tag, the row count is a
control that reads its value back, and for the same reason.

Known gap: `/docs` renders one flat table per component, so a list element's or
a nested object's own fields aren't shown there — `aggregations` reads as "list
of aggregation" and the reducer's doc comment carries the shape. `rotate` has
always had the same gap. Fixing it means recursing `FieldTable`, not changing
the reflection, which already carries the whole tree.

`FieldType::Connection(kind)` works the same way and for the same reason, one step further: a `connection` field carries `#[schemars(extend("x-connection" = "kafka"))]`, and the marker holds the *kind* — "any connection" is the wrong set to offer, since a kafka input can only use a kafka connection. `Family::Connection` is a fourth family, so a connection kind documents itself on `/docs` and generates its own form through the same machinery a component does.

`FieldType::PipelineId` is the one field type the schema alone can't derive: a pipeline id is a `String` like any other, so the field says so where it's declared — `#[schemars(extend("x-pipeline-id" = true))]` on `PipelineConfig.upstream` — and `docs.rs` looks for the marker, not for the field's name. The rule about not knowing component names covers their field names too, so any component that grows a reference to another pipeline gets the dropdown by adding that attribute. The options can't come from the schema either: they are the running graph, so `AddPipelineModal` derives them from the pipeline list and passes them down to `FieldEditor`. That control is the only one in the modal that reads its value back — the list can arrive after the modal opened, and a rebuild must re-mark what was already chosen rather than drop it.

### The state tab of `/docs`

`/docs` has a third tab explaining state buckets, and it is **not** a fifth
`Family`: a bucket is not a component — nothing builds one into a pipeline, and
`all_components()` is what the "add pipeline" form offers, so putting it there
would offer a bucket as a thing a pipeline is made of. `docs::state_docs()`
reflects `StateBucketConfig` and `PipelineState` into `StateDoc`s instead — same
`FieldDoc`s, same `FieldTable`, no `Family` — and each carries the **path** it
goes at in the file (`state.<name>`, `pipelines[].state`) in place of the
`"type"` tag a component's header shows.

The prose around them is written rather than reflected, and that is the split:
the fields are generated so a bound that grows a field can't leave the page
behind, while *why buckets are global* and *what sharing one costs* are things a
schema cannot say. `frontend/src/docs.rs::state_sections` is the pure half — it
interleaves the written sections with the generated ones so the sidebar is one
list in reading order, and `STATE_OVERVIEW`/`STATE_TRANSFORMS`/`STATE_INSPECTING`
are constants because the page renders the anchors and the sidebar links to
them. No search box on this tab: four sections is not a list to filter.

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

A card also carries a `.card-spawn` handle — "add a pipeline fed by this one",
which opens `AddPipelineModal` with its one input already pointed here. It sits
on the **bottom** edge because that is the face the new card's edge will be
routed out of, and to the **left** because the log's "jump to latest" already
owns the middle of that edge. It *is* its own hover zone: a transparent strip to
detect the pointer nearing the edge would have to swallow clicks meant for the
log underneath, so the button is always present, always that small, and only its
opacity changes. The seed rides on `AppState.add_upstream`, written only through
`open_add` — the modal is mounted under a `<Show>`, so it reads the seed once at
construction, and routing every open through one method is what stops a stale id
reaching the sidebar's plain `+`. `form::draft_fed_by` builds the draft by
looking for an input with a `FieldType::PipelineId` field rather than by naming
the `pipeline` input, the same rule `docs.rs` follows: any input that grows a
reference to another pipeline becomes the one this seeds, with no edit here.

The same applies to the edge handles: `ChannelGrip` and `PortGrip` are each two `<line>`s, a fat transparent one that catches the pointer (`.edges` sets `pointer-events: none`, so the hit line turns it back on for itself) and a visible grip. Note the label is an `aria-label` and not an SVG `<title>` child — `leptos_meta` claims `<title>` for the document's, and the browser tab ends up named after whichever edge rendered last. Their `.vertical` classes mean *opposite* things (a channel's is the route's direction, a port's is the face's), which is why the cursor rules are per-class rather than shared.

Drags are tracked with window-level listeners rather than on the card (a fast pointer leaves the card behind, and a `mouseup` outside it would never arrive). The delta is divided by the zoom, applied to the geometry captured at press time rather than accumulated, and written into `arrangement` live so the edges follow; the `PUT` happens once, on release. It's a browser-tab property — the API accepts writes either way, which is fine for a dev tool but shouldn't be mistaken for enforcement. Edits apply to the runtime immediately, so `revert` (reload the file) is the only undo, and `unsaved changes` in the navbar is the only thing between a session's work and a restart.

### Selecting cards

`CanvasState.selected` is a **set** (`frontend/src/selection.rs`), not an
`Option<PipelineId>`, because arranging a graph happens in handfuls: dragging
twenty cards into place one at a time is what this exists to stop. `Selection`
is the pure half — `only`/`toggle`/`add_all`/`covers`, plus `descendants`, which
walks the same `(id, upstreams)` pairs `pipelines_from` produces so "select
children" and the canvas agree about what feeds what. Unit-tested there;
`app.rs` holds it in a signal and does the clicking.

Growing a selection is **edit mode only**. Read-only names one pipeline to look
at it, and a set there would be a state with nothing to do — so `shift_key` is
always read together with `state.editing()`.

Four rules on the click side, and the middle one is what makes a group draggable
at all:

- **Shift toggles**, on a card and on a sidebar row alike. A shift-click on a
  row moves no camera: it is about building a set, and gliding to whichever row
  was clicked last would fight that.
- **A press on a card that is already selected leaves the selection alone**, so
  a group can be grabbed by any of its members. Collapsing to the card pressed —
  which is what a plain click does to an *unselected* card — would make it
  impossible to drag the rest. The cost is that a plain click never *reduces* a
  selection; empty canvas is the way out.
- **A shift-press that deselected a card does not then drag it.** `grab` returns
  when the card it just took out of the selection isn't in it any more,
  otherwise the gesture would undo itself in one movement.
- **Clicking empty canvas clears.** The only gesture that scales past a couple
  of cards. The edge handles stop propagation before the canvas hears them, so
  dragging a line never counts as a click into nothing.

`Dragging` therefore carries a `Vec<DragCard>` rather than one id and one
geometry, and `graph::dragged_all` applies the one delta to all of them — the
cards keep their positions relative to each other and only the group travels,
while each still snaps to the grid on its own. A **resize is always one card**:
there is no reading of resizing six of them from one corner, so only the move
arm builds the list.

**Text selection is off by default and opted back into.** `body` sets
`user-select: none`, and a short list in `main.scss` — the card's `.property`
rows, `.log-body`, the connection and state cards, `.docs-content`, `.modal`,
`input`/`textarea` — turns it back on. The canvas is a set of controls that are
almost all dragged, every one of those gestures sweeps a pointer across labels,
and shift-click is the browser's "extend the selection to here" as well as ours;
the result was a blue smear that was never what anyone meant. The test for
adding to the opt-in list is "would someone reasonably want this in their
clipboard" — a button, a sidebar row and a card's title bar are all no.

Two consequences worth knowing. `.pipelines.panning` has to name its
*descendants* (`&, *`) because an explicit `text` on a descendant beats an
inherited `none`, so a pan sweeping across a card would otherwise highlight
everything it passed over. And `press_selects` calls `prevent_default` on a
shift-press: inside a card the config and log are deliberately selectable, so
without it the shift-click that adds a card to the selection also paints half of
it blue.

The row's `⋯` menu (`RowMenu`) is positioned like `ConnectionCard` and for the
same reason — the sidebar scrolls, so it is `position: fixed` at the button's
viewport position and closes on a scroll above it. It isn't drawn on a `repeat`
row, for the reason the delete isn't: the open menu is keyed by id, and two rows
for one pipeline would open together. "select children" **adds** to the
selection rather than replacing it, which is what makes two branches of a
fan-out reachable in two clicks, and it moves no camera.

The sidebar has three tabs (`SidebarTab`): pipelines and connections, each with its own `+` and armed delete, and **state**, which has neither — buckets are part of the graph's logic and live in the config file, so that tab is a window rather than an editor. It polls `GET /api/state` once a second *while mounted* (a bucket changes per message, so pushing it would be the `/events` firehose again for a readout nobody watches per-message) and a click opens a card pinned to the row's viewport y, the same trick `ConnectionList` uses.

**A trap worth knowing before adding anything else that polls**: the navbar, the sidebar and the whole canvas are inside *one* `<Suspense>` in `Canvas`. A `LocalResource` read anywhere under it re-suspends that boundary on every refetch, so a resource polled once a second tears the canvas down and rebuilds it once a second — which is exactly what the first version of this tab did. The state tab therefore reads through an `Effect` + `spawn_local` into a plain `RwSignal` instead. The existing `pipelines`/`connections` resources are fine because they only refetch on an explicit `reload`; anything on a timer needs this treatment or a suspense boundary of its own.

The `+` in the pipelines tab opens `AddPipelineModal` (`frontend/src/app.rs`), whose pure half is `frontend/src/form.rs` — drafts in, `POST /api/pipelines` body or a list of `FormError`s out, unit tested like `graph.rs`/`inspector.rs`/`docs.rs`. One non-obvious constraint shapes the component: the field boxes are **uncontrolled** (`value=` once, `on:input` writes, never reads back), because the field list is rebuilt when the kind or variant changes and a rebuild on every keystroke would destroy the `<input>` being typed into. `DraftSignals` exists for the same reason — per-part signals so typing doesn't invalidate the list.

The pipelines tab has two arrangements and a search box, and which rows that comes to is `frontend/src/sidebar.rs` — pure, unit-tested, fed by `graph::pipelines_from` so the sidebar and the canvas derive from one description of the graph. `Flat` sorts by id *here* rather than trusting the server, which walks a `HashMap`. `Tree` has to answer the DAG: a pipeline with several upstreams is listed under each, in full under the **deepest** parent (ties by id) — the one the canvas draws the card below — and as a `repeat` row under the rest. A repeat doesn't recurse (it would draw a subtree twice, and would not terminate in a cycle) and gets no delete, since the armed state is keyed by id and two `×`s arming together read as two pipelines. Anything the walk can't reach — a cycle — is appended as a root rather than dropped. Search keeps the *ancestors* of a match and not its descendants; the rows are pre-order, which is what makes that a single backwards pass. The list is rebuilt wholesale rather than `<For>`-keyed because an id isn't unique in tree mode, and the search `<input>` sits outside that closure for the same reason the modal's fields are uncontrolled. The mode lives in `AppState` (the tab strip unmounts the list) and the query doesn't (a filter is transient).

`frontend/src/docs.rs` holds the page's pure logic (search filtering, grouping, anchors, doc-comment rendering) with unit tests, same convention as `graph.rs`/`inspector.rs`. One trap worth remembering: the docs lists are rebuilt with plain closures rather than `<For>`, because keying groups by family leaves stale components on screen when a filter changes a group's contents without changing its key.

### The throughput harness (`kayak-bench`)

`just bench` answers "is this slower than it was?" and "how much can one server
take?" with numbers rather than memories. Guide section: "benchmarking".

It drives the runtime **in process** — no socket, no broker, no filesystem —
through the same seams the integration tests use (`PipelineRuntime::from_parts`,
`BuildCtx`), fed by `testing::LoadInput` and discarding through
`testing::NullOutput`. Measurement is `Pipeline::counters` and nothing else:
three relaxed atomics the run loop adds to unconditionally, outside the feed's
`receiver_count()` gate, so differencing them across a window is a complete
count and the act of measuring changes nothing. That property is why no
instrumentation had to be added to the runtime for this to exist — keep it.

Five things are load-bearing:

- **`LoadInput` spends cooperative budget rather than yielding**
  (`tokio::task::consume_budget`, not `yield_now`). A `next()` that returned
  `Ready` without ever awaiting would never hand a worker back — tokio's budget
  is spent by *resources*, and a loop touching none is invisible to it — but
  `yield_now` reschedules on every call, and with one task on the runtime that
  round trip costs more than the entire run loop. The first sweep built on it
  reported one pipeline as slower than each of ten and adding a filter as a 3×
  speed-up. Both halves of that lesson are in the type's docs; don't undo
  either.
- **`LoadInput` is deliberately not an `InputKind`.** Nothing in the config
  surface can generate load, and an input whose purpose is to saturate a core
  does not belong in a file people commit. (`dummy` can't stand in — its
  `duration` is whole seconds. See the roadmap.)
- **The scenario suite is a fixed list, and its names are the baseline's keys.**
  Adding one is free (a baseline reads it as `new`); *changing* one silently
  breaks comparability, so rename at the same time. `LoadInput`'s generated
  message is part of that contract too — widening it invalidates every recorded
  number.
- **Absolutes are per machine, ratios travel.** Baselines are filed under a
  hardware-derived id in `bench/baselines/`, committed, and carry a manifest
  (commit + `-dirty`, rustc, profile, cpu, cores, os). `--save` refuses a debug
  build and a filtered run, both because the file would be compared against
  wrongly later. `--compare` is print-only on purpose: a threshold needs
  measured run-to-run noise, not a guess.
- **A row with no transforms measures passes, not messages.** With an empty
  chain and a discarding output nothing ever touches an individual message — the
  batch is an `Arc` cloned rather than walked — so `batch1 → batch1000` is a
  clean factor of ten each step and the `msgs/s` on `batch1000` is the batch
  size read back out. That's what the `passes/s` column is for.

**What the first sweep found** — the thing that justified building it: the run
loop asked `self.events.receiver_count() > 0` once per pass, and tokio
implements that as `self.shared.tail.lock()` — one mutex on one shared channel,
taken by every pipeline on every pass, capping the whole process at ~6.5M
passes/sec regardless of cores or pipeline count. Fixed the same day (see
`events::Watchers` under "the ui feed is a sample"); the sweep measured +189% /
+592% / +804% at ten / a hundred / a thousand pipelines. The `pipelines*` rows
are what keep that honest, and the two baselines either side of it are in
`bench/baselines/`' git history.

### The doc site (`website/`, `docsgen/`)

A VitePress site whose **prose is written and whose every reference table is
generated**, which is the whole design: `docs/guide.md` was split into pages
under `website/` (canvas / pipelines / io / operating / contributing) and is now
a pointer, and nothing in those pages restates a field, a type or a status code.

`kayak_docsgen::files()` is pure — it returns what should be on disk — and
`main.rs` writes it. What it emits is **markdown partials**, not pages: no
title, no front matter, pulled into a prose page with VitePress'
`<!--@include: -->`. So `website/reference/inputs.md` is a page of prose about
what an input *is* that ends by including the generated tables for every input
there is. Per-component partials are emitted as well as the per-family ones, so
a page can interleave prose between two components at the cost of naming them.

It knows the name of no component and no endpoint, exactly as `kayak-core/src/
docs.rs` doesn't. A component added to the config enums gets a partial, a place
in its family's page and a sidebar entry from one `just docs` run — the sidebar
too, which is why `.vitepress/generated/sidebar.json` exists and is imported by
`config.mts` rather than being a list someone maintains.

Four things are load-bearing:

- **The generated files are committed**, so the site builds with no Rust
  toolchain (the CI job is `npm ci && npm run build` and nothing else). What
  keeps them honest is `docsgen/tests/site.rs`, which regenerates in memory and
  compares — a stale reference is a red `just ci` with `just docs` as the fix.
- **Vue compiles these pages**, so a bare `<name>` in a doc comment or an anchor
  is a *build error*, not a stray angle bracket. That's what `slug()` is for,
  and why the site build is its own CI job: the failure is invisible from Rust.
- **The requirement column is a `<Badge>` rather than a word** because styling
  it by position would reach the description column of every three-column table
  on the site. Same reason the description is styled as `td:last-child`: it is
  second in a metadata table and third in a response table.
- **Nesting is written out here and not in the app.** `/docs` renders one flat
  table per component and leaves a `buffer`'s or a `rotate`'s own fields to the
  doc comment; there is room on a page, so `nested()` recurses the `FieldType`
  tree the reflection already carries.

The look comes from `landing/visual-language.md` (`.vitepress/theme/kayak.css`)
and the site is `force-dark` for the reason the product is.

## Notes

- `docs/roadmap.md` holds the current TODO list — check it for what's in flight before proposing work.
- Leptos config lives in the root `Cargo.toml` under `[[workspace.metadata.leptos]]`; `site-addr` there (6767) is what the binary binds unless `LEPTOS_SITE_ADDR` or `--listen` says otherwise. `src/listen.rs` holds that precedence rule (`--listen` > env > `Cargo.toml`) and the loopback warning, both pure and tested — the flag is an `Option<SocketAddr>` and **must stay one**, because a clap default would win over the env var and break `cargo leptos watch` and the container image alike.
- `Dockerfile` is a two-stage cargo-leptos build, documented in `website/operating/deployment.md`. The runtime image is the *runtime and nothing else*: binary, site directory, `LEPTOS_SITE_*` env vars, uid 10001, `ENTRYPOINT` = the binary so container args are server flags. **No config is baked in** — bare it serves an empty graph, and a deployment mounts one into `/kayak` (the WORKDIR, owned by the run user because saving writes there). The sample is carried at `/usr/share/kayak/example` for a tour, connections and layout file beside it under the same stem or they stop being found. The builder installs `cmake` for `rdkafka-sys`; nothing else, since TLS is rustls and zlib is vendored.
- **`example_config/` is the sample everything is tried against**, and it is one directory because the set travels together: the connections and layout files are *derived* from the config's path, so they only find each other side by side. `tests/config.rs` and `tests/graph.rs` read the files from there by relative path, so moving or renaming them breaks those tests — which is the point, the sample is not allowed to rot. `secrets.json` is gitignored anywhere in the tree; `just dev` creates the sample's from `secrets.example.json`.
