# roadmap

What's in flight, what's planned, and what's known to be broken — kept here so
the next piece of work is a list to check rather than a conversation to
re-derive. See the [docs site](../website/) for how the finished parts behave, and
[`CLAUDE.md`](../CLAUDE.md) for how they're implemented.

## currently working on

- [x] expose a standardised http api specification
      (done 2026-08-07: OpenAPI 3.1 at `/api/openapi.json`, rendered at
      `/api/reference`, plus an "http api" tab on `/docs` — all three off the
      one table `api_router` is built from. See "the http api reference" above.)
- [x] let systems push data in over http
      (done 2026-08-08: the `http` input, serving
      `POST /api/pipelines/{id}/messages` off the pipeline's own id. See
      "posting into a pipeline" above.)
- [ ] add filter transform
- [x] add some kind of component plugin registry which can be used to generate docs
      (done 2026-08-04: no registry in the end — `/docs` reflects over the config
      schemas instead, so a component documents itself through its doc comments.
      See "the component reference" above.)

## todo

- [x] basic authentication
      (done 2026-08-11: a `--server-config` file with an `auth` section, two
      roles, HTTP Basic for machines and a session cookie for the browser. Off
      without the flag. The required role lives in the `api_docs` table beside
      everything else about an endpoint, so the reference, the spec and the
      middleware are one fact. See "authentication" above.)
- [x] protect the http input
      (done 2026-08-11: an optional `auth` on the `http` input — a bearer token,
      or a fixed value in a header of your choosing — checked by the inbox
      registry, so the credential lives and dies with the endpoint it guards.
      Per pipeline, `${NAME}` out of the secret store, constant-time, and
      refused at build time if it names a header the `envelope` copies. Absent
      by default. See "protecting the endpoint" above.)
- [ ] **an hmac option for the http input's `auth`.** The bearer token is a
      shared secret that travels on every request, so it is only as private as
      the transport. A GitHub-style signature over the body
      (`x-hub-signature-256`) keeps the secret off the wire entirely and is what
      most webhook senders already speak. It slots in as a third
      `HttpAuthConfig` variant — the registry check is already the right shape —
      but it needs the raw body, which the handler currently hands straight to
      the JSON extractor, so it is its own change.
- [ ] **nothing rate-limits the ingest endpoint.** A wrong token costs an
      attacker a round trip, same as a wrong password, so a short token on a
      public network is guessable. The `auth` check is constant-time, which is a
      different problem.
- [ ] **hashed passwords in the settings file.** Passwords resolve from the
      secret store today, which keeps the file committable but means the value
      lives in the environment. Argon2id hashes would let the file stand alone —
      but they need a `kayak hash-password` subcommand to be usable at all, so
      it is its own change rather than a tweak to the config type.
- [ ] **sessions do not survive a restart.** They are a `HashMap` in the
      process, which is what makes logout genuinely revoke; the cost is that a
      deploy signs everyone out. Fixing it properly means a signing key with
      somewhere to live and a rotation story, which is a bigger decision than it
      looks.
- [ ] **nothing rate-limits a failed login.** An account is only as good as its
      password. A per-address backoff on `POST /api/auth/login` is the cheap
      version; it needs somewhere to keep the counters that isn't a memory leak.

- [x] **keep logs and stats so a card can show what happened overnight**
      (done 2026-08-12, in memory: unconditional counters in the run loop,
      sampled on a five-second tick into two ring buffers, plus failures
      aggregated to one signature per distinct message. `history.retention_secs`
      in the server config is the only knob and a day is the default; the buffer
      sizes are derived from it. Deliberately *not* fed from `/events` — a
      persistent subscriber would hold the `receiver_count()` gate open and make
      every headless server pay the browser-attached cost. See "history" in
      CLAUDE.md.)
- [ ] **history does not survive a restart.** It is a ring buffer in the
      process, which covers the case it was built for — the pipeline died, the
      server didn't — but a deploy loses the record. SQLite behind
      `History::get` is the shape of the fix and the trait seam is already
      there; it costs a dependency, a schema-migration story for a file that
      outlives an upgrade, a third directory boundary after `save_dir` and
      `--data-dir`, and a degrade-to-nothing path when the disk is full. Worth
      doing once the in-memory shape has proven itself against real usage.
- [ ] **no message payloads are kept, on purpose.** History carries counts and
      failure texts and nothing else: payloads re-couple storage to throughput,
      and a day of message bodies in a file beside the server is the argument
      `inputs::http::ALLOWED_HEADERS` already makes about credentials outliving
      the request by years. If it happens it is opt-in per pipeline, with its
      own much shorter retention, and it is a data-retention decision rather
      than a UI toggle.
- [ ] **the diagnostic stream as an input kind.** A fleet wants central
      observability, not a ring buffer per host. Exposing what the run loop
      counts as something a pipeline can consume would let operators route it
      into their own postgres or object store with the machinery that already
      exists — and it is the honest answer at the point where someone asks for a
      week of retention.
- [x] **a throughput baseline that can be taken again in six months**
      (done 2026-08-13: `kayak-bench`, a workspace crate driving the run loop
      in process through `PipelineRuntime::from_parts` and measuring with the
      counters the run loop already keeps. Twelve scenarios sweeping batch
      size, transform chain, pipeline count, graph depth and whether a browser
      is attached; per-machine baselines committed under `bench/baselines/`;
      ratios reported separately from absolutes because only the ratios survive
      leaving the machine. `just bench`, deliberately not part of `just ci`.
      See "benchmarking" in the guide.)
- [x] **`receiver_count()` on the shared event channel serialised every
      pipeline in the process**
      (found 2026-08-13 by the first real sweep — the reason the harness was
      worth building — and fixed the same day. The run loop asked
      `self.events.receiver_count() > 0` once per pass to decide whether to
      report, and tokio implements that as `self.shared.tail.lock()`: one
      mutex, on one channel, taken by every pipeline on every pass. The whole
      server was capped at ~6.5M passes a second however many cores or
      pipelines it had. The gate itself was right — see "the ui feed is a
      sample" in `CLAUDE.md` — only the *reading* of it was expensive, so the
      count moved to an `AtomicUsize` (`events::Watchers`) that
      `AppState::subscribe_events` maintains with a guard, turning a lock into
      a relaxed load. Measured on an M1 Max: **+189% at ten pipelines, +592% at
      a hundred, +804% at a thousand**, and total throughput now *rises* with
      pipeline count instead of flatlining. `bench/baselines/` holds both
      numbers in its git history.)
- [ ] **the http ingest path has no load test.** `kayak-bench` measures the run
      loop and stops at the axum layer on purpose. What is untested under load
      is the whole request path — the JSON extractor, the inbox `try_send`, per
      request overhead — and its most interesting number, the rate at which
      `Backpressure` starts turning 202s into 503s. It needs an external driver
      (`oha`/`vegeta`/`k6`) against a real binary rather than an in-process
      harness, with server-side truth read back off
      `GET /api/pipelines/{id}/history`. See "benchmarking" in the guide.
- [ ] **the dummy input cannot go faster than one message a second.**
      `DummyConfig.duration` is a whole number of seconds, which makes the one
      input needing no broker useless for trying anything under load by hand.
      An optional `interval_ms` winning over `duration` when present would be
      wire-compatible. (The bench does not need it — `testing::LoadInput` is
      deliberately not a config kind, since an input whose purpose is to
      saturate a core does not belong in a file people commit.)
- [ ] make sure to clean up old template based UI stuff
      (2026-08-04: `/docs` and `templates/docs.html` are gone — Askama is now
      only used by the dead `/ui` index handler, which is all that's left)
- [x] map message fields onto real database columns, with types
      (done 2026-08-10: `columns` on the postgres output, plus `create_table`,
      `primary_key` and `indexes`. The mapping and its logical types live in
      `kayak-core/src/columns.rs` so the next database output reuses them whole.
      See "database outputs and column mapping" above.)
- [x] a transform that reshapes a message
      (done 2026-08-10: `map` — copy, constant, coalesce, cast, concat,
      arithmetic and drop over an ordered list of mappings, with `keep` and
      `on_missing`. Declared in `kayak-core/src/mapping.rs`, evaluated in
      `src/transforms/map.rs`, and it is what gave `fields` a write side. See
      "reshaping messages" above.)
- [x] **a scripted transform (rhai).**
      (done 2026-08-14: `script`, with `message` and `batch` scope, `emit()`,
      state access, inline or file-sourced. The case it was revisited on is the
      one this entry predicted plus a fourth the entry missed — **arrays inside
      a message**, which nothing declarative reaches at all and which turned out
      to be the strongest argument. See "scripting" in the guide.
      Two notes from the entry survived and two were adjusted. The op budget is
      indeed load-bearing and is enforced, along with separate size caps — the
      budget counts operations and one operation can allocate. `rhai::serde` is
      indeed avoided, but with a hand-written bidirectional walk rather than a
      copy-on-write `Dynamic`: a rooted-path CoW type costs the ergonomics of
      native rhai values (`for line in msg.lines` stops working without
      registering an iterator, and every operator needs re-registering), which
      is most of why a scripting language was worth having. Whether the walk is
      fast enough is a `just bench` question and has not been measured yet — see
      the entry below.)
- [ ] measure the `script` transform in `kayak-bench`. Nothing in the scenario
      suite touches it, so the cost of the `Value` ↔ `Dynamic` walk against the
      cost of the interpreter is currently an argument rather than a number.
      Worth a row with an empty script (the walk alone) and one with a
      field-touching script, at a couple of batch sizes. If the walk dominates,
      the copy-on-write type the original entry described is the fix, and the
      ergonomic cost of it is the thing to weigh. Note the scenario names are
      the baseline's keys — adding rows is free, renaming existing ones is not.
- [x] **an opcua input.** (done 2026-08-14: a subscription with a monitored item
      per node, one message per value change, nodes named or found by browsing.
      See "the opcua input" in `CLAUDE.md` and
      [the page](../website/io/opcua-input.md).)
- [ ] **an opcua output.** Writing values back to a server: a `write` per
      message, mapping fields onto nodes the way the postgres output maps them
      onto columns. The connection and the value conversion are already here;
      what it needs is the mapping (a `ColumnPlan`-shaped question, one node per
      field) and a decision about what a rejected write does to the batch.
- [ ] **polling on the opcua input.** The subscription is the right default and
      is what a plant server is built for, but a `read` of a fixed node list on
      a timer is worth having for the servers that throttle subscriptions, and
      for the case where "the value every ten seconds whether or not it moved"
      is what a downstream store wants. It is a `mode: subscribe | poll` on the
      same component: the node list, the value conversion and the message shape
      are all shared, and only the reading half changes.
- [ ] **signed and encrypted opcua sessions.** Today every session is
      `SecurityPolicy::None` and a plaintext one — which is honest but is a
      network you have to trust. `Basic256Sha256` with `Sign` or `SignAndEncrypt`
      needs a client certificate, somewhere for it to live (the same question
      the mqtt connection's missing TLS raises: a `Secret`? a path resolved
      against `--data-dir`?), and a server trust list. It also has a cheap first
      step worth taking on its own: an application instance certificate would
      silence the two ERROR lines the client logs on every connect (see `QUIET`
      in `main.rs`).
- [ ] add time based buffer for the transform buffer
- [ ] make outputs optional (for example, when a parent pipeline is only used to push data to children)
- [x] think about necessary metadata to add to each message
      (done 2026-08-08: `envelope` on any input, attached in band. See "message
      metadata" above — and note the field paths that came with it, which make
      nested payloads reachable for the first time.)
- [x] deal with all unwraps -- this will bite us in the ass soon otherwise
      (done 2026-08-03: no unwrap/expect left in src/; see "known issues" below
      for the things that pass turned up but didn't change)
- [x] show config in the "cards" in the web ui
      (done 2026-08-04: tabbed property list, see "the canvas" above)
- [x] select several cards and drag them together
      (done 2026-08-12: shift-click a card or a sidebar row adds it to the
      selection, dragging any member moves the whole set by one delta, and a
      row's `⋯` menu selects a pipeline with everything downstream of it. Empty
      canvas clears. Edit mode only. See "selecting cards" in CLAUDE.md.)
- [x] give pipeline ability to have multiple inputs
      (done 2026-08-04: and multiple outputs. `inputs` and `outputs` are arrays
      in the config now — a breaking wire-format change, the singular `input`
      and `output` keys are gone. See "pipelines" below.)
- [ ] new transform (i guess?): wait_for_condition (should it be called buffer_until_condition? or perhaps both are needed?)
      for example, we need to wait for x: a and z: b. for this, we also need the multiple input thing
      (2026-08-09: the state half of this landed — named buckets plus `remember`
      and `recall`, see "state" above. What is left is the *session window*, now
      tracked with the rest of the machine-cycle work under "the machine-cycle
      scenario" below.)

## ecosystem positioning

Came out of comparing kayak against the closest existing tools (Benthos/Bento,
Redpanda Connect, NiFi, Arroyo, Conduit, StreamPipes, Vector, Fluvio) — see the
readme's "why" for the short version. The ideas hold up; the gaps below are
what stand between "well-built prototype" and "something worth someone else
picking up."

- [ ] **no durability or scaling story.** State lives in one process' memory
      (see "state" in the guide) and there is no checkpointing of input
      positions, so kayak is honestly a single-node tool today. That's a
      legitimate niche — Benthos proved "stream processing without Flink's
      operational weight" is a real market — but it needs to be a *stated*
      scope rather than a gap people discover by hitting it. Either write that
      boundary down explicitly (readme + guide), or decide it's worth chasing
      and scope what a distributed mode would need, starting at the input as
      the durability argument under "state" already says.
- [ ] **the connector list is thin.** nats, kafka, mqtt, redis, http and two
      dummies in; nats, kafka, mqtt, redis, http, postgres, clickhouse, file,
      s3 and stdout out — against
      Benthos/Redpanda Connect's 300+. The five-touchpoint recipe for adding a
      component (config enum, `build()` arm, impl module, wire-format sample,
      doc comment) is cheap by design, but "cheap to add" isn't "already
      there" for someone evaluating whether kayak fits their stack today.
      Candidates worth prioritising next, roughly in order of how common they
      are: AMQP 0-9-1 (RabbitMQ — a **separate** connection kind from AMQP
      1.0, which is a different protocol and client library and not yet worth
      building against without a concrete target like Azure Service Bus or
      Artemis) and mysql — which, like clickhouse, is a `ColumnPlan` plus a
      DDL renderer and nothing else. (Redis and a generic HTTP/webhook output
      were the two ahead of them and are both done.)
- [ ] **the mqtt connection has no TLS field.** Plaintext only — a real gap
      for anything beyond a local broker, and deliberately not bolted on
      without answering where a CA certificate lives first (a `Secret`? a path
      resolved against `--data-dir`, the way the `file` connection's root is?
      that's the `file` output's sandbox question again, one level up). See
      the doc comment on `kayak_core::connections::MqttConnection`.
- [ ] **`AckMode::OnDelivery` only reaches this pipeline's own outputs.** See
      `src/inputs/ack.rs`'s module docs for the full reasoning — the short
      version is that following an acknowledgement transitively through the
      `pipeline`-input graph would couple an input's redelivery behaviour to
      the liveness of pipelines several hops away, which nothing else in
      kayak does. Worth a real design pass now that mqtt (and, later, AMQP)
      have genuine redelivery semantics their client libraries hold the ack
      open for, unlike kafka's timer-based commit: candidates are a second
      `AckMode` for "wait for at least one output" (today it's
      implicitly "all of them"), and a considered answer — not an accident —
      to whether "delivered" should ever mean more than this pipeline.
- [ ] **no license file.** Can't be adopted by anyone else without one,
      whatever the code quality. Pick one before calling anything past this
      point "released."
- [ ] **no release/packaging story beyond the Dockerfile.** No crates.io
      publish, no versioned releases, no CHANGELOG. Fine for a project in
      active development; blocking for "someone else can depend on this."
- [ ] **a published, versioned docs site (VitePress off the same reflection).**
      The in-app `/docs` tab only ever shows the schema of whatever binary is
      running, and only exists once a server is up — no good for evaluating
      kayak before installing it, and no way to browse an older release's
      reference after a newer one ships. `GET /api/docs` and
      `/api/openapi.json` are already the clean boundary (same JSON the
      Leptos page and Scalar render from), so this is a generator script —
      JSON in, markdown + frontmatter out — run in CI on tag, not a new
      reflection layer. `vitepress-openapi` can consume `/api/openapi.json`
      directly, so the HTTP-API half is close to free. Do this *after* fixing
      the flat-table gap noted under "the component reference" in
      `CLAUDE.md` (list/nested-object fields don't render) — that's a minor
      omission in-app, but reads as broken docs once it's public. Supplements
      the in-app page rather than replacing it; they answer different
      questions.

## the machine-cycle scenario

The worked case this is being built towards, kept here so the remaining pieces
are a list rather than a conversation to re-derive. An injection-moulding
machine publishes to nats — `<machine>.cycle_status` (1 opens a cycle, 0 closes
it), `<machine>.unit_id`, `<machine>.recipe`, `<machine>.temperature` and
`<machine>.pressure` at 2 Hz. Per cycle we want the average pressure per unit on
one subject, and every temperature reading of that cycle posted as one array to
an ML service with the answer published on another.

The target graph is four pipelines: one reads `*.*` and attributes the readings,
one cuts them into cycles, and two reduce each cycle. **One wildcard
subscription rather than five inputs is load-bearing** — merged inputs have no
ordering between them, so a `cycle_status: 0` could overtake the last reading of
its own cycle, while a single subscription is delivered in publish order.

Already in: the envelope (the subject is where `machine_id` lives), field paths,
and state buckets with `remember`/`recall` (attributing readings to the current
unit and recipe).

Left to build, roughly in dependency order:

- [ ] **the session window transform** — the heart of it. Keyed by the
      pipeline's `state.key`, opened and closed by `Condition` lists (the same
      type `remember` already takes), emitting one batch per completed cycle so
      that a downstream `reducer` over the whole batch *is* a per-cycle
      aggregation. Needs `max_messages` so a cycle that never closes is capped
      rather than fatal, and a `linger` on close — a small grace period before
      emitting — which is the cheap answer to the boundary race. Decide whether
      the boundary messages are included (I'd say yes: they're data).
- [ ] **a tick for transforms** — the window's idle-timeout needs one, and so
      does the "idle file output holds its part open" issue below. Transforms
      are currently only ever driven by an arriving batch, which is also why
      bucket eviction is lazy. One mechanism, three users.
- [ ] **`subject_fields` on the nats input** — name the subject's tokens so
      `machine_7.temperature` arrives as `_meta.machine_id` and `_meta.signal`.
      Without it a wildcard subscription is unusable, since nothing can address
      part of a subject. Small, and unblocks keying by machine.
- [ ] **request shaping and response merging on the http transform** — it
      currently posts the batch verbatim and *replaces* it with the reply, so
      the ML call can neither send `{machine_id, unit_id, temperatures: [...]}`
      nor keep the identifiers it needs to publish the answer under. Wants
      headers/auth, a timeout and a retry too, and while in there: `verb` is
      accepted and ignored (see known issues).
- [ ] **templated output subjects and topics** — `kayak.{machine_id}.avg_pressure`.
      Without it every machine's results land on one subject with the id only in
      the body, which throws away the routing nats is for.
- [ ] **compound conditions on `filter`** — `remember` already takes a list of
      `Condition` meaning "all of these", while `filter` still takes a single
      externally-tagged `FilterKind`. Moving `filter` onto the same type would
      make one spelling of "a test on a message" and let a filter match on two
      fields, which the cycle pipelines want. A wire-format change to `filter`.
- [ ] **rename `RecallMissingPolicy::Null`** — probably to `keep`, reading
      against `skip`. Bare `on_missing: null` in YAML parses as a null and fails
      with `invalid type: unit value`, which points nowhere near the problem;
      the value has to be quoted. Kayak's own writer quotes it, so this only
      bites hand-written config — but it shouldn't bite at all.
- [ ] **a `map` transform** (set / rename / copy / drop a field) — no way to
      reshape a message today. Mostly wanted for things the items above cover
      specifically, so it is last; worth doing if a third case turns up.

Not planned, and worth knowing why: **event-time windows with watermarks.** The
linger above is a fudge over arrival order. Doing it properly means reading the
OPC timestamps and holding windows open against a watermark, which is a much
larger concept — and the durability argument under "state" says the same thing:
correctness across restarts starts at the input, with checkpointed positions,
not at the pieces downstream of it.

## known issues

Found during the error-handling pass on 2026-08-03. Each one needs a decision,
which is why they weren't just fixed.

- [x] **splitter drops the remainder.** (fixed 2026-08-11: a leftover is emitted
      as a **short final batch**, not held for the next `apply()` — a transform
      gets no tick, so a remainder on a stream that then goes quiet would be held
      for the life of the pipeline, and the same missing tick is behind the idle
      file part and the lazy bucket eviction. `out_size: 0` is now refused at
      build time rather than quietly emitting a batch per message.)
- [ ] **the http transform ignores `verb`.** Every request is a POST regardless
      of what the config says. Honouring it would change behaviour for existing
      configs, so it needs a decision first. Note the `http` **output** honours
      its own `verb` (and refuses the bodyless methods), so the two components
      now read the same field differently — which is the argument for settling
      this rather than leaving it.
- [ ] **dead pipelines stay in the map.** When a run loop exits (e.g. its input
      errored), the `PipelineHandle` stays in `AppState`, so `GET /api/pipelines`
      lists a pipeline that isn't running. `join_handle` is never inspected.
      Needs a real lifecycle/status concept — running / stopped / failed —
      probably surfaced in the UI cards too.
- [x] **file output has a hardcoded path.** (fixed 2026-08-07: it now takes a
      `file` connection, a `path` under it, a `format` and a `rotate` policy,
      and is sandboxed by `--data-dir`. See "file output" above.)
- [ ] **parquet file output.** The format is `ndjson` or `json_array` so far.
      Parquet needs the arrow ecosystem — worth a feature gate, given what it
      costs every build — and raises a question the JSON formats don't: messages
      are untyped, so a writer has to infer a schema and decide what to do with
      the batch that does not match it.
- [x] **object-store (s3) output.** (done 2026-08-08: a separate `s3` output and
      `s3` connection sharing `src/outputs/rotate.rs` whole, with rustfs in
      `docker-compose.yaml` to write into. See "s3 output" above. Azure Blob and
      GCS are the same shape again — a connection kind, a destination module and
      a `FieldType::Connection` marker — and `object_store` already speaks both,
      so they are feature flags and config rather than new machinery.)
- [ ] **date partitioning.** `dt=2026-08-07/` in the path is what makes an
      object store queryable, and is a different thing from rotation. Needs the
      writer to hold several open parts keyed by partition rather than one.
- [ ] **an idle file output holds its part open.** `interval_secs` is only
      checked when a batch arrives, so a pipeline that goes quiet does not close
      its part on the interval. Wants a timer, which means the output needs a
      tick it does not currently get.
- [x] **`--port` does nothing.** (fixed 2026-08-11: replaced with `--listen`,
      which takes a whole `SocketAddr` and actually binds. `src/listen.rs` holds
      the precedence rule — `--listen` > `LEPTOS_SITE_ADDR` > `Cargo.toml` — and
      its tests; the flag is an `Option` so that absent is byte-for-byte the old
      behaviour and `cargo leptos watch` keeps working.)
- [ ] **serving under a path prefix.** An ingress at `example.com/kayak/` needs
      the prefix in every URL the server emits, and the wasm bundle is built
      once so it cannot be compiled in — it has to reach the browser from the
      SSR'd HTML (a `<meta>` read on hydrate, rather than a `<base href>`, which
      silently re-points every relative URL on the page). Touches
      `ApiClient.base` (already a field, already always empty — that seam is
      cut), the `/events` literal in `app.rs`, the `/pkg/` URLs in `shell`,
      `leptos_router`'s base, `Router::nest`, the session cookie's `Path=/` (two
      kayaks behind one host otherwise clobber each other's sessions) and the
      OpenAPI `servers` entry. Call the flag `base_path`, not `base_url`: an
      external origin is a *different* feature, wanted by OIDC redirect URIs and
      nothing that exists today. A host-based ingress needs none of this and
      works now.
- [x] **hurl tests are stale.** (fixed 2026-08-03: replaced with
      `hurl/tests/pipelines-crud.hurl`, which hits `/api/pipelines` and asserts the
      409/422/204 codes. Its old job is now done in-process by `tests/api.rs`.)
