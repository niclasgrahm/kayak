# kayak - graph-based stream processing

## the canvas

Cards are laid out automatically as a top-to-bottom hierarchy — a `streamer`
input makes its pipeline a child of the one it names as upstream — with curved
edges from each parent's bottom edge to its child's top edge. Positions are
computed, not stored: there is no card dragging yet.

It is a DAG rather than a tree: a pipeline with several `streamer` inputs has
several parents, and sits one row below the deepest of them so that every edge
still points downwards.

An edge lights up when a batch crosses it and fades back over ~700ms, so a busy
graph glows rather than strobes (and doesn't animate at all under
`prefers-reduced-motion`). The signal is the *downstream's* `input` UI event,
which means a pipeline whose input is buffered blinks once per closed window
rather than once per message — its upstream is feeding it continuously, but
nothing observable happens until the buffer closes. A node with several
upstreams lights *all* its incoming edges: the event says a batch arrived, not
which input carried it.

| gesture | does |
| --- | --- |
| wheel / trackpad scroll | zoom about the cursor, 20%–250% (shown in the navbar) |
| drag empty canvas | pan (dragging *on* a card selects its text instead) |
| click a name in the sidebar | glide the camera to centre that node |

Each card shows its config as a tabbed property list — inputs / transforms /
outputs — over a live message log. The log carries failures as well as messages:
a `UiEvent` is either a `batch` or an `error`, and an error is logged in red as
`<stage> error: <cause>` on the card of the streamer it happened in. That covers
the three places the run loop tolerates a failure — a transform that threw, an
output that couldn't emit, an input that died — and it's the same text the
server log shows, so a card no longer just goes quiet for reasons only visible
in the terminal. `frontend/src/log.rs` turns an event into log lines and is unit
tested; `frontend/src/inspector.rs` builds those rows
from `serde_json::Value` rather than by matching on the config enums, so a new
component kind or a new field shows up without touching the frontend; the row
names are the wire names.

All the geometry — layout, edge paths, zoom anchoring, the camera glide — lives
in `frontend/src/graph.rs` as pure functions with unit tests, and the same goes
for the inspector rows. Keep it that way: the Leptos components should only feed
those functions and render the result, since anything inside a component can't
be tested without a browser.

## the component reference

`/docs` is a generated reference for every input, transform and output: field
names, types, which are required, and what each one does. Nothing about it is
written by hand — `streamer_core::docs` reflects over the same `JsonSchema`
derives the config types already carry, and `schemars` carries the doc comments
through as descriptions.

What that means in practice: **the doc comments on the config structs in
`streamer-core/src/config.rs` are the documentation**. Add a component and it
appears; add a field and it appears; leave the doc comment off and a unit test
fails (`every_component_has_a_description_from_its_doc_comment`). Two things are
worth knowing when writing them: blank lines start a new paragraph and single
newlines don't, and `backticks` render as code.

The page itself is a Leptos route with a searchable sidebar; the search matches
kinds, field names and descriptions, so "subject" finds both nats components.
The same data is served as JSON at `GET /api/docs` for anything that isn't a
browser. The arranging logic is pure and unit-tested in `frontend/src/docs.rs`,
same as `graph` and `inspector`.

## pipelines

A pipeline is `inputs → [transforms] → outputs`, and all three are arrays.

Every input is **merged** into one stream: the transform chain runs once per
batch, whichever input produced it, and there is no ordering between two
different inputs. Every output then receives **every** batch. So "archive to
postgres and watch it on stdout" is one pipeline with two outputs, not two
pipelines.

At least one input is required — a pipeline with none could never produce
anything, so it's rejected at build time. Zero outputs is fine: such a pipeline
exists to feed the ones downstream of it, and still fans out to them.

The failure rules follow from that. One input dying is reported on the card and
survived, because the others are still feeding the pipeline; only the last one
going takes the run loop with it. One output failing is reported and skipped for
that batch, and its siblings and the downstream pipelines still get theirs. An
output that can't `init()` at all is fatal, since it would never accept anything.

Merging runs a pump task per input (`inputs::merge`) rather than `select!`ing
over them. Selecting drops the losing futures on every iteration, and an input
that waits on a timer would have its timer restarted every time a chattier
sibling produced — starving it forever. There's a test for exactly that.

## secrets

Config files are meant to be version controlled, so they carry *references* to
secrets rather than the secrets themselves. Any field typed `Secret` — currently
the `urls` of the nats input and output, and the `password` of the postgres
output — accepts `${NAME}` placeholders:

```json
{ "type": "nats", "urls": "nats://app:${NATS_PASSWORD}@broker:4222", "subject": "s" }
```

Those are filled in when the pipeline is built, from two sources consulted in
order:

1. the process environment;
2. a JSON file of `"NAME": "value"` pairs passed as `--secrets ./secrets.json`.

The environment comes first so a single secret can be overridden for one run
without touching the file. The flip side is that an unrelated environment
variable with a colliding name shadows the file, so keep the names specific;
a shadowed lookup is logged at debug level. `secrets.example.json` shows the
file format, and `secrets.json` is gitignored.

A value with no `${...}` in it is passed through untouched, so fields that hold
nothing sensitive need no special handling. An unknown name is an error, not an
empty string — the streamer fails to start (or the `POST /api/streams` gets a
4xx) rather than quietly connecting without credentials.

The resolved value never leaves the runtime component that needs it. `Secret`
(in `streamer-core`, so wasm-safe) only ever holds the unresolved template, and
that is what `GET /api/streams` returns and what the UI shows. `Resolved` (in
`src/secrets.rs`) holds the real value but prints the *template* from `Display`
and `Debug`, so a connection error logs
`nats://app:${NATS_PASSWORD}@broker:4222` and nothing worth leaking. Getting at
the value takes an explicit `.expose()`, which is the thing to grep for in
review. Writing a password inline instead of referencing it defeats all of
this — that's the habit the syntax exists to replace.

## testing

`just ci` (= `just lint` + `just test`) is what has to be green before pushing;
GitHub Actions runs the same two commands. Everything runs offline — no NATS, no
running server, no ports.

The runtime lives in `src/lib.rs`, not in `main.rs`, purely so the tests in
`tests/` can reach it. `main.rs` is only argument parsing plus the Leptos wiring.

Five layers, cheapest first:

| where | what it covers |
| --- | --- |
| `src/transforms/*.rs` `#[cfg(test)]` | pure per-transform logic: what's kept, dropped, buffered, split |
| `tests/config.rs` | the JSON wire format of every component kind |
| `tests/pipeline.rs` | the run loop: transform chaining, error tolerance, fan-out, cancellation, UI events |
| `tests/graph.rs` | `AppState`: ids, upstream wiring, lifecycle |
| `tests/api.rs` | the HTTP surface and its status codes, via `tower::oneshot` |
| `hurl/tests/*.hurl` | one smoke test against a really running server (`just test-http`) |

Two things to know when adding a component:

- `tests/config.rs::every_component_kind_has_a_wire_format_sample` reads the
  variants straight out of the generated JSON schema and fails until you add a
  sample for the new one. That's deliberate — it's the guard rail that keeps the
  wire format covered as the component list grows.
- `src/testing.rs` has the test doubles: `ScriptedInput`, `CollectingOutput`,
  `FailOnNth`, and `StreamerRuntime::from_parts` to assemble a pipeline without
  going through a config. Prefer these over touching the network in a test.

Timing-dependent tests use `#[tokio::test(start_paused = true)]` so a 10-second
window costs no wall time.

Not covered by `just test`, and deliberately so: the NATS and kafka
input/outputs, the HTTP transform and the postgres output, which are thin
wrappers over their clients — they need `docker compose up` and are exercised by
`just start-baseline` / `just test-http`. What *is* tested offline for postgres is the part with a
decision in it: `Table::parse` in `src/outputs/postgres.rs`, which validates the
configured table name and builds the two statements. The table name cannot be a
bind parameter, so it is interpolated into the SQL text, and that check is the
only thing standing between `config.json` and an arbitrary statement.

`docker compose up` also brings up a single-node kafka (KRaft, no zookeeper) on
:9092 with a publisher putting one JSON line a second on `test.events`, which
the `kafka_events` pipeline consumes and `slow_requests` filters back out to
`test.slow`. The broker advertises two listeners — `localhost:9092` for the
server running on the host, `kafka:29092` for the other containers — because
they can't both reach it by the same name.

Two things worth knowing when playing with the kafka input. It joins a consumer
group, so **two servers running the same config share the topic**: with a
one-partition topic only one of them gets an assignment and the other looks
broken. And leaving a group takes a session timeout to notice, so after killing
a server the next one can sit idle for ~45s before kafka rebalances the
partition onto it. Both of those cost me a confusing ten minutes; they are kafka
working as designed, not the pipeline being wrong.

`docker compose up` also brings up postgres on :5432 (database `kayak`, role
`kayak`, password `hunter2`), which is where the `sensors_archive` pipeline in
`config.json` writes. Because that pipeline's password is a `${POSTGRES_PASSWORD}`
reference, running the server against the sample config now needs a secret:

```bash
cp secrets.example.json secrets.json
cargo run -- --config config.json --secrets ./secrets.json
```

## currently working on

- [ ] add filter transform
- [x] add some kind of component plugin registry which can be used to generate docs
      (done 2026-08-04: no registry in the end — `/docs` reflects over the config
      schemas instead, so a component documents itself through its doc comments.
      See "the component reference" above.)

## todo

- [ ] make sure to clean up old template based UI stuff
      (2026-08-04: `/docs` and `templates/docs.html` are gone — Askama is now
      only used by the dead `/ui` index handler, which is all that's left)
- [ ] add time based buffer for the transform buffer
- [ ] make outputs optional (for example, when a parent node is only used to push data to children)
- [ ] think about necessary metadata to add to each message
- [x] deal with all unwraps -- this will bite us in the ass soon otherwise
      (done 2026-08-03: no unwrap/expect left in src/; see "known issues" below
      for the things that pass turned up but didn't change)
- [x] show config in the "cards" in the web ui
      (done 2026-08-04: tabbed property list, see "the canvas" above)
- [x] give streamer ability to have multiple inputs
      (done 2026-08-04: and multiple outputs. `inputs` and `outputs` are arrays
      in the config now — a breaking wire-format change, the singular `input`
      and `output` keys are gone. See "pipelines" below.)
- [ ] new transform (i guess?): wait_for_condition (should it be called buffer_until_condition? or perhaps both are needed?)
      for example, we need to wait for x: a and z: b. for this, we also need the multiple input thing

## known issues

Found during the error-handling pass on 2026-08-03. Each one needs a decision,
which is why they weren't just fixed.

- [ ] **splitter drops the remainder.** `src/transforms/splitter.rs` — with
      `out_size: 3` and a 10-message batch, message 10 is silently discarded
      (the existing `// TODO: theres stuff left here`). Decide whether leftovers
      are emitted as a short final batch or held until the next `apply()`.
      `known_bug_the_remainder_is_currently_discarded` pins today's behaviour;
      flip that test when the decision is made.
- [ ] **the http transform ignores `verb`.** Every request is a POST regardless
      of what the config says. Honouring it would change behaviour for existing
      configs, so it needs a decision first.
- [ ] **dead streamers stay in the map.** When a run loop exits (e.g. its input
      errored), the `StreamerHandle` stays in `AppState`, so `GET /api/streams`
      lists a pipeline that isn't running. `join_handle` is never inspected.
      Needs a real lifecycle/status concept — running / stopped / failed —
      probably surfaced in the UI cards too.
- [ ] **file output has a hardcoded path.** `src/outputs/file.rs` writes to an
      absolute path under `/Users/niclas/...` and truncates it on every
      `init()`. `FileOutputConfig` is an empty struct; it wants at least a
      `path`, and a decision on truncate vs. append.
- [ ] **`--port` does nothing.** `src/main.rs` only logs it; the listener binds
      `leptos_options.site_addr` from `Cargo.toml`. Running the binary outside
      `cargo leptos` therefore falls back to port 3000. Either wire the arg into
      the leptos options or drop it.
- [x] **hurl tests are stale.** (fixed 2026-08-03: replaced with
      `hurl/tests/streams-crud.hurl`, which hits `/api/streams` and asserts the
      409/422/204 codes. Its old job is now done in-process by `tests/api.rs`.)
