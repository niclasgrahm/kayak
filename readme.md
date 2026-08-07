# kayak - graph-based stream processing

## the canvas

Cards are laid out automatically as a top-to-bottom hierarchy — a `streamer`
input makes its pipeline a child of the one it names as upstream — until you
drag one somewhere else, at which point that card stays put and everything else
carries on being placed for you. See "arranging the canvas" below.

It is a DAG rather than a tree: a pipeline with several `streamer` inputs has
several parents, and sits one row below the deepest of them so that every edge
still points downwards.

Edges are **orthogonal and grid-aligned**: they leave a card's face, run along a
grid line to a channel between the two cards, and turn in. Vertical wins
whenever there is room for it, because the graph is a flow and down the page is
what the flow means — a child one row below its parent is joined bottom to top
whether it sits underneath or right across the canvas. The side faces are for
cards that are *level* with each other, which is exactly when a sideways line is
the one that reads right; a card dragged above its parent leaves by the top, and
the line reads as running backwards because it does. Edges sharing a face fan
out along it rather than piling onto one point, in the order of the cards they
lead to.

**Three parts of a route are draggable in edit mode**, and each is the answer to
something automatic routing can't get right on its own. Double-click any of them
to put that part back to automatic.

*The channel* — the middle segment, the part between the two cards and the only
one not pinned to a port. A dozen edges between the same two rows all choose the
same half-way line and lie on top of each other; pull them apart and each is
followable. Stored as an *offset* from that half-way line rather than a
coordinate, so it survives either card being moved, and clamped to the gap
between the cards, since past either end the route would double back. A route
with no middle to move — a straight line between two aligned cards, an L-shape
between perpendicular faces — has no handle, because one that did nothing would
be worse than none.

*The two ends.* Which *face* an edge uses stays automatic — that answer is
nearly always right and it has to keep up as cards move — but where along that
face it attaches is yours. The fan-out spreads ends evenly, which is a good
default and a poor answer when two of the lines need to cross to get where they
are going. Stored as a distance from the start of the face (its left end across
the top and bottom, its top end up the sides) rather than a fraction of it,
because that is what the drag meant: "a card's width in from the corner" should
stay put when the card is made taller.

Each stored end carries the face it was measured on. When the router changes its
mind — a card moves, and an edge that left by the bottom now leaves by the side
— the old number means nothing on the new face, so it is ignored and that end
goes back to automatic. Self-healing, and no cleanup pass over the file.

A pinned end still takes up its slot in the fan-out, even though its position is
then thrown away. That is what keeps nudging one line from shifting its
siblings out from under you; it costs an unused gap in the fan, which is a much
smaller surprise.

One known rough edge, visible rather than wrong: a channel can run straight
through a card that happens to sit between the two ends. That used to need an
obstacle-aware router to fix; now it needs a drag.

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
| `edit` in the navbar | switch out of read-only, revealing the controls below |
| `+` in the sidebar header | open the "add node" modal |
| `×` on a sidebar row | delete that pipeline (click twice — the first click arms it) |
| drag a card's title bar (edit mode) | move it; it snaps to the grid |
| drag a card's bottom-right corner (edit mode) | resize it; also snapped |
| double-click a card's title bar (edit mode) | put it back under the automatic layout |
| drag the middle of a line (edit mode) | move that line's channel closer to one card or the other |
| drag the end of a line (edit mode) | slide where it connects along the card's face |
| double-click either (edit mode) | put that part of the line back to automatic |

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

## editing the graph

The canvas has two modes and **starts in read-only**. That is the default
because the primary use of this page is watching a running system: a live view
should not have a delete button one click from the pipeline list. The edit
controls are not disabled in read-only, they are absent.

`edit` in the navbar reveals them. The `+` in the sidebar header opens a modal
that builds a pipeline: an id, then any number of inputs, transforms and
outputs, each picked from a dropdown and configured field by field. Submitting
it is a `POST /api/streams`; the `×` on a sidebar row is a `DELETE`, armed by
the first click and fired by the second.

**Edits are live, not staged.** Creating a pipeline starts it running
immediately; deleting one cancels its run loop immediately. The canvas stays a
true window onto the server — a node you just added streams messages like any
other — which is the whole reason the editor and the live view are the same
screen. The price is that there is no draft to throw away, so `revert` (below)
is the undo.

Note that the mode is a property of the browser tab, not of the server: the API
still accepts writes regardless. This is a local development tool and the API is
its documented interface. If you ever want the mode enforced, that belongs on
the server as a flag, not in the UI.

**The form is generated, like the docs are.** It is built from the same
`streamer_core::docs` reflection over the config schemas, so a new component
appears in the dropdown with the right fields, the right required markers, the
right dropdowns for closed-value fields, and the right validation — without
anyone touching the frontend. Field doc comments become the labels' tooltips.

Three things the field types decide:

- a field with a closed set of values (`sum | avg | min | max`) is a dropdown,
  and starts blank rather than showing the first value it hasn't recorded;
- a structured field — an input's `buffer`, which is `static | tumbling` with
  different fields either way — is taken as literal JSON, because there is no
  single control that edits it;
- an enum-shaped component (the `filter` transform) gets a `form` picker for its
  `Numeric` / `String` variants, and its fields follow the choice.

Validation is `frontend/src/form.rs`: pure, unit tested, and the same rules
serde applies, so what it accepts the server accepts. It reports every problem
at once rather than one per submit, against the field and the component it
belongs to — two `nats` inputs stay distinguishable. It is not a security
boundary; the server still rejects what it must, and its message (a duplicate
id, an unknown upstream) is shown verbatim in the modal footer.

### the config file

The `--config` file is a **load source and a save target, never a mirror**. The
server reads it at startup and writes it only when asked. Nothing you do to the
graph reaches disk on its own.

That is deliberate, and it was not the first design. Writing through on every
edit conflates "what the server is running" with "what's in the file", and that
conflation has sharp edges in both directions: merely *loading* a file rewrote
it (load goes through create, create wrote), and a stray click in a live view
became a committed change. Separating the two makes both impossible.

So the loop is explicit and symmetric:

| | |
| --- | --- |
| load | file → runtime, at startup |
| `revert` | file → runtime, again — the undo for a session of editing |
| `save as…` | runtime → file |

**JSON or YAML.** The file can be either, and the extension decides which:
`.yaml` and `.yml` are read as YAML, anything else as JSON. That is the whole
rule — a `.yaml` file that isn't YAML fails to start rather than being retried as
something else, because a second guess would hide the typo. The format is a
property of the *file* and stops at the parser: a `Config` doesn't remember which
one it came from, so the two describe exactly the same pipelines and can be
mixed freely (load JSON, save YAML, restart from that).

The `save as…` modal offers the choice, wired to the file name — picking a format
renames the file, and typing a `.yaml` name selects YAML — so the two halves of
the decision can't disagree. `POST /api/config/save` takes an optional `format`
of `"json"` or `"yaml"`; leaving it out takes the format from the name.

**Unsaved changes.** Because edits are live and the file is untouched, the two
can diverge invisibly, and a restart would drop the work. The navbar says
`unsaved changes` whenever they have. The check is exact rather than a heuristic:
`persist::render` is deterministic, so the server compares the rendered graph
against the rendering of what it last loaded or saved. Add a pipeline and remove
it again and the warning goes away, because the graph really is back where it
started. That comparison is always made in JSON, whatever the file is written
in — it is a fingerprint of the graph, and re-spelling it as YAML hasn't changed
which pipelines are running.

**Saving** takes a bare file name, written beside the config the server started
from. That constraint is a security boundary, not a convenience: the browser
can't write to the server's disk, the server does on request, so an
unconstrained path would be an arbitrary-write primitive for anyone who can
reach the UI. Names containing a separator, a `..` or a root are refused rather
than normalised — normalising is where these checks go wrong. Overwriting the
loaded file is just typing its own name, which the modal warns about.

Two properties make the output worth version controlling, both in
`src/persist.rs` and both tested:

- **Deterministic.** Pipelines are topologically sorted — parents before the
  children that name them as `upstream`, which is the order a config file has to
  be in to replay — and ties are broken by id. The same graph always renders the
  same bytes, so a diff means the graph changed, not that a `HashMap` was
  iterated twice.
- **Atomic.** The whole file is rendered before anything is replaced, so a
  failure partway through leaves the previous file rather than half a new one.

A generated petname is written out, since it's the name a downstream's
`upstream` would have to reference. `revert` parses the file before tearing
anything down, so reverting to a file that has been broken by hand leaves the
running graph alone; it also *picks up* hand edits, which makes it the way to
reload a file you changed in an editor.

Reverting also **waits for the old run loops to stop before building the new
ones**, bounded by a few seconds for the one thing that can't be cancelled — an
output already inside `emit()`. Overlapping the two graphs isn't merely untidy:
two run loops for the same pipeline would share a kafka consumer group or a nats
subscription and double up on every output.

That teardown is also where a subtle bug lived, worth knowing about because the
shape recurs. Cancelling every streamer and *then* dropping the upstreams wakes
each downstream with two things ready at once — its own cancellation, and an
"upstream streamer 'x' is gone" from the closing channel. `select!` picks
randomly between ready branches, so a third of the time the run loop reported
the shutdown *it had been asked to perform* as a pipeline failure. Those errors
went to the UI, where they landed on the cards of the newly built streamers that
had just inherited the same ids — so a perfectly good revert looked like it had
produced a broken graph. The fix is `biased;` in the run loop's `select!` plus a
cancellation check before reporting any input failure: an input dying because we
asked it to is not news. An input dying on a streamer that is *still running*
still is, which is the distinction the check makes.

`GET /api/settings` reports the file name and whether there are unsaved changes.
Without `--config` there is nowhere to save, and the UI says so rather than
offering a button that can only fail.

## arranging the canvas

Where the cards sit is **not configuration**, and it is deliberately kept out of
the config file. A config file with pixel coordinates in it stops being
reviewable, and nothing about a position changes what the server runs. So it
lives in its own file beside the config: `config.json` is arranged by
`config.layout.json`, `pipelines.yaml` by `pipelines.layout.json`. Derived from
the config path rather than configured, so the pair travels together.

It is generated and maintained by the program, and meant to be committed —
which is why it is written deterministically (ids in one order, no `null`s for
things nobody set) and atomically, the same as the config file. It holds only
the cards someone has actually moved:

```json
{
  "version": 1,
  "nodes": {
    "everything": { "x": 760, "y": 1180, "width": 360, "height": 320 }
  },
  "edges": [
    {
      "from": "sensors",
      "to": "hot_readings",
      "offset": -60,
      "from_port": { "side": "bottom", "along": 260 }
    }
  ]
}
```

An absent id is the normal case, not a gap: that card is laid out
automatically. `height` is absent unless the card was resized, because the two
are different things — normally a card is as tall as its content, and only an
explicit resize pins it. `edges` is absent entirely unless a line
has been adjusted, each of the three adjustments is absent unless it was made,
and an entry disappears once *all* of them are back to automatic — an undone
adjustment shouldn't leave a no-op behind in a committed file. An entry naming a
pipeline that no longer exists is kept rather than pruned; it costs nothing and
it is still there if the pipeline comes back.

Here the write-through rule is the *opposite* of the config file's, and for the
same reason: moving a card changes nothing the server runs, so `PUT
/api/layout` writes immediately (on release, not per frame) and arranging the
canvas never counts as an unsaved change. There is no save step because there is
nothing worth reviewing before it lands. It is a full replacement rather than a
patch, which is what makes "put everything back to automatic" an ordinary send
of a smaller map. Without a `--config` there is nowhere to put the file and the
arrangement lasts only as long as the process — honest, and better than refusing
to let someone tidy the canvas.

**The grid is the unit.** `GRID` in `frontend/src/graph.rs` is 20px, the card
width is 18 cells, positions and sizes snap to it, ports sit on its lines, and
edge channels run along them — the background grid you can see is the same one
things land on. A route only reads as "along the grid" if the things it connects
are on the grid too, which is also why measured card heights are rounded *up* to
the next line (up, so content still fits; and idempotent, so the measure → lay
out → render loop doesn't oscillate between two heights).

Pinning a card does *not* take it out of the automatic flow: the row it came
from keeps its slot, so dragging one card doesn't rearrange every other card on
the canvas. Two cards can then be dragged on top of each other, which is the
user's business in the same way it is in any other editor with a canvas.

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
| `tests/persist.rs` | the config file: that editing *doesn't* touch it, that saving does, that a save can't escape its directory, and that reverting reloads it |
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
