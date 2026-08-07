# kayak - graph-based stream processing

## the canvas

Cards are laid out automatically as a top-to-bottom hierarchy — a `pipeline`
input makes its pipeline a child of the one it names as upstream — until you
drag one somewhere else, at which point that card stays put and everything else
carries on being placed for you. See "arranging the canvas" below.

It is a DAG rather than a tree: a pipeline with several `pipeline` inputs has
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

**Channels are separated automatically.** The middle segment of every route
wants the same place — half way between the two rows — so a fan-out would be
drawn as one thick line with a few stubs coming out of it. Instead each edge
takes the nearest grid line to half way that no other channel is already lying
along, working outwards a cell at a time, so an edge only moves as far as it has
to and a graph with room to spare looks exactly as it would have. Edges the user
has placed by hand are laid down first and the automatic ones route around them.

**Three parts of a route are draggable in edit mode**, and each is the answer to
something automatic routing can't get right on its own. Double-click any of them
to put that part back to automatic.

*The channel* — the middle segment, the part between the two cards and the only
one not pinned to a port. The automatic separation keeps the lines apart but has
no opinion about which one should pass above which; dragging is how you say.
Stored as an *offset* from the half-way line rather than a coordinate, so it
survives either card being moved, and clamped to the gap between the cards,
since past either end the route would double back. A stored offset of zero is a
real answer — "on the half-way line, whatever else is there" — which is why
handing a channel back to the automatic separation is done by double-clicking
it, not by dragging it back to the middle. A route with no middle to move — a
straight line between two aligned cards, an L-shape between perpendicular faces
— has no handle, because one that did nothing would be worse than none.

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
nothing observable happens until the buffer closes. A pipeline with several
upstreams lights *all* its incoming edges: the event says a batch arrived, not
which input carried it.

| gesture | does |
| --- | --- |
| wheel / trackpad scroll | zoom about the cursor, 20%–250% (shown in the navbar) |
| drag empty canvas | pan (dragging *on* a card selects its text instead) |
| click a name in the sidebar | glide the camera to centre that pipeline |
| `flat` / `tree` in the sidebar header | switch between the pipelines in id order and the same set nested under the upstreams that feed them |
| type in the sidebar's search box | narrow the list; in tree mode a match keeps the chain above it |
| `edit` in the navbar | switch out of read-only, revealing the controls below |
| `+` in the sidebar header | open the "add pipeline" modal |
| `×` on a sidebar row | delete that pipeline (click twice — the first click arms it) |
| drag a card's title bar (edit mode) | move it; it snaps to the grid |
| drag a card's bottom-right corner (edit mode) | resize it; also snapped |
| double-click a card's title bar (edit mode) | put it back under the automatic layout |
| drag the middle of a line (edit mode) | move that line's channel closer to one card or the other |
| drag the end of a line (edit mode) | slide where it connects along the card's face |
| double-click either (edit mode) | put that part of the line back to automatic |

The sidebar's pipeline list has the same two views of the graph the canvas has,
behind the `flat` / `tree` button in its header. Flat is every pipeline once, in
id order — sorted in the browser, because `GET /api/pipelines` walks a hash map
and its order changes between reloads. Tree nests each pipeline under the
upstreams that feed it, which the graph being a DAG makes slightly more than an
indent: a pipeline with several upstreams is listed under each of them, in full
under the *deepest* one — the parent the canvas draws its card below, so the two
agree about where it lives — and as a dimmed pointer under the rest. Those
pointers don't repeat their children and carry no delete: one pipeline, one
`×`.

The search box above the list filters it. In flat mode that is just the matching
rows; in tree mode the ancestors of a match are kept as well, because a match
indented under nothing has lost the one thing the tree was for. Descendants are
not — searching for a root would otherwise show the whole graph. Both modes and
the filter are `frontend/src/sidebar.rs`, unit-tested away from the browser like
`graph.rs`.

Each card shows its config as a tabbed property list — inputs / transforms /
outputs — over a live message log. The log carries failures as well as messages:
a `UiEvent` is either a `batch` or an `error`, and an error is logged in red as
`<stage> error: <cause>` on the card of the pipeline it happened in. That covers
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
it is a `POST /api/pipelines`; the `×` on a sidebar row is a `DELETE`, armed by
the first click and fired by the second.

**Edits are live, not staged.** Creating a pipeline starts it running
immediately; deleting one cancels its run loop immediately. The canvas stays a
true window onto the server — a pipeline you just added streams messages like
any other — which is the whole reason the editor and the live view are the same
screen. The price is that there is no draft to throw away, so `revert` (below)
is the undo.

Note that the mode is a property of the browser tab, not of the server: the API
still accepts writes regardless. This is a local development tool and the API is
its documented interface. If you ever want the mode enforced, that belongs on
the server as a flag, not in the UI.

**The form is generated, like the docs are.** It is built from the same
`kayak_core::docs` reflection over the config schemas, so a new component
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

**Saving** takes a bare file name, written into the one directory the server
saves to. That constraint is a security boundary, not a convenience: the browser
can't write to the server's disk, the server does on request, so an
unconstrained path would be an arbitrary-write primitive for anyone who can
reach the UI. Names containing a separator, a `..` or a root are refused rather
than normalised — normalising is where these checks go wrong. Overwriting the
loaded file is just typing its own name, which the modal warns about.

**Starting without a config file.** `--config` is optional, and a server without
one still runs whatever you build in the UI — so it can also be asked to write
that graph out. In edit mode the navbar offers `create config file` instead of
`save as…`, and the modal names the directory the file will appear in: the
process's working directory, chosen when the server was started and never by the
request, exactly as `--config`'s directory would be. The file that save creates
*becomes* the server's config file, so from then on there is a `revert` to go
back to, an `unsaved changes` marker that means something, and a home for both
the canvas arrangement and the connections — which are written out at that
moment rather than being lost.
Saving under a second name later is still a copy: the loaded file stays the one
the server works against.

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
shape recurs. Cancelling every pipeline and *then* dropping the upstreams wakes
each downstream with two things ready at once — its own cancellation, and an
"upstream pipeline 'x' is gone" from the closing channel. `select!` picks
randomly between ready branches, so a third of the time the run loop reported
the shutdown *it had been asked to perform* as a pipeline failure. Those errors
went to the UI, where they landed on the cards of the newly built pipelines that
had just inherited the same ids — so a perfectly good revert looked like it had
produced a broken graph. The fix is `biased;` in the run loop's `select!` plus a
cancellation check before reporting any input failure: an input dying because we
asked it to is not news. An input dying on a pipeline that is *still running*
still is, which is the distinction the check makes.

`GET /api/settings` reports the file name, the directory saves land in, and
whether there are unsaved changes. No file name means there is no config file
*yet*, which is what turns the navbar's `save as…` into `create config file`.

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
  "pipelines": {
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
of a smaller map. Without a config file there is nowhere to put it, so the
arrangement lives in memory — until a save creates one, which writes it out on
the spot rather than losing the tidying that came before it.

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
written by hand — `kayak_core::docs` reflects over the same `JsonSchema`
derives the config types already carry, and `schemars` carries the doc comments
through as descriptions.

What that means in practice: **the doc comments on the config structs in
`kayak-core/src/config.rs` are the documentation**. Add a component and it
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

## the sample

`example_config/` is what to point the server at while working on it, and what
`just dev` uses:

| | |
| --- | --- |
| `config.json` | the worked example: every component kind |
| `config.yaml` | the same graph, spelled as YAML |
| `config.connections.{json,yaml}` | the systems those pipelines name |
| `config.layout.json` | where the cards sit on the canvas |
| `secrets.example.json` | what the `${NAME}` references resolve against |

One directory because the set travels together: the connections and layout files
are *derived* from the config's path, so they only find each other when they sit
side by side. `tests/config.rs` and `tests/graph.rs` load these files, so a
sample that stops parsing — or a component added to the JSON and not the YAML —
fails `just test` rather than rotting quietly.

`heartbeat_to_disk` is the file output's sample, and it hangs off `heartbeat`
rather than off the nats source on purpose: the dummy input needs nothing
running, so it is the one pipeline in here that writes real output on a bare
`just dev` with no `docker compose up`. Twenty messages a part at one a second,
so you see it rotate while you watch. It is also why `just dev`, `tests/graph.rs`
and the `Dockerfile` all pass `--data-dir dev_data` — without the flag that
pipeline refuses to build and takes the whole load down with it, which is the
closed default working as intended.

## connections

A kafka cluster or a nats server is usually shared. One pipeline per topic on
the same brokers is the normal shape, and repeating the broker list — and its
`${NAME}` references — in every one of them is both tedious and a way for them
to drift apart. So the connection is declared once, under a name, in a third
file beside the config:

```json
// config.connections.json
{
  "prod-kafka": { "type": "kafka", "brokers": "${KAFKA_BROKERS}" },
  "local-nats": { "type": "nats", "urls": "nats://localhost:4222" }
}
```

```json
// config.json
{ "type": "kafka", "connection": "prod-kafka", "topic": "orders", "group": "kayak" }
```

The split between the two is **"what does the system need" against "what does
this pipeline want from it"**: brokers, urls and credentials belong to the
connection; the topic, the consumer group, the subject and the postgres table
belong to the component. There is no inline form — a component names a
connection or it does not build.

One kind serves both directions: a `kafka` connection is what a kafka input
consumes from *and* what a kafka output publishes to. The kind is checked as
well as the name, so a nats connection in a kafka input is refused at build time
with an error saying which kind it actually is, rather than being handed to a
broker as a broker list. An unknown name lists the ones that do exist, since the
usual cause is a typo.

**Where the file comes from.** `--connections <path>` names it outright, which
is how two configs share one; without the flag it is derived from the config's
name and format — `config.json` → `config.connections.json`, `pipelines.yaml` →
`pipelines.connections.yaml`. A derived file that isn't there means "no
connections", which is the ordinary state of a graph built out of dummies; a
file named with the flag has to exist, because starting without it would fail
later and further from the cause.

**It follows the config file's rules, not the layout file's.** Adding a
connection in the UI changes what the server can build, so it is an unsaved
change, and only a save writes it — the same save, since a config saved without
the connections it names would not start. `revert` reloads both files, the
connections first, because the pipelines being rebuilt name them.

**A connection is read when a component is built.** Editing one therefore
reaches new and rebuilt pipelines rather than the running ones, and deleting one
a running pipeline still names is refused with a 409 listing them — delete those
first. Nothing is pooled: two pipelines on one connection each get their own
client, built from the same settings.

In the UI, connections are the second tab in the sidebar, with the same `+` and
the same armed delete as the pipelines. The form is generated the same way too —
a connection kind is documented on `/docs` and gets its controls from the same
schema reflection. Secrets are *referenced* there and never entered: a field
takes `${NAME}`, and what that resolves to stays a deployment concern.

## file output

Writes each batch into rotating files in a directory. It exists for local
development and testing — the object-store output is what this shape is being
built towards for anything else — but the parts that will be shared with it are
already split out: rotation, part naming and encoding live in
`src/outputs/rotate.rs`, which touches no filesystem at all, and only the
destination lives in `src/outputs/file.rs`.

```jsonc
// config.connections.json — where this server may write
{ "local-files": { "type": "file", "root": "./dev_data/events" } }

// config.json — what this pipeline writes there
{ "type": "file", "connection": "local-files", "path": "orders",
  "format": "ndjson", "rotate": { "max_rows": 100000, "interval_secs": 3600 } }
```

`format` is `ndjson` (the default — one JSON message per line) or `json_array`
(the whole file is one array, closed when the part rotates). Prefer `ndjson` for
anything that streams: the file is valid after every batch, so a run that is
still going, or that died, is still readable.

`rotate` closes the current file and starts the next. `max_rows` counts
messages, `interval_secs` measures from when the part was *opened*; either may
be omitted, and whichever comes first wins. With neither, a pipeline writes one
file for as long as it runs. Rotation is checked **after** a batch is written,
so a batch is never split across two files and `max_rows` is a floor rather than
a ceiling — a batch of 500 arriving at 999 rows makes a file of 1499.

Part names are generated, not configured: `2026-08-07T14-00-00Z-000001.ndjson`.
The open timestamp makes a run's parts sort chronologically under a plain `ls`
or an object-store prefix listing, and the sequence number keeps two parts
opened in the same second distinct — which a row trigger on a fast pipeline will
do, and where a collision would lose data silently.

### the sandbox

**A file output cannot write anywhere until the server is told where.** This is
the same problem `persist::save_path` solves for config files, and for the same
reason: the browser does not write to the server's disk, the *server* does, on
request. `POST /api/pipelines` and `POST /api/connections` both take their
contents from an HTTP body, so an unconstrained path in either would turn the
pipeline editor into an arbitrary-write primitive — and this one writes
attacker-influenced *content*, at whatever volume the pipeline carries.

So there are two layers, and neither is sufficient alone:

1. **`--data-dir <path>`**, fixed when the process starts and reachable by no
   request. Without the flag file outputs refuse to build at all. The closed
   default is deliberate: a disk writer is not something a deployment should get
   without asking for it.
2. **the connection's `root`**, which arrives over HTTP like anything else and
   is therefore checked against layer 1 rather than trusted. It is what lets an
   operator hand different pipelines different subtrees.

The component's `path` is relative to the root. Paths are **refused, never
normalised** — an absolute path or one containing `..` fails the build rather
than being trimmed, because trimming leaves whoever wrote it believing it meant
something, and a normaliser is one edge case away from being the hole it was
written to close. After resolving, the landing directory is canonicalized and
re-checked against both layers, which is what stops a symlink planted inside the
root from pointing out of it.

All of it is decided at **build** time, and the build creates the directory: a
path that escapes, or a root nobody can write to, fails the pipeline that owns
it rather than surfacing an hour into a run. `just dev` passes
`--data-dir dev_data` so the component is usable without ceremony; the directory
is gitignored, being output rather than a fixture.

Two things it does not do yet. An `interval_secs` rotation is only noticed when
the next batch arrives, so an idle pipeline holds its part open past the
interval — there is no timer task closing it. And a pipeline cancelled
mid-part leaves a `json_array` file unclosed; every batch is flushed as it is
written, so no data is lost, but the trailing `]` is missing. `ndjson` has
neither problem, which is another reason it is the default.

## secrets

Config files are meant to be version controlled, so they carry *references* to
secrets rather than the secrets themselves. Any field typed `Secret` — these all
live on connections now: the `urls` of a nats connection, the `brokers` of a
kafka one, the `password` of a postgres one — accepts `${NAME}` placeholders:

```json
{ "prod-nats": { "type": "nats", "urls": "nats://app:${NATS_PASSWORD}@broker:4222" } }
```

Those are filled in when the pipeline is built, from two sources consulted in
order:

1. the process environment;
2. a JSON file of `"NAME": "value"` pairs passed as `--secrets ./secrets.json`.

The environment comes first so a single secret can be overridden for one run
without touching the file. The flip side is that an unrelated environment
variable with a colliding name shadows the file, so keep the names specific;
a shadowed lookup is logged at debug level. `example_config/secrets.example.json`
shows the file format; anything named `secrets.json` is gitignored, wherever it
sits, which is why `just dev` creates the sample's copy rather than the
repository carrying one.

A value with no `${...}` in it is passed through untouched, so fields that hold
nothing sensitive need no special handling. An unknown name is an error, not an
empty string — the pipeline fails to start (or the `POST /api/pipelines` gets a
4xx) rather than quietly connecting without credentials.

The resolved value never leaves the runtime component that needs it. `Secret`
(in `kayak-core`, so wasm-safe) only ever holds the unresolved template, and
that is what `GET /api/pipelines` returns and what the UI shows. `Resolved` (in
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
  `FailOnNth`, and `PipelineRuntime::from_parts` to assemble a pipeline without
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
`config.json` writes. That pipeline names the `local-postgres` connection in
`config.connections.json`, whose password is a `${POSTGRES_PASSWORD}` reference,
so running the server against the sample config needs a secret:

```bash
just dev
```

That is the whole of it: `just dev` copies `example_config/secrets.example.json`
into place if there is no `example_config/secrets.json` yet, then runs
`cargo leptos watch` against the sample. `just dev-yaml` is the same graph in its
other spelling.

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
- [ ] make outputs optional (for example, when a parent pipeline is only used to push data to children)
- [ ] think about necessary metadata to add to each message
- [x] deal with all unwraps -- this will bite us in the ass soon otherwise
      (done 2026-08-03: no unwrap/expect left in src/; see "known issues" below
      for the things that pass turned up but didn't change)
- [x] show config in the "cards" in the web ui
      (done 2026-08-04: tabbed property list, see "the canvas" above)
- [x] give pipeline ability to have multiple inputs
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
- [ ] **object-store (s3) output.** The reason `src/outputs/rotate.rs` is split
      out from `src/outputs/file.rs`: rotation, part naming and encoding are
      already shared, and what is left is a destination. Its connection replaces
      `root` with a bucket and credentials; `--data-dir` does not apply, since
      the sandbox there is whatever the credentials can reach.
- [ ] **date partitioning.** `dt=2026-08-07/` in the path is what makes an
      object store queryable, and is a different thing from rotation. Needs the
      writer to hold several open parts keyed by partition rather than one.
- [ ] **an idle file output holds its part open.** `interval_secs` is only
      checked when a batch arrives, so a pipeline that goes quiet does not close
      its part on the interval. Wants a timer, which means the output needs a
      tick it does not currently get.
- [ ] **`--port` does nothing.** `src/main.rs` only logs it; the listener binds
      `leptos_options.site_addr` from `Cargo.toml`. Running the binary outside
      `cargo leptos` therefore falls back to port 3000. Either wire the arg into
      the leptos options or drop it.
- [x] **hurl tests are stale.** (fixed 2026-08-03: replaced with
      `hurl/tests/pipelines-crud.hurl`, which hits `/api/pipelines` and asserts the
      409/422/204 codes. Its old job is now done in-process by `tests/api.rs`.)
