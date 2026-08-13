# the canvas

Cards are laid out automatically as a top-to-bottom hierarchy — a `pipeline`
input makes its pipeline a child of the one it names as upstream — until you
drag one somewhere else, at which point that card stays put and everything else
carries on being placed for you. See [arranging the canvas](/canvas/arranging-the-canvas).

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
| drag empty canvas | pan |
| click a name in the sidebar | glide the camera to centre that pipeline |
| `flat` / `tree` in the sidebar header | switch between the pipelines in id order and the same set nested under the upstreams that feed them |
| type in the sidebar's search box | narrow the list; in tree mode a match keeps the chain above it |
| click a card's `config` / `stats` / `logs` heading | fold that part away, or bring it back — a shut part stops being fed |
| `▸` at the left of a log row (on hover) | open that row's payload, pretty-printed — the log pauses so it can be read |
| `5s` / `1m` / `5m` on a card's chart | change the bar width, and so how far back the chart reaches |
| `edit` in the navbar | switch out of read-only, revealing the controls below |
| `+` in the sidebar header | open the "add pipeline" modal |
| `×` on a sidebar row | delete that pipeline (click twice — the first click arms it) |
| shift-click a card or a sidebar row (edit mode) | add that pipeline to the selection, or take it out again |
| `⋯` on a sidebar row (edit mode) | `select children` — add that pipeline and everything downstream of it to the selection |
| click empty canvas | clear the selection |
| drag a card's title bar (edit mode) | move it — and every other selected card with it; they snap to the grid |
| drag a card's bottom-right corner (edit mode) | resize it; also snapped |
| double-click a card's title bar (edit mode) | put it back under the automatic layout |
| drag the middle of a line (edit mode) | move that line's channel closer to one card or the other |
| drag the end of a line (edit mode) | slide where it connects along the card's face |
| double-click either (edit mode) | put that part of the line back to automatic |

**Selecting more than one card** is an edit-mode thing, and it exists because a
graph of any size is arranged in handfuls rather than card by card. Shift-click
builds the set — on the cards or on the sidebar rows, whichever is closer to
hand — and dragging any card in it moves all of them together, keeping their
positions relative to each other. A row's `⋯` menu offers `select children`,
which adds that pipeline and its whole subtree; it *adds* rather than replacing,
so two branches of a fan-out take two clicks. A plain click on a card that isn't
selected selects just that one, and a plain click on one that already is leaves
the set alone — which is what lets a group be grabbed by any of its members. The
way out is a click on empty canvas.

**Text is only selectable where it is there to be read.** A card's settings and
its log rows can be swept over and copied — a broker url or a failing message is
exactly the kind of thing that wants pasting somewhere else — and so can the
documentation pages, the connection and bucket cards, and anything typed into.
Everything else on the canvas is a control: dragging a card, a line or the view
itself sweeps the pointer across labels, and shift-click is the browser's
"extend the selection to here" as well as the one that adds a card to the
selection. Both used to leave a blue smear across the ui that was never what
anyone meant.

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

A card is **three collapsible parts**: config, stats and logs, each with a
heading that toggles it. Config and stats start open and the log starts shut —
it is the expensive one and the one you go looking for. Which parts are open is
a property of the browser tab, like maximizing a card and unlike arranging one:
it isn't written to the layout file and doesn't survive a reload, because it is
a way of looking at a card rather than a change to the graph.

**A shut section is not fed.** The body is unmounted rather than hidden, so a
shut log is not a two-hundred row list with `display: none` on it, and the feed
stops writing to it — which is the point, since a canvas of nine cards is nine
of everything. What each part does about it differs by what it would otherwise
get wrong. A shut log still takes the counters and none of the rows, so opening
one onto a busy pipeline reads what that pipeline is doing rather than climbing
from zero over ten seconds. A shut chart takes nothing at all and is emptied on
the way down: a bar is a fact about a moment, a gap in a bar chart reads as an
idle pipeline, and there is no honest way to draw the moments nobody was
watching. So the chart always says "since you opened this".

The **stats** part is a rolling bar chart of the pipeline's throughput: one pair
of bars per time unit, messages in against messages out, newest on the right and
sliding left as time passes. The unit is `5s`, `1m` or `5m` and the window is
thirty of them — two and a half minutes, half an hour, two and a half hours.
Changing it starts the chart again, because minutes can't be cut back out of
seconds. The one number on it is the tallest bar in the window, which is also
the scale the bars are drawn against; there is no axis, since a grid behind
thirty bar pairs on a 360px card is noise.

Two things it counts deliberately. **Out is summed over every output** — each
output gets every batch, so a pipeline with two of them shows twice as much
leaving as arriving, and one of them dying is visible as the gap it is. And a
bar counts what the *feed skipped* as well as what it carried: `/events` is
sampled under load and every batch says how many passes were dropped to reach
it, so counting only what arrives would draw the sampling rate rather than the
pipeline. The counting is the browser's, though — it is fed by the same event
stream the log is, so nothing is recorded while the tab is closed and none of it
survives a reload. `frontend/src/stats.rs` is the bucketing and the bar
geometry, pure and unit tested like `log.rs`; the chart itself is two `<path>`
elements in a fixed viewBox, which is what makes it cheap enough to redraw on
every card once a second.

The **config** part is a tabbed property list — inputs / transforms / outputs —
and the **logs** part is the live message log. The log carries failures as well
as messages: a `UiEvent` is either a `batch` or an `error`, and an error is
logged in red as
`<stage> error: <cause>` on the card of the pipeline it happened in. That covers
the three places the run loop tolerates a failure — a transform that threw, an
output that couldn't emit, an input that died — and it's the same text the
server log shows, so a card no longer just goes quiet for reasons only visible
in the terminal. `frontend/src/log.rs` turns an event into log lines and is unit
tested; `frontend/src/inspector.rs` builds those rows
from `serde_json::Value` rather than by matching on the config enums, so a new
component kind or a new field shows up without touching the frontend; the row
names are the wire names.

The bar above the log acts on the log and nothing else. The `in` / `out` / `err`
chips filter it, `flat` / `grouped` swaps between an event per row and a batch's
whole journey per row, and three buttons do what they say: **pause** stops
keeping new events so a moving log can be read, **copy** puts the rows on the
clipboard as tab-separated `time · stage · text`, and **clear** empties it.

A row is one **batch**, summarised to a single line — and the arrow that appears
at its left edge on hover opens that batch out: every message the feed carried,
laid out and coloured, with a copy button of its own on the box and another on
each message. A row that is being read must not scroll away, so **opening one
pauses the log**; collapsing it leaves the log paused, since the pause button is
where a stopped log is resumed. What the box can show is bounded by what the
feed carries (`kayak_core::MESSAGES_PER_BATCH`, and each message cut to
`MAX_MESSAGE_BYTES`), and it says so at the bottom when the batch was wider than
that. A message the cut left as invalid JSON — or an error's text, which gets
the same treatment, since a row truncates it and there is nowhere else to read
it — is shown as it stands rather than not at all.

`frontend/src/pretty.rs` is the pure half, unit tested like `log.rs`. It
**re-indents the text rather than parsing it into a `Value` and printing that
back**, which is worth knowing before touching it: `serde_json::Map` is a
`BTreeMap` here, so a round trip would silently sort every payload's keys, and
re-serializing a number loses the digits the source wrote. The laying out
happens when a box opens and never on the path of an ordinary log update — the
same rule the rest of the feed follows.

Two details there are deliberate. Pausing stops the *log*, not the pipeline, so
the throughput readout and the error badge go on counting while it is paused —
its tooltip says how many events went past — and resuming jumps back to the
newest line rather than leaving the reader stranded mid-history. And a wheel
over a log that has somewhere to scroll scrolls it instead of zooming the
canvas; over one with nothing to scroll it falls through and zooms, since a
pane that doesn't scroll shouldn't swallow the gesture.

All the geometry — layout, edge paths, zoom anchoring, the camera glide — lives
in `frontend/src/graph.rs` as pure functions with unit tests, and the same goes
for the inspector rows. Keep it that way: the Leptos components should only feed
those functions and render the result, since anything inside a component can't
be tested without a browser.
