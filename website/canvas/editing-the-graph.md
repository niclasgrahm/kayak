# editing the graph

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

Four things the field types decide:

- a field with a closed set of values (`sum | avg | min | max`) is a dropdown,
  and starts blank rather than showing the first value it hasn't recorded;
- a field with fields of its own — a file output's `rotate` — is those fields,
  indented under it;
- a field that is a *choice* of shapes — an input's `buffer`, which is `static`
  with a `size`, `tumbling` with a `window_seconds`, or `batch` with both — is
  the choice first and then whichever fields it implies. Pick `tumbling` and the
  `size` box is replaced by a `window_seconds` box; nothing you filled in for the
  other one is sent;
- an enum-shaped component (the `filter` transform) gets a `form` picker for its
  `Numeric` / `String` variants, and its fields follow the choice — the same
  idea one level up.

Between them that is the whole config surface: there is no field anywhere that
has to be filled in as raw JSON, and a test fails if a new one ever is.

Validation is `frontend/src/form.rs`: pure, unit tested, and the same rules
serde applies, so what it accepts the server accepts. It reports every problem
at once rather than one per submit, against the field and the component it
belongs to — two `nats` inputs stay distinguishable. It is not a security
boundary; the server still rejects what it must, and its message (a duplicate
id, an unknown upstream) is shown verbatim in the modal footer.

## seeing the data while you build

Every field reference in a pipeline — a column's `field`, a filter's
comparison, an aggregation's source — is a name you have to already know, and
a config file is a bad place to find out you didn't. So the form can go and
look.

**`fetch messages`**, on any input in the modal, builds that input exactly as a
pipeline would, takes a few real messages from it and shows them in a panel
beside the form. Nothing is created: there is no pipeline afterwards, and
nothing is acknowledged to the broker — a sample has not delivered anything
anywhere.

It stops at **whichever bound it reaches first: five messages, or five
seconds**, and the panel counts while it waits. That is why a source ticking
once a second gives you four — the first message arrives a second after the
input is built, and the fifth would land just as the window closes. A quiet
subject samples empty and says so: none of these inputs can replay what was
published before the sample started.

The whole sample arrives at once, when the request answers. Watching them
trickle in would mean a streaming response, which is a different endpoint
shape — see the roadmap.

**Sampling is not free for every kind of input, and the ones where it isn't say
what they did.** A kafka sample reads under a throwaway consumer group, so it
neither rebalances your pipeline's group nor commits on its behalf — which also
means it starts where the input's `start_at` says rather than where the
pipeline has got to. An mqtt sample connects under a client id of its own,
because a broker disconnects the older client holding one. An input `buffer` is
ignored, since a buffer's job is to make the pipeline wait. Each of those shows
as a note above the messages.

An `http` input cannot be sampled at all and says so: it is posted to rather
than read from. Create the pipeline and post a message to its endpoint.

**The messages then go down the rest of the draft.** The transforms you have
configured are built and run over the sample — through the production
`build()`, so a transform that will not build here would not have built there
either — and the panel shows what each stage handed on. That is per stage and
per batch because that is where the answer usually is: a `splitter` hands on
several batches, a `filter` that matched nothing hands on none, and a `buffer`
hands on nothing at all because it is still holding what it was given. Nothing
is emitted to any output; a dry run that emitted would be a pipeline.

**A whole mapping can be filled in from it.** A list whose rows map a message
field onto something — a database output's `columns` — grows a `fill from
sample` button beside `+ add` once something has been sampled. It adds a row
per field the sample carried: the path in the field box, a name made from it
(the whole path, since `sensor.id` and `device.id` must not become one column),
and the type the sample suggests.

Three things it deliberately will not decide for you. A field the sample
disagreed about — a number in one message, a string in the next — gets its row
with the **type left blank**, because there is no honest suggestion and an
unanswered required box is what that should look like. **Nullability is never
guessed**: five messages cannot prove a field is always there, and a column
declared `NOT NULL` on that evidence is a pipeline that fails at three in the
morning, so every filled row is nullable and you tighten the ones you know
about. And it **appends**, skipping fields already mapped, so pressing it twice
adds nothing and a row you have edited is never overwritten.

**What it learns fills in the field boxes.** Every box that names a field of the
messages offers what the sample carried, with the type and an example value —
and offers it *as of that point in the chain*, so an output's column mapping is
suggested the fields that will actually reach it rather than the ones the input
produced. They are suggestions and never a closed list: a sample is a handful
of messages, so a field that only appears when something breaks is still a
field you can type.

**And it feeds the script editor.** A [`script`](/pipelines/scripting)
transform is the one component whose configuration cannot be checked by looking
at it, so what the sample reaches it with is put straight into the editor's
messages box and the script is run over it as you type. See
[writing one in the ui](/pipelines/scripting#writing-one-in-the-ui).

Both halves are ordinary endpoints — `POST /api/inputs/sample` and
`POST /api/pipelines/dry-run` — so the same thing is available to anything
else that wants it.

## the config file

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
