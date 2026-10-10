# the canvas

kayak has an optional web UI on the same port as the API. Use it to look at the
running graph and at the messages in each pipeline. You do not need the UI to
run kayak. Each action in the UI is also an HTTP endpoint and a change to the
config. See the [http api reference](/reference/api).

The main page is the canvas. It shows each pipeline as a card. A line connects
a pipeline to each pipeline that it feeds through a `pipeline` input.

## layout

kayak puts the cards in rows from top to bottom. A pipeline is one row below
its upstream. A pipeline with several upstreams is one row below the deepest
upstream, so that each line points down.

When you move a card, that card stays where you put it. kayak continues to
place the other cards. See [arranging the canvas](/canvas/arranging-the-canvas).

The lines are horizontal and vertical, on the grid. Lines that share a face of a
card spread out along that face. Lines between the same two rows go on
different grid lines, so that a fan-out does not look like one thick line.

A line flashes when a batch goes across it. The signal is the `input` event of
the downstream pipeline. Thus a pipeline with an input `buffer` flashes once
for each closed window. A pipeline with several upstreams flashes all its
incoming lines. With `prefers-reduced-motion`, the lines do not move.

## the sidebar

The sidebar has three tabs:

- **pipelines**: the list of pipelines. Click a name to move the view to that
  card.
- **connections**: the connections of the server.
- **state**: the state buckets and their contents. This tab only shows the
  buckets. The UI refreshes it once a second while it is open.

The pipelines tab has two views. Select `flat` or `tree` in its header:

- `flat` shows each pipeline one time, sorted by id.
- `tree` shows each pipeline under its upstreams. A pipeline with several
  upstreams is shown in full under the deepest upstream. Under the other
  upstreams, it is a dimmed row with no children and no delete button.

The search box filters the list. In tree mode, a match keeps its ancestors in
the list.

## a card

A card has three sections: **config**, **stats** and **logs**. Click a heading
to open or close its section. Config and stats are open at the start. Logs is
closed. A closed section gets no data from the feed, so it costs nothing.

The UI keeps the open sections, and the maximized card, in the browser tab
only. They do not go into the layout file. A reload resets them.

The button in the title bar maximizes the card to fill the canvas. Click it
again to restore the card.

### config

The config section shows the inputs, the transforms and the outputs on three
tabs. When a tab has more than one component, each component is one line at the
start. Click a heading to open its settings. The transforms tab has a row of
chips, one for each step. Click a chip to open that step.

A maximized card shows the three stages side by side.

Select `yaml` or `json` on the heading to see the config as text. The text
comes from `GET /api/pipelines/{id}/config`. It is the config that the pipeline
runs, in the format of a saved file. The `{ }` button beside a component opens
the text at that component.

For a `script` transform, the config section shows the script. It is the
script that the pipeline was built with. It is not the file on disk now. The
section shows when the file on disk changed after the build.

### stats

The stats section has a bar chart of the throughput. Each pair of bars is one
time unit: messages in and messages out. The newest bar is at the right.

| unit | window (30 bars) |
| --- | --- |
| `5s` | 2.5 min |
| `1m` | 30 min |
| `5m` | 2.5 h |

A change of the unit starts the chart again. The chart starts with the
[history](/operating/history) of the server, so it is full when it opens.

- **Out** is the sum over all outputs. A pipeline with two outputs shows two
  times as many messages out as in.
- The bars include the messages that the sampled feed did not send. Thus the
  bars show the real rate of the pipeline.
- Failures are on a separate strip with its own scale.

Below the chart, the card lists each distinct failure from the history, with
its time and its count. When there are no failures, the list is not shown.

### logs

The logs section shows the live feed of the pipeline. Each row is one batch at
one stage. A failure is a red row: `<stage> error: <cause>`. It is the same
text as in the server log.

The feed is a sample. The server reports at most about 10 passes per second
for each pipeline. A gap in the log shows passes that the UI did not get.

The bar above the log has these controls:

- `in`, `out` and `err` filter the rows.
- `flat` shows one event per row. `grouped` shows one pass per row.
- **pause** stops new rows. The counters continue.
- **copy** copies the rows as tab-separated text: time, stage, text.
- **clear** removes the rows.

The arrow at the left of a row opens the batch. It shows each message that the
feed carried, formatted, with a copy button. To open a row also pauses the log.
The feed carries a limited number of messages per batch, and cuts long
messages. The box says so when it does not show the complete batch.

## gestures

| gesture | what it does |
| --- | --- |
| wheel or trackpad scroll | zoom at the pointer, from 20% to 250% |
| drag the empty canvas | move the view |
| click a name in the sidebar | move the view to that card |
| click a section heading | open or close that section |
| `5s` / `1m` / `5m` on the chart | change the time unit |
| `▸` at the left of a log row | open the batch and pause the log |
| `edit` in the navbar | show the edit controls |

Over a pane that can scroll, such as the log, the wheel scrolls the pane. On a
maximized card, the wheel always scrolls.

The edit controls are on [editing the graph](/canvas/editing-the-graph) and
[arranging the canvas](/canvas/arranging-the-canvas).

## for contributors

The pure logic of the UI is in modules with unit tests:
`frontend/src/graph.rs` (layout, line routes, zoom), `frontend/src/sidebar.rs`,
`frontend/src/log.rs`, `frontend/src/stats.rs`, `frontend/src/pretty.rs` and
`frontend/src/inspector.rs`. The Leptos components only call these functions
and render the result. Keep new logic in these modules. Code in a component
cannot be tested without a browser.
