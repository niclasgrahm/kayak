# kayak — visual language

Everything here is taken from the running product (`style/main.scss`,
`frontend/src/`, and the screenshots in `screenshots/`). It describes what
kayak *looks like today* so a landing page can look like the same thing.

---

## 1. The one-sentence brief

kayak looks like a **piece of engineering software, not a SaaS marketing site**:
a dark, dense, grid-aligned editor canvas in the tradition of a game engine or a
node editor (Godot's GraphEdit is the acknowledged reference), where monospace
carries all the data and the only saturated colour on screen is data itself.

If the landing page reads as "developer tool with a real UI behind it", it is
right. If it reads as gradient-hero-with-rounded-cards, it is wrong.

---

## 2. Palette

These are the actual CSS custom properties. Use them verbatim; don't
re-harmonise them.

### Surfaces (dark, low-contrast, four steps)

| token | hex | used for |
| --- | --- | --- |
| `--bg-canvas` | `#1d2129` | the canvas itself — the darkest large field |
| `--bg-panel` | `#262b33` | docks, sidebars, card bodies |
| `--bg-titlebar` | `#1b1f26` | card title bars, the navbar |
| `--bg-hover` | `#2f3540` | hover / raised rows, input fields |
| `--border` | `#14171c` | **darker than the fills it separates** |

The border rule is the single most characteristic trait: borders are *darker*
than the surfaces on both sides, like a seam or a groove, never a lighter
hairline. Panels read as inset, not as floating cards. There are no drop
shadows on the canvas.

### Text

| token | hex | used for |
| --- | --- | --- |
| `--text` | `#cdced2` | body text, card titles — a warm off-white, never `#fff` |
| `--text-dim` | `#85878c` | labels, timestamps, section headings, inactive tabs, edges |

Roughly 70% of all text on screen is `--text-dim`. The interface whispers so the
data can speak.

### Accent & signal

| token | hex | meaning |
| --- | --- | --- |
| `--accent` | `#699ce8` | selection, focus, live edges, the one blue |
| `--error` | `#e06c75` | failures — text, the error strip on a chart |
| `--error-bg` | `#3a2226` | a barely-there tint on an error row in a log |
| `--stat-in` | `#699ce8` | messages arriving (chart series 1) |
| `--stat-out` | `#d8a657` | messages leaving (chart series 2) |

There is **one** accent hue. Blue/amber was chosen over blue/green for the two
chart series specifically because they are read side by side at 3px wide and
must survive the common colour-blindnesses. Keep that pair if the landing page
draws any chart.

### JSON syntax colours (muted, four)

| token | hex | |
| --- | --- | --- |
| `--json-key` | `#7fbbb3` | keys — the brightest, because a payload is scanned down its keys |
| `--json-str` | `#a7c080` | strings |
| `--json-num` | `#d8a657` | numbers |
| `--json-literal` | `#d699b6` | `true` / `false` / `null` |

Deliberately muted, not a rainbow: the box sits inside a card on a dark canvas,
and the job is to tell a key from a value at a glance. This is the palette to
use for any code sample on the landing page.

### There is no light theme

kayak is dark-only, and that is a product fact, not an oversight. A landing page
may be light if it wants — but every screenshot and every embedded UI element in
it is dark, so a light page has to be built to hold dark rectangles well.

---

## 3. Typography

Two families, and the split is meaningful:

- **`"Noto Sans", "Open Sans", system-ui, sans-serif`** — labels, prose,
  buttons, headings. Everything the *interface* says.
- **`"JetBrains Mono", ui-monospace, monospace`** — every value, every id,
  every payload, timestamps, the zoom percentage, connection strings, field
  values. Everything the *system* says.

That distinction is worth reproducing on the landing page: prose in the sans,
and every pipeline id, component name (`nats`, `reducer`, `clickhouse`), config
key and JSON snippet in the mono.

**Sizes are small and there are few of them.** Base is `13px`; the scale in use
is 14 / 13 / 12 / 11 / 10 / 9 px. Nothing in the app is larger than 14px. A
landing page will obviously need display sizes, but the UI chrome it borrows
should stay small and tight — the density *is* the aesthetic.

**Section labels are the signature type treatment**: `10–11px`, `uppercase`,
`letter-spacing: 0.04–0.08em`, in `--text-dim`. That is how `CONFIG`, `STATS`,
`LOGS`, `PIPELINES`, `BUCKETS`, `CONNECTIONS` and the connection-kind chips
(`KAFKA`, `NATS`, `S3`) are set. It is the cheapest way to make a block of page
read as kayak.

**Everything is lowercase.** The product name is `kayak`, never `Kayak`.
Buttons say `edit`, `save as…`, `revert`, `sign in`, `cancel`, `create`,
`pause`, `copy`, `clear`. Tabs say `canvas`, `docs`, `pipelines`,
`connections`, `state`, `components`, `http api`. Headings in the guide are
lowercase too. Only the uppercase section labels above break it, and they break
it completely. Sentence-case Title Case is off-brand.

---

## 4. Geometry & spacing

- **`--radius: 4px`.** One radius, everywhere. Nothing is pill-shaped, nothing
  is a large rounded card.
- **A 20px grid** (`graph::GRID`) is the unit for the whole canvas: the
  background dot/line grid, card positions, card sizes, port positions and the
  channels edges run along. A card is **18 cells = 360px** wide.
- Padding is tight — `4px 10px` on the navbar, a few px on rows. Rows in a
  config table are ~19px tall.
- The canvas background is a **1px grid at ~4.5% white** on `--bg-canvas`,
  20×20px, scaled with zoom. This is a strong, reusable motif: a faint grid
  behind a hero is instantly "kayak's canvas".

---

## 5. The shapes that make up the UI

Worth knowing, because they are what a landing-page illustration should be made
of:

**The card.** One pipeline. A title bar (`--bg-titlebar`, the id in `--text`,
bold-ish, with a `⤢` maximize glyph at the right), then three collapsible
sections, each headed by a `▾`/`▸` and an uppercase label:

- `CONFIG` — three tabs (`inputs (1)` / `transforms (0)` / `outputs (1)`), an
  uppercase kind chip (`NATS`), then two-column name/value rows: the name in
  dim sans on the left, the value in mono inside a slightly raised inset box on
  the right.
- `STATS` — a legend (`■ in ■ out ■ err`), three bar-width chips (`5s` `1m`
  `5m`), and the chart: **30 slots, two thin bars per slot** (blue in, amber
  out) with a thin **red strip beneath** for failures on its own scale.
  Gridlines are dashed at 1px; axis numbers sit at the right in tiny mono.
  Below it, when there are any, `failures on record` — a timestamp, `4m ago`,
  the stage (`transform map`), the error text in `--error`, and a `×13` tally
  at the far right.
- `LOGS` — filter chips (`in` `out` `err`), a rate readout (`2/s`), then
  `flat` `pause` `copy` `clear`, then dense mono rows: `13:28:43.117  IN
  {"_meta":{"connection":"local-nats"…` — one batch per line, truncated with a
  `…`. A `▸` on the left opens the row into a pretty-printed, syntax-coloured
  JSON box.

**The edge.** 2px, `--text-dim`, **orthogonal only** — never a bezier, never a
diagonal. It leaves a card's face, runs along a grid line to a channel between
the rows, and turns in, with small rounded corners. Vertical wins whenever
there is room, because the graph is a flow and down the page is what the flow
means. When a batch crosses it, it turns `--accent` and thickens to 3px in
120ms, then fades back over 700ms — **a busy graph glows rather than strobes**.
(Off entirely under `prefers-reduced-motion`.)

**The sidebar.** ~218px, `--bg-panel`, a tab strip at the top
(`pipelines` / `connections` / `state`), an uppercase header row with a `+`, a
search box, then a flat list of ids in sans; a selected row gets a
`--bg-hover` fill and a subtle accent edge. Connection rows carry a right-
aligned uppercase kind chip.

**The navbar.** 12px, `--bg-titlebar`, one border-bottom. `kayak` at the far
left, then `canvas` / `docs`. At the right: `edit`, the username, `sign out`,
and the zoom percentage in mono, right-aligned in a fixed 4ch box so it doesn't
twitch while zooming. In edit mode the left of that group becomes
`auto layout` · `revert` · `save as…` · `editing`.

---

## 6. Motion

Very little, all of it meaningful:

- Edge pulse: 120ms up, 700ms fade out. The only "alive" animation.
- The camera **glides** to a pipeline when you click its name in the sidebar,
  it doesn't cut.
- Charts redraw once a second; the log delivers at most once per animation
  frame.
- No hover lifts, no shadows appearing, no parallax, no entrance animations.

The whole UI is honest about being a *view onto a running process*: the motion
that exists is data moving, not decoration.

---

## 7. Voice in the interface

Terse, lowercase, factual, occasionally blunt. Real strings from the app:

- `starts running now — save the config to keep it`
- `waiting for messages…`
- `failures on record`
- `optional — generated if left blank`
- `highest in this window: 5`
- `five seconds a bar — the last two and a half minutes`
- `unsaved changes`

Note the em dashes, the lowercase, and that every one of them says a *fact*
about the system rather than encouraging the user. Marketing copy for kayak
should sound like these strings written a little longer — never like a hero
tagline with exclamation marks. There is no mascot, no illustration style, and
no emoji anywhere in the product.

---

## 8. Doing and not doing

**Do**

- Dark surfaces, dark seams, one blue.
- Mono for every value, id and component name.
- Uppercase micro-labels with letter-spacing for section headings.
- The 20px grid as a background texture.
- Orthogonal, right-angled connectors if you draw a graph.
- Real screenshots, dense and full of text, shown large enough to read.
- 4px corners.

**Don't**

- Gradients (there is not one in the product), glows, glassmorphism, drop
  shadows.
- A second accent hue, or recolouring the blue/amber chart pair.
- Rounded 12–16px cards, pill buttons, big airy padding.
- Title Case, ALL-CAPS headlines, or capitalising "Kayak".
- Curved/bezier connectors between nodes — kayak's edges are square.
- Stock photography, 3D renders, abstract blobs, isometric illustrations.
- Light-mode screenshots (they don't exist).

---

## 9. Assets in this folder

`screenshots/` — all captured at 1600×1000 CSS px, 2× device scale, from a
server running `example_config/` with live NATS / Kafka / MQTT / Redis /
Postgres / ClickHouse / S3 traffic:

| file | what it shows |
| --- | --- |
| `01-canvas-overview.png` | the whole graph zoomed out — one root fanning out to six children and four failing ones. The DAG shape. |
| `02-canvas-fanout.png` | mid-zoom, `sensors` selected, edges fanning to its children, live charts on every card |
| `03-card-detail.png` | a single card, close, at 100% — config / stats / logs |
| `04-log-expanded.png` | a maximized card with a log row opened into pretty-printed, colour-coded JSON |
| `05-failure-history.png` | the failure story: an error strip in bursts, plus `failures on record` with a `×13` tally |
| `06-connections.png` | the connections tab with a connection inspector, over three failing cards |
| `07-state-buckets.png` | the state tab: a bucket's live contents, key by key |
| `08-add-pipeline.png` | the add-pipeline modal — a form generated from the component schema |
| `09-login.png` | the sign-in page (very minimal; good for a "runs with auth" note) |
| `10-docs-components.png` | the generated component reference at `/docs` |
| `11-docs-http-api.png` | the generated HTTP API reference |

Best hero candidates: `02-canvas-fanout.png` (alive, legible, clearly a graph)
or `01-canvas-overview.png` (shape of the whole thing). `04-log-expanded.png`
and `05-failure-history.png` are the two that best carry a feature section.
