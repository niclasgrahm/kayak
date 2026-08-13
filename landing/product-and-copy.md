# kayak — what it is, and the copy for the landing page

Source material for a landing page. Section 1–5 are facts about the product;
section 6 onward is ready-to-use copy, in kayak's voice, that can be lifted or
rewritten.

Status note for whoever writes the page: kayak is **pre-1.0, a single binary, no
hosted service, no signup, no pricing**. The page is a project page, not a SaaS
funnel. The only call to action is "run it". (There is no LICENSE file in the
repo yet — settle that before the page names a licence.)

---

## 1. The product in one paragraph

**kayak is a graph-based stream processor with a live canvas.** You describe
pipelines — `inputs → transforms → outputs` — in a JSON or YAML file. kayak
runs them, and a web canvas shows the graph *while it is running*: a card per
pipeline, an edge for every hand-off between them, and on each card its config,
a throughput chart, and a live log of the actual messages going through. You can
edit the graph from that same screen, and every button on it is an HTTP endpoint
you could have called yourself.

## 2. The problem it addresses

Stream processing today is roughly two things:

- **Write the consumer yourself.** A Kafka client, a loop, some transforms, a
  database write. Complete control; now you own retries, batching, backpressure,
  metrics and a dashboard you have to build separately to find out what it's
  doing.
- **Adopt a platform.** Flink, Spark Streaming, Benthos/Redpanda Connect and
  friends. Powerful, and they hide the running graph behind logs, YAML and
  whatever observability you wire up around them. When something goes wrong at
  02:14, you go and read logs.

kayak aims at the gap between those. Pipelines are **described, not coded**, and
the running graph is a **thing you can look at**, not a thing you infer from
metrics. It is built for small-to-medium data-wrangling jobs where seeing what's
happening is most of the value: moving sensor readings into Postgres, fanning a
topic out to four destinations, aggregating a stream into rollups, catching a
webhook and archiving it to S3.

## 3. How it actually works

**A pipeline** is `inputs → [transforms] → outputs`. All three are arrays. Every
input is merged into one stream; every output receives every batch. So "archive
to Postgres *and* watch it on stdout" is one pipeline with two outputs, not two
pipelines.

**A graph, not a list.** One pipeline's `pipeline` input subscribes to another's
output, so pipelines feed pipelines and the whole thing is a DAG. That's what
the canvas draws: cards laid out top-to-bottom by depth, with orthogonal edges
that light up as batches cross them.

**Messages are plain JSON, all the way through.** No schema to declare, no
registry, no code generation. Transforms address fields by name — including
dotted paths like `_meta.subject` — and a field either exists or it doesn't.

**Everything the systems need is declared once.** Brokers, URLs and credentials
live in a `connections` file under a name; a component names the connection and
adds only what *it* wants from that system (topic, subject, table, prefix).
Credentials are `${ENV_VAR}` references resolved at build time and never
serialised back out.

**The config file is a load source and a save target, never a mirror.** Creating
a pipeline from the UI starts it immediately and writes nothing; the navbar says
`unsaved changes` until you save, and `revert` reloads the file. The saved file
is deterministic and meant to be committed.

### The inventory (as of today)

| | |
| --- | --- |
| **inputs** | `nats` · `kafka` · `mqtt` · `redis` · `http` (things push *into* kayak) · `pipeline` (another pipeline's output) · `dummy` (a generated heartbeat) |
| **transforms** | `filter` · `reducer` (aggregate + group by) · `map` (reshape, cast, concat, arithmetic) · `splitter` · `buffer` · `remember` / `recall` (state across messages) · `http` (call a service) |
| **outputs** | `postgres` · `clickhouse` · `s3` (and S3-compatible) · `file` · `kafka` · `nats` · `mqtt` · `redis` · `http` (webhook) · `stdout` |
| **connections** | `kafka` · `nats` · `mqtt` · `redis` · `postgres` · `clickhouse` · `s3` · `file` |

Also in the box: message **metadata** (attach the subject/topic/partition/offset
an input knows to the message itself), **buffering** by count, by time window or
either-first, **acknowledgement modes** (`on_receipt` / `on_delivery`, honoured
by Kafka and MQTT), **column mapping** for the database outputs, **state
buckets** shared across pipelines, **history** (a day of per-pipeline counters
and failure signatures, in memory), and **authentication** with two roles
(admin / read-only), off by default.

### The parts you don't have to build

- A **generated reference** at `/docs` — every component, every field, every
  type, written from the config structs themselves, so it cannot drift.
- A **generated HTTP API reference**, plus OpenAPI 3.1 at `/api/openapi.json` —
  generated from the same table the server builds its routes from, so it
  describes the server you are actually talking to.
- **History on every card**: a throughput chart, a failure strip on its own
  scale, and `failures on record` with first-seen / last-seen / count — so a
  pipeline that broke at 02:14 has something to show at 08:00, with nobody
  subscribed overnight.

## 4. Who it's for

- Backend and data engineers who want a pipeline in a config file and in git,
  not a job in a cluster.
- Small teams with real streams and no platform team.
- Anyone debugging a stream who is tired of `console.log`-ing a consumer to find
  out what shape the messages are.
- IoT / telemetry / event plumbing: brokers in, database and object store out.

Explicitly **not** for: exactly-once distributed transactional processing,
petabyte batch jobs, or anything that needs a cluster. kayak is one process.

## 5. Facts worth quoting (accurate, don't inflate)

- **One Rust binary.** Axum server, Leptos (WASM) frontend, no runtime
  dependency, no database of its own, no agent, no sidecar. A container image
  with nothing baked in.
- **Fast enough that the pipeline is never the bottleneck.** On an Apple M1 Max,
  the in-process harness (`just bench`, no network, no disk) measures ~7M
  pipeline passes/sec on one pipeline, ~31M messages/sec through a `filter` in
  100-message batches, and ~5.6 billion messages/sec across 1000 concurrent
  pipelines. These are runtime numbers with I/O excluded — quote them as "the
  runtime isn't the bottleneck", never as end-to-end throughput.
- **The UI costs the server almost nothing.** The event feed is deliberately a
  *sample*: run loops publish at most ~10 passes a second, and only when a
  browser is actually attached. A headless server pays nothing for a UI nobody
  has opened.
- **Tests and lints are strict**: clippy pedantic with `-D warnings`, tests
  required for new behaviour, and a sample config that fails the build if a new
  component isn't added to it.

---

## 6. Copy: headline options

Voice rules: lowercase, terse, factual, em dashes welcome, no exclamation marks,
no "unleash/supercharge/effortless". Say what it does.

**Primary (recommended)**

> ## stream processing you can watch
> kayak runs `inputs → transforms → outputs` pipelines from a config file and
> draws the whole graph while it's running — every card live, every edge
> lighting up as batches cross it.

**Alternatives**

> ## your pipelines, on a canvas, running
> Describe them in JSON or YAML. kayak runs them and shows you.

> ## the graph is the interface
> Not a dashboard beside your stream processor. The running graph itself.

> ## config in, canvas out
> nats, kafka, mqtt, http in. postgres, clickhouse, s3, files out. All of it
> visible while it runs.

**Sub-headline / support line**

> A single Rust binary. Pipelines in a file you can commit, a live canvas you
> can edit them from, and an HTTP API behind every button.

**Primary CTA**: `just dev` — with the three lines that actually work:

```bash
git clone …/kayak && cd kayak
docker compose up -d      # optional: nats, kafka, mqtt, redis, postgres, s3
just dev                  # → localhost:6767
```

Secondary CTA: read the guide / browse the generated component reference.

---

## 7. Copy: feature sections

Each of these pairs with a screenshot from `screenshots/`.

### See the graph, not the logs
*(`02-canvas-fanout.png` or `01-canvas-overview.png`)*

Every pipeline is a card. Every hand-off between pipelines is an edge, and an
edge lights up when a batch crosses it — so a busy graph glows, a stalled one
doesn't. Cards lay themselves out top-to-bottom by depth until you drag one
somewhere else; then that one stays put and everything else carries on being
placed for you. Pan, zoom, and the arrangement is saved beside your config.

### Every card carries its own log
*(`04-log-expanded.png`)*

A card holds its config, a throughput chart and a live log of the batches going
through it — in, out, errors. Open a row and the payload is pretty-printed and
colour-coded, with the log paused so you can read it. It's the message that
actually went through your pipeline thirty seconds ago, not a sample you set up
in advance.

### Find out what broke at 2am
*(`05-failure-history.png`)*

The live feed answers "what is happening". History answers "what happened".
kayak keeps a day of per-pipeline counters and a record of every distinct
failure — first seen, last seen, how many times — with a failure strip drawn on
its own scale beneath the throughput chart, because three failures next to fifty
thousand messages would otherwise be a bar one pixel high. It costs nothing when
nobody's watching, which is the point: nobody *was* watching at 02:14.

### Pipelines are configuration
*(a code block; use the JSON syntax colours from `visual-language.md`)*

```yaml
- id: sensors_10s_avg
  inputs:
    - type: pipeline
      upstream: sensors
      buffer: { type: tumbling, window_seconds: 10 }
  transforms:
    - type: reducer
      group_by: [sensor, _meta.subject]
      aggregations:
        - { function: avg, as: mean,    field: value }
        - { function: min, as: lowest,  field: value }
        - { function: max, as: highest, field: value }
        - { function: count, as: readings }
  outputs:
    - type: stdout
```

JSON or YAML, your choice — the file decides by its extension. Commit it, diff
it, review it. Editing from the canvas applies immediately and writes nothing
until you save.

### Declare a system once
*(`06-connections.png`)*

Brokers, URLs and credentials live under a name in a connections file; a
component says which connection it uses and only what it wants from it — a
topic, a subject, a table, a bucket prefix. Passwords are `${ENV_VAR}`
references that resolve at startup and are never sent back out to the browser.

### Remember things between messages
*(`07-state-buckets.png`)*

Named, bounded state buckets, declared at the top of the config and shared by
the pipelines that use them: one pipeline remembers the current recipe per
machine, six others stamp it onto their output. `remember` is a tap, `recall`
reads — and the state tab shows you every key in a bucket, live.

### Add a pipeline without leaving the page
*(`08-add-pipeline.png`)*

The add-pipeline form is generated from the same component schemas as the
reference — so every component has working controls, dropdowns and validation
the day it's added, with no form to write. The new pipeline starts running the
moment you hit `create`.

### Documentation that can't drift
*(`10-docs-components.png`, `11-docs-http-api.png`)*

`/docs` is generated by reflecting over the config types: the doc comments on
the structs *are* the reference, and a component with no documentation fails the
test suite. The HTTP API reference and the OpenAPI 3.1 spec come from the same
table the server builds its routes from — an endpoint that isn't in it isn't
routed at all.

### Everything is an endpoint
*(optional; a small code block)*

The canvas is a client. Anything it does, you can do:

```bash
curl localhost:6767/api/pipelines
curl -X POST localhost:6767/api/pipelines/ingest/messages \
     -d '{"sensor":"a","value":1}'
```

The `http` input turns a pipeline into an ingest endpoint at its own id, with
optional per-endpoint bearer or header auth — separate from the server's own
sign-in, because a device posting readings isn't an operator.

---

## 8. Copy: the "why not X" section

Honest, short, no competitor-bashing. Suggested framing:

> **vs. writing a consumer.** You'd get there — and then you'd build batching,
> backpressure, retries, metrics and a page to look at them on. kayak is those
> parts, with your logic as configuration.
>
> **vs. Flink / Spark.** Those are clusters, and they're the right answer at a
> scale where you need one. kayak is a single binary for the jobs below that
> line, where the cost of the platform exceeds the cost of the problem.
>
> **vs. Benthos / Redpanda Connect.** Closest neighbour, and the difference is
> the canvas: kayak's graph is a live view you can edit, not a config you deploy
> and then observe from somewhere else. kayak has fewer components; it shows you
> more about the ones it has.

## 9. What kayak deliberately doesn't do

Worth saying on the page — it builds more trust than another feature bullet:

- **One process.** No clustering, no distributed state, no exactly-once across
  a fleet.
- **No expression language.** `map` reshapes; it doesn't compute. One operation
  per mapping, no nesting, no conditionals — the point where chaining reads
  badly is where a real scripting language is the honest answer, and that
  boundary is meant to be visible.
- **State and history are in memory.** Durable state without checkpointed input
  positions would be worse than none, so it starts at the input, not the store.
- **Tables are created, never altered.** `IF NOT EXISTS` and nothing more;
  migrating a live table from a config file is a much bigger promise.
- **Pre-1.0.** Things move. The roadmap is in the repo.

## 10. Facts, names, spellings

- The name is **kayak**, always lowercase, even at the start of a sentence.
- Component names are lowercase mono: `nats`, `kafka`, `mqtt`, `redis`,
  `reducer`, `clickhouse`, `s3`.
- The default port is **6767**.
- Sign-in for the sample: `niclas` / `hunter2` (admin), `viewer` / `hunter2`
  (read-only). Fine to show in a screenshot; it's a committed example
  credential.
- Built with Rust, Axum, Tokio and Leptos. Frontend is WASM, server-rendered.
- Repository layout: `kayak-core` (shared types, compiles to wasm), the root
  crate (server + runtime), `frontend` (Leptos), `kayak-bench` (throughput
  harness).

## 11. Suggested page structure

1. Hero — headline, sub-headline, the three-line install, and
   `02-canvas-fanout.png` large enough to read.
2. The three-step model — inputs → transforms → outputs, as a small orthogonal
   diagram in kayak's own edge style.
3. Feature sections, alternating text/screenshot, in this order: the canvas ·
   the log · failure history · config as code · connections · state · generated
   docs.
4. The inventory table (inputs / transforms / outputs / connections) — dense,
   mono, one glance.
5. "Why not X" and "what it doesn't do", side by side.
6. Footer CTA: `just dev`, links to the repo, the guide, and `/docs`.

Keep the whole page dark, dense and legible. The product's argument is that you
can *see* the thing running — so the page's job is mostly to get real
screenshots in front of someone at a size where they can read the text in them.
