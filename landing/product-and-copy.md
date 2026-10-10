# kayak: the positioning brief

This file is source material for the landing page, the readme and the doc
site. Sections 1 to 8 are facts about the product. Section 9 is the structure
of the landing page.

Write all copy by the rules in [`copy-style.md`](copy-style.md). That file is
binding. This file says what is true. `copy-style.md` says how to write it.

kayak is **pre-1.0, one binary, with no hosted service, no signup and no
pricing**. The page is a project page. The only call to action is "run it".

---

## 1. The product in one paragraph

kayak is a stream processor. It is one Rust binary. You write pipelines as
`inputs → transforms → outputs` in a JSON or YAML file, and you keep that file
in version control. You run the container image
(`ghcr.io/niclasgrahm/kayak`) with that file. That is the complete deployment.
Users of Benthos / Redpanda Connect, Vector or Fluent Bit know this model.

## 2. The problem

A team with real streams has two usual options:

- **Write the consumer.** A client library, a loop, the transforms and a
  database write. The team then also owns the retries, the batches, the
  backpressure and the metrics.
- **Run a platform.** Flink or Spark Streaming. These need a cluster, and the
  cost of the cluster is often larger than the cost of the problem.

kayak is a third option. You describe the pipelines in a config file. One
process runs them. The runtime is fast, so the systems around kayak set the
throughput.

## 3. How it works

- **A pipeline** is `inputs → [transforms] → outputs`. All three are lists.
  kayak merges the inputs into one stream. Each output gets each batch.
- **A graph of pipelines.** The `pipeline` input reads the output of another
  pipeline. You can make fan-out, fan-in and chains of any depth.
- **Plain JSON.** There is no schema to declare. Transforms address fields by
  name or by dotted path (`sensor.id`, `_meta.subject`).
- **Connections.** You declare a broker, a database or a bucket one time, with
  a name, in `config.connections.yaml`. A component names the connection and
  adds only what it wants from that system (a topic, a subject, a table).
- **Secrets.** Credentials are `${NAME}` references. kayak resolves them from
  the environment first and then from the `--secrets` file.
- **State buckets.** Named, bounded buckets, declared in the config file and
  shared between pipelines. `remember` writes, `recall` reads.
- **Metadata as fields.** An input can add what it knows about a message (the
  subject, the topic, the offset) as ordinary JSON fields. Each transform can
  then use them.
- **The config file is the source.** A typical deployment does not change the
  file at runtime. You change the file, review the diff and deploy again.

### The typical workflow

1. Write `config.yaml` and `config.connections.yaml`.
2. Commit them.
3. Run `docker run -v "$PWD:/kayak" ghcr.io/niclasgrahm/kayak --config /kayak/config.yaml`,
   or deploy the same image to Kubernetes with the config in a ConfigMap.
4. Change the file, review the diff, deploy again.

## 4. Inventory

Verified against `InputKind`, `TransformKind` and `OutputKind` in
`kayak-core/src/config.rs` and `ConnectionKind` in
`kayak-core/src/connections.rs`. Check these lists again when you add a
component.

| | |
| --- | --- |
| **inputs** (12) | `nats` · `kafka` · `mqtt` · `redis` · `opcua` · `http` · `http_poll` · `postgres` · `clickhouse` · `indu` · `pipeline` · `dummy` |
| **transforms** (18) | `filter` · `map` · `reducer` · `splitter` · `buffer` · `remember` · `recall` · `http` · `script` · `deadband` · `throttle` · `pivot` · `derive` · `rolling` · `smooth` · `detect` · `resample` · `features` |
| **outputs** (12) | `postgres` · `clickhouse` · `s3` · `file` · `kafka` · `nats` · `mqtt` · `redis` · `http` · `indu` · `tidepool` · `stdout` |
| **connections** (11) | `kafka` · `nats` · `mqtt` · `redis` · `postgres` · `clickhouse` · `s3` · `file` · `opcua` · `indu` · `tidepool` |

Also in the product:

- An input **buffer** by count, by time window, or by the first of the two.
- `max_batch` on the broker inputs.
- **Acknowledgement modes** (`ack: on_receipt`, `ack: on_delivery`). With
  `on_delivery`, a `kafka` or `mqtt` input acknowledges a message after the
  outputs of the pipeline return.
- **Column mapping** for the `postgres` and `clickhouse` outputs (and a check
  of the table for `tidepool`).
- **Rotation** for the `file` and `s3` outputs, by row count or by time.
- **The `opcua` input**: a monitored item per node, one message for each value
  change.
- **The `http` input**: an ingest endpoint per pipeline, with an optional
  bearer or header credential.
- **Authentication** with two roles (`admin`, `read`). Accounts come from the
  server config (`basic`) or from an identity provider (`jwt`). It is off
  without `--server-config`.
- **History**: by default one day of counters and failure records per
  pipeline, in memory.
- **A generated reference** at `/docs` and `/api/docs`, and an **OpenAPI 3.1**
  document at `/api/openapi.json`.
- **A web UI.** Optional. See section 7.

## 5. Facts to quote

Do not make these numbers larger. Do not round them up.

**Performance.** From `bench/baselines/` (Apple M1 Max, 10 cores, release
build, in process, no network, no disk):

| scenario | result |
| --- | --- |
| one pipeline, no transforms | about 7 million passes per second |
| one pipeline, batches of 100, one `filter` | about 31 million messages per second |
| 1000 pipelines at the same time, batches of 100 | about 5.6 billion messages per second, 14 MiB resident |
| one pipeline at rest | about 9 MiB resident |

Always say that the numbers exclude I/O. They show that the runtime is not
the bottleneck. They are not end-to-end throughput.

Other performance facts:

- No garbage collector.
- Fan-out shares a message (`Arc`). It does not copy it.
- `max_batch` and `buffer` share the cost per batch between many messages.
- The `clickhouse` output writes one insert per batch.
- A server with no browser attached does no work for the web UI.

**Deployment.**

- One Rust binary. The frontend is compiled into it. No database of its own,
  no agent, no sidecar.
- The image `ghcr.io/niclasgrahm/kayak` is for `linux/amd64` and
  `linux/arm64`. It runs as uid 10001 and contains no config.
- The default port is 6767.
- The flags: `--config`, `--connections`, `--secrets`, `--data-dir`,
  `--server-config`, `--listen`, `--debug`.

**Quality.** Clippy pedantic with `-D warnings`. New behaviour needs a test.
The sample config must contain each component kind, or the tests fail.

## 6. Who it is for

- Backend and data engineers who want pipelines in a config file in git.
- Small teams with real streams and no platform team.
- IoT, telemetry and event plumbing: brokers and OPC UA in, databases and
  object stores out.
- Users of Benthos / Redpanda Connect, Vector or Fluent Bit.

## 7. The web UI

The web UI is a convenience. It is not the product.

- Do not lead with it. Do not make it the headline, the first section or the
  main image.
- Mention it one time on the landing page, near the end: "The server also has
  a web UI. Use it to look at the running graph and the messages in each
  pipeline."
- The screenshots in `screenshots/` show the UI. Use them only in the web UI
  section of the guide.

## 8. What kayak does not do

Say this on the page. It helps the reader to make a correct decision.

- **One process.** No cluster, no distributed state, no exactly-once delivery
  across machines.
- **State and history are in memory.** A restart clears them.
- **No table migrations.** The `postgres` and `clickhouse` outputs create a
  table if it does not exist. They do not change an existing table.
- **No expression language.** `map` does one operation per mapping. Use
  `script` for more.
- **Pre-1.0.** The config format can change between minor versions.

## 9. Names and spellings

- The name is **kayak**, lowercase, also at the start of a sentence.
- Component names and field names are lowercase, in backticks: `nats`,
  `reducer`, `max_batch`.
- Headings are lowercase. Body text uses sentence capitalization.
- The sample sign-in is `niclas` / `hunter2` (admin) and `viewer` / `hunter2`
  (read-only). It is a committed example credential.
- Built with Rust, Axum, Tokio and Leptos.
- Crates: `kayak-core` (shared types, compiles to wasm), the root crate
  (server and runtime), `frontend` (Leptos), `kayak-bench` (throughput
  harness), `docsgen` (the doc site reference).

## 10. Page structure

The landing page is `website/.vitepress/theme/Landing.vue`.

1. **Hero.** What kayak is (one binary, config in git, run the container). A
   config snippet. The `docker run` command as the call to action.
2. **Performance.** The four bench numbers and the I/O caveat. The other
   performance facts as a short list.
3. **Composability.** A diagram of one pipeline that feeds three pipelines. The
   list from section 3. A pipeline fed by a pipeline, and a connections file.
4. **Feature completeness.** The inventory table from section 4, and a grid of
   the other features.
5. **Operation.** The `docker run` command, `server.yaml`, the flags, and the
   HTTP API (ingest, history, OpenAPI).
6. **What kayak does not do.** Section 8, in four boxes.
7. **Web UI.** One short paragraph, with a link to the guide.
8. **Footer.** The one-command try, and links to github, getting started, the
   reference and deployment.

Keep the page dark, dense and legible. Use the visual language in
[`visual-language.md`](visual-language.md).
