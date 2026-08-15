# getting started

kayak is a graph-based stream processing engine: you describe pipelines as
`inputs → transforms → outputs` in a config file, kayak runs them, and a live
web canvas shows the graph while it's running.

## what you need

[Rust](https://rustup.rs), [`just`](https://github.com/casey/just) and
[`cargo-leptos`](https://github.com/leptos-rs/cargo-leptos)
(`cargo install cargo-leptos`). Docker is optional — it's needed only for the
nats, kafka, mqtt, opc ua, database and S3 pipelines in the sample graph.

## run it

```bash
just dev
```

That builds the frontend, starts the server on `localhost:6767` against the
worked example in `example_config/`, and creates a secrets file for you on
first run. Sign in as `niclas` / `hunter2` (admin) or `viewer` / `hunter2`
(read-only) — the sample runs with [authentication](/operating/authentication)
on by default, so both sides of the login are there to look at.

To see every pipeline in the sample actually flowing, bring up the systems it
talks to first:

```bash
docker compose up
just dev
```

Without Docker the sample still runs; the pipelines with nothing to talk to
show a connection error on their card, and the dummy-input pipelines
(`heartbeat`, `ingest`) work regardless. [the sample graph](/pipelines/the-sample)
walks through what's in there and why, including the four pipelines that are
deliberately broken.

## once it's up

- the canvas is at `/` — pan and zoom, click a card to open its log
- `/docs` is the same generated reference this site's [reference](/reference/)
  section renders, served by the running server
- push a message straight into the `ingest` pipeline:

```bash
curl -X POST localhost:6767/api/pipelines/ingest/messages \
  -d '{"sensor":"a","value":1}'
```

## a pipeline, whole

The smallest useful config file: read a subject, drop the messages that don't
matter, write what's left to a file.

```json
{
  "pipelines": [
    {
      "id": "warm-sensors",
      "inputs": [
        { "type": "nats", "connection": "local-nats", "subject": "sensors.>" }
      ],
      "transforms": [
        {
          "type": "filter",
          "Numeric": { "field": "value", "operator": "greater_than", "value": 30.0 }
        }
      ],
      "outputs": [
        {
          "type": "file",
          "connection": "local-files",
          "path": "warm",
          "format": "ndjson",
          "rotate": { "max_rows": 10000 }
        }
      ]
    }
  ]
}
```

The systems named there — `local-nats`, `local-files` — are
[connections](/io/connections), declared once in a file beside this one rather
than repeated in every pipeline that uses them. What each component accepts is
in the [reference](/reference/).

## where to go next

| | |
| --- | --- |
| [the canvas](/canvas/the-canvas) | what you're looking at, and how edges are routed |
| [the pipeline model](/pipelines/pipelines) | inputs, transforms, outputs, and how pipelines feed each other |
| [connections](/io/connections) | declaring the systems pipelines talk to |
| [reference](/reference/) | every component and every endpoint, generated |
| [deployment](/operating/deployment) | the container image, and what it deliberately doesn't bake in |
