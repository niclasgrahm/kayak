# getting started

kayak is a stream processor in one binary. You write pipelines as
`inputs → transforms → outputs` in a config file. You keep the file in version
control and run the container image with it.

## try it

This command starts one pipeline. The pipeline sends a message to stdout each
second:

```bash
docker run --rm -p 6767:6767 --entrypoint sh ghcr.io/niclasgrahm/kayak \
  -c 'echo "[{id: ticker, inputs: [{type: dummy, duration: 1}], outputs: [{type: stdout}]}]" > c.yaml && exec kayak --config c.yaml'
```

`transforms` and `outputs` are optional. An input with no output is also a
complete pipeline.

## run your own config

1. Write a config file:

   ```yaml
   # pipelines/config.yaml
   - id: readings
     inputs:
       - type: dummy
         duration: 1
     outputs:
       - type: stdout
   ```

2. Commit the file.
3. Mount the directory and name the file:

   ```bash
   docker run -p 6767:6767 -v "$PWD/pipelines:/kayak" \
     ghcr.io/niclasgrahm/kayak --config /kayak/config.yaml
   ```

4. To change a pipeline, change the file, review the diff and deploy again.

The image contains the runtime and no config. The `ENTRYPOINT` of the image is
the binary, so the arguments of the container are the flags of the server.
[Deployment](/operating/deployment) tells you how to run kayak in production.

## a complete pipeline

This config reads a NATS subject, drops the readings of 30 or less, and writes
the other readings to a file:

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
          "conditions": [
            { "type": "numeric", "field": "value", "operator": "greater_than", "value": 30.0 }
          ]
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

`local-nats` and `local-files` are [connections](/io/connections). You declare
them one time, in a file beside the config. Each pipeline that uses a system
refers to its connection by name. The [reference](/reference/) gives the fields
of each component.

The `file` output can write only under the directory that `--data-dir` names.
Without that flag, the pipeline does not build.

## the worked example

`example_config/` is the sample graph. It uses each component kind and each
connection kind. To run it, you need a checkout of the repository, because it
names the systems in `docker-compose.yaml` and reads credentials from a secrets
file. You also need these tools:

- [Rust](https://rustup.rs)
- [`just`](https://github.com/casey/just)
- [`cargo-leptos`](https://github.com/leptos-rs/cargo-leptos) (`cargo install cargo-leptos`)

Do these steps:

1. Start the systems that the sample uses:

   ```bash
   docker compose up
   ```

2. Start the server against the sample:

   ```bash
   just dev
   ```

`just dev` builds the server, starts it on `localhost:6767` and creates the
secrets file on the first run. The sample has
[authentication](/operating/authentication) on. Sign in as `niclas` / `hunter2`
(admin) or `viewer` / `hunter2` (read-only).

You can run the sample without `docker compose up`. The pipelines that have no
system to connect to then report a connection error. The `heartbeat` pipeline
(a `dummy` input) and the `ingest` pipeline (an `http` input) work.
[The sample graph](/pipelines/the-sample) describes each pipeline. It also
describes the four `broken_*` pipelines. These pipelines fail when they run, so
the sample has failure records to show.

To send a message to the `ingest` pipeline:

```bash
curl -X POST localhost:6767/api/pipelines/ingest/messages \
  -d '{"sensor":"a","value":1}'
```

The server shows the generated reference at `/docs`. It is the same reference as
the [reference](/reference/) section of this site.

The server also has a web UI at `/`. Use it to look at the running graph and the
messages in each pipeline. It is optional.

## where to go next

| | |
| --- | --- |
| [the pipeline model](/pipelines/pipelines) | inputs, transforms, outputs, and how pipelines feed each other |
| [the config file](/pipelines/the-config-file) | the format of the file, and how to keep it in version control |
| [connections](/io/connections) | how to declare the systems that pipelines use |
| [reference](/reference/) | each component and each endpoint, generated from the code |
| [deployment](/operating/deployment) | the container image, its flags, and Kubernetes |
| [web ui](/canvas/the-canvas) | the optional view of the running graph |
