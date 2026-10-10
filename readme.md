# kayak

**kayak** is a stream processor. It is one Rust binary. You write pipelines as
`inputs → transforms → outputs` in a config file, and you keep that file in
version control. You run the container image with that file. That is the
complete deployment.

Users of Benthos / Redpanda Connect, Vector or Fluent Bit know this model.
kayak works the same way.

## try it

This command starts one pipeline. The pipeline sends a message to stdout each
second:

```bash
docker run --rm -p 6767:6767 --entrypoint sh ghcr.io/niclasgrahm/kayak \
  -c 'echo "[{id: ticker, inputs: [{type: dummy, duration: 1}], outputs: [{type: stdout}]}]" > c.yaml && exec kayak --config c.yaml'
```

`transforms` and `outputs` are optional. An input with no output is also a
complete pipeline:

```bash
docker run --rm -p 6767:6767 --entrypoint sh ghcr.io/niclasgrahm/kayak \
  -c 'echo "[{id: ticker, inputs: [{type: dummy, duration: 1}]}]" > c.yaml && exec kayak --config c.yaml'
```

## run your own pipelines

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

JSON works in all places where YAML works. The file extension sets the format.

When you replace the dummy with a real source, declare the system as a
**connection**. A connection is a broker, a database or a bucket with a name.
kayak reads connections from a file beside the config
(`config.connections.yaml`). Each component refers to a connection by name.
Credentials are `${NAME}` references. kayak resolves them from the environment
or from a secrets file. See
[connections](https://propell.dev/kayak/io/connections).

## why kayak

**Performance.** kayak has no garbage collector. The `just bench` harness
measures the cost of the run loop. On an Apple M1 Max, in process, the harness
measured these numbers:

| scenario | result |
| --- | --- |
| one pipeline, no transforms | about 7 million passes per second |
| one pipeline, batches of 100, one `filter` | about 31 million messages per second |
| 1000 pipelines at the same time, batches of 100 | about 5.6 billion messages per second, 14 MiB resident |
| one pipeline | about 9 MiB resident |

The numbers exclude I/O (no network, no disk). They show that the runtime is
not the bottleneck. They are not end-to-end throughput.

**Composability.** The parts are small and they connect:

- A pipeline can have many inputs and many outputs.
- The `pipeline` input reads the output of another pipeline. You can make
  fan-out, fan-in and chains of any depth.
- A connection declares a system one time. Many components refer to it.
- State buckets are shared between pipelines.
- Each transform does one thing. Use `script` (rhai) when the other transforms
  are not sufficient.
- Message metadata is ordinary JSON fields, so each transform can use it.

**Feature completeness.** The inventory is below. The
[reference](https://propell.dev/kayak/reference/) documents each component and
each field. kayak generates the reference from its code, and your server shows
it at `/docs`.

## the components

| | |
| --- | --- |
| **inputs** | `nats`, `kafka`, `mqtt`, `redis`, `opcua`, `http` (other systems post to it), `http_poll`, `postgres`, `clickhouse`, `indu`, `pipeline` (the output of another pipeline), `dummy` (for tests) |
| **transforms** | `filter`, `map`, `reducer` (aggregate, with `group_by`), `splitter`, `buffer`, `remember` / `recall` (state buckets), `http` (call a service or a model), `script` (rhai), and the streaming statistics: `deadband`, `throttle`, `pivot`, `derive`, `rolling`, `smooth`, `detect`, `resample`, `features` |
| **outputs** | `postgres` and `clickhouse` with column mapping, `file` and `s3` with rotation, `kafka`, `nats`, `mqtt`, `redis`, `http`, `indu`, `tidepool`, `stdout` |
| **connections** | `kafka`, `nats`, `mqtt`, `redis`, `postgres`, `clickhouse`, `s3`, `file`, `opcua`, `indu`, `tidepool` |

Each input can buffer by count, by time window, or by the first of the two.

## running it for real

The image contains the runtime and nothing else. It contains no config, so a
container with no arguments serves an empty graph. To deploy, mount a config
and name it on the command line. The `ENTRYPOINT` of the image is the binary,
so the arguments of the container are the flags of the server.

Read these points before you deploy kayak:

- **Pin a tag.** `latest` is the tip of `main`. A release tag such as `v0.2.2`
  publishes the image tags `0.2.2` and `0.2`. The images are for `linux/amd64`
  and `linux/arm64`.
- **Turn on authentication.** Without `--server-config`, any user who can
  reach the port can create and delete pipelines and rewrite the config. kayak
  shows a warning at startup when it has no authentication and binds to an
  address other than loopback. The two roles are `admin` and `read`.
- **Set `--data-dir`.** `file` outputs can write only under this directory.
  Without the flag, `file` outputs do not build.
- **kayak is pre-1.0.** The config format is not stable. Breaking changes can
  occur between minor versions.

[Deployment](https://propell.dev/kayak/operating/deployment) tells you about
Kubernetes, probes and the uid of the image.

## documentation

- **[The doc site](https://propell.dev/kayak/)**: the guide (the pipeline
  model, metadata, transforms, outputs, connections, secrets, authentication
  and deployment) and a generated reference for each component and each
  endpoint.
- **`/docs` on your server**: the same reference, generated from the binary
  that you run. The server also gives the HTTP API as OpenAPI 3.1 at
  `/api/openapi.json`.
- **[docs/roadmap.md](docs/roadmap.md)**: the work in progress, the planned
  work and the known problems.

The server also has a web UI. Use it to look at the running graph and the
messages in each pipeline. You do not need it to run kayak.

## contributing

Bug reports are the most useful contribution now. Reports of the type "I tried
to do X and could not" are also useful.
[CONTRIBUTING.md](CONTRIBUTING.md) tells you how to build from source, how to
run the tests, and the licence of a contribution.
[CLAUDE.md](CLAUDE.md) describes the architecture: how the crates fit together
and the reasons for the design.

Report security issues through [SECURITY.md](SECURITY.md). Do not use the
issue tracker for them.

## licence

kayak is **AGPL-3.0-or-later**. The exception is `kayak-core` (the shared
config types and DTOs), which is **Apache-2.0**. Any software that talks to
kayak can use `kayak-core` freely.

You can self-host kayak, modify it and run it inside a company. The licence
asks only that you keep the notices. The AGPL applies when you offer a
*modified* kayak to other users over a network. A commercial licence is
available if the AGPL does not suit you.

[licensing.md](licensing.md) gives the reasons, the third-party notices and the
licence of a contribution.
