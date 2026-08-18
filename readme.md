# kayak

**kayak** is a graph-based stream processing engine: you describe pipelines as
`inputs → transforms → outputs` in a config file, kayak runs them, and a live
web canvas shows the graph while it's working — a card for every pipeline,
edges for how data flows between them, and a log and throughput chart on each
one.

Messages are plain JSON the whole way through. There is no schema to define up
front, and a pipeline can feed another pipeline, so the graph is a DAG rather
than a list of independent jobs.

![the state tab, showing a running graph and a bucket inspector](state-tab.png)

## try it, in one command

```bash
docker run --rm -p 6767:6767 --entrypoint sh ghcr.io/niclasgrahm/kayak \
  -c 'echo "[{id: ticker, inputs: [{type: dummy, duration: 1}]}]" > c.yaml && exec kayak --config c.yaml'
```

Open **http://localhost:6767** — one pipeline, ticking once a second. Click the
card to watch its log, drag it around, add another pipeline fed by it.

That config really is the whole thing: `transforms` and `outputs` are optional,
so an input on its own is a complete pipeline. Add a `stdout` output and the
messages show up in your terminal too:

```bash
docker run --rm -p 6767:6767 --entrypoint sh ghcr.io/niclasgrahm/kayak \
  -c 'echo "[{id: ticker, inputs: [{type: dummy, duration: 1}], outputs: [{type: stdout}]}]" > c.yaml && exec kayak --config c.yaml'
```

## running your own pipelines

Write a config and mount it:

```yaml
# pipelines/config.yaml
- id: readings
  inputs:
    - type: dummy
      duration: 1
  outputs:
    - type: stdout
```

```bash
docker run -p 6767:6767 -v "$PWD/pipelines:/kayak" \
  ghcr.io/niclasgrahm/kayak --config /kayak/config.yaml
```

JSON works everywhere YAML does — the extension decides.

Swap the dummy for a real source when you're ready. Inputs and outputs that
talk to a broker, a database or a bucket name a **connection** instead of
repeating a host: connections are declared once in a file beside the config,
and their credentials are `${NAME}` references resolved from the environment
or a secrets file, never written back out. See
[connections](https://propell.dev/kayak/io/connections).

## what you can wire together

| | |
| --- | --- |
| **inputs** | nats, kafka, mqtt, redis, OPC UA, http (pushed to, rather than polled), another pipeline, and a dummy source for testing |
| **transforms** | filter, reduce/aggregate with grouping, map (reshape and cast fields), buffer, split, a scripted transform in [rhai](https://rhai.rs), an http call out, and `remember`/`recall` over named state buckets |
| **outputs** | postgres and ClickHouse with real column mapping, files and S3-compatible object storage with rotation, kafka, nats, mqtt, redis, http, and stdout |

Any input can be batched by count, by time window, or both. Every component and
every field is documented in the [reference](https://propell.dev/kayak/reference/),
which is generated from the code — and served by your own server at `/docs`.

## why

Most stream-processing tools are either code — write a consumer, wire it up
yourself — or heavyweight platforms that hide the running graph behind logs and
dashboards you assemble separately. kayak aims at the space between: pipelines
you describe in a config file, watch running as a graph, and reshape from the
same screen. It's built for small-to-medium data-wrangling jobs where you want
to *see* what's happening rather than trust that it is.

The canvas is a real view onto the running server, not a mockup. Watching it,
editing the graph from it, and driving the same JSON/HTTP API by hand are the
same interface.

## running it for real

The image is the runtime and nothing else — no config is baked in, so bare it
serves an empty graph. A deployment is a config mounted in and named on the
command line; the `ENTRYPOINT` is the binary, so the container's arguments are
the server's flags.

Four things worth knowing before you put it anywhere real:

- **Pin a tag.** `latest` is the tip of `main`. Release tags are `0.1.0` and
  `0.1`. Both `linux/amd64` and `linux/arm64` are published, built natively.
- **Turn authentication on.** Without `--server-config`, anyone who can reach
  the port can create and delete pipelines and rewrite the config. kayak warns
  about this at startup when it isn't bound to loopback.
- **`--data-dir` bounds where pipelines may write.** Without it, file outputs
  refuse to build at all — a closed default rather than a stub.
- **Pre-1.0.** The config format isn't stable yet, and breaking changes will
  happen between minor versions.

[Deployment](https://propell.dev/kayak/operating/deployment) covers
Kubernetes, probes, the uid the image runs as, and what it deliberately
doesn't bake in.

## documentation

- **[the docs site](https://propell.dev/kayak/)** — the guide: the canvas and
  the editor, the pipeline and metadata model, every transform and output,
  connections, secrets, authentication and deployment, plus a generated
  reference for every component and every endpoint.
- **`/docs` on your own server** — the same reference, generated from the
  binary you're running. The HTTP API is also served as OpenAPI at
  `/api/openapi.json`.
- **[docs/roadmap.md](docs/roadmap.md)** — what's in flight, planned, or known
  to be broken.

## contributing

Bug reports and "I tried to do X and couldn't" issues are the most useful thing
right now. [CONTRIBUTING.md](CONTRIBUTING.md) has how to build from source, how
to run the tests, and what a contribution is licensed under.
[CLAUDE.md](CLAUDE.md) is the architecture tour — how the crates fit together
and why particular things are built the way they are.

Security issues go through [SECURITY.md](SECURITY.md), not the issue tracker.

## licence

kayak is **AGPL-3.0-or-later**, except `kayak-core` — the shared config types
and DTOs — which is **Apache-2.0** so anything talking to kayak can be built
against it freely.

Self-hosting, modifying and running kayak inside a company are what the licence
is for, and ask nothing of you beyond keeping the notices. Offering a *modified*
kayak to others over a network is the case the AGPL covers, and a commercial
licence is available for anyone that doesn't suit.

[licensing.md](licensing.md) has the reasoning, the third-party notices and what
a contribution is licensed under.
