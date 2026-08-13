# deployment

The `Dockerfile` builds one image that is the *runtime* and nothing else: the
server binary, the WASM bundle and the assets beside it. **No config is baked
in.** Started bare it comes up with an empty graph and serves the UI, which is a
container that runs with no arguments and a Kubernetes Deployment that needs no
volume:

```bash
docker build -t kayak .
docker run -p 6767:6767 kayak
```

A deployment is then a config mounted in and named on the command line. The
image's `ENTRYPOINT` is the binary, so the container's arguments *are* the
server's flags:

```bash
docker run -p 6767:6767 -v "$PWD/pipelines:/kayak" kayak \
  --config /kayak/config.json \
  --secrets /kayak/secrets.json \
  --data-dir /data
```

`/kayak` is the working directory and is owned by the run user, which matters
twice: relative paths in a config resolve against it, and *saving* a config
writes back beside the file it was loaded from. Everything else is the flags
documented on `--help`; the connections and layout files are found by derived
name beside the config, so mounting the directory rather than the one file is
what you want.

The sample graph travels along at `/usr/share/kayak/example` for a tour with
nothing mounted. It needs two things on the command line, both of them the
design working rather than packaging gaps: the data directory, because it has a
file output, and the secrets its connections reference — as environment
variables here, which is the shortest way to see the env-first resolution
working:

```bash
docker run -p 6767:6767 \
  -e NATS_PASSWORD=hunter2 -e POSTGRES_PASSWORD=hunter2 -e CLICKHOUSE_PASSWORD=hunter2 \
  kayak --config /usr/share/kayak/example/config.json --data-dir /kayak/dev_data
```

Without the secrets the server refuses to start rather than connecting without
credentials, which is what an unresolved `${NAME}` is supposed to do. The nats,
kafka, postgres and s3 pipelines then report connection errors on their cards
unless the container can reach those systems — `docker compose up` brings them
up on the host, so join that network and name the services rather than
`localhost`. `heartbeat` and its file output run regardless.

Points worth knowing before it goes anywhere real:

- **It runs as uid 10001**, declared as a number so a `runAsNonRoot` pod and the
  image's own default are the same identity, and a `chown` on a mounted volume
  is a number that survives a rebuild. Nothing in the image needs write access;
  the filesystem can be read-only if the config isn't going to be saved from the
  UI.
- **Port 6767**, set through `LEPTOS_SITE_ADDR` in the image (`0.0.0.0`, since
  the `Cargo.toml` default of `127.0.0.1` reaches nothing from outside a
  container). `--listen 0.0.0.0:8080` on the command line overrides it; without
  the flag that env var is what binds. Binding every interface is right *here* —
  the isolation is which ports the container publishes, not which address it
  listens on — and is the thing to think twice about on a host, since an
  unauthenticated kayak is a control plane.
- **Probes are plain HTTP.** The image carries no `curl` or `wget`, so an
  exec-style healthcheck has nothing to run: use a Kubernetes `httpGet` against
  `GET /api/pipelines`, which is also what a compose healthcheck should reach
  from outside. There is no dedicated health endpoint yet.
- **File outputs stay off without `--data-dir`**, in a container as everywhere
  else. That is the closed default working, not a packaging oversight — see
  "file output".
- **Secrets are environment variables first.** `${NAME}` references resolve
  against the process environment before the `--secrets` file, so a k8s
  `Secret` reaching the container as env vars needs no file mounted at all.

The build stage is `cargo leptos build --release` with the cargo registry and
`target/` on BuildKit cache mounts, so a rebuild after a code change is
incremental locally and no build artifacts reach a layer. `librdkafka` is
compiled from source, which is why the builder installs `cmake`; TLS is rustls
and zlib is vendored, so nothing else is.
