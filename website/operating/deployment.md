# deployment

The `Dockerfile` builds one image that is the *runtime* and nothing else: the
server binary, with the WASM bundle and the assets compiled *into* it. **No
config is baked in.** Started bare it comes up with an empty graph and serves the UI, which is a
container that runs with no arguments and a Kubernetes Deployment that needs no
volume:

```bash
docker run -p 6767:6767 ghcr.io/niclasgrahm/kayak
```

Every push to `main` publishes that image, tagged `latest` and `sha-<short>`;
a `v1.2.3` tag additionally publishes `1.2.3` and `1.2`. Pin a version for
anything you care about — `latest` is the tip of `main`, not a release. Only
`linux/amd64` is published today, so an arm64 host runs it under emulation.

Building it yourself is the same image:

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

The build stage is `cargo leptos build --release --bin-features embed-assets`
with the cargo registry and `target/` on BuildKit cache mounts, so a rebuild
after a code change is incremental locally and no build artifacts reach a
layer. `librdkafka` is compiled from source, which is why the builder installs
`cmake`; TLS is rustls and zlib is vendored, so nothing else is.

## the binary carries the frontend

`embed-assets` is what makes the release artifact **one file**. Without it the
server reads the WASM bundle, the stylesheet and the vendored API-reference
renderer off a `target/site` directory at runtime, found through
`LEPTOS_SITE_ROOT` — so the binary and that directory have to travel together,
and a binary moved on its own serves a page whose bundle 404s. That failure
looks like a blank canvas rather than like a missing file, which is the reason
this is not left to whoever does the copying.

```bash
just build   # cargo leptos build --release --bin-features embed-assets
```

The feature is **off in every development build**, and deliberately: the site
directory is a build output, so embedding it would make `cargo check`,
`cargo test` and `just ci` all wait on a WASM toolchain. `cargo leptos watch`
and `just dev` therefore keep serving off disk, which is also what makes hot
reload work. Nothing else differs — a build without the feature behaves exactly
as the server did before it existed.

What the embedded server adds on top of a directory read: an `ETag` per file
and a `304` for a browser that already holds it, and `br`/`gzip` negotiation
against precompressed variants if there are any. `--precompress` on the build
produces them, at the cost of carrying three copies of the bundle in the
binary; the image does not pass it. Responses are `cache-control: no-cache`,
which means revalidate rather than do not store — asset names are stable across
releases, so anything longer-lived would serve last release's bundle after a
deploy.

`LEPTOS_SITE_ROOT` is unset in the image because there is no such directory in
it. `LEPTOS_SITE_PKG_DIR` stays: that one is the URL prefix the rendered page
links its bundle under, which is a path in the page rather than a path on
disk.
