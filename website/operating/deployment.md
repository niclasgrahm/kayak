# deployment

Run kayak as a container image. Keep the config file and the connections file
in version control. Mount them into the container and give the path of the
config as a flag. This is the complete deployment.

```bash
docker run -p 6767:6767 -v "$PWD:/kayak" ghcr.io/niclasgrahm/kayak:0.2.2 \
  --config /kayak/config.yaml
```

To change the deployment, change the file, review the diff and deploy again.

## the image

The image is `ghcr.io/niclasgrahm/kayak`. It contains these items:

- the `kayak` binary at `/usr/local/bin/kayak`, with the web UI compiled into it;
- a CA bundle, for outbound TLS;
- the license, at `/usr/share/kayak/LICENSE`;
- the sample graph, at `/usr/share/kayak/example`.

The image does not contain a config, connections, secrets or a server config.
Without arguments, the server starts with an empty graph.

The `ENTRYPOINT` is the binary. Thus the arguments of the container are the
flags of the server.

### tags

| tag | what it is |
| --- | --- |
| `0.2.2` | one release (from the git tag `v0.2.2`) |
| `0.2` | the newest release of that minor version |
| `sha-<short>` | one commit on `main` |
| `latest` | the newest commit on `main` |

Pin a release tag in production. `latest` is the tip of `main` and is not a
release.

Each tag is a manifest list for `linux/amd64` and `linux/arm64`. Each
architecture is built on a runner of that architecture, without emulation. A
pull selects the correct architecture without a `--platform` flag.

## flags

| flag | what it does |
| --- | --- |
| `--config <path>` | the pipelines, as JSON or YAML. The extension selects the format. |
| `--connections <path>` | the connections file. Optional. Without it, kayak reads `<config-stem>.connections.<ext>` beside the config. |
| `--secrets <path>` | a JSON file of `"NAME": "value"` pairs for `${NAME}` references. Environment variables have priority. |
| `--server-config <path>` | authentication and history settings, as JSON or YAML. Without it, the server authenticates nobody. |
| `--data-dir <path>` | the directory that `file` outputs can write to. kayak creates it. Without it, `file` outputs do not build. |
| `--listen <addr>` | the address and port, as one value: `0.0.0.0:6767`, `[::]:6767`. |
| `--debug` | more detail in the log. |

Most deployments do not need `--connections`. Put the connections file beside
the config with the same stem: `config.yaml` and `config.connections.yaml`.
Mount the directory, not the single file, so that kayak finds both. The layout
file of the web UI (`config.layout.json`) uses the same rule.

The server config is separate from the config because it belongs to the
process. See [authentication](/operating/authentication) and
[history](/operating/history) for its contents.

### the listen address

The image sets `LEPTOS_SITE_ADDR=0.0.0.0:6767`. Thus the container listens on
port 6767 on all interfaces. Use `--listen` to change the address:

```bash
docker run -p 8080:8080 ghcr.io/niclasgrahm/kayak:0.2.2 --listen 0.0.0.0:8080
```

The order of priority is `--listen`, then `LEPTOS_SITE_ADDR`, then
`127.0.0.1:6767`.

Do not expose a server without authentication. Any client that can reach the
port can delete pipelines and change the config. kayak logs a warning at startup
when the server has no authentication and the address is not loopback. Inside a
container, `0.0.0.0` is correct. Control access with the ports that you publish
and with `--server-config`.

### secrets

A `${NAME}` reference resolves against the environment first, and then against
the `--secrets` file. Thus environment variables are sufficient, and you do not
need a secrets file. The server does not start when a reference does not
resolve. See [secrets](/io/secrets).

## docker

Put the config, the connections and the server config in one directory:

```
deploy/
  config.yaml
  config.connections.yaml
  kayak.server.yaml
```

Run the image with that directory at `/kayak`:

```bash
docker run -d --name kayak -p 6767:6767 \
  -v "$PWD/deploy:/kayak" \
  -v kayak-data:/data \
  -e POSTGRES_PASSWORD \
  -e KAYAK_ADMIN_PASSWORD \
  ghcr.io/niclasgrahm/kayak:0.2.2 \
  --config /kayak/config.yaml \
  --server-config /kayak/kayak.server.yaml \
  --data-dir /data
```

`/kayak` is the working directory of the image. Relative paths in the config
resolve against it. The run user owns it, so a save from the web UI can write
the config back to it.

## docker compose

```yaml
services:
  kayak:
    image: ghcr.io/niclasgrahm/kayak:0.2.2
    command:
      - --config=/kayak/config.yaml
      - --server-config=/kayak/kayak.server.yaml
      - --data-dir=/data
    ports:
      - "6767:6767"
    volumes:
      - ./deploy:/kayak
      - kayak-data:/data
    environment:
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      KAYAK_ADMIN_PASSWORD: ${KAYAK_ADMIN_PASSWORD}
    stop_grace_period: 30s

volumes:
  kayak-data:
```

Use the service names of the other containers in the connections file, for
example `postgres`. Do not use `localhost`. Inside the container, `localhost`
is the kayak container.

## kubernetes

Put the config and the connections in a ConfigMap. Put the credentials in a
Secret. Give the Secret to the container as environment variables.

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: kayak-config
data:
  config.yaml: |
    - id: sensors
      inputs:
        - type: nats
          connection: plant-nats
          subject: sensors.>
      outputs:
        - type: postgres
          connection: archive
          table: readings
  config.connections.yaml: |
    plant-nats:
      type: nats
      urls: nats://nats:4222
    archive:
      type: postgres
      host: postgres
      port: 5432
      database: kayak
      user: kayak
      password: ${POSTGRES_PASSWORD}
  kayak.server.yaml: |
    auth:
      type: basic
      users:
        admin:
          password: ${KAYAK_ADMIN_PASSWORD}
          role: admin
---
apiVersion: v1
kind: Secret
metadata:
  name: kayak-secrets
type: Opaque
stringData:
  POSTGRES_PASSWORD: change-me
  KAYAK_ADMIN_PASSWORD: change-me
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: kayak
spec:
  replicas: 1
  selector:
    matchLabels: { app: kayak }
  template:
    metadata:
      labels: { app: kayak }
    spec:
      terminationGracePeriodSeconds: 30
      securityContext:
        runAsNonRoot: true
        runAsUser: 10001
        runAsGroup: 10001
        fsGroup: 10001
      containers:
        - name: kayak
          image: ghcr.io/niclasgrahm/kayak:0.2.2
          args:
            - --config=/etc/kayak/config.yaml
            - --server-config=/etc/kayak/kayak.server.yaml
            - --data-dir=/data
          ports:
            - containerPort: 6767
          envFrom:
            - secretRef: { name: kayak-secrets }
          volumeMounts:
            - { name: config, mountPath: /etc/kayak, readOnly: true }
            - { name: data, mountPath: /data }
          securityContext:
            readOnlyRootFilesystem: true
            allowPrivilegeEscalation: false
          readinessProbe:
            httpGet: { path: /api/auth/me, port: 6767 }
          livenessProbe:
            httpGet: { path: /api/auth/me, port: 6767 }
            periodSeconds: 20
      volumes:
        - name: config
          configMap: { name: kayak-config }
        - name: data
          emptyDir: {}
```

The [connections reference](/reference/connections) gives the fields of each
connection kind.

Obey these rules:

- **Mount the ConfigMap as a directory.** kayak finds `config.connections.yaml`
  because it is beside `config.yaml`. A `subPath` mount of one key breaks this.
- **Use one replica per config.** Two replicas run every pipeline two times.
  For example, two kafka inputs share the partitions of one consumer group.
- **Give `--data-dir` a writable volume.** Use an `emptyDir` or a
  PersistentVolumeClaim. Omit `--data-dir` when the config has no `file`
  output.
- **Expect a read-only config.** A ConfigMap mount is read-only. A save or a
  layout change from the web UI fails. Change the config in git and deploy
  again.

### probes

Use an `httpGet` probe. The image has no `curl` and no `wget`, so an `exec`
probe has nothing to run. Use `GET /api/auth/me`. It answers `200` without
credentials, also on a server with authentication. `GET /api/pipelines` also
works, but only on a server without authentication.

The server opens its port after it builds every pipeline in the config. Thus a
successful probe means that the config loaded. A pipeline that fails later
does not fail the probe. Use [history](/operating/history) to monitor the
pipelines.

### user and file system

The image runs as uid 10001 and gid 10001. The uid is a number, so
`runAsNonRoot` accepts it. Use the same number when you `chown` a volume.
kayak writes only to these locations:

- the `--data-dir` directory, for `file` outputs;
- the directory of the config, when you save from the web UI or move a card.

Thus `readOnlyRootFilesystem: true` is safe.

## the sample graph

The image contains the sample graph of the repository. Use it to look at kayak
without a config of your own:

```bash
docker run -p 6767:6767 \
  -e NATS_PASSWORD=hunter2 -e POSTGRES_PASSWORD=hunter2 -e CLICKHOUSE_PASSWORD=hunter2 \
  -e S3_ACCESS_KEY_ID=rustfsadmin -e S3_SECRET_ACCESS_KEY=rustfsadmin \
  ghcr.io/niclasgrahm/kayak:0.2.2 \
  --config /usr/share/kayak/example/config.json --data-dir /kayak/dev_data
```

The sample needs two things:

- the secrets that its connections refer to (without them, the server does not
  start);
- `--data-dir`, because the sample has a `file` output.

The pipelines that use nats, kafka, postgres, clickhouse and s3 report
connection errors until the container can reach those systems. `docker compose
up` in the repository starts them. `heartbeat` and its outputs work without
them.

## build the image yourself

```bash
docker build -t kayak .
docker run -p 6767:6767 kayak
```

The build stage runs `cargo leptos build --release --bin-features embed-assets`.
The cargo registry and `target/` are BuildKit cache mounts. Thus a rebuild is
incremental, and no build artifact goes into a layer. The builder installs
`cmake` to compile `librdkafka`. TLS is rustls and zlib is vendored, so the
build needs no other system packages.

## the binary carries the frontend {#the-binary-carries-the-frontend}

A release build compiles the web UI into the binary: the WASM bundle, the
stylesheet and the API reference renderer. Thus a release is one file.

```bash
just build   # cargo leptos build --release --bin-features embed-assets
```

The `embed-assets` feature is off in development builds. Without it, the server
reads the files from `target/site`, through `LEPTOS_SITE_ROOT`. Thus
`cargo check`, `cargo test` and `just ci` do not need a WASM build. The hot
reload of `cargo leptos watch` and `just dev` also needs the feature off.

The embedded server sends an `ETag` for each file and a `304` when the browser
has the file. It also serves `br` and `gzip` variants when the build made them
with `--precompress`. The image does not use `--precompress`, because it puts
three copies of the bundle in the binary. Responses have
`cache-control: no-cache`. The asset names do not change between releases, so a
browser must revalidate each file.

The image does not set `LEPTOS_SITE_ROOT`, because the image has no site
directory. It sets `LEPTOS_SITE_PKG_DIR`, which is the URL prefix of the bundle
in the page.

## shutting down

`SIGTERM` and `SIGINT` stop the server in this sequence:

1. The server refuses new connections.
2. It closes the `/events` streams.
3. It lets the open requests complete.
4. It stops the pipelines and waits for each run loop to end.

In step 4, each output runs `finish`. A `file` output with `json_array` writes
the closing bracket. An `s3` output uploads its current part. That part exists
only in memory, so a process that stops without `finish` loses it.

Two limits apply. You cannot configure them:

- **10 s** for the open connections to close;
- **5 s** for the run loops to end.

After a limit, the server writes a log line and continues to stop. A second
signal stops the process immediately.

A shutdown does not write the config file. Unsaved changes from the web UI are
lost on a restart. State buckets and history are in memory, so a restart also
loses them.

kayak is pid 1 in the container, and it handles `SIGTERM` itself. Set a stop
timeout longer than 15 s: `docker stop --time`, `stop_grace_period` in compose,
or `terminationGracePeriodSeconds` in Kubernetes. The Kubernetes default of
30 s is sufficient.
