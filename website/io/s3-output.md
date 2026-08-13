# s3 output

The same writer pointed at a bucket instead of a directory: same generated part
names, same `format`, same `rotate`. Anything S3-compatible works — the sample
is the rustfs in `docker-compose.yaml`, and leaving `endpoint` out addresses real
AWS S3 in `region`.

```jsonc
// config.connections.json — the bucket and the credentials that reach it
{ "local-s3": { "type": "s3", "bucket": "events",
                "access_key_id": "${S3_ACCESS_KEY_ID}",
                "secret_access_key": "${S3_SECRET_ACCESS_KEY}",
                "endpoint": "http://localhost:9000", "allow_http": true } }

// config.json — what this pipeline writes there
{ "type": "s3", "connection": "local-s3", "prefix": "orders",
  "format": "ndjson", "rotate": { "max_rows": 100000, "interval_secs": 3600 } }
```

`prefix` is to a bucket what `path` is to a root; objects land at
`<prefix>/<generated part name>`. An empty prefix writes at the top of the
bucket, which is legal here in a way an empty `path` is not — the connection *is*
the bucket, so there is nothing to insist on. The bucket has to exist: this
output creates objects and never buckets.

**Why this is a separate component from `file`.** The two share everything in
`src/outputs/rotate.rs` and differ in one thing that runs deep: **an object store
has no append.** A file output opens a file and writes each batch into it, so a
part is readable on disk while it is still filling. A bucket has no such state —
an object exists or it does not, and `PUT` writes it whole. So a part is
accumulated in memory and uploaded when it rotates, which makes `rotate`
**required** on this output and optional on the file one. Without a trigger a
pipeline would hold its entire run in RAM and upload it once at the end, so the
output refuses to build rather than doing that quietly.

That also makes rotation the thing that decides how soon data is visible.
`max_rows: 20` on a one-a-second pipeline means an object every twenty seconds
and never sooner. When the pipeline stops, the part in memory is uploaded by
`OutputDestination::finish` — without that hook a cancelled pipeline would lose
it outright, since there is no half-written object on the other side to recover.

Multipart upload is the obvious alternative and is deliberately not used yet: S3
requires every part but the last to be at least 5 MiB, so "one multipart part per
batch" does not work at the batch sizes a pipeline produces.

**There is no `--data-dir` here and there cannot be.** The local sandbox works
because the server can ask the filesystem where a path really landed; nothing
equivalent exists for a remote namespace. The boundary for this output is the
credentials on its connection — a key that can write one bucket does the job
`--data-dir` does locally, and that is where to spend the care. `allow_http` is
the one guard rail on this side: plaintext credentials are refused unless the
connection asks for them, which is what the local rustfs does and a real
deployment should not.

`docker compose up` brings up rustfs on `:9000` with the bucket `events` already
made (a one-shot `mc` container does that). It writes to a tmpfs, so the bucket
is empty again after a `docker compose down` — which is what you want from a
fixture.
