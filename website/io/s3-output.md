# s3 output

The `s3` output writes each batch to objects in a bucket. It uses the same file
names, the same `format` and the same `rotate` as the
[file output](/io/file-output). It works with any S3-compatible store. Omit
`endpoint` to use AWS S3 in `region`.

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

## fields

- **`prefix`**: the key prefix. Objects go to `<prefix>/<generated part name>`.
  An empty prefix writes at the top of the bucket.
- **`format`**: `ndjson` (the default) or `json_array`.
- **`rotate`**: required. See below.

The bucket must exist. The output creates objects. It does not create buckets.

## rotation is required

An object store cannot append to an object. Thus the output keeps the current
part in memory and uploads it when it rotates. Without a trigger, the pipeline
holds its whole run in memory. Thus the output does not build without
`rotate`. A `rotate` without `max_rows` or `interval_secs` also fails.

Rotation sets how soon data is visible in the bucket. For example, `max_rows:
20` on a pipeline with one message each second makes one object each 20 s.

Rotation also sets how much memory the pipeline uses. Choose `max_rows` and
`interval_secs` to keep a part to a size that the server can hold.

When the pipeline stops, the output uploads the part that is in memory. This
includes a stop through `SIGTERM` or `SIGINT`. If the process crashes, that
part is lost.

The output does not use multipart upload. S3 requires 5 MiB or more for each
part except the last, and a batch is usually smaller.

## security

There is no `--data-dir` limit for this output. The credentials on the
connection set what the output can write. Give the connection a key that can
write only to the bucket that it needs.

kayak refuses to send credentials over plain HTTP. Set `"allow_http": true` on
the connection to permit it. The local rustfs needs this. Do not set it in a
production deployment.

## trying it

`docker compose up` starts rustfs on `:9000` and creates the bucket `events`.
rustfs writes to a tmpfs, so `docker compose down` empties the bucket.
