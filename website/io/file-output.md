# file output

The `file` output writes each batch into rotating files in a directory on the
server. Use it for local development and tests. For production storage, use the
[s3 output](/io/s3-output).

```jsonc
// config.connections.json — where this server may write
{ "local-files": { "type": "file", "root": "./dev_data/events" } }

// config.json — what this pipeline writes there
{ "type": "file", "connection": "local-files", "path": "orders",
  "format": "ndjson", "rotate": { "max_rows": 100000, "interval_secs": 3600 } }
```

## format

- **`ndjson`** (the default): one JSON message on each line. The file is valid
  after each batch. Thus you can read the file while the pipeline runs, or
  after a crash.
- **`json_array`**: the whole file is one JSON array. The output closes the
  array when the file rotates or when the pipeline stops.

Use `ndjson` for a stream.

## rotation

`rotate` closes the current file and starts a new file. It has two triggers.
Both are optional. The first trigger that occurs closes the file.

- **`max_rows`**: close the file when it holds this many messages.
- **`interval_secs`**: close the file this many seconds after it opened.

Without `rotate`, the pipeline writes one file for as long as it runs.

The output checks rotation **after** it writes a batch. It never splits a batch
across two files. Thus `max_rows` is a minimum. For example, a batch of 500
messages that arrives at 999 rows makes a file of 1499 rows.

An `interval_secs` rotation occurs only when the next batch arrives. A pipeline
with no traffic keeps its file open after the interval.

## file names

kayak generates the file names, for example
`2026-08-07T14-00-00Z-000001.ndjson`. The time is when the file opened. Thus a
plain `ls` lists the files in time order. The sequence number keeps two files
that open in the same second separate.

## the sandbox

A file output cannot write until you tell the server where it can write. There
are two limits:

1. **`--data-dir <path>`** on the command line. Only the operator can set it.
   Without this flag, file outputs do not build.
2. **The `root` of the `file` connection.** It must be inside `--data-dir`.
   Use different roots to give different pipelines different directories.

The `path` of the component is relative to the root. kayak refuses an absolute
path and a path that contains `..`. It does not change the path to make it
valid. After it resolves the path, kayak checks the real directory again. Thus
a symbolic link inside the root cannot point out of it.

kayak does all these checks at build time, and the build creates the directory.
A bad path fails the pipeline at startup.

The container image does not set `--data-dir`. Add it to the command line
when you use a file output, for example:

```bash
docker run -v "$PWD:/kayak" ghcr.io/niclasgrahm/kayak \
  --config /kayak/config.yaml --data-dir /kayak/data
```

`just dev` passes `--data-dir dev_data`. Git ignores that directory.
