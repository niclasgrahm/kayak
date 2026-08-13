# file output

Writes each batch into rotating files in a directory. It exists for local
development and testing — the object-store output is what this shape is being
built towards for anything else — but the parts that will be shared with it are
already split out: rotation, part naming and encoding live in
`src/outputs/rotate.rs`, which touches no filesystem at all, and only the
destination lives in `src/outputs/file.rs`.

```jsonc
// config.connections.json — where this server may write
{ "local-files": { "type": "file", "root": "./dev_data/events" } }

// config.json — what this pipeline writes there
{ "type": "file", "connection": "local-files", "path": "orders",
  "format": "ndjson", "rotate": { "max_rows": 100000, "interval_secs": 3600 } }
```

`format` is `ndjson` (the default — one JSON message per line) or `json_array`
(the whole file is one array, closed when the part rotates). Prefer `ndjson` for
anything that streams: the file is valid after every batch, so a run that is
still going, or that died, is still readable.

`rotate` closes the current file and starts the next. `max_rows` counts
messages, `interval_secs` measures from when the part was *opened*; either may
be omitted, and whichever comes first wins. With neither, a pipeline writes one
file for as long as it runs. Rotation is checked **after** a batch is written,
so a batch is never split across two files and `max_rows` is a floor rather than
a ceiling — a batch of 500 arriving at 999 rows makes a file of 1499.

Part names are generated, not configured: `2026-08-07T14-00-00Z-000001.ndjson`.
The open timestamp makes a run's parts sort chronologically under a plain `ls`
or an object-store prefix listing, and the sequence number keeps two parts
opened in the same second distinct — which a row trigger on a fast pipeline will
do, and where a collision would lose data silently.

## the sandbox

**A file output cannot write anywhere until the server is told where.** This is
the same problem `persist::save_path` solves for config files, and for the same
reason: the browser does not write to the server's disk, the *server* does, on
request. `POST /api/pipelines` and `POST /api/connections` both take their
contents from an HTTP body, so an unconstrained path in either would turn the
pipeline editor into an arbitrary-write primitive — and this one writes
attacker-influenced *content*, at whatever volume the pipeline carries.

So there are two layers, and neither is sufficient alone:

1. **`--data-dir <path>`**, fixed when the process starts and reachable by no
   request. Without the flag file outputs refuse to build at all. The closed
   default is deliberate: a disk writer is not something a deployment should get
   without asking for it.
2. **the connection's `root`**, which arrives over HTTP like anything else and
   is therefore checked against layer 1 rather than trusted. It is what lets an
   operator hand different pipelines different subtrees.

The component's `path` is relative to the root. Paths are **refused, never
normalised** — an absolute path or one containing `..` fails the build rather
than being trimmed, because trimming leaves whoever wrote it believing it meant
something, and a normaliser is one edge case away from being the hole it was
written to close. After resolving, the landing directory is canonicalized and
re-checked against both layers, which is what stops a symlink planted inside the
root from pointing out of it.

All of it is decided at **build** time, and the build creates the directory: a
path that escapes, or a root nobody can write to, fails the pipeline that owns
it rather than surfacing an hour into a run. `just dev` passes
`--data-dir dev_data` so the component is usable without ceremony; the directory
is gitignored, being output rather than a fixture.

One thing it does not do yet: an `interval_secs` rotation is only noticed when
the next batch arrives, so an idle pipeline holds its part open past the
interval — there is no timer task closing it. A part left open when the pipeline
*stops* is fine, though — the run loop calls `OutputDestination::finish` on its
way out, which is what closes a `json_array`'s trailing `]`.
