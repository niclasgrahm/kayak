# changelog

Notable changes, newest first. Versions are the git tags the container images
are published under — see "running it" in the readme.

kayak is pre-1.0, so the middle number is the one that moves when something
breaks: a change that stops an existing config file loading is a `0.x` bump,
and everything else is a `0.x.y`. What "notable" means here is *what an
operator would want to know before upgrading* — a behaviour change, a new
component, a default that moved. Refactors and internal work are in the git
history, not here.

## 0.1.2 — 2026-08-19

### Added

- **A blank server now offers to create a project.** Started with no
  `--config`, kayak used to open an empty canvas with no explanation. It now
  greets you with a create-a-project dialog — a file name, a JSON/YAML picker
  and the directory the file lands in — which goes through the same
  `POST /api/config/save` the UI has always used. Declining is "not now": the
  canvas behind says the server has no project yet and offers the dialog back.
  There is deliberately no project *picker* yet.

- **You can sample an input's messages while configuring it.** A "fetch
  messages" button on every input in the add-pipeline form builds the input
  exactly as a pipeline would, takes what arrives inside a bounded wait, and
  shows it — and what it carries then fills in the field suggestions and a
  database output's column mapping. `POST /api/inputs/sample` and
  `POST /api/pipelines/dry-run` are the endpoints; the latter puts those
  messages down the draft's transforms so an output is offered the fields that
  will actually reach it.

  **Nothing is acknowledged**, so sampling cannot lose a message. Kafka reads
  under a throwaway consumer group and mqtt under its own client id, so a
  running pipeline's offsets and connection are untouched; anything a sample
  changed comes back in `notes` and is shown. The `http` input cannot be
  sampled — it is posted to, not read from. Suggestions stop short of what a
  handful of messages cannot prove: nullability is never inferred, and a field
  the sample disagreed about gets no suggested type.

### Fixed

- **An output that isn't up yet no longer kills the pipeline permanently.**
  `init_outputs` returned the error, which ended the run loop before it began —
  and since nothing removes a handle when a run loop exits, a `postgres` output
  pointed at a database that simply hadn't started yet left the pipeline
  registered, dead, and unrecoverable short of restarting the server. It now
  retries on the same backoff every input and output already reconnects on,
  cancellable so a delete doesn't wait out a sleep, and resumed at the failing
  output rather than restarted. No batch reaches an output that hasn't
  initialised — that invariant is unchanged.

  Retrying is deliberately not conditional on the kind of failure: a wrong
  password and a downed host are the same error to most drivers. A permanent
  failure is legible anyway, as one error whose count climbs.

- **Pipeline cards say what the run loop is doing** — starting, running,
  stopped or failed — so a pipeline that died is visible as such instead of
  looking idle. The badge updates on the next load of the pipeline list rather
  than live.

- **"Save as" refuses to overwrite when it is creating.** `save_config_as`
  never checked whether the target existed. Start kayak in a directory that
  already holds a `config.json`, forget the `--config` flag, accept the new
  dialog's suggested name, and the config, its connections and its layout were
  replaced by an empty graph, silently. Creating now sends `overwrite: false`
  and the server answers **409** naming the file, with nothing written. The
  check covers all three files a save writes, not just the config.

  The field defaults to `true`, so an omitted one is byte-for-byte the old
  behaviour and an existing `curl` keeps working.

- **A downstream pipeline whose receiver was gone is now pruned** rather than
  kept and failed against for the life of the upstream.

### Security

- `quinn-proto` bumped to 0.11.17, covering four remote memory-exhaustion
  advisories upstream (GHSA-qfwj-vfxf-92j2, GHSA-2hv7-gw8g-gpq5,
  GHSA-hmxj-32vh-65vr, GHSA-4w2j-m93h-cj5j). **No kayak build was affected**:
  quinn is reqwest's HTTP/3 backend, that feature is not enabled, and the crate
  is in the lockfile without being in any build's dependency tree. This is
  lockfile hygiene, not a fix for a reachable path.

## 0.1.1 — 2026-08-18

### Fixed

- **The server now shuts down when it is asked to.** `SIGTERM` and `SIGINT`
  were both unhandled, so nothing ran on the way out. That mattered most for
  outputs holding an unfinished part: a `file` output never closed its
  `json_array`, leaving a file no reader could parse, and the `s3` output lost
  its buffered part **outright** — an object store has no append, so a part
  that has not rotated yet exists nowhere but in memory.

  Under docker it was worse. The image's `ENTRYPOINT` is the binary, so kayak
  runs as pid 1, and pid 1 has no default action for a signal it has not
  handled: `docker stop` was ignored, and every container stop was a
  ten-second wait for a `SIGKILL`.

  Stopping is now ordered — new connections refused, `/events` streams ended,
  open requests drained, then the pipelines cancelled and awaited so every
  output gets its `finish`. Bounded at ten seconds for the drain and five for
  the run loops, after which it says so in the log and carries on stopping. A
  second signal still kills it immediately. A shutdown never writes the config
  file: unsaved changes are still unsaved after a restart.

  See "shutting down" in the deployment guide.

### Security

- `h2` bumped to 0.4.16 for [RUSTSEC-2026-0258], a transitive dependency (via
  hyper) that queued empty HTTP/2 DATA frames without limit — unbounded memory,
  or a panic on length overflow, against a server whose streams are not being
  drained. Low severity upstream.

[RUSTSEC-2026-0258]: https://rustsec.org/advisories/RUSTSEC-2026-0258

### Documentation

- The readme is written for people running kayak rather than for people working
  on it; the contributor material moved to the doc site.

## 0.1.0 — 2026-08-18

First public release. The graph-based stream processor, its canvas UI and the
generated reference, published as `ghcr.io/niclasgrahm/kayak` for both
`linux/amd64` and `linux/arm64`.
