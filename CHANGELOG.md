# changelog

Notable changes, newest first. Versions are the git tags the container images
are published under — see "running it" in the readme.

kayak is pre-1.0, so the middle number is the one that moves when something
breaks: a change that stops an existing config file loading is a `0.x` bump,
and everything else is a `0.x.y`. What "notable" means here is *what an
operator would want to know before upgrading* — a behaviour change, a new
component, a default that moved. Refactors and internal work are in the git
history, not here.

## Unreleased

## 0.2.2 — 2026-10-09

**Patch**: additions and a fix, nothing an existing config has to change
for.

### Added

- **A card's config can be read whole.** Components on a tab with several
  fold to one line each, the transforms tab shows the chain as chips, and a
  maximized card lays every stage out side by side. A `fields | yaml | json`
  switch on the config heading shows the pipeline's config as text, with a
  copy button, served by a new `GET /api/pipelines/{id}/config`.

### Fixed

- **A card's config pane scrolls.** The wheel over it used to zoom the
  canvas, so a long transform chain could not be read; over a maximized card
  it zoomed the card's contents.

## 0.2.1 — 2026-10-09

**Patch, not minor**: every config that loaded on 0.2.0 still loads. The one
change of shape — `filter` taking a list of conditions — reads the old
single-comparison spelling too. Everything else is additions, all of them so
that a chain of stock transforms can say what used to need a script.

### Changed

- **`filter` takes a list of conditions.** It is now `{"type": "filter",
  "conditions": [...], "invert": false}`, the same conditions `remember`'s
  `when` and the `buffer` gate take, all of which must hold. A config in the
  old one-comparison spelling (`{"type": "filter", "Numeric": {...}}`) still
  loads, as a filter with that one condition, but is saved in the new
  spelling from then on.

### Added

- **`detect` learns more carefully, and says what it judged against.**
  `learn: normal_only` keeps a flagged reading out of its own baseline
  (`zscore`, `mad`, `ewma`), with `readapt_after_seconds` as the way out for
  a lasting change; `with_baseline` writes `<as>_expected` and `<as>_band`;
  and a new `ewma` method follows normal with time constants for the mean
  and the spread, with a `min_spread` floor.
- **A `pivot` transform.** One reading per message in, rows out: remembers
  the latest value of each of `names` per key and writes them all onto every
  message — a machine's state, fault and counters as one row.
- **A `throttle` transform.** At most one message per key every `seconds`,
  by the message's own time or arrival; the rest are dropped, whole. Keyed
  and bounded by the pipeline's state bucket like the other streaming
  transforms.
- **`when` and `reset_when` on every streaming transform** (`deadband`,
  `throttle`, `derive`, `rolling`, `smooth`, `detect`, `resample`). `when`
  picks the messages a transform applies to and passes the rest through
  untouched — so a state message beside numeric readings no longer fails
  the batch. `reset_when` starts a key's series over.
- **More ways to say a condition.** `not_equal_to` for numbers and strings,
  and `one_of` / `none_of` for testing a string field against a list. On
  `filter`, `remember` and the `buffer` gate alike.
- **`invert` on `filter`**: drop what the conditions match and keep the rest.
- **`min` and `max` in `map`'s arithmetic**, so a clamp is two mappings, and
  **`on_zero` on a division**: `null` or a value of your choosing instead of
  failing the batch when the divisor field holds zero.
- **A `time_bucket` mapping on `map`.** The start of the hour, day or shift
  a time falls in, counted on a named time zone's wall clock — so grouping a
  stateful transform by it starts its series over every period.
- **`smooth`'s `ewma` by time.** `tau_seconds` beside `alpha` and
  `half_life`, with a `time` field on `smooth`: a reading's weight follows
  how long it lasted rather than how many readings came before it, which is
  what an irregularly spaced series needs.

## 0.2.0 — 2026-10-08

**Minor, not patch**, because one change stops an existing config loading:
the `http` transform now refuses `verb: GET` and `verb: DELETE` at build
time (see *Changed*). Everything else is additions and one fix.

### Changed

- **The `http` transform honours `verb`, and refuses `GET` and `DELETE`.**
  It used to accept `verb` and always `POST`. A request with no body sends
  none of the messages, so a pipeline configured with either now fails to
  start, saying why, instead of quietly posting. Remove the `verb` or set
  `POST`/`PUT`/`PATCH`.

### Fixed

- **The `indu` output's `at` in epoch milliseconds.** A number in the field
  `at` names was sent as a string of digits, which Indu refuses as an
  unparseable timestamp — every row, so every batch failed. It is now sent
  as the RFC 3339 instant it names, as the docs always said it would be. A
  string is passed through unchanged.

### Added

- **`postgres` and `clickhouse` inputs.** Every other input is pushed to;
  these ask. Each runs a `table` or a `query` on a timer and hands each row
  on as a message — the whole relation every tick (`mode: snapshot`, for
  reference data) or only the rows above a watermark on a column that grows
  (`mode: incremental`, paged, `start_from: newest | oldest`, optional
  `lag_secs`). The watermark is kept in memory, so `ack: on_delivery` is
  refused.
- **An `http_poll` input.** The same snapshot for a system that only has an
  api: a `GET` every `interval_secs`, the whole reply handed on each time —
  one message per element of an array, `items` a JSON pointer into a reply
  that wraps its records. `auth` is the http output's.
- **Six streaming-statistics transforms: `deadband`, `derive`, `rolling`,
  `smooth`, `detect`, `resample`.** Each keeps a series per `group_by` key in
  the pipeline's declared state bucket, so a pipeline using one needs a
  `state` block. `deadband` passes a message only when its value moved;
  `derive` writes rates, deltas, running totals and wrap-tolerant counters;
  `rolling` writes the reducer's aggregations over a sliding window onto each
  message; `smooth` is ewma, median, Hampel or a trailing Savitzky–Golay;
  `detect` flags anomalies by z-score, MAD, CUSUM, an ewma chart, the Western
  Electric rules or a flatline; `resample` puts a series on a regular grid.
  See "streaming statistics" on the site.
- **A `features` transform.** A window of readings becomes the handful of
  numbers a model wants — waveform descriptors and, with a sample rate, the
  spectrum and named frequency bands — one message per group.
- **The `http` transform does a round trip.** `response: merge` writes the
  reply under `as` onto the message that asked, so the identifiers survive
  (`replace`, the old behaviour, stays the default); `body: batch | message`,
  `auth`, `timeout_seconds` and `retries` with backoff are new.
- **Time on the message, and statistics in a script.** A time is read off a
  message one way everywhere — RFC 3339 or epoch milliseconds, an error for
  anything else. The reducer gains `slope` (per second, against a `time`
  field), and scripts gain `parse_time`/`format_time` and the numbers:
  `pluck`, `mean`, `median`, `std`, `variance`, `quantile`, `zscore`,
  `linfit`, `peaks`, `histogram` and more. See "time and numbers" on the site.
- **A running script can be read from its card.** A `script` transform's
  source is now a row of its own in the card's transforms tab — `inline · N
  lines` or the file's path — that folds open to a short highlighted peek and
  opens a read-only viewer with the whole script and every module it
  imported. It shows the text the pipeline was *built* with, and says so when
  a file has changed on disk since. Served by the new
  `GET /api/pipelines/{id}/transforms/{index}/script`, at `read` access.
- **A `tidepool` connection and a `tidepool` output.** kayak writes a
  pipeline's messages into a table of a Tidepool server, one NDJSON request
  per batch, with `columns` spelled as the database outputs spell them (or
  messages sent as they are). The table is read on start and the mapping
  checked against it, so a column it doesn't have, a type it can't take or a
  null where it wants a value fails the start. A refused batch fails with
  Tidepool's problems quoted by row and column; a busy server (`503`) or an
  unreachable one is retried for up to `retry_seconds` under one idempotency
  key per batch, so a retry never writes twice. After a refusal the table is
  read again, since Tidepool's config changes live. See "tidepool" on the
  site.
- **An `indu` connection and an `indu` output.** kayak writes a pipeline's
  results into Indu Cloud as *streams* — series that are not sensors —
  through `POST /ingest/v1/streams`. One message yields one reading per
  entry in `series`, with `{field}` placeholders in the stream name so one
  output serves every machine a pipeline reduces over; an unknown stream is
  created on the platform on first sight. A `207` is a failure naming the
  refused row, and each batch carries an idempotency key. See "indu" on the
  site.
- **An `indu` input.** The other direction: kayak reads sensors and streams
  out of Indu Cloud live, over the platform's server-sent-events endpoint,
  under the same connection. Sensors are named `<device>/<sensor>` and
  streams by the name they were written under, resolved on the first read;
  every reading is one message carrying the name, the ids, the unit and the
  time. A dropped connection reconnects with backoff, readings the platform
  dropped are an error on the card, and `backfill` starts each series from
  its latest value. kayak's first SSE input.

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
