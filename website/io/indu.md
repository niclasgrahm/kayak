# indu

Indu Cloud is the industrial data platform kayak was built beside: devices,
sensors, an org tree, a historian, alerts. kayak reaches it through one
connection and two components — the `indu` **input** reads sensors and streams
out of the platform live, and the `indu` **output** writes a pipeline's results
back into it as *streams*, series that are not sensors.

## the connection

```yaml
# pipelines.connections.yaml
indu:
  type: indu
  url: https://app.acme.indu.cloud
  ingest_url: https://ingest.acme.indu.cloud   # only when ingest has its own host
  api_key: ${INDU_API_KEY}
```

`url` is the deployment's origin. `ingest_url` is for a deployment that serves
`/ingest/v1/…` on a separate host, which the single-server install does; leave
it out and ingest is reached under `url`. `api_key` is an Indu API key —
minted on the platform's *API keys* page or by `indud apps register --kind
kayak`, bound to a role there — and arrives here as a `${NAME}` reference like
every other credential. A connection rather than fields on the components, for
the reason every other server-backed component has one: the origin and the key
are *what the platform is*, and every `indu` input and output in the graph
names the same one.

## the input

```yaml
inputs:
  - type: indu
    connection: indu
    sensors:
      - press-3/temperature
      - press-3/pressure
    streams:
      - press-3/oee
    backfill: true
```

A live subscription over the platform's server-sent-events endpoint, one
message per reading:

```json
{"kind": "sensor", "name": "press-3/temperature", "device": "press-3",
 "sensor": "temperature", "label": "Temperature", "unit": "°C",
 "sensor_id": "…", "device_id": "…",
 "at": "2026-09-02T10:00:00+00:00", "ts": 1788343200000, "value": 71.2}

{"kind": "stream", "name": "press-3/oee", "label": "press-3/oee", "unit": "%",
 "stream_id": "…", "at": "…", "ts": 1788343201000, "value": 0.83}
```

- **`sensors`** — `<device>/<sensor>`: the device's id, a slash, the
  sensor's id, both as the platform names them. Not UUIDs — the names a
  person knows, and the names on the platform's own screens. The split is at
  the first slash.
- **`streams`** — by the name a stream was written under (its `external_id`),
  or, for a stream the platform computes itself, its display name.
- **`backfill`** — start each series from its latest value, so a pipeline
  restarted at 03:00 has a value for every machine at 03:00 rather than at
  the next reading. On by default.
- **`max_batch`** — most readings in one batch; one unless said otherwise,
  and never a wait for more.

The names are resolved through the platform's API on the first read, under
the connection's key. A name that cannot be found — a typo, a stream nobody
has written yet, a sensor the key may not see — is reported on the card with
every missing name listed, and looked for again after a pause, the way a
broker that is down is: the usual case is a stream another pipeline is about
to create, and a pipeline that refused to start over it would have to be
started again by hand.

A dropped connection reconnects with kayak's backoff, one error on the card
per outage. Readings the platform dropped because this connection could not
keep up arrive as an error too — `indu dropped 12 readings…` — rather than
as a silent gap: the historian has them, the pipeline does not. `_meta.event`
under an `envelope` says which platform event carried a message, `reading`
or `stream_reading`.

## the output

```yaml
outputs:
  - type: indu
    connection: indu
    series:
      - stream: "{machine}/oee"
        value: oee
        unit: "%"
      - stream: "{machine}/availability"
        value: stats.availability
    at: _meta.received_at
```

Every message yields one reading per entry in `series`. A reducer emitting
`{machine, oee, stats: {availability}}` with the two entries above writes two
streams per machine — `press-3/oee`, `press-3/availability` — and one output
serves every machine the pipeline reduces over, because `stream` may carry
`{field}` placeholders filled from the message.

- **`stream`** — the stream's name on the Indu side, its `external_id`. An
  unknown name is created on the platform on first sight, when the
  connection's key may create streams; from then on it is a series like any
  sensor's — charted in the historian, alerted on, placed in the org tree.
- **`value`** — the field holding the number, as a path. A message where it is
  missing, or not a number, is skipped *for that series* rather than failing
  the batch: a reducer that emits `oee` for some machines and not others is
  not an error. The same goes for a message missing a placeholder's field.
- **`unit`** — recorded when the stream is created, ignored afterwards.
- **`at`** — the field holding the reading's time, as an RFC 3339 string or
  epoch milliseconds. Absent, the time the batch is sent is used. An
  `envelope` on the input puts the receive time at `_meta.received_at`, which
  is usually the honest answer.

What fails the batch: **anything but a full acceptance.** Indu answers `207`
for a batch it partly refused, and the refused rows are terminal — a stream
the key may not write to, a value that is not a number — so a `207` is
reported with the first row error quoted rather than counted as delivered.
The same backoff gate the `http` output uses holds the next batches after a
failure, so a platform that is down gets one attempt every few seconds rather
than one per message. Each batch carries an idempotency key, so one kayak
sends twice — a reconnect, a restart mid-flight — lands once.

Nothing is connected at startup: like the `http` output, there is no request
to make that would not be a delivery. A bad origin or an empty key is caught
at build time; an unreachable platform is heard about on the first batch.

## what it looks like from the other side

On the Indu side the stream appears under *Other streams* in the namespace
until someone places it at an org node, with the unit the first batch named.
The historian charts it live as the batches arrive; an alert can watch it; the
assistant can find it. `docs/streams.md` in the `indud` repository is the
design this output is the kayak half of.
