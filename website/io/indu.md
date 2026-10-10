# indu

Indu Cloud is an industrial data platform with devices, sensors, an org tree, a
historian and alerts. kayak connects to it with one connection kind and two
components:

- the `indu` **input** reads sensors and streams from the platform, live;
- the `indu` **output** writes the results of a pipeline back to the platform
  as *streams*. A stream is a series that is not a sensor.

## the connection

```yaml
# pipelines.connections.yaml
indu:
  type: indu
  url: https://app.acme.indu.cloud
  ingest_url: https://ingest.acme.indu.cloud   # only when ingest has its own host
  api_key: ${INDU_API_KEY}
```

- **`url`**: the origin of the deployment.
- **`ingest_url`**: set this only when the deployment serves `/ingest/v1/…` on
  a different host. The single-server install does this. Without it, kayak
  sends ingest requests to `url`.
- **`api_key`**: an Indu API key, as a `${NAME}` reference. Make the key on the
  *API keys* page of the platform, or with `indud apps register --kind kayak`.
  The key gets a role there.

All `indu` inputs and outputs in the graph can use the same connection.

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

The input subscribes to the server-sent-events endpoint of the platform. It
sends one message for each reading:

```json
{"kind": "sensor", "name": "press-3/temperature", "device": "press-3",
 "sensor": "temperature", "label": "Temperature", "unit": "°C",
 "sensor_id": "…", "device_id": "…",
 "at": "2026-09-02T10:00:00+00:00", "ts": 1788343200000, "value": 71.2}

{"kind": "stream", "name": "press-3/oee", "label": "press-3/oee", "unit": "%",
 "stream_id": "…", "at": "…", "ts": 1788343201000, "value": 0.83}
```

- **`sensors`**: each entry is `<device>/<sensor>`, with the ids that the
  platform shows. These are names, not UUIDs. kayak splits the entry at the
  first slash.
- **`streams`**: the name that the stream was written under (its
  `external_id`). For a stream that the platform calculates, use its display
  name.
- **`backfill`**: start each series with its latest value. The default is
  `true`. Thus a pipeline that restarts at 03:00 has a value for each machine
  at 03:00.
- **`max_batch`**: the largest number of readings in one batch. The default is
  1. The input does not wait for a batch to fill.

kayak looks up the names through the platform API on the first read, with the
key of the connection. If a name is not found, the input reports an error that
lists all the missing names. Then it tries again after a pause. A stream that
another pipeline creates soon is the usual cause, so the pipeline does not stop.

When the connection drops, the input reconnects with backoff. It reports one
error for each outage. If the platform drops readings because the connection
was too slow, the input reports an error, for example `indu dropped 12
readings…`. The historian keeps those readings, but the pipeline does not get
them.

With an `envelope`, `_meta.event` gives the platform event that carried the
message: `reading` or `stream_reading`.

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

The output writes one reading for each entry in `series`, for each message.
For example, a reducer sends `{machine, oee, stats: {availability}}`. With the
two entries above, the output writes two streams for each machine:
`press-3/oee` and `press-3/availability`.

- **`stream`**: the name of the stream in Indu (its `external_id`). It can
  hold `{field}` placeholders, which the output fills from the message. Thus
  one output serves all machines. If the key can create streams, Indu creates
  an unknown stream when it first sees it. After that, the stream is a series
  like a sensor: the historian charts it, alerts can watch it, and you can put
  it in the org tree.
- **`value`**: the field path of the number. If the field is missing or is not
  a number, the output skips that message for that series only. The batch does
  not fail. A message without the field of a placeholder is also skipped for
  that series.
- **`unit`**: Indu records the unit when it creates the stream. After that,
  Indu ignores it.
- **`at`**: the field that holds the time of the reading, as an RFC 3339 string
  or as epoch milliseconds. Without it, the output uses the time when it sends
  the batch. An `envelope` on the input puts the receive time at
  `_meta.received_at`.
- **`timeout_seconds`**: the longest time for one request. The default is 30.

### errors and retries

- **Only a full acceptance is a success.** Indu returns `207` when it refuses
  some rows. A refused row is a permanent error, for example a stream that the
  key cannot write to. The output reports a `207` as a failure and quotes the
  first row error.
- **After a failure, the output waits before the next request.** During the
  backoff, the next batches fail at once. A platform that is down gets one
  attempt every few seconds.
- **Each batch has an idempotency key.** If kayak sends a batch two times, for
  example after a reconnect, Indu writes it one time.
- **The output does not connect at startup.** A bad origin or an empty key
  fails at build time. An unreachable platform fails the first batch.

## on the indu side {#what-it-looks-like-from-the-other-side}

Indu shows a new stream under *Other streams* in the namespace, with the unit
of the first batch. Move it to an org node to place it in the tree. The
historian charts the stream live as the batches arrive.
