# the model round trip

Kayak runs models; it does not fit them. Anything that has to be *trained*
lives behind an http endpoint where the data scientists are, and this page is
about making that a good place to be rather than a dead end: a window of
readings goes out as the handful of numbers a model wants, and the answer
comes back onto the message that asked, with every identifier intact.

```json
{
  "id": "cycle_quality",
  "inputs": [{ "type": "pipeline", "upstream": "cycles" }],
  "transforms": [
    { "type": "features", "field": "temperature", "group_by": ["machine_id", "unit_id"],
      "time": "ts", "include": ["mean", "std", "slope", "rms", "n_peaks", "duration"] },
    { "type": "http", "url": "https://models.example/quality", "verb": "POST",
      "body": "message", "response": "merge", "as": "prediction",
      "auth": { "type": "bearer", "token": "${MODEL_TOKEN}" }, "retries": 2 }
  ],
  "outputs": [{ "type": "nats", "connection": "plant", "subject": "kayak.quality" }]
}
```

Each cycle arrives as a batch, leaves `features` as one message —
`{machine_id, unit_id, mean, std, slope, rms, n_peaks, duration}` — goes to
the model as exactly that, and comes back as the same message with
`prediction` written on it. The nats output publishes it with the machine and
unit still there to route by.

## features: the window as seven numbers

A model endpoint rarely wants four hundred raw temperatures, and a vibration
waveform should never leave the edge at all. `features` folds a window into
one descriptor per group: the reducer's shape — a batch in, one message per
`group_by` combination out, the group fields kept under their leaf names —
with a closed set of waveform descriptors in place of the aggregation list.
Pair it with a [`buffer`](/pipelines/pipelines#buffering-an-input) or a
session's worth of readings, or it only ever sees one at a time.

`include` picks from: `mean`, `std`, `min`, `max`, `range`, `slope`, `skew`,
`kurtosis`, `rms`, `crest_factor`, `zero_crossings`, `n_peaks`, `autocorr_1`,
`dominant_frequency`, `count`, `duration`. Each is written under its own
name. `bands` adds the power in named frequency bands:

```json
{ "type": "features", "field": "vibration", "group_by": ["_meta.machine_id"],
  "sample_rate_hz": 2000,
  "include": ["rms", "crest_factor", "kurtosis", "dominant_frequency"],
  "bands": [{ "low_hz": 10, "high_hz": 100, "as": "low_band" },
            { "low_hz": 100, "high_hz": 1000, "as": "high_band" }] }
```

Three things to know. `slope` and `duration` are in seconds off the `time`
field, and per message without one. The spectral features
(`dominant_frequency`, `bands`) need a sample rate: `sample_rate_hz` when
given, which wins because a source with coarse timestamps would derive
nonsense, and otherwise one derived from `time`; with neither, they refuse to
build. And a feature the window can't answer — the slope of one reading, a
tone in a flat signal — is `null` rather than a failed batch, because a short
cycle is still data. Band power is in mean-square units, so the bands of a
signal sum to its `rms` squared.

Nothing here keeps state, so unlike the
[streaming statistics](/pipelines/streaming-statistics) it needs no bucket.

## the http transform: out, and back onto the message

The `http` transform sends the batch somewhere and carries on with the
answer. What it always did — and still does by default — is `response:
replace`: the reply *is* the new batch, so the service on the other end is
the transform. That is right when the service reshapes the data. It is wrong
for a model: a service that answers `{"score": 0.93}` has thrown away the
machine id the pipeline needs to publish that under.

`response: merge` is the round trip. The reply is written onto the message
that caused it, under `as`, and nothing the pipeline sent is lost. Under
`body: message` each message goes on its own and gets its own reply. Under
`body: batch` the whole batch goes as one array, and a reply that is an array
of the batch's length is a verdict per message, written element-wise; any
other reply is a verdict on the batch and goes onto every message.

The shape of the request is otherwise the shape of the message — that is what
`features`, `reduce` and `map` in front of it are for — with two knobs for
the API's own conventions: `wrap` puts the body under a key
(`{"instances": [...]}`), and `unwrap` reads the reply out from under one
(`{"predictions": [...]}`).

The rest is what any endpoint needs: `auth` is the same block the http input
and output take, `timeout_seconds` bounds a request (thirty seconds when left
out), and `verb` is honoured — `GET` and `DELETE` are refused, since a request
with no body sends no messages.

## when it fails

Anything but a 2xx fails the batch, with the endpoint's own words quoted on
the card. Two mechanisms sit around that, and they are different things:

- **`retries` sleeps.** A request that failed to reach the endpoint, or was
  answered 5xx or 429, is tried again that many times, each wait a little
  longer than the last, *inside the pass* — because failing a batch of real
  readings over a proxy hiccup is worse than a second's delay. A 4xx is not
  retried: the endpoint has said no, and asking again is not going to change
  its mind. Zero when left out.
- **The gate skips.** Once a batch has failed for good, the next batches are
  refused without a round trip until the backoff says to try again — the same
  thing the http and clickhouse outputs do, and what keeps a down endpoint from
  being hammered on every batch.

They compose: each pass the gate allows may retry.

## seeing it run

`heartbeat_features` in the [sample](/pipelines/the-sample) runs the whole
loop against the server's own `ingest` endpoint, so it works on a bare
`just dev`: a ten-second window off the heartbeat, seven features, a POST of
the descriptor, and `{"accepted": 1}` merged back under `ingest`.
