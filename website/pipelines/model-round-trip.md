# the model round trip

kayak runs models. It does not train them. A trained model runs in one of two
places:

- **In the pipeline**, as an ONNX file that the [`infer`](#infer) transform
  runs. There is no network hop and no extra service, which suits edge
  devices. A model update is a pipeline replace.
- **Behind an HTTP endpoint**, which the rest of this page is about. The model
  server owns the model's lifecycle (deployments, canaries, GPUs) and kayak
  never restarts for it.

You can use both, one or the other per pipeline.

## infer: an ONNX model in the pipeline {#infer}

Export the model to ONNX (`skl2onnx`, `torch.onnx.export`, `tf2onnx`) and put
the file beside the config, for example under `models/`. The model needs one
float input and one float output. `shape` says how messages become its input:

- `rows`: each message is a row, and the batch is a `[messages, inputs]`
  tensor. Use it for scorers and classifiers. Each message gets its answer
  under the names in `outputs`.
- `window`: the last `size` readings of each series, `[1, size, inputs]`. Use
  it for forecasters. The window is kept in the pipeline's state bucket, keyed
  by `group_by`.

```json
{
  "id": "pump_forecast",
  "state": { "bucket": "pump_windows" },
  "inputs": [{ "type": "http" }],
  "transforms": [{
    "type": "infer",
    "model": { "type": "file", "path": "models/pump_forecast_v1.onnx" },
    "shape": { "type": "window", "size": 24, "group_by": ["pump"], "time": "time",
               "emit": { "type": "points", "step_seconds": 0.5,
                         "mark": { "field": "origin", "inferred": "forecast",
                                   "observed": "observed" } } },
    "inputs": ["flow"],
    "outputs": ["flow"],
    "provenance": "model"
  }],
  "outputs": [{ "type": "http", "url": "http://127.0.0.1:6768/forecasts", "body": "batch" }]
}
```

kayak loads the model and runs it once on zeros when it builds the pipeline.
A wrong path, an input or output of the wrong shape, or an operator kayak
cannot run stops the build with a message that names the model.

The model runs on the CPU, on a thread of its own, so a slow model does not
hold up other pipelines. It does hold up its own pipeline: a batch waits for
the model. The weights stay in the server's memory, and running out of memory
stops the whole server. Use `infer` for small, fast models. Put a large
model, or one that needs a GPU, [behind http](#behind-http).

### telling forecasts from readings

A consumer of a forecast nearly always needs to know which values the model
made. The answer depends on `emit`:

- `merge` (the default) writes the forecast onto the reading that completed
  the window, as an array of `horizon` values under each output name. The
  field name is the label.
- `points` sends one new message per step ahead, after the reading. Each has
  the `group_by` fields, `step` (1 to the horizon), the outputs and, with
  `step_seconds`, a time. `mark` writes one field that tells the two kinds
  apart: `inferred` on the points and, if you give it, `observed` on everything
  else. With the same output name as the input (`flow` above), a consumer reads
  one field and filters on the label.

`provenance` writes the model's path and sha256 onto everything the model
made. It is what tells a consumer, or an audit a year later, which model made
a value.

### changing the model

Write the new model under a **new file name** and replace the pipeline with
the new `path` (`PUT /api/pipelines/{id}`, or the card's edit button). kayak
loads and trial-runs the new model before it stops the old one. A model that
cannot run is refused and the old one goes on running. The window is in the
state bucket, so a new model with the same `size` forecasts from its first
reading. To roll back, replace again with the old path.

Do not overwrite the file in place. If the new model is broken, rebuilding the
old config reads the same broken file. A replace with an unchanged config also
reads the file again, so in-place overwrites do work, without the safety net.

`examples/forecast/` in the repository is the whole loop: a training script, a
service that feeds the pipeline and scores its forecasts, and a script that
swaps the model while it runs.

## behind http

Two transforms connect a pipeline to a model behind an HTTP endpoint:

- `features` makes one message with a small set of numbers from a window of
  readings.
- The `http` transform sends that message to the model and writes the answer
  back onto the message. The identifiers stay on the message.

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

Each cycle arrives as a batch. `features` makes one message from it:
`{machine_id, unit_id, mean, std, slope, rms, n_peaks, duration}`. The `http`
transform sends that message to the model. It writes the answer onto the same
message under `prediction`. The nats output publishes the message with the
machine and the unit still on it.

## features: the window as seven numbers

`features` makes one message for each `group_by` combination in a batch. It
keeps the group fields under the last segment of their path, the same as the
reducer. Use it to send a small descriptor to a model, not the raw readings.
Put a [`buffer`](/pipelines/pipelines#buffering-an-input) on the input. Without
a buffer, each batch can have only one reading.

`include` selects from these features: `mean`, `std`, `min`, `max`, `range`,
`slope`, `skew`, `kurtosis`, `rms`, `crest_factor`, `zero_crossings`,
`n_peaks`, `autocorr_1`, `dominant_frequency`, `count`, `duration`. kayak writes
each feature under its own name. `bands` adds the power in named frequency
bands:

```json
{ "type": "features", "field": "vibration", "group_by": ["_meta.machine_id"],
  "sample_rate_hz": 2000,
  "include": ["rms", "crest_factor", "kurtosis", "dominant_frequency"],
  "bands": [{ "low_hz": 10, "high_hz": 100, "as": "low_band" },
            { "low_hz": 100, "high_hz": 1000, "as": "high_band" }] }
```

Rules:

- `slope` and `duration` are in seconds, from the `time` field. Without `time`,
  they are per message.
- The spectral features (`dominant_frequency`, `bands`) need a sample rate.
  `sample_rate_hz` sets it. Without `sample_rate_hz`, kayak calculates it from
  `time`. With neither, the transform does not build. Set `sample_rate_hz` for
  a source with coarse timestamps.
- A feature that the window cannot calculate is `null`. Examples are the slope
  of one reading, or a tone in a flat signal. The batch does not fail.
- Band power is in mean-square units. The bands of a signal sum to the square
  of its `rms`.

`features` keeps no state. It does not need a bucket, unlike the
[streaming statistics](/pipelines/streaming-statistics).

## the http transform: out, and back onto the message

The `http` transform sends the batch to an endpoint. `response` sets what it
does with the reply:

- `replace` is the default. The reply becomes the new batch. Use it when the
  service changes the shape of the data.
- `merge` writes the reply onto the message that caused it, under `as`. The
  message keeps all its fields. Use it for a model, which usually replies only
  with a result, for example `{"score": 0.93}`.

`body` sets how kayak sends the batch:

- `body: message` sends each message in its own request. Each message gets its
  own reply.
- `body: batch` sends the whole batch as one array. If the reply is an array
  with the same length as the batch, kayak writes element *n* onto message *n*.
  kayak writes any other reply onto every message.

The request body is the message. Use `features`, `reduce` and `map` in front of
the transform to give it the correct shape. Two settings adapt to the
conventions of an API:

- `wrap` puts the body under a key, for example `{"instances": [...]}`.
- `unwrap` reads the reply from under a key, for example
  `{"predictions": [...]}`.

Other settings:

- `auth` is the same block as on the http input and output.
- `timeout_seconds` is the limit for one request. The default is 30 s.
- `verb` sets the method. kayak refuses `GET` and `DELETE`, because a request
  without a body sends no messages.

## when it fails

A reply that is not 2xx fails the batch. The error contains the text from the
endpoint. Two settings control what happens around a failure:

- **`retries`** sets how many times to try a request again in the same pass.
  kayak retries when the request did not reach the endpoint, or when the reply
  is 5xx or 429. Each wait is longer than the one before. A 4xx is not retried.
  The default is 0.
- **The gate** applies after a batch fails on all its tries. kayak then refuses
  the next batches without a request, until the backoff ends. The http and
  clickhouse outputs do the same. This prevents a stream of requests to an
  endpoint that is down.

The two work together. Each pass that the gate allows can retry.

## an example that runs {#seeing-it-run}

`heartbeat_features` in the [sample](/pipelines/the-sample) runs the full round
trip against the `ingest` endpoint of the same server. It needs no other
service. It does these steps:

1. It collects a ten-second window from the heartbeat.
2. It calculates seven features.
3. It posts the descriptor to `ingest`.
4. It merges the reply `{"accepted": 1}` back under `ingest`.
