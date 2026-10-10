# streaming statistics

Seven transforms keep a small state for each key and calculate as readings
arrive: `deadband`, `throttle`, `derive`, `rolling`, `smooth`, `detect` and
`resample`. Use them for these tasks:

- reduce the data at the edge: `deadband`, `throttle`, `resample`;
- check the health of a sensor: the flatline of `deadband`, and `detect`;
- calculate rates from counters: `derive`;
- calculate rolling averages and control charts: `rolling`, `smooth`, `detect`.

## the shape they share

All seven use the same settings:

- **`group_by` is the key.** It is the same list as in the reducer. Use it for
  one series for each machine, or for each machine and signal. Without
  `group_by`, there is one series for the whole stream.
- **The state is in the [state bucket](/pipelines/state) of the pipeline.** A
  stateful transform in a pipeline with no `state` block does not build. The
  `max_keys` and `idle_timeout_secs` of the bucket set the limits of every
  window and every last value. A series that is idle for longer than the
  timeout starts again with no state.
- **`time` uses [the time rule](/pipelines/time-and-numbers):** an RFC 3339
  string or epoch milliseconds. Without `time`, kayak uses the arrival time.
  Only the transforms that measure per second or by age have `time`.
- **`on_missing`** sets what happens to a message without the field or without
  its key. `error` is the default and fails the batch. `skip` sends the message
  on with no change. A value that is present and is not a number is always an
  error.
- **`when` and `reset_when`** are lists of [conditions](/pipelines/state), the
  same as for `filter` and `remember`. See below.

```json
{
  "id": "line_3_temperatures",
  "inputs": [{ "type": "nats", "connection": "plant", "subject": "line3.*.temperature",
               "envelope": { "type": "merge" } }],
  "state": { "bucket": "line_3" },
  "transforms": [
    { "type": "smooth",   "field": "value", "method": { "type": "hampel", "size": 7 },
      "group_by": ["_meta.subject"] },
    { "type": "deadband", "field": "value", "delta": 0.5, "max_seconds": 60,
      "flatline_seconds": 600, "group_by": ["_meta.subject"] },
    { "type": "detect",   "field": "value", "method": { "type": "zscore", "size": 60 },
      "group_by": ["_meta.subject"] }
  ],
  "outputs": [{ "type": "clickhouse", "connection": "warehouse", "table": "line3_temps" }]
}
```

This pipeline removes spikes, drops readings that did not change, and flags
readings that changed too much. It uses one bucket and one key.

## when, and starting over

A stream often contains more than one kind of message. For example, an
industrial feed sends the state of a machine (`RUNNING`, `OFF`) as separate
messages beside the readings. A state is a string, and these transforms refuse
a value that is not a number. `when` selects the messages that a transform
uses. The other messages go through with no change:

```yaml
- type: smooth
  field: value
  method: { type: ewma, tau_seconds: 180 }
  time: ts
  as: smoothed
  when:
    - { type: string, field: sensor, operator: equal_to, value: pump_vibration }
  reset_when:
    - { type: string, field: value, operator: equal_to, value: "OFF" }
```

A message that matches `reset_when` clears the state of its key. The series
starts again from the next reading. Use it when a machine stops or when a part
is replaced.

- kayak checks `reset_when` before `when`. In the example, the `OFF` message
  resets the average and goes through.
- All conditions in a list must be true.
- A message that does not match `when` needs no `group_by` field.
- Under `resample`, a message that does not match `when` is not part of the
  series.

## deadband

`deadband` drops a message unless the field changed by more than `delta` since
the last message that passed. `delta` is in the units of the field. With
`mode: percent`, it is a percentage of the last value that passed. The first
message for each key always passes.

Two timers add to the band:

- `max_seconds` passes a message when that time has gone by since the last
  message passed. Thus a steady value is confirmed at intervals.
- `flatline_seconds` checks the health of the sensor. If the value has not
  changed for that time, the next message passes with `stuck: true`. This
  occurs one time for each flat period. The flat period starts at the last
  *change*, so a `max_seconds` confirmation does not reset it.

## throttle

`throttle` passes at most one message for each key every `seconds`. It drops
the other messages. The first message for each key passes. After that, the next
message passes when `seconds` have gone by since the last message passed.

```yaml
- type: throttle
  seconds: 5
  group_by: [machine]
  time: ts
```

`throttle` uses only the clock. It does not read a value, so the messages that
pass keep all their fields. Put it in front of an output that only needs a
message every few seconds.

- The interval starts at the message that passed. It does not follow a fixed
  grid.
- `throttle` holds nothing back. A key that becomes quiet sends nothing until
  its next message.
- With a `time` field, a message that is earlier than the last message that
  passed is dropped.

To get the *last* value of each interval, or a value from a quiet key, use
[`resample`](#resample).

## derive

`derive` writes a value that needs the previous message:

- `rate`: the change per second, from the `time` field;
- `delta`: the change;
- `cumsum`: the sum so far;
- `counter`: the total of a counter, through resets and wraps.

Each function writes under its own `as`:

```json
{ "type": "derive", "group_by": ["meter"], "time": "ts",
  "derive": [
    { "function": "rate",    "field": "pulses", "as": "pulses_per_second" },
    { "function": "counter", "field": "pulses", "as": "pulses_total", "wrap_at": 65536 }
  ] }
```

The first message for each key has no previous message. `rate` and `delta`
write `null` for it. If a `counter` value goes down, and `wrap_at` is set,
kayak reads it as a wrap through the top. Without `wrap_at`, kayak reads it as
a reset.

## rolling

`rolling` calculates the reducer's aggregations over the last `size` messages of
a series. It writes the results onto each message. It uses the same
`{function, field, as}` list as the reducer, for example `avg`, `stddev`, `min`,
`max`, `median` and `slope`. It sends one message for each message it gets.

- `size` is required. It is the limit of the window.
- `seconds` also removes readings older than that time, from the `time` field.
- The window includes the current message. The first message for each key gets
  a window of one.
- `count` needs a `field` here. Use it with a `filter` downstream to skip the
  warm-up.

## smooth

`smooth` smooths a field against the values before it. It writes the result to
the same field, or under `as`. `method.type` selects one of four methods:

| | |
|---|---|
| `ewma` | exponentially weighted, by `alpha` or `half_life` in messages, or by `tau_seconds` in time. It needs no window. |
| `median` | the median of the last `size`, the current value included. It removes single spikes. |
| `hampel` | keeps the value, unless it is more than `threshold` scaled MADs from the median of the window. Then it writes the median. Use it in front of a detector. |
| `savitzky_golay` | fits a polynomial of `order` to the last `size` and gives its value at the newest point. It keeps the shape of peaks. |

`savitzky_golay` uses only past values, because a stream has no future values.
Until the window has more than `order` values, the reading goes through with no
change.

`alpha` and `half_life` count readings. Use them only for a series with a
steady rate. **`tau_seconds` counts time.** Each reading moves the average by
`1 − e^(−Δt/τ)` of the distance to the reading. Δt is the time since the
previous reading. Use `tau_seconds` for a sensor that reports on change, or for
a feed that stops and catches up:

```yaml
- type: smooth
  field: value
  method: { type: ewma, tau_seconds: 180 }   # a three-minute time constant
  time: ts
  as: smoothed
```

Only `ewma` with `tau_seconds` reads `time`. The other methods use the order of
the readings. Thus `smooth` refuses `time` with the other methods. A reading
older than the last one does not change the average.

## detect

`detect` flags anomalies in a field against its own series. It writes a boolean
under `as` (default `anomaly`). It writes the score beside it as
`<as>_score`. The score tells how far outside normal the reading is, in the
units of the method. A `filter` downstream can use a stricter limit.
`mode: only_anomalies` sends only the anomalies.

**kayak flags nothing during the warm-up** of `min_samples` messages for each
key. The methods are:

- **`zscore` and `mad`** compare a reading with the `size` readings *before*
  it. The window does not include the reading, so a spike does not change its
  own baseline. `mad` is robust. Use it when the baseline contains outliers.
- **`cusum`, `ewma_chart` and `western_electric`** are control charts. They
  fix their baseline at the end of the warm-up. `cusum` sums the drift from a
  `target`, or from the warm-up mean. It finds a small shift that continues.
  `ewma_chart` is the smoothed version. `western_electric` applies the four
  classic rules and writes the rule that fired under `<as>_rule`.
- **`flatline`** flags when the last `size` values are identical.
- **`ewma`** has no window. Normal is an exponentially weighted mean and
  spread. Each has its own time constant: `mean_tau_seconds` and
  `spread_tau_seconds`. Use it for a series that drifts slowly and arrives at
  irregular times. `min_spread` is the smallest deviation that the method
  accepts, in the units of the field. Without it, a signal that was perfectly
  flat flags its first small change. `ewma` reads `time`.

### what normal learns from

`zscore`, `mad` and `ewma` continue to learn. By default they learn from every
reading, anomalies included. Thus a change that continues becomes the new
normal.

- `learn: normal_only` keeps flagged readings out of the baseline. A step then
  stays flagged while it continues.
- `readapt_after_seconds` sets how long a run of flagged readings can continue
  before the method learns from them. Use it with `normal_only`, so that a
  machine at a new level does not stay flagged.

```yaml
- type: detect
  field: value
  method: { type: ewma, mean_tau_seconds: 120, spread_tau_seconds: 600,
            threshold: 4, min_spread: 1.0 }
  min_samples: 120
  learn: normal_only
  readapt_after_seconds: 300
  with_baseline: true
  time: ts
  group_by: [sensor]
```

`with_baseline: true` writes the baseline beside the flag and the score:

- `<as>_expected` is the normal value.
- `<as>_band` is the distance from normal that counts as an anomaly.

Both are in the units of the field. Use them to draw a normal band on a chart.
Both are `null` during the warm-up. They are also `null` where a method has no
value in those units. The `cusum` band is `null`, and `flatline` has neither.

Only `ewma` and `readapt_after_seconds` read `time` on `detect`. kayak refuses
`time` with the other methods.

## resample

`resample` puts a series on a regular grid. It sends one message for each key
every `interval_seconds`, at times that are multiples of the interval. It
changes the shape of the stream. Each message it sends is new and contains:

- the `group_by` fields;
- the grid time, under the name of the `time` field, or `time`;
- the value, under `as`.

A grid point is the start of its interval. The `method` sets how the readings
in an interval become one value, and when kayak sends it:

- `last` and `mean` send an interval when a reading after the interval arrives.
  They skip an empty interval.
- `linear` sends a grid point when the first reading at or after it arrives. It
  interpolates against the last reading before the point.
- `forward_fill` repeats the last reading for an empty interval, for up to
  `max_gap_seconds`. Use it to make a regular series from a sparse signal, for
  example the output of a `deadband`.

::: tip the clock
Without a `time` field, kayak uses the wall clock. A `forward_fill` series then
continues to send grid points when its readings stop. With a `time` field, the
readings set the clock. An open interval of a quiet key then waits for the next
reading of that key.
:::

## what is not here {#what-is-deliberately-not-here}

A session window, that is a `buffer` with `until` for each key, is not
available. None of these transforms train a model. To use a trained model, call
it with the [`http` transform](/pipelines/model-round-trip).
