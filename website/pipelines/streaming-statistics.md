# streaming statistics

Seven transforms that each keep a little state per key and do arithmetic over
it as readings arrive — the first tier of statistics in the stream, and the
one that needs no dependency at all. Most of the value for the industrial
cases lives here: edge data reduction (`deadband`, `throttle`, `resample`), sensor health
(`deadband`'s flatline, `detect`), rates off counters (`derive`), and the
rolling averages and control charts a plant floor has always run (`rolling`,
`smooth`, `detect`).

## the shape they share

Every one of them is configured the same way, and the sameness is the design:

- **`group_by` is the key.** The reducer's list: a series per machine, or per
  machine and signal. Leave it out for one series over the whole stream.
- **State lives in the pipeline's [state bucket](/pipelines/state).** A
  stateful transform in a pipeline with no `state` refuses to build, the way
  `recall` does. That is what bounds it: the bucket's `max_keys` and
  `idle_timeout_secs` are the bound and the expiry on every window and every
  last-seen value, and the state tab shows them. A series that goes idle past
  the timeout starts again from nothing.
- **`time` is read by [the one rule](/pipelines/time-and-numbers)** — an RFC
  3339 string or epoch milliseconds — and is arrival time when left out.
  Only the transforms that measure something per second or by age carry it.
- **`on_missing`** says what happens to a message without the field or
  without its key: `error` fails the batch (the default, as it is the
  reducer's), `skip` passes the message through untouched. A value that is
  present and isn't a number is an error whatever `on_missing` says.
- **`when` and `reset_when`** are [conditions](/pipelines/state), the list
  `filter` and `remember` take. A message `when` doesn't match passes through
  untouched and leaves the state alone; a message `reset_when` matches clears
  its key's state first, so the series starts over. More on both below.

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

Clean the spikes, throw away the readings that didn't move, flag the ones that
moved too much — with one bucket declared at the top of the file and one key
named three times.

## when, and starting over

A stream rarely carries one kind of message. An industrial feed sends a
machine's *state* (`RUNNING`, `OFF`) beside its readings, as messages of their
own, and a state is a string — which every transform here would reject as a
present non-number. `when` says which messages a transform is about; the rest
pass through it untouched, so the state is still there for what comes after:

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

`reset_when` is the other half: a message it matches clears its key's state,
so the series starts over from the next reading rather than averaging across a
machine that was switched off, or a part that was replaced. It is checked
before `when` and whatever `when` says — here the state message resets the
average and is itself passed through. Both are lists of the same conditions
`filter` takes, and all of a list has to hold.

Neither condition is about the key: a message `when` doesn't match needs no
`group_by` field, since the transform has nothing to say about it. Under
`resample`, which emits grid points rather than the messages it is given, a
message `when` doesn't match is simply not part of the series.

## deadband

Drops a message unless the field moved by more than `delta` — in the field's
units, or as a `percent` of the last value that passed — since the last
message that passed. The first per key always passes. It is the single most
used transform in any historian pipeline, and a *stateful* filter, which is
why `filter` cannot be it.

Two clocks sit beside the band. `max_seconds` passes a message anyway once
that long has gone by since anything passed, so a steady value is still
confirmed now and then. `flatline_seconds` is the sensor-health half: when the
value has not moved in that long, the next message passes carrying
`stuck: true`, once per flat stretch, so a stuck instrument is distinguishable
downstream from a quiet one. The stretch is measured from the last *change*,
so a `max_seconds` confirmation of a stuck value does not reset it.

## throttle

Passes at most one message per key every `seconds` and drops the rest. The
first message per key passes, and after that the first one at least `seconds`
after the last that passed:

```yaml
- type: throttle
  seconds: 5
  group_by: [machine]
  time: ts
```

Where `deadband` decides by the *value*, `throttle` decides by the clock alone,
so the messages it lets through are whole — every field intact. That makes it
the one to put in front of an output writing several fields per message, which
is the usual reason for wanting it: a computation that can answer on every
reading, feeding a sink that only needs to hear every few seconds.

The interval runs from the message that passed, not from a clock grid, and
nothing is held back to be sent later — a key that goes quiet mid-interval sends
nothing more until its next message, and a message earlier than the last one
passed (a late reading, under a `time` field) is dropped. When what matters is
the *last* value of each interval, or a quiet key should still report, that is
[`resample`](#resample).

## derive

Writes onto each message something that needs the previous one — a `rate`
per second (off the `time` field), a `delta`, a `cumsum`, or a `counter`
that survives resets and wraps. Several at once, each under its own `as`:

```json
{ "type": "derive", "group_by": ["meter"], "time": "ts",
  "derive": [
    { "function": "rate",    "field": "pulses", "as": "pulses_per_second" },
    { "function": "counter", "field": "pulses", "as": "pulses_total", "wrap_at": 65536 }
  ] }
```

The first message per key has no previous, and `rate` and `delta` write
`null` for it. A `counter` reads a drop below the previous value as a wrap
when `wrap_at` is set — the increase runs through the top — and as a reset
otherwise.

## rolling

The reducer's aggregations over the last `size` messages of a series, written
onto each message — `avg`, `stddev`, `min`, `max`, `median`, `slope` and the
rest, the same `{function, field, as}` list. A second component rather than
a `window` on `reduce` because the cardinality differs: one message out per
message in.

`size` is always required, because it is the bound; `seconds` on top of it
also drops what is older than that, off the `time` field. The window
includes the current message, so the first per key gets a window of one, and
`count` — which needs a `field` here — is how a downstream `filter` reads the
warm-up.

## smooth

Smooths a field against the values before it, over the field itself or under
`as`. Four methods, chosen by `method.type`:

| | |
|---|---|
| `ewma` | exponentially weighted, by `alpha` or `half_life` in messages, or by `tau_seconds` in time — cheap, no window |
| `median` | the median of the last `size`, this one included — removes single-sample spikes outright |
| `hampel` | keep the value unless it is `threshold` scaled MADs from the window's median, else the median — the right first stage in front of any detector |
| `savitzky_golay` | a polynomial of `order` fitted to the last `size`, evaluated at the newest — keeps the shape of peaks a moving average flattens |

The Savitzky–Golay here is *trailing*, because a stream cannot see the future,
and until the window holds more than `order` values the reading passes
untouched.

`alpha` and `half_life` count readings, which is only right for a series that
arrives at a steady rate. **`tau_seconds` counts time**: each reading moves the
average `1 − e^(−Δt/τ)` of the way towards it, for the Δt since the reading
before, so ten readings a second apart move it exactly as far as one reading
ten seconds after the last. That is the spelling for a sensor that reports on
change, or a feed that stalls and catches up:

```yaml
- type: smooth
  field: value
  method: { type: ewma, tau_seconds: 180 }   # a three-minute time constant
  time: ts
  as: smoothed
```

`time` is read by that method and no other — the rest are about the order of
the readings, not when they came — so `smooth` refuses a `time` beside them
rather than ignoring it. A reading older than the last one moves nothing.

## detect

Flags anomalies in a field against its own series — one component with a
`method`, the way `smooth` is. It writes a boolean
under `as` (`anomaly` when left out) and `<as>_score` beside it, how far
outside normal the reading was in the method's own units, so a `filter`
downstream can be stricter than the threshold. `mode: only_anomalies` turns
the stream into an alarm feed.

**Nothing is flagged during the warm-up** of `min_samples` messages per key,
because until then there is no idea of normal to be outside of. Two families:

- The window methods, `zscore` and `mad`, measure a reading against the
  `size` readings *before* it — never including it, so a spike does not pull
  the baseline it is measured against. `mad` is the robust one: prefer it
  when the baseline itself has outliers in it.
- The chart methods freeze their baseline at the end of the warm-up and hold
  the series to it, which is what a control chart is. `cusum` (with a
  `target`, or the warm-up mean without one) sums the drift and catches a
  small sustained shift a single-point test never sees; `ewma_chart` is the
  smoothed twin; `western_electric` is the four classic rules, and writes
  which one fired under `<as>_rule`.

`flatline` is the sixth: the last `size` values identical, a stuck instrument
seen from the detector's side.

`ewma` is the seventh, and a window method without a window: normal is an
exponentially weighted mean and spread, each following the series with a time
constant of its own (`mean_tau_seconds`, `spread_tau_seconds`), so it suits a
series that drifts slowly and arrives irregularly. `min_spread` is the smallest
deviation it will believe, in the field's units — without one, a signal that
has been perfectly flat flags its first wobble. It reads `time`.

### what normal learns from

The methods that keep learning — `zscore`, `mad` and `ewma` — learn from every
reading by default, anomalies included, so a lasting change becomes the new
normal as fast as the baseline follows anything. `learn: normal_only` keeps a
flagged reading out of its own baseline instead, so a step is flagged for as
long as it lasts. That needs a way out, or a machine that genuinely moved to a
new level is flagged for ever: `readapt_after_seconds` says how long a run of
flagged readings may last before they are learned from anyway, until the
baseline has caught up.

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

`with_baseline: true` writes what the reading was judged against beside the
flag and the score: `<as>_expected`, what normal was, and `<as>_band`, how far
from it counted — both in the field's own units, which is what a chart drawing
a normal band around a signal needs. They are `null` during warm-up, and where
a method has nothing in those units to say (`cusum`'s threshold is on the
accumulated drift, so its band is `null`; `flatline` has neither).

`time` on `detect` is read by `ewma` and `readapt_after_seconds` only, and
refused beside anything else, the way `smooth` refuses it.

## resample

Puts a series onto a regular grid — one message per key per
`interval_seconds`, at times that are multiples of it, whichever rate the
readings arrive at. The precondition every window model has, and the one
transform here that changes the stream's shape: what comes out is a new
message carrying the group fields, the grid time under the `time` field's
name (or `time`), and the value under `as`.

A grid point stands for the interval starting at it, and the `method` says
both how the readings in it become one value and when it can be emitted.
`last` and `mean` emit an interval once a reading past it arrives, and skip an
empty one. `linear` emits a grid point once the first reading at or past it
arrives, interpolated against the last before. `forward_fill` is the one that
emits from a quiet series: an empty interval repeats the last reading, for up
to `max_gap_seconds` — which is what turns a sparse change-on-value signal
(a `deadband`'s output, say) back into a regular one.

::: tip the tick
Under arrival time — no `time` field — the wall clock is the readings' clock,
and a `forward_fill` series keeps emitting while its readings have gone
quiet: this is the run loop's tick at work, the second transform to use it
after `buffer`. With a `time` field the readings' own clock is driving, and
the wall clock says nothing about whether an interval is over, so a quiet
key's open interval waits for that key's next reading.
:::

## what is deliberately not here

A per-key spelling of `buffer`'s `until` — a session window — is the next
thing on the roadmap and is its own transform, not a mode of these. Nothing
here trains: a model that has to be fitted stays behind the `http` transform,
and the window models (forecasting, decomposition, changepoints) are the next
tier, behind a feature flag.
