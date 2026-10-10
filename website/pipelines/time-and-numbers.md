# time and numbers

This page describes two things that statistics on a stream need: the rule for
the time of a message, and the functions that a script can use on a batch.

## time on a message

kayak has one rule to read a time from a message. Every component that reads a
time uses it:

**A time is an RFC 3339 string or a number of milliseconds since the epoch.**

```json
{ "ts": "2026-09-14T08:15:30.250Z", "value": 21.5 }
{ "ts": 1789460130250,              "value": 21.5 }
```

kayak does not guess any other format. A string without an offset, for example
`"2026-09-14 08:15"`, is an error. A bare date or a word is also an error. The
error message gives the value.

::: tip the one exception
The `cast` to `timestamp` in the [`map` transform](/pipelines/reshaping-messages#casting)
reads a number as **seconds**. This is the same rule as the column mapping of
the database outputs. Everywhere else, a number is milliseconds. This includes
the `time` field of a component and `parse_time` in a script.
:::

A component that reads times has a `time` setting that names the field:

- Without `time`, the time of a message is the time it **arrived**. This is
  correct for a live stream.
- Set `time` for a replay, or for a source that sends its own timestamps.
- If the `time` field is absent from a message, the batch fails. kayak does not
  use the arrival time instead.

### a slope in the reducer

The reducer function `slope` fits a least-squares line of a field against time.
It gives the change **per second**:

```json
{
  "type": "reducer",
  "time": "ts",
  "group_by": ["machine_id"],
  "aggregations": [
    { "function": "avg",   "field": "temperature", "as": "mean" },
    { "function": "slope", "field": "temperature", "as": "warming" }
  ]
}
```

- `slope` requires `time`. kayak refuses to build a `slope` without it.
- A group with fewer than two readings, or with all readings at the same time,
  gives `null`.
- With `on_missing: skip`, kayak removes the time together with the value of a
  skipped reading. The pairs stay correct.

The reducer also has `stddev` and `median`. Together with
[`map` arithmetic](/pipelines/reshaping-messages), they can calculate values
such as Cpk for each lot.

## numbers over a batch

A [script](/pipelines/scripting) in `batch` scope gets all messages of the batch
as an array. The array functions calculate results from the values. They run in
Rust, so they are faster than a loop in the script.

```rhai
let t = pluck(batch, "temperature");   // the field across the batch, as an array
let fit = linfit(t);                   // #{slope, intercept, r2}, against position
emit([#{
    machine_id: batch[0].machine_id,
    mean: mean(t), std: std(t), slope: fit.slope, n: t.len()
}]);
```

This script makes one message with the identifiers and the statistics from a
window of readings. Send that message to a model endpoint, not the raw readings.

The functions follow three rules:

- **`pluck` skips absent values.** If a message does not contain the field, or
  the field is `null`, `pluck` leaves it out. To check that all messages
  contained the field, compare `pluck(batch, p).len()` with `batch.len()`. A
  value that is present and is not a number fails the message.
- **An undefined result is `()`.** It is never NaN. Examples are the mean of
  nothing, the slope of one point and the z-scores of a flat series. Use
  `if mean(v) == ()` to find the warm-up.
- **Spread is the population kind.** `std` divides by *n*. The reducer's
  `stddev` does the same.

The functions:

| | |
|---|---|
| one number out | `sum`, `mean`, `median`, `min`, `max`, `std`, `variance`, `quantile(v, q)`, `mad`, `skew`, `kurtosis`, `rms`, `autocorr(v, lag)` |
| an array out | `zscore`, `diff`, `cumsum`, `ewma(v, alpha)`, `peaks` |
| a map out | `linfit(v)` / `linfit(xs, ys)`, `histogram(v, bins)` |
| pairs and scalars | `clamp(x, lo, hi)`, `interp(xs, ys, x)`, `dtw(a, b)` |
| time | `parse_time(value)` → millis, `format_time(millis)` → RFC 3339 |

Notes:

- `linfit(v)` fits against the position in the array. The slope is per message.
- `linfit(xs, ys)` fits against a second array. For a slope per second, write
  `linfit(pluck(batch, "t"), pluck(batch, "temperature"))`.
- `dtw` measures the difference between two series when one series can run
  faster or slower than the other. Two cycles with the same shape at different
  speeds give a small value. Compare a cycle with a
  [remembered](/pipelines/state) reference cycle.
- `parse_time` uses the time rule above. It gives milliseconds for both
  spellings. `()` gives `()`, so an absent field stays absent. A string that is
  not a time fails the message.

These functions have no state and no timer. For a value that must remember the
last reading, use the [streaming statistics](/pipelines/streaming-statistics).
