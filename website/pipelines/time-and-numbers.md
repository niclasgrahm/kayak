# time and numbers

Two small things that most statistical work on a stream needs first: a rule
for *when* a message happened, and a way to do arithmetic over a batch of them
without leaving the config file. Both are here, and both are prerequisites
for the streaming statistics that follow them on the roadmap.

## time on a message

Nearly everything statistical wants to know when a reading was taken — a rate
is a delta over a time, a slope is per second, a window is so many seconds
wide. Kayak has one rule for reading that off a message, and every component
that reads a time goes through it:

**A time is an RFC 3339 string or a number of milliseconds since the epoch.**

```json
{ "ts": "2026-09-14T08:15:30.250Z", "value": 21.5 }
{ "ts": 1789460130250,              "value": 21.5 }
```

Milliseconds rather than seconds because `now_millis()` is what a script
writes, and a script reading its own field back in the other unit is out by a
factor of a thousand and visible only on a chart. Anything else — a
`"2026-09-14 08:15"` without an offset, a bare date, a word — is an error
naming the value, never a guess: a reading silently moved by twelve hours is
worse than a batch that fails and says which field.

::: tip the one exception
The [`map` transform's `cast` to `timestamp`](/pipelines/reshaping-messages)
reads a bare number as **seconds**, because it is a *conversion* into the
column mapping's world and that is what `to_timestamp` means there. That is
the only place seconds are assumed. Reading a time — here, in `slope`, in a
script's `parse_time` — is always milliseconds.
:::

A component that reads times carries a `time` setting naming the field. Leave
it out and each message's time is when it **arrived**, which is right for a
stream read live and wrong for a replay; the setting is what a replay sets. A
configured field that is missing from a message fails the batch rather than
falling back to arrival — the same reason the reducer's `on_missing` defaults
to `error`.

### a slope in the reducer

The first thing to use it is the reducer's `slope`, which fits a least-squares
line of a field against time and reports how fast it is changing **per
second**:

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

`slope` refuses to build without a `time` — a slope with no time is a slope
per nothing, and arrival time would make every batch a vertical line. A group
with fewer than two readings, or all of them at the same instant, has no slope
and reports `null`, as `avg` reports `null` of nothing. `on_missing: skip`
drops a reading's time along with its value, so the pairing survives.

The reducer's `stddev` and `median` were already there; with `slope` beside
them, "Cpk per lot" is a `reducer` and a little
[`map` arithmetic](/pipelines/reshaping-messages).

## numbers over a batch

A [script](/pipelines/scripting) in `batch` scope is handed every message of
the batch as an array. What it lacked was a way to get from that to *numbers*,
and then to answers — and a loop in an interpreter over four hundred readings
is the slow way to a mean. So a script has a set of array functions, all
generated into the table on the scripting page, and one bridge:

```rhai
let t = pluck(batch, "temperature");   // the field across the batch, as an array
let fit = linfit(t);                   // #{slope, intercept, r2}, against position
emit([#{
    machine_id: batch[0].machine_id,
    mean: mean(t), std: std(t), slope: fit.slope, n: t.len()
}]);
```

That is the cycle-features script — a window of raw readings in, one
descriptor with the identifiers out — and it is what a model endpoint
actually wants to be sent, rather than the four hundred readings.

Three rules to know, and they are the same three the transforms follow:

- **`pluck` skips what is missing.** A message that doesn't carry the field,
  or carries `null`, is left out; `pluck(batch, p).len() == batch.len()` is
  the check that none were. A value that *is* there and isn't a number fails
  the message — `on_missing` is about a sparse stream, not a wrong one.
- **Undefined is `()`, never NaN.** The mean of nothing, the slope of one
  point, the z-scores of a flat series. `if mean(v) == ()` is the warm-up
  check, the same one `recall` gets, and no NaN can travel silently through a
  comparison downstream.
- **Spread is the population kind.** A window holds every reading that arrived
  in it, so `std` divides by *n* — the rule the reducer's `stddev` has always
  followed.

What there is, grouped by shape:

| | |
|---|---|
| one number out | `sum`, `mean`, `median`, `min`, `max`, `std`, `variance`, `quantile(v, q)`, `mad`, `skew`, `kurtosis`, `rms`, `autocorr(v, lag)` |
| an array out | `zscore`, `diff`, `cumsum`, `ewma(v, alpha)`, `peaks` |
| a map out | `linfit(v)` / `linfit(xs, ys)`, `histogram(v, bins)` |
| pairs and scalars | `clamp(x, lo, hi)`, `interp(xs, ys, x)`, `dtw(a, b)` |
| time | `parse_time(value)` → millis, `format_time(millis)` → RFC 3339 |

`linfit(xs, ys)` is how a slope per second is had in a script —
`linfit(pluck(batch, "t"), pluck(batch, "temperature"))` — where `linfit(v)`
alone fits against position and is per message. `dtw` is the one that earns a
sentence: it is how unlike two series are once one is allowed to run faster or
slower than the other, so two cycles of the same shape at different speeds are
close, and it is what to compare against a
[remembered](/pipelines/state) reference cycle.

`parse_time` and `format_time` are the one time rule spelled for a script:
`parse_time(msg.ts)` is milliseconds whichever of the two spellings the field
held, `()` passes through as `()` so a missing field stays missing, and a
string that is not a time fails the message naming it.

Everything here is a pure function over an array. What it is *not* is
per-key state or a tick — a value that has to remember the last reading, or
fire when nothing arrives, is a transform, and those are the next thing on the
roadmap.
