# reshaping messages

`map` is the transform that changes what a message *looks like*: renames,
promotions, constants, casts, defaults and projections, applied in order.

```json
{ "type": "map", "mappings": [
  { "type": "copy", "from": "_meta.subject" },
  { "type": "coalesce", "from": ["temp_c", "readings.celsius"], "as": "celsius" },
  { "type": "cast", "from": "recorded_at", "to": "timestamp" },
  { "type": "drop", "from": ["_meta"] }
]}
```

One message in, **one message out, always** — `map` never drops a message and
never makes two. That's what keeps it out of the territory `filter`, `splitter`
and `reduce` already own, and it's why `on_missing` has no "drop the message"
arm: that is a `filter`, one link along the chain.

The eight mappings:

| | |
|---|---|
| `copy` | rename, or promote something out of a nested object |
| `constant` | write a fixed value — the site, the environment, the feed's name |
| `coalesce` | the first of several fields the message actually carries |
| `cast` | convert a value to another JSON shape |
| `concat` | join fields and literal text into one string |
| `arithmetic` | one operation on two numbers, each a field or a literal |
| `time_bucket` | the start of the hour, day or shift a time falls in |
| `drop` | take fields off |

**`mappings` is an ordered list and the order is the semantics.** Each mapping
reads whatever the ones before it wrote, which is how an intermediate field
works, and therefore how a two-step calculation is expressed:

```yaml
- type: map
  mappings:
  - { type: arithmetic, as: _offset, operator: subtract,
      left: { type: field, field: fahrenheit }, right: { type: value, value: 32 } }
  - { type: arithmetic, as: celsius, operator: divide,
      left: { type: field, field: _offset }, right: { type: value, value: 1.8 } }
  - { type: drop, from: [_offset] }
```

That is also the deliberate limit. **`map` reshapes; it does not compute.** One
arithmetic operation per mapping, no nested expressions, no per-field
conditionals — because the version that has those is an expression language with
a syntax to design, and the honest answer at that point is an embedded scripting
language rather than an expression tree spelled in YAML. Two steps read fine and
four don't, and that unpleasantness is information about which tool you want.

`arithmetic`'s operators are `add`, `subtract`, `multiply`, `divide`, `min` and
`max`. The last two are what a clamp is made of — `min` against a literal is a
ceiling and `max` a floor, so keeping a percentage in 0–100 is two mappings:

```yaml
  - { type: arithmetic, as: _capped, operator: min,
      left: { type: field, field: health }, right: { type: value, value: 100 } }
  - { type: arithmetic, as: health_pct, operator: max,
      left: { type: field, field: _capped }, right: { type: value, value: 0 } }
```

A divisor that is a literal zero is refused when the pipeline is built. A
divisor *field* that turns out to hold zero fails the batch, unless the
mapping's `on_zero` says what it means instead: `{ type: null }` writes `null`
— right for a chart, which should show nothing there — and
`{ type: value, value: 1 }` writes a number, right for a ratio over an empty
period that a sum downstream has to be able to add up.

It's a list rather than an object keyed by target name for the same reason: a
JSON object's key order is not something a config file should have to rely on,
and here order decides the answer.

## time buckets

`time_bucket` writes the start of the calendar period a time falls in. Its
reason to exist is `group_by`: a stateful transform grouped by the bucket
keeps one series per period, so "per shift", "per hour" and "since midnight"
need no window of their own — and the [state bucket](/pipelines/state)'s idle
timeout forgets the periods that are over.

```yaml
- type: map
  mappings:
  - type: time_bucket
    from: ts                       # RFC 3339 or epoch milliseconds
    every_seconds: 28800           # eight hours
    offset_seconds: 21600          # starting at 06:00
    timezone: Europe/Stockholm     # on this clock
    as: shift
- type: derive
  derive: [{ function: counter, field: good_parts, as: good }]
  group_by: [machine, shift]       # the count starts over every shift
```

Periods are counted on the time zone's **wall clock**, which is the point of
naming one: the morning shift starts at 06:00 in July and in December, two
different UTC hours, and the night the clocks go back holds a nine-hour night
shift. Without `timezone` the clock is UTC. Buckets line up with local
midnight on 1 January 1970 and `offset_seconds` moves them on — which makes
days start at midnight, and weeks on a Thursday (an offset of four days
starts them on a Monday). A start that falls in the hour skipped in spring is
the first instant after the gap; one in the hour that happens twice in autumn
is the first of the two.

`format: millis` writes the start as epoch milliseconds instead of an RFC 3339
string, for an output that wants a number.

## keep

`keep: all` (the default) passes the message through with the mappings laid over
it. `keep: mapped` emits **only** the fields the mappings wrote — a projection,
which is what prepares a message for an output with a shape of its own, and what
sweeps up the intermediates a chained arithmetic leaves behind. A `drop` beside
`keep: mapped` is refused at build time: it's either a no-op or a
misunderstanding of what `mapped` does.

## missing fields

`on_missing` is `error` by default, on the reducer's argument — a mapping that
silently produced nothing is wrong in a way nothing downstream can see. `omit`
leaves the target unwritten, `null` writes it as `null`.

The better tool for a stream that is genuinely sparse is a **`default` on the
one mapping that expects it**, which answers before `on_missing` does and says
which field is the sparse one rather than loosening the rule for all of them.

**Absent and `null` are the same fact**, the reading `reduce` and the column
mapping already make.

## casting

`cast` is the one place in kayak that coerces rather than checks, and that's the
division of labour with a `postgres` column mapping, which never converts: a
stream that needs converting says so once, here, instead of at each of three
outputs.

`text`, `integer`, `float`, `boolean`, `timestamp`, `date`, `uuid`, `json`.
Deliberately a smaller set than the column mapping's types even though they
overlap — `integer` and `bigint` are one thing in JSON, and `decimal` is absent
because a JSON number can't hold one distinctly from a float, so a cast claiming
to would be a lie. `json` means something different again: it parses a **string
containing JSON**, for the common case of a payload that arrived double-encoded.

It stays conservative about the conversions that could go two ways. `12.5` to
`integer` is an error, not a rounding — which way to round is not something the
config said. A timestamp is an RFC 3339 string or a number read as **seconds**
since the epoch, the same reading the column mapping makes — and the one place
seconds are assumed. Everywhere a time is *read* rather than converted (the
reducer's `time`, a script's `parse_time`) a number is milliseconds; see
[time and numbers](/pipelines/time-and-numbers).

**A value that is present and won't convert is an error whatever `on_missing`
says.** `on_missing` is about a stream that is sparser than the config expected;
a `"twelve"` in a field cast to `float` is a stream that isn't what the config
says it is, and treating that as absent would hide the difference forever.

## what is refused at build time

The reducer's rule, applied here: anything that would otherwise be a
strange-looking message once per batch forever fails when the pipeline is
created. No mappings; a blank `as` or `from`; two mappings writing the same
field; a `coalesce` over fewer than two fields; a `concat` with no parts; a
`drop` with no fields, or one under `keep: mapped`; division by a literal zero.

There is deliberately **no** check that a mapping doesn't read a field a later
mapping writes. It looks like a bug and often isn't — the message may already
carry that field and be having it replaced afterwards — so the check would
refuse working configs, and a false refusal is worse than the warning it saves.
