# reshaping messages

The `map` transform changes the shape of a message. It can rename fields, move
them out of nested objects, add constants, cast values, set defaults and select
fields. The mappings run in order.

```json
{ "type": "map", "mappings": [
  { "type": "copy", "from": "_meta.subject" },
  { "type": "coalesce", "from": ["temp_c", "readings.celsius"], "as": "celsius" },
  { "type": "cast", "from": "recorded_at", "to": "timestamp" },
  { "type": "drop", "from": ["_meta"] }
]}
```

`map` always sends one message for each message it gets. It never drops a
message and never makes two. To drop messages, use `filter`. To split a
message, use `splitter`. To combine messages, use `reduce`.

There are eight mappings:

| | |
|---|---|
| `copy` | rename a field, or move it out of a nested object |
| `constant` | write a fixed value, for example the site or the environment |
| `coalesce` | write the first field from a list that the message contains |
| `cast` | convert a value to a different JSON type |
| `concat` | join fields and literal text into one string |
| `arithmetic` | do one operation on two numbers, each a field or a literal |
| `time_bucket` | write the start of the hour, day or shift that a time is in |
| `drop` | remove fields |

## order and arithmetic

`mappings` is an ordered list. Each mapping reads the fields that the mappings
before it wrote. Use an intermediate field for a calculation with two steps:

```yaml
- type: map
  mappings:
  - { type: arithmetic, as: _offset, operator: subtract,
      left: { type: field, field: fahrenheit }, right: { type: value, value: 32 } }
  - { type: arithmetic, as: celsius, operator: divide,
      left: { type: field, field: _offset }, right: { type: value, value: 1.8 } }
  - { type: drop, from: [_offset] }
```

Each `arithmetic` mapping does one operation. There are no nested expressions
and no conditions. For a longer calculation or a condition, use a
[script](/pipelines/scripting).

The operators are `add`, `subtract`, `multiply`, `divide`, `min` and `max`. Use
`min` with a literal as an upper limit and `max` with a literal as a lower
limit. This example keeps a percentage between 0 and 100:

```yaml
  - { type: arithmetic, as: _capped, operator: min,
      left: { type: field, field: health }, right: { type: value, value: 100 } }
  - { type: arithmetic, as: health_pct, operator: max,
      left: { type: field, field: _capped }, right: { type: value, value: 0 } }
```

Division by zero:

- A literal zero divisor is an error when kayak builds the pipeline.
- A divisor field that contains zero fails the batch.
- Set `on_zero` on the mapping to write a value for a zero divisor.
  `{ type: null }` writes `null`, which a chart shows as a gap.
  `{ type: value, value: 1 }` writes a number, which a sum downstream can add.

## time buckets

`time_bucket` writes the start of the calendar period that a time is in. Use it
as a `group_by` field. A stateful transform that groups by the bucket keeps one
series for each period. Thus "per shift", "per hour" and "since midnight" need
no special window. The idle timeout of the [state bucket](/pipelines/state)
removes the periods that are over.

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
  group_by: [machine, shift]       # the count starts again every shift
```

kayak counts periods on the **wall clock** of the time zone. A shift that starts
at 06:00 starts at 06:00 in July and in December. On the night that the clocks
go back, the night shift is nine hours long. Without `timezone`, the clock is
UTC.

Buckets align with local midnight on 1 January 1970. `offset_seconds` moves
them. Thus days start at midnight, and weeks start on a Thursday. An offset of
four days starts weeks on a Monday.

At a clock change:

- If a period starts in the hour that the spring change skips, the period starts
  at the first instant after the gap.
- If a period starts in the hour that occurs twice in autumn, the period starts
  at the first of the two.

`format: millis` writes the start as epoch milliseconds. The default is an
RFC 3339 string.

## keep

- `keep: all` is the default. It sends the message with the mappings applied to
  it.
- `keep: mapped` sends **only** the fields that the mappings wrote. Use it to
  prepare a message for an output with a fixed shape. It also removes the
  intermediate fields of a calculation.

kayak refuses a `drop` together with `keep: mapped`.

## missing fields

`on_missing` sets what happens when a field is absent:

- `error` is the default. It fails the batch.
- `omit` does not write the target field.
- `null` writes the target field as `null`.

For a field that is often absent, set `default` on that one mapping. The default
applies before `on_missing`, and it applies to that field only.

kayak reads `null` and an absent field as the same thing.

## casting

`cast` converts a value. It is the only place in kayak that converts a value.
The column mapping of the database outputs only checks values. Cast a value one
time in `map`, and every output gets the converted value.

The types are `text`, `integer`, `float`, `boolean`, `timestamp`, `date`, `uuid`
and `json`.

- There is no `bigint`. JSON has one type for integers.
- There is no `decimal`. A JSON number cannot keep a decimal apart from a float.
- `json` parses a **string that contains JSON**. Use it for a payload that
  arrived encoded two times.

`cast` does not guess:

- `12.5` to `integer` is an error. kayak does not round.
- A `timestamp` cast reads an RFC 3339 string, or a number as **seconds** since
  the epoch. This is the same rule as the column mapping.

Everywhere else, kayak reads a number as a time in milliseconds. See
[time and numbers](/pipelines/time-and-numbers).

**A value that is present and does not convert is an error.** `on_missing` does
not change this. For example, `"twelve"` in a field cast to `float` fails the
batch.

## what is refused at build time

kayak refuses to build a pipeline with these errors in a `map`:

- no mappings;
- an empty `as` or `from`;
- two mappings that write the same field;
- a `coalesce` with fewer than two fields;
- a `concat` with no parts;
- a `drop` with no fields, or a `drop` with `keep: mapped`;
- division by a literal zero.

kayak does not check if a mapping reads a field that a later mapping writes.
The message can already contain that field.
