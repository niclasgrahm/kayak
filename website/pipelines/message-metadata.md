# message metadata

An input knows facts about a message that the message itself does not contain.
Examples are the nats subject, the kafka topic, partition and offset, or the
HTTP method of a post. Add `envelope` to any input to attach these facts to each
message. The [input reference](/reference/inputs) lists the fields that each
input attaches.

```json
{ "type": "nats", "connection": "opc", "subject": "*.temperature",
  "envelope": { "type": "wrap", "payload": "value", "meta": "_meta" } }
```

## metadata is ordinary fields

kayak writes the metadata into the message as ordinary JSON fields. Thus every
transform can read it with a field path. A reducer groups by the subject the
same way it groups by any other field:

```json
{ "type": "reducer", "group_by": ["_meta.subject"],
  "aggregations": [{ "function": "avg", "field": "value", "as": "mean" }] }
```

This also works through transforms that change the number of messages, for
example `reduce`, `splitter` and the `http` transform. The metadata is part of
each message, so it goes where the message goes.

Use this for machine data. Subscribe to `*.temperature`, and the subject gives
the name of the machine.

## the two shapes

- **`merge`** adds the metadata as one more field. The default field is
  `_meta`. The fields of the payload do not move, so the transforms downstream
  do not change. If the payload is not a JSON object, kayak skips the message
  and logs a warning.
- **`wrap`** puts the payload under a field of its own, beside the metadata:
  `{"value": 1, "_meta": {…}}`. The default payload field is `value`. This
  works with any payload, for example a bare number. Field paths downstream
  must then start with the payload field: `value.temperature`, not
  `temperature`.

If you do not set `envelope`, kayak sends each message on as it arrived. Adding
an envelope changes the shape of every message from that input. Change the
field references downstream at the same time.

The metadata goes to the outputs. A nats output or an ndjson file contains
`_meta` unless a transform removes it. Use `drop` in a
[`map`](/pipelines/reshaping-messages) to remove it. The metadata field can
also have the same name as a field in the payload. Set `meta` and `payload` to
other names to prevent this.

A `pipeline` input usually needs no envelope. The metadata from the upstream
pipeline is already in the message. An envelope on a `pipeline` input adds
facts about that one connection. A `wrap` there puts the upstream message
inside a new one.

The `http` input passes on only five headers: `content-type`, `user-agent`,
`x-request-id`, `x-correlation-id` and `traceparent`. It drops all other
headers. This prevents a credential, for example `x-api-key`, from getting into
a file or an object store.

## field paths

All transforms that read a field by name accept a dotted path. Examples are
`filter`, the `group_by` and the aggregations of `reduce`, and `map`. A path
reaches `_meta.subject` and fields in nested payloads.

**An exact key has priority over a path.** `a.b` reads the literal key `"a.b"`
if the message has one. If not, it reads the field `b` inside the object `a`.
Thus a source with dots in its field names works without an escape rule.

A reducer that groups by a path writes the group under the **last segment** of
the path. `group_by: ["_meta.machine_id"]` writes `machine_id`. If two paths
have the same last segment, kayak refuses to build the pipeline.

`map` can also **write** a path. The write rule is:

1. If the message already has the literal key, `map` writes to that key. Thus a
   write goes back to the same place as the read.
2. If not, `map` writes through the path and makes the objects on the way.
   `as: "sensor.id"` on a message with no `sensor` makes a `sensor` object.
3. If the path goes through a value that is not an object, the mapping fails.
   kayak does not replace a value with an object, because that removes the old
   value.
