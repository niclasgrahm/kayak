# message metadata

An input knows things about a message that the message doesn't say: the nats
subject it arrived on, the topic, partition and offset of a kafka record, the
method it was posted with. `envelope` on any input is what attaches that to the
message, and the exact fields each input attaches are listed under it on
`/docs` — declared in `kayak-core/src/metadata.rs`, so an input added without
saying what it attaches fails a test rather than shipping an empty section.

```json
{ "type": "nats", "connection": "opc", "subject": "*.temperature",
  "envelope": { "type": "wrap", "payload": "value", "meta": "_meta" } }
```

**Metadata is attached in band** — as ordinary fields on the JSON, not as a
sidecar travelling beside it. That is the whole design decision, and it is
deliberately not what Benthos does. The reason is the transforms that change
cardinality: a reducer collapses five hundred messages into one, a splitter
divides one into several, the http transform replaces the batch with a service's
reply. Out of band, every one of them has to answer "whose metadata comes out?"
and there is no good general answer — Benthos picks the first message's, which
is arbitrary. In band the question doesn't exist. Metadata is data, so

```json
{ "type": "reducer", "group_by": ["_meta.subject"],
  "aggregations": [{ "function": "avg", "field": "value", "as": "mean" }] }
```

is a `group_by` like any other, and nothing in the reducer knows that metadata
is a thing. That is also what the machine-data case needs: subscribe to
`*.temperature` and the machine's name exists *only* in the subject.

Two shapes, because a payload is not always an object:

- **`merge`** adds the metadata as one more field. The payload's own fields stay
  exactly where they were, so nothing downstream of an input that grows an
  envelope has to change. A payload that isn't a JSON object is skipped with a
  warning — there is nowhere to put the field — the same way a non-JSON payload
  already is.
- **`wrap`** puts the whole payload under a field of its own beside the
  metadata: `{"value": 1, "_meta": {…}}`. Works whatever the payload is, which
  is what a source of bare readings needs. The cost is that field references
  downstream now go through the payload field — `value.temperature` rather than
  `temperature` — and that is exactly why both shapes exist rather than one.

**Leaving `envelope` out is the default and means what it always meant**: the
message is passed on as it arrived, byte for byte. Attaching metadata changes
the shape of every message from that input, which is not something to do to a
running config without being asked — the same promise `max_batch` makes about
batching.

What it costs, said plainly: metadata reaches your outputs (a nats publish or an
ndjson file carries `_meta` unless something removes it) and the key can collide
with one the payload already uses. Both are yours to decide, which is what the
`meta` and `payload` field names are for.

A `pipeline` input usually wants no envelope at all: being in band, whatever the
upstream attached is already on the message and arrives with it. Setting one
there says something about *that hop* rather than replacing it, and a `wrap`
would nest the upstream's message inside a new one.

The `http` input's headers are the one place this is restricted. Only
`content-type`, `user-agent`, `x-request-id`, `x-correlation-id` and
`traceparent` are passed on; everything else is dropped. It is an allow-list
rather than a deny-list or an `x-` prefix rule on purpose — the prefix rule is
exactly the one that passes `x-api-key` through, and a credential written into a
file or an object store is a leak that outlives the request by years.

## field paths

Everything that addresses a field by name — `filter`, `reduce`'s `group_by` and
its aggregations — takes a dotted path, which is what makes `_meta.subject`
reachable and, incidentally, nested payloads reachable at all.

**An exact key wins over a path.** `a.b` is the value under the literal key
`"a.b"` if the message has one, and only otherwise the `b` inside the `a`. That
ordering is what makes paths a compatible addition: a source whose field names
contain dots keeps working exactly as it did, and no config has to learn an
escaping rule to say what it already said.

A reducer grouping by a path writes the group out under the path's **last
segment**: `group_by: ["_meta.machine_id"]` emits `machine_id`, because that is
the field the next pipeline wants and it shouldn't have to spell the previous
one's input shape. Two paths that would land on the same leaf are refused when
the pipeline is built, like every other collision there.

Paths can be **written** as well as read — that's what `map` needs, and it's the
only transform that does it. The read rule has an obvious meaning (both readings
exist, prefer the exact one) and the write rule doesn't, so it is spelled out:

1. If the message already has the **literal key**, that's what gets written.
   Which is what makes a write round-trip a read — copying `a.b` to `a.b` puts
   the value back where it was found, whichever of the two shapes that was.
2. Otherwise the path is written through, creating the objects on the way:
   `as: "sensor.id"` on a message with no `sensor` makes one.
3. A path running through something that is **not** an object is an error, not
   an overwrite. Replacing a scalar with an object to make room for a field
   inside it loses data in a way nothing downstream can see.
