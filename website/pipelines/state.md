# state

A transform sees one batch. `state` is what lets a pipeline carry something
from one batch to the next — the unit currently being produced, the recipe in
force, the last reading from each machine — so a fast stream can be attributed
to a slow-moving fact that arrived on a different message.

Buckets are **global and named**, declared once at the top of the config and
referred to by the pipelines that use them:

```yaml
state:
  machine_state:
    max_keys: 5000
    idle_timeout_secs: 900

pipelines:
  - id: machine_cycles
    state:
      bucket: machine_state
      key: _meta.machine_id
    inputs: [...]
    transforms:
      - type: remember
        when:
          - type: string
            field: _meta.signal
            operator: equal_to
            value: unit_id
        remember:
          - { field: value, as: unit_id }
      - type: recall
        recall: [unit_id]
        on_missing: skip
    outputs: [...]
```

Global rather than per-pipeline because of the shape one step past the example
above: a `recipes` pipeline consumes the recipe stream and remembers the current
recipe per machine, and six unrelated pipelines want to stamp it onto their
output. Per-pipeline state has no answer to that but six copies of one fact and
six edges that exist only to carry it. Named-and-global is also the shape
connections already have, down to the "declared in a file, referred to by name"
part.

**The one rule that isn't enforced, and matters.** Two pipelines sharing a
bucket are two run loops with no ordering between them: a reader can see the
value from before or after a given write depending on nothing it can observe. So
*ordering-sensitive correlation belongs in one pipeline; sharing is for state
whose value doesn't change on the timescale of a message*. A recipe that updates
hourly is safe to share. A unit id that changes every cycle is not — put the
`remember` and the `recall` in the same transform chain, where there is one
stream and one order.

`remember` and `recall` are two transforms rather than one because **where they
sit in the chain is the semantics**: recall after remember means a message
carrying a new unit id comes out carrying it, and before means it comes out
carrying the previous one.

- `remember` writes matching messages into the bucket and **passes the batch on
  unchanged** — it is a tap, not a filter. `when` is a list of conditions, all
  of which must match; leave it out to remember from everything.
- `recall` writes the named values onto every message as top-level fields, so a
  reducer downstream can `group_by` them without knowing where they came from.
  `on_missing` defaults to `skip`: every stateful pipeline has a warm-up in
  which nothing has been remembered yet, and passing those messages on
  unattributed makes a reducer lump them into one bogus group. `null` passes
  them with the gap showing; `error` fails the batch.

The `key` is a field path on the *pipeline*, not on the bucket, because it is a
property of that stream — the same machine id arrives as `_meta.machine_id` from
a nats subscription and as `machine_id` after a reducer has flattened it. The
cost is that two pipelines sharing a bucket can key it differently with nothing
to catch them. Leave `key` out for one bucket-wide value.

**Every bucket is bounded and there is no way to ask for an unbounded one.**
`max_keys` defaults to 10000 and evicts the least recently written key past it;
`idle_timeout_secs` forgets a key that long after its last write. A keyed store
with no limit is a leak with a week-long fuse, so the only question is what the
limits are.

**It is in memory, and it does not survive a restart.** That is a decision
rather than a stage: the store is touched on every message, which rules out a
network round trip; and durability without checkpointed *input positions* would
be worse than none. A core nats subscription has no replay at all, so restoring
a half-finished piece of work whose remaining messages were never delivered
produces an answer that is wrong in a way nothing downstream can see. Dropping
it is the honest answer. A durable backend is a later swap behind the same
shape, and it starts at the input rather than at the store.

It **does** survive a revert, which is the case that actually bites: reloading
the config rebuilds every pipeline, and an edit to an unrelated pipeline
shouldn't cost an hour of accumulated state. A bucket whose *declaration*
changed is a different bucket and starts empty.

## gating a buffer on a bucket

The third thing a bucket can do is hold a pipeline back. The `buffer`
transform's `until` reads a key in a bucket and releases everything it is
holding once the conditions are true:

```yaml
transforms:
  - type: buffer
    until:
      bucket: ingest-control
      key: nightly-load        # omit for the bucket-wide value
      conditions:
        - type: string
          field: status
          operator: equal_to
          value: run_complete
    max_messages: 100000
```

Because buckets are global, the pipeline that opens the gate is usually **not**
the one that was waiting: a loader marks its run complete with `remember`, and
somewhere else a pipeline that has been gathering readings all the while hands
them on in one batch. That is the case this exists for, and it is not
expressible as a chain — the two sides have different inputs and different
rates.

Four things to know before reaching for it:

- **It is a gate on the whole buffer, not a test on each message.** When it
  opens, everything held goes on as one batch. A `field` here is a name inside
  the bucket *entry* — the entry is read as an object, so a dotted path reaches
  into a remembered value exactly as it does into a message — and several
  conditions mean all of them, as they do on `remember`'s `when`.
- **`max_messages` is required** whenever `size` isn't also set. A gate that
  never opens is otherwise a buffer that grows at the rate of the stream until
  the process dies; reaching the bound releases everything and says so in the
  log once.
- **The wait is real, not "until the next message".** A `buffer` is the one
  transform that gets a tick of its own: it can wake the run loop when the
  bucket is written or when a `seconds` window closes, so a gate opening at
  02:14 on a stream that has gone quiet hands its messages on at 02:14. Nothing
  else in the chain works this way, and everything else in kayak is still
  driven purely by arriving batches.
- **It is not a synchronisation primitive**, however much it reads like one.
  Two pipelines are two run loops with no ordering between them, so the gate
  says "the bucket said so at the moment we looked" — the same sharp edge
  sharing a bucket already has, one step more tempting. If the *timing* has to
  be exact, both halves belong in one pipeline.

The cost on the hot path is close to nothing, and deliberately so: the gate is
read once per arriving batch rather than once per message, and only when the
bucket has actually been written since the last look — which is an atomic load,
not the bucket's lock. A buffer holding nothing subscribes to neither the clock
nor the bucket.

The `state` tab in the sidebar lists the buckets and how full each one is;
clicking one opens a card beside it with the keys, their values and when each
was last written. It polls once a second while that tab is open and costs
nothing when it isn't — through a plain signal rather than a resource, because
the page has one suspense boundary around the whole canvas and a polled resource
under it rebuilds the canvas on every tick. It is read-only, and that is the whole family: buckets
are part of the graph's logic, so they live in the config file and there is
nothing here to write. `GET /api/state` and `GET /api/state/{bucket}` are the
same thing over HTTP.
