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
            operator: EqualTo
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

The `state` tab in the sidebar lists the buckets and how full each one is;
clicking one opens a card beside it with the keys, their values and when each
was last written. It polls once a second while that tab is open and costs
nothing when it isn't — through a plain signal rather than a resource, because
the page has one suspense boundary around the whole canvas and a polled resource
under it rebuilds the canvas on every tick. It is read-only, and that is the whole family: buckets
are part of the graph's logic, so they live in the config file and there is
nothing here to write. `GET /api/state` and `GET /api/state/{bucket}` are the
same thing over HTTP.
