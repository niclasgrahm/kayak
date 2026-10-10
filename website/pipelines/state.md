# state

A transform sees one batch at a time. A state bucket keeps a value from one
batch to the next. Examples are the unit that a machine makes now, the recipe in
use, or the last reading from each machine. With a bucket, a pipeline can add a
slow fact from one message to the fast messages that come after it.

Buckets are **global and named**. Declare them one time at the top of the config
file. A pipeline names the bucket that it uses:

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

Many pipelines can use one bucket. For example, one pipeline reads the recipe
stream and remembers the current recipe for each machine. Six other pipelines
recall the recipe and add it to their messages.

::: warning
Do not share a bucket between pipelines for data where the order is important.
Two pipelines have no order between them. A reader can see the value from
before or after a write.
:::

Share a bucket only for a value that changes slowly, compared to the rate of the
messages. A recipe that changes each hour is safe to share. A unit id that
changes each cycle is not safe. Put the `remember` and the `recall` for it in
the same pipeline.

## remember and recall

The position of `remember` and `recall` in the chain sets the result. If
`recall` comes after `remember`, a message with a new unit id gets the new id.
If `recall` comes first, the message gets the previous id.

- `remember` writes the messages that match into the bucket. It sends the batch
  on with no change. `when` is a list of conditions, and all of them must
  match. Without `when`, `remember` uses every message.
- `recall` writes the named values onto every message as top-level fields. A
  reducer downstream can then `group_by` them. `on_missing` sets what happens
  when the bucket has no value yet:
  - `skip` is the default. It sends the message on without the value.
  - `null` writes the value as `null`.
  - `error` fails the batch.

Every stateful pipeline has a warm-up period, before the first value is in the
bucket. `skip` is the default so that the warm-up does not fail the pipeline.

`key` is a field path on the pipeline, not on the bucket. The same machine id
can arrive as `_meta.machine_id` from nats, and as `machine_id` after a reducer.
Two pipelines that share a bucket can use different keys. kayak does not check
that the keys agree. Leave out `key` to keep one value for the whole bucket.

## limits

Every bucket has limits. You cannot make a bucket with no limit.

- `max_keys` is 10000 by default. When a bucket is full, kayak removes the key
  with the oldest write.
- `idle_timeout_secs` removes a key that long after its last write.

## lifetime

A bucket is in memory. **It does not survive a restart of the server.**

A bucket survives a `revert`. A revert rebuilds every pipeline, but it keeps
the contents of the buckets. If the declaration of a bucket changed, the bucket
starts empty.

## readings into rows: pivot

Many industrial and IoT sources send one reading per message. `pivot` makes
rows from them. It remembers the latest value of each of `names` for each key,
and writes all of them onto every message:

```yaml
state:
  machines: { max_keys: 500, idle_timeout_secs: 3600 }
pipelines:
  - id: machine_rows
    state: { bucket: machines }
    inputs: [{ type: indu, connection: indu, sensors: [press-3/state, press-3/fault] }]
    transforms:
      - type: pivot
        name: sensor            # which reading this is
        value: value            # the value of the reading
        names: [state, fault]
        group_by: [device]
```

The message `{"device": "press-3", "sensor": "fault", "value": "NONE"}` gets
`"state": "RUNNING", "fault": "NONE"` beside its own fields, after kayak has
seen both names. Rules:

- A message updates its own name before kayak writes the row. Thus it always
  contains its own reading.
- kayak leaves out a name that it has not seen yet for that key. It does not
  write `null`.
- `into: machine` writes the row under an object, not at the top level.
- `names` is required. A row contains these names and nothing else. A message
  with a name that is not on the list adds nothing, but it still gets the row.

`pivot` also accepts `when` and `reset_when`, the same as the
[streaming statistics](/pipelines/streaming-statistics).

## gating a buffer on a bucket

The `buffer` transform can wait for a bucket. Its `until` setting reads a key in
a bucket. When the conditions are true, the buffer sends all the messages it
holds:

```yaml
transforms:
  - type: buffer
    until:
      bucket: ingest-control
      key: nightly-load        # omit for the value of the whole bucket
      conditions:
        - type: string
          field: status
          operator: equal_to
          value: run_complete
    max_messages: 100000
```

Buckets are global, so a different pipeline usually opens the gate. For
example, a loader writes "run complete" with `remember`. Another pipeline
collects readings and sends them as one batch when the gate opens.

Rules:

- **The gate applies to the whole buffer.** When it opens, all held messages go
  on as one batch. A `field` in a condition is a field inside the bucket entry.
  A dotted path reaches into a remembered value. All conditions must be true.
- **`max_messages` is required** when `size` is not set. When the buffer reaches
  `max_messages`, it sends everything and logs one warning.
- **The buffer does not wait for the next message.** It wakes when someone
  writes to the bucket, or when a `seconds` window closes. A gate that opens on
  a quiet stream sends its messages at once.
- **Do not use the gate to synchronize two pipelines exactly.** Two pipelines
  have no order between them. If the timing must be exact, put both halves in
  one pipeline.

The gate has almost no cost. The buffer reads the gate one time for each
batch that arrives, and only when the bucket changed.

## looking at a bucket

`GET /api/state` lists the buckets and how full each one is.
`GET /api/state/{bucket}` gives the keys, their values and the time of the last
write to each. The web UI shows the same data in the `state` tab of the sidebar.
