# durability

By default a kayak server keeps everything in memory, and a restart starts the
graph from nothing: buckets are empty, a `buffer` transform has forgotten what
it held, and a kafka input resumes from whatever its consumer group last
committed. Start the server with `--state-dir` and it **checkpoints**
instead, and what it promises changes:

- **Exactly-once state.** After a crash, every piece of state — a bucket's
  values, a rolling window, a buffered batch, an s3 part that has not been
  uploaded yet, a database input's watermark — reflects each message once:
  none lost, none counted twice.
- **At-least-once output.** Every message that entered a [rewindable
  input](#which-inputs-can-be-replayed) reaches every output at least once. The
  messages processed between the last checkpoint and a crash reach the outputs
  a second time. An output that is idempotent — an upsert keyed on the
  message, an idempotency key — turns that into exactly once end to end.
- **A graceful restart replays nothing.** A shutdown or a revert stops the
  graph at a consistent point first, so the next start carries on exactly where
  this one stopped.

```sh
kayak --config pipelines.yaml --state-dir /var/lib/kayak/state
```

## the knob

How often is the one decision, in the `--server-config` file:

```yaml
checkpoint:
  interval_secs: 10   # the default
  timeout_secs: 60    # a checkpoint that takes longer is abandoned
```

The interval is the usual trade. Shorter means less replayed after a crash and
acknowledgements reaching a broker sooner; longer means fewer flushes and fewer
copies. A checkpoint copies only state that *changed* since the last one — a
bucket nothing has written to is not copied or written at all — so a short
interval on a graph whose state rarely moves costs almost nothing.

## how it works

The same way Flink does it, at a single server's scale. Messages in flight are
never written anywhere. Instead, every `interval_secs` the server takes a
**consistent cut**: where each input had got to, and what each component was
holding at that point. On a restart the inputs rewind to the cut and the
messages after it are processed again.

The cut is made with **barriers**, markers that travel through the graph along
with the data. A pipeline with no upstream takes its barrier between two
batches. A pipeline fed by others receives it down the edge, behind everything
its upstream sent first. A pipeline fed by *several* others waits until the
barrier has arrived from all of them. The write to disk happens off the
pipelines, and a checkpoint counts as committed only once its files are
fsynced. Only then is anything acknowledged to a broker.

Pipelines that share nothing checkpoint independently, so a pipeline whose
output is down holds back its own checkpoints and nobody else's. Pipelines that
*are* connected, by an edge or by sharing a state bucket, commit together or
not at all.

## which inputs can be replayed

| | inputs | after a crash |
|---|---|---|
| **rewindable** | `kafka`, `postgres` and `clickhouse` (incremental), `http` | resume exactly at the checkpoint |
| **fed by another pipeline** | `pipeline` | covered by the upstream's checkpoint |
| **not replayable** | `nats`, `redis`, `mqtt`, `opcua`, `indu`, `dummy`, `http_poll` | state stays consistent; messages in flight at the crash are lost |

- **kafka.** Offsets are part of the checkpoint, and they are stored for commit
  to the broker only once a checkpoint holding them is durable, whatever `ack`
  says. On a restart the input commits the checkpoint's offsets for its group
  before subscribing, so the broker can never be behind the state that was
  restored.
- **postgres / clickhouse.** The `incremental` watermark is part of the
  checkpoint, so a restart resumes after it rather than from `start_from`.
  `snapshot` reads have nothing to resume: the next poll *is* the replay.
- **http.** Every post is appended to a write-ahead log under the state
  directory and fsynced **before** the `202`. One fsync covers every post that
  arrived in the meantime, so the cost is latency rather than throughput. A
  restart replays every logged post the last checkpoint did not cover. This is
  the only place checkpointing costs something per message, and only `http`
  inputs pay it. A post that cannot be logged gets a `503`, and the client
  should send it again.
- **mqtt** connects with a clean session, so a broker does not redeliver
  across a reconnect anyway. **nats** core keeps nothing for an absent
  subscriber; use a JetStream consumer if you need replay.

## outputs under checkpointing

**A failing output no longer drops the batch.** It is retried, holding its
pipeline where it is, until the destination takes it. Dropping it and then
checkpointing past it is exactly the loss checkpoints exist to prevent. A
permanent failure shows up the way an output that cannot start does: one error
on the card whose count climbs. A transform error still drops the batch, since
a transform that fails on a message fails the same way on a replay.

At every checkpoint an output makes what it has written durable. `file` fsyncs
its parts, `nats` flushes its client, and `s3` puts its unuploaded part into
the checkpoint, so the rows in it survive a crash and are uploaded once. The
outputs whose writes are already synchronous — the databases, `http`, `kafka`
— have nothing to do.

## things to know

- **A pipeline that stops without a savepoint holds back its region's
  checkpoints.** That covers a pipeline that crashed, or whose last input died.
  The pipelines connected to it keep running but stop committing until it is
  rebuilt (revert, restart) or deleted. Committing past it would move an
  upstream's position past batches it never processed, and a restart would lose
  them. The server says so in the log.
- **Writers of a shared bucket meet at the barrier.** A bucket is copied once
  every pipeline bound to it has reached the checkpoint, so none of them writes
  past the cut while it is being copied. A bucket with one writer, the usual
  case, involves no waiting.
- **Downtime counts toward `idle_timeout_secs`.** A restored entry's age is
  measured from when it was last written, so a series idle past its timeout
  starts fresh instead of computing across the outage.
- **A component whose config changed starts empty.** State is restored only to
  a component whose own config is unchanged, at the same place in the chain.
  Editing an output does not cost a transform its window; editing the
  transform does. A bucket whose declaration changed starts empty, the same
  rule a revert applies.
- **One server per state directory.** The directory is locked, and a second
  server pointed at it refuses to start rather than corrupting the first one's
  checkpoints. Mind this in a rolling deploy.
- **Deleting a pipeline deletes its checkpoint** and its logged posts, so a
  pipeline created later under the same id starts from nothing.
- **Changing a pipeline keeps its checkpoint.** `PUT /api/pipelines/{id}` stops
  the old config at a checkpoint barrier, a savepoint for that pipeline alone.
  The new config restores every component that is unchanged at the same place
  in the chain, so nothing is replayed or counted twice. The pipelines around
  it take an ordinary checkpoint and keep running. If the barrier does not
  arrive within 10 s, kayak stops the old config without one, and the new
  config replays from the last checkpoint that completed.
