# Durability: checkpoints

Design note, 2026-10-10. What kayak promises about a message once it has
entered the system, how that promise is kept across a crash, and what it costs.

## The goal, stated precisely

"Pick up exactly where it crashed" is not something any stream processor does,
Flink included, and it is worth being exact about what is meant instead:

- **Exactly-once state.** After a crash and a restart, every piece of state —
  a `buffer` transform's held messages, a rolling window in a bucket, a sql
  input's watermark, an s3 output's unuploaded part — reflects each input
  message **once**: not zero times (lost) and not twice (double-counted).
- **At-least-once output.** Every message that entered a replayable input
  reaches every output at least once. Duplicates are possible after a crash,
  for exactly the messages processed between the last completed checkpoint and
  the crash. An output that is idempotent (an upsert, an idempotency key)
  turns that into exactly-once end to end; making the non-idempotent ones
  transactional is later work (see "Not in this change").
- **The default stays as cheap as it is today.** Checkpointing is off unless
  `--state-dir` is given, and without it nothing on the hot path changes.

## How Flink does it, and why kayak copies the shape

Flink never persists messages in flight. It persists **positions** (how far
each source had read) and **state** (what each operator holds), periodically,
as a consistent cut — and on recovery rewinds the sources and replays. The
durable copy of the messages themselves is the source's own log (a kafka
topic), which already exists.

The cut is made with **barriers**: numbered markers injected into every source
stream and carried along with the data. An operator that has seen barrier *n*
on all of its inputs snapshots its state and forwards the barrier. Everything
before the barrier is in the snapshot; nothing after it is. When every sink
has seen barrier *n*, checkpoint *n* is complete and only then is anything
"committed" (kafka offsets, transactions).

The costs are per checkpoint, not per message: a barrier is one extra item
every few seconds, the snapshot is a copy of state, and an operator with
several inputs pauses the faster ones while it waits for the slowest barrier
(alignment). That is the property worth copying.

## The kayak mapping

### Inputs: three classes

| Class | Inputs | What recovery means |
|---|---|---|
| **Rewindable** — has a position | `kafka`, `postgres`, `clickhouse`, `http` (with the write-ahead log below) | The position is part of the checkpoint; restore rewinds to it. |
| **Fed by another pipeline** | `pipeline` | No position: the upstream's barrier travels down the edge, and everything the upstream sent before it is processed before the downstream snapshots. |
| **Gone once received** | `nats` (core), `redis` (pub/sub), `mqtt`, `opcua`, `indu`, `dummy`, `http_poll` | Nothing upstream can replay. State stays consistent with what was processed; messages in flight at the crash are lost. Said plainly on the reference pages. |

`http_poll` is listed last for honesty rather than because anything is lost —
it is snapshot-only, so the next poll after a restart *is* the replay. `mqtt`
is there because the input connects with a clean session, so a broker does
not redeliver across a reconnect anyway; holding PUBACKs for a checkpoint
interval would throttle a stream to the broker's in-flight window and buy
nothing.

### Positions travel on the acknowledgement

The existing [`Ack`](../src/inputs/ack.rs) already crosses every layer that
matters — `Buffered` folds acks, `Merged` forwards them, the run loop fires
them once a pass is done. A position is the same fact (how far this input has
got), so it rides there: an `Ack` may now *also* carry a position and say that
it waits for a checkpoint (`defers_to_checkpoint`).

That placement is what makes the snapshot correct despite read-ahead. A
`Merged` pump or an input `buffer` may have read messages the run loop has not
processed yet; the position the run loop records is the one carried by the
last delivery it **processed**, never the input's own idea of how far it has
read. Messages read ahead are after the cut and are replayed.

Deferred acks are coalesced per input as they accumulate (kafka keeps the
highest offset per partition, a poller keeps its latest watermark), so the
memory held between two checkpoints is per input and per partition, not per
message.

### Barriers

The coordinator (`src/checkpoint/`) bumps an epoch every `interval_secs` and
tells every **root** pipeline — one with no `pipeline` input — through a
`watch` channel the run loop `select!`s beside its input. A root takes the
barrier between two passes, which needs no alignment: a run loop processes one
delivery at a time, so between passes its state is exactly "everything
delivered so far".

Pipelines fed by others receive the barrier **in band**: the edge between two
pipelines carries `Edge::Batch` or `Edge::Barrier`, and the upstream sends the
barrier after its own snapshot. A pipeline with several `pipeline` inputs
**aligns** in `Merged`: once barrier *n* has arrived on one of them, that pump
is not read again until *n* has arrived on all of them. A pipeline with a mix
of pipeline inputs and its own sources is triggered by the alignment, not by
the coordinator, and records its sources' positions at that moment.

An input `buffer` holding messages when a barrier arrives hands them on first
and the barrier after, so nothing the upstream counted as delivered is stuck
outside the snapshot. The cost — an input buffer's batch is cut short at a
checkpoint — falls only on pipeline inputs with a `buffer`; the `buffer`
*transform*, which is where windowing belongs, snapshots its contents instead.

At its barrier a run loop:

1. asks every output to make what it has emitted durable (`checkpoint()` — an
   fsync for `file`, a producer flush for kafka/nats, a no-op for the outputs
   whose `emit` is already synchronous) and to describe what it holds
   (`snapshot()` — the s3 output's unuploaded part);
2. snapshots every transform and records the input positions;
3. hands the snapshot and the deferred acks to the coordinator and forwards
   the barrier downstream — **the run loop does not wait for the disk**;
4. if it is bound to a state bucket that another pipeline is also bound to,
   waits until all of them have reached the barrier, and the last to arrive
   copies the bucket. See "Buckets" below.

### Completion, regions, and when acks fire

A checkpoint is complete for a **region** — a connected component of the
graph, where pipelines are connected by edges and by sharing a bucket — when
every member has submitted its snapshot. The coordinator's writer task then
writes the members' files and the region's buckets, fsyncs them, rewrites the
manifest atomically (temp, fsync, rename, fsync the directory), and only then
fires the deferred acks. Regions are independent, so a pipeline whose output
is down holds up its own region's checkpoints and nobody else's.

Acks of an epoch that never completes are not lost: they fire with the next
epoch of that region that does, since a later position covers an earlier one.
An epoch older than `timeout_secs` is abandoned rather than waited on forever.

Kafka is the one input whose broker keeps its own copy of the position, and
the order above is what keeps the two from disagreeing: offsets are stored for
commit only after the manifest naming them is durable, and on restore the
input commits the manifest's offsets for its group before subscribing, so the
broker can never be behind the state that was restored.

### Buckets

Buckets are global, and Flink has no equivalent — its operators never share
mutable state, precisely so a barrier can cut it cleanly. Kayak's answer is to
treat a bucket as a join point: every pipeline bound to a bucket (its `state`
block) is a writer, and a bucket is copied when **all** of its writers have
reached the barrier and before any of them has gone on. Writers forward their
barrier *before* waiting, so a writer downstream of another writer can still
reach it — the wait cannot deadlock on the graph's own edges.

A bucket with one writer — by far the common case — is copied inline by that
writer, with no wait. A bucket whose write version has not moved since the
last checkpoint is not copied or written at all; the manifest keeps pointing
at the previous file.

Restored entries take their age from the wall clock they were written at, so
downtime counts toward `idle_timeout_secs`: a `derive` or `rolling` series
idle past its timeout starts fresh instead of computing across a gap of
hours. A bucket whose declaration changed is restored empty — the same rule
`Buckets::rebuilt` applies on revert.

Readers that are not writers do not wait; they read whatever the bucket holds,
as now. A replay re-reads the bucket as it is at replay time, which is the
same non-determinism any lookup against external state has, and the rule
already documented for shared buckets ("only for state that doesn't change on
the timescale of a message") is what makes it harmless.

### Outputs under checkpointing

An output whose `emit` fails no longer drops the batch. It is retried on the
output's backoff until it succeeds, holding the pipeline (and therefore the
input) where it is: dropping it and then completing a checkpoint past it is
exactly the loss checkpointing exists to prevent. A permanent failure is
legible the way `init_outputs` failures already are — one error signature whose
count climbs. Only the outputs that failed are retried; siblings that took the
batch are not given it twice.

Transform errors still drop the batch: a transform failing on a message fails
identically on replay, so retrying cannot help.

### Restore

On startup the manifest is read before any pipeline is built. Each component's
snapshot is restored only if that component's **own config is unchanged**
(compared as JSON, at the same position in the chain): editing an output does
not discard a transform's window, but editing the transform does. A position
is restored the same way, per input.

### Stop with a savepoint

Shutdown and revert no longer just cancel. They trigger a final checkpoint
whose barrier also tells each run loop to stop once it has passed it: the
graph quiesces at a consistent cut, outputs `finish` before they snapshot (so
the s3 part is uploaded rather than snapshotted and uploaded twice), the
checkpoint completes, and only stragglers are cancelled. A graceful restart
therefore replays nothing. A region that cannot reach its savepoint within the
grace period (an output that is down) is cancelled and restored from its last
completed checkpoint instead.

### The http input's write-ahead log

`http` is the one input where kayak *is* the log: the client was told 202, and
nothing else holds the message. Under checkpointing, a post is appended to a
per-pipeline log under the state directory and fsynced **before** the 202 (one
fsync covers every post that arrived meanwhile — group commit), and the
position is the log offset. Restore replays from the checkpointed offset;
completed checkpoints truncate whole segments behind them. This is the only
per-message disk cost in the design, and only `http` inputs pay it.

## What implementing it taught

The design above held; five rules were added to it, each found by the chaos
tests in `tests/checkpoint.rs` (thirty crashes at arbitrary points against a
two-pipeline graph, and ten against the server end to end with the write-ahead
log; the invariant is that message *n* always carries `seq = n + 1`, which a
lost message and a double-counted one both break):

- **Epochs never restart.** A new process carries on from the manifest's
  highest epoch. Snapshot files are named by epoch, and a reused number let a
  new snapshot overwrite one the committed manifest still named.
- **A pipeline joins the coordinator before it subscribes to its upstream.**
  Otherwise the upstream, running, can find itself alone in its region,
  commit, and acknowledge batches the newcomer never snapshotted.
- **A pipeline that stops without a savepoint stays in its region, dead**, and
  holds the region's checkpoints until it is rebuilt or deleted — the rule a
  failed task follows in Flink. Recomputing the region without it let an
  upstream commit past what it never processed.
- **One process per state directory**, enforced by an exclusive lock: two
  coordinators garbage-collect each other's snapshots.
- **The input `buffer` had to become cancel-safe.** It kept the batch it was
  gathering in `next()`'s locals, and the run loop drops that future whenever
  another `select!` arm wins — a pre-existing loss (any pipeline pairing an
  input buffer with a timed `buffer` transform), made frequent by the trigger
  arm. The batch lives on the struct now.

And three about cost, each found by benchmarking the branch against `main`
with interleaved runs, and each paid by *every* pipeline — checkpointed or not
— until it was fixed:

- Polling the trigger as the first arm of the run loop's biased `select!`
  registered and dropped a waiter on every pass. A root now asks
  `has_changed()` — an atomic load — between passes, and the arm that awaits
  it sits behind the input as a pointer-wide `poll_fn` whose wait is boxed once
  and kept, so only an idle pipeline ever registers.
- Adding the input index and the barrier to `Delivery` doubled it to 48 bytes
  and cost ~10% at the per-pass floor, because a delivery is moved through
  several futures on every pass. Both moved onto the ack instead: a barrier is
  a `BarrierAck` on an empty batch, and the index a `FromInput` wrapper that
  `Merged` adds only to acks that defer to a checkpoint.
- An extra `async fn` layer around the `select!` cost a few percent; the run
  loop's helpers on the pass are synchronous.

## What it costs

- **Off (no `--state-dir`)**: nothing. Acks fire at the end of the pass as
  before; the trigger arm of the `select!` is `pending()`.
- **On, per pass**: one deferred-ack coalesce (a downcast and a max per kafka
  partition) and an extra `select!` arm on a `watch` receiver.
- **On, per checkpoint**: a barrier through each edge, the outputs' flushes, a
  copy of each stateful transform and of each *changed* bucket, and the file
  writes — off the run loop, on the coordinator's writer task.
- **Alignment**: a pipeline with several upstreams, and the writers of a shared
  bucket, pause for the slowest barrier. Single-upstream pipelines and
  single-writer buckets never wait.
- **Outputs**: a failing output now holds its pipeline instead of dropping
  data. That is a behaviour change, and the point.

`kayak-bench` has three checkpointed rows — `checkpointed1`,
`checkpointed`, `checkpointed_depth3` — beside the rows they mirror. Measured
on an M1 Max with paired, interleaved runs (the machine was loaded, so trust
the ratios rather than the absolute numbers), turning checkpointing on costs:

| row | off | on | cost |
|---|---|---|---|
| one message per batch (the per-pass floor) | 3.31M passes/s | 2.73M | ~17% |
| a hundred per batch | 3.70M | 3.30M | ~11% |
| three pipelines deep | 0.98M | 0.86M | ~13% |
| a hundred per batch, a checkpoint inside each 12 s window | 4.85M | 4.01M | ~17% |

That is ~40–60 ns a pass — one boxed, deferred ack and its coalesce — on a
pipeline that otherwise does nothing at all; a real input's own per-batch work
(a kafka poll, a parse) dwarfs it. **Without `--state-dir` the cost is nil**:
the same rows against `main` are within the measurement noise (2–4% at the
synthetic floor, none on any row that does real work), which took the three
fixes above to get to.

## Not in this change

- **Transactional outputs** (two-phase commit tied to checkpoint completion —
  kafka transactions, files renamed into place on commit). At-least-once to
  non-idempotent outputs is what this change promises.
- **Deterministic idempotency keys** for `tidepool`/`indu`: today's key carries
  a random run id, so a replay is not deduplicated. Making it deterministic
  needs batch boundaries that are stable across a replay, which `max_batch`
  (it drains whatever has *already arrived*) does not give.
- **Unaligned checkpoints**: alignment under heavy backpressure delays a
  checkpoint; persisting in-flight edges instead is Flink's fix and is not
  needed at kayak's scale yet.
- **mqtt output flushing** at a checkpoint: rumqttc gives no way to wait for
  its queue to drain, so a publish queued in the instant before a crash can be
  lost. The nats output flushes; the outputs whose `emit` is synchronous need
  nothing.
- **Replicated / remote state** (a cluster): the coordinator's writer is the
  seam a remote store would plug into, and nothing here assumes a local disk
  beyond it.
