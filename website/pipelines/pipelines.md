# pipelines

A pipeline is `inputs → [transforms] → outputs`, and all three are arrays.

Every input is **merged** into one stream: the transform chain runs once per
batch, whichever input produced it, and there is no ordering between two
different inputs. Every output then receives **every** batch. So "archive to
postgres and watch it on stdout" is one pipeline with two outputs, not two
pipelines.

At least one input is required — a pipeline with none could never produce
anything, so it's rejected at build time. Zero outputs is fine: such a pipeline
exists to feed the ones downstream of it, and still fans out to them.

The failure rules follow from that. One input dying is reported on the card and
survived, because the others are still feeding the pipeline; only the last one
going takes the run loop with it. One output failing is reported and skipped for
that batch, and its siblings and the downstream pipelines still get theirs. An
output that can't `init()` at all is fatal, since it would never accept anything.

Merging runs a pump task per input (`inputs::merge`) rather than `select!`ing
over them. Selecting drops the losing futures on every iteration, and an input
that waits on a timer would have its timer restarted every time a chattier
sibling produced — starving it forever. There's a test for exactly that.

## buffering an input

`buffer` sits beside `envelope` on any input and gathers messages into bigger
batches before the transforms see them. It has three shapes, which are the same
two limits with different halves left off:

```jsonc
{"buffer": {"type": "static",   "size": 100}}                        // count
{"buffer": {"type": "tumbling", "window_seconds": 10}}               // time
{"buffer": {"type": "batch",    "size": 100, "window_seconds": 10}}  // either
```

`batch` closes on whichever limit is reached first, which is what a stream with a
varying rate wants: the count bounds how big a batch gets when the input is busy,
and the window bounds how long a message waits when it is quiet. It is the usual
choice in front of an output that pays per write — the `sensors_archive` pipeline
in the sample buffers that way ahead of its postgres insert.

Two rules hold for all three:

- **A buffer never emits an empty batch.** The window opens when the *first
  message of the batch* arrives, not when the buffer was asked for one, so an
  input that goes quiet emits nothing at all rather than a tick of nothing every
  window. The cost, and it is deliberate: windows aren't aligned to a wall clock.
  What a buffer promises is a bound on how long a message waits, not a cadence.
- **`size` is a floor, not a ceiling.** An arriving batch is never split, so an
  input already batching on its own can overshoot — the same rule a file output's
  `max_rows` follows.

Don't confuse it with the two neighbours it reads like. `max_batch` on the kafka
and nats inputs never *waits*: it takes one message and drains whatever has
already arrived, so a quiet topic still yields batches of one. And the `buffer`
*transform* is a different component in a different place — it batches what the
transforms in front of it produced, and it is the one that can wait on a
[state bucket](/pipelines/state#gating-a-buffer-on-a-bucket).

## acknowledging an input

`ack` sits beside `buffer` and `envelope` on any input and decides when it
tells its broker a message is done with:

```jsonc
{"ack": "on_receipt"}   // the default — before any transform or output sees it
{"ack": "on_delivery"}  // after this pipeline has finished with it
```

`on_receipt` is what every input has always done, and it is what you get by
leaving `ack` out. It's cheap and it's what a broker with no other option
gives you anyway — but a crash between receipt and output can lose the
message, because the broker was told to forget it before this pipeline had
done anything at all.

`on_delivery` acknowledges once the batch has cleared **this pipeline**:
every output it owns has been sent the batch (whether or not that send
succeeded — see below), and every downstream pipeline fed from it has
accepted the handoff into its inbox. It does **not** follow the message any
further than that. If pipeline A feeds pipeline B and B's own output fails,
A has already acknowledged — "delivered" means "this pipeline is done with
it," not "the whole graph is done with it." Chasing a message through the
graph would tie one input's redelivery to the health of pipelines several
hops away that can be edited or deleted independently of it, which is a
guarantee kayak doesn't make.

A failing output does not withhold the acknowledgement either way. `on_delivery`
answers "did this pipeline attempt every send," not "did every sink succeed" —
a stronger per-output guarantee is a real idea for later, not something this
mode already gives you.

Only inputs with a broker-side notion of "received" vs "delivered" can honour
`on_delivery` — today that's **kafka** (it turns off the client's automatic
offset store and stores the offset itself once the batch clears the pipeline)
and **mqtt at qos `at_least_once` or `exactly_once`** (the client is told to
leave acking to us, and the broker holds the message open for redelivery until
it hears back). An mqtt subscription at qos `at_most_once` has no redelivery
at all, so `on_delivery` is refused there too — the same rule mqtt's own qos
already draws. Every other input — `nats`, `redis`, `opcua`, `dummy`, `http`, `pipeline` —
refuses to build if you ask it for `on_delivery`, rather than silently
behaving like `on_receipt`. The full reasoning, including why the scope stops
at this pipeline's own outputs, lives in `src/inputs/ack.rs`'s module docs.
