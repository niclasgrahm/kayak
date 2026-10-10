# pipelines

A pipeline has three parts: `inputs`, `transforms` and `outputs`. All three are
arrays.

```yaml
- id: sensors_archive
  inputs:
    - type: pipeline
      upstream: sensors
      buffer: { type: batch, size: 10, window_seconds: 30 }
  transforms: []
  outputs:
    - { type: postgres, connection: local-postgres, table: readings }
    - { type: stdout }
```

## inputs, transforms and outputs

kayak merges all inputs of a pipeline into one stream. The transform chain runs
one time for each batch, from any input. There is no order between batches from
two different inputs.

Each output gets every batch. To write the same data to postgres and to stdout,
use one pipeline with two outputs.

A pipeline must have at least one input. kayak refuses to build a pipeline with
no inputs. A pipeline can have zero outputs. Such a pipeline sends its batches
only to the pipelines downstream of it.

The `pipeline` input connects pipelines into a graph. Its `upstream` field names
another pipeline. One pipeline can feed many downstream pipelines, and one
pipeline can read from many upstream pipelines. The graph can have any depth.

## failures

- **An input fails.** kayak reports the error and the pipeline continues with
  its other inputs. The pipeline stops when its last input stops.
- **An output fails on a batch.** kayak reports the error and skips that output
  for that batch. The other outputs and the downstream pipelines still get the
  batch.
- **An output cannot start.** The run loop does not start until every output
  starts. kayak tries again with a backoff. It does not read the inputs in the
  meantime.

## buffering an input

Add `buffer` to any input to collect messages into larger batches before the
transforms get them. There are three types:

```jsonc
{"buffer": {"type": "static",   "size": 100}}                        // count
{"buffer": {"type": "tumbling", "window_seconds": 10}}               // time
{"buffer": {"type": "batch",    "size": 100, "window_seconds": 10}}  // either
```

A `batch` buffer closes when it reaches one of its two limits. The `size` limit
sets the largest batch when the input is busy. The `window_seconds` limit sets
the longest wait for a message when the input is quiet. Use `batch` when the
rate of the input changes. Use it in front of an output that has a cost per
write, for example a database insert.

The three types follow two rules:

- **A buffer never sends an empty batch.** The window opens when the first
  message of the batch arrives. Thus a quiet input sends nothing. Windows do not
  align with the wall clock. A buffer sets a limit on the wait of a message. It
  does not give a fixed cadence.
- **`size` is a minimum.** kayak does not split a batch that arrives. An input
  that already sends batches can make a buffer larger than `size`.

Two other settings have similar names:

- `max_batch` on the kafka and nats inputs does not wait. It takes one message
  and adds the messages that already arrived. A quiet topic gives batches of one.
- The `buffer` *transform* is a different component. It collects the output of
  the transforms in front of it. It can also wait on a
  [state bucket](/pipelines/state#gating-a-buffer-on-a-bucket).

## acknowledging an input

Add `ack` to any input to set when the input tells its broker that a message is
done:

```jsonc
{"ack": "on_receipt"}   // the default: before any transform or output gets it
{"ack": "on_delivery"}  // after this pipeline is done with it
```

`on_receipt` is the default. It has a low cost. If the process stops between
receipt and output, the message is lost, because the broker already forgot it.

`on_delivery` acknowledges a batch when this pipeline is done with it. That is
when both of these are true:

- kayak sent the batch to every output of the pipeline.
- Every downstream pipeline accepted the batch into its inbox.

The acknowledgement does not wait for downstream pipelines to finish. If
pipeline A feeds pipeline B and the output of B fails, A has already
acknowledged the message.

A failed output does not stop the acknowledgement. `on_delivery` means that the
pipeline tried every output. It does not mean that every output succeeded.

Only two inputs accept `on_delivery`:

- **kafka.** The input stores the offset itself after the batch clears the
  pipeline.
- **mqtt at qos `at_least_once` or `exactly_once`.** The broker keeps the
  message open for redelivery until kayak acknowledges it.

mqtt at qos `at_most_once` has no redelivery, so it refuses `on_delivery`.
Every other input also refuses to build with `on_delivery`.
