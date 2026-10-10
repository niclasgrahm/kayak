# inputs

An input is where the messages of a pipeline come from. A pipeline can have
several inputs. kayak merges them into one stream. Each input has its own task,
so a busy input cannot starve a slow input or an input on a timer. When one
input fails, kayak reports the failure and the other inputs continue. The
pipeline stops only when its last input stops.

Every input kind accepts three more fields. They are on the wrapper of the
input, so the tables below do not show them:

- **`buffer`** collects messages before the transforms see them: by count, by
  time, or by the first of the two. It never sends an empty batch. Its window
  opens at the first message, not at a clock boundary. Thus it limits the
  latency, and it does not give a fixed cadence. See
  [buffering an input](/pipelines/pipelines#buffering-an-input).
- **`envelope`** adds what the input knows about a message to the message, as
  ordinary JSON fields. Without it, kayak passes messages on as they arrive.
  See [message metadata](/pipelines/message-metadata).
- **`ack`** sets when the input acknowledges a message to its broker. Only
  inputs with a broker that can tell the difference accept it. The other inputs
  refuse to build with it. See
  [acknowledging an input](/pipelines/pipelines#acknowledging-an-input).

`max_batch`, on the inputs that have it, is different from `buffer`. It never
waits. It takes one message and then adds the messages that already arrived.
Thus a quiet topic gives batches of one, whatever the limit is. Only a backlog
fills a batch. Increase `max_batch` to let a consumer catch up on a backlog.

<!--@include: ./generated/components/inputs.md-->
