# inputs

An input is where a pipeline's messages come from. A pipeline may have several
— they are merged into one stream, each pumped by its own task so a slow or
timer-driven input can't be starved by a busy one — and one input failing is
reported and survived; the pipeline stops only when the last one is gone.

Three fields are declared by no input kind and accepted by all of them, because
they sit on the wrapper rather than on the kind:

- **`buffer`** gathers messages before the transforms see them, by count, by
  time, or by whichever comes first. It never emits an empty batch, and its
  window opens at the first message rather than at the clock — so what it
  promises is a latency bound, not a cadence. See
  [buffering an input](/pipelines/pipelines#buffering-an-input).
- **`envelope`** attaches what the input knows about a message *to* the message,
  in band, as ordinary JSON fields. Absent, messages are passed on byte for
  byte as they arrive. See [message metadata](/pipelines/message-metadata).
- **`ack`** says when the input tells its broker a message is done with. Only
  inputs with a broker-side notion of the difference honour it; the rest refuse
  to build rather than quietly ignoring it. See
  [acknowledging an input](/pipelines/pipelines#acknowledging-an-input).

`max_batch`, on the inputs that have it, is a third thing again: it never
*waits*. It takes one message and then drains whatever has already arrived, so
a quiet topic yields batches of one however high the cap is and only a catch-up
ever fills one. That is what makes it the cheapest fix there is for a consumer
replaying a backlog.

<!--@include: ./generated/components/inputs.md-->
