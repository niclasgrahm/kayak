# history

A card's log and chart are fed by `/events`, which is a **live sample**: the
server only produces it while a browser is attached, and it drops passes under
load on purpose. That is right for watching a pipeline and useless for the
question that actually gets asked — *it broke at 02:14 and I got here at 08:00,
what happened?* Nobody was watching at 02:14, so there was no feed.

So the server also keeps a record, in memory, whether or not anyone is looking:

- **throughput**, as counts per time bucket — messages in, messages out,
  failures;
- **failures**, aggregated to one entry per distinct message, with when it was
  first seen, when it was last seen and how many times it has happened.

A broker that was down for six hours is *one* line saying so with a tally,
which is both cheaper to keep and easier to read at 08:00 than the six hours of
log it replaces.

**No message payloads are kept.** History is counts and error texts and nothing
else. That is a deliberate limit rather than a gap: payloads would tie the
storage to the throughput, and a day of message bodies sitting beside the server
is a data-retention decision, not a UI feature.

## the knob

One duration, in the `--server-config` file:

```yaml
history:
  retention_secs: 86400   # a day. the default; `0` turns it off entirely
```

A day is what a server with no settings file runs, because the person this is
for is the one who doesn't know to go looking for it. The buffers are **ring
buffers whose size is derived from the retention** — fixed capacity, oldest
bucket dropped off the end — so memory is flat in uptime *and* in throughput: a
pipeline doing eight million messages a second costs the same as an idle one,
because a bucket holds counts and never messages. Reckon on about 58 kB per
pipeline for a day. `0` allocates nothing and records nothing.

There are two resolutions. A five-second one covering the last half hour, which
is what a card's chart is backfilled from so it starts full instead of drawing
itself over the following two minutes; and a one-minute one covering the whole
retention, which is the overnight record. Only the second is configurable — the
first is sized by what a card can display.

## reading it

Open a card's **stats** section: the chart arrives already filled, and anything
the pipeline has failed at is listed underneath it with a time, how long ago and
a count. Nothing is listed when there is nothing to list.

Or ask directly:

```bash
curl localhost:6767/api/pipelines/my-pipeline/history
curl 'localhost:6767/api/pipelines/my-pipeline/history?resolution=fine'
```

An unknown pipeline answers with an empty history rather than a 404 — the record
deliberately outlives the pipeline, so that deleting one, or reverting a config
(which rebuilds every pipeline in it), does not throw away the evidence of what
went wrong.

## what it does not do

**It does not survive a restart.** This is a ring buffer in the process, so a
deploy loses the record. That covers the case it was built for — the pipeline
died, the server didn't — and not the one where the server itself was restarted.
Making it durable means a database, which is tracked in the roadmap.

**Two pipelines' histories are independent and nothing correlates them.** It is
a per-pipeline readout, not a metrics system. At the point where you want a week
of retention, alerting, or one dashboard across a fleet, the honest answer is a
real metrics store rather than a bigger number in `retention_secs`.
