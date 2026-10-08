# polling an api

The `http` input is reached: something posts to the pipeline. The `http_poll`
input reaches out: a `GET` on a timer, and the whole reply handed on every
time. It is the [database inputs](/io/database-inputs)' `snapshot` mode for a
system that only has an api.

```jsonc
// config.json — the machine list from an ERP, every hour
{ "type": "http_poll",
  "url": "https://erp.example.com/api/machines",
  "interval_secs": 3600,
  "items": "/data/machines",
  "auth": { "type": "bearer", "token": "${ERP_TOKEN}" },
  "max_batch": 500 }
```

## what a reply becomes

An array is one message per element; anything else is one message. `items`
is a JSON pointer into a reply that wraps its records — `/data/machines`
reads the array at `data.machines` — and what it points at is split the same
way. A pointer at nothing is a failed read rather than an empty snapshot: an
api that changed its shape should show up on the card, not as a pipeline that
quietly stopped sending.

`max_batch` is the knob it is on every input. It defaults to one message per
batch, so a reply of 500 machines is 500 passes through the run loop unless it
is raised, and raising it is the cheapest fix there is.

## why snapshots only

This is for **reference data**: a list of machines, recipes, sites or
thresholds that changes rarely and that something downstream needs all of. It
reads the whole thing every time and hands all of it on, with no notion of
what changed since the last read. That is cheap when the list is small, and
harmless when the sink keeps the latest value per key, such as a Tidepool
table with a primary key or a `remember` transform keyed by id. Sending the
same rows again changes nothing, and a periodic read repairs anything that
went missing.

There is deliberately no incremental mode. An api has no common way to ask
for "rows after this one" (a cursor parameter, a `Link` header, a page
number, a `since`), and choosing one would mean choosing an api. A source that
grows faster than it can be read whole every interval is a stream, and
belongs on a broker or behind the [`http` input](/io/posting-into-a-pipeline).

Deletes are not seen either: a machine missing from the list is just not sent.
If that matters, have the api report it (`"active": false`) rather than leave
it out.

## failures, the interval and the reply's size

`interval_secs` counts from the *end* of one read to the start of the next,
and the first read happens as soon as the pipeline starts. A read that fails is
reported once on the card and retried on the backoff every broker input
reconnects on. That covers an unreachable host, a status other than 2xx (the
api's own complaint is quoted), a body that is not JSON, and an `items` that
points at nothing. Once a read succeeds, the interval starts again.

A reply is held whole, so it is bounded: past 64 MiB the read fails rather than
the process growing to fit. `timeout_seconds` (30 by default) bounds how long
one request can take.

`auth` is the same block the `http` output presents: a `bearer` token or a
header of your choosing, with the value as a `${NAME}` reference. With an
`envelope`, each message carries the `url` it came from (minus any username
or password) and a `polled_at` that every message of one read shares.

<!--@include: ../reference/generated/components/inputs/http_poll.md-->
