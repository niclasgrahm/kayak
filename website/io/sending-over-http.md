# sending over http

The other direction: the `http` **output** is a pipeline pushing its results at
a webhook or an ingest API.

```json
{ "type": "http", "url": "https://example.com/hooks/readings" }
```

That is the whole of the required config — POST, the batch as one JSON array,
no credential. Four optional fields shape it:

- **`verb`** — `POST` by default, `PUT` and `PATCH` accepted. `GET` and
  `DELETE` are refused at build time: a request with no body would send none of
  the messages, so a pipeline configured that way would make a round trip per
  batch and deliver nothing.
- **`body`** — `batch` (the default) sends the whole batch as one JSON array in
  one request; `message` sends one request per message. Which one is the
  receiving API's business, not a tuning knob. Under `message` the requests go
  out in order and the first failure fails the batch, so the messages after it
  are not sent — the same all-or-nothing a broker publish loop has.
- **`auth`** — the same block the `http` input takes, read the other way round:
  the input compares what arrived against it, the output presents it. A
  `bearer` token or a header of your choosing, and the value is a `${NAME}`
  reference like every other credential. The input's rule about `envelope`
  headers doesn't apply here, since an output reads no headers at all.
- **`timeout_seconds`** — 30 by default, and it is also the longest a slow
  endpoint can hold the pipeline up, since a batch whose request times out is a
  failed batch.

Three things worth knowing:

- **There is no connection behind it**, unlike every other output that talks to
  a server. A connection holds *what a system is* against what one pipeline
  wants from it, and for a webhook the url is the whole of the first half —
  there is nothing left to name once and share.
- **Anything but a 2xx fails the batch**, and the endpoint's own response body
  is quoted in the error (cut at 300 characters). That is what makes a webhook
  that is *rejecting* the data show up on the card rather than being written off
  as delivered. The reply is otherwise discarded — a service that answers with
  something the pipeline should carry on with is the `http` **transform**, not
  this.
- **A failing endpoint is not retried per batch.** The same backoff gate the
  nats, redis and clickhouse outputs use: after a failure the next batches fail
  immediately without touching the network until the delay has passed, so a
  webhook that is down gets one attempt every few seconds rather than one per
  message. Nothing is connected at startup, either — there is no request to
  make that would not be a delivery, so a url that is wrong is caught at build
  time and one that is unreachable is heard about on the first batch.

`heartbeat_to_webhook` in `example_config/` is the sample, and it points at the
server's own `ingest` endpoint on `127.0.0.1:6767` — so it is the one http
output that works under `just dev` with nothing else running, the same trick
`heartbeat_to_disk` uses. Change the port the server binds and change that url
with it.
