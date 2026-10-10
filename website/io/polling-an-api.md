# polling an api

The `http_poll` input sends a `GET` request on a timer. It sends the whole reply
into the pipeline on each read. It is the `snapshot` mode of the
[database inputs](/io/database-inputs), for a system that has only an API.

```jsonc
// config.json — the machine list from an ERP, every hour
{ "type": "http_poll",
  "url": "https://erp.example.com/api/machines",
  "interval_secs": 3600,
  "items": "/data/machines",
  "auth": { "type": "bearer", "token": "${ERP_TOKEN}" },
  "max_batch": 500 }
```

The input has no connection. The `url` and the `auth` are on the component.

## what a reply becomes

- A JSON array becomes one message for each element.
- Any other JSON value becomes one message.
- **`items`** is a JSON pointer into a reply that wraps its records. For
  example, `/data/machines` reads the array at `data.machines`. The input
  splits the value at the pointer with the same rules.
- If the pointer finds nothing, the read fails. Thus a change in the shape of
  the API shows as an error, and the pipeline does not stop without a sign.

### performance

Set `max_batch`. The default is 1. With the default, a reply of 500 machines
makes 500 passes through the run loop. The input does not wait for a batch to
fill.

## snapshots only {#why-snapshots-only}

Use this input for **reference data**: a list of machines, recipes, sites or
thresholds that changes rarely. The input reads the whole list each time and
sends all of it. It does not know what changed since the last read.

This works well when the list is small and the destination keeps the latest
value for each key. Examples are a Tidepool table with a primary key, or a
`remember` transform keyed by id. A row that arrives again changes nothing. A
row that was lost arrives on the next read.

There is no incremental mode. APIs use many different methods to ask for new
rows, for example a cursor parameter, a `Link` header, a page number or a
`since` value. If the data grows too fast to read whole on each interval, send
it through a broker or to the [`http` input](/io/posting-into-a-pipeline).

The input does not see deletes. A machine that is not in the list is not sent.
If deletes are important, make the API report them, for example with
`"active": false`.

## failures, the interval and the reply size {#failures-the-interval-and-the-reply-s-size}

- **`interval_secs`**: the wait after the end of one read and before the next
  read. The first read occurs when the pipeline starts.
- **`timeout_seconds`**: the longest time for one request. The default is 30.
- **Reply size**: the input holds a reply in memory, so the limit is 64 MiB.
  A larger reply fails the read.

These problems fail a read:

- a host that is not reachable;
- a status that is not 2xx (the error quotes the reply of the API);
- a body that is not JSON;
- an `items` pointer that finds nothing.

The input reports a failure one time and retries with backoff. After a read
succeeds, the interval starts again.

## credentials and metadata

`auth` is the same block as on the [`http` output](/io/sending-over-http): a
`bearer` token or a header that you name. Write the value as a `${NAME}`
reference.

With an `envelope`, each message carries:

- the `url` that it came from, without a username or password;
- a `polled_at` time, which is the same for all messages of one read.

<!--@include: ../reference/generated/components/inputs/http_poll.md-->
