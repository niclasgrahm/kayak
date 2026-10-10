# history

The server keeps a record of what each pipeline did. The record is in memory.
The server keeps it for every pipeline, also when no client is connected. Read
it with one request:

```bash
curl localhost:6767/api/pipelines/sensors/history
curl 'localhost:6767/api/pipelines/sensors/history?resolution=fine'
```

## what it records

The record has two parts:

- **counts per time bucket**: messages in, messages out and failures;
- **failures**: one entry for each distinct failure, with the time it was first
  seen, the time it was last seen and a count.

A broker that is down for six hours gives one failure entry with a count. It
does not give six hours of log lines.

The counts are complete. The run loop increments three counters on every pass,
and the server reads them every 5 s. The failure count includes the repeats
that the log of the web UI does not show.

The record contains no message payloads. It contains counts and error texts
only. Thus its size does not change with the throughput.

## the response

`GET /api/pipelines/{id}/history` returns a `PipelineHistory`:

```json
{
  "resolution": "coarse",
  "bucket_secs": 60,
  "buckets": [
    { "start": 1786612800, "inbound": 6000, "outbound": 6000, "errors": 0 },
    { "start": 1786612860, "inbound": 0, "outbound": 0, "errors": 59 }
  ],
  "errors": [
    {
      "stage": "output",
      "component": 0,
      "message": "connection refused",
      "first_seen": 1786612861000,
      "last_seen": 1786634461000,
      "count": 21600
    }
  ],
  "dropped_signatures": 0
}
```

| field | meaning |
| --- | --- |
| `resolution` | the resolution of the buckets: `coarse` or `fine` |
| `bucket_secs` | the width of one bucket, in seconds |
| `buckets` | oldest first, with no gaps. An empty bucket has zeros. |
| `buckets[].start` | the start of the bucket, in seconds since the epoch |
| `buckets[].inbound` | messages that arrived at the inputs |
| `buckets[].outbound` | messages that the transform chain gave to the outputs, counted one time per batch |
| `buckets[].errors` | failures at any stage |
| `errors` | distinct failures, most recently seen first, 64 at most |
| `errors[].stage` | `input`, `transform` or `output` |
| `errors[].component` | the index of the component in its list. `null` for an input failure. |
| `errors[].first_seen`, `last_seen` | milliseconds since the epoch |
| `errors[].count` | how many times the failure occurred |
| `dropped_signatures` | distinct failures that the server removed to stay at 64 |

A failure has the same identity when its stage, its component and its text are
the same. A non-zero `dropped_signatures` usually means that the error text
contains a message id or an offset.

The query parameter `resolution` selects one of two rings:

| resolution | bucket | covers |
| --- | --- | --- |
| `coarse` (the default) | 60 s | the configured retention |
| `fine` | 5 s | the last 30 min |

An unknown value of `resolution` gives the default. An unknown pipeline id gives
an empty history with status `200`, not a `404`. A deleted pipeline keeps its
record until the retention ends. A config revert rebuilds every pipeline and
keeps their records.

## configure the retention {#the-knob}

Set the retention in the `--server-config` file:

```yaml
history:
  retention_secs: 86400   # one day, the default
```

- The default is one day, also without a `--server-config` file.
- The maximum is 604800 (seven days). The server does not start with a larger
  value.
- `0` turns history off. The server then allocates nothing and records nothing,
  and the endpoint returns an empty history.

The rings have a fixed size, which kayak calculates from the retention. When a
ring is full, kayak removes the oldest bucket. A day of retention costs about
58 kB per pipeline. This cost does not change with the throughput or the
uptime. You cannot configure the fine ring.

## in the web UI {#reading-it}

The **stats** section of a card shows the history. The chart starts with the
recorded buckets, so it is full when it opens. Below the chart, the card lists each
failure with its time and its count. When there are no failures, the list is
not shown.

## limits {#what-it-does-not-do}

- **A restart loses the record.** The record is in the memory of the process.
  It shows a pipeline that failed while the server continued to run. A durable
  store is on the roadmap.
- **The record is per pipeline.** It does not correlate two pipelines. For long
  retention, alerts or a dashboard across many servers, use a metrics system.
  Poll the history endpoint to feed it.
