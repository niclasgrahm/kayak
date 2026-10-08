# tidepool

Tidepool is the live analytics server kayak was built beside: tables held in
memory, a semantic model declared as code, dashboards that update as data
arrives. kayak is its connector layer. The `tidepool` **output** writes a
pipeline's messages into one of its tables, one request per batch.

## the connection

```yaml
# pipelines.connections.yaml
tidepool:
  type: tidepool
  url: http://localhost:7070
  token: ${TIDEPOOL_INGEST_TOKEN}   # only when the server's ingest is guarded
  allow_http: true                  # a token over plaintext has to be asked for
```

`token` is the server's ingest token (its admin token works too). Leave it out
for a server whose ingest is open, which is how a local one runs. With a token,
a plaintext `http://` url needs `allow_http`, for the clickhouse connection's
reason: the token goes with every batch.

## the output

```yaml
outputs:
  - type: tidepool
    connection: tidepool
    table: readings
    columns:
      - name: machine
        type: text
        field: tag
        nullable: false
      - name: at
        type: timestamp
        nullable: false
      - name: value
        type: decimal
```

The table has to exist: Tidepool's project declares it, with its column
types, and kayak never creates one. On start the output reads the table and
checks the mapping against it, so these fail the start rather than every
batch:

- a column the table doesn't have (the error lists the ones it has)
- a type Tidepool can't take from that mapping type
- a column Tidepool needs in every row that the mapping writes null for, or
  doesn't write at all

`columns` is spelled as the [database outputs](./database-outputs) spell it.
The pairs that fit:

| mapping type | Tidepool column |
| --- | --- |
| `text`, `uuid`, `json` | `string` |
| `integer` | `int32`, `int64`, `float64` |
| `bigint` | `int64` |
| `float` | `float64` |
| `decimal` | `decimal(p,s)`, `float64` |
| `boolean` | `bool` |
| `timestamp` | `timestamptz` |
| `date` | `date` |

A decimal goes across with the digits it was written with, never through a
float. Leave `columns` out to send each message as a row as it is, for messages
already shaped like the table.

## what fails a batch, and what is retried

Tidepool checks every value and writes a batch whole or not at all. A batch
it refuses fails with its problems quoted by row and column, as the card
shows them:

```
tidepool at http://localhost:7070/ (table 'readings') refused the batch
(400 Bad Request): row 0, column at: a value is required; and 2 more
```

That batch isn't sent again; it would be refused the same way. After a
refusal the output reads the table again before the next batch, because
Tidepool's config changes live: if a column was dropped meanwhile, the card
says so instead of repeating row errors.

A busy server (`503`, which is Tidepool's backpressure) or one that can't be
reached is retried after its `Retry-After`, for up to `retry_seconds` (30 by
default). The pipeline waits meanwhile, which is the point: there is nowhere
better to put the messages. Every attempt carries the same `Idempotency-Key`,
`{pipeline}:{run}:{batch}`, and Tidepool answers a key it has seen with the
first answer, so a request that landed and lost its reply is never written
twice.

Batches are worth making big: put a `buffer` on the input (a second, or a few
thousand messages) rather than sending a request per message.
