# tidepool

Tidepool is a live analytics server. It keeps tables in memory and updates its
dashboards as data arrives. The `tidepool` **output** writes the messages of a
pipeline into one Tidepool table, with one request for each batch.

## the connection

```yaml
# pipelines.connections.yaml
tidepool:
  type: tidepool
  url: http://localhost:7070
  token: ${TIDEPOOL_INGEST_TOKEN}   # only when the server's ingest is guarded
  allow_http: true                  # a token over plaintext has to be asked for
```

- **`url`**: the address of the Tidepool server.
- **`token`**: the ingest token of the server. The admin token also works.
  Omit it when the ingest of the server is open. A local server is open by
  default.
- **`allow_http`**: set this to `true` to send a token over a plain `http://`
  url. Without it, kayak refuses the connection, because the token goes with
  every batch.

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

The table must exist. The Tidepool project declares the table and its column
types. kayak does not create tables in Tidepool.

At startup, the output reads the table and compares the mapping with it. These
problems stop the start, before any batch:

- a column that the table does not have (the error lists the columns that it
  has);
- a mapping type that the Tidepool column cannot take;
- a column that Tidepool requires in every row, which the mapping does not
  write or writes as null.

If Tidepool is not reachable at startup, the output tries again with backoff.

`columns` uses the same format as the
[database outputs](./database-outputs#column-mapping). These pairs are valid:

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

A `decimal` keeps the digits of the message. It does not go through a float.

Omit `columns` when the messages already have the shape of the table. The
output then sends each message as one row.

Other fields:

- **`on_extra_fields`**: `ignore` (the default) or `error`, for a message with
  fields that no column reads.
- **`retry_seconds`**: how long the output retries one batch. The default
  is 30.
- **`timeout_seconds`**: the longest time for one request. The default is 30.

## what fails a batch, and what is retried

Tidepool checks every value. It writes all of a batch or none of it. When it
refuses a batch, the error quotes the problems by row and column:

```
tidepool at http://localhost:7070/ (table 'readings') refused the batch
(400 Bad Request): row 0, column at: a value is required; and 2 more
```

The output does not send a refused batch again, because Tidepool refuses it
again. Before the next batch, the output reads the table again. The table
config of Tidepool can change while it runs. If someone removed a column, the
error then names the column.

The output retries these failures for up to `retry_seconds`:

- a `503` (Tidepool is busy);
- other `5xx` statuses;
- a `409` that says Tidepool still processes the same idempotency key;
- a server that is not reachable.

The output waits for the `Retry-After` time of the reply, or uses its backoff.
The pipeline waits during the retries. All attempts for one batch use the same
`Idempotency-Key`, `{pipeline}:{run}:{batch}`. For 24 hours, Tidepool returns
the first answer for a key that it knows. Thus a retry never writes a batch two
times.

A `400`, a `404`, a `422` or another `409` fails the batch at once.

## performance

Send large batches. Put a `buffer` on the input, for example 1 second or a few
thousand messages. One request for each message is slow.
