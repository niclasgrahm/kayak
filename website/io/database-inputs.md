# database inputs

The `postgres` and `clickhouse` inputs run a query on a timer. Each row becomes
one message, with the column names as its fields.

```jsonc
// config.json — follow a table as rows are added to it
{ "type": "postgres", "connection": "local-postgres",
  "table": "readings",
  "interval_secs": 5,
  "mode": { "type": "incremental", "field": "id" },
  "max_batch": 100 }

// ...or poll a query as reference data
{ "type": "clickhouse", "connection": "local-clickhouse",
  "query": "SELECT sensor, max(value) AS peak FROM sensor_readings GROUP BY sensor",
  "interval_secs": 30,
  "mode": { "type": "snapshot" } }
```

The server converts each row to JSON: postgres with `row_to_json`, ClickHouse
with `JSONEachRow`. A timestamp is ISO 8601, a `numeric` keeps its digits, and a
`jsonb` column gives the value that it holds:

```json
{"id": 42, "sensor": "press-3", "value": 21.5, "recorded_at": "2026-01-01T12:00:00.123456+00:00"}
```

## a table and a query are the same thing

Give exactly one of `table` and `query`. The input puts it in a subquery. Then
it adds the `columns` projection, the cursor condition, the order and the page
limit around it.

- An incremental read of a `query` needs no placeholder and no `ORDER BY`. The
  query must only return the field that the input follows.
- The query must be one `SELECT` with no semicolon at the end. Anything that
  the server accepts in a subquery is valid, including `WITH`.
- `columns` is a list of the columns to select. To leave out a large column,
  do not put it in the list. To remove a field later in the pipeline, use a
  [`map`](/pipelines/reshaping-messages).

## the two modes

### snapshot

**`snapshot`** reads the whole relation on each read, in one query. It sends
every row. Use it for reference data, for example a table of recipes or
thresholds that a `remember` transform keeps. Use it also for an aggregate that
the server calculates.

A snapshot has no page limit. Use it only for a relation that fits in memory.

With an `envelope`, all rows of one read have the same `polled_at`. Use it to
tell the rows of one snapshot from the rows of the previous one.

### incremental

**`incremental`** follows a column that grows, for example an `id` or an
`updated_at`. It reads only the rows above the highest value that it sent. This
value is the **watermark**. The input reads in pages, in the order of the
column.

- **`field`**: the column to follow. The input never reads rows where this
  column is `null`.
- **`start_from`**: `newest` (the default) or `oldest`. With `newest`, the
  first read finds the highest value and reads only rows above it. With
  `oldest`, the input reads the whole table first, page by page, and then
  follows it.
- **`lag_secs`**: for a timestamp column only. The input leaves rows that are
  newer than `now()` minus this many seconds for a later read. This gives a
  late transaction time to commit. The server refuses it on a numeric column.

Rules for the watermark:

- **It is in memory.** After a restart, the input starts again from
  `start_from`. Thus an incremental input is *at least once* across a restart.
- **It moves when the input sends the rows.** It does not wait for the
  outputs. The run loop acknowledges a batch whether or not its outputs
  succeed (see [acknowledging an input](/pipelines/pipelines#acknowledging-an-input)).
  Thus `ack: on_delivery` fails at build time.
- **The input handles ties at a page boundary.** Many rows can have the same
  timestamp. The input cuts a full page before its last distinct value. It
  reads those rows again, complete, on the next page. If all rows on a page have
  the same value, the input cannot cut the page. It sends the page as it is and
  logs a warning. Increase `page_size`, or follow a field with fewer ties.
- **The input does not see rows that commit late.** A long transaction can
  commit a row with a value below the watermark. Use `lag_secs` for this case.
  For every change at commit time, you need change data capture. This input
  does not do that.
- **The input does not see deletes.** It sees an update only if the update
  increases the cursor field. Use an `updated_at` column for this.

**Index the field that you follow.** Each read is `WHERE field > $1 ORDER BY
field LIMIT n`. On an indexed column, this is one index lookup. On a column
without an index, it is a scan of the whole table on each read.

## paging, batching and the interval

- **`page_size`**: the largest number of rows in one query. The default is
  1000. A full page starts the next query at once. A short page ends the read.
- **`interval_secs`**: the wait after the end of one read and before the next
  read. Thus a slow read never overlaps the next one. The first read occurs when
  the pipeline starts.
- **`max_batch`**: the largest number of rows in one batch. The default is 1.
  The input does not wait for a batch to fill.

### performance

Set `max_batch`. With the default of 1, a read of 1000 rows makes 1000 passes
through the run loop. With `max_batch: 1000`, it makes one pass.

## failures

If the database is down, the read fails. The input reports the error one time
and retries with backoff. The watermark does not change, so the input skips no
rows. A table that does not exist yet is the same case. The input starts to
read when the table exists.

## sampling

To see some rows before you create the pipeline, post the input to
`POST /api/inputs/sample`:

```bash
curl -X POST localhost:6767/api/inputs/sample \
     -H 'content-type: application/json' \
     -d '{"input": {"type": "postgres", "connection": "local-postgres",
          "table": "readings", "interval_secs": 5,
          "mode": {"type": "incremental", "field": "id"}}}'
```

If the input starts from the newest rows, the sample reads from the oldest
rows. The `notes` in the reply say so. The sample does not change the pipeline.
The web UI uses the same endpoint.

## what is on the wire

The watermark goes to the server as **text**. The server casts it to the type
of the column.

- Postgres returns `(field)::text` with each row. The next query uses
  `($1::text)::<type>`. The input reads the type from a prepared statement.
- ClickHouse returns `toString(field)`. The next query uses
  `CAST({cursor:String} AS <type>)`. The input reads the type with `DESCRIBE`.

Text keeps the exact value of every type. The
[database outputs](/io/database-outputs) use the same method in the other
direction.

The ClickHouse input sends two settings with each request:

- `output_format_json_quote_64bit_integers=0`, so an `Int64` arrives as a
  number. By default, ClickHouse sends it as a string.
- `date_time_output_format=iso`, so a `DateTime` arrives as
  `2026-01-01T12:00:00Z`. The postgres output refuses the default format.

<!--@include: ../reference/generated/components/inputs/postgres.md-->

<!--@include: ../reference/generated/components/inputs/clickhouse.md-->
