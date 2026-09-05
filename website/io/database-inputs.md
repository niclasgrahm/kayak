# database inputs

Every other input is reached by its messages: a broker pushes, a device
publishes, a client posts. A database does none of that, so the `postgres`
and `clickhouse` inputs *ask* — a query on a timer, each row handed on as one
message with the column names as its fields.

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

The row is rendered by the server itself — `row_to_json` on postgres,
`JSONEachRow` on ClickHouse — so a timestamp is ISO 8601, a `numeric` keeps
its digits, a `jsonb` column arrives as the value it holds, and the list of
what each type looks like is the server's rather than one maintained here:

```json
{"id": 42, "sensor": "press-3", "value": 21.5, "recorded_at": "2026-01-01T12:00:00.123456+00:00"}
```

## a table and a query are the same thing

`table` and `query` are two ways of naming a relation, and exactly one of them
is given. Everything else is wrapped *around* it as a subquery — the `columns`
projection, the cursor condition, the ordering, the page limit — so a
hand-written query is the source and not the whole statement. An incremental
read of a query needs no placeholder and no `ORDER BY` of its own; it needs
only to return the field it follows. The query is one `SELECT`, with no
trailing semicolon, and anything the server accepts in a subquery is fine,
a `WITH` included.

`columns` is a projection and nothing more: it generates the select list.
There is deliberately no `exclude` — that would need the table's own column
list to subtract from, and dropping a field is what a [`map`](/pipelines/reshaping-messages)
already does. The one case an exclusion is really for, a blob column not worth
transferring, is a `columns` list that leaves it out.

## the two modes

**`snapshot`** reads the whole relation every tick, in one query, and hands on
every row. That is for reference data — a table of recipes or thresholds that
a `remember` transform keeps current, an aggregate the server computes — and
for relations that fit in memory, since there is no page limit on a snapshot.
With an `envelope`, every row of one read carries the same `polled_at`, which
is what tells one snapshot's rows from the previous one's.

**`incremental`** follows a column that grows — an `id`, an `updated_at` — and
reads only rows past the highest value already handed on, in pages ordered by
that column. Everything worth knowing about it is a consequence of that
**watermark**:

- **It lives in memory.** A restart starts over from `start_from`, so an
  incremental input is *at least once* across a restart. `start_from`
  defaults to `newest` — the first read finds the highest value there is and
  reads only rows above it — because replaying a whole table into a pipeline
  is the surprising outcome and the one to ask for. `oldest` reads the table
  through first, page by page, and then follows it.
- **It moves when rows are handed on**, not when they are delivered. The run
  loop acknowledges a batch whether or not its outputs succeeded (see
  [acknowledging an input](/pipelines/pipelines#acknowledging-an-input)), so
  tying the watermark to the acknowledgement would buy nothing today, and
  `ack: on_delivery` is refused rather than accepted as a promise the input
  cannot keep.
- **Ties at a page boundary are handled.** Several rows can share a timestamp,
  and a page that ends in the middle of them would lose the rest. A full page
  is cut before its last distinct value and those rows are read again on the
  next page, whole. The one page that cannot be cut — every row on it shares
  the value — is handed on as it is, with a warning; raise `page_size` or
  follow a field with fewer ties.
- **Rows that commit late are never seen.** A row written with a cursor value
  below the watermark — a long transaction, a clock behind the others — is
  behind the input by the time it is visible. `lag_secs` holds the input back
  from `now()` by that much to give such rows time to land. It cannot turn a
  polling input into change-data capture; if you need every change the moment
  it commits, that is a replication slot and a different component.
- **Deletes are invisible, and updates only as visible as the field makes
  them.** A row that is updated is read again only if the update moves its
  cursor, which is what an `updated_at` column is for.

**Index the field you follow.** Every read is `WHERE field > $1 ORDER BY field
LIMIT n`, which is one index probe on an indexed column and a scan of the whole
table on an unindexed one, once per tick, against a database that has other
work to do.

## paging, batching and the interval

`page_size` (1000 by default) bounds one query and so bounds what the input
holds. A read that fills a page asks for the next one straight away; only a
page that comes back short ends the read, and only then does `interval_secs`
start — counted from the *end* of one read to the start of the next, so a read
that takes longer than the interval never overlaps itself. The first read
happens as soon as the pipeline starts.

`max_batch` is the same knob it is everywhere: rows already read are grouped
up to that many, and the input never waits for a batch to fill. It defaults
to one message per batch, which is the promise every input makes; a poll that
returns a thousand rows is a thousand passes through the run loop unless it is
raised, and raising it is the cheapest fix there is.

A database that is down is a read that fails: reported once on the card,
retried on the same backoff every broker input reconnects on, and the
watermark untouched, so nothing is skipped over an outage. A table that does
not exist yet is the same case and comes right when the table does.

## sampling

The sample button works, and better than on a broker: a database still holds
what was written before the sample started, so the rows are there to show.
An input that would start from the newest rows is sampled from the oldest
instead, and the sample says so — the pipeline itself is untouched.

## what is on the wire

The watermark travels as **text**. Postgres hands back `(field)::text` beside
each row and the next query casts it with `($1::text)::<type>`, the type
read off a prepared statement; ClickHouse hands back `toString(field)` and
the next query does `CAST({cursor:String} AS <type>)`, the type read off a
`DESCRIBE`. Text is the one representation every type round-trips exactly, and
it means the input never has to bind a timestamp or a numeric as a value of
its own — the same division of labour the [database outputs](/io/database-outputs)
buy with `$n::text::NUMERIC`, in the other direction.

Two ClickHouse settings ride on every request and are the difference between
rows a pipeline can use and rows it cannot:
`output_format_json_quote_64bit_integers=0`, because the server quotes every
`Int64` as a string by default, and `date_time_output_format=iso`, so a
`DateTime` arrives as `2026-01-01T12:00:00Z` rather than a bare date the
postgres output would refuse to write.

<!--@include: ../reference/generated/components/inputs/postgres.md-->

<!--@include: ../reference/generated/components/inputs/clickhouse.md-->
