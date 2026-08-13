# database outputs and column mapping

The postgres output writes one row per message. Without a `columns` list it
writes the table it has always written — an `id`, a `received_at` and the whole
message in a `jsonb` `payload` — and with one, each message field lands in a
real column with a real type:

```jsonc
// config.json
{ "type": "postgres", "connection": "local-postgres", "table": "readings",
  "columns": [
    { "name": "sensor",      "type": "text",      "nullable": false },
    { "name": "value",       "type": "float",     "nullable": false },
    { "name": "recorded_at", "type": "timestamp", "field": "ts" },
    { "name": "subject",     "type": "text",      "field": "_meta.subject" },
    { "name": "raw",         "type": "json",      "message": true }
  ],
  "indexes": [ { "columns": ["recorded_at"] } ] }
```

`field` defaults to the column's name, so a message that already uses those
names needs nothing but a name and a type. It is a [field path](/pipelines/message-metadata#field-paths)
like every other field reference here, which is what makes `_meta.subject`
reach whatever the input's envelope attached, and `message: true` is the audit
column — the whole message, and the only source that can never be missing.

**The types are logical, not postgres'.** `float` rather than `double
precision`, `timestamp` rather than `timestamptz`. The list —
`text integer bigint float decimal boolean timestamp date uuid json` — lives in
`kayak-core/src/columns.rs` with the rest of the mapping, and each database
output renders it into its own DDL. That is the point of the split: the next
database output reuses the mapping whole, and a config written against one does
not have to be rewritten to point at another. Being a closed set, it also
becomes a dropdown in the add-pipeline form for free.

**Values are checked, never coerced.** A string `"12.5"` in a `float` column
fails the batch rather than arriving as a number, because a mapping that accepts
anything guarantees nothing. What is lenient is how a value travels: every
mapped column is bound as text and cast in the statement
(`$2::text::NUMERIC`), which keeps a number's own digits — nothing routes a
decimal through an f64 — and hands the parsing of a timestamp or a uuid to the
server, whose error about a malformed one is better than anything reimplemented
here. A `timestamp` takes a string the server parses, or a number read as
**seconds** since the epoch.

**A missing field writes `NULL`** by default; `on_missing` takes `error` or
`skip_row` (leave the whole message out) instead. A column declared
`"nullable": false` defaults to `error`, since there is nothing else it could
do, and telling one to write null is refused when the pipeline is built rather
than discovered as a constraint violation an hour into a run. A field that is
present but `null` counts as missing — the same reading the reducer takes.
`on_extra_fields: "error"` is the opposite direction: for a stream whose shape
is supposed to be fixed, a new field appearing is news rather than noise.

**Creating the table.** On by default, `IF NOT EXISTS`, and it never *alters*: a
table whose shape has moved on fails the insert with the server's own error
rather than being migrated from a config file, which is a far bigger promise
than "create it if it isn't there". `create_table: false` is for a table someone
else owns. Without a `primary_key` the created table gets an `id` and a
`received_at` of its own; naming one says the data carries its own identity and
drops both — and makes those columns not-null, because postgres would anyway.
`indexes` are created with the table and are named after it and their columns.
A `primary_key` or an `index` naming a column nothing maps fails the build.

## clickhouse

The `clickhouse` output takes the same `columns` list, spelled exactly the same
way — that is what the mapping being database-neutral is *for*, and a config
pointed at one server moves to the other by changing the type and the
connection:

```jsonc
// config.json
{ "type": "clickhouse", "connection": "local-clickhouse", "table": "sensor_readings",
  "columns": [
    { "name": "sensor",      "type": "text" },
    { "name": "value",       "type": "float" },
    { "name": "recorded_at", "type": "timestamp", "field": "ts" },
    { "name": "raw",         "type": "json",      "message": true }
  ],
  "order_by": ["recorded_at", "sensor"] }
```

Three things differ, and each is ClickHouse being itself rather than a gap.

**`order_by` in place of `primary_key`.** ClickHouse has no auto-increment
column and no unique constraint, so there is no surrogate key to fall back on
and nothing here pretends otherwise: `order_by` names the MergeTree *sorting*
key — how the table is laid out and indexed — and it does not deduplicate.
Naming none gets you a `received_at` of its own, sorted by that, which is the
honest analogue of postgres' `id`/`received_at` pair. Named columns are made
not-null, because ClickHouse will not sort by a nullable one. There is no
`indexes` field: the sorting key is the index.

**A batch is one insert.** Postgres executes a statement per message; ClickHouse
merges parts in the background and a row-at-a-time insert makes a part per row.
So a batch becomes one request — which makes an input `buffer` worth more here
than anywhere else, and `sensors_to_clickhouse` in the sample buffers 100
messages or 5 seconds ahead of its insert.

**It speaks the HTTP interface**, which is the port every deployment exposes
(and the only one ClickHouse Cloud has). The connection is a `url`, a
`database`, a `user` and a `password`; the database has to exist already, since
an output creates tables and never databases. A plaintext `http://` url needs
`"allow_http": true` on the connection — the credentials go with every insert —
which is exactly the rule the s3 connection follows, and what the local server
in `docker-compose.yaml` asks for.

Rows travel as `JSONCompactEachRow` rather than as text with a cast, which is
the same division of labour spelled the way this server spells it: a number's
own digits go across untouched, and the server parses a timestamp or a uuid.
`json` columns are created as `String` holding the JSON text — `JSONExtract`
reads them — and `date` as `Date32`, since `Date` starts in 1970.

`docker compose up` brings up ClickHouse on `:8123` with the database `kayak`
and the role `kayak` (password `hunter2`, which is what `${CLICKHOUSE_PASSWORD}`
resolves to in `example_config/secrets.example.json`).
