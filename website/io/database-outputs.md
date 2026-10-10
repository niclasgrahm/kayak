# database outputs and column mapping

The `postgres` and `clickhouse` outputs insert messages into a table. Both use
the same `columns` list to map message fields to typed columns.

## postgres

The `postgres` output writes one row for each message.

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

Without `columns`, the table has three columns: an `id`, a `received_at` and a
`jsonb` `payload` with the whole message.

The `table` name can include a schema (`analytics.readings`). It can contain
only letters, digits and underscores.

The output prepares one insert statement for each batch and runs it for each
message. There is no transaction around the batch. If a row fails, the rows
before it stay in the table, and the batch fails.

## column mapping

Each entry in `columns` maps one message field to one column:

- **`name`**: the column name. Letters, digits and underscores only.
- **`type`**: the logical type of the column. See below.
- **`field`**: the [field path](/pipelines/message-metadata#field-paths) to
  read. The default is the column name. A path such as `_meta.subject` reads
  the metadata that the input envelope added.
- **`message`**: set to `true` to store the whole message in this column. Only
  a `json` column can do this, and not together with `field`. Use it for an
  audit column.
- **`nullable`**: the default is `true`. With `false`, the column is `NOT NULL`
  and a missing field is an error.
- **`on_missing`**: what to do when the message does not have the field. See
  below.

### types

The types are logical. Each output converts them to the types of its server:

`text`, `integer`, `bigint`, `float`, `decimal`, `boolean`, `timestamp`,
`date`, `uuid`, `json`.

Thus you can move a config from one database output to another without a
change to the columns.

### values are checked, not converted

The output checks each value against the column type. It does not convert
values. For example, the string `"12.5"` in a `float` column fails the batch.

- `integer` is 32-bit. A number with a fraction, or a number out of range, is
  an error.
- `decimal` keeps the digits of the message. It does not go through a float.
- `timestamp` takes a string that the server parses (RFC 3339), or a number of
  **seconds** since the epoch.
- `date` takes a string such as `2026-08-10`.

The postgres output sends each value as text and casts it in the statement
(`$2::text::NUMERIC`). The server parses timestamps and uuids, and it reports
malformed values.

### missing and extra fields

- **`on_missing`** has three values. `null` writes `NULL` and is the default.
  `error` fails the batch. `skip_row` leaves the whole message out.
- A column with `"nullable": false` uses `error` as the default. If you set
  `on_missing: null` on it, the pipeline fails at build time.
- A field that is present with the value `null` counts as missing.
- **`on_extra_fields`**: `ignore` (the default) writes the mapped columns and
  ignores other fields. `error` fails the batch when a message has a field that
  no column reads. Use `error` for a stream with a fixed shape.

### creating the table

- **`create_table`**: the default is `true`. The output runs `CREATE TABLE IF
  NOT EXISTS` when it connects. Set it to `false` for a table that another
  system owns.
- The output never alters a table. If the table has a different shape, the
  insert fails with the error from the server.
- **`primary_key`**: the columns of the primary key. Without it, the created
  table gets its own `id` and `received_at`. With it, the output drops those two
  columns and makes the key columns `NOT NULL`.
- **`indexes`**: indexes to create with the table. Each index lists mapped
  columns in order and can set `"unique": true`. kayak names each index after
  the table and its columns.

A `primary_key` or an index that names a column that is not mapped fails the
build. kayak refuses all such conflicts at build time.

## clickhouse

The `clickhouse` output uses the same `columns` list. To move a config from
postgres to clickhouse, change the `type` and the `connection`:

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

Without `columns`, the table has a `received_at` column and a `payload` column
with each message as JSON text.

Differences from postgres:

- **`order_by` replaces `primary_key`.** It sets the MergeTree sorting key,
  which is also the index of the table. It does not remove duplicates. Without
  `order_by`, the table gets a `received_at` column and sorts by it. kayak
  makes the `order_by` columns `NOT NULL`, because ClickHouse cannot sort by a
  nullable column. There is no `indexes` field.
- **The table name** can include a database (`analytics.readings`). This
  overrides the database of the connection.
- **`json` columns** are `String` columns that hold the JSON text. Use
  `JSONExtract` to read them.
- **`date` columns** are `Date32`.

### performance

**The output sends one insert for each batch.** ClickHouse makes a new part
for each insert, so small inserts are slow. Put a `buffer` on the input:

```jsonc
{ "type": "nats", "connection": "local-nats", "subject": "sensors",
  "buffer": { "type": "batch", "size": 100, "window_seconds": 5 } }
```

`sensors_to_clickhouse` in the sample buffers 100 messages or 5 s before each
insert.

### the connection

The output uses the HTTP interface of ClickHouse. This is the port that all
deployments expose, including ClickHouse Cloud.

- The connection has a `url`, a `database`, a `user` and a `password`.
- The database must exist. The output creates tables. It does not create
  databases.
- kayak refuses a plain `http://` url, because the credentials go with every
  insert. Set `"allow_http": true` on the connection to permit it. The local
  server in `docker-compose.yaml` needs this.

The output sends rows as `JSONCompactEachRow`. Numbers keep their digits, and
the server parses timestamps and uuids.

`create_table: false` still fails at startup if the server is not reachable.
The output checks the connection when it starts.

## trying it

`docker compose up` starts ClickHouse on `:8123` with the database `kayak` and
the role `kayak`. The password is `hunter2`. That is the value of
`${CLICKHOUSE_PASSWORD}` in `example_config/secrets.example.json`.
