# the sample

The directory `example_config/` holds a sample graph. It uses every component
kind and every connection kind. `just dev` runs the server against it.

| file | content |
| --- | --- |
| `config.json` | the pipelines and the state buckets |
| `config.yaml` | the same graph in YAML |
| `config.connections.{json,yaml}` | the systems that the pipelines name |
| `config.layout.json` | the positions of the cards in the web UI |
| `secrets.example.json` | the values for the `${NAME}` references |
| `server.yaml` | the accounts for `just dev` |
| `scripts/` | the rhai files that the script pipelines use |

Keep the files in one directory. kayak finds the connections file and the layout
file from the path of the config file. See [the config file](/pipelines/the-config-file).

`just dev` makes `secrets.json` from `secrets.example.json` if it does not
exist. It also passes `--data-dir dev_data`. Without that flag, the file output
of `heartbeat_to_disk` does not build, and the server does not start. To run the
sample from the container image, give the same flag. See
[deployment](/operating/deployment).

## what the sample needs

- **Without other services.** The pipelines below `heartbeat` and `ingest` need
  nothing else. `heartbeat` is a `dummy` input that sends a sine wave (±10, one
  period per minute) one time per second.
- **With `docker compose up`.** The pipelines that read from or write to nats,
  kafka, mqtt, redis, OPC UA, postgres, clickhouse and s3 need the services in
  `docker-compose.yaml`.
- **Tidepool.** `sensors_to_tidepool` waits in its start-up until a Tidepool
  server runs on port 7070.

`server.yaml` turns on authentication. Sign in as `niclas` (admin) or `viewer`
(read only). The password for both is `hunter2`, from `secrets.example.json`.
`just dev-yaml` loads `config.yaml` without `--server-config`, so it has no
sign-in. See [authentication](/operating/authentication).

## the roots

| pipeline | input | notes |
| --- | --- | --- |
| `heartbeat` | `dummy` | no envelope; it feeds most of the samples below |
| `ingest` | `http` | `wrap` envelope, payload under `body`; see [posting into a pipeline](/io/posting-into-a-pipeline) |
| `sensors` | `nats` | `merge` envelope; it feeds the sensor samples |
| `kafka_events`, `mqtt_events`, `redis_events` | `kafka`, `mqtt`, `redis` | `merge` envelope |
| `opcua_line1` | `opcua` | three named nodes, then a `pivot` |
| `opcua_anomalies` | `opcua` | browses a folder, with a deadband on the input |
| `readings_from_postgres` | `postgres` | incremental by `id`, from the newest row |
| `sensor_peaks_from_clickhouse` | `clickhouse` | a snapshot of a `GROUP BY` query, every 30 s |
| `components_from_api` | `http_poll` | the component reference of the same server, every 5 minutes |

`ingest` uses `wrap` because a client can post any JSON value to it, also a bare
number. `merge` cannot add a field to a value that is not an object.

The two SQL inputs read what other sample pipelines write.
`readings_from_postgres` reads the `readings` table that `sensors_archive`
writes. `sensor_peaks_from_clickhouse` reads the table that
`sensors_to_clickhouse` writes. See [database inputs](/io/database-inputs).

`components_from_api` fetches `/api/docs`, which needs no sign-in. A `map` with
`keep: mapped` keeps `kind`, `family` and `polled_at`. See
[polling an api](/io/polling-an-api).

`opcua_line1_10s_avg` groups the OPC UA readings by `name`. An `opcua` reading
contains its tag in the message, not in the envelope. See
[opcua input](/io/opcua-input).

## the graph

- `everything` has three inputs: the `sensors` pipeline, the `heartbeat`
  pipeline, and the nats subject that `sensors_100_max` publishes to. It has two
  outputs: stdout and nats.
- `heartbeat_pairs` uses the `buffer` transform and the `splitter` transform.
- `slow_requests` filters `kafka_events` and writes to kafka and stdout.
- `hot_alerts` is at depth 3: `sensors` → `hot_readings` → `hot_alerts`.

## metadata

- `sensors_10s_avg` groups by `["sensor", "_meta.subject"]`. The reducer writes
  the grouped path as `subject`. This needs the `merge` envelope on `sensors`.
- `heartbeat` has no envelope. Its output in the file and in the s3 bucket is
  the plain message.

## state

| pipeline | bucket | what it shows |
| --- | --- | --- |
| `heartbeat_peaks` | `heartbeat_peaks` | `remember` with `when` (value above 8), then `recall` with `on_missing: null` |
| `sensor_state` | `sensor_state` | a keyed bucket, one entry for each sensor |
| `heartbeat_swings` | `heartbeat_swings` | a script with `remember` and `recall` |
| `opcua_line1` | `line1_tags` | `pivot` |
| `heartbeat_trend`, `heartbeat_grid` | `heartbeat_stats` | the streaming statistics |

`heartbeat_peaks`, `heartbeat_swings` and `line1_tags` have `max_keys: 1`. Their
pipelines declare no `key`, so each bucket holds one value. `heartbeat_stats`
has `max_keys: 8`, because each transform keeps its own state under the key.

## map

- `heartbeat_shaped` uses `keep: all`. It has a `constant`, a `concat` that reads
  the constant, and a calculation in two steps through `_scaled`. The last
  mapping drops `_scaled`.
- `sensors_projected` uses `keep: mapped` and `on_missing: omit`. It writes four
  fields: it copies `_meta.subject`, and it coalesces two spellings of the
  reading.

## scripts

| pipeline | source | scope |
| --- | --- | --- |
| `heartbeat_banded` | inline | message |
| `heartbeat_swings` | `scripts/swings.rhai` | message |
| `heartbeat_extremes` | `scripts/extremes.rhai` | batch |

- `heartbeat_banded` writes a band onto the message with a condition. `map`
  cannot do this.
- `heartbeat_swings` recalls the previous reading, remembers the current one,
  and sends the direction and the change.
- `heartbeat_extremes` has a 10 s `tumbling` buffer on its input. Without it,
  each batch holds one message. It sends `lowest`, `highest` and the `spread`
  between them.
- The two file scripts import `scripts/shared/readings.rhai`. See
  [sharing code between scripts](/pipelines/scripting#sharing-code-between-scripts).

The inline script in `config.json` is hard to read, because JSON escapes the
newlines. `config.yaml` shows the same script as a block.

## streaming statistics

- `heartbeat_trend` adds values to each message: `smooth`, `derive`, `rolling`
  and `detect`. A `throttle` at the end passes one message every 10 s.
- `heartbeat_grid` has a `deadband` (delta 2, confirmation every 15 s) and a
  `resample` with `forward_fill` on a 5 s grid. Some grid points come from the
  clock, because the deadband holds readings back.

See [streaming statistics](/pipelines/streaming-statistics).

`heartbeat_features` is the [model round trip](/pipelines/model-round-trip). It
has a 10 s buffer, a `features` transform with a sample rate of 1 Hz, and an
`http` transform. The `http` transform posts to the `ingest` endpoint and
merges the reply `{"accepted": 1}` under `ingest`.

## outputs

| pipeline | output | needs |
| --- | --- | --- |
| `heartbeat_to_disk` | `file`, ndjson, a new part every 20 rows or 60 s | `--data-dir` |
| `heartbeat_to_s3` | `s3`, the same rotation | rustfs |
| `heartbeat_to_redis` | `redis` | redis |
| `heartbeat_to_webhook` | `http`, to the `ingest` endpoint | nothing |
| `sensors_archive` | `postgres` without `columns`, and stdout | postgres |
| `hot_readings` | `postgres` with `columns` and an index | postgres |
| `sensors_60s_sum` | `postgres` with `columns`, table `sensor_sums` | postgres |
| `sensors_to_clickhouse` | `clickhouse` | clickhouse |
| `sensors_to_tidepool` | `tidepool` | Tidepool on port 7070 |

`heartbeat_to_webhook` uses the port 6767 from `Cargo.toml`. If you change the
port, change the URL too.

`sensors_archive` has no `columns`, so it writes the default table with one
`payload` column. `hot_readings` maps columns: a `timestamp` from `ts`, a
`text` from `_meta.subject`, a `json` column with the whole message, and an
index. `sensors_60s_sum` maps the three results of its reducer.

kayak makes a mapped table with `IF NOT EXISTS`. It does not change a table that
exists. If your database has an old `hot_readings` table with a different shape,
drop it.

## the four broken ones

Four pipelines contain errors. They show how a failure looks in the history
and on a card. All four read from `heartbeat`, so they fail one time per second
without other services.

| pipeline | how it breaks |
| --- | --- |
| `broken_cast` | casts a timestamp string to a number. A present value that does not convert is an error. See [casting](/pipelines/reshaping-messages#casting) |
| `broken_aggregate` | sums a field that the heartbeat does not contain, with the default `on_missing: error` |
| `broken_webhook` | posts to a port where nothing listens. This gives a long network error |
| `broken_intermittently` | the same bad cast after a `value > 8` filter. It fails for about 12 s in every 60 s |

`broken_intermittently` fails in bursts, which is the shape of a real outage.

The four pipelines write four error lines per second to the console of
`just dev`. No other sample pipeline depends on them. You can remove them from
your copy of the file.
