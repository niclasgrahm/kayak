# the sample

`example_config/` is what to point the server at while working on it, and what
`just dev` uses:

| | |
| --- | --- |
| `config.json` | the worked example: every component kind, and the state buckets |
| `config.yaml` | the same graph, spelled as YAML |
| `config.connections.{json,yaml}` | the systems those pipelines name |
| `config.layout.json` | where the cards sit on the canvas |
| `secrets.example.json` | what the `${NAME}` references resolve against |
| `server.yaml` | the accounts `just dev` runs with |

One directory because the set travels together: the connections and layout files
are *derived* from the config's path, so they only find each other when they sit
side by side. `tests/config.rs` and `tests/graph.rs` load these files, so a
sample that stops parsing — or a component added to the JSON and not the YAML —
fails `just test` rather than rotting quietly.

`server.yaml` is the odd one out: it is not part of the graph and is not
derived from the config's path, because it describes *the server* rather than
the work (see [authentication](/operating/authentication)). It is here so that `just dev` runs
**with a login** — sign in as `niclas` / `hunter2` for an admin, or
`viewer` / `hunter2` to see what a read-only account gets. Developing against
the open path would leave the login page and the role checks as the one part of
the UI nobody ever looks at. `just dev-yaml` passes no `--server-config` and is
the way past the login when the login is not what is being worked on.

`ingest` is the http input's sample and needs nothing running either — it is a
root pipeline with no source, waiting to be posted to (see [posting into a
pipeline](/io/posting-into-a-pipeline)).

Three of them are the OPC UA sample and want `docker compose up opcua`, which is
Microsoft's PLC simulator: `opcua_line1` names three of its nodes and gives them
plant-ish names, `opcua_anomalies` browses a folder instead of naming anything
and puts a deadband on it, and `opcua_line1_10s_avg` reduces the first per tag
over ten seconds — which is a plain `group_by` on `name`, because an opcua
reading carries its tag in the message rather than behind the envelope (see
[opcua input](/io/opcua-input)).

Two of the roots are the SQL inputs, and both read what other pipelines in
the sample wrote, so they do real work under `docker compose up` and show a
round trip: `readings_from_postgres` follows the `readings` table that
`sensors_archive` fills, incrementally by `id` and starting from the newest
row, so it echoes each archived reading a few seconds after it lands;
`sensor_peaks_from_clickhouse` is the snapshot shape over a *query* — a
`GROUP BY` the server runs every thirty seconds over what `sensors_to_clickhouse`
inserted — which is the reference-data case, an aggregate polled rather than a
stream followed (see [database inputs](/io/database-inputs)).

## the four broken ones

`broken_cast`, `broken_aggregate`, `broken_webhook` and `broken_intermittently`
fail on purpose, and **they are meant to be red**. There is nowhere else to see
what a failing pipeline looks like: everything worth looking at in a card's
history — a failure signature with a tally climbing, throughput arriving and
nothing leaving, a chart with holes in it — only exists once something is
actually broken, and an example graph where everything works is an example of
exactly half the UI.

All four hang off `heartbeat`, which is a `dummy` ticking once a second, so they
fail at that rate whether or not `docker compose` is up, and they need no
service to be down in order to do it. Each breaks somewhere different on
purpose:

| | how it breaks |
| --- | --- |
| `broken_cast` | casts a timestamp to a number. A present value that will not convert is an error whatever `on_missing` says — see [casting](/pipelines/reshaping-messages#casting) |
| `broken_aggregate` | sums a field the heartbeat does not carry, with the reducer's default `on_missing: error` |
| `broken_webhook` | posts to a port nothing listens on. A long, ugly, real network error — the one that tests what a card does with a message too wide for it |
| `broken_intermittently` | the same bad cast behind a `value > 8` filter, so it only fails at the top of the heartbeat's sine wave: a burst of about twelve seconds in every sixty, and quiet in between |

The last one is the one to look at. A pipeline that fails *constantly* is a
solid block of red and tells you nothing about the shape of an outage; a chart
that goes wrong for twelve seconds a minute is what an intermittent fault
actually looks like, and it is the case the history feature exists to make
legible.

They are noisy by design: four error lines a second in the `just dev` console.
Deleting them from the config file is a fine thing to do while working on
something else — nothing else in the sample depends on them.

**Three of the roots attach metadata, and the choice of shape is the point.**
`sensors` and `kafka_events` use `merge`, so their downstream pipelines — which
filter and group on `value`, `sensor`, `ts` and `latency_ms` — carry on reading
exactly the fields they always did and gain a `_meta` beside them. `ingest` uses
`wrap`, because it is the one input whose payload isn't ours to assume: anything
can be posted to an endpoint, including a bare number, and `merge` has nowhere
to attach a field on one. Its payload lands under `body`.

`heartbeat` deliberately has **no** envelope — the default is worth seeing in
the sample too, and it is the pipeline whose output is written to disk and to a
bucket, where the un-enveloped shape is the easier one to read.

`sensors_10s_avg` then groups by `["sensor", "_meta.subject"]`, which is the
whole in-band argument in one line: the reducer needs nothing new to reach the
subject, and the grouped path comes out as `subject`. It is paired with the
`sensors` envelope — a test fails if that envelope is ever dropped, since
`on_missing: skip` would leave that reducer silently emitting nothing.

`heartbeat_peaks` is the state sample, and it hangs off `heartbeat` for the
reason `heartbeat_to_disk` does — it fills a bucket on a bare `just dev` with
nothing else running. It remembers the last heartbeat above 8 and stamps every
message with it, which is `when`, `remember` and `recall` in one four-line
chain; `on_missing: null` is what makes the messages before the first peak
readable rather than dropped. Its bucket is `max_keys: 1` because the pipeline
declares no `key` — one bucket-wide value, shown in the card as "the whole
bucket". `sensor_state` beside it is the keyed shape, one entry per sensor, and
needs `docker compose up`.

**All three script pipelines hang off `heartbeat`** for the reason
`heartbeat_to_disk` does, and between them they cover both sources and both
scopes. `heartbeat_banded` is the inline one and is deliberately two lines: a
conditional writing a band onto the message, which is the smallest thing `map`
cannot express at all. `heartbeat_swings` is the file-sourced one at
`scripts/swings.rhai` and is the script-plus-state sample — it recalls the
previous reading, remembers this one, and emits the direction and the delta,
which is the comparison `remember`/`recall` have no spelling for. Its bucket is
`max_keys: 1` for the reason `heartbeat_peaks`' is.

`heartbeat_extremes` is the `batch` scope one, and the `buffer` on its input is
the point rather than a detail: without one the heartbeat arrives a message at a
time and every batch would hold a single reading, which is what makes batch
scope look pointless. It emits `spread` alongside `lowest` and `highest` —
arithmetic *between* two aggregates, which a reducer cannot do.

The two file-sourced scripts also share a module: `scripts/shared/readings.rhai`
holds the classification both use, `import`ed by a path relative to the config's
directory — the [shared-code sample](/pipelines/scripting#sharing-code-between-scripts),
and the shape a project grows into once two scripts want the same helper.

Note the sample is JSON, which is the format inline scripts read worst in: the
inline one is a single escaped `\n` away from being unreadable, and that is a
fair advertisement for keeping scripts in files or writing the config in YAML.
`config.yaml` beside it renders the same script as a literal block.

`heartbeat_to_disk` is the file output's sample, and it hangs off `heartbeat`
rather than off the nats source on purpose: the dummy input needs nothing
running, so it is the one pipeline in here that writes real output on a bare
`just dev` with no `docker compose up`. Twenty messages a part at one a second,
so you see it rotate while you watch. `heartbeat` emits its numeric payload — a
sine wave, ±10 over a minute — so what lands on disk has a shape rather than
being the same message a thousand times; `payload: text` swaps it for random
sentences. It is also why `just dev` and
`tests/graph.rs` both pass `--data-dir dev_data`, and why the sample can't be
run out of the container image without the same flag — without it that pipeline
refuses to build and takes the whole load down with it, which is the closed
default working as intended.

`heartbeat_to_s3` is its object-store twin, off the same `heartbeat` and with the
same rotation, so the two can be watched side by side — one directory filling up,
one bucket. Unlike its twin it does need `docker compose up`, for the rustfs it
writes to.

**Both `keep` shapes of `map` are in there on purpose too.**
`heartbeat_shaped` hangs off `heartbeat` for the reason `heartbeat_to_disk`
does — it is the map sample that reshapes real messages on a bare `just dev` —
and it uses `keep: all`, so the heartbeat's own fields are still there under the
ones it adds. It is also the worked two-step arithmetic: `value` scaled and
offset through a `_scaled` the last mapping drops again, plus a `concat` reading
the `line` a `constant` wrote two mappings earlier. `sensors_projected` is the
other shape — `keep: mapped` and `on_missing: omit`, promoting `_meta.subject`
and coalescing two spellings of the reading into four fields and nothing else.

**Both spellings of the postgres output are in there on purpose.**
`hot_readings` maps columns — a nullable and a not-null one, a `timestamp` read
from `ts`, a `text` read from `_meta.subject`, an audit column holding the whole
message, and an index — while `sensors_archive` maps none and so still writes
the single-`payload` table, which is the compatibility promise being exercised
rather than described. `sensor_sums` maps the reducer's three answers, which is
the case column mapping is really for: a rollup whose columns are the query.
Note that the mapped tables are created from the config, so a database that
already holds a `hot_readings` from before this landed has to have it dropped —
creation is `IF NOT EXISTS` and never alters.
