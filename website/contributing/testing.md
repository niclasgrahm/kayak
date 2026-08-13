# testing

`just ci` (= `just lint` + `just test`) is what has to be green before pushing;
GitHub Actions runs the same two commands. Everything runs offline — no NATS, no
running server, no ports.

The runtime lives in `src/lib.rs`, not in `main.rs`, purely so the tests in
`tests/` can reach it. `main.rs` is only argument parsing plus the Leptos wiring.

Five layers, cheapest first:

| where | what it covers |
| --- | --- |
| `src/transforms/*.rs` `#[cfg(test)]` | pure per-transform logic: what's kept, dropped, buffered, split |
| `tests/config.rs` | the JSON wire format of every component kind |
| `tests/pipeline.rs` | the run loop: transform chaining, error tolerance, fan-out, cancellation, UI events |
| `tests/graph.rs` | `AppState`: ids, upstream wiring, lifecycle |
| `tests/api.rs` | the HTTP surface and its status codes, via `tower::oneshot` |
| `tests/persist.rs` | the config file: that editing *doesn't* touch it, that saving does, that a save can't escape its directory, and that reverting reloads it |
| `hurl/tests/*.hurl` | one smoke test against a really running server (`just test-http`) |

Two things to know when adding a component:

- `tests/config.rs::every_component_kind_has_a_wire_format_sample` reads the
  variants straight out of the generated JSON schema and fails until you add a
  sample for the new one. That's deliberate — it's the guard rail that keeps the
  wire format covered as the component list grows.
- `src/testing.rs` has the test doubles: `ScriptedInput`, `CollectingOutput`,
  `FailOnNth`, and `PipelineRuntime::from_parts` to assemble a pipeline without
  going through a config. Prefer these over touching the network in a test.

Timing-dependent tests use `#[tokio::test(start_paused = true)]` so a 10-second
window costs no wall time.

The `http` output is the exception to that rule and worth knowing about before
adding another: `outputs::http::tests` stands a real axum endpoint on a
loopback port and asserts what went over the wire — the batch/message shapes,
the verb, the token on every request, a non-2xx failing the batch, and the gate
letting exactly one of five failing batches reach the network. Nothing outside
the process is touched, so it runs offline like everything else; the client is
`reqwest` either way, and there is no equivalent of standing up kafka.

Not covered by `just test`, and deliberately so: the NATS and kafka
input/outputs, the HTTP transform, the database outputs' round trip and the
upload half of the s3 output, which are thin wrappers over their clients — they need
`docker compose up` and are exercised by
`just start-baseline` / `just test-http`. For s3 that means the `PUT` itself is
untested offline; what *is* tested is everything that decides *what* is uploaded
— rotation, part naming and encoding in `outputs::rotate::tests`, shared verbatim
with the file output and covered end-to-end there against a real directory — plus
every build-time refusal in `outputs::s3::tests` (no rotation trigger, plaintext
endpoint without `allow_http`, a connection of the wrong kind), which are the
rules with a decision in them. What *is* tested offline for postgres is
everything with a decision in it, which is now most of the output: `Table::parse`
and `Identifier::parse`, which validate every name that reaches the SQL text —
neither a table nor a column can be a bind parameter, so those checks are the
only thing standing between `config.json` and an arbitrary statement — the
statements built from a mapping (`outputs::postgres::tests`), and the mapping
itself in `outputs::columns::tests`: which value is accepted into which type,
what a missing field does, and every build-time refusal. What needs a server is
the round trip, which is the part with no decision in it.

`outputs::clickhouse::tests` is the same list one item longer, because that
output writes its own wire format rather than handing values to a driver: the
DDL and the insert, the sorting key's effect on both, the build-time refusals
(including the plaintext url), and — the one that is really about the *pair* of
modules — that every column type comes out of the mapping as text the row
builder turns into a parseable JSON line.

`docker compose up` also brings up a single-node kafka (KRaft, no zookeeper) on
:9092 with a publisher putting one JSON line a second on `test.events`, which
the `kafka_events` pipeline consumes and `slow_requests` filters back out to
`test.slow`. The broker advertises two listeners — `localhost:9092` for the
server running on the host, `kafka:29092` for the other containers — because
they can't both reach it by the same name.

Two things worth knowing when playing with the kafka input. It joins a consumer
group, so **two servers running the same config share the topic**: with a
one-partition topic only one of them gets an assignment and the other looks
broken. And leaving a group takes a session timeout to notice, so after killing
a server the next one can sit idle for ~45s before kafka rebalances the
partition onto it. Both of those cost me a confusing ten minutes; they are kafka
working as designed, not the pipeline being wrong.

`docker compose up` also brings up postgres on :5432 and ClickHouse on :8123
(both database `kayak`, role `kayak`, password `hunter2`), which is where
`sensors_archive` and `sensors_to_clickhouse` in `config.json` write. Those
pipelines name the `local-postgres` and `local-clickhouse` connections in
`config.connections.json`, whose passwords are `${POSTGRES_PASSWORD}` and
`${CLICKHOUSE_PASSWORD}` references, so running the server against the sample
config needs secrets:

```bash
just dev
```

That is the whole of it: `just dev` creates `example_config/secrets.json` from
`secrets.example.json` if it isn't there, and **tops up any keys it is missing**
if it is, then runs `cargo leptos watch` against the sample. The top-up is what
keeps a checkout working when a new component adds a secret to the sample —
otherwise a file created months ago fails the next `just dev` with an unresolved
`${NAME}`. Values already in your file are never overwritten, since one of them
may be a real credential. `just dev-yaml` is the same graph in its
other spelling.
