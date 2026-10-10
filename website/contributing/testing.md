# testing

`just ci` must be green before you push. It runs `just lint` and `just test`.
GitHub Actions runs the same two commands. All of these tests run offline: no
NATS, no server, no ports.

The runtime is in `src/lib.rs`, not in `main.rs`, so that the tests in `tests/`
can reach it. `main.rs` contains only the argument parser and the Leptos
wiring.

## the layers

The layers are in sequence from the cheapest to the most expensive:

| where | what it covers |
| --- | --- |
| `src/transforms/*.rs` `#[cfg(test)]` | the logic of each transform: what it keeps, drops, buffers or splits |
| `tests/config.rs` | the JSON wire format of every component kind |
| `tests/pipeline.rs` | the run loop: the transform chain, error tolerance, fan-out, cancellation, UI events |
| `tests/graph.rs` | `AppState`: ids, upstream wiring, lifecycle |
| `tests/api.rs` | the HTTP surface and its status codes, through `tower::oneshot` |
| `tests/persist.rs` | the config file: an edit does not write it, a save does, a save cannot leave its directory, and a revert loads it again |
| `tests/live_sql.rs` | the sql inputs against a real postgres and clickhouse (`just test-live`, not in `just ci`) |
| `hurl/tests/*.hurl` | one smoke test against a server that runs (`just test-http`) |

## when you add a component

- `tests/config.rs::every_component_kind_has_a_wire_format_sample` reads the
  variants from the generated JSON schema. It fails until you add a sample for
  the new kind. Thus the wire format of every kind has a test.
- `src/testing.rs` has the test doubles: `ScriptedInput`, `CollectingOutput`,
  `FailOnNth`, `MapSecretStore`, and `PipelineRuntime::from_parts` to assemble a
  pipeline without a config. Use these doubles. Do not use the network in a
  test.

Use `#[tokio::test(start_paused = true)]` for a test that depends on time. Then
a window of 10 s costs no wall time.

## tests against a local endpoint

The `http` output is an exception to the rule above. `outputs::http::tests`
starts a real axum endpoint on a loopback port. The tests check what goes over
the wire:

- the shapes `batch` and `message`;
- the verb;
- the token on every request;
- a non-2xx status that fails the batch;
- the gate, which lets one of five failing batches reach the network.

Nothing outside the process is touched, so these tests also run offline.
`reqwest` is the client in both cases. kafka has no equivalent.

## what `just test` does not cover

These components are thin wrappers over their clients:

- the NATS and kafka inputs and outputs;
- the `http` transform;
- the round trip of the database outputs;
- the upload of the s3 output.

They need `docker compose up`. `just start-baseline` and `just test-http` use
them.

For s3, no offline test does the `PUT`. The offline tests cover the decisions
about what kayak uploads:

- `outputs::rotate::tests` covers rotation, part names and encoding. The file
  output uses the same code, and its tests run against a real directory.
- `outputs::s3::tests` covers every build-time refusal: no rotation trigger, a
  plaintext endpoint without `allow_http`, and a connection of the wrong kind.

For postgres, the offline tests cover most of the output:

- `Table::parse` and `Identifier::parse` check every name in the SQL text. A
  table name or a column name cannot be a bind parameter. Thus these checks are
  the only protection against an arbitrary statement from `config.json`.
- `outputs::postgres::tests` covers the statements that kayak builds from a
  mapping.
- `outputs::columns::tests` covers the mapping: which value each type accepts,
  what a missing field does, and every build-time refusal.

Only the round trip needs a server, and it has no decision in it.

`outputs::clickhouse::tests` covers the same items and one more. This output
writes its own wire format and does not give values to a driver. The tests
cover these items:

- the DDL and the insert;
- the effect of the sorting key on both;
- the build-time refusals, including a plaintext url;
- that every column type gives text that the row builder turns into a valid
  JSON line. This test checks the two modules together.

## the compose stack

`docker compose up` starts the services that the sample config uses.

**kafka.** A single-node kafka (KRaft, no zookeeper) listens on :9092. A
publisher puts one JSON line per second on `test.events`. The `kafka_events`
pipeline reads it, and `slow_requests` filters it back out to `test.slow`. The
broker advertises two listeners. `localhost:9092` is for the server on the
host, and `kafka:29092` is for the other containers.

Two kafka behaviors cause confusion:

- **Two servers with the same config share the topic.** The kafka input joins a
  consumer group. With a topic of one partition, only one server gets an
  assignment. The other server shows no messages.
- **A new server can wait about 45 s.** kafka notices that a member left the
  group only after a session timeout. Until then, it does not move the
  partition to the new server.

This is the normal behavior of kafka.

**postgres and ClickHouse.** postgres listens on :5432 and ClickHouse on :8123.
Both have the database `kayak`, the role `kayak` and the password `hunter2`.
`sensors_archive` and `sensors_to_clickhouse` in `config.json` write to them.
They use the connections `local-postgres` and `local-clickhouse` in
`config.connections.json`. The passwords there are `${POSTGRES_PASSWORD}` and
`${CLICKHOUSE_PASSWORD}`, so the server needs secrets:

```bash
just dev
```

`just dev` creates `example_config/secrets.json` from `secrets.example.json`
when the file is not there. When the file is there, `just dev` adds the keys
that are missing. Thus a checkout continues to work when a new component adds a
secret to the sample. `just dev` never changes a value that is in the file,
because a value can be a real credential. Then it runs `cargo leptos watch`
against the sample.

## the dev recipes

| recipe | what it starts |
| --- | --- |
| `just dev` | the sample graph from `config.json`, with a login (`example_config/server.yaml`) |
| `just dev-yaml` | the same graph from `config.yaml`, without a login |
| `just dev-bare` | an empty graph without a login, with every connection of the sample |
| `just dev-blank` | an empty graph with no config and no connections |

Use `just dev-bare` to build a pipeline in the UI against real systems. Start
the services with `docker compose up` first. `dev-bare` gives `--connections`
explicitly, because there is no config file to derive the path from. It loads
the same connections file as `just dev`.
