# connections

A connection declares an external system one time, under a name. Many
components can then refer to that name. Put the connections in a second file
beside the config file. See [the config file](/pipelines/the-config-file) for
the basics of the two files.

```json
// config.connections.json
{
  "prod-kafka": { "type": "kafka", "brokers": "${KAFKA_BROKERS}" },
  "local-nats": { "type": "nats", "urls": "nats://localhost:4222" }
}
```

```json
// config.json
{ "type": "kafka", "connection": "prod-kafka", "topic": "orders", "group": "kayak" }
```

## what goes where

- The connection holds what the system needs: brokers, urls, hosts and
  credentials.
- The component holds what one pipeline wants from the system: a topic, a
  consumer group, a subject or a table.

There is no inline form. A component must name a connection, or it does not
build.

One connection kind serves both directions. For example, a kafka input and a
kafka output can use the same `kafka` connection.

kayak checks the kind as well as the name. A nats connection in a kafka input
fails at build time, and the error gives the actual kind. An unknown name also
fails, and the error lists the names that exist.

## where the file is

- With `--connections <path>`, kayak reads that file. The file must exist. Use
  this flag to share one connections file between two configs.
- Without the flag, kayak derives the name and the format from the config file.
  `config.json` gives `config.connections.json`. `pipelines.yaml` gives
  `pipelines.connections.yaml`. If the derived file does not exist, the graph
  has no connections.

## when a change takes effect

kayak reads a connection when it builds a component. A change to a connection
reaches only the pipelines that kayak builds or rebuilds after the change.
Running pipelines keep the old settings.

kayak does not pool clients. Two pipelines on one connection each get their own
client, with the same settings.

You can also change connections through the HTTP API:

- `POST /api/connections` adds a connection.
- `DELETE /api/connections/{connection_id}` removes a connection. If a running
  pipeline names it, the server refuses with a 409 and lists the pipelines.
  Delete those pipelines first.
- `POST /api/config/save` writes the config file and the connections file
  together. A change through the API is not on disk until you save.
- `POST /api/config/revert` reloads both files. It loads the connections first,
  because the pipelines refer to them.

The web UI also has a connections tab in the sidebar.

## credentials

Do not write a credential into the connections file. Write a `${NAME}`
reference, and supply the value from the environment or from a secrets file.
See [secrets](/io/secrets).
