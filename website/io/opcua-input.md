# opcua input

An OPC UA server exposes an address space of *nodes*. The server can be a PLC,
a gateway or the front end of a historian. The `opcua` input subscribes to the
nodes that you name. It sends a message each time a value changes.

```jsonc
// config.connections.json — the server, once
{ "local-opcua": { "type": "opcua", "endpoint": "opc.tcp://localhost:50000" } }

// config.json — what this pipeline reads from it
{ "type": "opcua", "connection": "local-opcua",
  "nodes": [
    { "node_id": "ns=3;s=FastUInt1", "name": "line1_units" },
    { "node_id": "ns=3;s=SlowUInt1", "name": "tank_level" }
  ],
  "publish_interval_ms": 1000, "max_batch": 100 }
```

Each change is one message. The message carries the tag and the value:

```json
{
  "node": "ns=3;s=FastUInt1",
  "name": "line1_units",
  "value": 25,
  "status": "Good",
  "source_timestamp": "2026-01-01T12:00:00.123Z",
  "server_timestamp": "2026-01-01T12:00:00.130Z"
}
```

## the server pushes the changes {#the-server-pushes-kayak-does-not-poll}

The input uses an OPC UA subscription. The server samples each node and sends
only the values that changed. A tag that does not change costs nothing. Ten
thousand tags use one session.

`publish_interval_ms` sets how often the server can send changes. The default
is 1000. It is not a sample rate.

The server applies these three fields:

- **`sampling_interval_ms`**: how often the server reads each node. Without
  it, the server samples at the publish interval.
- **`queue_size`**: how many samples the server keeps for a node between two
  publishes. The default is 1. Thus a value that changes two times in one
  interval arrives one time, as the latest value. To get every sample, increase
  `queue_size` and decrease `sampling_interval_ms`.
- **`deadband`**: how much a value must change before the server reports it,
  in the units of the value. Without it, an analog signal reports on every
  sample. The deadband applies only to numeric nodes.

Use `deadband` to decrease the volume of an industrial stream. The server
applies it, so the data that it removes never crosses the network.

### performance

Set `max_batch` on this input. One publish carries every node that changed in
the interval. With the default of 1, 200 tags at 1 Hz make 200 batches each
second. With `max_batch: 200`, they can travel as one batch. The input does not
wait for a batch to fill, so a quiet plant still gives batches of one message.

## naming nodes, or browsing for them

`nodes` names each node in OPC UA notation:

- `ns=2;s=Name` for a string identifier;
- `ns=2;i=1042` for a numeric identifier;
- `g=<guid>` for a GUID;
- `b=<base64>` for an opaque identifier.

A node id without `ns=` is in namespace 0, which belongs to the server. The
optional `name` is the tag name in the messages. Without a `name`, the messages
carry the node id.

`browse` points at a node, usually a folder. The input subscribes to every
variable under it. Each tag gets the display name from the server.

```jsonc
{ "type": "opcua", "connection": "local-opcua",
  "browse": { "root": "ns=3;s=Anomaly", "depth": 2 }, "deadband": 1.0 }
```

- `depth` sets how many levels below the root the input follows. The default
  is 3. The value 0 is refused, and there is no value for "all levels". The
  address space of a plant server can have thousands of nodes.
- You can use `nodes` and `browse` together. You must give at least one of
  them. The input subscribes one time to a node that both find, with the name
  from `nodes`.
- The input browses when the pipeline starts. A tag that is added later
  arrives after the next restart. A tag that is removed stops without an error.
  Use `nodes` when the config file must say exactly what the pipeline reads.

## the tag is part of the message

Other inputs put their metadata in the optional
[envelope](/pipelines/message-metadata). The `opcua` input always puts `node`,
`name`, `value`, `status` and the two timestamps in the message. A value
without its node is not useful data. The envelope adds only the connection.

Thus the rest of the pipeline uses ordinary fields. To aggregate per tag, use
`group_by` on `name`:

```jsonc
{ "type": "reducer", "group_by": ["name"], "on_missing": "skip",
  "aggregations": [
    { "function": "avg", "field": "value", "as": "mean" },
    { "function": "max", "field": "source_timestamp", "as": "last_seen" },
    { "function": "count", "as": "readings" }
  ] }
```

**`status` is always present.** A failed instrument reports a status such as
`BadDeviceFailure` one time, with no value, and then sends nothing. The input
sends these readings with `value: null`. Use a `filter` to act on them. The
server usually omits a `Good` status, and the input then writes `"Good"`.

**Use `source_timestamp` to aggregate or partition.** It is the time when the
device produced the value. The `received_at` of the envelope is the time when
kayak read the value. On a slow link, or after a stall, the two times are
different.

## values

kayak converts numbers, booleans, strings, timestamps, node ids and arrays to
JSON. Byte strings become base64. A `Float` keeps its shortest decimal form: a
node that holds `0.1` gives `0.1`, not `0.10000000149011612`.

Some values have no JSON form, for example a structured type of the server or
a nested data value. The input skips such a reading and logs a warning.

## security

The session is **not encrypted** (`SecurityPolicy::None`). It signs in
anonymously, or with a username and a password from the connection:

```jsonc
{ "local-opcua": { "type": "opcua", "endpoint": "opc.tcp://localhost:50000",
                   "username": "kayak", "password": "${OPCUA_PASSWORD}" } }
```

Set both credential fields, or neither. Both accept a reference to the
[secret store](/io/secrets).

Use an OPC UA connection only on a network that you trust. kayak does not
support signed or encrypted sessions yet.

When it opens a session, the client logs two errors about a missing
*application instance certificate*. These errors are normal, because there is
no encryption. If readings arrive after the errors, the input works.

kayak dials the endpoint **directly**. It does not ask the server for its
endpoint list first. A server behind docker, NAT or a load balancer often
advertises a hostname that the client cannot resolve. kayak dials the endpoint
from the connections file.

## outages {#when-the-plant-goes-away}

- **Short outages:** the client retries the session without a limit. It then
  creates the subscription and the monitored items again. A short network
  failure causes a gap in the data and nothing more.
- **A session that cannot start:** the input reports an error one time for each
  outage and retries with backoff. The connect attempt has a timeout.
- **A session that ends permanently:** the input reports an error and retries
  with backoff.
- **A node that the server refuses:** the input logs it and continues with the
  other nodes.

There is no acknowledgement mode. An OPC UA subscription has no acknowledgement
that a client can hold back. Thus `ack: on_delivery` fails at build time.

## trying it

`docker compose up opcua` starts the OPC PLC simulator from Microsoft on
`opc.tcp://localhost:50000`. Its values change on their own. The sample graph
has three pipelines for it:

- `opcua_line1` subscribes to three nodes by name;
- `opcua_anomalies` browses the `Anomaly` folder with a deadband;
- `opcua_line1_10s_avg` reads from `opcua_line1` and aggregates each tag over
  10 s.
