# opcua input

The industrial one. An OPC UA server — a PLC, a gateway, a historian's
front end — exposes an address space of *nodes*, and this input subscribes to
the ones you name, taking a message every time a value changes.

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

Each change is one message, and it carries the tag as well as the value:

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

## the server pushes; kayak does not poll

An OPC UA subscription is the protocol's own streaming shape: the server samples
each monitored node at its end and sends what changed. A tag that hasn't moved
costs nothing, ten thousand tags are one session, and `publish_interval_ms` is
how often the server *may send* rather than how often anything is measured.

Three knobs shape what arrives, and all three are applied by the server:

- **`sampling_interval_ms`** — how often it looks at each node. Left out, it
  samples at the publishing interval, which is what a server does when nobody
  asks.
- **`queue_size`** — how many samples it may hold for a node between publishes.
  The default of 1 means a value that moves twice in one interval is reported
  once, as the latest. Raise it, with a shorter sampling interval, when every
  sample matters rather than the current value.
- **`deadband`** — how far a value must move before it is worth reporting, in
  the value's own units. Without one an analogue signal reports on every sample,
  because the last digit is always moving. This is the cheapest thing there is
  to cut the volume of an industrial stream, and it happens before the data
  reaches the network.

`max_batch` matters more here than on other inputs. One publish carries every
node that changed in the interval, so two hundred tags at 1 Hz is two hundred
batches a second through the run loop unless they are allowed to travel
together. As everywhere else, raising it only coalesces changes that had
*already* arrived — a quiet plant still yields batches of one.

## naming nodes, or browsing for them

`nodes` names them one at a time, in OPC UA's own notation — `ns=2;s=Name`,
`ns=2;i=1042`, `g=<guid>`, `b=<base64>`, with the `ns=` optional in the server's
own namespace 0. The optional `name` is what messages call the tag; without one
they carry the node id, which is exact and unreadable.

`browse` points at a node — usually a folder — and subscribes to every variable
underneath it, naming each after the server's own display name:

```jsonc
{ "type": "opcua", "connection": "local-opcua",
  "browse": { "root": "ns=3;s=Anomaly", "depth": 2 }, "deadband": 1.0 }
```

The two combine, and a node reached both ways is subscribed to once, keeping the
name the config gave it. What browsing costs is that what the pipeline reads is
decided by the address space **at the moment it starts**: a tag added tomorrow
arrives with the next restart, and one removed silently stops. An explicit list
is the one that says in the file exactly what is being read.

`depth` defaults to 3, and there is deliberately no spelling for "all of them" —
a browse of a plant server's whole address space is thousands of nodes, and the
pipeline that asked for it would find out by subscribing to all of them.

## the tag is part of the message

Every other input puts what it knows about a message behind the opt-in
[envelope](/pipelines/message-metadata), because the message means something
without it. A reading does not: `21.5` with no node and no name is not data. So
`node`, `name`, `value`, `status` and the timestamps are always on the message
itself, and the envelope adds only which connection it came through.

That is what makes the rest of a pipeline ordinary. Grouping per tag is a
`group_by` on a field like any other:

```jsonc
{ "type": "reducer", "group_by": ["name"], "on_missing": "skip",
  "aggregations": [
    { "function": "avg", "field": "value", "as": "mean" },
    { "function": "max", "field": "source_timestamp", "as": "last_seen" },
    { "function": "count", "as": "readings" }
  ] }
```

**`status` is always present, and that is the point.** A failed instrument does
not go quiet — it reports `BadDeviceFailure` with no value, once, and then
nothing. Those readings are passed on with `value: null` rather than dropped, so
a `filter` can act on them; dropping them would make a broken sensor look like a
steady one. A `Good` status is normally left off the wire entirely, and an
absent one is read as `Good`.

**`source_timestamp` is when the device says the value was produced**, which is
the one to reduce or partition by. The envelope's `received_at` is when kayak
read it; on a slow link those are not the same instant, and on a link that
stalled and caught up they are not even close.

## values

Numbers, booleans, strings, byte strings (base64), timestamps, node ids and
arrays all become the JSON you would expect. A `Float` keeps the digits it was
sent with — a node holding `0.1` says `0.1`, not `0.10000000149011612`. What has
no honest JSON form — a server's own structured type, a nested data value —
skips that reading with a warning rather than guessing, the same answer every
input gives a payload it cannot parse.

## security, and what is not here yet

The session is **unencrypted** (`SecurityPolicy::None`) and signs in
anonymously, or with a username and password from the connection:

```jsonc
{ "local-opcua": { "type": "opcua", "endpoint": "opc.tcp://localhost:50000",
                   "username": "kayak", "password": "${OPCUA_PASSWORD}" } }
```

Both credential fields are set together or not at all, and both resolve from the
[secret store](/io/secrets). Signed and encrypted sessions need a client
certificate, somewhere for it to live and a server trust list, which is its own
change rather than a field bolted on here — so until then, treat an OPC UA
connection as something for a network you trust.

One consequence shows up in the log and is not a fault: the client prints two
errors about a missing *application instance certificate* when it opens a
session. There is no certificate because there is no encryption. A pipeline that
logs those and then reports readings is working.

The endpoint is **dialled directly**: kayak does not ask the server for its
endpoint list first. Discovery is the usual way to write this and the usual way
for it to fail, because a server behind docker, NAT or a load balancer
advertises the hostname it knows itself by — which is regularly not one the
client can resolve. What the connection file says is what is dialled.

## when the plant goes away

Short outages are healed underneath the pipeline: the session retries forever
and the subscription and its monitored items are recreated on the other side, so
a network blip costs a gap in the data and nothing else. What is reported as a
failure — once per outage, on the pipeline's card — is a session that could not
be established at all, or one whose connection ended for good; those are retried
on a backoff. A node the server refuses is logged and the others carry on: one
tag renamed in the plant should cost that tag, not the pipeline.

There is no acknowledgement mode here. An OPC UA subscription has none that a
consumer could withhold, so `ack: on_delivery` is refused at build time rather
than silently behaving like `on_receipt`.

## trying it

`docker compose up opcua` starts Microsoft's OPC PLC simulator on
`opc.tcp://localhost:50000` with an address space that moves on its own. The
sample graph's `opcua_line1` subscribes to three of its nodes by name,
`opcua_anomalies` browses its `Anomaly` folder with a deadband, and
`opcua_line1_10s_avg` reduces the first of those per tag over ten seconds.
