# connections

A kafka cluster or a nats server is usually shared. One pipeline per topic on
the same brokers is the normal shape, and repeating the broker list — and its
`${NAME}` references — in every one of them is both tedious and a way for them
to drift apart. So the connection is declared once, under a name, in a third
file beside the config:

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

The split between the two is **"what does the system need" against "what does
this pipeline want from it"**: brokers, urls and credentials belong to the
connection; the topic, the consumer group, the subject and the postgres table
belong to the component. There is no inline form — a component names a
connection or it does not build.

One kind serves both directions: a `kafka` connection is what a kafka input
consumes from *and* what a kafka output publishes to. The kind is checked as
well as the name, so a nats connection in a kafka input is refused at build time
with an error saying which kind it actually is, rather than being handed to a
broker as a broker list. An unknown name lists the ones that do exist, since the
usual cause is a typo.

**Where the file comes from.** `--connections <path>` names it outright, which
is how two configs share one; without the flag it is derived from the config's
name and format — `config.json` → `config.connections.json`, `pipelines.yaml` →
`pipelines.connections.yaml`. A derived file that isn't there means "no
connections", which is the ordinary state of a graph built out of dummies; a
file named with the flag has to exist, because starting without it would fail
later and further from the cause.

**It follows the config file's rules, not the layout file's.** Adding a
connection in the UI changes what the server can build, so it is an unsaved
change, and only a save writes it — the same save, since a config saved without
the connections it names would not start. `revert` reloads both files, the
connections first, because the pipelines being rebuilt name them.

**A connection is read when a component is built.** Editing one therefore
reaches new and rebuilt pipelines rather than the running ones, and deleting one
a running pipeline still names is refused with a 409 listing them — delete those
first. Nothing is pooled: two pipelines on one connection each get their own
client, built from the same settings.

In the UI, connections are the second tab in the sidebar, with the same `+` and
the same armed delete as the pipelines. The form is generated the same way too —
a connection kind is documented on `/docs` and gets its controls from the same
schema reflection. Secrets are *referenced* there and never entered: a field
takes `${NAME}`, and what that resolves to stays a deployment concern.
