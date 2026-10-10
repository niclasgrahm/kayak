# secrets

Keep the config file and the connections file in version control. Do not put
secrets in them. Write a `${NAME}` reference in each field that holds a
credential:

```json
{ "prod-nats": { "type": "nats", "urls": "nats://app:${NATS_PASSWORD}@broker:4222" } }
```

Fields of the type `Secret` accept references. Most of them are on connections,
for example the `urls` of a nats connection, the `brokers` of a kafka
connection and the `password` of a postgres connection. The `auth` tokens of
the `http` input, the `http` output and the `http_poll` input also accept
references.

## where the values come from

kayak fills in the references when it builds the pipeline. It looks in two
sources, in this order:

1. the process environment;
2. a JSON file of `"NAME": "value"` pairs, given as `--secrets ./secrets.json`.

The environment comes first. Thus you can override one secret for one run, and
the file stays the same. In the container image, set the values as environment
variables.

Give the secrets specific names. An unrelated environment variable with the
same name hides the value in the file. kayak logs a hidden lookup at debug
level.

`example_config/secrets.example.json` shows the file format. Git ignores every
file named `secrets.json` in the repository.

## rules

- A value without `${...}` goes through unchanged.
- An unknown name is an error. The pipeline does not start, and
  `POST /api/pipelines` returns a 4xx. kayak never connects with an empty
  credential.
- kayak keeps the resolved value inside the component that uses it.
  `GET /api/pipelines` and the web UI show the `${NAME}` template.
- Error messages and log lines also show the template. A failed nats
  connection logs `nats://app:${NATS_PASSWORD}@broker:4222`.

Do not write a password directly into a config field. kayak then cannot keep it
out of the API responses and the logs.
