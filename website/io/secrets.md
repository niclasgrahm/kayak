# secrets

Config files are meant to be version controlled, so they carry *references* to
secrets rather than the secrets themselves. Any field typed `Secret` — these all
live on connections now: the `urls` of a nats connection, the `brokers` of a
kafka one, the `password` of a postgres one — accepts `${NAME}` placeholders:

```json
{ "prod-nats": { "type": "nats", "urls": "nats://app:${NATS_PASSWORD}@broker:4222" } }
```

Those are filled in when the pipeline is built, from two sources consulted in
order:

1. the process environment;
2. a JSON file of `"NAME": "value"` pairs passed as `--secrets ./secrets.json`.

The environment comes first so a single secret can be overridden for one run
without touching the file. The flip side is that an unrelated environment
variable with a colliding name shadows the file, so keep the names specific;
a shadowed lookup is logged at debug level. `example_config/secrets.example.json`
shows the file format; anything named `secrets.json` is gitignored, wherever it
sits, which is why `just dev` creates the sample's copy rather than the
repository carrying one.

A value with no `${...}` in it is passed through untouched, so fields that hold
nothing sensitive need no special handling. An unknown name is an error, not an
empty string — the pipeline fails to start (or the `POST /api/pipelines` gets a
4xx) rather than quietly connecting without credentials.

The resolved value never leaves the runtime component that needs it. `Secret`
(in `kayak-core`, so wasm-safe) only ever holds the unresolved template, and
that is what `GET /api/pipelines` returns and what the UI shows. `Resolved` (in
`src/secrets.rs`) holds the real value but prints the *template* from `Display`
and `Debug`, so a connection error logs
`nats://app:${NATS_PASSWORD}@broker:4222` and nothing worth leaking. Getting at
the value takes an explicit `.expose()`, which is the thing to grep for in
review. Writing a password inline instead of referencing it defeats all of
this — that's the habit the syntax exists to replace.
