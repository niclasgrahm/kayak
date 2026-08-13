# how the reference is generated

Nothing in this section is written by hand, and that is the point.

kayak's config types derive `JsonSchema`, and `schemars` carries their doc
comments through as descriptions — so **the doc comments on the config structs
are the documentation**. `kayak_core::docs` reflects over those schemas and
produces a description of every component: kind, family, fields, types, which
are required, what the closed sets accept, and what a nested field's own shape
is. Three things read it, and none of them restate it:

- the **`/docs` page** on a running server, and the "add pipeline" form that
  generates its controls from the same field types;
- **`GET /api/docs`**, for anything that isn't a browser;
- **this site** — `just docs` writes markdown partials under
  `website/reference/generated/`, and the pages here pull them in.

The HTTP tables come the same way from one level up. `kayak_core::api_docs::endpoints()`
is not a *description* of the routes — the router is a fold over it, so an
endpoint that isn't in the table is never registered and an entry with no
handler doesn't compile. This site, `/api/openapi.json` and the `/docs` tab are
three renderings of that one table.

::: tip what this means for you
If a table here is wrong, the fix is in the Rust source — a doc comment in
`kayak-core/src/config.rs` or an entry in `kayak-core/src/api_docs.rs` — and
every consumer picks it up at once. A component with no doc comment fails a
unit test, and a site that has drifted from the source fails another
(`kayak-docsgen`'s `tests/site.rs`), so neither can be left behind.
:::

Adding to any of it is documented from the other side: [how the component
reference works](/contributing/how-the-component-reference-works) and [how the
api reference works](/contributing/how-the-api-reference-works).

## the sections

| | |
| --- | --- |
| [inputs](/reference/inputs) | where messages come from, and the `buffer`, `envelope` and `ack` every input shares |
| [transforms](/reference/transforms) | what happens between the input and the output |
| [outputs](/reference/outputs) | where messages go |
| [connections](/reference/connections) | the systems components name rather than configure inline |
| [state buckets](/reference/state) | what pipelines remember between batches |
| [http api](/reference/api) | every endpoint, its access, and everything it can fail with |
| [schemas](/reference/schemas) | the request and response bodies those endpoints name |

## how to read a component table

Every component is selected by a `type` tag in the config file, and the fields
in its table sit beside that tag:

```json
{ "type": "nats", "connection": "local-nats", "subject": "sensors.>" }
```

A field whose type is a closed set lists what it accepts. A field with a shape
of its own — an input's `buffer`, a file output's `rotate` — gets its own table
underneath the main one, one per variant where it is a choice between shapes. A
component whose *whole* shape is a choice, like `filter`, has a table per
variant instead of one of its own.

Inputs additionally carry a **metadata** table: the fields that input attaches
to each message when its [`envelope`](/pipelines/message-metadata) is set. That
half can't be reflected — a schema cannot know what a nats subscription knows —
so it is declared in `kayak-core/src/metadata.rs`, and an input added without
declaring it fails the test suite.

::: details reading it as data instead
The same content is served as JSON by a running kayak, and as an OpenAPI 3.1
document:

```bash
curl localhost:6767/api/docs          # every component
curl localhost:6767/api/openapi.json  # the whole HTTP surface
```

This site ships that spec too, at [`/openapi.json`](/openapi.json).
:::
