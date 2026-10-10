# how the reference is generated

Nobody writes this section by hand. kayak generates it from the source code.

The config types of kayak derive `JsonSchema`. `schemars` carries their doc
comments through as descriptions. Thus **the doc comments on the config structs
are the documentation**. `kayak_core::docs` reflects over those schemas and
describes every component: its kind, family, fields, types, required fields,
the values of each closed set, and the shape of each nested field. Three
consumers read that description:

- the **`/docs` page** of a server, and the "add pipeline" form of the web UI;
- **`GET /api/docs`**, for clients that are not browsers;
- **this site**: `just docs` writes markdown partials under
  `website/reference/generated/`, and the pages here include them.

The HTTP tables come from `kayak_core::api_docs::endpoints()`. The router is a
fold over that table. Thus an endpoint that is not in the table is never
registered, and an entry with no handler does not compile. This site,
`/api/openapi.json` and the `/docs` tab are three renderings of the one table.

::: tip if a table is wrong
Fix the Rust source: a doc comment in `kayak-core/src/config.rs` or an entry in
`kayak-core/src/api_docs.rs`. Every consumer gets the fix at once. A component
with no doc comment fails a unit test. A site that does not match the source
fails another test (`tests/site.rs` in `kayak-docsgen`).
:::

To add to the reference, see [how the component reference
works](/contributing/how-the-component-reference-works) and [how the api
reference works](/contributing/how-the-api-reference-works).

## the sections

| | |
| --- | --- |
| [inputs](/reference/inputs) | where messages come from, and the `buffer`, `envelope` and `ack` fields of every input |
| [transforms](/reference/transforms) | what happens between the input and the output |
| [outputs](/reference/outputs) | where messages go |
| [connections](/reference/connections) | the systems that components refer to by name |
| [state buckets](/reference/state) | what pipelines remember between batches |
| [http api](/reference/api) | every endpoint, its access, and each failure it can return |
| [schemas](/reference/schemas) | the request and response bodies of those endpoints |

## how to read a component table

A `type` tag in the config file selects each component. The fields in its table
go beside that tag:

```json
{ "type": "nats", "connection": "local-nats", "subject": "sensors.>" }
```

A field with a closed set of values lists the values. A field with a shape of
its own, such as `buffer` on an input or `rotate` on a file output, has its own
table below the main table. When the field is a choice between shapes, it has
one table per shape. A component whose complete shape is a choice has one table
per variant.

Inputs also have a **metadata** table. It lists the fields that the input adds
to each message when you set its [`envelope`](/pipelines/message-metadata). A
schema cannot know what a nats subscription knows, so kayak cannot reflect this
part. `kayak-core/src/metadata.rs` declares it. An input without a declaration
fails the test suite.

::: details read it as data
A server serves the same content as JSON and as an OpenAPI 3.1 document:

```bash
curl localhost:6767/api/docs          # every component
curl localhost:6767/api/openapi.json  # the complete HTTP surface
```

This site also has the document, at [`/openapi.json`](/openapi.json).
:::
