# the http api reference

`/docs` has a second tab, **http api**. It lists every endpoint of the server:
what it takes, what it returns, and the statuses with which it can fail. Two
more endpoints serve the same content:

- `GET /api/openapi.json` serves an [OpenAPI 3.1](https://spec.openapis.org/oas/v3.1.0)
  document.
- `GET /api/reference` renders that document as a reference page, with a panel
  that sends requests.

The three agree because they come from one table:
`kayak_core::api_docs::endpoints()`. **The table is the routes.** `api_router`
is a fold over it (`src/endpoints.rs`). Thus an endpoint that is not in the
table is never registered. `handler_for` matches on an `Operation` enum, so a
table entry with no handler does not compile. `route_of` takes the method from
the table. Thus you cannot document an endpoint as `PUT` and wire it to
`post(...)`.

You write this table by hand. It is not reflected. A Rust doc comment on an
axum handler is not available at runtime, so there is nothing to reflect over.
**Thus the prose is in the table.** Each handler has a one-line `///` that
points to its entry. This is the opposite of the rule for config structs. Write
the description of an endpoint in its `ApiDoc` entry, not on the handler.

The bodies are an exception. Each body names a schema. `api_docs::schemas()`
generates the schemas with `schema_for!`, the same reflection as the component
reference. Thus the request and response shapes cannot drift from the Rust
types.

`src/openapi.rs` renders the table as the document. The schemas are the only
complex part. `schemars` 1.x emits JSON Schema 2020-12, which OpenAPI 3.1 embeds
without change. Each generated schema is a root with its shared definitions in
its own `$defs`. `src/openapi.rs` moves these definitions into one
`components/schemas` and rewrites the `$ref`s. The rest is `json!` literals.

Keep these three properties:

- **The error body is a Rust type.** `api_docs::ApiError` exists so that the
  error schema in the document is generated.
  `an_error_body_matches_the_documented_shape` in `tests/api.rs` deserializes a
  real failure into it. No other test connects `AppError` to the document.
- **`/events` has a limited description.** OpenAPI can say that a response is
  `text/event-stream`. It cannot describe the events in the stream. That is the
  job of AsyncAPI. Thus `Body::EventStream` renders as a string body with
  prose. With a JSON body, clients try to parse the stream as one document.
- **The renderer is in the repository.** `assets/scalar.js` is committed, and
  the page loads it from the server. Thus `just dev` works without a network.
  The file is 3.5 MB.

To add an endpoint, change three places:

1. Add an `Operation` variant in `kayak-core/src/api_docs.rs`.
2. Add an `ApiDoc` entry in the same file.
3. Add the handler arm in `src/endpoints.rs`.

The compiler names two of them. The document, the `/docs` tab and the rendered
reference follow without more changes.
