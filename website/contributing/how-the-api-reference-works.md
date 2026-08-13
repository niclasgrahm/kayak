# the http api reference

The same idea one level up. `/docs` has a second tab, **http api**, listing
every endpoint the server serves: what it takes, what it gives back, and which
statuses it can fail with. Beside it, `GET /api/openapi.json` serves the whole
thing as an [OpenAPI 3.1](https://spec.openapis.org/oas/v3.1.0) document, and
`GET /api/reference` renders that document as a full reference page with a
request panel you can fire calls from.

The reason all three agree is that they come from one table:
`kayak_core::api_docs::endpoints()`. That table isn't a *description* of the
routes — **it is the routes**. `api_router` is a fold over it (`src/endpoints.rs`),
so an endpoint that isn't in the table is never registered, and `handler_for`
matches on an `Operation` enum, so a table entry with no handler doesn't
compile. The method comes from the table too, by way of `route_of`, so an entry
documented as a `PUT` and wired to `post(...)` isn't expressible either.

Unlike the component reference this table is written rather than reflected, and
it has to be: a Rust doc comment on an axum handler isn't readable at runtime,
so there is nothing to reflect over. **The prose therefore lives in the table**
and each handler carries a one-line `///` pointing at it — the opposite of the
convention for config structs, and worth knowing before you write an endpoint's
docs in the wrong place. The bodies are the exception: they name a schema, and
`api_docs::schemas()` generates those with the same `schema_for!` reflection the
component reference uses, so the request and response shapes can't drift from
the Rust types.

`src/openapi.rs` renders the table as the spec. The only real work there is the
schemas: `schemars` 1.x emits JSON Schema 2020-12, which OpenAPI 3.1 embeds
unchanged, but each generated schema is a *root* carrying its shared definitions
in its own `$defs` — so those are hoisted into one `components/schemas` and the
`$ref`s rewritten. Everything else is a `json!` literal.

Three things worth preserving:

- **The error body is a Rust type.** `api_docs::ApiError` exists so the spec's
  error schema is generated rather than written, and
  `an_error_body_matches_the_documented_shape` in `tests/api.rs` deserializes a
  real failure into it — nothing else connects `AppError` to what the spec
  claims.
- **`/events` is described honestly and no further.** OpenAPI can say a response
  is `text/event-stream` but has no way to describe the *events* in it; that is
  AsyncAPI's job. `Body::EventStream` therefore renders as a string body with
  prose, rather than as a JSON body clients would try to parse in one piece.
- **The renderer is vendored.** `assets/scalar.js` is committed and the page
  loads it from this server, because `just dev` has to work with no network. It
  is 3.5 MB, which is the price of that.

Adding an endpoint touches three places: an `Operation` variant and an `ApiDoc`
entry in `kayak-core/src/api_docs.rs`, and the handler arm in `src/endpoints.rs`.
The compiler names two of them for you. The spec, the `/docs` tab and the
rendered reference all follow with nothing further.
