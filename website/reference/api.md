---
outline: [2, 2]
---

# http api

Every endpoint the server serves. This is generated from
`kayak_core::api_docs::endpoints()` — the table `api_router` is folded over —
so it describes the routes that exist and cannot describe any that don't. The
same table is served as an [OpenAPI 3.1 document](/openapi.json), which a
running server also renders at `/api/reference` with a panel you can fire calls
from.

**Access** is the badge on each endpoint, and it is enforced by the middleware
the router applies from this same entry rather than being a second fact that
agrees with it today. On a server with no accounts configured, none of it
applies: nobody is identified, so nothing is checked. See
[authentication](/operating/authentication).

Bodies link to the [schemas](/reference/schemas) they name, which are generated
from the Rust types the handlers actually deserialize into.

<!--@include: ./generated/api/endpoints.md-->
