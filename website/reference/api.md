---
outline: [2, 2]
---

# http api

This page lists every endpoint of the server. kayak generates it from
`kayak_core::api_docs::endpoints()`. The router is built from the same table.
Thus the page describes exactly the routes that exist. The same table is also
an [OpenAPI 3.1 document](/openapi.json). A server renders it at
`/api/reference`, with a panel that sends requests.

**Access** is the badge on each endpoint. The middleware applies the access
from the same table entry. On a server with no accounts, kayak checks nothing.
See [authentication](/operating/authentication).

Each body links to its [schema](/reference/schemas). The schemas come from the
Rust types that the handlers deserialize.

<!--@include: ./generated/api/endpoints.md-->
