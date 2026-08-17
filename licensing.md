# licensing

kayak is split across two licences, and the line between them is deliberate.

| what | licence | why |
| --- | --- | --- |
| `kayak-core` | Apache-2.0 | the shared vocabulary — config types, DTOs, the endpoint table. Anything talking to kayak needs these, so copyleft here would be a tax on writing a client. |
| the server (`kayak`), `frontend`, `kayak-bench`, `kayak-docsgen` | AGPL-3.0-or-later | the engine and the UI compiled into it. |

Copyright © 2026 Niclas Grahm.

## why the server is AGPL

Self-hosting kayak, modifying it, running it inside a company, embedding it in
something you don't distribute — all of that is what the licence is *for*, and
none of it asks anything of you beyond keeping the notices.

What the AGPL adds over the GPL is section 13: if you modify kayak and offer it
to other people **over a network**, those users are entitled to your modified
source. That is the one case this is aimed at — someone running a modified
kayak as a service without the changes ever coming back.

If that doesn't suit — you want to embed a modified kayak in a proprietary
product, or offer it as a service without publishing your changes — a
commercial licence is available and that is deliberately the arrangement. Open
an issue or get in touch.

## why kayak-core is not

`kayak-core` is dependency-light on purpose and compiles for `wasm32` as well
as native: it exists so the frontend and the server can share one set of config
types. That makes it the natural thing to build a client, a config generator or
a test harness against, and every one of those is a use worth encouraging
rather than licensing. It carries no engine, no runtime and no UI — nothing
that is worth protecting.

Apache-2.0 code can be used inside an AGPL-3.0 work, so the split is one-way
and works: the server may depend on core, and nothing about core's licence
weakens the server's.

## contributing

Contributions are inbound = outbound: a patch to `kayak-core` is Apache-2.0, a
patch anywhere else is AGPL-3.0-or-later, unless the patch says otherwise.

A CLA is likely to be required before external contributions are merged, so
that the commercial licence above stays possible to grant. That isn't in place
yet; it will be before the first merged pull request.

## third-party code

Two dependencies are worth naming explicitly because their terms are not the
usual MIT/Apache:

- **`assets/scalar.js`** — the vendored [Scalar](https://scalar.com) API
  reference renderer, MIT, committed as a build artifact. Its licence is beside
  it in `assets/scalar.LICENSE`, and MIT requires that notice to travel with
  any distribution — including inside a binary built with `embed-assets`. Keep
  the two files together.
- **`async-opcua`** — MPL-2.0. File-level copyleft: it reaches that crate's own
  files and nothing here, and it places no condition on kayak's licence. The
  source is on crates.io, which is what satisfies the obligation.

Everything else in the tree is MIT, Apache-2.0 or BSD as of writing. That has
not been audited across the full dependency graph yet; `cargo deny check
licenses` is the intended gate and is not wired up.

## adding a crate

Every workspace member declares a `license` in its `Cargo.toml`, and
`tests/licensing.rs` fails if one doesn't — a crate added without a licence is
a crate nobody can use, and the failure is easier to see now than after a
release.
