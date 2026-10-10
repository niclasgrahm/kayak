# the component reference

`/docs` is a generated reference for every input, transform and output. It
gives the field names, the types, the required fields and a description of
each. Nobody writes it by hand. `kayak_core::docs` reflects over the
`JsonSchema` derives of the config types. `schemars` carries the doc comments
through as descriptions.

Thus **the doc comments on the config structs in `kayak-core/src/config.rs` are
the documentation**. A new component appears in the reference, and so does a
new field. A component without a doc comment fails a unit test
(`every_component_has_a_description_from_its_doc_comment`).

Two rules apply when you write a doc comment:

- A blank line starts a new paragraph. A single newline does not.
- Text in backticks renders as code.

The page is a Leptos route with a sidebar that you can search. The search
matches kinds, field names and descriptions. For example, "subject" finds both
nats components. `GET /api/docs` serves the same data as JSON for clients that
are not browsers. The logic that arranges the page is in `frontend/src/docs.rs`.
It is pure and has unit tests, like `graph.rs` and `inspector.rs`.
