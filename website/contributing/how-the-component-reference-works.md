# the component reference

`/docs` is a generated reference for every input, transform and output: field
names, types, which are required, and what each one does. Nothing about it is
written by hand — `kayak_core::docs` reflects over the same `JsonSchema`
derives the config types already carry, and `schemars` carries the doc comments
through as descriptions.

What that means in practice: **the doc comments on the config structs in
`kayak-core/src/config.rs` are the documentation**. Add a component and it
appears; add a field and it appears; leave the doc comment off and a unit test
fails (`every_component_has_a_description_from_its_doc_comment`). Two things are
worth knowing when writing them: blank lines start a new paragraph and single
newlines don't, and `backticks` render as code.

The page itself is a Leptos route with a searchable sidebar; the search matches
kinds, field names and descriptions, so "subject" finds both nats components.
The same data is served as JSON at `GET /api/docs` for anything that isn't a
browser. The arranging logic is pure and unit-tested in `frontend/src/docs.rs`,
same as `graph` and `inspector`.
