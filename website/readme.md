# website/

The documentation site of kayak. It uses [VitePress](https://vitepress.dev).
People write the prose. kayak generates every reference table.

```bash
just docs-dev     # :5173, hot reload (run npm install in this directory first, one time)
just docs         # generate the reference from the Rust source again
just docs-build   # production build into .vitepress/dist
```

## the files

| | |
| --- | --- |
| `index.md`, `.vitepress/theme/Landing.vue` | the landing page. `index.md` is one line that mounts the component. The copy, the replica of a pipeline card and the tour are in the `.vue` file. The source is `landing/`. |
| `getting-started.md`, `canvas/`, `pipelines/`, `io/`, `operating/`, `contributing/` | the prose: one page per section of the guide. It replaces `docs/guide.md`. |
| `reference/*.md` | prose about a family of components. Each page ends with an `<!--@include: -->` of the generated tables. |
| `reference/generated/`, `public/openapi.json`, `.vitepress/generated/sidebar.json` | **generated. Do not edit.** `cargo run -p kayak-docsgen` writes them. |

The generated files are committed, so the site builds on a machine without a
Rust toolchain. `docsgen/tests/site.rs` fails when they do not match the
schemas. Thus a reference that is out of date makes `just ci` red.

## where a change goes

| you changed | what to do |
| --- | --- |
| the fields or doc comments of a component | run `just docs`. The tables, the sidebar and `/api/docs` follow. |
| an endpoint, in `kayak-core/src/api_docs.rs` | run `just docs`. The OpenAPI document and the `/docs` tab also follow. |
| the purpose of a family of components | edit the prose at the top of `reference/<family>.md` |
| any other text | edit the page under `canvas/`, `pipelines/`, `io/`, `operating/` or `contributing/` |
| the navigation | edit `.vitepress/config.mts`. The entries for each component and each tag are generated. |

A new component in the config enums appears on the site with no edit in this
directory. `just docs` writes its partial, adds it to the page of its family
(the page includes the family as a whole) and adds it to the sidebar.

To put prose between components, include the partials of single components
instead of the family:

```md
<!--@include: ./generated/components/inputs/nats.md-->

Some prose about nats specifically.

<!--@include: ./generated/components/inputs/kafka.md-->
```

Then you must add each new input to the page by hand. For this reason, the
family pages include the family as a whole.

## design

The colors, the type and the geometry come from `landing/visual-language.md`:
the palette of kayak, borders darker than the surfaces they separate, square
corners, small type, all text lowercase. The styles are in
`.vitepress/theme/kayak.css`. The site is dark only, like the product.
