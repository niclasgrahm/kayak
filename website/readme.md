# website/

kayak's documentation site — [VitePress](https://vitepress.dev), prose written
by hand, every reference table generated.

```bash
just docs-dev     # :5173, hot reload (npm install in here once, first)
just docs         # regenerate the reference from the Rust source
just docs-build   # production build into .vitepress/dist
```

## the split

| | |
| --- | --- |
| `getting-started.md`, `canvas/`, `pipelines/`, `io/`, `operating/`, `contributing/` | the prose. This is the guide that used to be `docs/guide.md`, one page per section. |
| `reference/*.md` | prose *about* a family of components, ending in an `<!--@include: -->` of the generated tables. |
| `reference/generated/`, `public/openapi.json`, `.vitepress/generated/sidebar.json` | **generated — do not edit.** Written by `cargo run -p kayak-docsgen`. |

The generated files are committed so the site builds on a machine with no Rust
toolchain, and `docsgen/tests/site.rs` fails when they no longer match the
schemas — a stale reference is a red `just ci`, not something noticed later.

## where a change goes

| you changed | what to do |
| --- | --- |
| a component's fields or doc comments | `just docs`. The tables, the sidebar and `/api/docs` all follow. |
| an endpoint, in `kayak-core/src/api_docs.rs` | `just docs`. So do the OpenAPI spec and the `/docs` tab. |
| what a *family* of components is for, or how to think about one | edit the prose at the top of `reference/<family>.md` |
| anything narrative | edit the page under `canvas/`, `pipelines/`, `io/`, `operating/` or `contributing/` |
| the navigation | `.vitepress/config.mts` — except the per-component and per-tag entries, which are generated |

A component added to the config enums appears here with no edit in this
directory at all: `just docs` writes its partial, adds it to its family's page
(they include the family whole) and puts it in the sidebar.

Interleaving prose with a *particular* component is what the per-component
partials are for — include them one at a time instead of the family, and write
between them:

```md
<!--@include: ./generated/components/inputs/nats.md-->

Some prose about nats specifically.

<!--@include: ./generated/components/inputs/kafka.md-->
```

The cost of doing that is that a new input then has to be added to the page by
hand, which is why the family pages don't do it by default.

## design

Colours, type and geometry come from `landing/visual-language.md` — kayak's own
palette, borders darker than the surfaces they separate, square corners, small
type, everything lowercase. It's in `.vitepress/theme/kayak.css`, and the site
is dark-only for the reason the product is.
