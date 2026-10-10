# landing/

Input material for the landing page of kayak and for all other user-facing
copy. Nothing here is part of the build or of the test suite.

The page itself is `website/.vitepress/theme/Landing.vue`. It is the home page
of the doc site, at the root of propell.dev/kayak.

| | |
| --- | --- |
| [`copy-style.md`](copy-style.md) | the binding style guide for all user-facing text: the positioning, and the Simplified Technical English rules. Read it before you write copy. |
| [`product-and-copy.md`](product-and-copy.md) | the positioning brief: what kayak is, who it is for, the component inventory, the facts and numbers to quote, what kayak does not do, and the structure of the landing page. |
| [`visual-language.md`](visual-language.md) | the styling brief: palette, typography, geometry and motion. All of it comes from `style/main.scss` and the running UI. |
| [`screenshots/`](screenshots) | eleven screenshots of the web UI, captured against `example_config/` with live NATS, Kafka, MQTT and Redis traffic. The table of contents is at the end of `visual-language.md`. Use them only for the web UI section. |

The screenshots are 1600×1000 CSS px at 2× device scale (3200×2000 PNGs), from
`just dev` with `docker compose up`.

To take them again:

1. Run `docker compose up -d && just dev`.
2. Sign in as `niclas` / `hunter2`.
3. Open `localhost:6767`.
