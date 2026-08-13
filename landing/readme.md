# landing/

Input material for generating kayak's landing page. Nothing here is part of the
build or the test suite; it exists to be handed to a design tool.

| | |
| --- | --- |
| [`visual-language.md`](visual-language.md) | the styling brief — palette, typography, geometry, motion, voice, and the do/don't list. All taken from `style/main.scss` and the running UI. |
| [`product-and-copy.md`](product-and-copy.md) | what kayak is, who it's for, the component inventory, and ready-to-use copy for every section of the page. |
| [`screenshots/`](screenshots) | eleven screenshots of the running server, captured against `example_config/` with live NATS / Kafka / MQTT / Redis traffic. Table of contents at the end of `visual-language.md`. |

Screenshots were taken at 1600×1000 CSS px at 2× device scale (so the PNGs are
3200×2000), on `just dev` with `docker compose up` behind it.

To retake them: `docker compose up -d && just dev`, sign in as
`niclas` / `hunter2`, and drive `localhost:6767`.
