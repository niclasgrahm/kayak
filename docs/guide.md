# the guide

The guide now lives in `website/`, as the [kayak documentation site][site] —
one page per section, with the component and HTTP reference generated from the
config schemas rather than written beside them.

```bash
just docs-dev     # the site on :5173
just docs         # regenerate the reference tables after changing a component
```

Where the sections went:

| | |
| --- | --- |
| the canvas, editing, arranging | `website/canvas/` |
| the pipeline model, metadata, reshaping, state, the sample | `website/pipelines/` |
| connections, secrets, http in and out, file / s3 / database outputs | `website/io/` |
| authentication, history, deployment | `website/operating/` |
| testing, benchmarking, how the reference generates itself | `website/contributing/` |
| every component and every endpoint | `website/reference/`, generated |

`docs/roadmap.md` stays here: it is a working list rather than documentation.

[site]: ../website/
