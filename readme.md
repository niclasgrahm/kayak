# kayak - graph-based stream processing

## currently working on

- [ ] add filter transform
- [ ] add some kind of component plugin registry which can be used to generate docs

## todo

- [ ] make sure to clean up old template based UI stuff
- [ ] add time based buffer for the transform buffer
- [ ] make outputs optional (for example, when a parent node is only used to push data to children)
- [ ] think about necessary metadata to add to each message
- [x] deal with all unwraps -- this will bite us in the ass soon otherwise
      (done 2026-08-03: no unwrap/expect left in src/; see "known issues" below
      for the things that pass turned up but didn't change)
- [ ] show config in the "cards" in the web ui
- [ ] give streamer ability to have multiple inputs
- [ ] new transform (i guess?): wait_for_condition (should it be called buffer_until_condition? or perhaps both are needed?)
      for example, we need to wait for x: a and z: b. for this, we also need the multiple input thing

## known issues

Found during the error-handling pass on 2026-08-03. Each one needs a decision,
which is why they weren't just fixed.

- [ ] **splitter drops the remainder.** `src/transforms/splitter.rs` — with
      `out_size: 3` and a 10-message batch, message 10 is silently discarded
      (the existing `// TODO: theres stuff left here`). Decide whether leftovers
      are emitted as a short final batch or held until the next `apply()`.
- [ ] **dead streamers stay in the map.** When a run loop exits (e.g. its input
      errored), the `StreamerHandle` stays in `AppState`, so `GET /api/streams`
      lists a pipeline that isn't running. `join_handle` is never inspected.
      Needs a real lifecycle/status concept — running / stopped / failed —
      probably surfaced in the UI cards too.
- [ ] **file output has a hardcoded path.** `src/outputs/file.rs` writes to an
      absolute path under `/Users/niclas/...` and truncates it on every
      `init()`. `FileOutputConfig` is an empty struct; it wants at least a
      `path`, and a decision on truncate vs. append.
- [ ] **`--port` does nothing.** `src/main.rs` only logs it; the listener binds
      `leptos_options.site_addr` from `Cargo.toml`. Running the binary outside
      `cargo leptos` therefore falls back to port 3000. Either wire the arg into
      the leptos options or drop it.
- [ ] **hurl tests are stale.** `hurl/tests/*.hurl` POST to
      `http://localhost:6767` (root); the routes are `/api/streams`. They also
      predate the 409/422 status codes the API now returns.
