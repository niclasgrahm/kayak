# kayak - graph-based stream processing

## testing

`just ci` (= `just lint` + `just test`) is what has to be green before pushing;
GitHub Actions runs the same two commands. Everything runs offline — no NATS, no
running server, no ports.

The runtime lives in `src/lib.rs`, not in `main.rs`, purely so the tests in
`tests/` can reach it. `main.rs` is only argument parsing plus the Leptos wiring.

Five layers, cheapest first:

| where | what it covers |
| --- | --- |
| `src/transforms/*.rs` `#[cfg(test)]` | pure per-transform logic: what's kept, dropped, buffered, split |
| `tests/config.rs` | the JSON wire format of every component kind |
| `tests/pipeline.rs` | the run loop: transform chaining, error tolerance, fan-out, cancellation, UI events |
| `tests/graph.rs` | `AppState`: ids, upstream wiring, lifecycle |
| `tests/api.rs` | the HTTP surface and its status codes, via `tower::oneshot` |
| `hurl/tests/*.hurl` | one smoke test against a really running server (`just test-http`) |

Two things to know when adding a component:

- `tests/config.rs::every_component_kind_has_a_wire_format_sample` reads the
  variants straight out of the generated JSON schema and fails until you add a
  sample for the new one. That's deliberate — it's the guard rail that keeps the
  wire format covered as the component list grows.
- `src/testing.rs` has the test doubles: `ScriptedInput`, `CollectingOutput`,
  `FailOnNth`, and `StreamerRuntime::from_parts` to assemble a pipeline without
  going through a config. Prefer these over touching the network in a test.

Timing-dependent tests use `#[tokio::test(start_paused = true)]` so a 10-second
window costs no wall time.

Not covered by `just test`, and deliberately so: the NATS input/output and the
HTTP transform, which are thin wrappers over their clients — they need
`docker compose up` and are exercised by `just start-baseline` / `just test-http`.

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
      `known_bug_the_remainder_is_currently_discarded` pins today's behaviour;
      flip that test when the decision is made.
- [ ] **the http transform ignores `verb`.** Every request is a POST regardless
      of what the config says. Honouring it would change behaviour for existing
      configs, so it needs a decision first.
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
- [x] **hurl tests are stale.** (fixed 2026-08-03: replaced with
      `hurl/tests/streams-crud.hurl`, which hits `/api/streams` and asserts the
      409/422/204 codes. Its old job is now done in-process by `tests/api.rs`.)
