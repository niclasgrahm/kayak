# benchmarking

`just bench` sweeps the run loop and prints what it costs. It exists so that
"is this slower than it was?" has an answer that isn't a memory, and so that
"how much can one server take?" has a number beside it.

```bash
just bench                      # the suite, as a table
just bench --compare            # ... and the deltas against this machine's baseline
just bench --save               # ... and record this run as that baseline
just bench --filter pipelines   # just the multi-pipeline rows
just bench --duration 20        # longer windows, less noise
```

It is **not** part of `just ci`, deliberately: a minute-long sweep in the
pre-push loop is a minute-long sweep people learn to skip. Run it when you have
touched the run loop, the transforms or anything on the per-batch path — and
before a release, so the baseline keeps up.

## what it measures, and what it doesn't

`kayak-bench` drives the runtime **in process**: no socket, no broker, no
filesystem. It builds pipelines through the same seams the integration tests
use (`PipelineRuntime::from_parts`, `BuildCtx`), feeds them with
`testing::LoadInput` and discards through `testing::NullOutput`, so what a run
measures is the run loop, the merge, the transform chain and the fan-out —
and nothing that varies with what else is on the machine.

Measurement is `Pipeline::counters` and nothing else. Those are three relaxed
atomics the run loop adds to unconditionally, outside the event feed's
`receiver_count()` gate, so reading them before and after a window is a
*complete* count — no sampler, no history store, no subscriber, and therefore
nothing that changes the number by being asked for it. This is why a bench
needed no instrumentation added to the runtime.

What it does not cover is the whole server: axum, the JSON extractor, TLS, the
inbox channel and per-request overhead are the `http` input's path and want an
external driver (`oha`, `vegeta`, `k6`) posting to
`POST /api/pipelines/{id}/messages` against a real binary — with server-side
truth read back off `GET /api/pipelines/{id}/history`, which counts the same
counters. The number to look for there is the rate at which `503`s start, since
the ingest endpoint `try_send`s and reports backpressure rather than blocking.
That layer isn't built yet.

## reading the table

```
scenario          pipes  batch   tf     msgs/s  passes/s   per pipe     rss errors
batch100              1    100    0    714.17M     7.14M    714.17M      9M      0
map1                  1    100    1      1.78M     17.8k      1.78M     10M      0
pipelines100        100    100    0    636.99M     6.37M      6.37M     11M      0
```

**Look at `passes/s` first on any row with no transforms.** With an empty chain
and a discarding output, nothing in the run loop ever touches an individual
message — the batch is an `Arc` that gets cloned rather than walked, and the
counters take its length — so those rows measure the cost of *a pass* and
nothing else. Their `msgs/s` is that times the batch size, which is why the
`batch1 → batch1000` sweep is a clean factor of ten each step and why reading
7 GB/s off `batch1000` as a data rate is just reading the batch size back out.
The rows with a transform in them are the ones where `msgs/s` means messages.

`per pipe` is the column to read down the `pipelines*` rows: total throughput
rising while this falls is what "scales, but not for free" looks like. `rss` is
the whole process' resident set at the end of that row, so it includes what
earlier rows left behind — read it as a high-water mark, not as the cost of one
row.

Any row with a non-zero `errors` measured a broken graph rather than a slow
one. Those rows are left out of the ratios and the run says so.

## baselines, and why they are per machine

A throughput number on its own is not comparable to anything: the same commit
on a laptop on battery, in a two-core container and on a workstation differs by
more than most regressions anyone would care about. So every run carries a
manifest — commit (with a `-dirty` marker), rustc version, profile, cpu, cores,
OS — and baselines are filed under a machine id derived from the hardware, in
`bench/baselines/<machine>.json`, committed.

`--save` refuses two things, both because the file would be compared against
wrongly later: a **debug build** (it measures the optimiser's absence, and
several of the hot paths inline away entirely under `--release`) and a
**filtered run** (it would silently drop every scenario it didn't measure).

`--compare` prints deltas and stops there. There is deliberately no threshold
and no non-zero exit: a gate needs to know how much run-to-run noise this suite
actually has on this machine, which is a question a few weeks of recorded runs
answer and a guess does not.

## ratios are the numbers that travel

The absolute rows only mean something next to another row taken on the same
box. The **ratios** divide two runs taken seconds apart on one machine, so the
cpu, the compiler and the background load cancel — those are the ones worth
quoting in a review, putting a threshold on later, or comparing against a
number someone recorded on different hardware a year ago.

```
ratio                value   meaning
watched              0.96x   throughput with a browser attached to /events, against nobody watching
map1                 0.00x   throughput with one map, against an empty chain that touches no message
pipelines100         0.01x   per-pipeline throughput at a hundred, against one
filter5/filter1      0.21x   throughput at five filters, against one
```

`watched` is the one to keep an eye on: the run loop's reporting is gated on
`receiver_count() > 0`, so a browser attaching changes what every pipeline on
the box costs. That gate was worth 46% of throughput before the feed was
throttled (see "the ui feed is a sample" in `CLAUDE.md`), and this row is what
keeps the number honest rather than remembered.

## adding a scenario

`kayak-bench/src/scenario.rs` is a fixed list, and that is the point: a
baseline is only worth keeping if the run that produced it and the run six
months later asked the same questions. **Adding** a scenario costs nothing — a
baseline has no entry for it and the comparison reads it as `new`. **Changing**
one is what breaks comparability, so change its name at the same time and let
the old row age out.

The same applies to `LoadInput`'s generated message: its field set is part of
what every number means, so widening it invalidates every baseline taken before
the change. Treat it as part of the format rather than as a detail of the
double.
