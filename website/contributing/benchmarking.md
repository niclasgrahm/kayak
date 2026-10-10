# benchmarking

`just bench` measures the run loop and prints the result. Use it to answer two
questions with numbers: "is this change slower?" and "how much can one server
do?"

```bash
just bench                      # the suite, as a table
just bench --compare            # ... and the deltas against the baseline of this machine
just bench --save               # ... and record this run as that baseline
just bench --filter pipelines   # only the multi-pipeline rows
just bench --duration 20        # longer windows, less noise
```

`just bench` is not part of `just ci`. The sweep takes about a minute, and a
slow pre-push check is a check that people skip. Run it when you change the run
loop, the transforms or other code on the per-batch path. Also run it before a
release, so that the baseline stays current.

## the numbers

These are the numbers of the committed baseline,
`bench/baselines/apple-m1-max-10c-darwin-26-5-2.json` (Apple M1 Max, 10 cores,
release build). They exclude all I/O. They show the cost of the runtime, not
end-to-end throughput.

| scenario | pipelines | batch | transforms | msgs/s | passes/s | rss |
| --- | --- | --- | --- | --- | --- | --- |
| `batch1` | 1 | 1 | 0 | 7.10M | 7.10M | 9M |
| `batch10` | 1 | 10 | 0 | 70.77M | 7.08M | 9M |
| `batch100` | 1 | 100 | 0 | 677.38M | 6.77M | 9M |
| `batch1000` | 1 | 1000 | 0 | 7.00G | 7.00M | 9M |
| `filter1` | 1 | 100 | 1 | 31.38M | 313.8k | 10M |
| `filter5` | 1 | 100 | 5 | 6.54M | 65.4k | 10M |
| `map1` | 1 | 100 | 1 | 1.78M | 17.8k | 10M |
| `pipelines10` | 10 | 100 | 0 | 1.74G | 17.43M | 11M |
| `pipelines100` | 100 | 100 | 0 | 4.30G | 42.99M | 11M |
| `pipelines1000` | 1000 | 100 | 0 | 5.61G | 56.12M | 15M |
| `depth3` | 3 (one chain) | 100 | 0 | 175.67M ingested | 1.76M | 16M |
| `watched` | 1 | 100 | 0 | 677.95M | 6.78M | 16M |

The `rss` column uses decimal megabytes, as the tool prints it. The baseline
has no rows for `rolling1` and `rolling1000`. Those scenarios are newer than the
baseline, so `--compare` shows them as `new`.

## what it measures {#what-it-measures-and-what-it-doesnt}

`kayak-bench` runs the runtime **in the process**. There is no socket, no
broker and no file system. It builds pipelines through the same seams as the
integration tests (`PipelineRuntime::from_parts`, `BuildCtx`). `testing::LoadInput`
feeds them and `testing::NullOutput` discards the output. Thus a run measures
the run loop, the merge, the transform chain and the fan-out. It does not
measure anything that changes with the other load on the machine.

The only measurement is `Pipeline::counters`. These are three relaxed atomics.
The run loop increments them on every pass, outside the gate of the event feed.
Thus a read before and after a window gives a complete count. No sampler, no
history store and no subscriber is involved, so the measurement does not change
the result. The runtime needed no extra instrumentation for the bench.

The bench does not measure the complete server. The path of the `http` input
also includes axum, the JSON extractor, TLS, the inbox channel and the cost per
request. To measure that path, use an external tool (`oha`, `vegeta`, `k6`) to
post to `POST /api/pipelines/{id}/messages` on a real binary. Read the
server-side counts from `GET /api/pipelines/{id}/history`, which uses the same
counters. Look for the rate at which `503` responses start. The ingest endpoint
uses `try_send` and reports backpressure. It does not block. kayak has no
harness for this layer yet.

## read the table {#reading-the-table}

The tool prints a table like this one:

```
scenario          pipes  batch   tf     msgs/s  passes/s   per pipe     rss errors
--------------------------------------------------------------------------------
batch100              1    100    0    677.38M     6.77M    677.38M      9M      0
map1                  1    100    1      1.78M     17.8k      1.78M     10M      0
pipelines100        100    100    0      4.30G    42.99M     42.99M     11M      0
```

**On a row with no transforms, read `passes/s` first.** With an empty chain and
a discarding output, the run loop never touches a single message. The batch is
an `Arc` that kayak clones and does not walk, and the counters add its length.
Thus these rows measure the cost of one pass. Their `msgs/s` is `passes/s`
times the batch size. This is why each step of `batch1` to `batch1000` is a
factor of ten. The 7.00G `msgs/s` of `batch1000` is the batch size, not a data
rate. On the rows with a transform, `msgs/s` counts messages.

On the `pipelines*` rows, read the `per pipe` column. The total throughput goes
up while the throughput per pipeline goes down. This shows that the runtime
scales, at a cost.

`rss` is the resident set of the complete process at the end of the row. It
includes what earlier rows left behind. Read it as a high-water mark, not as
the cost of one row.

A row with a non-zero `errors` measured a broken graph, not a slow graph. The
tool removes those rows from the ratios and says so.

## baselines are per machine {#baselines-and-why-they-are-per-machine}

An absolute number has no meaning on its own. The same commit gives very
different numbers on a laptop on battery, in a container with two cores and on
a workstation. The difference is larger than most regressions. Thus every run
carries a manifest: the commit (with a `-dirty` marker), the rustc version, the
profile, the cpu, the cores and the OS. kayak files each baseline under a
machine id from the hardware, at `bench/baselines/<machine>.json`, and the
file is committed.

`--save` refuses two kinds of run, because a later comparison against them is
wrong:

- **a debug build.** It measures the absence of the optimizer. Several hot
  paths inline away completely under `--release`.
- **a filtered run.** It removes every scenario that it did not measure.

`--compare` prints the deltas and does nothing more. It has no threshold and no
non-zero exit status. A threshold needs a measurement of the run-to-run noise
on the machine. Some weeks of recorded runs can give that measurement.

## the ratios {#ratios-are-the-numbers-that-travel}

The absolute rows have meaning only beside another row from the same machine. A
**ratio** divides two runs from the same machine, taken seconds apart. The cpu,
the compiler and the background load cancel. Quote ratios in a review, use them
for a threshold later, or compare them with a number from different hardware.

From the committed baseline:

```
ratio                value   meaning
------------------------------------------------------------------------
watched              1.00x   throughput with a browser attached to /events, against nobody watching
filter1              0.05x   throughput with one filter, against an empty chain that touches no message
map1                 0.00x   throughput with one map, against an empty chain that touches no message
depth3               0.26x   throughput ingested three pipelines deep, against one
pipelines10          0.26x   per-pipeline throughput at ten, against one
pipelines100         0.06x   per-pipeline throughput at a hundred, against one
pipelines1000        0.01x   per-pipeline throughput at a thousand, against one
filter5/filter1      0.21x   throughput at five filters, against one
```

All ratios divide by `batch100`, except `filter5/filter1`. That ratio divides
five filters by one, so it gives the cost of each extra filter.

Watch the `watched` ratio. A browser on `/events` changes the cost of every
pipeline on the server. Before kayak throttled the feed, a browser cost 46% of
the throughput. See "the ui feed is a sample" in `CLAUDE.md`. This row keeps
that number measured.

## add a scenario {#adding-a-scenario}

`kayak-bench/src/scenario.rs` is a fixed list. A baseline is useful only if the
run that made it and a run six months later ask the same questions.

- **To add** a scenario costs nothing. The baseline has no entry for it, and
  the comparison shows it as `new`.
- **To change** a scenario breaks the comparison. Change its name at the same
  time, and let the old row go out of use.

The same rule applies to the message that `LoadInput` generates. Its fields are
part of what every number means. A wider message makes every earlier baseline
invalid. Treat the message as part of the format.
