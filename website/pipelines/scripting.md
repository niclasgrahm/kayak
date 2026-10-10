# scripting

The `script` transform runs a [rhai](https://rhai.rs) script on each message, or
on the whole batch. It sends the values that the script gives to `emit`.

```yaml
- type: script
  source:
    type: inline
    code: |
      msg.band = if msg.value > 7.0 { "high" } else { "normal" };
      msg
```

The script gets the message as `msg`. It sends values with `emit(value)`:

- Call `emit` zero times to drop the message.
- Call `emit` one time to replace the message.
- Call `emit` many times to split the message.

Use a script only when the other transforms are not sufficient. A config that
uses `filter`, `map` and `splitter` is easier to read and to review than a
script that does the same work.

## what a script reaches that nothing else does

**Arrays inside a message.** `splitter` makes many messages from one, and
`reduce` combines a batch. No transform goes through a list inside one message.

```rhai
let total = 0;
for line in msg.lines {
    total += line.qty * line.price;
}
msg.total = total;
msg
```

**Conditions.** `map` has no conditions. `filter` can only drop the whole
message. Use a script for a severity ladder, or for a fallback that `coalesce`
cannot express.

**String work.** Use a script to parse a log line, a `k=v` pair or a URL query.

## scripts and state together

A script can use a [state bucket](/pipelines/state). With a bucket, a script
can compare a message with an earlier one. Use this for deduplication, change
detection, sessions and thresholds with hysteresis.

```rhai
let previous = recall(msg.id);
remember(msg.id, #{ value: msg.value });

if previous == () {
    msg.direction = "unknown";      // every stateful pipeline has a warm-up
} else {
    msg.direction = if msg.value > previous.value { "rising" } else { "falling" };
}
msg
```

- `recall` gives `()` for a key that has no value yet. Use `if recall(k) == ()`
  to find the warm-up.
- The script uses the bucket in the `state` block of its pipeline. A script
  cannot name a different bucket.
- The limits of the bucket apply. A script cannot write more than `max_keys`
  keys.

::: warning
Do not [share a bucket between pipelines](/pipelines/state) for data where the
order is important. Two pipelines that share a bucket have no order between
them. A script makes this mistake easy.
:::

## message scope and batch scope

`scope: message` is the default. The operation budget applies to each message.
The batch keeps its structure. A script that sends nothing for a message drops
only that message.

`scope: batch` gives the script the whole batch as `batch`. Each value that the
script sends is a **whole batch**. Thus write `emit([msg])`, not `emit(msg)`.
Use batch scope to deduplicate a batch, to divide it, or to calculate a value
across it.

```rhai
let small = [];
let large = [];
for m in batch {
    if m.n > 1 { large.push(m); } else { small.push(m); }
}
emit(small);
emit(large);
```

Put a [`buffer`](/pipelines/pipelines#buffering-an-input) on the input when you
use batch scope. Without a buffer, many inputs send one message per batch.

To calculate across a batch, use the array functions. `pluck(batch, "value")`
gives that field across the batch as an array. `mean`, `std`, `linfit` and the
other functions calculate a result from it. See
[time and numbers](/pipelines/time-and-numbers) for the list and the rules.

## what a script is given

<!--@include: ../reference/generated/script-builtins.md-->

`throw "reason"` is part of rhai. It fails the batch with the text `reason`.

kayak generates this table from the functions that the engine registers. Thus
the table is always complete.

- `msg.a.b` is ordinary rhai indexing. Most scripts use it.
- `field(msg, "a.b")` reads a [field path](/pipelines/message-metadata#field-paths).
  Use it for a path that the script makes at runtime, and for a literal key with
  dots, for example a key that an envelope writes. The name is not `get`,
  because `get` on a rhai map reads an exact key only.

If the script does not call `emit`, kayak sends the value of the last
expression. If the script calls `emit`, kayak ignores the last expression.

## inline or in a file

`source` has two types. `inline` holds the code in the config. `file` names a
file:

```yaml
- type: script
  source: { type: file, path: scripts/swings.rhai }
```

- The path is relative to the **directory of the config file**. The path cannot
  go out of that directory.
- A server without `--config` refuses a file source. It has no directory to read
  from. An inline script works on every server.
- kayak reads the file when it builds the pipeline. After you change the file,
  do a `revert` (`POST /api/config/revert`) to load it again.

A file gets the highlighting and formatting of your editor. An inline script
keeps the pipeline in one place. **Use a YAML config for inline scripts.** YAML
shows the code as a block. JSON must escape every newline.

## sharing code between scripts

A script can `import` other rhai files. The path is relative to the
**directory of the config file**, and it cannot go out of that directory. Do not
write the `.rhai` extension. kayak adds it.

```rhai
import "scripts/shared/readings" as readings;

msg.direction = readings::direction(msg.delta);
msg
```

A project with shared code has this layout:

```
config.yaml
scripts/swings.rhai
scripts/shared/readings.rhai
```

Rules:

- **kayak resolves all imports when it builds the pipeline.** If a module is
  missing or does not compile, the pipeline does not start. A running pipeline
  does not read files. After you change a module, do a `revert`.
- **The top level of a module runs one time, at build time.** A script gets the
  functions and the exported constants of a module. Put the code for each
  message in the script that imports the module.
- **The path must be a literal.** An import with a path that the script makes
  at runtime does not resolve.
- **A server without `--config` refuses imports**, also in an inline script.
- **Only `.rhai` files resolve.** An import cannot open other files in the
  directory, for example `secrets.json`.

The sample uses an import. `scripts/shared/readings.rhai` in `example_config/`
holds the classification that two heartbeat scripts use.

## trying one out

`POST /api/scripts/dry-run` runs a script on messages that you send. It does not
make a pipeline.

```bash
curl -s localhost:6767/api/scripts/dry-run -H 'content-type: application/json' -d '{
  "source": { "type": "inline", "code": "msg.total = msg.a + msg.b; msg" },
  "messages": [{"a": 1, "b": 2}]
}'
```

```json
{ "outcome": "emitted", "batches": [[{"a": 1, "b": 2, "total": 3}]] }
```

**A script with a bug gives status 200.** The response tells where the bug is:

```json
{ "outcome": "failed", "stage": "compile", "message": "...", "line": 2, "column": 9 }
```

Status 400 means that the request itself is wrong. Examples are bad JSON, or a
`file` source that kayak cannot read.

The dry run does not use a live bucket. It gets a private bucket with the
values from `state` in the request body. The response contains the contents of
that bucket at the end. kayak then discards the bucket.

## writing one in the ui

The script editor in the web UI uses the same dry-run endpoint. It compiles and
runs the script a short time after the last keystroke. It runs the script on
messages from a [sample](/canvas/editing-the-graph#seeing-the-data-while-you-build)
of the input.

## the sandbox

A script runs inside the task of the run loop. A script that never stops blocks
a worker thread. These limits prevent that:

- **Operation budget.** `max_operations` on the transform is 100000 by default.
  A script that goes above it fails the batch. Increase it for a script that
  goes through a large array.
- **Size limits.** A string can be 256 KiB. An array can have 100000 elements.
  A map can have 10000 entries. The operation budget does not limit memory, so
  these limits are separate.
- **No filesystem, no network and no `eval`.** Imports resolve at build time.
  [Imports](#sharing-code-between-scripts) and a running script cannot read
  files. To call a service, use the [`http` transform](/reference/transforms).
- **No state between runs.** Each run gets a new scope. A top-level variable
  does not keep its value. Keep state in a bucket.

## what is checked when

kayak **compiles the script when it builds the pipeline**. A syntax error stops
the pipeline from starting. Other errors occur only when a message arrives. An
absent field, a type that does not convert or a `throw` fails that batch.

kayak does not check at build time if a script calls `remember` or `recall`
without a `state` block on the pipeline. This is an error at runtime. The error
message tells you what to add.
