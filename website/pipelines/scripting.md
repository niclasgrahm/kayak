# scripting

`script` runs a [rhai](https://rhai.rs) script over each message, or over the
whole batch, and emits whatever the script asks for.

```yaml
- type: script
  source:
    type: inline
    code: |
      msg.band = if msg.value > 7.0 { "high" } else { "normal" };
      msg
```

It reaches the message as `msg` and emits with `emit(value)` — **zero times to
drop it, once to replace it, many times to split it**. That covers what
`filter`, `map` and `splitter` do, which is exactly why it should not be reached
for first: a script that only copies fields is a `map` written the hard way, and
a config that says what it does is worth more than one that can do anything.

## what a script reaches that nothing else does

Three things, and they are the reason this exists rather than a longer list of
transforms:

**Arrays inside a message.** `splitter` turns one message into many and `reduce`
folds a batch, but nothing walks a list *within* a message. There is no
declarative spelling of "total the line items" whose body isn't arbitrary code.

```rhai
let total = 0;
for line in msg.lines {
    total += line.qty * line.price;
}
msg.total = total;
msg
```

**Conditionals.** `map` has none by design and `filter` can only drop the whole
message, so a severity ladder or a fallback deeper than `coalesce` has nowhere
else to live.

**String work.** Parsing a log line, a `k=v` pair or a URL query is a long tail
that a `regex_extract` and a `split` would answer about half of.

## scripts and state together

This is the combination that unlocks a class of problem rather than a case.
[State buckets](/pipelines/state) give a pipeline memory; `remember` and
`recall` give it a way to write and read. What they have no spelling for is the
*comparison* — and that is where deduplication, change detection, sessionisation
and thresholds with hysteresis all live.

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

`recall` answers `()` for a key nothing has been written under yet, which is
distinguishable from an entry holding nothing — `if recall(k) == ()` is the
warm-up check. The bucket is the one the pipeline declares in its `state` block;
a script cannot name another, for the reason `remember` and `recall` cannot.

The bucket's bounds still apply, and they are enforced by the store rather than
by the script, so a script cannot write past `max_keys` however many keys it
invents.

::: warning
The rule about [sharing a bucket between pipelines](/pipelines/state) matters
more here, not less. A script makes ordering-sensitive correlation easy to
write, and two pipelines sharing a bucket are two run loops with no ordering
between them.
:::

## message scope and batch scope

`scope: message` is the default and is what nearly everything wants. The
operation budget is then spent per message, the batch structure is preserved,
and a script that emits nothing for one message has dropped exactly that message.

`scope: batch` hands the script the whole batch as `batch`, and every emitted
value is a **whole batch** — so `emit([msg])`, not `emit(msg)`. It is for the
things that are about the batch itself: deduplicating within it, repartitioning
it, or computing something across it that `reduce` has no function for.

```rhai
let small = [];
let large = [];
for m in batch {
    if m.n > 1 { large.push(m); } else { small.push(m); }
}
emit(small);
emit(large);
```

Batch scope is only interesting when something upstream has made the batches
worth looking at — put a [`buffer`](/reference/inputs) on the input, or the
script will see one message at a time.

## what a script is given

| | |
|---|---|
| `msg` | the message, in `message` scope |
| `batch` | the messages, as an array, in `batch` scope |
| `emit(value)` | emit a message (or, in `batch` scope, a batch) |
| `field(msg, "a.b")` | read a [field path](/pipelines/message-metadata), with the same rules every other transform follows |
| `recall(key)` | what the pipeline's bucket holds under `key`, or `()` |
| `remember(key, #{ ... })` | write into the pipeline's bucket |
| `now()` | the time as an RFC 3339 string |
| `now_millis()` | the time as a millisecond epoch |
| `warn(text)` | a log line, reported once per distinct text |
| `throw "reason"` | fail this batch, with `reason` on the card |

`msg.a.b` is ordinary rhai indexing and is what most scripts will use.
`field(msg, "a.b")` is for the paths that cannot spell — the ones whose segments
are chosen at runtime, and the literal dotted keys an
[envelope](/pipelines/message-metadata) writes. It is **not** called `get`: rhai
object maps already have a `get` method, and `get(msg, "a.b")` is method-call
sugar for `msg.get("a.b")`, which finds an exact key and never walks a path.

The last expression is **sugar for a single `emit`**, and only when the script
emitted nothing itself — which is what makes the one-liner above a one-liner. A
script that did both means the `emit`s.

## inline or in a file

`source` is either spelling:

```yaml
- type: script
  source: { type: file, path: scripts/swings.rhai }
```

A file is resolved against the **directory the config file is in** — the same
place the connections and layout files live — and it may not climb out of it.
That is also why a server started without `--config` refuses a file-sourced
script: there is no directory to resolve against, and the working directory
would be a boundary that moved depending on where the server was launched from.
Inline scripts work either way, which is what the HTTP API and the UI carry.

Which to use is mostly about how you work. A file gets an editor's
highlighting, a formatter and a place to keep test cases; inline keeps the
pipeline in one piece and is the only form the UI can edit. **Prefer a YAML
config for inline scripts** — it renders them as a literal block, where JSON has
to escape every newline.

The file is read when the pipeline is built, so editing it takes a revert to
pick up.

## sharing code between scripts

A script may `import` other rhai files, and the boundary is the one a file
source already has: the path is relative to the **config file's directory**, it
may not climb out, and the `.rhai` extension is implied.

```rhai
import "scripts/shared/readings" as readings;

msg.direction = readings::direction(msg.delta);
msg
```

That makes a more involved project look like this, with the helpers written
once instead of pasted into every script that classifies the same way:

```
config.yaml
scripts/swings.rhai
scripts/shared/readings.rhai
```

Three rules keep imports as reviewable as the rest of the config:

- **Everything resolves when the pipeline is built.** A missing or broken
  module is a pipeline that refuses to start, the same rule a script that does
  not parse follows — and a running pipeline never touches the filesystem.
  Editing a module, like editing a script file, takes a revert to pick up.
- **A module's top level runs once, at build time.** What a script reaches
  through an import is the module's functions and exported constants, not a
  body re-run per message — so a module is for functions, and anything
  per-message belongs in the importing script.
- **The path is a literal.** An import whose path is assembled at runtime
  resolves nothing, for the reason `eval` is refused: a script whose
  dependencies cannot be read off the page is not one a reviewer can approve.

Imports follow the file source's other consequence too: a server started
without `--config` has no directory to resolve against and refuses them, even
in an inline script. And only `.rhai` files resolve at all — an import can
never open anything else that lives beside the config, which matters because
`secrets.json` usually does.

The sample uses this: `scripts/shared/readings.rhai` in `example_config/` holds
the classification both heartbeat scripts share.

## trying one out

`POST /api/scripts/dry-run` runs a script over messages you hand it, without
creating a pipeline:

```bash
curl -s localhost:6767/api/scripts/dry-run -H 'content-type: application/json' -d '{
  "source": { "type": "inline", "code": "msg.total = msg.a + msg.b; msg" },
  "messages": [{"a": 1, "b": 2}]
}'
```

```json
{ "outcome": "emitted", "batches": [[{"a": 1, "b": 2, "total": 3}]] }
```

**A script with a bug in it is a 200, not a 400.** The request was well formed
and the server answered it completely; where the bug is *is* the answer:

```json
{ "outcome": "failed", "stage": "compile", "message": "...", "line": 2, "column": 9 }
```

A 400 means the request itself was wrong — malformed JSON, or a `file` source
naming something unreadable.

State is never live here. The run gets a private bucket, seeded from `state` in
the body and thrown away afterwards, and what it holds at the end comes back in
the response — so a stateful script can be exercised without touching what the
server is running. In the UI, the same endpoint is behind the **try it** pane
under the script editor.

## the sandbox

A script runs **synchronously inside the run loop's task**. That one fact is
what shapes everything here: a script that loops forever would wedge a worker
thread, not merely break its own pipeline.

- **Every script runs under an operation budget.** `max_operations` on the
  transform, with a generous default; exceeding it fails the batch. Raise it for
  a script that legitimately walks a large array.
- **Sizes are bounded separately**, because the budget counts operations and one
  operation can allocate — a doubling string reaches a gigabyte in thirty of
  them.
- **There is no filesystem, no network and no `eval`.** rhai's default module
  resolver reads `import`ed files off disk with no boundary; here
  [imports](#sharing-code-between-scripts) resolve when the pipeline is built,
  under the config directory's boundary, and a running script resolves nothing.
  A script that needs a service is the [`http`
  transform](/reference/transforms), which can await; this cannot.
- **Nothing survives between runs.** Each run gets a fresh scope, so a top-level
  variable is not a way to accumulate. That is deliberate: it would be state
  outside every bound the buckets enforce and invisible in the state tab. All
  persistence goes through a bucket.

## what is checked when

The script is **compiled when the pipeline is built**, so a syntax error is a
pipeline that refuses to start rather than one that fails every batch forever —
the same rule the reducer's build-time checks follow. What cannot be known until
a message arrives — a field that isn't there, a type that won't convert, a
`throw` — fails that batch and shows up on the card.

The one thing not checked at build time is whether a script calls `remember` or
`recall` without the pipeline declaring a `state` block. Knowing that means
walking the compiled script, which rhai only exposes behind a feature flag, so
it is a runtime error instead — one that says exactly what to add.
