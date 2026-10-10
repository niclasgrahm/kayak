# copy style

The rules for all user-facing text about kayak: the landing page, the doc site,
the readme, and the documentation that the server shows at `/docs` (the doc
comments on the config types, the endpoint table, the script built-ins).

Two parts. Part 1 says what to write about. Part 2 says how to write it.

## 1. Positioning

### What kayak is

kayak is a stream processor. It is one Rust binary. You write pipelines in a
config file, and you keep that file in version control. You run the container
image with that file. That is the complete deployment. Users of Benthos /
Redpanda Connect, Vector or Fluent Bit know this model. kayak works the same
way.

### The three things to show

1. **Performance.** The run loop is fast and its cost is measured. Use the
   numbers from `bench/baselines/` (Apple M1 Max, in-process, no network, no
   disk). Always say that the numbers exclude I/O. They show that the runtime
   is not the bottleneck. They are not end-to-end throughput.

   | scenario | result |
   | --- | --- |
   | one pipeline, no transforms | about 7 million passes per second |
   | one pipeline, batches of 100, one `filter` | about 31 million messages per second |
   | 1000 pipelines at the same time, batches of 100 | about 5.6 billion messages per second, 14 MiB resident |
   | one pipeline at rest | about 9 MiB resident |

   Other performance facts: no garbage collector; messages are shared
   (`Arc`), not copied, on fan-out; `max_batch` and `buffer` amortize the
   per-batch cost; the clickhouse output writes one insert per batch.

2. **Composability.** Small parts that connect:
   - a pipeline can have many inputs and many outputs;
   - the `pipeline` input connects pipelines into a graph (fan-out, fan-in,
     any depth);
   - connections declare a system once, and many components refer to it;
   - state buckets are shared between pipelines;
   - transforms are small and do one thing: `filter`, `map`, `reduce`,
     `splitter`, `buffer`, `remember`/`recall`, the streaming statistics, the
     `http` transform for a model or a service, and `script` (rhai) when the
     other transforms are not sufficient;
   - message metadata is ordinary JSON fields, so every transform can use it.

3. **Feature completeness.** Show the full inventory. Inputs, transforms,
   outputs, connections. Also: secrets from the environment or a file,
   authentication, acknowledgement modes (`on_receipt`, `on_delivery`) for kafka and mqtt, column
   mapping for the database outputs, file and s3 rotation, OPC UA, a generated
   reference, an OpenAPI 3.1 document, a day of counters and failure records
   per pipeline (in memory).
   Check each claim against the code before you write it.

### The web UI

kayak has a web UI. It is a convenience, not the product. Do not lead with it.
Do not make it the headline, the first section, or the main image.

- Mention it one time on the landing page, near the end, as an optional tool:
  "The server also has a web UI. Use it to look at the running graph and the
  messages in each pipeline."
- In the guide, the UI pages are in one section ("web ui") after the sections
  about pipelines, I/O and operation.
- In other pages, do not tell the user to click on something to do a task.
  Give the config, the command or the HTTP request. A UI step is an optional
  extra, after the config.
- Do not use "canvas", "card" or "edge" outside the web UI section, unless the
  text is about the UI.

### The typical workflow (use this in examples)

1. Write `config.yaml` (or `.json`) and `config.connections.yaml`.
2. Commit them.
3. Run `docker run -v "$PWD:/kayak" ghcr.io/niclasgrahm/kayak --config /kayak/config.yaml`,
   or deploy the same image to Kubernetes with the config in a ConfigMap.
4. Change the file, review the diff, deploy again.

## 2. Language: ASD-STE100 Simplified Technical English

Apply STE wherever possible. Technical names (component names, field names,
code, product names) are permitted as they are. Technical verbs from the
domain (subscribe, publish, parse, serialize, deploy, commit, batch, buffer)
are permitted.

### Sentences

- **Procedural sentences: 20 words maximum. Descriptive sentences: 25 words
  maximum.** Count the words. Code in backticks counts as one word.
- **One instruction per sentence.** Two actions at the same time are the only
  exception.
- **One topic per sentence.** Do not join two facts with a dash, a semicolon
  or "which".
- **Paragraphs: one topic, six sentences maximum.**
- **Use the active voice.** Not "the batch is dropped". Write "kayak drops the
  batch" or "the output drops the batch".
- **Use the imperative for instructions.** "Set `max_batch` to 100." Not "You
  may want to set…", not "Consider setting…".
- **Use only simple tenses**: present, simple past, simple future. Do not use
  "has been", "is going to", "would have".
- **Do not use the -ing form** as a verb or a noun ("when using", "batching
  helps"). Write "when you use", "a batch helps". Technical names are an
  exception (`on_missing`, "load balancing" as a term).
- **Use articles** ("the", "a") and do not omit words to make text short.
- **Use vertical lists** for sequential steps and for a set of items.
- **Write warnings and cautions with the command first**, then the reason:
  "Do not share a bucket between pipelines for ordered data. The two pipelines
  have no order between them."
- **Use "must" for a requirement, "can" for a possibility, "do not" for a
  prohibition.** Avoid "should", "might", "may", "would", "could".
- **No contractions** (don't, it's, isn't, you'll).
- **Noun clusters: three words maximum.** Not "pipeline input buffer window
  size". Write "the window size of the input buffer".

### Words

One word has one meaning. Use the same word for the same thing on every page.

| do not write | write |
| --- | --- |
| ensure | make sure |
| utilize, leverage | use |
| perform, carry out | do |
| approximately, roughly | about |
| prior to | before |
| subsequent, following (as "after") | after |
| in order to | to |
| via | through, with |
| allows you to, lets you | you can |
| numerous, a number of | many |
| obtain, get hold of | get |
| commence, kick off | start |
| terminate | stop |
| sufficient, enough of | sufficient (approved), or rewrite |
| emit (in prose) | send |
| take down, bring up, set up (phrasal verbs) | stop, start, configure |
| e.g., i.e., etc. | for example, that is (or rewrite) |

### Things to remove

The current copy has a voice that reads as machine-written. Remove these
patterns:

- Rhetorical contrast: "not X, but Y", "X, not Y", "rather than" in every
  paragraph. State what it is.
- Design-justification in user text: "deliberately", "on purpose", "that is
  the point", "the honest answer", "load-bearing", "this is what makes",
  "worth knowing", "the one thing", "and that is a decision". User text says
  what kayak does and what to do. The design reasons belong in `CLAUDE.md`
  and `docs/`. Keep a reason only when the user needs it to make a correct
  decision, and then give it in one sentence.
- Em dashes as joints between clauses. Use a full stop.
- Clever asides, metaphors, personification ("a pipeline that broke at 02:14
  has something to show at 08:00", "the graph glows").
- Marketing words: effortless, seamless, powerful, blazing, unleash, simply,
  just, easily, robust.
- Hedges and filler: "really", "actually", "quite", "a little", "of course",
  "in fact", "basically".
- Second-guessing the reader: "you might think", "it is tempting to".
- Exclamation marks.

### Format

- The name is **kayak**, lowercase, also at the start of a sentence.
- Headings stay lowercase (the site's visual style). Body text uses normal
  sentence capitalization.
- Component names and field names in backticks: `nats`, `max_batch`.
- Units: "ms", "s", "MiB". Numbers with digits.

### Example

Before:

> `batch` closes on whichever limit is reached first, which is what a stream
> with a varying rate wants: the count bounds how big a batch gets when the
> input is busy, and the window bounds how long a message waits when it is
> quiet.

After:

> A `batch` buffer closes when it reaches one of its two limits. The `size`
> limit sets the largest batch when the input is busy. The `window_seconds`
> limit sets the longest wait for a message when the input is quiet. Use
> `batch` when the rate of the input changes.

## 3. Technical constraints (for whoever edits)

- VitePress compiles the pages as Vue. A bare `<name>` in text or in a doc
  comment is a build error. Put it in backticks.
- If you change a heading that other pages link to, keep the old anchor with
  VitePress syntax: `## new heading {#old-slug}`.
- Do not change a statement of fact unless you verified it in the code. Copy
  must be correct before it is short.
- The reference pages under `website/reference/generated/` are generated by
  `just docs` from the doc comments. Do not edit them by hand.
