use crate::PipelineId;
use crate::connections::ConnectionId;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// A config value that may *reference* secrets rather than contain them.
///
/// On the wire it is an ordinary JSON string, but `${NAME}` placeholders in it
/// are replaced with real values when the pipeline is built, against whatever
/// secret store the server was started with:
///
/// ```json
/// { "type": "nats", "urls": "nats://app:${NATS_PASSWORD}@broker:4222" }
/// ```
///
/// The unresolved form is the only one this type ever holds. That is what makes
/// it safe to commit, safe to hand back from `GET /api/pipelines` and safe to show
/// in the UI — a resolved value exists only inside the built runtime component,
/// never in a `Config`. Resolution deliberately lives in the root crate: this
/// crate compiles to wasm for the frontend, which must not be able to hold a
/// resolved secret at all.
///
/// A value with no `${...}` in it is passed through untouched, so fields that
/// hold nothing sensitive need no special handling.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(transparent)]
pub struct Secret(String);

impl Secret {
    #[must_use]
    pub fn new(template: impl Into<String>) -> Self {
        Self(template.into())
    }

    /// The unresolved value, `${NAME}` references and all. This is what gets
    /// logged, serialised and displayed; use the resolver in the root crate to
    /// get at the real value.
    #[must_use]
    pub fn template(&self) -> &str {
        &self.0
    }
}

impl From<&str> for Secret {
    fn from(template: &str) -> Self {
        Self::new(template)
    }
}

impl From<String> for Secret {
    fn from(template: String) -> Self {
        Self::new(template)
    }
}

impl std::fmt::Display for Secret {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Subscribes to a nats subject. Each message is parsed as JSON and emitted as
/// a batch of one; a payload that isn't JSON is skipped with a warning rather
/// than taking the pipeline down. The connection is opened on the first read.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "nats")]
pub struct NatsConfig {
    /// name of the nats connection to subscribe on — see "connections" in the
    /// readme. The server it points at is declared once, in the connections
    /// file, rather than repeated in every pipeline that uses it.
    #[schemars(extend("x-connection" = "nats"))]
    pub connection: ConnectionId,
    /// the subject to subscribe to
    pub subject: String,
}

/// Consumes JSON messages from a kafka topic, each emitted as a batch of one.
///
/// A payload that isn't JSON is skipped with a warning rather than taking the
/// pipeline down, same as the nats input. The consumer connects on the first
/// read and joins a consumer group, so kafka remembers where this pipeline got
/// to between restarts.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "kafka")]
pub struct KafkaConfig {
    /// name of the kafka connection to consume from — see "connections" in the
    /// readme. The brokers are declared once, in the connections file, rather
    /// than repeated in every pipeline reading from the same cluster.
    #[schemars(extend("x-connection" = "kafka"))]
    pub connection: ConnectionId,
    /// the topic to consume from
    pub topic: String,
    /// consumer group id. Kafka tracks the read position per group, so two
    /// pipelines sharing a group split the topic between them, and two with
    /// different groups each get every message.
    pub group: String,
    /// where to start when the group has no committed position yet: `earliest`
    /// replays the topic from the beginning, `latest` only sees new messages.
    /// Defaults to `latest`.
    pub start_at: Option<KafkaStartAt>,
}

/// Where a new consumer group starts reading.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum KafkaStartAt {
    Earliest,
    Latest,
}

/// Emits one generated message on a fixed interval — a heartbeat for testing a
/// pipeline without a real source attached.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "dummy")]
pub struct DummyConfig {
    /// seconds between messages
    pub duration: u64,
}

/// Accepts messages posted to this pipeline's own endpoint,
/// `POST /api/pipelines/{id}/messages` — the pipeline is the receiving end of
/// an http API rather than something that reaches out to a broker.
///
/// The endpoint is derived from the pipeline's id and appears as soon as the
/// pipeline is running; nothing is configured about it here. The body is one
/// JSON message or an array of them, and an array arrives as one batch. A
/// pipeline can only have one of these — two would share an endpoint, and which
/// of them a request went to would be a coin toss — so a second one fails to
/// build.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "http")]
pub struct HttpInputConfig {
    /// how many posted batches may queue up ahead of the pipeline before it
    /// starts refusing them with a `503`. Defaults to 1024. The queue is what
    /// lets a burst through; refusing past it is deliberate, since the
    /// alternative is holding a request open until the pipeline catches up.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capacity: Option<usize>,
}

/// Takes another pipeline's output as its input. This is what makes the
/// pipelines a graph: several pipelines can read from the same upstream, and it
/// fans out to all of them. The upstream must already exist when this pipeline
/// is created, so declare it earlier in the config file.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "pipeline")]
pub struct PipelineConfig {
    /// id of the pipeline to read from
    #[schemars(extend("x-pipeline-id" = true))]
    pub upstream: PipelineId,
}

/// How the messages in a file are laid out.
///
/// Both are JSON — the difference is whether the file is one document or one
/// document per line. `ndjson` is the one to want for anything that streams:
/// the file is valid after every batch, so a run that is still going (or that
/// died) is still readable, and every tool that eats logs eats it.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FileFormat {
    /// one JSON message per line, appended as it arrives
    #[default]
    Ndjson,
    /// the whole file is a single JSON array, closed when the file rotates
    JsonArray,
}

/// When a file is closed and the next one started.
///
/// Both triggers are optional and are checked together — whichever comes first
/// rotates. With neither, a pipeline writes one file for as long as it runs.
///
/// Shared with the object-store output rather than local-only: "how big does a
/// part get" is the same question on a disk and in a bucket, and the answer
/// belongs in one place.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct RotationConfig {
    /// close the file once it holds this many messages
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_rows: Option<usize>,
    /// close the file this many seconds after it was opened. Measured from the
    /// open, not from the last write, so files line up on a predictable cadence.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval_secs: Option<u64>,
}

/// Writes each batch to files in a directory on the server.
///
/// The directory comes from a `file` connection and the `path` below is
/// relative to it; the server's `--data-dir` is what both are confined to, so a
/// server started without that flag cannot write files at all. Names are
/// generated rather than configured — `<open time>-<sequence>.<ext>`, which
/// sorts chronologically and cannot collide across rotations.
///
/// Meant for local development and testing. The object-store output is what
/// this shape is being built towards for anything else.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "file")]
pub struct FileOutputConfig {
    /// name of the file connection to write under — see "connections" in the
    /// readme. The root directory lives there; the path below is this output's
    /// own.
    #[schemars(extend("x-connection" = "file"))]
    pub connection: ConnectionId,
    /// directory to write into, relative to the connection's root, e.g.
    /// `orders`. Must stay inside the root: an absolute path or one containing
    /// `..` is refused rather than trimmed.
    pub path: String,
    /// how the messages are laid out. Defaults to `ndjson`.
    // omitted rather than written as `null` when absent, so a config saved back
    // out is the file someone hand-wrote — same rule as a postgres port
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format: Option<FileFormat>,
    /// when to close a file and start the next one. Without this, one file per
    /// run.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rotate: Option<RotationConfig>,
}

/// Publishes every message in the batch to a nats subject, one message per
/// publish.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "nats")]
pub struct NatsOutputConfig {
    /// name of the nats connection to publish on — see "connections" in the
    /// readme.
    #[schemars(extend("x-connection" = "nats"))]
    pub connection: ConnectionId,
    /// the subject to publish to
    pub subject: String,
}

/// Inserts every message in the batch into a postgres table, one row per
/// message.
///
/// The table is created on connect if it isn't there, with a `jsonb` column
/// holding the whole message. Mapping fields out into real columns and types is
/// not supported yet.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "postgres")]
pub struct PostgresOutputConfig {
    /// name of the postgres connection to insert through — see "connections"
    /// in the readme. The host, database and role live there; the table below
    /// is this output's own.
    #[schemars(extend("x-connection" = "postgres"))]
    pub connection: ConnectionId,
    /// the table to insert into, created if it does not exist. Optionally
    /// schema-qualified (`analytics.readings`); letters, digits and underscores
    /// only, since it cannot be sent as a query parameter.
    pub table: String,
}

/// Publishes every message in the batch to a kafka topic, one message per
/// record. Records are sent without a key, so they round-robin across the
/// topic's partitions.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "kafka")]
pub struct KafkaOutputConfig {
    /// name of the kafka connection to publish to — see "connections" in the
    /// readme.
    #[schemars(extend("x-connection" = "kafka"))]
    pub connection: ConnectionId,
    /// the topic to publish to
    pub topic: String,
}

/// Prints each batch to the server's stdout. Useful while building a pipeline
/// up; takes no settings.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "stdout")]
pub struct StdoutOutputConfig {}

/// Collects messages until it has `size` of them, then emits them as one batch.
///
/// Distinct from the `buffer` option on an input: this one sits in the transform
/// chain and batches what the earlier transforms produced, and it only counts —
/// the input-level one can also close a batch on a timer.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "buffer")]
pub struct BufferTransformConfig {
    /// how many messages make up a batch
    pub size: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub enum NumericFilterOperatorKind {
    GreaterThan,
    LessThan,
    EqualTo,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub enum StringFilterOperatorKind {
    EqualTo,
    Contains,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub enum FilterKind {
    Numeric {
        /// the field to filter on
        field: String,
        operator: NumericFilterOperatorKind,
        value: f64,
    },
    String {
        field: String,
        operator: StringFilterOperatorKind,
        value: String,
    },
}
/// Drops messages that don't match a condition, and drops the whole batch if
/// none of them do. Pick either the `Numeric` or the `String` form — the fields
/// differ because the comparisons do.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "filter")]
pub struct FilterTransformConfig {
    #[serde(flatten)]
    pub filter: FilterKind,
}

/// Posts the batch to an http endpoint as a JSON array and replaces it with the
/// JSON array in the response — so the service on the other end is the
/// transform.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "http")]
pub struct HttpTransformConfig {
    /// endpoint to send the batch to
    pub url: String,
    /// http method. Accepted but not honoured yet: every request is a POST.
    pub verb: String,
}
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ReduceFnKind {
    Sum,
    Avg,
    Min,
    Max,
}

/// Reduces a whole batch to a single message by aggregating one numeric field
/// across it. Pair it with a buffer, or it will only ever see one message at a
/// time.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "reducer")]
pub struct ReduceTransformConfig {
    /// how to aggregate
    pub function: ReduceFnKind,
    /// the field to aggregate; messages without it are ignored
    pub field: String,
}

/// Cuts one batch into several smaller ones — the opposite of `buffer`.
///
/// Note the current limitation: messages left over after the last whole chunk
/// are dropped, so 4 messages with `out_size: 3` emit one batch, not two.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "splitter")]
pub struct SplitterTransformConfig {
    /// how many messages go in each emitted batch
    pub out_size: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum InputKind {
    Dummy(DummyConfig),
    Http(HttpInputConfig),
    Kafka(KafkaConfig),
    Nats(NatsConfig),
    Pipeline(PipelineConfig),
}
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BufferConfig {
    Static { size: usize },
    Tumbling { window_seconds: usize },
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InputConfig {
    #[serde(flatten)]
    pub kind: InputKind,

    /// batch messages from this input before the transforms see them — by
    /// count (`static`) or by time (`tumbling`). Available on every input kind.
    /// Not to be confused with the `buffer` transform.
    // omitted rather than emitted as `null` when absent, so a config that comes
    // back out of `GET /api/pipelines` is byte-identical to the one that went in
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub buffer: Option<BufferConfig>,
}
/////// TRANSFORM
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum TransformKind {
    Buffer(BufferTransformConfig),
    Http(HttpTransformConfig),
    Splitter(SplitterTransformConfig),
    Reducer(ReduceTransformConfig),
    Filter(FilterTransformConfig),
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct TransformConfig {
    #[serde(flatten)]
    pub kind: TransformKind,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OutputKind {
    Stdout(StdoutOutputConfig),
    File(FileOutputConfig),
    Kafka(KafkaOutputConfig),
    Nats(NatsOutputConfig),
    Postgres(PostgresOutputConfig),
}
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct OutputConfig {
    #[serde(flatten)]
    pub kind: OutputKind,
}

/// One pipeline: every input is merged into one stream, that stream runs
/// through the transform chain in order, and each resulting batch goes to every
/// output.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Config {
    pub id: Option<String>,
    /// at least one. Batches arrive interleaved in the order the inputs produce
    /// them; there is no ordering between two different inputs.
    pub inputs: Vec<InputConfig>,
    pub transforms: Vec<TransformConfig>,
    /// may be empty — a pipeline that only feeds downstream pipelines needs no
    /// output of its own.
    pub outputs: Vec<OutputConfig>,
}

impl Config {
    /// The pipelines this one reads from: one per `pipeline` input, in the
    /// order they're declared. That is the whole of what makes the pipelines a
    /// graph rather than a list, so both the canvas layout and the config-file
    /// writer ask the question here rather than each matching on `InputKind`.
    ///
    /// The same upstream named twice comes back twice — de-duplicating is the
    /// caller's business, and both callers want a different answer.
    #[must_use]
    pub fn upstreams(&self) -> Vec<&PipelineId> {
        self.inputs
            .iter()
            // spelled out rather than wildcarded: a new input kind that names
            // another pipeline has to be added here, and the compiler is the
            // only thing that will say so
            .filter_map(|input| match &input.kind {
                InputKind::Pipeline(c) => Some(&c.upstream),
                InputKind::Dummy(_)
                | InputKind::Http(_)
                | InputKind::Kafka(_)
                | InputKind::Nats(_) => None,
            })
            .collect()
    }

    /// The connections this pipeline names, inputs before outputs, in declaration
    /// order.
    ///
    /// Asked here rather than by each caller matching on the kinds, for the same
    /// reason [`Config::upstreams`] is: it is what "is this connection still in
    /// use" and "does this graph name a connection that isn't configured" are
    /// both answered from. The same connection named twice comes back twice.
    #[must_use]
    pub fn connections(&self) -> Vec<&ConnectionId> {
        // spelled out rather than wildcarded: a new component that talks to a
        // configured system has to be added here, and the compiler is the only
        // thing that will say so
        let inputs = self.inputs.iter().filter_map(|input| match &input.kind {
            InputKind::Kafka(c) => Some(&c.connection),
            InputKind::Nats(c) => Some(&c.connection),
            InputKind::Dummy(_) | InputKind::Http(_) | InputKind::Pipeline(_) => None,
        });
        let outputs = self.outputs.iter().filter_map(|output| match &output.kind {
            OutputKind::Kafka(c) => Some(&c.connection),
            OutputKind::Nats(c) => Some(&c.connection),
            OutputKind::Postgres(c) => Some(&c.connection),
            OutputKind::File(c) => Some(&c.connection),
            OutputKind::Stdout(_) => None,
        });
        inputs.chain(outputs).collect()
    }
}
