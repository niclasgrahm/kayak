use crate::StreamerId;
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
/// it safe to commit, safe to hand back from `GET /api/streams` and safe to show
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
    /// connection url, e.g. `nats://localhost:4222`. May reference secrets as
    /// `${NAME}` — see "secrets" in the readme.
    pub urls: Secret,
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
    /// comma-separated broker list, e.g. `localhost:9092`. May reference
    /// secrets as `${NAME}` — see "secrets" in the readme.
    pub brokers: Secret,
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

/// Takes another streamer's output as its input. This is what makes the
/// pipelines a graph: several streamers can read from the same upstream, and it
/// fans out to all of them. The upstream must already exist when this streamer
/// is created, so declare it earlier in the config file.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "streamer")]
pub struct StreamerConfig {
    /// id of the streamer to read from
    pub upstream: StreamerId,
}

/// Appends each batch to a file as JSON.
///
/// Takes no settings yet: the path is currently hardcoded in the
/// implementation, which makes this output unusable outside its author's
/// machine. Fixing that means adding a `path` field here.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "file")]
pub struct FileOutputConfig {}

/// Publishes every message in the batch to a nats subject, one message per
/// publish.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "nats")]
pub struct NatsOutputConfig {
    /// connection url, e.g. `nats://localhost:4222`. May reference secrets as
    /// `${NAME}` — see "secrets" in the readme.
    pub urls: Secret,
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
    /// server hostname, e.g. `localhost`
    pub host: String,
    /// the database to connect to
    pub database: String,
    /// the role to connect as
    pub user: String,
    /// that role's password. May reference secrets as `${NAME}` — see
    /// "secrets" in the readme, and prefer a reference to a literal here.
    pub password: Secret,
    /// the table to insert into, created if it does not exist. Optionally
    /// schema-qualified (`analytics.readings`); letters, digits and underscores
    /// only, since it cannot be sent as a query parameter.
    pub table: String,
    /// server port. Defaults to 5432.
    pub port: Option<u16>,
}

/// Publishes every message in the batch to a kafka topic, one message per
/// record. Records are sent without a key, so they round-robin across the
/// topic's partitions.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "kafka")]
pub struct KafkaOutputConfig {
    /// comma-separated broker list, e.g. `localhost:9092`. May reference
    /// secrets as `${NAME}` — see "secrets" in the readme.
    pub brokers: Secret,
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
    Kafka(KafkaConfig),
    Nats(NatsConfig),
    Streamer(StreamerConfig),
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
    // back out of `GET /api/streams` is byte-identical to the one that went in
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
