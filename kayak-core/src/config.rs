use crate::PipelineId;
use crate::columns::{ColumnMapping, ExtraFieldPolicy, TableIndex};
use crate::connections::ConnectionId;
use crate::state::PipelineState;
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
    /// most messages to put in one batch. Defaults to 1 — one message per
    /// batch, which is what this input has always done.
    ///
    /// Raising it only ever coalesces messages that had *already arrived*: the
    /// input still returns as soon as it has one, so a quiet subject is no
    /// slower than it was.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_batch: Option<usize>,
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
    /// most messages to put in one batch. Defaults to 1 — one message per
    /// batch, which is what this input has always done.
    ///
    /// Raising it only ever coalesces records that had *already arrived*: the
    /// input still returns as soon as it has one, so an idle topic is no slower
    /// than it was. It is worth raising when a consumer is catching up on a
    /// backlog, where one-message batches make the run loop, the transforms and
    /// every downstream pipeline do their per-batch work a hundred times over.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_batch: Option<usize>,
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
///
/// Every message carries a `value` and the `current_time` it was emitted at.
/// What the `value` holds is the `payload` field's business: a number sampled
/// from a sine wave, so a chart of it has a shape, or a random sentence, so a
/// text transform has something to chew on.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "dummy")]
pub struct DummyConfig {
    /// seconds between messages
    pub duration: u64,
    /// what each message's `value` holds: a `number` sampled from a sine wave,
    /// or a random sentence as `text`. Defaults to `number`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<DummyPayload>,
    /// peak of the sine wave — it swings between `-amplitude` and `+amplitude`.
    /// Numeric payloads only; defaults to 1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub amplitude: Option<f64>,
    /// seconds for one full turn of the sine wave. Numeric payloads only;
    /// defaults to 60. Sampling is by wall clock rather than by message count,
    /// so the wave keeps its period whatever `duration` is.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub period: Option<f64>,
}

/// What a dummy input puts in each message's `value`.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DummyPayload {
    /// a number sampled from a sine wave
    #[default]
    Number,
    /// a random sentence
    Text,
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

/// Writes each batch to objects under a prefix in an S3-compatible bucket.
///
/// The same writer as the `file` output — the same part naming, the same
/// formats, the same rotation policy — pointed at a bucket instead of a
/// directory. What differs is that an object store has no append: a part is
/// buffered in memory and uploaded whole when it rotates, so `rotate` is
/// **required** here and is what decides both how often objects appear and how
/// much a running pipeline holds.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "s3")]
pub struct S3OutputConfig {
    /// name of the s3 connection to write through — see "connections" in the
    /// readme. The bucket and credentials live there; the prefix below is this
    /// output's own.
    #[schemars(extend("x-connection" = "s3"))]
    pub connection: ConnectionId,
    /// key prefix to write under, e.g. `orders` — objects land at
    /// `<prefix>/<generated part name>`. Leave it empty to write at the root of
    /// the bucket.
    pub prefix: String,
    /// how the messages are laid out. Defaults to `ndjson`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format: Option<FileFormat>,
    /// when to finish an object and start the next one. Required: an object
    /// store cannot be appended to, so without a rotation trigger a pipeline
    /// would hold its entire run in memory and upload it once, at the end.
    pub rotate: RotationConfig,
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
/// With `columns`, each entry names a column, its type and the field to read —
/// `{"name": "temperature", "type": "float", "field": "reading.temp_c"}`, and
/// `field` defaults to the column's name. Without them the table gets a single
/// `jsonb` column holding the whole message, which is what this output has
/// always done.
///
/// The table is created if it isn't there, from the columns above; set
/// `create_table` to false for a table someone else owns. Creation never
/// *alters* an existing table — a table whose shape has moved on fails the
/// insert with the server's own error rather than being migrated from a config
/// file.
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
    /// which message field goes in which column. Leave it out to store each
    /// message whole, as JSON, in a `payload` column.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<ColumnMapping>,
    /// create the table on connect if it does not exist. Defaults to true.
    // omitted rather than written as `null` when absent, so a config saved back
    // out is the file someone hand-wrote — same rule as the port
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub create_table: Option<bool>,
    /// the columns forming the created table's primary key. With none, the
    /// table gets an `id` of its own and a `received_at` timestamp; naming one
    /// here says the data carries its own identity and drops both.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub primary_key: Vec<String>,
    /// indexes to create with the table. Each names mapped columns, in order.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub indexes: Vec<TableIndex>,
    /// what to do about a message carrying fields no column reads
    #[serde(default, skip_serializing_if = "ExtraFieldPolicy::is_default")]
    pub on_extra_fields: ExtraFieldPolicy,
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

#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub enum NumericFilterOperatorKind {
    GreaterThan,
    LessThan,
    EqualTo,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
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
    pub verb: HttpVerb,
}

/// The http method an http transform sends with.
///
/// A closed set rather than a `String` because it is one: a request is made
/// with one of these or it is not made at all, and typing the name of a method
/// into a box is a way of finding that out one round trip later than necessary.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "UPPERCASE")]
pub enum HttpVerb {
    Get,
    Post,
    Put,
    Patch,
    Delete,
}

impl std::fmt::Display for HttpVerb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Get => "GET",
            Self::Post => "POST",
            Self::Put => "PUT",
            Self::Patch => "PATCH",
            Self::Delete => "DELETE",
        })
    }
}
/// How the values of one field are combined into a single answer.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReduceFnKind {
    /// The total. Numbers only.
    Sum,
    /// The arithmetic mean. Numbers only.
    Avg,
    /// The smallest value. Numbers compare as numbers and strings
    /// alphabetically, which is what makes `min` over an ISO timestamp the
    /// earliest one.
    Min,
    /// The largest value, comparing as `min` does.
    Max,
    /// How many messages there were. The one function that needs no `field` —
    /// given one, it counts the messages that carry it instead.
    Count,
    /// How many *different* values there were, compared by their JSON form.
    CountDistinct,
    /// The value from the first message of the group, whatever type it is.
    First,
    /// The value from the last message of the group.
    Last,
    /// Every value, as an array, in the order they arrived.
    Collect,
    /// The middle value, or the mean of the middle two. Numbers only.
    Median,
    /// The population standard deviation. Numbers only.
    Stddev,
}

/// What to do about a message that doesn't carry a field being aggregated or
/// grouped by. A field present but `null` counts as missing — it is the same
/// fact said two ways.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MissingFieldPolicy {
    /// Fail the pipeline. The default, because a sum over "whichever messages
    /// happened to have the field" is wrong in a way nothing downstream can see.
    #[default]
    Error,
    /// Leave that message out of that one aggregation. An aggregation left with
    /// no values at all reports `null` (or `0`, for the counts).
    Skip,
}

impl MissingFieldPolicy {
    /// Whether this is the value serde would supply anyway — so the field can
    /// be left out of the JSON a config round-trips to.
    #[must_use]
    pub fn is_default(&self) -> bool {
        matches!(self, Self::Error)
    }
}

/// One thing to compute over a group, and what to call it in the result.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "aggregation")]
pub struct Aggregation {
    /// how to combine the values
    pub function: ReduceFnKind,
    /// the field the emitted message carries this answer under. Two
    /// aggregations may not share one, and none may collide with a `group_by`
    /// field.
    #[serde(rename = "as")]
    pub output: String,
    /// the field to aggregate. Required by every function except `count`, which
    /// counts messages when it is left out.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
}

/// Reduces a batch to one message per group, carrying whatever was asked for
/// about it. Pair it with a buffer, or it will only ever see one message at a
/// time.
///
/// With no `group_by` the whole batch is one group and one message comes out;
/// with one, a message comes out per distinct combination of those fields, in
/// the order the groups were first seen. The emitted message carries the
/// grouping fields under their own names alongside the aggregations.
///
/// Each aggregation is a `function`, the `field` to apply it to and the `as`
/// name the answer is written under — `{"function": "avg", "field": "value",
/// "as": "mean"}`. `count` is the one function that needs no `field`: without
/// one it counts the messages in the group, with one it counts the messages
/// that carried it.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "reducer")]
pub struct ReduceTransformConfig {
    /// what to compute. At least one, and each needs a distinct `as`.
    pub aggregations: Vec<Aggregation>,
    /// the fields whose combination defines a group. Omit it to reduce the
    /// whole batch at once.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub group_by: Vec<String>,
    /// what to do about a message missing one of the fields above
    #[serde(default, skip_serializing_if = "MissingFieldPolicy::is_default")]
    pub on_missing: MissingFieldPolicy,
}

/// One test a message either passes or doesn't.
///
/// The same comparisons the `filter` transform makes, spelled as a tagged union
/// so that a *list* of them can be configured and rendered as a form. Several
/// conditions are read as "all of these" — there is no `or` and no nesting,
/// because the moment either exists this is an expression language with a
/// syntax to design, and everything so far has been reachable without one.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Condition {
    /// Compares a field to a number. A message whose field is missing or isn't
    /// a number does not match.
    Numeric {
        /// the field to test — a dotted path, like anywhere else
        field: String,
        operator: NumericFilterOperatorKind,
        value: f64,
    },
    /// Compares a field to a string, the same way.
    String {
        /// the field to test — a dotted path, like anywhere else
        field: String,
        operator: StringFilterOperatorKind,
        value: String,
    },
}

/// One thing to put in the pipeline's state bucket, and what to call it there.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct Remembered {
    /// the field to take the value from
    pub field: String,
    /// the name to remember it under, which is the name `recall` asks for it
    /// by. Two entries may not share one.
    #[serde(rename = "as")]
    pub output: String,
}

/// Writes values from matching messages into the pipeline's state bucket,
/// keyed by whatever the pipeline's `state.key` names.
///
/// The message itself is **passed on unchanged** — this is a tap on the stream,
/// not a filter. A transform called `remember` that quietly swallowed what it
/// remembered would be a surprise, and the message is usually still wanted.
///
/// Needs a `state` on the pipeline; it fails to build without one.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[schemars(title = "remember")]
pub struct RememberTransformConfig {
    /// which messages to remember from — all of these have to match. Leave it
    /// out to remember from every message, which is right for a stream carrying
    /// one kind of thing and wrong for one carrying several.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub when: Vec<Condition>,
    /// what to take from a matching message. At least one, each with a distinct
    /// `as`.
    pub remember: Vec<Remembered>,
}

/// Writes values from the pipeline's state bucket onto every message, under the
/// names they were remembered by.
///
/// This is how a slow-moving fact — the unit being produced, the recipe in
/// force — reaches the fast stream that has to be attributed to it. The values
/// land as top-level fields, so a `reducer` downstream can group by them
/// without knowing where they came from.
///
/// Needs a `state` on the pipeline; it fails to build without one.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[schemars(title = "recall")]
pub struct RecallTransformConfig {
    /// the names to read out of the bucket, as `remember` wrote them. Each one
    /// is written onto the message under the same name.
    pub recall: Vec<String>,
    /// what to do about a message whose key has nothing remembered under it yet
    #[serde(default, skip_serializing_if = "RecallMissingPolicy::is_default")]
    pub on_missing: RecallMissingPolicy,
}

/// What `recall` does when the bucket has nothing for a message's key.
///
/// It has its own set rather than sharing the reducer's [`MissingFieldPolicy`]
/// because the right default is the opposite one: every stateful pipeline has a
/// warm-up in which nothing has been remembered yet, so `error` would fail
/// every pipeline on startup, and it is `null` that has no counterpart there.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RecallMissingPolicy {
    /// Drop the message. The default: a reading that can't be attributed to the
    /// thing it is about is usually noise, and passing it on unattributed makes
    /// a reducer downstream lump every such message into one bogus group.
    #[default]
    Skip,
    /// Pass the message on with the missing names as `null`.
    Null,
    /// Fail the pipeline. Only right when the bucket is filled by something
    /// that has certainly run first.
    Error,
}

impl RecallMissingPolicy {
    /// Whether this is the value serde would supply anyway — so the field can
    /// be left out of the JSON a config round-trips to.
    #[must_use]
    pub fn is_default(&self) -> bool {
        *self == Self::default()
    }
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

/// Whether — and how — an input attaches metadata about where a message came
/// from.
///
/// The metadata itself is documented per input under "metadata" on this page:
/// the subject a nats message arrived on, the topic, partition and offset of a
/// kafka record, and so on, plus the pipeline and input kind that read it. It
/// is attached **in band**, as ordinary fields on the message, so every
/// transform can filter, group and aggregate on it exactly as it does on the
/// payload's own fields — `"group_by": ["_meta.subject"]` needs nothing new.
///
/// Leaving this out is the default and means what it always meant: the message
/// is passed on exactly as it arrived. Attaching metadata changes the shape of
/// every message from this input, which is not something to do to a running
/// config without being asked.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EnvelopeConfig {
    /// Add the metadata as one more field on the message. The payload's own
    /// fields stay exactly where they were, so nothing downstream has to
    /// change.
    ///
    /// Only works on a payload that is a JSON *object*: a message that is a
    /// bare number or string has nowhere to put the field, and is skipped with
    /// a warning rather than taking the pipeline down. Use `wrap` for those.
    Merge {
        /// the field the metadata object is written to. Defaults to `_meta`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        meta: Option<String>,
    },
    /// Put the whole payload under a field of its own, beside the metadata —
    /// `{"value": <what arrived>, "_meta": {…}}`.
    ///
    /// Works whatever the payload is, which is what a source of bare readings
    /// (a `1`, a `"recipe-a"`) needs. The cost is that every field reference
    /// downstream now goes through the payload field: `value.temperature`
    /// rather than `temperature`.
    Wrap {
        /// the field the original payload is written to. Defaults to `value`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        payload: Option<String>,
        /// the field the metadata object is written to. Defaults to `_meta`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        meta: Option<String>,
    },
}

/// The field an envelope writes metadata to when the config doesn't say.
pub const DEFAULT_META_FIELD: &str = "_meta";
/// The field a `wrap` envelope writes the payload to when the config doesn't
/// say.
pub const DEFAULT_PAYLOAD_FIELD: &str = "value";

impl EnvelopeConfig {
    /// The field the metadata object is written to.
    #[must_use]
    pub fn meta_field(&self) -> &str {
        let (Self::Merge { meta } | Self::Wrap { meta, .. }) = self;
        meta.as_deref()
            .map_or(DEFAULT_META_FIELD, |name| match name.trim() {
                "" => DEFAULT_META_FIELD,
                name => name,
            })
    }

    /// The field the payload is written to, for the shape that moves it.
    #[must_use]
    pub fn payload_field(&self) -> Option<&str> {
        match self {
            Self::Merge { .. } => None,
            Self::Wrap { payload, .. } => {
                Some(payload.as_deref().map_or(DEFAULT_PAYLOAD_FIELD, |name| {
                    match name.trim() {
                        "" => DEFAULT_PAYLOAD_FIELD,
                        name => name,
                    }
                }))
            }
        }
    }
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

    /// attach metadata about where each message came from — the subject, topic,
    /// partition and so on listed under "metadata" below. Available on every
    /// input kind. Omit it and messages are passed on exactly as they arrive.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub envelope: Option<EnvelopeConfig>,
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
    Remember(RememberTransformConfig),
    Recall(RecallTransformConfig),
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
    S3(S3OutputConfig),
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
    /// the state bucket this pipeline remembers things in, and what its
    /// messages are keyed by. Only needed by a pipeline with a `remember` or
    /// `recall` transform; those fail to build without it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<PipelineState>,
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
            OutputKind::S3(c) => Some(&c.connection),
            OutputKind::Stdout(_) => None,
        });
        inputs.chain(outputs).collect()
    }
}
