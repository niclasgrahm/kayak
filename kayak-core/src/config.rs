use crate::PipelineId;
use crate::columns::{ColumnMapping, ExtraFieldPolicy, TableIndex};
use crate::connections::ConnectionId;
use crate::mapping::MapTransformConfig;
use crate::script::ScriptTransformConfig;
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

/// Subscribes to a redis channel. Each message is parsed as JSON and emitted
/// as a batch of one; a payload that isn't JSON is skipped with a warning
/// rather than taking the pipeline down. The connection is opened on the
/// first read.
///
/// Plain `SUBSCRIBE`, not `PSUBSCRIBE` — a channel name is exact, the same
/// choice the nats input makes for a subject with no wildcard. Redis pub/sub
/// has no broker-side redelivery of any kind: an unsubscribed client simply
/// misses whatever was published while it was gone, and there is nothing an
/// ack could hold open — the same limitation `NatsConfig` has, for the same
/// reason.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "redis")]
pub struct RedisConfig {
    /// name of the redis connection to subscribe on — see "connections" in
    /// the readme. The server it points at is declared once, in the
    /// connections file, rather than repeated in every pipeline that uses it.
    #[schemars(extend("x-connection" = "redis"))]
    pub connection: ConnectionId,
    /// the channel to subscribe to
    pub channel: String,
    /// most messages to put in one batch. Defaults to 1 — one message per
    /// batch, which is what this input has always done.
    ///
    /// Raising it only ever coalesces messages that had *already arrived*: the
    /// input still returns as soon as it has one, so a quiet channel is no
    /// slower than it was.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_batch: Option<usize>,
}

/// One node an `opcua` input subscribes to, and what the messages call it.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "opcua node")]
pub struct OpcuaNodeConfig {
    /// the node's id, in OPC UA's own notation — `ns=2;s=Machine1.Temperature`
    /// for a string identifier, `ns=2;i=1042` for a numeric one, `g=` for a
    /// guid and `b=` for an opaque one. A node id with no `ns=` is in
    /// namespace 0, the server's own.
    pub node_id: String,
    /// what the messages from this node call it. Defaults to the node id
    /// itself, which is exact and unreadable; naming the tag here is what makes
    /// the rest of the pipeline — a `group_by`, a column mapping — legible.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

/// Everything under a node in the server's address space, found by browsing it
/// when the pipeline starts.
///
/// The convenient half of naming nodes, and the one with a cost worth knowing:
/// what this pipeline reads is then decided by the server's address space *at
/// the moment the pipeline starts*, so a tag added to the machine tomorrow is
/// picked up by a restart and a tag removed silently stops arriving. An
/// explicit `nodes` list is the one that says in the config file exactly what
/// is being read. The two combine — browse a folder and name the handful of
/// tags elsewhere that belong with it.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "opcua browse")]
pub struct OpcuaBrowseConfig {
    /// id of the node to browse under, in the same notation as `node_id` —
    /// typically a folder, e.g. `ns=2;s=Machine1`. Every *variable* found
    /// beneath it is subscribed to; folders and objects are followed, not
    /// subscribed.
    pub root: String,
    /// how many levels below the root to follow. Defaults to 3, and there is
    /// deliberately no spelling for "all of them": a browse of a plant server's
    /// whole address space is thousands of nodes, and the pipeline that asked
    /// for it would find that out by subscribing to them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub depth: Option<usize>,
}

/// Subscribes to variables on an OPC UA server, one message per value change.
///
/// The server pushes: this creates a subscription with a monitored item per
/// node and is told when a value changes, rather than reading them round-robin
/// on a timer. `publish_interval_ms` is how often the server may send, not how
/// often it samples — a tag that doesn't move produces no messages at all.
///
/// Each message is one reading, and carries the tag as well as the value:
///
/// ```json
/// {
///   "node": "ns=2;s=Machine1.Temperature",
///   "name": "temperature",
///   "value": 21.5,
///   "status": "Good",
///   "source_timestamp": "2026-01-01T12:00:00.123Z",
///   "server_timestamp": "2026-01-01T12:00:00.130Z"
/// }
/// ```
///
/// `status` is the reading's own quality and is **always present** — a sensor
/// that has failed reports `Bad...` with a `null` value rather than going
/// quiet, and a pipeline that acted on those as if they were readings would be
/// acting on nothing. `source_timestamp` is when the *device* says the value
/// was produced, which is the one to reduce or partition by; the envelope's
/// `received_at` is when kayak read it, and on a slow link those are not the
/// same instant.
///
/// The nodes are named by `nodes`, or found by `browse`, or both — one of them
/// is required, since an input with nothing to monitor would sit silent
/// forever. A node named twice is subscribed to once.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "opcua")]
pub struct OpcuaConfig {
    /// name of the opcua connection to subscribe on — see "connections" in the
    /// readme. The server it points at is declared once, in the connections
    /// file, rather than repeated in every pipeline that uses it.
    #[schemars(extend("x-connection" = "opcua"))]
    pub connection: ConnectionId,
    /// the nodes to subscribe to, named one by one.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub nodes: Vec<OpcuaNodeConfig>,
    /// a node to browse, subscribing to every variable found under it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub browse: Option<OpcuaBrowseConfig>,
    /// how often the server may send a batch of changes, in milliseconds.
    /// Defaults to 1000. This bounds how long a change waits, not how often
    /// anything is measured.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publish_interval_ms: Option<u64>,
    /// how often the server should *look* at each node, in milliseconds.
    /// Absent asks the server to sample at the publishing interval, which is
    /// what it does by default; a smaller value here is what fills a queue with
    /// intermediate readings between two publishes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sampling_interval_ms: Option<u64>,
    /// how many samples the server may hold for a node between publishes.
    /// Defaults to 1, which means a value that changes twice in one interval is
    /// reported once — the latest. Raise it, together with
    /// `sampling_interval_ms`, when every sample matters rather than the
    /// current value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_size: Option<u32>,
    /// how far a value must move before the server reports it, in the value's
    /// own units. Absent reports every change, however small — which on an
    /// analogue signal is every sample, since the last digit is always moving.
    ///
    /// This is applied by the *server*, so it saves the network and this
    /// pipeline alike. It only applies to numeric nodes; a string or a boolean
    /// is reported on every change whatever this says.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadband: Option<f64>,
    /// most messages to put in one batch. Defaults to 1 — one message per
    /// batch, which is what every other input does unless asked otherwise.
    ///
    /// Worth raising here more than elsewhere: one publish from the server
    /// carries every node that changed in the interval, so a subscription to
    /// two hundred tags at 1 Hz is two hundred batches a second through the run
    /// loop unless they are allowed to travel together. Raising it only ever
    /// coalesces changes that had *already arrived*.
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
    /// what a post must present to be accepted. Absent — the default — means
    /// the endpoint takes anything that reaches it, which is what every
    /// pipeline with an `http` input has always done.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<HttpAuthConfig>,
}

/// A credential carried in a header — checked by the `http` input on a post to
/// a pipeline's endpoint, and presented by the `http` output on a request it
/// sends.
///
/// One type for both directions because it is one fact: a fixed string in a
/// named header. The two halves read it differently — the input compares what
/// arrived against this, the output sets it — and only the input has the rule
/// about `ALLOWED_HEADERS`, since only the input can write a header into the
/// messages.
///
/// This is the **data plane's** own credential and has nothing to do with the
/// accounts in the settings file: those are people signing in to look at and
/// edit the graph, this is one system pushing data into one pipeline. A machine
/// posting readings should not need an account that can rewrite the config, and
/// a person with such an account should not thereby be able to post readings.
///
/// The token is a fixed string the sender repeats on every request, which makes
/// it **only as private as the transport**. kayak speaks plain HTTP; putting
/// TLS in front of it is the deployment's job, and without that the token is
/// readable by anything on the path. It is the same trade every log-ingest API
/// makes, and worth making deliberately rather than by accident.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum HttpAuthConfig {
    /// A token in the standard `Authorization` header, as
    /// `Authorization: Bearer <token>`. The one to reach for unless the system
    /// on the other end can't use that header.
    Bearer {
        /// the token. A `${NAME}` reference, so the config file holds the name
        /// and the secret store holds the value.
        token: Secret,
    },
    /// A fixed value in a header of your choosing — for webhook senders and
    /// receivers that can't use `Authorization` but can carry a header of their
    /// own, which is most of them.
    Header {
        /// the header's name, matched case-insensitively on the way in. On an
        /// `http` input it may not be one of the headers an `envelope` passes
        /// through, since that would write the credential into the messages.
        name: String,
        /// the exact value that header must have. A `${NAME}` reference, as
        /// above.
        value: Secret,
    },
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

/// The delivery guarantee to ask for on an mqtt subscribe or publish, spelled
/// the way mqtt itself names them rather than as the bare numbers `0`/`1`/`2`.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MqttQos {
    /// fire and forget — the broker never resends and there is no ack of any
    /// kind. The default.
    AtMostOnce,
    /// the broker resends until acknowledged, so a message may arrive more
    /// than once. Required for an input's `ack: on_delivery` to mean anything
    /// — see "acknowledgement modes" in the guide.
    AtLeastOnce,
    /// the broker's four-part handshake that guarantees exactly one delivery.
    /// The most expensive of the three; reach for `at_least_once` unless a
    /// duplicate would actually be wrong.
    ExactlyOnce,
}

/// Subscribes to an mqtt topic — or a topic *filter*, since mqtt's `+` and `#`
/// wildcards are valid here. Each message is parsed as JSON and emitted as a
/// batch of one; a payload that isn't JSON is skipped with a warning rather
/// than taking the pipeline down, the same rule every other input follows.
///
/// The connection is opened on the first read, and a stable client id is
/// derived from the pipeline's id and this topic — not configurable, since
/// nothing about it is a choice this pipeline needs to make and getting it
/// wrong (two inputs sharing one id) silently drops one of them.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "mqtt")]
pub struct MqttConfig {
    /// name of the mqtt connection to subscribe on — see "connections" in the
    /// readme. The broker it points at is declared once, in the connections
    /// file, rather than repeated in every pipeline that uses it.
    #[schemars(extend("x-connection" = "mqtt"))]
    pub connection: ConnectionId,
    /// the topic, or topic filter, to subscribe to
    pub topic: String,
    /// the quality of service to subscribe with. Defaults to `at_most_once`.
    /// `ack: on_delivery` needs at least `at_least_once` here — a QoS-0
    /// subscription has nothing for it to acknowledge.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub qos: Option<MqttQos>,
    /// most messages to put in one batch. Defaults to 1 — one message per
    /// batch, which is what this input has always done.
    ///
    /// Raising it only ever coalesces messages that had *already arrived*: the
    /// input still returns as soon as it has one, so a quiet topic is no
    /// slower than it was.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_batch: Option<usize>,
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

/// Publishes every message in the batch to a redis channel, one message per
/// publish.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "redis")]
pub struct RedisOutputConfig {
    /// name of the redis connection to publish on — see "connections" in the
    /// readme.
    #[schemars(extend("x-connection" = "redis"))]
    pub connection: ConnectionId,
    /// the channel to publish to
    pub channel: String,
}

/// Reads sensors and streams out of Indu Cloud, live, over
/// `/api/v1/live/sse` — the platform's own subscription protocol, under the
/// connection's API key.
///
/// Sensors and streams are named the way they are named on the platform
/// (customer-supplied ids, never UUIDs) and resolved through `/api/v1` on
/// the first read; a name the key cannot find or may not see is reported on
/// the card and looked for again after a pause, since a stream that does not
/// exist yet is the usual case for one another pipeline is about to write.
/// Every reading arrives as its own message, named — `{"kind": "sensor",
/// "name": "press-3/temperature", "value": 71.2, "at": …}` — with the
/// platform's ids riding along for anything that needs them. A dropped
/// connection reconnects with backoff; readings the connection could not keep
/// up with are reported as an error rather than silently missed.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "indu")]
pub struct InduInputConfig {
    /// name of the indu connection to read through — see "connections".
    #[schemars(extend("x-connection" = "indu"))]
    pub connection: ConnectionId,
    /// sensors to read, as `<device>/<sensor>` — the device's id followed by
    /// the sensor's, both as the platform knows them: `press-3/temperature`.
    /// The split is at the first `/`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sensors: Vec<String>,
    /// streams to read, by the name they were written under — `press-3/oee` —
    /// or, for a stream the platform computes itself, its display name.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub streams: Vec<String>,
    /// whether to start with each series' latest value before live readings
    /// arrive. Defaults to true, so a pipeline restarted at 03:00 has a value
    /// for every machine at 03:00 rather than at the next reading.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backfill: Option<bool>,
    /// most readings to put in one batch. Defaults to 1. Raising it only ever
    /// coalesces readings that had *already arrived* — a quiet sensor is no
    /// slower than it was.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_batch: Option<usize>,
}

/// One series an `indu` output writes: which stream a message's value goes
/// to, and which field carries the value.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct InduSeries {
    /// the stream's name on the Indu side, e.g. `press-3/oee`. May contain
    /// `{field}` placeholders filled from the message — `{machine}/oee` — so
    /// one output serves every machine a pipeline reduces over. A message
    /// missing a placeholder's field is skipped for this series.
    pub stream: String,
    /// the field holding the value, as a path (`oee`, `stats.mean`). Must be a
    /// number; a message where it is missing or not a number is skipped for
    /// this series rather than failing the batch.
    pub value: String,
    /// the unit Indu records when it creates the stream, e.g. `%`. Ignored
    /// once the stream exists.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unit: Option<String>,
}

/// Writes messages into Indu Cloud as **streams** — series that are not
/// sensors — through `POST /ingest/v1/streams`.
///
/// Every message yields one reading per entry in `series`; a reducer emitting
/// `{machine, oee, availability}` with two series entries writes two streams
/// per machine. An unknown stream is created on the Indu side on first sight,
/// when the connection's key may create streams. Anything but a full
/// acceptance fails the batch with Indu's own row errors quoted, so a stream
/// the key may not write to shows up on the card rather than being written
/// off as delivered.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "indu")]
pub struct InduOutputConfig {
    /// name of the indu connection to write through — see "connections".
    #[schemars(extend("x-connection" = "indu"))]
    pub connection: ConnectionId,
    /// the streams to write, one reading each per message. At least one.
    pub series: Vec<InduSeries>,
    /// the field holding the reading's time — an RFC 3339 string or epoch
    /// milliseconds. Absent, the time the batch is sent is used. An `envelope`
    /// puts an input's receive time at `_meta.received_at`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub at: Option<String>,
    /// how long one request may take before it is given up on, in seconds.
    /// Defaults to 30.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_seconds: Option<u64>,
}

/// Sends the batch to an http endpoint — the pipeline pushes its results at a
/// webhook or an ingest API rather than at a broker.
///
/// The counterpart of the `http` *input*, and the sending half of what the
/// `http` transform does: the transform replaces the batch with the reply, this
/// one is the end of the chain and the reply's body is discarded. What is not
/// discarded is its **status** — anything but a 2xx fails the batch, which is
/// what makes a webhook that is rejecting the data show up on the card rather
/// than being written off as delivered.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "http")]
pub struct HttpOutputConfig {
    /// endpoint to send to, e.g. `https://example.com/hooks/readings`
    pub url: String,
    /// http method. Defaults to `POST`. `GET` and `DELETE` are refused at build
    /// time — an output exists to send the messages somewhere, and a method
    /// with no body has nowhere to put them.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verb: Option<HttpVerb>,
    /// what one request carries. Defaults to `batch`, which is one request per
    /// batch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<HttpBodyKind>,
    /// what this output presents to be allowed to send. Absent — the default —
    /// sends no credential at all, which is what an open webhook wants.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<HttpAuthConfig>,
    /// how long one request may take before it is given up on, in seconds.
    /// Defaults to 30. A batch whose request times out is a failed batch, so
    /// this is also the longest a slow endpoint can hold the pipeline up.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_seconds: Option<u64>,
}

/// What the body of one request from an `http` output holds.
///
/// A closed set of two, and the choice is the receiving API's rather than a
/// tuning knob: an ingest endpoint that takes an array wants `batch`, a webhook
/// that takes one event per call wants `message`. There is no third spelling
/// (an envelope with a count, say) because that is the receiver's shape, and
/// shaping the request is the http transform's outstanding work, not this
/// component's.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HttpBodyKind {
    /// The whole batch as one JSON array, in one request. One round trip per
    /// batch however many messages it holds, which is why it is the default.
    #[default]
    Batch,
    /// One request per message, each body the message itself. Requests go out
    /// in order and the first failure fails the batch, so the messages after it
    /// are not sent — the same all-or-nothing a broker publish loop has.
    Message,
}

/// Publishes every message in the batch to an mqtt topic, one message per
/// publish.
///
/// A stable client id is derived from the pipeline's id and this topic, the
/// same as the mqtt input — not configurable, for the same reason.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "mqtt")]
pub struct MqttOutputConfig {
    /// name of the mqtt connection to publish on — see "connections" in the
    /// readme.
    #[schemars(extend("x-connection" = "mqtt"))]
    pub connection: ConnectionId,
    /// the topic to publish to
    pub topic: String,
    /// the quality of service to publish with. Defaults to `at_most_once`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub qos: Option<MqttQos>,
    /// ask the broker to keep this as the topic's *retained* message, handed
    /// to every future subscriber immediately on subscribe. Defaults to false.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retain: Option<bool>,
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

/// Inserts every batch into a ClickHouse table, one insert per batch.
///
/// `columns` is spelled exactly as the postgres output's is — each entry names
/// a column, its type and the field to read, and `field` defaults to the
/// column's name. Without them the table gets a single column holding each
/// message as JSON text.
///
/// Where it differs from postgres is what a created table is *sorted* by.
/// ClickHouse has no auto-increment column and no unique constraint, so there
/// is no surrogate `id` to fall back on: `order_by` names the MergeTree sorting
/// key, and a table that names none is sorted by the `received_at` timestamp it
/// gets for free. A sorting key does not deduplicate — naming one says how the
/// table is laid out and indexed, not that its rows are unique.
///
/// The table is created if it isn't there; set `create_table` to false for a
/// table someone else owns. Creation never *alters* an existing table.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "clickhouse")]
pub struct ClickhouseOutputConfig {
    /// name of the clickhouse connection to insert through — see "connections"
    /// in the readme. The url, database and user live there; the table below is
    /// this output's own.
    #[schemars(extend("x-connection" = "clickhouse"))]
    pub connection: ConnectionId,
    /// the table to insert into, created if it does not exist. Optionally
    /// database-qualified (`analytics.readings`), which overrides the
    /// connection's database; letters, digits and underscores only, since it
    /// cannot be sent as a query parameter.
    pub table: String,
    /// which message field goes in which column. Leave it out to store each
    /// message whole, as JSON text, in a `payload` column.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<ColumnMapping>,
    /// create the table on start if it does not exist. Defaults to true.
    // omitted rather than written as `null` when absent, so a config saved back
    // out is the file someone hand-wrote — same rule as the postgres port
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub create_table: Option<bool>,
    /// the columns the created table is sorted by — MergeTree's sorting key, and
    /// its index. With none, the table gets a `received_at` timestamp of its own
    /// and is sorted by that. Named columns are made `NOT NULL`, since a
    /// nullable key is not something ClickHouse sorts by.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub order_by: Vec<String>,
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

/// Holds messages back and hands them on when a *trigger* says to.
///
/// There are three triggers and they compose: a message count, a length of
/// time, and a condition on a state bucket. Any of them is enough on its own —
/// whichever comes first ends the wait, the same rule the input-level `batch`
/// buffer follows. A buffer with no trigger at all fails to build.
///
/// `size` is the one that has always been here and it behaves exactly as it
/// did: messages are handed on in batches of exactly that many, as they fill.
/// The other two release **everything currently held** as a single batch,
/// however much that is — which is the useful reading of "the run is finished,
/// send what you have".
///
/// Distinct from the `buffer` option on an input: that one batches what an
/// input produces, before any transform has seen it. This one sits in the
/// chain, so it batches what the transforms in front of it produced — after a
/// `filter` has thinned the stream, or a `recall` has enriched it.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[schemars(title = "buffer")]
pub struct BufferTransformConfig {
    /// hand messages on in batches of exactly this many, as they fill. On its
    /// own this is a buffer that only ever counts, and is what this transform
    /// has always done.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<usize>,

    /// release everything held this many seconds after the *first* held
    /// message. The window opens when a message is held rather than when the
    /// last batch went out, so this is a bound on how long a message waits and
    /// not a cadence — an idle buffer holds nothing and no clock is running.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seconds: Option<usize>,

    /// release everything held when a state bucket says so. This is the
    /// trigger a *different* pipeline can pull: buckets are global, so one
    /// pipeline can mark a run complete and this one hands on what it gathered
    /// while the run was going.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub until: Option<BufferGateConfig>,

    /// never hold more than this many messages: reaching it releases them all,
    /// whatever the triggers say, and says so in the log once. Required unless
    /// `size` is set, because `size` is its own bound — a buffer waiting on a
    /// condition that never comes true is otherwise a memory leak that grows
    /// at the rate of the stream.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_messages: Option<usize>,
}

/// A condition on a state bucket, as a release trigger for the `buffer`
/// transform.
///
/// The conditions are tested against the bucket entry rendered as an object —
/// the names `remember` wrote under are its fields — so `field` is a dotted
/// path exactly as it is everywhere else, and several conditions mean *all of
/// them*, exactly as they do on `remember`'s `when`.
///
/// Note what this is not: it is a gate on the whole buffer, not a test applied
/// to each held message. When it opens, everything held is handed on.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[schemars(title = "buffer gate")]
pub struct BufferGateConfig {
    /// which bucket to watch. Defaults to the one this pipeline's `state`
    /// names; a pipeline with no `state` of its own has to name it here.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bucket: Option<String>,

    /// which key in that bucket to read. A literal key, not a field path —
    /// this is one gate for the whole buffer, so there is no message to take a
    /// key from. Leave it out for the bucket-wide value, which is what
    /// `remember` writes when its pipeline's `state` has no `key`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,

    /// what has to be true of that key for the buffer to be released. All of
    /// them, and at least one — a gate with no conditions would be a buffer
    /// that releases on every write to the bucket.
    pub conditions: Vec<Condition>,
}

/// How a number is compared to the one in the config.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NumericFilterOperatorKind {
    GreaterThan,
    LessThan,
    EqualTo,
}

/// How a string is compared to the one in the config.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StringFilterOperatorKind {
    EqualTo,
    Contains,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub enum FilterKind {
    Numeric {
        /// the field to filter on
        #[schemars(extend("x-message-field" = true))]
        field: String,
        operator: NumericFilterOperatorKind,
        value: f64,
    },
    String {
        #[schemars(extend("x-message-field" = true))]
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
    #[schemars(extend("x-message-field" = true))]
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
    Mqtt(MqttConfig),
    Redis(RedisConfig),
    Opcua(OpcuaConfig),
    Indu(InduInputConfig),
}
/// How an input's messages are gathered into batches before the transforms see
/// them.
///
/// All three shapes are the same two limits with different halves left off — a
/// count, a time, or both, whichever is reached first. **A buffer never emits an
/// empty batch**: the clock starts when the first message of a batch arrives,
/// not when the window was asked for, so an input that goes quiet emits nothing
/// rather than a tick of nothing.
///
/// `size` is a floor rather than a ceiling, the same rule a file output's
/// `max_rows` follows: an arriving batch is never split, so an input already
/// producing batches of its own (`max_batch` on kafka and nats) can overshoot.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BufferConfig {
    /// Wait for a number of messages, however long that takes.
    Static {
        /// how many messages to gather before the batch is handed on
        size: usize,
    },
    /// Wait for a length of time, however few messages that gathers — but at
    /// least one. The window opens when the first message arrives.
    Tumbling {
        /// how long to gather messages for, measured from the first one
        window_seconds: usize,
    },
    /// Both limits: whichever is reached first ends the batch. The usual
    /// choice for a stream whose rate varies, since it bounds the batch size
    /// when the input is busy and the latency when it is quiet.
    Batch {
        /// how many messages end the batch immediately
        size: usize,
        /// how long to wait for them, measured from the first message in the
        /// batch
        window_seconds: usize,
    },
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
            Self::Wrap { payload, .. } => Some(payload.as_deref().map_or(
                DEFAULT_PAYLOAD_FIELD,
                |name| match name.trim() {
                    "" => DEFAULT_PAYLOAD_FIELD,
                    name => name,
                },
            )),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct InputConfig {
    #[serde(flatten)]
    pub kind: InputKind,

    /// batch messages from this input before the transforms see them — by
    /// count (`static`), by time (`tumbling`) or by whichever comes first
    /// (`batch`). Never emits an empty batch. Available on every input kind.
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

    /// when this input tells its broker a message is done with. Available on
    /// every input kind in the schema, but only honoured by ones with a
    /// broker-side notion of "received" vs "delivered" of their own (`kafka`,
    /// for now) — an input with nothing to acknowledge refuses to build rather
    /// than silently treating this as `on_receipt`. Defaults to `on_receipt`,
    /// which is what every input has always done. See "acknowledgement modes"
    /// in the guide.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ack: Option<AckMode>,
}

/// When an input acknowledges a message to its broker — see "acknowledgement
/// modes" in the guide for the reasoning and, importantly, its current scope.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AckMode {
    /// Acknowledge as soon as the message arrives, before any transform or
    /// output has touched it. The default, and the behaviour every input has
    /// always had — a crash between receipt and output can lose the message.
    OnReceipt,
    /// Acknowledge once the message has left *this* pipeline: every output
    /// this pipeline owns has returned, successfully or not, and every
    /// downstream pipeline fed from here has accepted it into its inbox. A
    /// failing output does not hold up the acknowledgement — see the
    /// architecture notes on why that is the current line, not a permanent
    /// one. Not yet propagated any further than this pipeline: a downstream
    /// pipeline's own outputs are not waited on.
    OnDelivery,
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
    Map(MapTransformConfig),
    Script(ScriptTransformConfig),
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
    Clickhouse(ClickhouseOutputConfig),
    Mqtt(MqttOutputConfig),
    Redis(RedisOutputConfig),
    Http(HttpOutputConfig),
    Indu(InduOutputConfig),
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
    /// may be omitted — a pipeline that only moves messages needs no transform.
    #[serde(default)]
    pub transforms: Vec<TransformConfig>,
    /// may be omitted — a pipeline that only feeds downstream pipelines needs no
    /// output of its own.
    #[serde(default)]
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
                | InputKind::Nats(_)
                | InputKind::Mqtt(_)
                | InputKind::Redis(_)
                | InputKind::Opcua(_)
                | InputKind::Indu(_) => None,
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
            InputKind::Mqtt(c) => Some(&c.connection),
            InputKind::Redis(c) => Some(&c.connection),
            InputKind::Opcua(c) => Some(&c.connection),
            InputKind::Indu(c) => Some(&c.connection),
            InputKind::Dummy(_) | InputKind::Http(_) | InputKind::Pipeline(_) => None,
        });
        let outputs = self.outputs.iter().filter_map(|output| match &output.kind {
            OutputKind::Kafka(c) => Some(&c.connection),
            OutputKind::Nats(c) => Some(&c.connection),
            OutputKind::Postgres(c) => Some(&c.connection),
            OutputKind::Clickhouse(c) => Some(&c.connection),
            OutputKind::File(c) => Some(&c.connection),
            OutputKind::S3(c) => Some(&c.connection),
            OutputKind::Mqtt(c) => Some(&c.connection),
            OutputKind::Redis(c) => Some(&c.connection),
            OutputKind::Indu(c) => Some(&c.connection),
            OutputKind::Stdout(_) | OutputKind::Http(_) => None,
        });
        inputs.chain(outputs).collect()
    }
}
