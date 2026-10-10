use crate::PipelineId;
use crate::columns::{ColumnMapping, ExtraFieldPolicy, TableIndex};
use crate::connections::ConnectionId;
use crate::mapping::MapTransformConfig;
use crate::script::ScriptTransformConfig;
use crate::sql::SqlPollConfig;
use crate::state::PipelineState;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// A text value that can refer to secrets with `${NAME}` references.
///
/// The value is an ordinary JSON string. When kayak builds the pipeline, it
/// replaces each `${NAME}` reference with the value from the secret store of
/// the server:
///
/// ```json
/// { "type": "nats", "urls": "nats://app:${NATS_PASSWORD}@broker:4222" }
/// ```
///
/// The config keeps only the reference, never the value. Thus you can commit
/// the file, and `GET /api/pipelines` does not show the secret.
///
/// kayak uses a value with no `${...}` reference as it is.
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

/// Subscribes to a nats subject and parses each message as JSON.
///
/// The input skips a payload that is not JSON and writes a warning to the log.
/// The input opens the connection on the first read.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "nats")]
pub struct NatsConfig {
    /// The name of the nats connection to subscribe on. Declare the connection
    /// in the connections file.
    #[schemars(extend("x-connection" = "nats"))]
    pub connection: ConnectionId,
    /// The subject to subscribe to.
    pub subject: String,
    /// The maximum number of messages in one batch. The default is 1.
    ///
    /// The input puts only messages that are already received into a batch.
    /// It does not wait for more messages, so a high value does not add
    /// latency on a quiet subject.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_batch: Option<usize>,
}

/// Subscribes to a redis channel and parses each message as JSON.
///
/// The input skips a payload that is not JSON and writes a warning to the log.
/// The input opens the connection on the first read.
///
/// The input uses `SUBSCRIBE`, so the channel name must be exact. Patterns are
/// not supported. Redis pub/sub does not send a message again. When the input
/// is not connected, it does not receive the messages that are published.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "redis")]
pub struct RedisConfig {
    /// The name of the redis connection to subscribe on. Declare the
    /// connection in the connections file.
    #[schemars(extend("x-connection" = "redis"))]
    pub connection: ConnectionId,
    /// The channel to subscribe to.
    pub channel: String,
    /// The maximum number of messages in one batch. The default is 1.
    ///
    /// The input puts only messages that are already received into a batch.
    /// It does not wait for more messages, so a high value does not add
    /// latency on a quiet channel.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_batch: Option<usize>,
}

/// One node that an `opcua` input subscribes to, and the name for it in the
/// messages.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "opcua node")]
pub struct OpcuaNodeConfig {
    /// The id of the node, in OPC UA notation. Use `ns=2;s=Machine1.Temperature`
    /// for a string identifier and `ns=2;i=1042` for a numeric identifier. Use
    /// `g=` for a GUID and `b=` for an opaque identifier. A node id with no
    /// `ns=` is in namespace 0, the namespace of the server.
    pub node_id: String,
    /// The name of the node in the messages. The default is the node id. Set
    /// a readable name to use in a `group_by` or a column mapping.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

/// All variables under a node in the address space of the server. The input
/// browses for them when the pipeline starts.
///
/// The input reads the address space only at the start. A tag that is added
/// to the server later is read only after a restart. A tag that is removed
/// stops without an error. Use a `nodes` list to name in the config file
/// exactly which nodes the input reads. You can use `browse` and `nodes`
/// together.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "opcua browse")]
pub struct OpcuaBrowseConfig {
    /// The id of the node to browse under, in the same notation as `node_id`.
    /// This is usually a folder, for example `ns=2;s=Machine1`. The input
    /// subscribes to each variable under it. It follows folders and objects
    /// but does not subscribe to them.
    pub root: String,
    /// The number of levels below the root to follow. The default is 3. The
    /// value 0 is not permitted. There is no value for "all levels", because
    /// the address space of a plant server can have thousands of nodes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub depth: Option<usize>,
}

/// Subscribes to variables on an OPC UA server and sends one message for each
/// change of a value.
///
/// The input makes a subscription with one monitored item for each node. The
/// server sends a value when it changes. The input does not poll. A tag that
/// does not change sends no messages. `publish_interval_ms` sets how
/// frequently the server can send. It does not set how frequently the server
/// samples.
///
/// Each message is one reading. It contains the tag and the value:
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
/// `status` is the quality of the reading and is always present. A failed
/// sensor sends a `Bad...` status with a `null` value. Use a `filter` to remove
/// these readings. `source_timestamp` is the time at which the device produced
/// the value. Use it to reduce or partition. The `received_at` field of the
/// envelope is the time at which kayak read the value.
///
/// Set `nodes`, `browse` or both. One of them is required. The input subscribes
/// to a node only one time, also when two settings name it.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "opcua")]
pub struct OpcuaConfig {
    /// The name of the opcua connection to subscribe on. Declare the
    /// connection in the connections file.
    #[schemars(extend("x-connection" = "opcua"))]
    pub connection: ConnectionId,
    /// The nodes to subscribe to, one entry for each node.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub nodes: Vec<OpcuaNodeConfig>,
    /// A node to browse. The input subscribes to each variable under it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub browse: Option<OpcuaBrowseConfig>,
    /// The interval at which the server can send a group of changes, in ms.
    /// The default is 1000. This value sets the longest time that a change
    /// waits.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub publish_interval_ms: Option<u64>,
    /// The interval at which the server samples each node, in ms. If you do not
    /// set it, the server samples at the publish interval. Set a smaller value
    /// to get more readings between two publishes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sampling_interval_ms: Option<u64>,
    /// The number of samples that the server keeps for one node between two
    /// publishes. The default is 1. With 1, the server sends only the latest
    /// value of a node that changes two times in one interval. Increase it,
    /// together with `sampling_interval_ms`, when you need each sample.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub queue_size: Option<u32>,
    /// The smallest change of a value that the server reports, in the units of
    /// the value. If you do not set it, the server reports each change.
    ///
    /// The server applies the deadband, so it decreases network traffic. It
    /// applies only to numeric nodes. The server reports each change of a
    /// string or a boolean.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadband: Option<f64>,
    /// The maximum number of messages in one batch. The default is 1.
    ///
    /// One publish from the server contains each node that changed in the
    /// interval. With 200 tags at 1 Hz and the default, the pipeline handles
    /// 200 batches each second. A higher value decreases this cost. The input
    /// puts only changes that are already received into a batch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_batch: Option<usize>,
}

/// Consumes JSON messages from a kafka topic.
///
/// The input skips a payload that is not JSON and writes a warning to the log.
/// The consumer connects on the first read and joins a consumer group. Kafka
/// keeps the read position of the group between restarts.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "kafka")]
pub struct KafkaConfig {
    /// The name of the kafka connection to consume from. Declare the
    /// connection in the connections file.
    #[schemars(extend("x-connection" = "kafka"))]
    pub connection: ConnectionId,
    /// The topic to consume from.
    pub topic: String,
    /// The consumer group id. Kafka keeps one read position for each group.
    /// Two pipelines in the same group divide the topic between them. Two
    /// pipelines in different groups each get all messages.
    pub group: String,
    /// The start position when the group has no committed position. `earliest`
    /// reads the topic from the start. `latest` reads only new messages. The
    /// default is `latest`.
    pub start_at: Option<KafkaStartAt>,
    /// The maximum number of messages in one batch. The default is 1.
    ///
    /// The input puts only records that are already received into a batch. It
    /// does not wait for more records, so a high value does not add latency on
    /// a quiet topic. Increase it when the consumer reads a backlog. Each batch
    /// has a fixed cost in the pipeline and in each downstream pipeline.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_batch: Option<usize>,
}

/// The position at which a new consumer group starts to read.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum KafkaStartAt {
    Earliest,
    Latest,
}

/// Sends one generated message at a fixed interval. Use it to test a pipeline
/// without a real source.
///
/// Each message contains a `value` and the `current_time` at which the input
/// sent it. The `payload` field sets the type of `value`. It can be a number
/// from a sine wave or a random sentence.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "dummy")]
pub struct DummyConfig {
    /// The time between two messages, in s.
    pub duration: u64,
    /// The type of `value` in each message. `number` is a number from a sine
    /// wave. `text` is a random sentence. The default is `number`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<DummyPayload>,
    /// The peak of the sine wave. The value goes from `-amplitude` to
    /// `+amplitude`. Applies only to the `number` payload. The default is 1.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub amplitude: Option<f64>,
    /// The time for one full cycle of the sine wave, in s. Applies only to the
    /// `number` payload. The default is 60. The input samples the wave by the
    /// clock, so the period does not change with `duration`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub period: Option<f64>,
}

/// The type of `value` in each message from a `dummy` input.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DummyPayload {
    /// A number from a sine wave.
    #[default]
    Number,
    /// A random sentence.
    Text,
}

/// Accepts messages that are posted to the endpoint of the pipeline,
/// `POST /api/pipelines/{id}/messages`.
///
/// kayak makes the endpoint from the pipeline id. The endpoint is available
/// when the pipeline runs. The body is one JSON message or an array of
/// messages. An array becomes one batch. A pipeline can have only one `http`
/// input. A second `http` input fails to build.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "http")]
pub struct HttpInputConfig {
    /// The maximum number of posted batches in the queue before the pipeline.
    /// The default is 1024. When the queue is full, the endpoint refuses a
    /// post with `503`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capacity: Option<usize>,
    /// The credential that a post must have. If you do not set it, the
    /// endpoint accepts all posts. A post without the correct credential gets
    /// `401`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<HttpAuthConfig>,
}

/// A credential in a header. The `http` input checks it on each post. The
/// `http` output, the `http` transform and the `http_poll` input send it on
/// each request.
///
/// This credential is for one pipeline only. It is not related to the user
/// accounts of the server.
///
/// The sender sends the same token on each request. Thus the token is only as
/// secure as the connection. kayak serves plain HTTP. Put TLS in front of
/// kayak, or other systems on the network path can read the token.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum HttpAuthConfig {
    /// A token in the standard `Authorization` header, as
    /// `Authorization: Bearer TOKEN`. Use this variant if the other system can
    /// use that header.
    Bearer {
        /// The token. Use a `${NAME}` reference, so that the config file keeps
        /// only the name and the secret store keeps the value.
        token: Secret,
    },
    /// A fixed value in a header that you name. Use this variant for a system
    /// that cannot use the `Authorization` header.
    Header {
        /// The name of the header. The `http` input compares the name without
        /// case. On an `http` input, the name must not be a header that an
        /// `envelope` copies into the messages.
        name: String,
        /// The exact value of the header. Use a `${NAME}` reference.
        value: Secret,
    },
}

/// Reads the output of another pipeline. Use it to connect pipelines into a
/// graph.
///
/// Many pipelines can read from the same upstream. Each of them gets all
/// batches. The upstream must exist when kayak creates this pipeline, so
/// declare the upstream first in the config file.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "pipeline")]
pub struct PipelineConfig {
    /// The id of the pipeline to read from.
    #[schemars(extend("x-pipeline-id" = true))]
    pub upstream: PipelineId,
}

/// The mqtt quality of service for a subscribe or a publish.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MqttQos {
    /// QoS 0. The broker does not send a message again and there is no
    /// acknowledgement. The default.
    AtMostOnce,
    /// QoS 1. The broker sends a message again until it gets an
    /// acknowledgement, so a message can arrive more than one time. An input
    /// with `ack: on_delivery` requires this level or higher.
    AtLeastOnce,
    /// QoS 2. A four-part handshake makes sure of exactly one delivery. This
    /// level has the highest cost. Use `at_least_once` if a duplicate message
    /// is not a problem.
    ExactlyOnce,
}

/// Subscribes to an mqtt topic and parses each message as JSON.
///
/// The topic can be a filter with the mqtt wildcards `+` and `#`. The input
/// skips a payload that is not JSON and writes a warning to the log.
///
/// The input opens the connection on the first read. kayak makes the client id
/// from the pipeline id and the topic. You cannot set the client id.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "mqtt")]
pub struct MqttConfig {
    /// The name of the mqtt connection to subscribe on. Declare the connection
    /// in the connections file.
    #[schemars(extend("x-connection" = "mqtt"))]
    pub connection: ConnectionId,
    /// The topic or topic filter to subscribe to.
    pub topic: String,
    /// The quality of service for the subscription. The default is
    /// `at_most_once`. `ack: on_delivery` requires `at_least_once` or
    /// `exactly_once`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub qos: Option<MqttQos>,
    /// The maximum number of messages in one batch. The default is 1.
    ///
    /// The input puts only messages that are already received into a batch.
    /// It does not wait for more messages, so a high value does not add
    /// latency on a quiet topic.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_batch: Option<usize>,
}

/// The layout of the messages in a file.
///
/// Both formats are JSON. Use `ndjson` for a stream. An `ndjson` file is valid
/// after each batch, so you can read it while the pipeline runs or after a
/// crash.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum FileFormat {
    /// One JSON message on each line. The output adds each message when it
    /// arrives.
    #[default]
    Ndjson,
    /// The file is one JSON array. The output closes the array when the file
    /// rotates.
    JsonArray,
}

/// When the output closes a file and starts the next file.
///
/// The two triggers are optional. The first trigger that is reached rotates
/// the file. With no trigger, the output writes one file while the pipeline
/// runs. The `file` and `s3` outputs use the same rotation settings.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct RotationConfig {
    /// Close the file when it contains this number of messages. The output
    /// does not divide a batch, so a file can contain more messages.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_rows: Option<usize>,
    /// Close the file this number of seconds after the output opened it. The
    /// time starts when the file opens, not at the last write.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval_secs: Option<u64>,
}

/// Writes each batch to files in a directory on the server.
///
/// A `file` connection gives the root directory, and `path` is relative to it.
/// The root must be inside the `--data-dir` of the server. Without that flag,
/// a `file` output fails to build. The output names each file
/// `<open time>-<sequence>.<ext>`, so the names sort by time and are unique.
///
/// Use the `file` output for local development and tests.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "file")]
pub struct FileOutputConfig {
    /// The name of the file connection to write under. Declare the connection
    /// in the connections file. The connection gives the root directory.
    #[schemars(extend("x-connection" = "file"))]
    pub connection: ConnectionId,
    /// The directory to write into, relative to the root of the connection,
    /// for example `orders`. The path must stay inside the root. kayak refuses
    /// an absolute path and a path that contains `..`.
    pub path: String,
    /// The layout of the messages. The default is `ndjson`.
    // omitted rather than written as `null` when absent, so a config saved back
    // out is the file someone hand-wrote — same rule as a postgres port
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format: Option<FileFormat>,
    /// When to close a file and start the next file. Without this setting, the
    /// output writes one file while the pipeline runs.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rotate: Option<RotationConfig>,
}

/// Writes each batch to objects under a prefix in an S3-compatible bucket.
///
/// The `s3` output uses the same file names, formats and rotation as the
/// `file` output. An object store cannot append to an object. Thus the output
/// keeps the current object in memory and uploads it when it rotates.
/// `rotate` is required. It sets how frequently objects appear and how much
/// memory the pipeline uses.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "s3")]
pub struct S3OutputConfig {
    /// The name of the s3 connection to write through. Declare the connection
    /// in the connections file. The connection gives the bucket and the
    /// credentials.
    #[schemars(extend("x-connection" = "s3"))]
    pub connection: ConnectionId,
    /// The key prefix to write under, for example `orders`. The output writes
    /// each object to `<prefix>/<part name>`. Set an empty prefix to write at
    /// the root of the bucket.
    pub prefix: String,
    /// The layout of the messages. The default is `ndjson`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub format: Option<FileFormat>,
    /// When to close an object and start the next object. Required. A
    /// `rotate` with no trigger fails to build.
    pub rotate: RotationConfig,
}

/// Publishes each message in the batch to a nats subject, one message for each
/// publish.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "nats")]
pub struct NatsOutputConfig {
    /// The name of the nats connection to publish on. Declare the connection
    /// in the connections file.
    #[schemars(extend("x-connection" = "nats"))]
    pub connection: ConnectionId,
    /// The subject to publish to.
    pub subject: String,
}

/// Publishes each message in the batch to a redis channel, one message for
/// each publish.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "redis")]
pub struct RedisOutputConfig {
    /// The name of the redis connection to publish on. Declare the connection
    /// in the connections file.
    #[schemars(extend("x-connection" = "redis"))]
    pub connection: ConnectionId,
    /// The channel to publish to.
    pub channel: String,
}

/// Reads live sensors and streams from Indu Cloud through `/api/v1/live/sse`,
/// with the API key of the connection.
///
/// Name sensors and streams with the ids that the platform uses. Do not use
/// UUIDs. The input finds the names through `/api/v1` on the first read. If
/// the key cannot find or see a name, the input reports an error. It then
/// tries again after a pause.
///
/// Each reading is one message, for example
/// `{"kind": "sensor", "name": "press-3/temperature", "value": 71.2, "at": …}`.
/// The message also contains the ids of the platform. When the connection
/// drops, the input connects again with backoff. If the input cannot read
/// all readings, it reports an error.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "indu")]
pub struct InduInputConfig {
    /// The name of the indu connection to read through. Declare the connection
    /// in the connections file.
    #[schemars(extend("x-connection" = "indu"))]
    pub connection: ConnectionId,
    /// The sensors to read, as `<device>/<sensor>`, for example
    /// `press-3/temperature`. Use the ids that the platform uses. kayak divides
    /// the name at the first `/`.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub sensors: Vec<String>,
    /// The streams to read, by the name they were written under, for example
    /// `press-3/oee`. For a stream that the platform calculates, use its
    /// display name.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub streams: Vec<String>,
    /// Send the latest value of each series before the live readings. The
    /// default is true. Thus a restarted pipeline has a value for each series
    /// immediately.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub backfill: Option<bool>,
    /// The maximum number of readings in one batch. The default is 1. The input
    /// puts only readings that are already received into a batch. It does not
    /// wait for more readings.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_batch: Option<usize>,
}

/// One series that an `indu` output writes: the stream that gets the value,
/// and the field that contains the value.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct InduSeries {
    /// The name of the stream in Indu, for example `press-3/oee`. The name can
    /// contain `{field}` placeholders that the output fills from the message,
    /// for example `{machine}/oee`. Thus one output can write a stream for each
    /// machine. The output skips this series for a message that does not have
    /// the field of a placeholder.
    pub stream: String,
    /// The field that contains the value, as a path (`oee`, `stats.mean`). The
    /// value must be a number. The output skips this series for a message
    /// where the value is missing or is not a number. The batch does not fail.
    pub value: String,
    /// The unit that Indu records when it creates the stream, for example `%`.
    /// Indu ignores it when the stream exists.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unit: Option<String>,
}

/// Writes messages into Indu Cloud as streams through
/// `POST /ingest/v1/streams`. A stream is a series that is not a sensor.
///
/// Each message gives one reading for each entry in `series`. For example, a
/// reducer that sends `{machine, oee, availability}` with two series entries
/// writes two streams for each machine. Indu creates an unknown stream when
/// the key of the connection has permission to create streams. If Indu does
/// not accept all rows, the batch fails. The error contains the row errors
/// from Indu.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "indu")]
pub struct InduOutputConfig {
    /// The name of the indu connection to write through. Declare the
    /// connection in the connections file.
    #[schemars(extend("x-connection" = "indu"))]
    pub connection: ConnectionId,
    /// The streams to write, one reading for each message. At least one entry
    /// is required.
    pub series: Vec<InduSeries>,
    /// The field that contains the time of the reading, as an RFC 3339 string
    /// or as ms since the epoch. If you do not set it, the output uses the time
    /// at which it sends the batch. An `envelope` puts the receive time of an
    /// input at `_meta.received_at`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub at: Option<String>,
    /// The maximum time for one request, in s. The default is 30.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_seconds: Option<u64>,
}

/// Sends the batch to an http endpoint, for example a webhook or an ingest
/// API.
///
/// The output ignores the body of the reply. A status other than 2xx fails the
/// batch. The error contains the reply of the endpoint. Use the `http`
/// transform if the pipeline needs the reply.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "http")]
pub struct HttpOutputConfig {
    /// The endpoint to send to, for example
    /// `https://example.com/hooks/readings`.
    pub url: String,
    /// The http method. The default is `POST`. `GET` and `DELETE` fail to
    /// build, because a request with no body cannot send the messages.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub verb: Option<HttpVerb>,
    /// The content of one request. The default is `batch`, one request for
    /// each batch.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<HttpBodyKind>,
    /// The credential that the output sends. If you do not set it, the output
    /// sends no credential.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<HttpAuthConfig>,
    /// The maximum time for one request, in s. The default is 30. A request
    /// that times out fails the batch. Thus a slow endpoint stops the pipeline
    /// for this time at most.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_seconds: Option<u64>,
}

/// The body of one request from an `http` output or an `http` transform.
///
/// Select the value that the API at the endpoint expects. Use `batch` for an
/// endpoint that takes an array. Use `message` for a webhook that takes one
/// event for each call.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HttpBodyKind {
    /// The full batch as one JSON array, in one request. The default.
    #[default]
    Batch,
    /// One request for each message. The body is the message. The requests go
    /// in sequence. The first failure fails the batch, and kayak does not send
    /// the messages after it.
    Message,
}

/// Publishes each message in the batch to an mqtt topic, one message for each
/// publish.
///
/// kayak makes the client id from the pipeline id and the topic. You cannot
/// set the client id.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "mqtt")]
pub struct MqttOutputConfig {
    /// The name of the mqtt connection to publish on. Declare the connection
    /// in the connections file.
    #[schemars(extend("x-connection" = "mqtt"))]
    pub connection: ConnectionId,
    /// The topic to publish to.
    pub topic: String,
    /// The quality of service for each publish. The default is `at_most_once`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub qos: Option<MqttQos>,
    /// Tell the broker to keep the message as the retained message of the
    /// topic. The broker sends it to each new subscriber. The default is false.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retain: Option<bool>,
}

/// Inserts each message in the batch into a postgres table, one row for each
/// message.
///
/// With `columns`, each entry names a column, its type and the field to read,
/// for example `{"name": "temperature", "type": "float", "field": "reading.temp_c"}`.
/// The default `field` is the name of the column. Without `columns`, the table
/// has an `id`, a `received_at` timestamp and a `payload` column of type
/// `jsonb` that contains the full message.
///
/// The output creates the table if it does not exist. Set `create_table` to
/// false for a table that another system owns. The output does not change an
/// existing table. If the table does not agree with the columns, the insert
/// fails with the error from postgres.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "postgres")]
pub struct PostgresOutputConfig {
    /// The name of the postgres connection to insert through. Declare the
    /// connection in the connections file. The connection gives the host, the
    /// database and the role.
    #[schemars(extend("x-connection" = "postgres"))]
    pub connection: ConnectionId,
    /// The table to insert into. The output creates it if it does not exist.
    /// You can add a schema (`analytics.readings`). Use only letters, digits
    /// and underscores.
    pub table: String,
    /// The column for each message field. If you do not set it, the output
    /// keeps each full message as JSON in a `payload` column.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<ColumnMapping>,
    /// Create the table on connect if it does not exist. The default is true.
    // omitted rather than written as `null` when absent, so a config saved back
    // out is the file someone hand-wrote — same rule as the port
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub create_table: Option<bool>,
    /// The columns of the primary key of the created table. If you do not set
    /// it, the table gets an `id` and a `received_at` timestamp. If you set it,
    /// the table does not get these two columns.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub primary_key: Vec<String>,
    /// The indexes to create with the table. Each index names mapped columns,
    /// in sequence.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub indexes: Vec<TableIndex>,
    /// What to do with a message that has fields that no column reads.
    #[serde(default, skip_serializing_if = "ExtraFieldPolicy::is_default")]
    pub on_extra_fields: ExtraFieldPolicy,
}

/// Inserts each batch into a `ClickHouse` table, one insert for each batch.
///
/// `columns` has the same format as on the `postgres` output. Each entry names
/// a column, its type and the field to read. The default `field` is the name of
/// the column. Without `columns`, the table has a `payload` column that
/// contains each message as JSON text.
///
/// `ClickHouse` has no auto-increment column and no unique constraint.
/// `order_by` names the sorting key of the `MergeTree` table. If you do not set
/// it, the table gets a `received_at` timestamp and is sorted by it. A sorting
/// key does not remove duplicate rows.
///
/// The output creates the table if it does not exist. Set `create_table` to
/// false for a table that another system owns. The output does not change an
/// existing table.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "clickhouse")]
pub struct ClickhouseOutputConfig {
    /// The name of the clickhouse connection to insert through. Declare the
    /// connection in the connections file. The connection gives the url, the
    /// database and the user.
    #[schemars(extend("x-connection" = "clickhouse"))]
    pub connection: ConnectionId,
    /// The table to insert into. The output creates it if it does not exist.
    /// You can add a database (`analytics.readings`). This database replaces
    /// the database of the connection. Use only letters, digits and
    /// underscores.
    pub table: String,
    /// The column for each message field. If you do not set it, the output
    /// keeps each full message as JSON text in a `payload` column.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<ColumnMapping>,
    /// Create the table on start if it does not exist. The default is true.
    // omitted rather than written as `null` when absent, so a config saved back
    // out is the file someone hand-wrote — same rule as the postgres port
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub create_table: Option<bool>,
    /// The columns that sort the created table. This is the sorting key of the
    /// `MergeTree` table and its index. If you do not set it, the table gets a
    /// `received_at` timestamp and is sorted by it. The output makes these
    /// columns `NOT NULL`, because `ClickHouse` cannot sort by a nullable key.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub order_by: Vec<String>,
    /// What to do with a message that has fields that no column reads.
    #[serde(default, skip_serializing_if = "ExtraFieldPolicy::is_default")]
    pub on_extra_fields: ExtraFieldPolicy,
}

/// Writes each batch into a Tidepool table, one request for each batch.
///
/// The table must exist in the Tidepool project. On start, the output checks
/// the columns against the table. Each mapped column must be in the table and
/// have a type that the mapping can write. Each required column of the table
/// must be written. If the check fails, the start fails.
///
/// `columns` has the same format as on the database outputs. If you do not set
/// it, the output sends each message as a row without changes. Tidepool checks
/// each value and refuses a batch that has a problem.
///
/// A refused batch fails. The error gives the problems by row and column. The
/// output tries again when the server is busy (`503`), when the server cannot
/// be reached and on other `5xx` errors. It tries again for up to
/// `retry_seconds`. Each try uses the same idempotency key, so Tidepool does
/// not write a batch two times.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "tidepool")]
pub struct TidepoolOutputConfig {
    /// The name of the tidepool connection to write through. Declare the
    /// connection in the connections file.
    #[schemars(extend("x-connection" = "tidepool"))]
    pub connection: ConnectionId,
    /// The table to write into, with the name from the Tidepool project.
    pub table: String,
    /// The column for each message field. If you do not set it, the output
    /// sends each message as a row without changes.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub columns: Vec<ColumnMapping>,
    /// What to do with a message that has fields that no column reads.
    #[serde(default, skip_serializing_if = "ExtraFieldPolicy::is_default")]
    pub on_extra_fields: ExtraFieldPolicy,
    /// The maximum time to try one batch again while the server is busy or
    /// cannot be reached, in s. The default is 30.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retry_seconds: Option<u64>,
    /// The maximum time for one request, in s. The default is 30.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_seconds: Option<u64>,
}

/// Publishes each message in the batch to a kafka topic, one record for each
/// message.
///
/// The records have no key, so kafka distributes them across the partitions
/// of the topic.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "kafka")]
pub struct KafkaOutputConfig {
    /// The name of the kafka connection to publish to. Declare the connection
    /// in the connections file.
    #[schemars(extend("x-connection" = "kafka"))]
    pub connection: ConnectionId,
    /// The topic to publish to.
    pub topic: String,
}

/// Prints each batch to the standard output of the server. Use it to test a
/// pipeline. It has no settings.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "stdout")]
pub struct StdoutOutputConfig {}

/// Keeps messages and sends them on when a trigger fires.
///
/// There are three triggers: a message count, a time, and a condition on a
/// state bucket. You can use them together. The first trigger that fires
/// releases the messages. A buffer with no trigger fails to build.
///
/// `size` sends batches of exactly that number of messages. `seconds` and
/// `until` send all messages that the buffer keeps, as one batch.
///
/// The `buffer` setting on an input is a different thing. It makes batches
/// before the transforms. The `buffer` transform makes batches at its
/// position in the chain, for example after a `filter` or a `recall`.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[schemars(title = "buffer")]
pub struct BufferTransformConfig {
    /// Send batches of exactly this number of messages, when each batch is
    /// full.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub size: Option<usize>,

    /// Send all kept messages this number of seconds after the first kept
    /// message. The time starts when the buffer keeps a message. An empty
    /// buffer sends nothing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub seconds: Option<usize>,

    /// Send all kept messages when a condition on a state bucket is true.
    /// Buckets are global, so a different pipeline can write the value that
    /// opens the gate.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub until: Option<BufferGateConfig>,

    /// The maximum number of messages to keep. At this number, the buffer sends
    /// all kept messages and writes one warning to the log. Required if `size`
    /// is not set. Without a limit, a condition that is never true makes the
    /// buffer use more and more memory.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_messages: Option<usize>,
}

/// A condition on a state bucket that releases a `buffer` transform.
///
/// The buffer tests the conditions against the bucket entry as an object. The
/// names that `remember` wrote are its fields. `field` is a dotted path, as in
/// all other transforms. All conditions must be true.
///
/// The gate applies to the full buffer. It does not test each kept message.
/// When the gate opens, the buffer sends all kept messages.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[schemars(title = "buffer gate")]
pub struct BufferGateConfig {
    /// The bucket to watch. The default is the bucket in the `state` of this
    /// pipeline. A pipeline with no `state` must set it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub bucket: Option<String>,

    /// The key in the bucket to read. This is a literal key, not a field path.
    /// If you do not set it, the gate reads the value for the full bucket.
    /// `remember` writes that value when the `state` of its pipeline has no
    /// `key`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,

    /// The conditions that must all be true to release the buffer. At least
    /// one condition is required.
    pub conditions: Vec<Condition>,
}

/// How a `numeric` condition compares a number to the value in the config.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum NumericFilterOperatorKind {
    GreaterThan,
    LessThan,
    EqualTo,
    NotEqualTo,
}

/// How a `string` condition compares a string to the value in the config.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StringFilterOperatorKind {
    EqualTo,
    NotEqualTo,
    Contains,
}

/// The spelling `filter` had before it took a list of [`Condition`]s: one
/// comparison, externally tagged by its kind (`{"Numeric": {...}}`). Still
/// read, so a config written then keeps loading; never written, since a
/// filter is saved as its `conditions`.
#[derive(Clone, Debug, Deserialize)]
pub enum FilterKind {
    Numeric {
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

impl From<FilterKind> for Condition {
    fn from(kind: FilterKind) -> Self {
        match kind {
            FilterKind::Numeric {
                field,
                operator,
                value,
            } => Condition::Numeric {
                field,
                operator,
                value,
            },
            FilterKind::String {
                field,
                operator,
                value,
            } => Condition::String {
                field,
                operator,
                value,
            },
        }
    }
}

/// Keeps the messages that match all `conditions` and drops the other
/// messages. With `invert`, it drops the messages that match and keeps the
/// other messages.
///
/// The transform drops a batch that has no messages left. A message that does
/// not have the field of a condition does not match that condition. A message
/// with a field of the wrong type also does not match. Thus `invert` keeps
/// these messages.
#[derive(Clone, Debug, Serialize, JsonSchema, PartialEq)]
#[schemars(title = "filter")]
pub struct FilterTransformConfig {
    /// The conditions that a message must match. At least one condition is
    /// required.
    pub conditions: Vec<Condition>,
    /// Drop the messages that match, and keep the other messages. The default
    /// is false.
    #[serde(default, skip_serializing_if = "is_false")]
    pub invert: bool,
}

#[allow(clippy::trivially_copy_pass_by_ref)] // serde's `skip_serializing_if` hands a reference
fn is_false(value: &bool) -> bool {
    !*value
}

impl<'de> Deserialize<'de> for FilterTransformConfig {
    /// The current spelling, or the single-comparison one it replaced — told
    /// apart by the `Numeric`/`String` key the old one hangs its fields off.
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        #[derive(Deserialize)]
        struct Current {
            conditions: Vec<Condition>,
            #[serde(default)]
            invert: bool,
        }

        let value = serde_json::Value::deserialize(deserializer)?;
        let legacy = value
            .as_object()
            .is_some_and(|object| object.contains_key("Numeric") || object.contains_key("String"));
        if legacy {
            let kind = FilterKind::deserialize(value).map_err(serde::de::Error::custom)?;
            return Ok(Self {
                conditions: vec![kind.into()],
                invert: false,
            });
        }
        let current = Current::deserialize(value).map_err(serde::de::Error::custom)?;
        Ok(Self {
            conditions: current.conditions,
            invert: current.invert,
        })
    }
}

/// Sends the batch to an http endpoint and continues with the reply. Use it
/// to call a model or another service.
///
/// `body` sets the content of one request: the full batch as a JSON array, or
/// one message. `wrap` puts the body under a key, for example
/// `{"instances": …}`. `response` sets what the transform does with the reply.
/// `replace` makes the reply the new batch. `merge` writes the reply onto the
/// message under `as`. `unwrap` reads the reply from under a key first.
///
/// A status other than 2xx fails the batch. The error contains the reply of the
/// endpoint. A network failure, a 5xx or a 429 is tried again `retries` times
/// with backoff.
///
/// For a model, put a `buffer` and a `features` transform before this
/// transform. Use `response: merge` to keep the identifiers of the message.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "http")]
pub struct HttpTransformConfig {
    /// The endpoint to send to.
    pub url: String,
    /// The http method. `GET` and `DELETE` fail to build, because a request
    /// with no body cannot send the messages.
    pub verb: HttpVerb,
    /// The content of one request. The default is `batch`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub body: Option<HttpBodyKind>,
    /// A key to put the body under, for an API that expects `{"key": …}`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wrap: Option<String>,
    /// What to do with the reply. The default is `replace`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub response: Option<HttpResponseKind>,
    /// A key to read the reply from, for an API that replies with
    /// `{"predictions": …}`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub unwrap: Option<String>,
    /// The field to write the reply under. Applies to `response: merge`.
    #[serde(default, rename = "as", skip_serializing_if = "Option::is_none")]
    pub output: Option<String>,
    /// The credential that the transform sends. If you do not set it, the
    /// transform sends no credential.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<HttpAuthConfig>,
    /// The maximum time for one request, in s. The default is 30.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_seconds: Option<u64>,
    /// The number of times to send a request again before the batch fails.
    /// Applies to a network failure, a 5xx and a 429. The default is 0. Each
    /// try waits longer than the previous try, and the pipeline waits too.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retries: Option<u32>,
}

/// What an `http` transform does with the reply.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum HttpResponseKind {
    /// The reply becomes the new batch. With `body: batch`, the reply must be a
    /// JSON array of messages. With `body: message`, the reply is a message or
    /// an array of messages. The default.
    #[default]
    Replace,
    /// The transform writes the reply onto the message that caused it, under
    /// `as`. With `body: batch`, an array reply with one entry for each message
    /// goes to the messages in sequence. Any other reply goes onto each
    /// message. The messages keep all their fields.
    Merge,
}

/// The http method of a request.
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
/// How a reducer combines the values of one field into one result.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReduceFnKind {
    /// The total. Numbers only.
    Sum,
    /// The arithmetic mean. Numbers only.
    Avg,
    /// The smallest value. Numbers compare as numbers. Strings compare in
    /// alphabetical sequence, so `min` of ISO timestamps is the earliest time.
    Min,
    /// The largest value. Values compare as for `min`.
    Max,
    /// The number of messages. This function does not need a `field`. With a
    /// `field`, it counts the messages that have the field.
    Count,
    /// The number of different values. The function compares the values as
    /// JSON.
    CountDistinct,
    /// The value from the first message of the group, of any type.
    First,
    /// The value from the last message of the group.
    Last,
    /// All values as an array, in the sequence of arrival.
    Collect,
    /// The middle value, or the mean of the two middle values. Numbers only.
    Median,
    /// The population standard deviation. Numbers only.
    Stddev,
    /// The rate of change of the field per second, from a least-squares line
    /// against the time of each message. Numbers only. The reducer must have a
    /// `time` setting, or it fails to build.
    Slope,
}

/// What to do with a message that does not have a field that is aggregated or
/// grouped by. A field with the value `null` is missing.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MissingFieldPolicy {
    /// Fail the batch. The default. A sum of only some of the messages gives a
    /// wrong result that is not visible downstream.
    #[default]
    Error,
    /// Do not use that message in that aggregation. An aggregation with no
    /// values gives `null`, or `0` for the counts.
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

/// One value to calculate for a group, and the field name for the result.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[schemars(title = "aggregation")]
pub struct Aggregation {
    /// How to combine the values.
    pub function: ReduceFnKind,
    /// The field that contains the result in the sent message. Each
    /// aggregation must have a different `as`. It must not be the same as a
    /// `group_by` field.
    #[serde(rename = "as")]
    pub output: String,
    /// The field to aggregate. Required for all functions except `count`.
    /// Without a `field`, `count` counts the messages.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(extend("x-message-field" = true))]
    pub field: Option<String>,
}

/// Reduces a batch to one message for each group. Put a buffer before it, or
/// it gets only one message at a time.
///
/// Without `group_by`, the full batch is one group and the reducer sends one
/// message. With `group_by`, it sends one message for each different
/// combination of those fields. The messages are in the sequence in which the
/// groups first occur. Each message contains the `group_by` fields and the
/// results of the aggregations.
///
/// Each aggregation has a `function`, the `field` to use and the name `as` for
/// the result, for example
/// `{"function": "avg", "field": "value", "as": "mean"}`.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "reducer")]
pub struct ReduceTransformConfig {
    /// The values to calculate. At least one aggregation is required. Each one
    /// must have a different `as`.
    pub aggregations: Vec<Aggregation>,
    /// The fields whose combination defines a group. If you do not set it, the
    /// full batch is one group.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub group_by: Vec<String>,
    /// What to do with a message that does not have one of the fields. The
    /// default is `error`.
    #[serde(default, skip_serializing_if = "MissingFieldPolicy::is_default")]
    pub on_missing: MissingFieldPolicy,
    /// The field that contains the time of each message, as an RFC 3339 string
    /// or as ms since the epoch. `slope` requires it. A message without this
    /// field fails the batch. If you do not set it, the time of each message is
    /// its arrival time.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(extend("x-message-field" = true))]
    pub time: Option<String>,
}

/// One test on a message.
///
/// `filter`, `remember` and the gate of a `buffer` use conditions. A list of
/// conditions means that all of them must match. There is no `or` and no
/// nesting. Use `invert` on a `filter`, or `none_of`, for a negative test.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Condition {
    /// Compares a field to a number. A message whose field is missing or is not
    /// a number does not match.
    Numeric {
        /// The field to test, as a dotted path.
        #[schemars(extend("x-message-field" = true))]
        field: String,
        /// How to compare the field.
        operator: NumericFilterOperatorKind,
        /// The number to compare to.
        value: f64,
    },
    /// Compares a field to a string. A message whose field is missing or is not
    /// a string does not match.
    String {
        /// The field to test, as a dotted path.
        #[schemars(extend("x-message-field" = true))]
        field: String,
        /// How to compare the field.
        operator: StringFilterOperatorKind,
        /// The string to compare to.
        value: String,
    },
    /// Matches when the field is a string equal to one of `values`. A message
    /// whose field is missing or is not a string does not match.
    OneOf {
        /// The field to test, as a dotted path.
        #[schemars(extend("x-message-field" = true))]
        field: String,
        /// The strings that match.
        values: Vec<String>,
    },
    /// Matches when the field is a string equal to none of `values`. A message
    /// whose field is missing or is not a string also does not match.
    NoneOf {
        /// The field to test, as a dotted path.
        #[schemars(extend("x-message-field" = true))]
        field: String,
        /// The strings that do not match.
        values: Vec<String>,
    },
}

/// One value to write into the state bucket of the pipeline, and its name
/// there.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
pub struct Remembered {
    /// The field to take the value from.
    pub field: String,
    /// The name for the value in the bucket. `recall` reads the value by this
    /// name. Each entry must have a different name.
    #[serde(rename = "as")]
    pub output: String,
}

/// Writes values from the messages that match into the state bucket of the
/// pipeline. The key is the field that `state.key` of the pipeline names.
///
/// The transform sends each message on without changes. It does not filter.
///
/// The pipeline must have a `state`, or the transform fails to build.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[schemars(title = "remember")]
pub struct RememberTransformConfig {
    /// The conditions that a message must match to be remembered. All of them
    /// must match. If you do not set it, the transform remembers values from
    /// each message. Set it when the stream contains more than one type of
    /// message.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub when: Vec<Condition>,
    /// The values to take from a message that matches. At least one entry is
    /// required. Each entry must have a different `as`.
    pub remember: Vec<Remembered>,
}

/// Writes values from the state bucket of the pipeline onto each message,
/// with the names from `remember`.
///
/// Use it to add a slow fact to a fast stream, for example the current recipe
/// of a machine. The values become top-level fields, so a `reducer` after it
/// can group by them.
///
/// The pipeline must have a `state`, or the transform fails to build.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[schemars(title = "recall")]
pub struct RecallTransformConfig {
    /// The names to read from the bucket, as `remember` wrote them. The
    /// transform writes each value onto the message with the same name.
    pub recall: Vec<String>,
    /// What to do with a message when the bucket has no value for its key.
    /// The default is `skip`.
    #[serde(default, skip_serializing_if = "RecallMissingPolicy::is_default")]
    pub on_missing: RecallMissingPolicy,
}

/// What `recall` does when the bucket has no value for the key of a message.
///
/// The default is `skip`, because the bucket is empty when the pipeline
/// starts. With `error`, the batches fail until the bucket has values.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RecallMissingPolicy {
    /// Drop the message. The default. Without the recalled values, a reducer
    /// after this transform puts all these messages into one wrong group.
    #[default]
    Skip,
    /// Send the message on with the missing names as `null`.
    Null,
    /// Fail the batch. Use it only when another component always fills the
    /// bucket first.
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

/// Divides one batch into smaller batches.
///
/// The last batch contains the messages that remain and can be smaller. For
/// example, 4 messages with `out_size: 3` give a batch of 3 and a batch of 1.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "splitter")]
pub struct SplitterTransformConfig {
    /// The number of messages in each sent batch.
    pub out_size: usize,
}

/// Reads a postgres table, view or query at an interval and sends each row as
/// a message.
///
/// The input runs a query each `interval_secs`. In `snapshot` mode, it reads
/// the full relation. In `incremental` mode, it reads only the rows after the
/// last read, by a column that increases. Each row becomes one JSON object
/// with the column names as fields. Postgres makes the object with
/// `row_to_json`. A timestamp is ISO 8601, a `numeric` keeps its digits, and
/// a `jsonb` column is a nested value:
///
/// ```json
/// {"id": 42, "sensor": "press-3", "value": 21.5, "recorded_at": "2026-01-01T12:00:00.123456+00:00"}
/// ```
///
/// An incremental read is at-least-once across a restart. The input keeps the
/// watermark in memory, and after a restart it starts again from `start_from`.
/// An incremental read does not see a deleted row. Put an index on the column
/// that it follows, or each read scans the full table.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "postgres")]
pub struct PostgresInputConfig {
    /// The name of the postgres connection to read through. Declare the
    /// connection in the connections file.
    #[schemars(extend("x-connection" = "postgres"))]
    pub connection: ConnectionId,
    #[serde(flatten)]
    pub poll: SqlPollConfig,
}

/// Reads a `ClickHouse` table, view or query at an interval and sends each row
/// as a message. It uses the HTTP interface of `ClickHouse`.
///
/// `ClickHouse` sends the rows as `JSONEachRow`. A `DateTime` is ISO 8601, an
/// `Int64` is a number, and a `Decimal` keeps its digits. The modes, the
/// watermark and the limits of an incremental read are the same as on the
/// `postgres` input.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "clickhouse")]
pub struct ClickhouseInputConfig {
    /// The name of the clickhouse connection to read through. Declare the
    /// connection in the connections file.
    #[schemars(extend("x-connection" = "clickhouse"))]
    pub connection: ConnectionId,
    #[serde(flatten)]
    pub poll: SqlPollConfig,
}

/// Gets a url at an interval and sends the full reply each time.
///
/// Each read is a `GET`. A reply that is an array gives one message for each
/// element. Any other reply gives one message. Use `items` for a reply that
/// has the records inside it, for example `{"data": {"machines": [...]}}`.
///
/// Use this input for reference data, for example a list of machines or
/// recipes that changes rarely. The input has no watermark and no pages. Each
/// read sends all records again. Send them to an output that writes the latest
/// value for each key.
///
/// A read fails when the url cannot be reached, when the status is not 2xx,
/// when the body is not JSON, or when `items` finds nothing. The input reports
/// the failure one time and tries again with backoff. The interval starts
/// again after the next successful read.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "http_poll")]
pub struct HttpPollConfig {
    /// The url to get, for example `https://erp.example.com/api/machines`.
    pub url: String,
    /// The time between two reads, in s. The time starts at the end of one
    /// read. The first read occurs when the pipeline starts.
    pub interval_secs: u64,
    /// The position of the records in the reply, as a JSON pointer. For
    /// example, `/data/machines` reads the array at `data.machines`. If you do
    /// not set it, the input uses the full reply. An array gives one message
    /// for each element. Any other value gives one message.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub items: Option<String>,
    /// The credential that the input sends. If you do not set it, the input
    /// sends no credential.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auth: Option<HttpAuthConfig>,
    /// The maximum time for one request, in s. The default is 30.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_seconds: Option<u64>,
    /// The maximum number of messages in one batch. The default is 1. The input
    /// puts only messages that are already read into a batch. It does not wait
    /// for a batch to fill.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_batch: Option<usize>,
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
    Postgres(PostgresInputConfig),
    Clickhouse(ClickhouseInputConfig),
    HttpPoll(HttpPollConfig),
}
/// How an input collects its messages into batches before the transforms.
///
/// The three types use two limits: a count, a time, or both. With both, the
/// first limit that is reached closes the batch. A buffer never sends an empty
/// batch. The time starts when the first message of a batch arrives, so a
/// quiet input sends nothing.
///
/// `size` is a minimum, not a maximum. The buffer does not divide a batch
/// that arrives. Thus an input with `max_batch` can give a larger batch.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum BufferConfig {
    /// Wait for a number of messages. There is no time limit.
    Static {
        /// The number of messages in a batch.
        size: usize,
    },
    /// Wait for a time. The batch contains at least one message. The time
    /// starts when the first message arrives.
    Tumbling {
        /// The time to collect messages, in s, from the first message.
        window_seconds: usize,
    },
    /// Use both limits. The first limit that is reached closes the batch. Use
    /// this type when the rate of the input changes. `size` sets the largest
    /// batch when the input is busy. `window_seconds` sets the longest wait
    /// when the input is quiet.
    Batch {
        /// The number of messages that closes the batch immediately.
        size: usize,
        /// The maximum time to wait, in s, from the first message in the batch.
        window_seconds: usize,
    },
}

/// How an input adds metadata about the source of each message.
///
/// The "metadata" section of each input lists its metadata. Examples are the
/// subject of a nats message and the topic, partition and offset of a kafka
/// record. The metadata also contains the pipeline and the input type.
///
/// The input adds the metadata as ordinary fields on the message. Thus each
/// transform can use it as it uses the fields of the payload, for example
/// `"group_by": ["_meta.subject"]`.
///
/// If you do not set an envelope, the input sends each message without
/// changes. An envelope changes the shape of each message from the input.
/// Update the field paths downstream when you add one.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum EnvelopeConfig {
    /// Add the metadata as one more field on the message. The fields of the
    /// payload do not move.
    ///
    /// This type works only on a payload that is a JSON object. The input
    /// skips a message that is a number or a string and writes a warning to
    /// the log. Use `wrap` for these payloads.
    Merge {
        /// The field for the metadata object. The default is `_meta`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        meta: Option<String>,
    },
    /// Put the full payload under a field, next to the metadata:
    /// `{"value": …, "_meta": {…}}`.
    ///
    /// This type works with all payloads, for example a `1` or a `"recipe-a"`.
    /// Each field path downstream must then start with the payload field, for
    /// example `value.temperature`.
    Wrap {
        /// The field for the original payload. The default is `value`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        payload: Option<String>,
        /// The field for the metadata object. The default is `_meta`.
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

    /// Collect messages from this input into batches before the transforms.
    /// Use a count (`static`), a time (`tumbling`) or the first of the two
    /// (`batch`). The buffer never sends an empty batch. Available on all
    /// input types. This is not the `buffer` transform.
    // omitted rather than emitted as `null` when absent, so a config that comes
    // back out of `GET /api/pipelines` is byte-identical to the one that went in
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub buffer: Option<BufferConfig>,

    /// Add metadata about the source of each message, for example the subject,
    /// the topic or the partition. The "metadata" section lists the fields.
    /// Available on all input types. If you do not set it, the input sends each
    /// message without changes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub envelope: Option<EnvelopeConfig>,

    /// When the input acknowledges a message to its broker. The default is
    /// `on_receipt`. Only the `kafka` and `mqtt` inputs support `on_delivery`.
    /// The `mqtt` input requires a `qos` of `at_least_once` or higher for it.
    /// On all other inputs, `on_delivery` fails to build.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ack: Option<AckMode>,
}

/// When an input acknowledges a message to its broker.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum AckMode {
    /// Acknowledge the message when it arrives, before the transforms and the
    /// outputs. The default. A crash before the output writes the message can
    /// lose it.
    OnReceipt,
    /// Acknowledge the message when it leaves this pipeline. Each output of
    /// this pipeline must return, with or without success. Each downstream
    /// pipeline must accept the message into its queue. A failed output does
    /// not stop the acknowledgement. kayak does not wait for the outputs of the
    /// downstream pipelines.
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
    Deadband(crate::streaming::DeadbandTransformConfig),
    Throttle(crate::streaming::ThrottleTransformConfig),
    Pivot(crate::streaming::PivotTransformConfig),
    Derive(crate::streaming::DeriveTransformConfig),
    Rolling(crate::streaming::RollingTransformConfig),
    Smooth(crate::streaming::SmoothTransformConfig),
    Detect(crate::streaming::DetectTransformConfig),
    Resample(crate::streaming::ResampleTransformConfig),
    Features(crate::streaming::FeaturesTransformConfig),
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
    Tidepool(TidepoolOutputConfig),
}
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct OutputConfig {
    #[serde(flatten)]
    pub kind: OutputKind,
}

/// One pipeline. kayak merges all inputs into one stream. The stream goes
/// through the transforms in sequence. Each batch that results goes to each
/// output.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Config {
    pub id: Option<String>,
    /// The inputs of the pipeline. At least one input is required. Batches
    /// arrive in the sequence in which the inputs make them. There is no
    /// sequence between two different inputs.
    pub inputs: Vec<InputConfig>,
    /// The transforms, in sequence. Optional. A pipeline that only moves
    /// messages needs no transform.
    #[serde(default)]
    pub transforms: Vec<TransformConfig>,
    /// The outputs. Optional. A pipeline that only sends to downstream
    /// pipelines needs no output.
    #[serde(default)]
    pub outputs: Vec<OutputConfig>,
    /// The state bucket of this pipeline, and the key of its messages. Required
    /// for `remember`, `recall` and the streaming transforms that keep state,
    /// for example `rolling`. These transforms fail to build without it.
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
                | InputKind::Indu(_)
                | InputKind::Postgres(_)
                | InputKind::Clickhouse(_)
                | InputKind::HttpPoll(_) => None,
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
            InputKind::Postgres(c) => Some(&c.connection),
            InputKind::Clickhouse(c) => Some(&c.connection),
            InputKind::Dummy(_)
            | InputKind::Http(_)
            | InputKind::HttpPoll(_)
            | InputKind::Pipeline(_) => None,
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
            OutputKind::Tidepool(c) => Some(&c.connection),
            OutputKind::Stdout(_) | OutputKind::Http(_) => None,
        });
        inputs.chain(outputs).collect()
    }
}
