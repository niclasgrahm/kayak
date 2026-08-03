use crate::StreamerId;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "nats")]
pub struct NatsConfig {
    pub urls: String,
    pub subject: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "dummy")]
pub struct DummyConfig {
    pub duration: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "streamer")]
pub struct StreamerConfig {
    pub upstream: StreamerId,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "file")]
pub struct FileOutputConfig {}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "nats")]
pub struct NatsOutputConfig {
    pub urls: String,
    pub subject: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "stdout")]
pub struct StdoutOutputConfig {}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "buffer")]
pub struct BufferTransformConfig {
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
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "filter")]
pub struct FilterTransformConfig {
    #[serde(flatten)]
    pub filter: FilterKind,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "http")]
pub struct HttpTransformConfig {
    pub url: String,
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

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "reducer")]
pub struct ReduceTransformConfig {
    pub function: ReduceFnKind,
    pub field: String,
}

// takes a batch, emits each message as a separate batch
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "splitter")]
pub struct SplitterTransformConfig {
    pub out_size: usize,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum InputKind {
    Dummy(DummyConfig),
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
    Nats(NatsOutputConfig),
}
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct OutputConfig {
    #[serde(flatten)]
    pub kind: OutputKind,
}

#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
pub struct Config {
    pub id: Option<String>,
    pub input: InputConfig,
    pub transforms: Vec<TransformConfig>,
    pub output: OutputConfig,
}
