//! The `infer` transform — the declaration.
//!
//! Runs an ONNX model over the stream: kayak runs models, it does not fit
//! them, so a model is trained wherever the data scientists are, exported, and
//! named here by file. The evaluation is a crate away in
//! `kayak::transforms::infer` (tract, behind the `onnx` feature), and the split
//! is the usual one: this has to compile for wasm so the form can render it.
//!
//! Three decisions shape the declaration:
//!
//! - **`model` is a tagged union with one arm today.** A `file` beside the
//!   config, under the `file` script source's boundary rules. An object-store
//!   arm (a connection and a key) is the next one, and spelling the field as a
//!   union now is what makes adding it a compatible change.
//! - **`shape` says how messages become the tensor**, and the two arms are the
//!   two cases that exist: `rows` (each message is a row, the batch is the
//!   tensor — classifiers, isolation forests, scorers) and `window` (the last
//!   `size` readings of a series — forecasters). Which outputs make sense
//!   follows from the shape, so `emit` lives on the window arm and nowhere
//!   else.
//! - **What the model made is labelled by the config, not guessed by the
//!   consumer.** Under `emit: merge` the answer is a field on the observed
//!   message and its *name* is the label. Under `emit: points` the forecast is
//!   messages of its own, interleaved with the observed ones, and `mark` writes
//!   one field that says which is which — with `observed` optional, because a
//!   consumer that only needs to recognise forecasts should not pay for a
//!   field on every reading. `provenance` writes which model file (path and
//!   sha256) produced a result, which is what lets a forecast be audited after
//!   the model was swapped.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::config::MissingFieldPolicy;

/// Where the model file is.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ModelSource {
    /// An `.onnx` file beside the config file.
    File {
        /// The path, relative to the directory of the config file, for
        /// example `models/pump_forecast_v1.onnx`. It must end in `.onnx` and
        /// stay in that directory. kayak reads the file when it builds the
        /// pipeline. To use a changed file, replace the pipeline or revert.
        path: String,
    },
}

impl ModelSource {
    /// The path the source names, for messages and provenance.
    #[must_use]
    pub fn path(&self) -> &str {
        match self {
            Self::File { path } => path,
        }
    }
}

/// How the messages become the input tensor of the model.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
#[allow(
    clippy::large_enum_variant,
    reason = "config, read once per build; a box would only add noise to every match"
)]
pub enum InferShape {
    /// Each message is one row. The batch is a `[messages, inputs]` tensor and
    /// the model must answer `[messages, outputs]`, or `[messages]` for one
    /// output. The transform writes each answer onto its message.
    Rows,
    /// The last `size` readings of each series. The tensor is
    /// `[1, size, inputs]` and the model must answer `[1, horizon]`, or
    /// `[1, horizon, outputs]`. The model runs once the window is full, then
    /// again after each `stride` readings. Needs a state bucket on the
    /// pipeline.
    Window {
        /// The number of readings the model takes.
        size: usize,
        /// Run the model after this many new readings. The default is 1, every
        /// reading.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        stride: Option<usize>,
        /// The fields that identify a series, as in `reduce`. Leave it out for
        /// one series.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        group_by: Vec<String>,
        /// The field that holds the time of each reading, as an RFC 3339
        /// string or as milliseconds since the epoch. Leave it out to use the
        /// arrival time. The forecast points count their time from it.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        #[schemars(extend("x-message-field" = true))]
        time: Option<String>,
        /// What the transform does with the answer. The default is `merge`.
        #[serde(default, skip_serializing_if = "InferEmit::is_default")]
        emit: InferEmit,
    },
}

/// What the transform does with the answer for a window.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum InferEmit {
    /// Write the answer onto the reading that completed the window. Each
    /// output gets an array of `horizon` values. The name of the output field
    /// is what tells a consumer that the values are inferred.
    #[default]
    Merge,
    /// Send one new message per horizon step, after the reading that completed
    /// the window. Each message has the `group_by` fields, the step, the
    /// outputs as numbers and, with `step_seconds`, a time.
    Points {
        /// The seconds between two forecast steps. With it, each point gets a
        /// time: the time of the last reading plus `step × step_seconds`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        step_seconds: Option<f64>,
        /// The field for the step number, from 1 to the horizon. The default is
        /// `step`.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        step_field: Option<String>,
        /// The field for the time of a point. The default is the window's
        /// `time` field, or `time` if the window has none.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        time_field: Option<String>,
        /// Label each message as inferred or observed.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        mark: Option<InferMark>,
    },
}

impl InferEmit {
    #[must_use]
    pub fn is_default(&self) -> bool {
        *self == Self::Merge
    }
}

/// A field that tells the forecast points from the readings.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct InferMark {
    /// The field the label is written to, for example `origin`.
    pub field: String,
    /// The label of a message the model made, for example `forecast`.
    pub inferred: String,
    /// The label of every other message that leaves the transform, for example
    /// `observed`. Leave it out to label only the forecast points.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub observed: Option<String>,
}

/// Runs an ONNX model on the stream, in process.
///
/// Give the model as a `.onnx` file beside the config. `inputs` names the
/// message fields that make the input tensor, in the sequence the model
/// expects them. `outputs` names the fields the answer goes to, one per column
/// of the model's output. The model must have one input of type `float` and
/// kayak reads its first output.
///
/// With `shape: rows`, each message is one row and gets its answer. With
/// `shape: window`, the transform keeps the last readings of each series in
/// the pipeline's state bucket and forecasts from them. The answer is merged
/// onto the reading, or sent as forecast points that `mark` can label.
///
/// kayak loads the model and runs it once on zeros when it builds the
/// pipeline, so a wrong path, a wrong shape or an operator that kayak cannot
/// run stops the build. To change the model, write the new file under a new
/// name and replace the pipeline with the new `path`. If the new model does not
/// build, the old one keeps running.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq)]
#[schemars(title = "infer")]
pub struct InferTransformConfig {
    /// The model file.
    pub model: ModelSource,
    /// How the messages become the input tensor.
    pub shape: InferShape,
    /// The numeric fields that make one row of the tensor, in the sequence
    /// that the model expects.
    pub inputs: Vec<String>,
    /// The fields that the answer goes to, one per output column of the model.
    pub outputs: Vec<String>,
    /// Write the model's path and sha256 under this field, on each message
    /// that has an answer. Leave it out to write nothing.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provenance: Option<String>,
    /// What to do with a message that does not have an input field or a group
    /// field. The default is `error`. With `skip`, the message passes through
    /// without an answer.
    #[serde(default, skip_serializing_if = "MissingFieldPolicy::is_default")]
    pub on_missing: MissingFieldPolicy,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// The defaults stay off the wire, so a config that comes back out of the
    /// API is the one that went in.
    #[test]
    fn a_bare_window_round_trips_bare() -> Result<(), serde_json::Error> {
        let wire = json!({
            "model": {"type": "file", "path": "models/m.onnx"},
            "shape": {"type": "window", "size": 24},
            "inputs": ["flow"],
            "outputs": ["forecast"]
        });
        let parsed: InferTransformConfig = serde_json::from_value(wire.clone())?;
        assert_eq!(
            parsed.shape,
            InferShape::Window {
                size: 24,
                stride: None,
                group_by: vec![],
                time: None,
                emit: InferEmit::Merge
            }
        );
        assert_eq!(serde_json::to_value(&parsed)?, wire);
        Ok(())
    }

    #[test]
    fn points_with_a_mark_round_trip() -> Result<(), serde_json::Error> {
        let wire = json!({
            "type": "points",
            "step_seconds": 60.0,
            "mark": {"field": "origin", "inferred": "forecast", "observed": "observed"}
        });
        let parsed: InferEmit = serde_json::from_value(wire.clone())?;
        assert!(
            matches!(&parsed, InferEmit::Points { mark: Some(m), .. } if m.inferred == "forecast")
        );
        assert_eq!(serde_json::to_value(&parsed)?, wire);
        Ok(())
    }
}
