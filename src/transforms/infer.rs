//! `infer`: an ONNX model run over the stream, in process.
//!
//! The declaration is [`kayak_core::infer::InferTransformConfig`] and its
//! module docs carry the argument for the shape. This is the evaluation, and
//! four things are decided here:
//!
//! - **A model is proved at build time, by running it.** Loading a file, fixing
//!   the input shape and optimising is not enough to know the answer's shape —
//!   an ONNX output may be symbolic — so the build runs the model once on zeros
//!   and checks what came back against `outputs` (and, for a window, reads the
//!   horizon off it). Everything a model can be wrong about is therefore a
//!   refused build, which is what makes replacing a pipeline with a new model
//!   safe: `replace_pipeline` rebuilds the old config when the new one fails.
//! - **Optimised plans are shared through [`Models`], and held weakly.** The
//!   key is the file's sha256 and the input shape, so two pipelines running one
//!   model optimise it once, and a replace can [`Models::prepare`] the new
//!   model *before* the old run loop is stopped — optimising is the slow part
//!   of a build, and doing it inside the stop is the pause a swap would
//!   otherwise cost. Weak, so a model nobody runs is not kept alive by the map.
//! - **The window lives in the bucket, not on the transform.** So a swap to a
//!   model with the same window starts forecasting at its first reading, with
//!   no warm-up, and the window survives a revert by the bucket's own rule.
//! - **The model runs on the run loop's task.** The models this is for (a
//!   linear forecaster, a boosted tree, a small autoencoder) run in
//!   microseconds; a model heavy enough for that to matter belongs behind the
//!   `http` transform, on hardware chosen for it.

use anyhow::{Context, Result, bail, ensure};
use kayak_core::config::MissingFieldPolicy;
use kayak_core::infer::{InferEmit, InferMark, InferShape, InferTransformConfig, ModelSource};
use kayak_core::streaming::Gate;
use serde_json::{Value, json};
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex, Weak};

use super::keyed::{Series, Window};
use super::script::source::{self, Beside};
use crate::{
    BuildCtx, fields,
    inputs::MessageBatch,
    time,
    transforms::{BuildTransform, Transform},
};

const MODEL: Beside = Beside {
    noun: "a model file",
    example: "models/forecast.onnx",
};

/// The shape a model is fixed to and optimised for — part of a plan's key,
/// since one file optimised for two shapes is two plans.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum Fixed {
    /// `[messages, inputs]`, the first dimension left open.
    Rows { inputs: usize },
    /// `[1, size, inputs]`.
    Window { size: usize, inputs: usize },
}

/// The optimised plans the running pipelines share. Owned by `AppState` and
/// carried on [`BuildCtx`], the way the http input's inboxes are.
#[derive(Default)]
pub struct Models {
    plans: Mutex<HashMap<(String, Fixed), Weak<Model>>>,
}

impl Models {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Load and optimise the model a config names, or hand back the plan that
    /// is already loaded for the same file and shape. Holding the returned
    /// `Arc` keeps the plan for the next build of the same thing.
    pub fn prepare(&self, config: &InferTransformConfig, dir: Option<&Path>) -> Result<Arc<Model>> {
        let fixed = fixed_of(config)?;
        let bytes = read_model(&config.model, dir)?;
        let sha256 = sha256_hex(&bytes);
        let key = (sha256.clone(), fixed.clone());
        if let Some(model) = self.lock().get(&key).and_then(Weak::upgrade) {
            return Ok(model);
        }
        // Optimised outside the lock: it is the slow part, and two pipelines
        // building two different models shouldn't wait on each other. Two
        // racing on the *same* model optimise it twice, once wasted.
        let model = Arc::new(
            Model::load(&bytes, sha256, &fixed)
                .with_context(|| format!("the model '{}'", config.model.path()))?,
        );
        let mut plans = self.lock();
        plans.retain(|_, plan| plan.strong_count() > 0);
        plans.insert(key, Arc::downgrade(&model));
        Ok(model)
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<(String, Fixed), Weak<Model>>> {
        self.plans
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// How many plans are alive, for the tests that pin the sharing.
    #[must_use]
    pub fn alive(&self) -> usize {
        self.lock()
            .values()
            .filter(|plan| plan.strong_count() > 0)
            .count()
    }
}

fn fixed_of(config: &InferTransformConfig) -> Result<Fixed> {
    let inputs = config.inputs.len();
    ensure!(
        inputs > 0,
        "an infer transform needs at least one field in 'inputs'"
    );
    Ok(match &config.shape {
        InferShape::Rows => Fixed::Rows { inputs },
        InferShape::Window { size, .. } => {
            ensure!(*size > 0, "an infer window's 'size' has to be at least one");
            Fixed::Window {
                size: *size,
                inputs,
            }
        }
    })
}

fn read_model(source: &ModelSource, dir: Option<&Path>) -> Result<Vec<u8>> {
    let ModelSource::File { path } = source;
    let Some(dir) = dir else {
        bail!(
            "this infer transform names the model file '{path}', but the server has no config \
             file to resolve it against — a model lives beside the config, and this server was \
             started without one. Start the server with --config"
        )
    };
    // Appended-extension rule's cousin: only an `.onnx` file is ever opened,
    // because the config's directory is where `secrets.json` usually lives
    // and a parse error about a file that should never have been read is a
    // disclosure, not a diagnostic.
    ensure!(
        Path::new(path.trim())
            .extension()
            .is_some_and(|ext| ext == "onnx"),
        "the model file '{path}' does not end in '.onnx'"
    );
    let file = source::locate(path, dir, &MODEL)?;
    std::fs::read(&file).with_context(|| format!("failed to read the model file '{path}'"))
}

fn sha256_hex(bytes: &[u8]) -> String {
    use sha2::{Digest, Sha256};
    use std::fmt::Write;
    Sha256::digest(bytes)
        .iter()
        .fold(String::with_capacity(64), |mut hex, b| {
            let _ = write!(hex, "{b:02x}");
            hex
        })
}

/// A loaded, optimised model.
pub struct Model {
    sha256: String,
    #[cfg(feature = "onnx")]
    plan: Arc<tract_onnx::prelude::TypedRunnableModel>,
}

impl Model {
    #[cfg(feature = "onnx")]
    fn load(bytes: &[u8], sha256: String, fixed: &Fixed) -> Result<Self> {
        use tract_onnx::prelude::*;
        let mut model = tract_onnx::onnx()
            .model_for_read(&mut &bytes[..])
            .map_err(|e| anyhow::anyhow!("{e:#}"))
            .context("it is not an ONNX model kayak can read")?;
        let declared = model
            .input_outlets()
            .map(<[OutletId]>::len)
            .unwrap_or_default();
        ensure!(
            declared == 1,
            "the model has {declared} inputs; infer feeds exactly one"
        );
        let shape: Vec<TDim> = match *fixed {
            Fixed::Rows { inputs } => vec![model.sym("N").into(), inputs.into()],
            Fixed::Window { size, inputs } => vec![1.into(), size.into(), inputs.into()],
        };
        model
            .set_input_fact(0, f32::fact(shape).into())
            .map_err(|e| anyhow::anyhow!("{e:#}"))
            .context("its input does not take a float tensor of the configured shape")?;
        let plan = model
            .into_optimized()
            .and_then(TypedModel::into_runnable)
            .map_err(|e| anyhow::anyhow!("{e:#}"))
            .context("kayak cannot run it")?;
        Ok(Self { sha256, plan })
    }

    #[cfg(not(feature = "onnx"))]
    fn load(_bytes: &[u8], _sha256: String, _fixed: &Fixed) -> Result<Self> {
        bail!("this server was built without ONNX support (the `onnx` feature)")
    }

    /// Run on `data`, laid out as `shape`, and return the first output as its
    /// shape and its values.
    #[cfg(feature = "onnx")]
    fn run(&self, shape: &[usize], data: &[f32]) -> Result<(Vec<usize>, Vec<f32>)> {
        use tract_onnx::prelude::*;
        let input = Tensor::from_shape(shape, data).map_err(|e| anyhow::anyhow!("{e:#}"))?;
        let outputs = self
            .plan
            .run(tvec!(input.into()))
            .map_err(|e| anyhow::anyhow!("{e:#}"))?;
        let first = outputs.first().context("the model returned no output")?;
        let as_f32 = first
            .cast_to::<f32>()
            .map_err(|e| anyhow::anyhow!("{e:#}"))?;
        let values = as_f32
            .to_plain_array_view::<f32>()
            .map_err(|e| anyhow::anyhow!("{e:#}"))?
            .iter()
            .copied()
            .collect();
        Ok((first.shape().to_vec(), values))
    }

    #[cfg(not(feature = "onnx"))]
    fn run(&self, _shape: &[usize], _data: &[f32]) -> Result<(Vec<usize>, Vec<f32>)> {
        bail!("this server was built without ONNX support (the `onnx` feature)")
    }

    /// [`Model::run`] on tokio's blocking pool. A run is CPU work with no
    /// `.await` in it, so on the run loop's task it would hold a worker for
    /// its whole length, and enough of those at once stall every pipeline, the
    /// API and the feed. Every run after the build goes through here; `run`
    /// itself is for the build's trial run, which happens off the run loop.
    async fn infer(
        self: &Arc<Self>,
        shape: Vec<usize>,
        data: Vec<f32>,
    ) -> Result<(Vec<usize>, Vec<f32>)> {
        let model = Arc::clone(self);
        offload(move || model.run(&shape, &data)).await
    }
}

/// Run `work` on tokio's blocking pool and wait for it without holding a
/// worker.
async fn offload<T: Send + 'static>(
    work: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<T> {
    tokio::task::spawn_blocking(work)
        .await
        .context("the model run panicked")?
}

impl Model {

    /// The sha256 of the file the model was loaded from.
    #[must_use]
    pub fn sha256(&self) -> &str {
        &self.sha256
    }
}

/// How many columns an answer of this shape has, given `rows` rows of it:
/// `[rows, columns]`, or `[rows]` for one.
fn columns_of(shape: &[usize], rows: usize) -> Option<usize> {
    match shape {
        [r] if *r == rows => Some(1),
        [r, c] if *r == rows => Some(*c),
        _ => None,
    }
}

impl BuildTransform for InferTransformConfig {
    fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn Transform>> {
        if self.outputs.is_empty() {
            bail!("an infer transform needs at least one field in 'outputs'");
        }
        for name in self.inputs.iter().chain(&self.outputs) {
            ensure!(
                !name.trim().is_empty(),
                "an infer transform has a blank field name"
            );
        }
        let mut seen = std::collections::HashSet::new();
        for name in &self.outputs {
            ensure!(
                seen.insert(name.trim()),
                "two outputs are both called '{name}'"
            );
        }
        let model = ctx.models.prepare(
            &self,
            ctx.script_dir.as_deref().map(std::path::PathBuf::as_path),
        )?;
        let provenance = self.provenance.as_ref().map(|field| {
            (
                field.trim().to_string(),
                json!({"path": self.model.path(), "sha256": model.sha256()}),
            )
        });
        let k = self.inputs.len();
        let m = self.outputs.len();
        let mode = match self.shape {
            InferShape::Rows => {
                let (shape, _) = model
                    .run(&[1, k], &vec![0.0; k])
                    .context("a trial run on zeros")?;
                ensure!(
                    columns_of(&shape, 1) == Some(m),
                    "the model answers {shape:?} for one row, and {m} output field(s) need \
                     [1, {m}]{}",
                    if m == 1 { " or [1]" } else { "" }
                );
                Mode::Rows
            }
            InferShape::Window {
                size,
                stride,
                group_by,
                time,
                emit,
            } => {
                let (shape, _) = model
                    .run(&[1, size, k], &vec![0.0; size * k])
                    .context("a trial run on zeros")?;
                let horizon = match shape.as_slice() {
                    [1, h] if m == 1 => *h,
                    [1, h, c] if *c == m => *h,
                    _ => bail!(
                        "the model answers {shape:?} for a window, and {m} output field(s) need \
                         [1, horizon, {m}]{}",
                        if m == 1 { " or [1, horizon]" } else { "" }
                    ),
                };
                ensure!(horizon > 0, "the model answers a horizon of zero steps");
                let stride = stride.unwrap_or(1);
                ensure!(
                    stride > 0,
                    "an infer window's 'stride' has to be at least one"
                );
                let time_name = time.clone();
                let mut outputs: Vec<&str> = self.outputs.iter().map(String::as_str).collect();
                outputs.sort_unstable();
                let series = Series::resolve(
                    ctx,
                    "infer",
                    format!("infer:{}", outputs.join(",")),
                    group_by,
                    time,
                    self.on_missing,
                    Gate::default(),
                )?;
                Mode::Window {
                    series,
                    size,
                    stride,
                    horizon,
                    emit: Emit::new(emit, time_name)?,
                }
            }
        };
        Ok(Box::new(InferTransform {
            model,
            provenance,
            inputs: self.inputs,
            outputs: self.outputs,
            on_missing: self.on_missing,
            mode,
        }))
    }
}

#[allow(clippy::large_enum_variant, reason = "one per transform, built once")]
enum Mode {
    Rows,
    Window {
        series: Series,
        size: usize,
        stride: usize,
        horizon: usize,
        emit: Emit,
    },
}

/// [`InferEmit`] with its defaults applied.
enum Emit {
    Merge,
    Points {
        step_seconds: Option<f64>,
        step_field: String,
        time_field: String,
        mark: Option<InferMark>,
    },
}

impl Emit {
    fn new(emit: InferEmit, window_time: Option<String>) -> Result<Self> {
        Ok(match emit {
            InferEmit::Merge => Self::Merge,
            InferEmit::Points {
                step_seconds,
                step_field,
                time_field,
                mark,
            } => {
                if step_seconds.is_some_and(|s| s <= 0.0 || !s.is_finite()) {
                    bail!("an infer transform's 'step_seconds' has to be more than zero");
                }
                if let Some(mark) = &mark {
                    ensure!(
                        !mark.field.trim().is_empty(),
                        "an infer transform's 'mark' needs a 'field'"
                    );
                    ensure!(
                        mark.observed.as_deref() != Some(mark.inferred.as_str()),
                        "an infer transform's 'mark' gives inferred and observed messages the \
                         same label, '{}', which tells them apart from nothing",
                        mark.inferred
                    );
                }
                let named = |field: Option<String>| field.filter(|f| !f.trim().is_empty());
                Self::Points {
                    step_seconds,
                    step_field: named(step_field).unwrap_or_else(|| "step".into()),
                    time_field: named(time_field)
                        .or(window_time)
                        .unwrap_or_else(|| "time".into()),
                    mark,
                }
            }
        })
    }
}

pub struct InferTransform {
    model: Arc<Model>,
    provenance: Option<(String, Value)>,
    inputs: Vec<String>,
    outputs: Vec<String>,
    on_missing: MissingFieldPolicy,
    mode: Mode,
}

/// An f32 the way it would be written by hand: through its own shortest
/// decimal form, for the reason the opcua input gives (`f64::from(0.1f32)` is
/// `0.10000000149011612`). Not finite is null: JSON has no spelling for it.
fn number(value: f32) -> Value {
    format!("{value}")
        .parse::<f64>()
        .ok()
        .and_then(serde_json::Number::from_f64)
        .map_or(Value::Null, Value::Number)
}

impl InferTransform {
    /// One row of the tensor, or `None` to pass the message through.
    fn row(&self, message: &Value) -> Result<Option<Vec<f64>>> {
        let mut row = Vec::with_capacity(self.inputs.len());
        for field in &self.inputs {
            match fields::get(message, field) {
                None | Some(Value::Null) => match self.on_missing {
                    MissingFieldPolicy::Skip => return Ok(None),
                    MissingFieldPolicy::Error => bail!("field '{field}' is missing from a message"),
                },
                Some(value) => row.push(value.as_f64().with_context(|| {
                    format!(
                        "field '{field}' is {}, not a number",
                        fields::describe(value)
                    )
                })?),
            }
        }
        Ok(Some(row))
    }

    fn stamp(&self, message: &mut Value) -> Result<()> {
        if let Some((field, value)) = &self.provenance {
            fields::set(message, field, value.clone())?;
        }
        Ok(())
    }

    /// A message leaving the transform that the model did not make.
    fn observed(&self, message: &Arc<Value>) -> Result<Arc<Value>> {
        if let Mode::Window {
            emit:
                Emit::Points {
                    mark:
                        Some(InferMark {
                            field,
                            observed: Some(label),
                            ..
                        }),
                    ..
                },
            ..
        } = &self.mode
        {
            let mut labelled = (**message).clone();
            fields::set(&mut labelled, field, Value::from(label.as_str()))?;
            return Ok(Arc::new(labelled));
        }
        Ok(Arc::clone(message))
    }

    #[allow(
        clippy::cast_possible_truncation,
        reason = "the model is fed f32, as ONNX models are"
    )]
    async fn rows(&self, batch: &MessageBatch) -> Result<MessageBatch> {
        let mut rows = Vec::with_capacity(batch.len());
        let mut data = Vec::with_capacity(batch.len() * self.inputs.len());
        for (index, message) in batch.iter().enumerate() {
            if let Some(row) = self.row(message)? {
                data.extend(row.into_iter().map(|v| v as f32));
                rows.push(index);
            }
        }
        if rows.is_empty() {
            return Ok(batch.clone());
        }
        let (shape, values) = self
            .model
            .infer(vec![rows.len(), self.inputs.len()], data)
            .await?;
        let m = self.outputs.len();
        ensure!(
            columns_of(&shape, rows.len()) == Some(m) && values.len() == rows.len() * m,
            "the model answered {shape:?} for {} rows",
            rows.len()
        );
        let mut out = batch.clone();
        for (r, &index) in rows.iter().enumerate() {
            let mut written = (*out[index]).clone();
            for (j, name) in self.outputs.iter().enumerate() {
                fields::set(&mut written, name.trim(), number(values[r * m + j]))?;
            }
            self.stamp(&mut written)?;
            out[index] = Arc::new(written);
        }
        Ok(out)
    }

    async fn window(&self, batch: &MessageBatch) -> Result<MessageBatch> {
        let Mode::Window {
            series,
            size,
            stride,
            horizon,
            emit,
        } = &self.mode
        else {
            return Ok(batch.clone());
        };
        let (size, stride, horizon) = (*size, *stride, *horizon);
        let k = self.inputs.len();
        let m = self.outputs.len();
        let mut out = Vec::with_capacity(batch.len());
        for message in batch {
            let Some(key) = series.key(message)? else {
                out.push(self.observed(message)?);
                continue;
            };
            let Some(row) = self.row(message)? else {
                out.push(self.observed(message)?);
                continue;
            };
            let now = series.millis(message)?;
            let due = series.update(&key, |state| due_window(state, now, &row, size, stride))?;
            let Some(data) = due else {
                out.push(self.observed(message)?);
                continue;
            };
            let (shape, values) = self.model.infer(vec![1, size, k], data).await?;
            ensure!(
                values.len() == horizon * m,
                "the model answered {shape:?} for a window, not the {horizon} steps it answered \
                 at build"
            );
            let values = &values;
            let column = |j: usize| (0..horizon).map(move |h| values[h * m + j]);
            match emit {
                Emit::Merge => {
                    let mut written = (**message).clone();
                    for (j, name) in self.outputs.iter().enumerate() {
                        let forecast: Vec<Value> = column(j).map(number).collect();
                        fields::set(&mut written, name.trim(), Value::Array(forecast))?;
                    }
                    self.stamp(&mut written)?;
                    out.push(Arc::new(written));
                }
                Emit::Points {
                    step_seconds,
                    step_field,
                    time_field,
                    mark,
                } => {
                    out.push(self.observed(message)?);
                    let group = series.group_fields_of(message);
                    for h in 0..horizon {
                        let mut point = Value::Object(serde_json::Map::new());
                        for (field, value) in &group {
                            fields::set(&mut point, field, value.clone())?;
                        }
                        fields::set(&mut point, step_field, Value::from(h + 1))?;
                        if let Some(step) = step_seconds {
                            #[allow(
                                clippy::cast_possible_truncation,
                                clippy::cast_precision_loss,
                                reason = "a horizon of steps in milliseconds, well within range"
                            )]
                            let at = now + ((h + 1) as f64 * step * 1000.0).round() as i64;
                            fields::set(&mut point, time_field, Value::from(time::format(at)?))?;
                        }
                        for (j, name) in self.outputs.iter().enumerate() {
                            fields::set(&mut point, name.trim(), number(values[h * m + j]))?;
                        }
                        self.stamp(&mut point)?;
                        if let Some(mark) = mark {
                            fields::set(
                                &mut point,
                                &mark.field,
                                Value::from(mark.inferred.as_str()),
                            )?;
                        }
                        out.push(Arc::new(point));
                    }
                }
            }
        }
        Ok(out)
    }
}

/// Push a reading into a key's stored window and say whether the model is due:
/// the window's values, flattened oldest first, once it is full and `stride`
/// readings have arrived since the last run (the first full window always
/// runs).
///
/// Stored as `{"window": [[millis, [inputs…]], …], "since": n}` — `since` is
/// absent until the window first fills.
#[allow(
    clippy::cast_possible_truncation,
    reason = "the model is fed f32, as ONNX models are"
)]
fn due_window(
    state: &mut Value,
    now: i64,
    row: &[f64],
    size: usize,
    stride: usize,
) -> Option<Vec<f32>> {
    if !state.is_object() {
        *state = json!({});
    }
    Window::push(&mut state["window"], now, json!(row), size, None);
    let points = Window::points(&state["window"]);
    if points.len() < size {
        return None;
    }
    let since = state.get("since").and_then(Value::as_u64);
    #[allow(clippy::cast_possible_truncation, reason = "a stride is a small count")]
    let due = since.is_none_or(|s| s + 1 >= stride as u64);
    if !due {
        state["since"] = Value::from(since.map_or(1, |s| s + 1));
        return None;
    }
    let data = points
        .iter()
        .filter_map(|p| p.get(1).and_then(Value::as_array))
        .flat_map(|row| row.iter().map(|v| v.as_f64().unwrap_or_default() as f32))
        .collect();
    state["since"] = Value::from(0);
    Some(data)
}

#[async_trait::async_trait]
impl Transform for InferTransform {
    async fn apply(&mut self, batch: Arc<MessageBatch>) -> Result<Vec<Arc<MessageBatch>>> {
        let out = match self.mode {
            Mode::Rows => self.rows(&batch).await?,
            Mode::Window { .. } => self.window(&batch).await?,
        };
        Ok(vec![Arc::new(out)])
    }
}

#[cfg(all(test, feature = "onnx"))]
mod tests {
    use super::*;
    use crate::testing::{batch, ctx_with_bucket, onnx};
    use kayak_core::infer::InferMark;

    #[tokio::test]
    async fn rows_writes_each_answer_onto_its_message() -> Result<()> {
        let dir = dir_with(&[("m.onnx", onnx::sum_and_difference())])?;
        let mut config = rows_config("m.onnx", &["sum", "diff"]);
        config.provenance = Some("_model".into());
        config.on_missing = MissingFieldPolicy::Skip;
        let mut transform = build(config, dir.path(), &Arc::new(Models::new()))?;
        let out = run(
            &mut transform,
            vec![
                json!({"a": 3, "b": 1}),
                json!({"a": 1}),
                json!({"a": 0.5, "b": 0.25}),
            ],
        )
        .await?;
        assert_eq!(
            (&out[0]["sum"], &out[0]["diff"]),
            (&json!(4.0), &json!(2.0))
        );
        assert_eq!(out[1], json!({"a": 1}), "skipped, untouched");
        assert_eq!(
            (&out[2]["sum"], &out[2]["diff"]),
            (&json!(0.75), &json!(0.25))
        );
        assert_eq!(out[0]["_model"]["path"], json!("m.onnx"));
        assert_eq!(
            out[0]["_model"]["sha256"],
            json!(sha256_hex(&onnx::sum_and_difference()))
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_missing_input_is_an_error_by_default_and_a_wrong_one_always() -> Result<()> {
        let dir = dir_with(&[("m.onnx", onnx::sum_and_difference())])?;
        let shared = Arc::new(Models::new());
        let mut strict = build(rows_config("m.onnx", &["sum", "diff"]), dir.path(), &shared)?;
        assert!(run(&mut strict, vec![json!({"a": 1})]).await.is_err());
        let mut lax = rows_config("m.onnx", &["sum", "diff"]);
        lax.on_missing = MissingFieldPolicy::Skip;
        let mut lax = build(lax, dir.path(), &shared)?;
        let err = run(&mut lax, vec![json!({"a": 1, "b": "2"})])
            .await
            .err()
            .map(|e| e.to_string());
        assert!(err.unwrap_or_default().contains("not a number"));
        Ok(())
    }

    #[tokio::test]
    async fn a_window_forecasts_once_full_and_per_key() -> Result<()> {
        let dir = dir_with(&[("models/w.onnx", onnx::last_plus_step())])?;
        let mut transform = build(
            window_config(InferEmit::Merge, None),
            dir.path(),
            &Arc::new(Models::new()),
        )?;
        let out = run(
            &mut transform,
            vec![
                reading("a", 0, 1.0),
                reading("a", 1, 2.0),
                reading("b", 0, 9.0),
                reading("a", 2, 3.0),
                reading("a", 3, 7.0),
            ],
        )
        .await?;
        let forecasts: Vec<&Value> = out.iter().map(|m| &m["forecast"]).collect();
        assert_eq!(
            forecasts,
            vec![
                &Value::Null,
                &Value::Null,
                &Value::Null,
                &json!([4.0, 5.0]),
                &json!([8.0, 9.0])
            ]
        );
        assert_eq!(out[3]["v"], json!(3.0), "the reading itself is kept");
        Ok(())
    }

    #[tokio::test]
    async fn stride_runs_the_first_full_window_and_then_every_nth() -> Result<()> {
        let dir = dir_with(&[("models/w.onnx", onnx::last_plus_step())])?;
        let mut transform = build(
            window_config(InferEmit::Merge, Some(2)),
            dir.path(),
            &Arc::new(Models::new()),
        )?;
        let messages = [0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0]
            .into_iter()
            .zip(0..)
            .map(|(v, t)| reading("a", t, v))
            .collect();
        let out = run(&mut transform, messages).await?;
        let ran: Vec<bool> = out.iter().map(|m| m.get("forecast").is_some()).collect();
        assert_eq!(ran, vec![false, false, true, false, true, false, true]);
        Ok(())
    }

    /// The question the points shape answers: in a batch that carries both,
    /// which messages did the model make? `mark` says so on every message, and
    /// the points follow the reading that prompted them.
    #[tokio::test]
    async fn points_follow_their_reading_and_are_labelled() -> Result<()> {
        let dir = dir_with(&[("models/w.onnx", onnx::last_plus_step())])?;
        let emit = InferEmit::Points {
            step_seconds: Some(60.0),
            step_field: None,
            time_field: None,
            mark: Some(InferMark {
                field: "origin".into(),
                inferred: "forecast".into(),
                observed: Some("observed".into()),
            }),
        };
        let mut config = window_config(emit, None);
        config.provenance = Some("model".into());
        let mut transform = build(config, dir.path(), &Arc::new(Models::new()))?;
        let out = run(
            &mut transform,
            vec![
                reading("a", 0, 1.0),
                reading("a", 1000, 2.0),
                reading("a", 2000, 3.0),
            ],
        )
        .await?;
        let origins: Vec<&Value> = out.iter().map(|m| &m["origin"]).collect();
        assert_eq!(
            origins,
            vec![
                &json!("observed"),
                &json!("observed"),
                &json!("observed"),
                &json!("forecast"),
                &json!("forecast")
            ]
        );
        assert_eq!(out[2]["v"], json!(3.0));
        assert_eq!(
            out[2].get("model"),
            None,
            "provenance is for what the model made"
        );
        let point = &out[3];
        assert_eq!(point["k"], json!("a"));
        assert_eq!(point["step"], json!(1));
        assert_eq!(point["forecast"], json!(4.0));
        assert_eq!(
            point["t"],
            json!("1970-01-01T00:01:02.000Z"),
            "the window's time field, plus a step"
        );
        assert_eq!(point["model"]["path"], json!("models/w.onnx"));
        assert_eq!(out[4]["step"], json!(2));
        assert_eq!(out[4]["forecast"], json!(5.0));
        Ok(())
    }

    #[tokio::test]
    async fn without_observed_only_the_points_are_labelled() -> Result<()> {
        let dir = dir_with(&[("models/w.onnx", onnx::last_plus_step())])?;
        let emit = InferEmit::Points {
            step_seconds: None,
            step_field: Some("ahead".into()),
            time_field: None,
            mark: Some(InferMark {
                field: "kind".into(),
                inferred: "predicted".into(),
                observed: None,
            }),
        };
        let mut transform = build(
            window_config(emit, None),
            dir.path(),
            &Arc::new(Models::new()),
        )?;
        let out = run(
            &mut transform,
            vec![
                reading("a", 0, 1.0),
                reading("a", 1, 2.0),
                reading("a", 2, 3.0),
            ],
        )
        .await?;
        assert_eq!(out[2], reading("a", 2, 3.0));
        assert_eq!(out[3]["kind"], json!("predicted"));
        assert_eq!(out[3]["ahead"], json!(1));
        assert_eq!(out[3].get("t"), None, "no step_seconds, no time");
        Ok(())
    }

    #[test]
    fn what_a_model_cannot_answer_is_refused_at_build() -> Result<()> {
        let dir = dir_with(&[
            ("m.onnx", onnx::sum_and_difference()),
            ("models/w.onnx", onnx::last_plus_step()),
            ("garbage.onnx", b"not a model".to_vec()),
            ("secrets.json", b"{}".to_vec()),
        ])?;
        let shared = Arc::new(Models::new());
        let refused = |config: InferTransformConfig| {
            build(config, dir.path(), &shared)
                .err()
                .map(|e| format!("{e:#}"))
                .unwrap_or_default()
        };
        assert!(refused(rows_config("m.onnx", &["only_one"])).contains("[1, 1]"));
        assert!(refused(rows_config("garbage.onnx", &["x"])).contains("not an ONNX model"));
        assert!(refused(rows_config("secrets.json", &["x"])).contains(".onnx"));
        assert!(refused(rows_config("../m.onnx", &["x", "y"])).contains(".."));
        assert!(refused(rows_config("absent.onnx", &["x", "y"])).contains("absent.onnx"));
        assert!(refused(rows_config("m.onnx", &[])).contains("outputs"));
        assert!(refused(rows_config("m.onnx", &["x", "x"])).contains("both called"));

        let mut wrong_size = window_config(InferEmit::Merge, None);
        if let InferShape::Window { size, .. } = &mut wrong_size.shape {
            *size = 4;
        }
        assert!(
            !refused(wrong_size).is_empty(),
            "the reshape inside expects three readings"
        );

        let same = InferEmit::Points {
            step_seconds: None,
            step_field: None,
            time_field: None,
            mark: Some(InferMark {
                field: "o".into(),
                inferred: "x".into(),
                observed: Some("x".into()),
            }),
        };
        assert!(refused(window_config(same, None)).contains("same label"));
        Ok(())
    }

    #[test]
    fn a_model_needs_a_config_directory_and_a_window_needs_a_bucket() -> Result<()> {
        let dir = dir_with(&[("models/w.onnx", onnx::last_plus_step())])?;
        let mut pipelines = HashMap::new();
        let (events, _) = tokio::sync::broadcast::channel(4);
        let mut bare = BuildCtx::new(&mut pipelines, "p".into(), events);
        let err = window_config(InferEmit::Merge, None)
            .build(&mut bare)
            .err()
            .map(|e| e.to_string());
        assert!(err.unwrap_or_default().contains("--config"));

        bare.script_dir = Some(Arc::new(dir.path().to_path_buf()));
        let err = window_config(InferEmit::Merge, None)
            .build(&mut bare)
            .err()
            .map(|e| e.to_string());
        assert!(err.unwrap_or_default().contains("state"));
        Ok(())
    }

    /// Two pipelines on one model optimise it once; a changed file is a new
    /// plan; and a plan nothing holds any more is let go.
    #[test]
    fn plans_are_shared_by_file_and_shape_and_held_weakly() -> Result<()> {
        let dir = dir_with(&[("m.onnx", onnx::sum_and_difference())])?;
        let shared = Models::new();
        let config = rows_config("m.onnx", &["sum", "diff"]);
        let first = shared.prepare(&config, Some(dir.path()))?;
        let second = shared.prepare(&config, Some(dir.path()))?;
        assert!(Arc::ptr_eq(&first, &second));
        assert_eq!(shared.alive(), 1);

        std::fs::write(dir.path().join("m.onnx"), onnx::doubling())?;
        let changed = shared.prepare(&config, Some(dir.path()))?;
        assert!(!Arc::ptr_eq(&first, &changed));
        assert_eq!(shared.alive(), 2);

        drop((first, second));
        assert_eq!(shared.alive(), 1);
        Ok(())
    }

    #[test]
    fn an_f32_is_written_in_its_own_shortest_form() {
        assert_eq!(number(0.1), json!(0.1));
        assert_eq!(number(f32::NAN), Value::Null);
    }

    fn rows_config(path: &str, outputs: &[&str]) -> InferTransformConfig {
        InferTransformConfig {
            model: ModelSource::File { path: path.into() },
            shape: InferShape::Rows,
            inputs: vec!["a".into(), "b".into()],
            outputs: outputs.iter().map(ToString::to_string).collect(),
            provenance: None,
            on_missing: MissingFieldPolicy::Error,
        }
    }

    fn window_config(emit: InferEmit, stride: Option<usize>) -> InferTransformConfig {
        InferTransformConfig {
            model: ModelSource::File {
                path: "models/w.onnx".into(),
            },
            shape: InferShape::Window {
                size: 3,
                stride,
                group_by: vec!["k".into()],
                time: Some("t".into()),
                emit,
            },
            inputs: vec!["v".into()],
            outputs: vec!["forecast".into()],
            provenance: None,
            on_missing: MissingFieldPolicy::Error,
        }
    }

    /// On a single-threaded runtime, the work waits for a task on that same
    /// runtime. Run inline it would hold the only thread, the task could never
    /// run, and the wait would time out.
    #[tokio::test(flavor = "current_thread")]
    async fn a_model_run_does_not_hold_the_runtime() -> Result<()> {
        let (tx, rx) = std::sync::mpsc::channel();
        tokio::spawn(async move { tx.send(()) });
        offload(move || {
            rx.recv_timeout(std::time::Duration::from_secs(5))
                .context("the runtime's other task never ran")
        })
        .await
    }

    #[tokio::test]
    async fn a_panicking_run_is_an_error_not_a_crash() {
        let result: Result<()> = offload(|| panic!("tract fell over")).await;
        assert!(result.is_err_and(|e| e.to_string().contains("panicked")));
    }

    fn build(
        config: InferTransformConfig,
        dir: &Path,
        shared: &Arc<Models>,
    ) -> Result<Box<dyn Transform>> {
        let mut pipelines = HashMap::new();
        let mut ctx = ctx_with_bucket(&mut pipelines, None)
            .with_script_dir(Some(Arc::new(dir.to_path_buf())))
            .with_models(Arc::clone(shared));
        config.build(&mut ctx)
    }

    async fn run(transform: &mut Box<dyn Transform>, messages: Vec<Value>) -> Result<Vec<Value>> {
        let out = transform.apply(batch(messages)).await?;
        Ok(out
            .iter()
            .flat_map(|b| b.iter().map(|m| (**m).clone()))
            .collect())
    }

    fn reading(k: &str, t: i64, v: f64) -> Value {
        json!({"k": k, "t": t, "v": v})
    }

    /// A directory holding `files`, as a config directory would.
    fn dir_with(files: &[(&str, Vec<u8>)]) -> anyhow::Result<tempfile::TempDir> {
        let dir = tempfile::tempdir()?;
        for (name, bytes) in files {
            let path = dir.path().join(name);
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(path, bytes)?;
        }
        Ok(dir)
    }
}
