//! `detect`: flag a reading that is not like the ones before it.
//!
//! The declaration is [`kayak_core::streaming::DetectTransformConfig`]; the
//! shape is [`super::keyed`]. Six methods, one rule they all obey: **nothing
//! is flagged during the warm-up**, because until `min_samples` readings have
//! been seen there is no idea of normal to be outside of. The flag is written
//! `false` and the score `null` for those, so a downstream `filter` on the
//! flag never has to know about warm-up.
//!
//! The window methods (`zscore`, `mad`) measure a reading against the window
//! *before* it — the reading joins the window only after it has been scored,
//! so a spike cannot pull the baseline it is measured against. The chart
//! methods (`cusum` without a `target`, `ewma_chart`, `western_electric`)
//! **freeze** their baseline — the mean and deviation of the warm-up — and
//! hold the series to it from then on, which is what a control chart is. A
//! baseline with no spread flags any departure from it; that is what the
//! arithmetic says, and it is right for a signal that was flat and moved.
//!
//! The score is in the method's own units — standard deviations for the
//! z-score family, the accumulated drift for CUSUM — and `null` where the
//! method has none (`flatline`) or where a zero spread made it infinite.
//!
//! The methods that keep learning (`zscore`, `mad`, `ewma`) take `learn`.
//! Under `normal_only` a flagged reading is scored and then *not* learned
//! from, so it cannot pull its own baseline — except in warm-up, which
//! learns everything, and once a run of flagged readings has lasted
//! `readapt_after_seconds`, from which point they are learned from until the
//! baseline has caught up and they stop being flagged. That is one rule for
//! every learning method rather than a jump of the level per method, and it
//! is what keeps `normal_only` from flagging a genuine change for ever.

use anyhow::{Result, bail};
use kayak_core::streaming::{DetectLearn, DetectMethod, DetectMode, DetectTransformConfig};
use serde_json::{Value, json};
use std::sync::Arc;

use super::keyed::{Reading, Series, Window};
use crate::{
    BuildCtx, fields,
    inputs::MessageBatch,
    stats,
    transforms::{BuildTransform, Transform},
};

/// The warm-up for a method with no window of its own.
const DEFAULT_MIN_SAMPLES: usize = 30;
/// The MAD-to-σ scale, and the 0.6745 the modified z-score is spelled with.
const MAD_SCALE: f64 = 1.4826;
/// How many standardised readings the Western Electric rules look back over.
const WESTERN_ELECTRIC_RUN: usize = 8;

impl BuildTransform for DetectTransformConfig {
    fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn Transform>> {
        if self.field.trim().is_empty() {
            bail!("a detect transform needs a 'field'");
        }
        let (method, window) = resolve(&self.method)?;
        let learns = matches!(method, Method::Zscore { .. } | Method::Mad { .. } | Method::Ewma { .. });
        if self.learn == DetectLearn::NormalOnly && !learns {
            bail!(
                "'learn: normal_only' is for the methods that keep learning their baseline \
                 (zscore, mad, ewma) — this one freezes its baseline after the warm-up, or has none"
            );
        }
        let readapt_millis = match self.readapt_after_seconds {
            None => None,
            Some(_) if self.learn != DetectLearn::NormalOnly => bail!(
                "'readapt_after_seconds' only means something beside 'learn: normal_only' — \
                 with 'all', flagged readings are learned from already"
            ),
            Some(seconds) => Some(millis(positive("readapt_after_seconds", seconds)?)),
        };
        if self.time.is_some() && !matches!(method, Method::Ewma { .. }) && readapt_millis.is_none() {
            bail!(
                "detect's 'time' is only read by the ewma method and by 'readapt_after_seconds' — \
                 every other method is about the order of the readings, not when they came"
            );
        }
        let min_samples = self.min_samples.unwrap_or_else(|| window.unwrap_or(DEFAULT_MIN_SAMPLES));
        if min_samples == 0 && matches!(method, Method::Cusum { target: None, .. } | Method::EwmaChart { .. } | Method::WesternElectric) {
            bail!("this detect method takes its baseline from the warm-up, so 'min_samples' cannot be zero");
        }
        let output = self.output.unwrap_or_else(|| "anomaly".to_string());
        if output.trim().is_empty() {
            bail!("a detect 'as' cannot be blank");
        }
        let series = Series::resolve(
            ctx,
            "detect",
            format!("detect:{output}"),
            self.group_by,
            self.time,
            self.on_missing,
            self.gate,
        )?;
        Ok(Box::new(DetectTransform {
            series,
            field: self.field,
            method,
            mode: self.mode,
            min_samples,
            output,
            with_baseline: self.with_baseline,
            learning: Learning {
                normal_only: self.learn == DetectLearn::NormalOnly,
                readapt_millis,
            },
        }))
    }
}

/// A number that has to be above zero, or the config's mistake named.
fn positive(name: &str, value: f64) -> Result<f64> {
    if value <= 0.0 || !value.is_finite() {
        bail!("a detect '{name}' has to be more than zero, not {value}");
    }
    Ok(value)
}

/// The method with its defaults filled in and its spelling checked, and the
/// window it holds when it holds one — the default warm-up.
fn resolve(method: &DetectMethod) -> Result<(Method, Option<usize>)> {
    Ok(match method {
        DetectMethod::Zscore { size, threshold } => {
            if *size < 2 {
                bail!("a zscore window needs a 'size' of at least two");
            }
            (Method::Zscore { size: *size, threshold: positive("threshold", threshold.unwrap_or(3.0))? }, Some(*size))
        }
        DetectMethod::Mad { size, threshold } => {
            if *size < 3 {
                bail!("a mad window needs a 'size' of at least three");
            }
            (Method::Mad { size: *size, threshold: positive("threshold", threshold.unwrap_or(3.5))? }, Some(*size))
        }
        DetectMethod::Cusum { target, drift, threshold } => {
            if *drift < 0.0 || !drift.is_finite() {
                bail!("a cusum 'drift' has to be zero or more");
            }
            (
                Method::Cusum { target: *target, drift: *drift, threshold: positive("threshold", *threshold)? },
                None,
            )
        }
        DetectMethod::EwmaChart { alpha, threshold } => {
            let alpha = alpha.unwrap_or(0.2);
            if !(0.0 < alpha && alpha <= 1.0) {
                bail!("an ewma_chart 'alpha' is a weight above 0 and up to 1, not {alpha}");
            }
            (Method::EwmaChart { alpha, threshold: positive("threshold", threshold.unwrap_or(3.0))? }, None)
        }
        DetectMethod::WesternElectric {} => (Method::WesternElectric, None),
        DetectMethod::Flatline { size } => {
            if *size < 2 {
                bail!("a flatline needs a 'size' of at least two");
            }
            (Method::Flatline { size: *size }, Some(*size))
        }
        DetectMethod::Ewma {
            mean_tau_seconds,
            spread_tau_seconds,
            threshold,
            min_spread,
        } => {
            let min_spread = min_spread.unwrap_or(0.0);
            if min_spread < 0.0 || !min_spread.is_finite() {
                bail!("an ewma 'min_spread' has to be zero or more, not {min_spread}");
            }
            (
                Method::Ewma {
                    mean_tau_millis: positive("mean_tau_seconds", *mean_tau_seconds)? * 1000.0,
                    spread_tau_millis: positive("spread_tau_seconds", *spread_tau_seconds)? * 1000.0,
                    threshold: positive("threshold", threshold.unwrap_or(3.0))?,
                    min_spread,
                },
                None,
            )
        }
    })
}

#[allow(clippy::cast_possible_truncation, reason = "seconds to millis, well within range")]
fn millis(seconds: f64) -> i64 {
    (seconds * 1000.0).round() as i64
}

#[derive(Clone, Copy, Debug)]
enum Method {
    Zscore { size: usize, threshold: f64 },
    Mad { size: usize, threshold: f64 },
    Cusum { target: Option<f64>, drift: f64, threshold: f64 },
    EwmaChart { alpha: f64, threshold: f64 },
    WesternElectric,
    Flatline { size: usize },
    Ewma { mean_tau_millis: f64, spread_tau_millis: f64, threshold: f64, min_spread: f64 },
}

/// What a learning method learns from — `learn` and `readapt_after_seconds`,
/// resolved.
#[derive(Clone, Copy, Debug, Default)]
struct Learning {
    normal_only: bool,
    readapt_millis: Option<i64>,
}

impl Learning {
    /// Whether this reading is learned from, keeping the key's run of flagged
    /// readings up to date on the way. Warm-up learns everything: there is no
    /// normal yet for a reading to be outside of.
    fn learns(self, state: &mut Value, anomaly: bool, warm: bool, now: i64) -> bool {
        if !anomaly {
            if let Some(object) = state.as_object_mut() {
                object.remove("flagged_since");
            }
            return true;
        }
        if !self.normal_only || !warm {
            return true;
        }
        let since = state["flagged_since"].as_i64().unwrap_or(now);
        state["flagged_since"] = json!(since);
        self.readapt_millis.is_some_and(|readapt| now - since >= readapt)
    }
}

/// What one reading was judged to be.
#[derive(Clone, Debug, PartialEq)]
struct Verdict {
    anomaly: bool,
    score: Option<f64>,
    /// Which Western Electric rule fired, when one did.
    rule: Option<u8>,
    /// What normal was, in the field's units.
    expected: Option<f64>,
    /// How far from `expected` a reading had to be to be flagged.
    band: Option<f64>,
}

impl Verdict {
    const fn quiet() -> Self {
        Self { anomaly: false, score: None, rule: None, expected: None, band: None }
    }

    fn scored(score: f64, threshold: f64) -> Self {
        Self { anomaly: score > threshold, score: Some(score), ..Self::quiet() }
    }

    /// A departure from a baseline with no spread: flagged, unscored.
    const fn flat_departure(departed: bool) -> Self {
        Self { anomaly: departed, ..Self::quiet() }
    }

    /// The same verdict, saying what it was judged against.
    const fn against(self, expected: f64, band: Option<f64>) -> Self {
        Self { expected: Some(expected), band, ..self }
    }
}

/// The frozen baseline of a chart method, taken from the warm-up.
fn baseline(state: &mut Value, value: f64, min_samples: usize) -> Option<(f64, f64)> {
    if let (Some(mean), Some(std)) = (state["mean"].as_f64(), state["std"].as_f64()) {
        return Some((mean, std));
    }
    Window::push(&mut state["warm"], 0, json!(value), min_samples.max(1), None);
    let warm = Window::numbers(Window::points(&state["warm"]));
    if warm.len() < min_samples {
        return None;
    }
    let (mean, std) = (stats::mean(&warm)?, stats::stddev(&warm)?);
    state["mean"] = json!(mean);
    state["std"] = json!(std);
    state["warm"] = Value::Null;
    Some((mean, std))
}

impl Method {
    /// Judge one reading against the key's state, which is edited in place.
    #[allow(
        clippy::float_cmp,
        reason = "exact equality is the question asked of a baseline with no spread"
    )]
    #[allow(clippy::too_many_lines, reason = "one arm per method, each short")]
    fn judge(self, state: &mut Value, value: f64, now: i64, min_samples: usize, learning: Learning) -> Verdict {
        if !state.is_object() {
            *state = json!({});
        }
        let seen = usize::try_from(state["n"].as_u64().unwrap_or(0)).unwrap_or(usize::MAX);
        state["n"] = json!(seen + 1);
        let warm = seen >= min_samples;

        match self {
            Method::Zscore { size, threshold } => {
                let before = Window::numbers(Window::points(&state["window"]));
                let verdict = if !warm || before.len() < 2 {
                    Verdict::quiet()
                } else {
                    match (stats::mean(&before), stats::stddev(&before)) {
                        (Some(mean), Some(std)) if std > 0.0 => {
                            Verdict::scored((value - mean).abs() / std, threshold).against(mean, Some(threshold * std))
                        }
                        (Some(mean), _) => Verdict::flat_departure(value != mean).against(mean, Some(0.0)),
                        _ => Verdict::quiet(),
                    }
                };
                if learning.learns(state, verdict.anomaly, warm, now) {
                    Window::push(&mut state["window"], 0, json!(value), size, None);
                }
                verdict
            }
            Method::Mad { size, threshold } => {
                let before = Window::numbers(Window::points(&state["window"]));
                let verdict = if !warm || before.len() < 3 {
                    Verdict::quiet()
                } else {
                    match (stats::median(&before), stats::mad(&before)) {
                        (Some(median), Some(mad)) if mad > 0.0 => {
                            Verdict::scored((value - median).abs() / (MAD_SCALE * mad), threshold)
                                .against(median, Some(threshold * MAD_SCALE * mad))
                        }
                        (Some(median), _) => Verdict::flat_departure(value != median).against(median, Some(0.0)),
                        _ => Verdict::quiet(),
                    }
                };
                if learning.learns(state, verdict.anomaly, warm, now) {
                    Window::push(&mut state["window"], 0, json!(value), size, None);
                }
                verdict
            }
            Method::Ewma { mean_tau_millis, spread_tau_millis, threshold, min_spread } => {
                let (Some(mean), Some(variance), Some(at)) =
                    (state["mean"].as_f64(), state["variance"].as_f64(), state["at"].as_i64())
                else {
                    state["mean"] = json!(value);
                    state["variance"] = json!(0.0);
                    state["at"] = json!(now);
                    return Verdict::quiet();
                };
                let spread = variance.sqrt().max(min_spread);
                let verdict = if !warm {
                    Verdict::quiet()
                } else if spread > 0.0 {
                    Verdict::scored((value - mean).abs() / spread, threshold).against(mean, Some(threshold * spread))
                } else {
                    Verdict::flat_departure(value != mean).against(mean, Some(0.0))
                };
                if learning.learns(state, verdict.anomaly, warm, now) {
                    // a reading older than the last lasted no time at all
                    #[allow(clippy::cast_precision_loss, reason = "a gap in millis")]
                    let elapsed = (now - at).max(0) as f64;
                    let diff = value - mean;
                    let towards = 1.0 - (-elapsed / mean_tau_millis).exp();
                    let widen = 1.0 - (-elapsed / spread_tau_millis).exp();
                    state["mean"] = json!(mean + towards * diff);
                    state["variance"] = json!((1.0 - widen) * (variance + widen * diff * diff));
                }
                state["at"] = json!(now.max(at));
                verdict
            }
            Method::Cusum { target, drift, threshold } => {
                let target = match target {
                    Some(t) => t,
                    None => match baseline(state, value, min_samples) {
                        Some((mean, _)) => mean,
                        None => return Verdict::quiet(),
                    },
                };
                if !warm {
                    return Verdict::quiet();
                }
                let up = (state["up"].as_f64().unwrap_or(0.0) + (value - target) - drift).max(0.0);
                let down = (state["down"].as_f64().unwrap_or(0.0) + (target - value) - drift).max(0.0);
                let score = up.max(down);
                let anomaly = score >= threshold;
                state["up"] = json!(if anomaly && up >= threshold { 0.0 } else { up });
                state["down"] = json!(if anomaly && down >= threshold { 0.0 } else { down });
                // the threshold is on the accumulated drift, not on the value,
                // so there is no band in the field's units to report
                Verdict { anomaly, score: Some(score), ..Verdict::quiet() }.against(target, None)
            }
            Method::EwmaChart { alpha, threshold } => {
                let Some((mean, std)) = baseline(state, value, min_samples) else {
                    return Verdict::quiet();
                };
                let smoothed = match state["ewma"].as_f64() {
                    None => value,
                    Some(prev) => alpha * value + (1.0 - alpha) * prev,
                };
                state["ewma"] = json!(smoothed);
                if !warm {
                    return Verdict::quiet();
                }
                let width = std * (alpha / (2.0 - alpha)).sqrt();
                if width > 0.0 {
                    Verdict::scored((smoothed - mean).abs() / width, threshold).against(mean, Some(threshold * width))
                } else {
                    Verdict::flat_departure(smoothed != mean).against(mean, Some(0.0))
                }
            }
            Method::WesternElectric => {
                let Some((mean, std)) = baseline(state, value, min_samples) else {
                    return Verdict::quiet();
                };
                if !warm {
                    return Verdict::quiet();
                }
                if std <= 0.0 {
                    return Verdict::flat_departure(value != mean).against(mean, Some(0.0));
                }
                let z = (value - mean) / std;
                Window::push(&mut state["run"], 0, json!(z), WESTERN_ELECTRIC_RUN, None);
                let run = Window::numbers(Window::points(&state["run"]));
                let rule = western_electric(&run);
                // the band is rule one's: the other three are about runs, and
                // have no single distance to report
                Verdict { anomaly: rule.is_some(), score: Some(z.abs()), rule, ..Verdict::quiet() }
                    .against(mean, Some(3.0 * std))
            }
            Method::Flatline { size } => {
                Window::push(&mut state["window"], 0, json!(value), size, None);
                let window = Window::numbers(Window::points(&state["window"]));
                let flat = warm && window.len() >= size && window.iter().all(|w| *w == value);
                Verdict::flat_departure(flat)
            }
        }
    }
}

/// The first Western Electric rule the newest of `run` breaks, over the
/// standardised readings oldest-first.
fn western_electric(run: &[f64]) -> Option<u8> {
    let newest = *run.last()?;
    if newest.abs() > 3.0 {
        return Some(1);
    }
    let above = newest > 0.0;
    let beyond = |last: usize, limit: f64, needed: usize| {
        run.len() >= last
            && run[run.len() - last..]
                .iter()
                .filter(|z| (**z > 0.0) == above && z.abs() > limit)
                .count()
                >= needed
    };
    if beyond(3, 2.0, 2) {
        return Some(2);
    }
    if beyond(5, 1.0, 4) {
        return Some(3);
    }
    if beyond(8, 0.0, 8) {
        return Some(4);
    }
    None
}

pub struct DetectTransform {
    series: Series,
    field: String,
    method: Method,
    mode: DetectMode,
    min_samples: usize,
    output: String,
    with_baseline: bool,
    learning: Learning,
}

#[async_trait::async_trait]
impl Transform for DetectTransform {
    async fn apply(&mut self, batch: Arc<MessageBatch>) -> Result<Vec<Arc<MessageBatch>>> {
        let mut out: MessageBatch = Vec::with_capacity(batch.len());
        for message in batch.iter() {
            let Some(key) = self.series.key(message)? else {
                if self.mode == DetectMode::Annotate {
                    out.push(Arc::clone(message));
                }
                continue;
            };
            let Reading::Value(value) = self.series.number(message, &self.field)? else {
                if self.mode == DetectMode::Annotate {
                    out.push(Arc::clone(message));
                }
                continue;
            };
            let now = self.series.millis(message)?;
            let verdict = self.series.update(&key, |state| {
                self.method.judge(state, value, now, self.min_samples, self.learning)
            })?;
            if self.mode == DetectMode::OnlyAnomalies && !verdict.anomaly {
                continue;
            }
            let mut written = (**message).clone();
            fields::set(&mut written, &self.output, Value::Bool(verdict.anomaly))?;
            fields::set(&mut written, &format!("{}_score", self.output), verdict.score.map_or(Value::Null, Value::from))?;
            if matches!(self.method, Method::WesternElectric) {
                fields::set(&mut written, &format!("{}_rule", self.output), verdict.rule.map_or(Value::Null, Value::from))?;
            }
            if self.with_baseline {
                fields::set(&mut written, &format!("{}_expected", self.output), verdict.expected.map_or(Value::Null, Value::from))?;
                fields::set(&mut written, &format!("{}_band", self.output), verdict.band.map_or(Value::Null, Value::from))?;
            }
            out.push(Arc::new(written));
        }
        if out.is_empty() {
            return Ok(vec![]);
        }
        Ok(vec![Arc::new(out)])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{batch, ctx_with_bucket};
    use kayak_core::config::MissingFieldPolicy;

    fn config(method: DetectMethod, min_samples: Option<usize>) -> DetectTransformConfig {
        DetectTransformConfig {
            gate: kayak_core::streaming::Gate::default(),
            field: "v".into(),
            method,
            mode: DetectMode::Annotate,
            min_samples,
            output: None,
            group_by: vec![],
            on_missing: MissingFieldPolicy::Error,
            with_baseline: false,
            learn: DetectLearn::All,
            readapt_after_seconds: None,
            time: None,
        }
    }

    async fn flags(config: DetectTransformConfig, values: &[f64]) -> Result<Vec<Value>> {
        let mut pipelines = std::collections::HashMap::new();
        let mut ctx = ctx_with_bucket(&mut pipelines, None);
        let mut transform = config.build(&mut ctx)?;
        let messages = values.iter().map(|v| json!({"v": v})).collect();
        let out = transform.apply(batch(messages)).await?;
        Ok(out.iter().flat_map(|b| b.iter().map(|m| (**m).clone())).collect())
    }

    fn anomalies(out: &[Value]) -> Vec<bool> {
        out.iter().map(|m| m["anomaly"].as_bool().unwrap_or(false)).collect()
    }

    #[tokio::test]
    async fn zscore_measures_against_the_window_before_and_not_during_warm_up() -> Result<()> {
        let out = flags(
            config(DetectMethod::Zscore { size: 4, threshold: Some(2.0) }, None),
            &[10.0, 12.0, 11.0, 13.0, 50.0, 12.0],
        )
        .await?;
        assert_eq!(anomalies(&out), vec![false, false, false, false, true, false]);
        assert_eq!(out[3]["anomaly_score"], json!(null), "warm-up is unscored");
        assert!(out[4]["anomaly_score"].as_f64().unwrap_or(0.0) > 2.0);
        Ok(())
    }

    #[tokio::test]
    async fn mad_is_robust_to_an_outlier_in_its_own_baseline() -> Result<()> {
        // the 50 in the window would wreck a z-score baseline; the median and
        // MAD are unmoved by it
        let out = flags(
            config(DetectMethod::Mad { size: 5, threshold: Some(3.5) }, Some(3)),
            &[10.0, 11.0, 10.0, 50.0, 11.0, 10.0, 30.0],
        )
        .await?;
        assert_eq!(anomalies(&out), vec![false, false, false, true, false, false, true]);
        Ok(())
    }

    #[tokio::test]
    async fn cusum_catches_a_small_sustained_shift() -> Result<()> {
        let mut readings = vec![10.0; 10];
        readings.extend(std::iter::repeat_n(10.6, 10));
        let out = flags(
            config(DetectMethod::Cusum { target: Some(10.0), drift: 0.1, threshold: 1.9 }, Some(0)),
            &readings,
        )
        .await?;
        let flagged: Vec<usize> = anomalies(&out).iter().enumerate().filter(|(_, f)| **f).map(|(i, _)| i).collect();
        assert_eq!(flagged, vec![13, 17], "fires once four steps of 0.5 have accumulated, resets, fires again");

        // with no target the warm-up mean is the target
        let out = flags(
            config(DetectMethod::Cusum { target: None, drift: 0.1, threshold: 1.9 }, Some(5)),
            &readings,
        )
        .await?;
        assert!(anomalies(&out)[13] && !anomalies(&out)[9]);
        Ok(())
    }

    #[tokio::test]
    async fn the_chart_methods_freeze_their_baseline_at_the_end_of_warm_up() -> Result<()> {
        let mut readings: Vec<f64> = (0..10).map(|i| 10.0 + f64::from(i % 2)).collect(); // 10, 11, 10, 11 …
        readings.extend([10.0, 11.0, 20.0, 20.0, 20.0, 20.0]);
        let out = flags(
            config(DetectMethod::EwmaChart { alpha: Some(0.5), threshold: Some(3.0) }, Some(10)),
            &readings,
        )
        .await?;
        let flagged = anomalies(&out);
        assert!(!flagged[11] && flagged[12], "{flagged:?}");

        let out = flags(config(DetectMethod::WesternElectric {}, Some(10)), &readings).await?;
        assert_eq!(out[12]["anomaly_rule"], json!(1), "20 is more than 3σ from a 10.5 ± 0.5 baseline");
        assert_eq!(out[11]["anomaly_rule"], json!(null));
        Ok(())
    }

    #[tokio::test]
    async fn western_electric_rules_beyond_the_first() {
        assert_eq!(western_electric(&[0.0, 2.5, 0.5, 2.5]), Some(2), "two of three beyond 2σ");
        assert_eq!(western_electric(&[1.5, 1.5, 0.0, 1.5, 1.5]), Some(3), "four of five beyond 1σ");
        assert_eq!(western_electric(&[0.5; 8]), Some(4), "eight on one side");
        assert_eq!(western_electric(&[0.5, -0.5, 0.5, -0.5, 0.5, -0.5, 0.5, -0.5]), None);
        assert_eq!(western_electric(&[2.5, -2.5, 2.5]), Some(2), "same side only counts");
    }

    #[tokio::test]
    async fn flatline_and_only_anomalies_mode() -> Result<()> {
        let mut config = config(DetectMethod::Flatline { size: 3 }, None);
        config.mode = DetectMode::OnlyAnomalies;
        let out = flags(config, &[1.0, 1.0, 2.0, 2.0, 2.0, 2.0, 3.0]).await?;
        assert_eq!(out.len(), 2, "only the two flat readings come out");
        assert!(out.iter().all(|m| m["anomaly"] == json!(true)));
        Ok(())
    }

    #[test]
    fn contradictions_are_refused_at_build() {
        let mut pipelines = std::collections::HashMap::new();
        let mut ctx = ctx_with_bucket(&mut pipelines, None);
        for (method, min) in [
            (DetectMethod::Zscore { size: 1, threshold: None }, None),
            (DetectMethod::Zscore { size: 5, threshold: Some(0.0) }, None),
            (DetectMethod::Cusum { target: None, drift: 0.1, threshold: 1.0 }, Some(0)),
            (DetectMethod::EwmaChart { alpha: Some(0.0), threshold: None }, None),
            (DetectMethod::Flatline { size: 1 }, None),
        ] {
            assert!(config(method.clone(), min).build(&mut ctx).is_err(), "{method:?} should be refused");
        }
    }

    /// Readings `(seconds, value)` with their time in `t`.
    async fn flags_at(config: DetectTransformConfig, readings: &[(i64, f64)]) -> Result<Vec<Value>> {
        let mut pipelines = std::collections::HashMap::new();
        let mut ctx = ctx_with_bucket(&mut pipelines, None);
        let mut transform = config.build(&mut ctx)?;
        let messages = readings.iter().map(|(t, v)| json!({"t": t * 1000, "v": v})).collect();
        let out = transform.apply(batch(messages)).await?;
        Ok(out.iter().flat_map(|b| b.iter().map(|m| (**m).clone())).collect())
    }

    /// Ten readings around 10.5, then a step to 20 held for ten seconds.
    fn a_step() -> Vec<(i64, f64)> {
        let mut readings: Vec<(i64, f64)> = (0..10).map(|t| (t, if t % 2 == 0 { 10.0 } else { 11.0 })).collect();
        readings.extend((10..20).map(|t| (t, 20.0)));
        readings
    }

    fn zscore_learning(learn: DetectLearn, readapt_after_seconds: Option<f64>) -> DetectTransformConfig {
        let mut config = config(DetectMethod::Zscore { size: 5, threshold: Some(3.0) }, Some(5));
        config.learn = learn;
        config.readapt_after_seconds = readapt_after_seconds;
        config.time = readapt_after_seconds.map(|_| "t".to_string());
        config
    }

    /// The point of `normal_only`: a flagged reading is kept out of the window,
    /// so a step is flagged for as long as it lasts instead of being learned
    /// as normal within a few readings.
    #[tokio::test]
    async fn normal_only_keeps_a_flagged_reading_out_of_its_own_baseline() -> Result<()> {
        let step = &anomalies(&flags_at(zscore_learning(DetectLearn::All, None), &a_step()).await?)[10..];
        assert!(step[0], "the step is flagged");
        assert!(!step[9], "learning everything makes the step normal: {step:?}");

        let step = &anomalies(&flags_at(zscore_learning(DetectLearn::NormalOnly, None), &a_step()).await?)[10..];
        assert!(step.iter().all(|flagged| *flagged), "flagged for as long as it lasts: {step:?}");
        Ok(())
    }

    /// `readapt_after_seconds` is the way out: three seconds into a run of
    /// flagged readings they are learned from, until the baseline has caught
    /// up and they stop being flagged.
    #[tokio::test]
    async fn readapt_learns_a_lasting_change_after_its_time() -> Result<()> {
        let config = zscore_learning(DetectLearn::NormalOnly, Some(3.0));
        let step = anomalies(&flags_at(config, &a_step()).await?)[10..].to_vec();
        assert_eq!(&step[..3], &[true, true, true], "{step:?}");
        assert!(!step[9], "the new level became normal: {step:?}");
        Ok(())
    }

    /// `with_baseline` writes what normal was and how far from it counted, in
    /// the field's units — `null` while there is no baseline yet.
    #[tokio::test]
    async fn with_baseline_writes_what_a_reading_was_judged_against() -> Result<()> {
        let mut config = config(DetectMethod::Zscore { size: 5, threshold: Some(3.0) }, Some(5));
        config.with_baseline = true;
        let out = flags(config, &[10.0, 11.0, 10.0, 11.0, 10.0, 30.0]).await?;
        assert_eq!(out[0]["anomaly_expected"], Value::Null, "warm-up has no baseline");
        assert_eq!(out[0]["anomaly_band"], Value::Null);
        let expected = out[5]["anomaly_expected"].as_f64().unwrap_or_default();
        let band = out[5]["anomaly_band"].as_f64().unwrap_or_default();
        assert!((expected - 10.4).abs() < 1e-9, "{expected}");
        assert!((band - 3.0 * 0.24f64.sqrt()).abs() < 1e-9, "{band}");
        assert_eq!(out[5]["anomaly"], json!(true));

        // and nothing extra without it
        let out = flags(config_without_baseline(), &[1.0, 2.0]).await?;
        assert!(out[1].get("anomaly_expected").is_none());
        Ok(())
    }

    fn config_without_baseline() -> DetectTransformConfig {
        config(DetectMethod::Zscore { size: 5, threshold: None }, Some(1))
    }

    fn ewma(min_spread: Option<f64>, learn: DetectLearn) -> DetectTransformConfig {
        let mut config = config(
            DetectMethod::Ewma {
                mean_tau_seconds: 10.0,
                spread_tau_seconds: 60.0,
                threshold: Some(4.0),
                min_spread,
            },
            Some(3),
        );
        config.learn = learn;
        config.with_baseline = true;
        config.time = Some("t".into());
        config
    }

    /// The mean follows by time: one τ after the last reading it has moved
    /// 1 − 1/e of the way, whatever the count of readings.
    #[tokio::test]
    async fn the_ewma_baseline_follows_by_time() -> Result<()> {
        let mut config = ewma(Some(100.0), DetectLearn::All);
        config.min_samples = Some(1);
        let out = flags_at(config, &[(0, 0.0), (10, 1.0), (20, 1.0)]).await?;
        let after_one_tau = out[2]["anomaly_expected"].as_f64().unwrap_or_default();
        assert!((after_one_tau - (1.0 - (-1.0f64).exp())).abs() < 1e-12, "{after_one_tau}");
        Ok(())
    }

    /// `min_spread` is the smallest deviation believed: a signal that has been
    /// perfectly flat does not flag its first small wobble.
    #[tokio::test]
    async fn min_spread_keeps_a_flat_signals_first_wobble_quiet() -> Result<()> {
        let readings: Vec<(i64, f64)> = (0..6).map(|t| (t, 5.0)).chain([(6, 5.5)]).collect();
        let strict = anomalies(&flags_at(ewma(None, DetectLearn::All), &readings).await?);
        assert!(strict[6], "with no floor, any departure from a flat baseline is flagged");
        let floored = anomalies(&flags_at(ewma(Some(1.0), DetectLearn::All), &readings).await?);
        assert!(!floored[6], "half a unit is inside four of a floor of one");
        Ok(())
    }

    #[tokio::test]
    async fn an_ewma_spike_under_normal_only_does_not_move_normal() -> Result<()> {
        let readings = [(0, 5.0), (1, 5.0), (2, 5.0), (3, 5.0), (4, 100.0), (5, 5.0)];
        let out = flags_at(ewma(Some(0.5), DetectLearn::NormalOnly), &readings).await?;
        assert_eq!(out[4]["anomaly"], json!(true));
        assert_eq!(out[5]["anomaly_expected"], out[4]["anomaly_expected"], "the spike was learned from");
        let out = flags_at(ewma(Some(0.5), DetectLearn::All), &readings).await?;
        assert_ne!(out[5]["anomaly_expected"], out[4]["anomaly_expected"]);
        Ok(())
    }

    #[test]
    fn learning_settings_that_mean_nothing_are_refused() {
        let mut pipelines = std::collections::HashMap::new();
        let mut ctx = ctx_with_bucket(&mut pipelines, None);
        let mut frozen = config(DetectMethod::EwmaChart { alpha: None, threshold: None }, None);
        frozen.learn = DetectLearn::NormalOnly;
        let readapt_without_normal_only = zscore_learning(DetectLearn::All, Some(3.0));
        let mut time_unread = config(DetectMethod::Zscore { size: 5, threshold: None }, None);
        time_unread.time = Some("t".into());
        let mut no_tau = ewma(None, DetectLearn::All);
        no_tau.method = DetectMethod::Ewma {
            mean_tau_seconds: 0.0,
            spread_tau_seconds: 60.0,
            threshold: None,
            min_spread: None,
        };
        for (what, config) in [
            ("normal_only on a frozen baseline", frozen),
            ("readapt without normal_only", readapt_without_normal_only),
            ("a time nothing reads", time_unread),
            ("a zero time constant", no_tau),
        ] {
            assert!(config.build(&mut ctx).is_err(), "{what} was accepted");
        }
    }
}
