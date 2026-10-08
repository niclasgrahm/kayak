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

use anyhow::{Result, bail};
use kayak_core::streaming::{DetectMethod, DetectMode, DetectTransformConfig};
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
        let positive = |name: &str, value: f64| -> Result<f64> {
            if value <= 0.0 || !value.is_finite() {
                bail!("a detect '{name}' has to be more than zero, not {value}");
            }
            Ok(value)
        };
        let (method, window) = match &self.method {
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
        };
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
            None,
            self.on_missing,
        )?;
        Ok(Box::new(DetectTransform {
            series,
            field: self.field,
            method,
            mode: self.mode,
            min_samples,
            output,
        }))
    }
}

#[derive(Clone, Copy, Debug)]
enum Method {
    Zscore { size: usize, threshold: f64 },
    Mad { size: usize, threshold: f64 },
    Cusum { target: Option<f64>, drift: f64, threshold: f64 },
    EwmaChart { alpha: f64, threshold: f64 },
    WesternElectric,
    Flatline { size: usize },
}

/// What one reading was judged to be.
#[derive(Clone, Debug, PartialEq)]
struct Verdict {
    anomaly: bool,
    score: Option<f64>,
    /// Which Western Electric rule fired, when one did.
    rule: Option<u8>,
}

impl Verdict {
    const fn quiet() -> Self {
        Self { anomaly: false, score: None, rule: None }
    }

    fn scored(score: f64, threshold: f64) -> Self {
        Self { anomaly: score > threshold, score: Some(score), rule: None }
    }

    /// A departure from a baseline with no spread: flagged, unscored.
    const fn flat_departure(departed: bool) -> Self {
        Self { anomaly: departed, score: None, rule: None }
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
    fn judge(self, state: &mut Value, value: f64, min_samples: usize) -> Verdict {
        if !state.is_object() {
            *state = json!({});
        }
        let seen = usize::try_from(state["n"].as_u64().unwrap_or(0)).unwrap_or(usize::MAX);
        state["n"] = json!(seen + 1);
        let warm = seen >= min_samples;

        match self {
            Method::Zscore { size, threshold } => {
                let before = Window::numbers(Window::points(&state["window"]));
                Window::push(&mut state["window"], 0, json!(value), size, None);
                if !warm || before.len() < 2 {
                    return Verdict::quiet();
                }
                match (stats::mean(&before), stats::stddev(&before)) {
                    (Some(mean), Some(std)) if std > 0.0 => Verdict::scored((value - mean).abs() / std, threshold),
                    (Some(mean), _) => Verdict::flat_departure(value != mean),
                    _ => Verdict::quiet(),
                }
            }
            Method::Mad { size, threshold } => {
                let before = Window::numbers(Window::points(&state["window"]));
                Window::push(&mut state["window"], 0, json!(value), size, None);
                if !warm || before.len() < 3 {
                    return Verdict::quiet();
                }
                match (stats::median(&before), stats::mad(&before)) {
                    (Some(median), Some(mad)) if mad > 0.0 => {
                        Verdict::scored((value - median).abs() / (MAD_SCALE * mad), threshold)
                    }
                    (Some(median), _) => Verdict::flat_departure(value != median),
                    _ => Verdict::quiet(),
                }
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
                Verdict { anomaly, score: Some(score), rule: None }
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
                    Verdict::scored((smoothed - mean).abs() / width, threshold)
                } else {
                    Verdict::flat_departure(smoothed != mean)
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
                    return Verdict::flat_departure(value != mean);
                }
                let z = (value - mean) / std;
                Window::push(&mut state["run"], 0, json!(z), WESTERN_ELECTRIC_RUN, None);
                let run = Window::numbers(Window::points(&state["run"]));
                let rule = western_electric(&run);
                Verdict { anomaly: rule.is_some(), score: Some(z.abs()), rule }
            }
            Method::Flatline { size } => {
                Window::push(&mut state["window"], 0, json!(value), size, None);
                let window = Window::numbers(Window::points(&state["window"]));
                let flat = warm && window.len() >= size && window.iter().all(|w| *w == value);
                Verdict { anomaly: flat, score: None, rule: None }
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
            let verdict = self
                .series
                .update(&key, |state| self.method.judge(state, value, self.min_samples))?;
            if self.mode == DetectMode::OnlyAnomalies && !verdict.anomaly {
                continue;
            }
            let mut written = (**message).clone();
            fields::set(&mut written, &self.output, Value::Bool(verdict.anomaly))?;
            fields::set(&mut written, &format!("{}_score", self.output), verdict.score.map_or(Value::Null, Value::from))?;
            if matches!(self.method, Method::WesternElectric) {
                fields::set(&mut written, &format!("{}_rule", self.output), verdict.rule.map_or(Value::Null, Value::from))?;
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
            field: "v".into(),
            method,
            mode: DetectMode::Annotate,
            min_samples,
            output: None,
            group_by: vec![],
            on_missing: MissingFieldPolicy::Error,
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
}
