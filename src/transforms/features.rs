//! `features`: a window of readings in, one descriptor out.
//!
//! The declaration is [`kayak_core::streaming::FeaturesTransformConfig`].
//! This is the reducer's shape — a batch in, one message per group out, the
//! group fields kept under their leaf names — with a closed set of waveform
//! descriptors in place of the aggregation list, and it shares the reducer's
//! grouping outright. Nothing here keeps state, so unlike the rest of the
//! streaming family it needs no bucket.
//!
//! Every feature is a function in [`crate::stats`] over the window's numbers;
//! this module only decides the sample rate and where the answers go. The
//! sample rate is `sample_rate_hz` when given — a source whose timestamps are
//! coarse (whole seconds on a 2 kHz waveform) would otherwise derive
//! nonsense — and otherwise `(n − 1) / duration` off the `time` field.
//! A window that can't say (one reading, no time passed) makes the spectral
//! features `null` rather than failing the batch: a short cycle is data.

use anyhow::{Result, bail};
use kayak_core::config::MissingFieldPolicy;
use kayak_core::streaming::{Band, FeatureKind, FeaturesTransformConfig};
use serde_json::{Map, Value};
use std::collections::HashSet;
use std::sync::Arc;

use super::reduce::{group_batch, present};
use crate::{
    BuildCtx, fields,
    inputs::MessageBatch,
    stats,
    time::MessageTime,
    transforms::{BuildTransform, Transform},
};

impl BuildTransform for FeaturesTransformConfig {
    fn build(self, _ctx: &mut BuildCtx) -> Result<Box<dyn Transform>> {
        if self.field.trim().is_empty() {
            bail!("a features transform needs a 'field'");
        }
        if self.include.is_empty() && self.bands.is_empty() {
            bail!("a features transform needs at least one feature in 'include' or one of 'bands'");
        }
        let mut names: HashSet<String> = HashSet::new();
        for feature in &self.include {
            if !names.insert(feature.field_name().to_string()) {
                bail!("the '{}' feature is listed twice", feature.field_name());
            }
        }
        for band in &self.bands {
            let name = band.output.trim();
            if name.is_empty() {
                bail!("a band needs an 'as' to write its power under");
            }
            if !(band.low_hz >= 0.0 && band.high_hz > band.low_hz && band.high_hz.is_finite()) {
                bail!("the '{name}' band has to run from a low_hz of zero or more to a higher high_hz");
            }
            if !names.insert(name.to_string()) {
                bail!("two features would both be written as '{name}'");
            }
        }
        for group in &self.group_by {
            let leaf = fields::leaf(group);
            if names.contains(leaf) {
                bail!("the '{leaf}' feature would overwrite the group_by field of that name");
            }
        }
        let spectral = self.include.iter().any(|f| f.is_spectral()) || !self.bands.is_empty();
        if spectral && self.time.is_none() && self.sample_rate_hz.is_none() {
            bail!(
                "the spectral features (dominant_frequency, bands) need a sample rate: give \
                 'sample_rate_hz', or a 'time' field to derive one from"
            );
        }
        if self.sample_rate_hz.is_some_and(|fs| fs <= 0.0 || !fs.is_finite()) {
            bail!("'sample_rate_hz' has to be more than zero");
        }
        Ok(Box::new(FeaturesTransform {
            field: self.field,
            include: self.include,
            bands: self.bands,
            group_by: self.group_by,
            time: MessageTime::new(self.time),
            sample_rate: self.sample_rate_hz,
            on_missing: self.on_missing,
        }))
    }
}

pub struct FeaturesTransform {
    field: String,
    include: Vec<FeatureKind>,
    bands: Vec<Band>,
    group_by: Vec<String>,
    time: MessageTime,
    sample_rate: Option<f64>,
    on_missing: MissingFieldPolicy,
}

/// The numbers of one window, with their times when there are any.
struct Window {
    values: Vec<f64>,
    /// Seconds, oldest first — only when the config names a `time` field.
    seconds: Option<Vec<f64>>,
}

impl Window {
    fn duration(&self) -> Option<f64> {
        let seconds = self.seconds.as_ref()?;
        Some(seconds.last()? - seconds.first()?)
    }

    /// The sample rate the spectral features run at.
    fn sample_rate(&self, configured: Option<f64>) -> Option<f64> {
        if let Some(fs) = configured {
            return Some(fs);
        }
        let duration = self.duration()?;
        if duration <= 0.0 || self.values.len() < 2 {
            return None;
        }
        #[allow(clippy::cast_precision_loss)]
        Some((self.values.len() as f64 - 1.0) / duration)
    }

    fn feature(&self, feature: FeatureKind, sample_rate: Option<f64>) -> Value {
        let v = &self.values;
        let answer: Option<f64> = match feature {
            FeatureKind::Mean => stats::mean(v),
            FeatureKind::Std => stats::stddev(v),
            FeatureKind::Min => stats::min(v),
            FeatureKind::Max => stats::max(v),
            FeatureKind::Range => stats::max(v).zip(stats::min(v)).map(|(hi, lo)| hi - lo),
            FeatureKind::Slope => match &self.seconds {
                Some(t) => stats::linfit(t, v).map(|f| f.slope),
                None => stats::linfit_indexed(v).map(|f| f.slope),
            },
            FeatureKind::Skew => stats::skew(v),
            FeatureKind::Kurtosis => stats::kurtosis(v),
            FeatureKind::Rms => stats::rms(v),
            FeatureKind::CrestFactor => stats::crest_factor(v),
            FeatureKind::ZeroCrossings => return Value::from(stats::zero_crossings(v)),
            FeatureKind::NPeaks => return Value::from(stats::peaks(v).len()),
            FeatureKind::Autocorr1 => stats::autocorr(v, 1),
            FeatureKind::DominantFrequency => sample_rate.and_then(|fs| stats::dominant_frequency(v, fs)),
            FeatureKind::Count => return Value::from(v.len()),
            FeatureKind::Duration => self.duration(),
        };
        answer.map_or(Value::Null, Value::from)
    }
}

impl FeaturesTransform {
    /// One group's window: its readings as numbers, with their times.
    fn window(&self, messages: &[Arc<Value>]) -> Result<Window> {
        let mut values = Vec::with_capacity(messages.len());
        let mut seconds = (!self.time.is_arrival()).then(|| Vec::with_capacity(messages.len()));
        for message in messages {
            let value = match present(message, &self.field) {
                Some(value) => value,
                None if self.on_missing == MissingFieldPolicy::Skip => continue,
                None => bail!("field '{}' is missing from a message", self.field),
            };
            let number = value
                .as_f64()
                .ok_or_else(|| anyhow::anyhow!("field '{}' is {}, not a number", self.field, fields::describe(value)))?;
            if let Some(seconds) = seconds.as_mut() {
                #[allow(clippy::cast_precision_loss, reason = "millis to seconds")]
                seconds.push(self.time.millis_of(message)? as f64 / 1000.0);
            }
            values.push(number);
        }
        Ok(Window { values, seconds })
    }
}

#[async_trait::async_trait]
impl Transform for FeaturesTransform {
    async fn apply(&mut self, batch: Arc<MessageBatch>) -> Result<Vec<Arc<MessageBatch>>> {
        if batch.is_empty() {
            return Ok(vec![]);
        }
        let mut out: MessageBatch = Vec::new();
        for group in group_batch(&batch, &self.group_by, self.on_missing)? {
            let window = self.window(&group.messages)?;
            if window.values.is_empty() {
                continue;
            }
            let sample_rate = window.sample_rate(self.sample_rate);
            let mut message = Map::new();
            for (name, value) in self.group_by.iter().zip(&group.key) {
                message.insert(fields::leaf(name).to_string(), value.clone());
            }
            for feature in &self.include {
                message.insert(feature.field_name().to_string(), window.feature(*feature, sample_rate));
            }
            for band in &self.bands {
                let power = sample_rate.and_then(|fs| stats::band_energy(&window.values, fs, band.low_hz, band.high_hz));
                message.insert(band.output.trim().to_string(), power.map_or(Value::Null, Value::from));
            }
            out.push(Arc::new(Value::Object(message)));
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
    use crate::testing::batch;
    use serde_json::json;

    fn config(include: Vec<FeatureKind>) -> FeaturesTransformConfig {
        FeaturesTransformConfig {
            field: "v".into(),
            include,
            bands: vec![],
            group_by: vec![],
            time: None,
            sample_rate_hz: None,
            on_missing: MissingFieldPolicy::Error,
        }
    }

    fn build(config: FeaturesTransformConfig) -> Result<Box<dyn Transform>> {
        let (events, _) = tokio::sync::broadcast::channel(16);
        let mut pipelines = std::collections::HashMap::new();
        let mut ctx = BuildCtx::new(&mut pipelines, "features-test".into(), events);
        config.build(&mut ctx)
    }

    async fn run(config: FeaturesTransformConfig, messages: Vec<Value>) -> Result<Vec<Value>> {
        let out = build(config)?.apply(batch(messages)).await?;
        Ok(out.iter().flat_map(|b| b.iter().map(|m| (**m).clone())).collect())
    }

    #[tokio::test]
    async fn a_window_becomes_one_descriptor_per_group() -> Result<()> {
        let mut config = config(vec![FeatureKind::Mean, FeatureKind::Std, FeatureKind::Count, FeatureKind::Slope]);
        config.group_by = vec!["m".into()];
        let out = run(
            config,
            vec![
                json!({"m": "a", "v": 1}),
                json!({"m": "b", "v": 10}),
                json!({"m": "a", "v": 3}),
                json!({"m": "a", "v": 5}),
            ],
        )
        .await?;
        assert_eq!(
            out,
            vec![
                json!({"m": "a", "mean": 3.0, "std": 2.0_f64.sqrt() * (4.0_f64 / 3.0).sqrt(), "count": 3, "slope": 2.0}),
                json!({"m": "b", "mean": 10.0, "std": 0.0, "count": 1, "slope": null}),
            ]
        );
        Ok(())
    }

    /// The cycle-features case: seconds off the time field give the slope
    /// its unit and the spectral features their sample rate.
    #[tokio::test]
    async fn time_gives_the_slope_seconds_and_the_spectrum_a_rate() -> Result<()> {
        // 8 Hz, so every timestamp is a whole number of milliseconds
        let fs = 8.0;
        let readings: Vec<Value> = (0..32)
            .map(|i| {
                let t = f64::from(i) / fs;
                json!({"ts": i * 125, "v": (2.0 * std::f64::consts::PI * 2.0 * t).sin() + 0.5 * t})
            })
            .collect();
        let mut config = config(vec![FeatureKind::Slope, FeatureKind::Duration, FeatureKind::DominantFrequency, FeatureKind::Rms]);
        config.time = Some("ts".into());
        config.bands = vec![Band { low_hz: 1.5, high_hz: 2.5, output: "around_2hz".into() }];
        let out = run(config, readings).await?;
        let m = &out[0];
        assert!((m["slope"].as_f64().unwrap_or_default() - 0.5).abs() < 0.1, "{m}");
        assert!((m["duration"].as_f64().unwrap_or_default() - 31.0 / fs).abs() < 1e-9, "{m}");
        assert_eq!(m["dominant_frequency"], json!(2.0));
        assert!(m["around_2hz"].as_f64().unwrap_or_default() > 0.3, "{m}");
        Ok(())
    }

    #[tokio::test]
    async fn a_configured_sample_rate_wins_and_a_short_window_reads_null() -> Result<()> {
        let mut config = config(vec![FeatureKind::DominantFrequency, FeatureKind::CrestFactor, FeatureKind::NPeaks, FeatureKind::ZeroCrossings]);
        config.sample_rate_hz = Some(8.0);
        let wave: Vec<Value> = [0.0, 1.0, 0.0, -1.0, 0.0, 1.0, 0.0, -1.0].iter().map(|v| json!({"v": v})).collect();
        let out = run(config.clone(), wave).await?;
        assert_eq!(out[0]["dominant_frequency"], json!(2.0));
        assert_eq!(out[0]["n_peaks"], json!(2));
        assert_eq!(out[0]["zero_crossings"], json!(3));
        let short = run(config, vec![json!({"v": 1.0})]).await?;
        assert_eq!(short[0]["dominant_frequency"], json!(null));
        assert_eq!(short[0]["crest_factor"], json!(1.0));
        Ok(())
    }

    #[tokio::test]
    async fn missing_follows_the_policy() -> Result<()> {
        assert!(run(config(vec![FeatureKind::Mean]), vec![json!({})]).await.is_err());
        let mut lax = config(vec![FeatureKind::Mean]);
        lax.on_missing = MissingFieldPolicy::Skip;
        let out = run(lax.clone(), vec![json!({}), json!({"v": 4})]).await?;
        assert_eq!(out, vec![json!({"mean": 4.0})]);
        assert!(run(lax, vec![json!({})]).await?.is_empty(), "a window of nothing emits nothing");
        Ok(())
    }

    #[test]
    fn contradictions_are_refused_at_build() {
        assert!(build(config(vec![])).is_err(), "nothing to compute");
        assert!(build(config(vec![FeatureKind::Mean, FeatureKind::Mean])).is_err());
        assert!(build(config(vec![FeatureKind::DominantFrequency])).is_err(), "no sample rate");
        let mut clash = config(vec![FeatureKind::Mean]);
        clash.group_by = vec!["x.mean".into()];
        assert!(build(clash).is_err());
        let mut band = config(vec![]);
        band.sample_rate_hz = Some(10.0);
        band.bands = vec![Band { low_hz: 5.0, high_hz: 2.0, output: "b".into() }];
        assert!(build(band).is_err(), "an upside-down band");
    }
}
