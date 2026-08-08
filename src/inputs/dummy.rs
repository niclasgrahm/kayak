use crate::BuildCtx;
use crate::inputs::BuildInput;
use crate::inputs::InputSource;
use crate::inputs::MessageBatch;
use anyhow::Result;
use chrono::Utc;
use rand::Rng;
use rand::RngExt;
use serde_json::Value;
use serde_json::json;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use kayak_core::config::DummyConfig;
use kayak_core::config::DummyPayload;

/// Peak of the sine wave when the config doesn't say.
const DEFAULT_AMPLITUDE: f64 = 1.0;
/// Seconds for one turn of the sine wave when the config doesn't say.
const DEFAULT_PERIOD: f64 = 60.0;

impl BuildInput for DummyConfig {
    fn build(self, _ctx: &mut BuildCtx) -> Result<Box<dyn InputSource>> {
        Ok(Box::new(DummyInput {
            interval: Duration::from_secs(self.duration),
            payload: self.payload.unwrap_or_default(),
            amplitude: self.amplitude.unwrap_or(DEFAULT_AMPLITUDE),
            period: self.period.unwrap_or(DEFAULT_PERIOD),
            started: Instant::now(),
        }))
    }
}

pub struct DummyInput {
    pub interval: Duration,
    payload: DummyPayload,
    amplitude: f64,
    period: f64,
    /// The wave is sampled against wall clock rather than a message count, so
    /// its period is what the config says whatever `interval` is.
    started: Instant,
}

#[async_trait::async_trait]
impl InputSource for DummyInput {
    async fn next(&mut self) -> Result<Arc<MessageBatch>> {
        tokio::time::sleep(self.interval).await;
        tracing::debug!("Emitting dummy message inside dummy input");
        let value = match self.payload {
            DummyPayload::Number => {
                let elapsed = self.started.elapsed().as_secs_f64();
                json!(sine_at(elapsed, self.amplitude, self.period))
            }
            DummyPayload::Text => Value::String(sentence(&mut rand::rng())),
        };
        Ok(Arc::new(vec![Arc::new(json!({
            "value": value,
            "current_time": Utc::now().to_string(),
        }))]))
    }
}

/// The wave at `elapsed` seconds in. A period of zero (or a negative one) would
/// divide the phase by nothing, so it reads as "no wave" and sits at zero
/// rather than producing a NaN that every downstream then has to survive.
fn sine_at(elapsed: f64, amplitude: f64, period: f64) -> f64 {
    if period <= 0.0 {
        return 0.0;
    }
    amplitude * (std::f64::consts::TAU * elapsed / period).sin()
}

const SUBJECTS: [&str; 8] = [
    "the kayak",
    "a curious otter",
    "the night shift",
    "every pipeline",
    "the river",
    "a tired engineer",
    "the second batch",
    "our best guess",
];

const VERBS: [&str; 8] = [
    "drifts past",
    "measures",
    "forgets",
    "counts",
    "argues with",
    "waits for",
    "repaints",
    "swallows",
];

const OBJECTS: [&str; 8] = [
    "the quiet harbour",
    "a stack of receipts",
    "yesterday's weather",
    "three loose bolts",
    "the last ferry",
    "a very long sentence",
    "the wrong timezone",
    "every open window",
];

/// A sentence assembled from the word banks — subject, verb, object, full stop.
fn sentence<R: Rng + RngExt + ?Sized>(rng: &mut R) -> String {
    let subject = SUBJECTS[rng.random_range(0..SUBJECTS.len())];
    let verb = VERBS[rng.random_range(0..VERBS.len())];
    let object = OBJECTS[rng.random_range(0..OBJECTS.len())];
    let mut sentence = format!("{subject} {verb} {object}.");
    sentence[..1].make_ascii_uppercase();
    sentence
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_wave_starts_at_zero_and_peaks_a_quarter_period_in() {
        assert!((sine_at(0.0, 1.0, 60.0) - 0.0).abs() < 1e-9);
        assert!((sine_at(15.0, 1.0, 60.0) - 1.0).abs() < 1e-9);
        assert!((sine_at(45.0, 1.0, 60.0) + 1.0).abs() < 1e-9);
    }

    #[test]
    fn amplitude_scales_the_wave_and_the_period_is_honoured() {
        assert!((sine_at(1.0, 5.0, 4.0) - 5.0).abs() < 1e-9);
        // One full turn later, back where it started.
        assert!((sine_at(5.0, 5.0, 4.0) - 5.0).abs() < 1e-9);
    }

    #[test]
    fn a_period_of_zero_is_flat_rather_than_nan() {
        assert!((sine_at(3.0, 1.0, 0.0)).abs() < 1e-9);
        assert!((sine_at(3.0, 1.0, -1.0)).abs() < 1e-9);
    }

    #[test]
    fn a_sentence_is_capitalised_and_ends_in_a_full_stop() {
        let mut rng = rand::rng();
        for _ in 0..50 {
            let s = sentence(&mut rng);
            assert!(s.ends_with('.'), "{s}");
            assert!(
                s.chars().next().is_some_and(char::is_uppercase),
                "not capitalised: {s}"
            );
            assert!(s.split_whitespace().count() > 3, "too short: {s}");
        }
    }

    #[test]
    fn sentences_vary() {
        let mut rng = rand::rng();
        let seen: std::collections::HashSet<String> =
            (0..50).map(|_| sentence(&mut rng)).collect();
        assert!(seen.len() > 1, "the same sentence 50 times running");
    }
}
