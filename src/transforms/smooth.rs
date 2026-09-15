//! `smooth`: a numeric field against the values before it.
//!
//! The declaration is [`kayak_core::streaming::SmoothTransformConfig`]; the
//! shape is [`super::keyed`]. Every method is a function in [`crate::stats`]
//! over the key's window (or, for `ewma`, over one remembered number); this
//! module only decides what is in the window and where the answer goes.
//!
//! Two things worth knowing. The window methods include the current value —
//! a median of the last five *is* of the last five, this one among them —
//! and until the window holds enough for the method (more than `order`
//! points for Savitzky–Golay) the value passes through unsmoothed rather
//! than as a fit with too few points behind it. And a Hampel filter with a
//! window whose MAD is zero replaces *any* deviation from the median: that
//! is what the arithmetic says, and it is the right answer for a flat signal
//! with a spike in it.

use anyhow::{Result, bail};
use kayak_core::streaming::{SmoothMethod, SmoothTransformConfig};
use serde_json::{Value, json};
use std::sync::Arc;

use super::keyed::{Reading, Series, Window};
use crate::{
    BuildCtx, fields,
    inputs::MessageBatch,
    stats,
    transforms::{BuildTransform, Transform},
};

/// The MAD-to-σ scale a Hampel filter uses.
const MAD_SCALE: f64 = 1.4826;

impl BuildTransform for SmoothTransformConfig {
    fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn Transform>> {
        if self.field.trim().is_empty() {
            bail!("a smooth transform needs a 'field'");
        }
        let method = match &self.method {
            SmoothMethod::Ewma { alpha, half_life } => {
                let alpha = match (alpha, half_life) {
                    (Some(_), Some(_)) => bail!("an ewma takes 'alpha' or 'half_life', not both"),
                    (None, None) => bail!("an ewma needs an 'alpha' or a 'half_life'"),
                    (Some(alpha), None) => {
                        if !(0.0..=1.0).contains(alpha) {
                            bail!("an ewma's 'alpha' is a weight from 0 to 1, not {alpha}");
                        }
                        *alpha
                    }
                    (None, Some(half_life)) => stats::alpha_for_half_life(*half_life)
                        .ok_or_else(|| anyhow::anyhow!("an ewma's 'half_life' has to be more than zero"))?,
                };
                Method::Ewma { alpha }
            }
            SmoothMethod::Median { size } => {
                if *size == 0 {
                    bail!("a median window needs a 'size' of at least one");
                }
                Method::Median { size: *size }
            }
            SmoothMethod::Hampel { size, threshold } => {
                if *size < 3 {
                    bail!("a hampel window needs a 'size' of at least three, or there is no median to speak of");
                }
                let threshold = threshold.unwrap_or(3.0);
                if threshold <= 0.0 || !threshold.is_finite() {
                    bail!("a hampel 'threshold' has to be more than zero");
                }
                Method::Hampel {
                    size: *size,
                    threshold,
                }
            }
            SmoothMethod::SavitzkyGolay { size, order } => {
                let order = order.unwrap_or(2);
                if order >= *size {
                    bail!("a savitzky_golay 'order' ({order}) has to be below its 'size' ({size})");
                }
                Method::SavitzkyGolay { size: *size, order }
            }
        };
        let output = self.output.unwrap_or_else(|| self.field.clone());
        let series = Series::resolve(
            ctx,
            "smooth",
            format!("smooth:{output}"),
            self.group_by,
            None,
            self.on_missing,
        )?;
        Ok(Box::new(SmoothTransform {
            series,
            field: self.field,
            output,
            method,
        }))
    }
}

/// The method with its defaults filled in and its spelling checked.
#[derive(Clone, Copy, Debug)]
enum Method {
    Ewma { alpha: f64 },
    Median { size: usize },
    Hampel { size: usize, threshold: f64 },
    SavitzkyGolay { size: usize, order: usize },
}

impl Method {
    /// The smoothed value, given the key's state — edited in place.
    fn smooth(self, state: &mut Value, value: f64, now: i64) -> f64 {
        match self {
            Method::Ewma { alpha } => {
                let next = match state.get("prev").and_then(Value::as_f64) {
                    None => value,
                    Some(prev) => alpha * value + (1.0 - alpha) * prev,
                };
                *state = json!({"prev": next});
                next
            }
            Method::Median { size } => {
                Window::push(state, now, json!(value), size, None);
                stats::median(&Window::numbers(Window::points(state))).unwrap_or(value)
            }
            Method::Hampel { size, threshold } => {
                Window::push(state, now, json!(value), size, None);
                let numbers = Window::numbers(Window::points(state));
                match (stats::median(&numbers), stats::mad(&numbers)) {
                    (Some(median), Some(mad)) if (value - median).abs() > threshold * MAD_SCALE * mad => median,
                    _ => value,
                }
            }
            Method::SavitzkyGolay { size, order } => {
                Window::push(state, now, json!(value), size, None);
                let numbers = Window::numbers(Window::points(state));
                if numbers.len() <= order {
                    return value;
                }
                stats::savitzky_golay_last(&numbers, order).unwrap_or(value)
            }
        }
    }
}

pub struct SmoothTransform {
    series: Series,
    field: String,
    output: String,
    method: Method,
}

#[async_trait::async_trait]
impl Transform for SmoothTransform {
    async fn apply(&mut self, batch: Arc<MessageBatch>) -> Result<Vec<Arc<MessageBatch>>> {
        let mut out: MessageBatch = Vec::with_capacity(batch.len());
        for message in batch.iter() {
            let Some(key) = self.series.key(message)? else {
                out.push(Arc::clone(message));
                continue;
            };
            let Reading::Value(value) = self.series.number(message, &self.field)? else {
                out.push(Arc::clone(message));
                continue;
            };
            // Arrival order is the only order a window here has a claim to;
            // the stored time is for the state tab, and nothing trims on it.
            let now = self.series.millis(message)?;
            let smoothed = self.series.update(&key, |state| self.method.smooth(state, value, now))?;
            let mut written = (**message).clone();
            fields::set(&mut written, &self.output, json!(smoothed))?;
            out.push(Arc::new(written));
        }
        Ok(vec![Arc::new(out)])
    }
}

#[cfg(test)]
#[allow(clippy::float_cmp, reason = "the expected values are exact in binary")]
mod tests {
    use super::*;
    use crate::testing::{batch, ctx_with_bucket};
    use kayak_core::config::MissingFieldPolicy;

    fn config(method: SmoothMethod) -> SmoothTransformConfig {
        SmoothTransformConfig {
            field: "v".into(),
            method,
            output: Some("s".into()),
            group_by: vec![],
            on_missing: MissingFieldPolicy::Error,
        }
    }

    async fn smoothed(config: SmoothTransformConfig, values: &[f64]) -> Result<Vec<f64>> {
        let mut pipelines = std::collections::HashMap::new();
        let mut ctx = ctx_with_bucket(&mut pipelines, None);
        let mut transform = config.build(&mut ctx)?;
        let messages = values.iter().map(|v| json!({"v": v})).collect();
        let out = transform.apply(batch(messages)).await?;
        Ok(out
            .iter()
            .flat_map(|b| b.iter().filter_map(|m| m["s"].as_f64()))
            .collect())
    }

    #[tokio::test]
    async fn ewma_follows_by_alpha_or_by_half_life() -> Result<()> {
        let by_alpha = smoothed(
            config(SmoothMethod::Ewma {
                alpha: Some(0.5),
                half_life: None,
            }),
            &[1.0, 3.0, 3.0],
        )
        .await?;
        assert_eq!(by_alpha, vec![1.0, 2.0, 2.5]);
        let by_half_life = smoothed(
            config(SmoothMethod::Ewma {
                alpha: None,
                half_life: Some(1.0),
            }),
            &[1.0, 3.0, 3.0],
        )
        .await?;
        assert_eq!(by_half_life, by_alpha, "a half-life of one message is an alpha of a half");
        Ok(())
    }

    #[tokio::test]
    async fn median_removes_a_spike_and_hampel_only_replaces_the_outlier() -> Result<()> {
        let spiky = [1.0, 1.0, 1.0, 100.0, 1.0, 1.0];
        let median = smoothed(config(SmoothMethod::Median { size: 3 }), &spiky).await?;
        assert_eq!(median, vec![1.0, 1.0, 1.0, 1.0, 1.0, 1.0]);

        let gentle = [1.0, 2.0, 3.0, 100.0, 4.0, 5.0];
        let hampel = smoothed(
            config(SmoothMethod::Hampel {
                size: 5,
                threshold: None,
            }),
            &gentle,
        )
        .await?;
        assert_eq!(hampel[3], 2.5, "the spike is replaced by the median of the four so far");
        assert_eq!(hampel[5], 5.0, "an ordinary value is left alone");
        Ok(())
    }

    #[tokio::test]
    async fn savitzky_golay_passes_through_until_the_window_can_carry_the_order() -> Result<()> {
        let out = smoothed(
            config(SmoothMethod::SavitzkyGolay {
                size: 5,
                order: Some(2),
            }),
            &[0.0, 1.0, 4.0, 9.0, 16.0, 25.0],
        )
        .await?;
        assert_eq!(&out[..2], &[0.0, 1.0], "too few points: untouched");
        assert!((out[5] - 25.0).abs() < 1e-9, "a parabola is reproduced exactly: {}", out[5]);
        Ok(())
    }

    #[tokio::test]
    async fn without_as_the_field_itself_is_replaced() -> Result<()> {
        let mut config = config(SmoothMethod::Median { size: 3 });
        config.output = None;
        let mut pipelines = std::collections::HashMap::new();
        let mut ctx = ctx_with_bucket(&mut pipelines, None);
        let mut transform = config.build(&mut ctx)?;
        let out = transform.apply(batch(vec![json!({"v": 1.0}), json!({"v": 9.0})])).await?;
        assert_eq!(*out[0][1], json!({"v": 5.0}));
        Ok(())
    }

    #[test]
    fn contradictions_are_refused_at_build() {
        let mut pipelines = std::collections::HashMap::new();
        let mut ctx = ctx_with_bucket(&mut pipelines, None);
        for method in [
            SmoothMethod::Ewma {
                alpha: Some(0.5),
                half_life: Some(2.0),
            },
            SmoothMethod::Ewma {
                alpha: None,
                half_life: None,
            },
            SmoothMethod::Ewma {
                alpha: Some(1.5),
                half_life: None,
            },
            SmoothMethod::Median { size: 0 },
            SmoothMethod::Hampel {
                size: 2,
                threshold: None,
            },
            SmoothMethod::SavitzkyGolay {
                size: 3,
                order: Some(3),
            },
        ] {
            assert!(config(method.clone()).build(&mut ctx).is_err(), "{method:?} should be refused");
        }
    }
}
