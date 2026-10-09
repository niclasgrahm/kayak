//! `throttle`: at most one message per key every so often.
//!
//! The declaration is [`kayak_core::streaming::ThrottleTransformConfig`]; the
//! shape is [`super::keyed`]. The state per key is one number — when the last
//! message passed — and the decision is whether this one is far enough past
//! it. Two things are decided here:
//!
//! - **The interval runs from the message that passed.** Not from a grid: a
//!   grid is `resample`'s, and it is what makes `resample` have to hold a
//!   value until the interval is over. A throttle holds nothing, so it needs
//!   no tick and nothing is lost on shutdown.
//! - **A message earlier than the last one passed is dropped.** Under a
//!   `time` field that is a late reading inside an interval already reported,
//!   which is exactly what a throttle is for; under arrival time it cannot
//!   happen.

use anyhow::{Result, bail};
use kayak_core::streaming::ThrottleTransformConfig;
use serde_json::{Value, json};
use std::sync::Arc;

use super::keyed::Series;
use crate::{
    BuildCtx,
    inputs::MessageBatch,
    transforms::{BuildTransform, Transform},
};

impl BuildTransform for ThrottleTransformConfig {
    fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn Transform>> {
        if !(self.seconds.is_finite() && self.seconds > 0.0) {
            bail!("a throttle's 'seconds' has to be more than zero, not {}", self.seconds);
        }
        let series = Series::resolve(
            ctx,
            "throttle",
            "throttle".to_string(),
            self.group_by,
            self.time,
            self.on_missing,
            self.gate,
        )?;
        #[allow(clippy::cast_possible_truncation, reason = "seconds to millis, well within range")]
        let interval_millis = (self.seconds * 1000.0).round() as i64;
        Ok(Box::new(ThrottleTransform {
            series,
            interval_millis,
        }))
    }
}

pub struct ThrottleTransform {
    series: Series,
    interval_millis: i64,
}

impl ThrottleTransform {
    /// Whether a message at `now` passes, given the key's state — edited in
    /// place when it does.
    fn passes(&self, state: &mut Value, now: i64) -> bool {
        let due = state
            .get("passed_at")
            .and_then(Value::as_i64)
            .is_none_or(|last| now - last >= self.interval_millis);
        if due {
            *state = json!({"passed_at": now});
        }
        due
    }
}

#[async_trait::async_trait]
impl Transform for ThrottleTransform {
    async fn apply(&mut self, batch: Arc<MessageBatch>) -> Result<Vec<Arc<MessageBatch>>> {
        let mut out: MessageBatch = Vec::with_capacity(batch.len());
        for message in batch.iter() {
            let Some(key) = self.series.key(message)? else {
                out.push(Arc::clone(message));
                continue;
            };
            let now = self.series.millis(message)?;
            if self.series.update(&key, |state| self.passes(state, now))? {
                out.push(Arc::clone(message));
            }
        }
        // an empty batch carries nothing downstream — the filter's rule
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

    fn config(seconds: f64) -> ThrottleTransformConfig {
        ThrottleTransformConfig {
            gate: kayak_core::streaming::Gate::default(),
            seconds,
            group_by: vec!["k".into()],
            time: Some("t".into()),
            on_missing: MissingFieldPolicy::Error,
        }
    }

    /// Messages `(key, seconds)` through one throttle, and the ones that came
    /// out, as `(key, seconds)` again.
    async fn passed(
        transform: &mut Box<dyn Transform>,
        messages: &[(&str, i64)],
    ) -> Result<Vec<(String, i64)>> {
        let messages = messages
            .iter()
            .map(|(k, t)| json!({"k": k, "t": t * 1000, "payload": {"many": "fields"}}))
            .collect();
        let out = transform.apply(batch(messages)).await?;
        Ok(out
            .iter()
            .flat_map(|b| b.iter())
            .map(|m| {
                assert_eq!(m["payload"], json!({"many": "fields"}), "a message passed changed");
                (
                    m["k"].as_str().unwrap_or_default().to_string(),
                    m["t"].as_i64().unwrap_or_default() / 1000,
                )
            })
            .collect())
    }

    #[tokio::test]
    async fn the_first_message_of_each_interval_passes_per_key() -> Result<()> {
        let mut pipelines = std::collections::HashMap::new();
        let mut ctx = ctx_with_bucket(&mut pipelines, None);
        let mut throttle = config(5.0).build(&mut ctx)?;
        let out = passed(
            &mut throttle,
            &[("a", 0), ("a", 1), ("b", 2), ("a", 4), ("a", 5), ("b", 6), ("a", 9), ("b", 7)],
        )
        .await?;
        assert_eq!(
            out,
            vec![("a".into(), 0), ("b".into(), 2), ("a".into(), 5), ("b".into(), 7)],
            "the interval is per key and runs from the message that passed"
        );
        Ok(())
    }

    /// The state outlives the batch: a throttle is about time, not about how
    /// the messages happened to be grouped on the way in.
    #[tokio::test]
    async fn the_interval_carries_across_batches() -> Result<()> {
        let mut pipelines = std::collections::HashMap::new();
        let mut ctx = ctx_with_bucket(&mut pipelines, None);
        let mut throttle = config(5.0).build(&mut ctx)?;
        assert_eq!(passed(&mut throttle, &[("a", 0)]).await?.len(), 1);
        assert_eq!(passed(&mut throttle, &[("a", 3)]).await?, vec![]);
        assert_eq!(passed(&mut throttle, &[("a", 5)]).await?.len(), 1);
        Ok(())
    }

    /// A late reading falls inside an interval already reported for, so it is
    /// dropped — and does not reset the interval.
    #[tokio::test]
    async fn a_late_message_is_dropped_and_moves_nothing() -> Result<()> {
        let mut pipelines = std::collections::HashMap::new();
        let mut ctx = ctx_with_bucket(&mut pipelines, None);
        let mut throttle = config(5.0).build(&mut ctx)?;
        let out = passed(&mut throttle, &[("a", 10), ("a", 2), ("a", 15)]).await?;
        assert_eq!(out, vec![("a".into(), 10), ("a".into(), 15)]);
        Ok(())
    }

    /// Nothing passing is no batch at all, rather than an empty one a reducer
    /// downstream would turn into a meaningless answer.
    #[tokio::test]
    async fn a_batch_with_nothing_passing_emits_nothing() -> Result<()> {
        let mut pipelines = std::collections::HashMap::new();
        let mut ctx = ctx_with_bucket(&mut pipelines, None);
        let mut throttle = config(5.0).build(&mut ctx)?;
        passed(&mut throttle, &[("a", 0)]).await?;
        let out = throttle.apply(batch(vec![json!({"k": "a", "t": 1000})])).await?;
        assert!(out.is_empty(), "{out:?}");
        Ok(())
    }

    #[tokio::test]
    async fn a_message_with_no_key_follows_on_missing() -> Result<()> {
        let mut pipelines = std::collections::HashMap::new();
        let mut ctx = ctx_with_bucket(&mut pipelines, None);
        let mut strict = config(5.0).build(&mut ctx)?;
        assert!(strict.apply(batch(vec![json!({"t": 0})])).await.is_err());

        let mut lax = ThrottleTransformConfig {
            on_missing: MissingFieldPolicy::Skip,
            ..config(5.0)
        }
        .build(&mut ctx)?;
        let unkeyed = || batch(vec![json!({"t": 0}), json!({"t": 1})]);
        assert_eq!(lax.apply(unkeyed()).await?[0].len(), 2, "unkeyed messages pass untouched");
        Ok(())
    }

    #[test]
    fn an_interval_that_is_not_positive_is_refused() {
        let mut pipelines = std::collections::HashMap::new();
        let mut ctx = ctx_with_bucket(&mut pipelines, None);
        for seconds in [0.0, -1.0, f64::NAN, f64::INFINITY] {
            assert!(config(seconds).build(&mut ctx).is_err(), "{seconds} was accepted");
        }
    }

    #[test]
    fn a_pipeline_without_state_cannot_throttle() {
        let mut pipelines = std::collections::HashMap::new();
        let (events, _) = tokio::sync::broadcast::channel(1);
        let mut ctx = BuildCtx::new(&mut pipelines, "throttle-test".into(), events);
        assert!(config(5.0).build(&mut ctx).is_err());
    }
}
