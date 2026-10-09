//! `deadband`: drop a message unless the value moved.
//!
//! The declaration is [`kayak_core::streaming::DeadbandTransformConfig`]; the
//! shape it shares with the other streaming transforms is [`super::keyed`].
//! What is decided here:
//!
//! - **The anchor is the last value that *passed*, forced passes included.**
//!   That is what the config promises and what a historian's exception
//!   filter does. The cost is that a value creeping by less than `delta`
//!   between forced passes is never reported as a change — which is what a
//!   deadband *is*, and `max_seconds` is the knob that bounds how long it can
//!   go on for.
//! - **The flatline clock runs from the last *change*, not the last pass.**
//!   A `max_seconds` confirmation of a stuck value is still a stuck value, so
//!   it must not reset the stretch; it carries `stuck: true` if the stretch is
//!   long enough, and the flag fires once per stretch either way.
//! - **A message that passes unchanged is the same `Arc`.** Only the flagged
//!   one is cloned to be written on, so the common path allocates nothing.

use anyhow::{Result, bail};
use kayak_core::streaming::{DeadbandMode, DeadbandTransformConfig};
use serde_json::{Value, json};
use std::sync::Arc;

use super::keyed::{Reading, Series};
use crate::{
    BuildCtx, fields,
    inputs::MessageBatch,
    transforms::{BuildTransform, Transform},
};

/// The field the flatline flag is written to.
pub const STUCK_FIELD: &str = "stuck";

impl BuildTransform for DeadbandTransformConfig {
    fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn Transform>> {
        if self.field.trim().is_empty() {
            bail!("a deadband needs a 'field'");
        }
        if self.delta < 0.0 || !self.delta.is_finite() {
            bail!("a deadband's 'delta' has to be a number of zero or more, not {}", self.delta);
        }
        for (name, seconds) in [("max_seconds", self.max_seconds), ("flatline_seconds", self.flatline_seconds)] {
            if seconds.is_some_and(|s| s <= 0.0 || !s.is_finite()) {
                bail!("a deadband's '{name}' has to be more than zero");
            }
        }
        let series = Series::resolve(
            ctx,
            "deadband",
            format!("deadband:{}", self.field),
            self.group_by,
            self.time,
            self.on_missing,
        )?;
        Ok(Box::new(DeadbandTransform {
            series,
            field: self.field,
            delta: self.delta,
            mode: self.mode,
            max_millis: self.max_seconds.map(millis),
            flatline_millis: self.flatline_seconds.map(millis),
        }))
    }
}

#[allow(clippy::cast_possible_truncation, reason = "seconds to millis, well within range")]
fn millis(seconds: f64) -> i64 {
    (seconds * 1000.0).round() as i64
}

pub struct DeadbandTransform {
    series: Series,
    field: String,
    delta: f64,
    mode: DeadbandMode,
    max_millis: Option<i64>,
    flatline_millis: Option<i64>,
}

/// What one message's turn decided.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Verdict {
    Pass { stuck: bool },
    Drop,
}

impl DeadbandTransform {
    /// Whether `value` is outside the band around `anchor`.
    fn moved(&self, anchor: f64, value: f64) -> bool {
        let band = match self.mode {
            DeadbandMode::Absolute => self.delta,
            DeadbandMode::Percent => anchor.abs() * self.delta / 100.0,
        };
        (value - anchor).abs() > band
    }

    /// One message against its key's state, which is edited in place.
    fn judge(&self, state: &mut Value, value: f64, now: i64) -> Verdict {
        let Some(anchor) = state.get("last").and_then(Value::as_f64) else {
            *state = json!({"last": value, "passed_at": now, "changed_at": now, "flagged": false});
            return Verdict::Pass { stuck: false };
        };
        if self.moved(anchor, value) {
            *state = json!({"last": value, "passed_at": now, "changed_at": now, "flagged": false});
            return Verdict::Pass { stuck: false };
        }
        let passed_at = state.get("passed_at").and_then(Value::as_i64).unwrap_or(now);
        let changed_at = state.get("changed_at").and_then(Value::as_i64).unwrap_or(now);
        let flagged = state.get("flagged").and_then(Value::as_bool).unwrap_or(false);
        let stuck = self.flatline_millis.is_some_and(|f| now - changed_at >= f);
        let forced = self.max_millis.is_some_and(|m| now - passed_at >= m);
        if forced || (stuck && !flagged) {
            state["passed_at"] = json!(if forced { now } else { passed_at });
            state["last"] = json!(if forced { value } else { anchor });
            if stuck {
                state["flagged"] = json!(true);
            }
            return Verdict::Pass { stuck };
        }
        Verdict::Drop
    }
}

#[async_trait::async_trait]
impl Transform for DeadbandTransform {
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
            let now = self.series.millis(message)?;
            match self.series.update(&key, |state| self.judge(state, value, now))? {
                Verdict::Pass { stuck: false } => out.push(Arc::clone(message)),
                Verdict::Pass { stuck: true } => {
                    let mut flagged = (**message).clone();
                    fields::set(&mut flagged, STUCK_FIELD, Value::Bool(true))?;
                    out.push(Arc::new(flagged));
                }
                Verdict::Drop => {}
            }
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
    use serde_json::json;

    fn config(delta: f64) -> DeadbandTransformConfig {
        DeadbandTransformConfig {
            field: "v".into(),
            delta,
            mode: DeadbandMode::Absolute,
            max_seconds: None,
            flatline_seconds: None,
            group_by: vec!["k".into()],
            time: Some("t".into()),
            on_missing: MissingFieldPolicy::Error,
        }
    }

    async fn run(config: DeadbandTransformConfig, messages: Vec<Value>) -> Result<Vec<Value>> {
        let mut pipelines = std::collections::HashMap::new();
        let mut ctx = ctx_with_bucket(&mut pipelines, None);
        let mut transform = config.build(&mut ctx)?;
        let out = transform.apply(batch(messages)).await?;
        Ok(out.iter().flat_map(|b| b.iter().map(|m| (**m).clone())).collect())
    }

    fn m(k: &str, t: i64, v: f64) -> Value {
        json!({"k": k, "t": t, "v": v})
    }

    #[tokio::test]
    async fn the_first_passes_and_then_only_a_move_past_the_band() -> Result<()> {
        let out = run(
            config(0.5),
            vec![m("a", 0, 10.0), m("a", 1000, 10.4), m("a", 2000, 10.6), m("a", 3000, 10.9), m("b", 0, 1.0)],
        )
        .await?;
        // 10.4 is within 0.5 of 10; 10.6 moved; 10.9 is within 0.5 of 10.6
        assert_eq!(out, vec![m("a", 0, 10.0), m("a", 2000, 10.6), m("b", 0, 1.0)]);
        Ok(())
    }

    #[tokio::test]
    async fn percent_mode_scales_the_band_with_the_anchor() -> Result<()> {
        let mut config = config(10.0);
        config.mode = DeadbandMode::Percent;
        let out = run(config, vec![m("a", 0, 100.0), m("a", 1, 109.0), m("a", 2, 111.0)]).await?;
        assert_eq!(out, vec![m("a", 0, 100.0), m("a", 2, 111.0)]);
        Ok(())
    }

    #[tokio::test]
    async fn max_seconds_confirms_a_steady_value() -> Result<()> {
        let mut config = config(1.0);
        config.max_seconds = Some(5.0);
        let out = run(config, vec![m("a", 0, 1.0), m("a", 4000, 1.0), m("a", 5000, 1.0), m("a", 6000, 1.0)]).await?;
        assert_eq!(out, vec![m("a", 0, 1.0), m("a", 5000, 1.0)]);
        Ok(())
    }

    /// The stretch is measured from the last *change*, a forced pass does not
    /// reset it, and the flag fires once per stretch.
    #[tokio::test]
    async fn a_flatline_is_flagged_once_per_stretch() -> Result<()> {
        let mut config = config(1.0);
        config.flatline_seconds = Some(10.0);
        config.max_seconds = Some(6.0);
        let out = run(
            config,
            vec![
                m("a", 0, 1.0),
                m("a", 6000, 1.0),  // forced, not yet stuck
                m("a", 11000, 1.0), // stuck: 11 s since the change; flagged, not forced
                m("a", 11500, 1.0), // still stuck, already flagged, not forced: dropped
                m("a", 12000, 1.0), // forced again, and still stuck
                m("a", 13000, 5.0), // a change ends the stretch
            ],
        )
        .await?;
        let stuck = |k, t, v| {
            let mut msg = m(k, t, v);
            msg["stuck"] = json!(true);
            msg
        };
        assert_eq!(
            out,
            vec![m("a", 0, 1.0), m("a", 6000, 1.0), stuck("a", 11000, 1.0), stuck("a", 12000, 1.0), m("a", 13000, 5.0)]
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_batch_entirely_dropped_is_no_batch() -> Result<()> {
        let out = run(config(1.0), vec![m("a", 0, 1.0)]).await?;
        assert_eq!(out.len(), 1);
        // a second call sees the state the first left
        let mut pipelines = std::collections::HashMap::new();
        let mut ctx = ctx_with_bucket(&mut pipelines, None);
        let mut transform = config(1.0).build(&mut ctx)?;
        transform.apply(batch(vec![m("a", 0, 1.0)])).await?;
        let again = transform.apply(batch(vec![m("a", 1, 1.2)])).await?;
        assert!(again.is_empty(), "nothing passed, so nothing was emitted");
        Ok(())
    }

    #[tokio::test]
    async fn a_message_without_the_field_follows_on_missing() -> Result<()> {
        let strict = config(1.0);
        assert!(run(strict, vec![json!({"k": "a", "t": 0})]).await.is_err());

        let mut lax = config(1.0);
        lax.on_missing = MissingFieldPolicy::Skip;
        let out = run(lax, vec![json!({"k": "a", "t": 0}), json!({"t": 0, "v": 1.0})]).await?;
        assert_eq!(out.len(), 2, "both pass through untouched");
        Ok(())
    }

    #[test]
    fn nonsense_is_refused_at_build() {
        let mut pipelines = std::collections::HashMap::new();
        let mut ctx = ctx_with_bucket(&mut pipelines, None);
        assert!(config(-1.0).build(&mut ctx).is_err());
        let mut zero = config(1.0);
        zero.max_seconds = Some(0.0);
        assert!(zero.build(&mut ctx).is_err());

        let (events, _) = tokio::sync::broadcast::channel(16);
        let mut stateless = BuildCtx::new(&mut pipelines, "x".into(), events);
        assert!(config(1.0).build(&mut stateless).is_err(), "no state, no deadband");
    }
}
