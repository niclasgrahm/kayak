//! `derive`: a rate, a delta, a running total, a wrap-tolerant counter.
//!
//! The declaration is [`kayak_core::streaming::DeriveTransformConfig`]; the
//! shape is [`super::keyed`]. Each derivation keeps `{last, at, acc}` under
//! its own `as` in the key's state, so derivations over different fields do
//! not share a previous value, and one over a field a message lacks (under
//! `on_missing: skip`) leaves both its output and its state alone rather than
//! writing a `null` it would then compute the next delta against.

use anyhow::{Result, bail};
use kayak_core::streaming::{Derivation, DeriveFnKind, DeriveTransformConfig};
use serde_json::{Value, json};
use std::collections::HashSet;
use std::sync::Arc;

use super::keyed::{Reading, Series};
use crate::{
    BuildCtx, fields,
    inputs::MessageBatch,
    transforms::{BuildTransform, Transform},
};

impl BuildTransform for DeriveTransformConfig {
    fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn Transform>> {
        if self.derive.is_empty() {
            bail!("a derive transform needs at least one derivation");
        }
        let mut names = HashSet::new();
        for derivation in &self.derive {
            let name = derivation.output.trim();
            if name.is_empty() {
                bail!("the {:?} derivation of '{}' needs an 'as'", derivation.function, derivation.field);
            }
            if derivation.field.trim().is_empty() {
                bail!("the '{name}' derivation needs a 'field'");
            }
            if !names.insert(name.to_string()) {
                bail!("two derivations are both called '{name}'");
            }
            if derivation.wrap_at.is_some() && derivation.function != DeriveFnKind::Counter {
                bail!("'wrap_at' on the '{name}' derivation only means something for a counter");
            }
            if derivation.wrap_at.is_some_and(|w| w <= 0.0 || !w.is_finite()) {
                bail!("the '{name}' counter's 'wrap_at' has to be more than zero");
            }
        }
        let mut outputs: Vec<&str> = self.derive.iter().map(|d| d.output.trim()).collect();
        outputs.sort_unstable();
        let series = Series::resolve(
            ctx,
            "derive",
            format!("derive:{}", outputs.join(",")),
            self.group_by,
            self.time,
            self.on_missing,
        )?;
        Ok(Box::new(DeriveTransform {
            series,
            derive: self.derive,
        }))
    }
}

pub struct DeriveTransform {
    series: Series,
    derive: Vec<Derivation>,
}

/// One derivation's answer for one message, given its state — edited in place.
fn step(derivation: &Derivation, state: &mut Value, value: f64, now: i64) -> Value {
    let previous = state.get("last").and_then(Value::as_f64);
    let at = state.get("at").and_then(Value::as_i64);
    let acc = state.get("acc").and_then(Value::as_f64).unwrap_or(0.0);

    let (answer, acc) = match derivation.function {
        DeriveFnKind::Delta => (previous.map_or(Value::Null, |p| json!(value - p)), acc),
        DeriveFnKind::Rate => {
            let rate = match (previous, at) {
                #[allow(clippy::cast_precision_loss, reason = "millis to seconds")]
                (Some(p), Some(then)) if now > then => json!((value - p) / ((now - then) as f64 / 1000.0)),
                _ => Value::Null,
            };
            (rate, acc)
        }
        DeriveFnKind::Cumsum => {
            let total = acc + value;
            (json!(total), total)
        }
        DeriveFnKind::Counter => {
            let increase = match previous {
                None => 0.0,
                Some(p) if value >= p => value - p,
                Some(p) => derivation.wrap_at.map_or(value, |wrap| (wrap - p) + value),
            };
            let total = acc + increase;
            (json!(total), total)
        }
    };
    *state = json!({"last": value, "at": now, "acc": acc});
    answer
}

#[async_trait::async_trait]
impl Transform for DeriveTransform {
    async fn apply(&mut self, batch: Arc<MessageBatch>) -> Result<Vec<Arc<MessageBatch>>> {
        let mut out: MessageBatch = Vec::with_capacity(batch.len());
        for message in batch.iter() {
            let Some(key) = self.series.key(message)? else {
                out.push(Arc::clone(message));
                continue;
            };
            let now = self.series.millis(message)?;
            let mut written = (**message).clone();
            let mut touched = false;
            for derivation in &self.derive {
                let Reading::Value(value) = self.series.number(message, &derivation.field)? else {
                    continue;
                };
                let output = derivation.output.trim().to_string();
                let answer = self.series.update(&key, |all| {
                    if !all.is_object() {
                        *all = json!({});
                    }
                    let state = &mut all[&output];
                    step(derivation, state, value, now)
                })?;
                fields::set(&mut written, &output, answer)?;
                touched = true;
            }
            out.push(if touched { Arc::new(written) } else { Arc::clone(message) });
        }
        Ok(vec![Arc::new(out)])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{batch, ctx_with_bucket};
    use kayak_core::config::MissingFieldPolicy;

    fn derivation(function: DeriveFnKind, output: &str) -> Derivation {
        Derivation {
            function,
            field: "v".into(),
            output: output.into(),
            wrap_at: None,
        }
    }

    fn config(derive: Vec<Derivation>) -> DeriveTransformConfig {
        DeriveTransformConfig {
            derive,
            group_by: vec!["k".into()],
            time: Some("t".into()),
            on_missing: MissingFieldPolicy::Error,
        }
    }

    async fn run(config: DeriveTransformConfig, messages: Vec<Value>) -> Result<Vec<Value>> {
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
    async fn delta_and_rate_need_a_previous_and_are_per_key() -> Result<()> {
        let out = run(
            config(vec![derivation(DeriveFnKind::Delta, "d"), derivation(DeriveFnKind::Rate, "r")]),
            vec![m("a", 0, 10.0), m("b", 0, 1.0), m("a", 2000, 14.0), m("a", 2000, 15.0)],
        )
        .await?;
        let answers: Vec<(Value, Value)> = out.iter().map(|o| (o["d"].clone(), o["r"].clone())).collect();
        assert_eq!(
            answers,
            vec![
                (json!(null), json!(null)),
                (json!(null), json!(null)),
                (json!(4.0), json!(2.0)),
                (json!(1.0), json!(null)), // no time passed, so no rate
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn cumsum_and_counter_accumulate() -> Result<()> {
        let mut wrapping = derivation(DeriveFnKind::Counter, "wrapped");
        wrapping.wrap_at = Some(100.0);
        let out = run(
            config(vec![
                derivation(DeriveFnKind::Cumsum, "sum"),
                derivation(DeriveFnKind::Counter, "reset"),
                wrapping,
            ]),
            vec![m("a", 0, 90.0), m("a", 1, 95.0), m("a", 2, 3.0)],
        )
        .await?;
        let answers: Vec<(Value, Value, Value)> =
            out.iter().map(|o| (o["sum"].clone(), o["reset"].clone(), o["wrapped"].clone())).collect();
        assert_eq!(
            answers,
            vec![
                (json!(90.0), json!(0.0), json!(0.0)),
                (json!(185.0), json!(5.0), json!(5.0)),
                // a drop from 95 to 3: a reset counts the new value, a wrap
                // at 100 counts the 5 through the top plus the 3
                (json!(188.0), json!(8.0), json!(13.0)),
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn a_skipped_message_leaves_its_state_alone() -> Result<()> {
        let mut config = config(vec![derivation(DeriveFnKind::Delta, "d")]);
        config.on_missing = MissingFieldPolicy::Skip;
        let out = run(config, vec![m("a", 0, 1.0), json!({"k": "a", "t": 1}), m("a", 2, 3.0)]).await?;
        assert_eq!(out[1], json!({"k": "a", "t": 1}), "passed through untouched");
        assert_eq!(out[2]["d"], json!(2.0), "the delta is against the last real value");
        Ok(())
    }

    #[test]
    fn contradictions_are_refused_at_build() {
        let mut pipelines = std::collections::HashMap::new();
        let mut ctx = ctx_with_bucket(&mut pipelines, None);
        assert!(config(vec![]).build(&mut ctx).is_err());
        assert!(
            config(vec![derivation(DeriveFnKind::Delta, "d"), derivation(DeriveFnKind::Rate, "d")])
                .build(&mut ctx)
                .is_err()
        );
        let mut wrong = derivation(DeriveFnKind::Delta, "d");
        wrong.wrap_at = Some(10.0);
        assert!(config(vec![wrong]).build(&mut ctx).is_err(), "wrap_at is a counter's");
    }
}
