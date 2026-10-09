//! `rolling`: the reducer's aggregations over a sliding window, written onto
//! each message.
//!
//! The declaration is [`kayak_core::streaming::RollingTransformConfig`]; the
//! shape is [`super::keyed`]. One window per aggregated *field* per key — two
//! aggregations over `temperature` share one window — each a
//! [`super::keyed::Window`] holding whatever the field held, so the reducer's
//! functions apply unchanged: `apply_function` is literally the reducer's,
//! and `slope` is the same fit against the window's times.
//!
//! The window includes the current message, so the first message per key
//! gets a window of one: an `avg` of itself, a `count` of `1`. That is the
//! warm-up, and `count` is how a downstream `filter` reads it.

use anyhow::{Context, Result, bail};
use kayak_core::config::{Aggregation, ReduceFnKind};
use kayak_core::streaming::RollingTransformConfig;
use serde_json::Value;
use std::collections::HashSet;
use std::sync::Arc;

use super::keyed::{Series, Window};
use super::reduce::{apply_function, present};
use crate::{
    BuildCtx, fields,
    inputs::MessageBatch,
    stats,
    transforms::{BuildTransform, Transform},
};

impl BuildTransform for RollingTransformConfig {
    fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn Transform>> {
        if self.aggregations.is_empty() {
            bail!("a rolling transform needs at least one aggregation");
        }
        if self.size == 0 {
            bail!("a rolling window's 'size' has to be at least one");
        }
        if self.seconds.is_some_and(|s| s <= 0.0 || !s.is_finite()) {
            bail!("a rolling window's 'seconds' has to be more than zero");
        }
        let mut names = HashSet::new();
        for aggregation in &self.aggregations {
            let name = aggregation.output.trim();
            if name.is_empty() {
                bail!("the {:?} aggregation needs an 'as'", aggregation.function);
            }
            if aggregation.field.as_deref().is_none_or(|f| f.trim().is_empty()) {
                bail!(
                    "the '{name}' aggregation needs a 'field' — in a rolling window even `count` \
                     counts the messages of the window that carried one"
                );
            }
            if !names.insert(name.to_string()) {
                bail!("two aggregations are both called '{name}'");
            }
        }
        let mut outputs: Vec<&str> = names.iter().map(String::as_str).collect();
        outputs.sort_unstable();
        let series = Series::resolve(
            ctx,
            "rolling",
            format!("rolling:{}", outputs.join(",")),
            self.group_by,
            self.time,
            self.on_missing,
            self.gate,
        )?;
        Ok(Box::new(RollingTransform {
            series,
            aggregations: self.aggregations,
            size: self.size,
            seconds: self.seconds,
        }))
    }
}

pub struct RollingTransform {
    series: Series,
    aggregations: Vec<Aggregation>,
    size: usize,
    seconds: Option<f64>,
}

/// One aggregation's answer over a window.
fn over_window(aggregation: &Aggregation, points: &[Value]) -> Result<Value> {
    if aggregation.function == ReduceFnKind::Slope {
        let fit = stats::linfit(&Window::seconds(points), &Window::numbers(points));
        return Ok(fit.map_or(Value::Null, |f| Value::from(f.slope)));
    }
    let values = Window::values(points);
    apply_function(aggregation, &values, points.len())
}

#[async_trait::async_trait]
impl Transform for RollingTransform {
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
            // Each field's window is pushed once, however many aggregations
            // read it; the answers are computed off the window after the push.
            let mut pushed: HashSet<&str> = HashSet::new();
            for aggregation in &self.aggregations {
                let Some(field) = aggregation.field.as_deref() else { continue };
                let Some(value) = present(message, field) else {
                    // `Series::number` decides absent-against-policy for a
                    // number; here the value may be any JSON, so the same
                    // rule is applied by asking it and discarding the number.
                    self.series.number(message, field)?;
                    continue;
                };
                let name = format!("window:{field}");
                let first_push = pushed.insert(field);
                let (size, seconds) = (self.size, self.seconds);
                let value = value.clone();
                let answer = self
                    .series
                    .update(&key, |all| {
                        if !all.is_object() {
                            *all = Value::Object(serde_json::Map::new());
                        }
                        let state = &mut all[&name];
                        if first_push {
                            Window::push(state, now, value, size, seconds);
                        }
                        over_window(aggregation, Window::points(state))
                    })?
                    .with_context(|| format!("aggregation '{}'", aggregation.output))?;
                fields::set(&mut written, aggregation.output.trim(), answer)?;
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
    use serde_json::json;

    fn aggregation(function: ReduceFnKind, output: &str) -> Aggregation {
        Aggregation {
            function,
            output: output.into(),
            field: Some("v".into()),
        }
    }

    fn config(aggregations: Vec<Aggregation>, size: usize) -> RollingTransformConfig {
        RollingTransformConfig {
            gate: kayak_core::streaming::Gate::default(),
            aggregations,
            size,
            seconds: None,
            group_by: vec!["k".into()],
            time: Some("t".into()),
            on_missing: MissingFieldPolicy::Error,
        }
    }

    async fn run(config: RollingTransformConfig, messages: Vec<Value>) -> Result<Vec<Value>> {
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
    async fn the_window_slides_by_count_and_is_per_key() -> Result<()> {
        let out = run(
            config(vec![aggregation(ReduceFnKind::Avg, "avg"), aggregation(ReduceFnKind::Count, "n")], 3),
            vec![m("a", 0, 1.0), m("a", 1, 2.0), m("b", 0, 10.0), m("a", 2, 3.0), m("a", 3, 4.0)],
        )
        .await?;
        let answers: Vec<(Value, Value)> = out.iter().map(|o| (o["avg"].clone(), o["n"].clone())).collect();
        assert_eq!(
            answers,
            vec![
                (json!(1.0), json!(1)),
                (json!(1.5), json!(2)),
                (json!(10.0), json!(1)),
                (json!(2.0), json!(3)),
                (json!(3.0), json!(3)), // 2, 3, 4
            ]
        );
        assert_eq!(out[4]["v"], json!(4.0), "the message itself is untouched");
        Ok(())
    }

    #[tokio::test]
    async fn the_window_also_expires_by_age() -> Result<()> {
        let mut config = config(vec![aggregation(ReduceFnKind::Sum, "sum")], 100);
        config.seconds = Some(2.0);
        let out = run(config, vec![m("a", 0, 1.0), m("a", 1000, 1.0), m("a", 2500, 1.0), m("a", 10_000, 1.0)])
            .await?;
        let sums: Vec<Value> = out.iter().map(|o| o["sum"].clone()).collect();
        assert_eq!(sums, vec![json!(1.0), json!(2.0), json!(2.0), json!(1.0)]);
        Ok(())
    }

    #[tokio::test]
    async fn slope_is_against_the_windows_times_and_text_takes_the_reducers_rules() -> Result<()> {
        let out = run(
            config(vec![aggregation(ReduceFnKind::Slope, "trend"), aggregation(ReduceFnKind::Max, "max")], 4),
            vec![m("a", 0, 1.0), m("a", 1000, 3.0), m("a", 2000, 5.0)],
        )
        .await?;
        assert_eq!(out[0]["trend"], json!(null));
        assert_eq!(out[2]["trend"], json!(2.0));
        assert_eq!(out[2]["max"], json!(5.0));

        let text = run(
            config(vec![aggregation(ReduceFnKind::Max, "latest")], 4),
            vec![json!({"k": "a", "t": 0, "v": "2024-01-02"}), json!({"k": "a", "t": 1, "v": "2024-01-01"})],
        )
        .await?;
        assert_eq!(text[1]["latest"], json!("2024-01-02"), "max over text is the reducer's max");
        Ok(())
    }

    #[test]
    fn contradictions_are_refused_at_build() {
        let mut pipelines = std::collections::HashMap::new();
        let mut ctx = ctx_with_bucket(&mut pipelines, None);
        assert!(config(vec![], 3).build(&mut ctx).is_err());
        assert!(config(vec![aggregation(ReduceFnKind::Avg, "a")], 0).build(&mut ctx).is_err());
        let mut fieldless = aggregation(ReduceFnKind::Count, "n");
        fieldless.field = None;
        assert!(config(vec![fieldless], 3).build(&mut ctx).is_err(), "count needs a field here");
        let mut timed = config(vec![aggregation(ReduceFnKind::Avg, "a")], 3);
        timed.seconds = Some(0.0);
        assert!(timed.build(&mut ctx).is_err());
    }
}
