//! `resample`: a series onto a regular grid.
//!
//! The declaration is [`kayak_core::streaming::ResampleTransformConfig`]; the
//! shape is [`super::keyed`]. This is the transform that changes the stream's
//! cardinality and its shape: what comes out is one *new* message per key per
//! grid point, not an annotated reading.
//!
//! **A grid point `g` stands for the interval `[g, g + interval)`**, and the
//! methods differ in when it can be emitted:
//!
//! - `last`, `mean` and `forward_fill` emit the interval once something says
//!   it is over — a reading at or past `g + interval`, or, when time is
//!   arrival time, the clock getting there. `forward_fill` is the only one
//!   that emits an *empty* interval, carrying the last reading for up to
//!   `max_gap_seconds`; the other two skip it.
//! - `linear` emits `g` once the first reading at or past `g` has arrived,
//!   interpolated against the last one before it. It never emits on the
//!   clock, since there is nothing to interpolate towards.
//!
//! **The tick is arrival time's.** When `time` is a field, the transform is
//! being driven by the readings' own clock and the wall clock says nothing
//! about whether an interval is over — so a quiet key's open interval waits
//! for that key's next reading. With arrival time the two clocks are one, and
//! `wakeup` sleeps until the earliest open interval ends. `flush` re-checks
//! against the clock rather than trusting the wakeup, as every flush does.
//!
//! State per key holds the open interval's readings, the last reading (for
//! `linear` and `forward_fill`), the next grid time, and the group fields as
//! they were on the first reading — since a tick emission has no message to
//! read them off.

use anyhow::{Result, bail};
use kayak_core::streaming::{ResampleMethod, ResampleTransformConfig};
use serde_json::{Map, Value, json};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use super::keyed::{Reading, Series};
use crate::{
    BuildCtx, fields,
    inputs::MessageBatch,
    stats,
    transforms::{BuildTransform, Transform},
};

impl BuildTransform for ResampleTransformConfig {
    fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn Transform>> {
        if self.field.trim().is_empty() {
            bail!("a resample transform needs a 'field'");
        }
        if self.interval_seconds <= 0.0 || !self.interval_seconds.is_finite() {
            bail!("a resample 'interval_seconds' has to be more than zero");
        }
        if self.max_gap_seconds.is_some() && self.method != ResampleMethod::ForwardFill {
            bail!("'max_gap_seconds' only means something for forward_fill");
        }
        if self.max_gap_seconds.is_some_and(|g| g <= 0.0 || !g.is_finite()) {
            bail!("a resample 'max_gap_seconds' has to be more than zero");
        }
        let output = self
            .output
            .unwrap_or_else(|| fields::leaf(&self.field).to_string());
        let time_name = self
            .time
            .as_deref()
            .map_or_else(|| "time".to_string(), |t| fields::leaf(t).to_string());
        let series = Series::resolve(
            ctx,
            "resample",
            format!("resample:{output}"),
            self.group_by,
            self.time,
            self.on_missing,
            self.gate,
        )?;
        Ok(Box::new(ResampleTransform {
            series,
            field: self.field,
            interval: millis(self.interval_seconds),
            method: self.method,
            max_gap: self.max_gap_seconds.map(millis),
            output,
            time_name,
            deadlines: HashMap::new(),
        }))
    }
}

#[allow(clippy::cast_possible_truncation, reason = "seconds to millis, well within range")]
fn millis(seconds: f64) -> i64 {
    (seconds * 1000.0).round().max(1.0) as i64
}

pub struct ResampleTransform {
    series: Series,
    field: String,
    interval: i64,
    method: ResampleMethod,
    max_gap: Option<i64>,
    output: String,
    time_name: String,
    /// When each key's open interval ends, in wall millis — only kept under
    /// arrival time, where the clock is allowed to close one. A mirror of the
    /// bucket, never the truth: `flush` reads the state and drops a key whose
    /// state is gone.
    deadlines: HashMap<String, i64>,
}

/// One emitted grid point.
struct Point {
    at: i64,
    value: f64,
}

impl ResampleTransform {
    fn grid(&self, t: i64) -> i64 {
        t.div_euclid(self.interval) * self.interval
    }

    /// Close every interval that ends at or before `until`, emitting what
    /// each had. For the three interval methods only.
    fn close_until(&self, state: &mut Value, until: i64) -> Vec<Point> {
        let mut out = Vec::new();
        let Some(mut next) = state["next"].as_i64() else {
            return out;
        };
        while next + self.interval <= until {
            let readings: Vec<(i64, f64)> = state["points"]
                .as_array()
                .map(|p| p.iter().filter_map(|r| Some((r[0].as_i64()?, r[1].as_f64()?))).collect())
                .unwrap_or_default();
            let in_interval: Vec<f64> = readings
                .iter()
                .filter(|(t, _)| *t >= next && *t < next + self.interval)
                .map(|(_, v)| *v)
                .collect();
            let value = match self.method {
                ResampleMethod::Last => in_interval.last().copied(),
                ResampleMethod::Mean => stats::mean(&in_interval),
                ResampleMethod::ForwardFill => in_interval.last().copied().or_else(|| {
                    let last_t = state["last"][0].as_i64()?;
                    let last_v = state["last"][1].as_f64()?;
                    (self.max_gap.is_none_or(|gap| next - last_t <= gap)).then_some(last_v)
                }),
                ResampleMethod::Linear => None,
            };
            if let Some(value) = value {
                out.push(Point { at: next, value });
            }
            if let Some(last) = in_interval.last() {
                state["last"] = json!([next + self.interval - 1, last]);
            }
            state["points"] = json!([]);
            next += self.interval;
        }
        state["next"] = json!(next);
        out
    }

    /// One reading against its key's state: what it lets out.
    fn feed(&self, state: &mut Value, t: i64, v: f64, group: &[(String, Value)]) -> Vec<Point> {
        if !state.is_object() {
            let next = match self.method {
                ResampleMethod::Linear => self.grid(t) + if self.grid(t) == t { 0 } else { self.interval },
                _ => self.grid(t),
            };
            *state = json!({
                "next": next,
                "points": [],
                "last": null,
                "group": Value::Object(group.iter().cloned().collect()),
            });
        }
        let mut out = Vec::new();
        if self.method == ResampleMethod::Linear {
            {
                let Some(mut next) = state["next"].as_i64() else { return out };
                let last = state["last"][0].as_i64().zip(state["last"][1].as_f64());
                while next <= t {
                    let value = match last {
                        Some((t0, v0)) if t > t0 => {
                            #[allow(clippy::cast_precision_loss, reason = "millis within an interval")]
                            let fraction = (next - t0) as f64 / (t - t0) as f64;
                            v0 + (v - v0) * fraction
                        }
                        _ => v,
                    };
                    out.push(Point { at: next, value });
                    next += self.interval;
                }
                state["next"] = json!(next);
                state["last"] = json!([t, v]);
            }
        } else {
            {
                out.extend(self.close_until(state, t));
                // a reading from before the open interval is late: it is
                // counted into the interval it would have closed, which is
                // gone, so it is dropped rather than mis-filed
                if state["next"].as_i64().is_some_and(|next| t >= next)
                    && let Some(points) = state["points"].as_array_mut()
                {
                    points.push(json!([t, v]));
                }
            }
        }
        out
    }

    /// The message a grid point comes out as.
    fn message(&self, state: &Value, point: &Point) -> Result<Value> {
        let mut out = Map::new();
        if let Some(group) = state["group"].as_object() {
            out.extend(group.iter().map(|(k, v)| (k.clone(), v.clone())));
        }
        out.insert(self.time_name.clone(), Value::String(crate::time::format(point.at)?));
        out.insert(self.output.clone(), json!(point.value));
        Ok(Value::Object(out))
    }

    /// Whether the clock may close intervals: only when it is the clock the
    /// readings are timed by.
    fn ticks(&self) -> bool {
        self.series.time().is_arrival() && self.method != ResampleMethod::Linear
    }
}

#[async_trait::async_trait]
impl Transform for ResampleTransform {
    async fn apply(&mut self, batch: Arc<MessageBatch>) -> Result<Vec<Arc<MessageBatch>>> {
        let mut out: MessageBatch = Vec::new();
        for message in batch.iter() {
            let Some(key) = self.series.key(message)? else { continue };
            let Reading::Value(value) = self.series.number(message, &self.field)? else { continue };
            let t = self.series.millis(message)?;
            let group = self.series.group_fields_of(message);
            let emitted = self.series.update(&key, |state| {
                let points = self.feed(state, t, value, &group);
                let next = state["next"].as_i64();
                points
                    .iter()
                    .map(|p| self.message(state, p))
                    .collect::<Result<Vec<_>>>()
                    .map(|messages| (messages, next))
            })??;
            if self.ticks()
                && let Some(next) = emitted.1
            {
                self.deadlines.insert(key, next + self.interval);
            }
            out.extend(emitted.0.into_iter().map(Arc::new));
        }
        if out.is_empty() {
            return Ok(vec![]);
        }
        Ok(vec![Arc::new(out)])
    }

    async fn wakeup(&mut self) {
        let Some(earliest) = self.deadlines.values().min().copied() else {
            std::future::pending::<()>().await;
            return;
        };
        let now = chrono::Utc::now().timestamp_millis();
        let wait = u64::try_from(earliest - now).unwrap_or(0);
        tokio::time::sleep(Duration::from_millis(wait)).await;
    }

    async fn flush(&mut self) -> Result<Vec<Arc<MessageBatch>>> {
        let now = chrono::Utc::now().timestamp_millis();
        let due: Vec<String> = self
            .deadlines
            .iter()
            .filter(|(_, deadline)| **deadline <= now)
            .map(|(key, _)| key.clone())
            .collect();
        let mut out: MessageBatch = Vec::new();
        for key in due {
            let closed = self.series.update(&key, |state| {
                if !state.is_object() {
                    return Ok((Vec::new(), None));
                }
                let points = self.close_until(state, now);
                let next = state["next"].as_i64();
                points
                    .iter()
                    .map(|p| self.message(state, p))
                    .collect::<Result<Vec<_>>>()
                    .map(|messages| (messages, next))
            })??;
            match closed.1 {
                // a forward-fill past its gap, or a key whose state was
                // evicted, has nothing more to say on the clock
                Some(next) if !closed.0.is_empty() || self.method == ResampleMethod::ForwardFill => {
                    self.deadlines.insert(key, next + self.interval);
                }
                _ => {
                    self.deadlines.remove(&key);
                }
            }
            out.extend(closed.0.into_iter().map(Arc::new));
        }
        if out.is_empty() {
            return Ok(vec![]);
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

    fn config(method: ResampleMethod) -> ResampleTransformConfig {
        ResampleTransformConfig {
            gate: kayak_core::streaming::Gate::default(),
            field: "v".into(),
            interval_seconds: 10.0,
            method,
            max_gap_seconds: None,
            output: None,
            group_by: vec!["k".into()],
            time: Some("ts".into()),
            on_missing: MissingFieldPolicy::Error,
        }
    }

    fn build(config: ResampleTransformConfig) -> Result<Box<dyn Transform>> {
        let mut pipelines = std::collections::HashMap::new();
        let mut ctx = ctx_with_bucket(&mut pipelines, None);
        config.build(&mut ctx)
    }

    async fn run(config: ResampleTransformConfig, readings: &[(&str, i64, f64)]) -> Result<Vec<Value>> {
        let mut transform = build(config)?;
        let messages = readings.iter().map(|(k, t, v)| json!({"k": k, "ts": t * 1000, "v": v})).collect();
        let out = transform.apply(batch(messages)).await?;
        Ok(out.iter().flat_map(|b| b.iter().map(|m| (**m).clone())).collect())
    }

    fn point(k: &str, secs: i64, v: f64) -> Value {
        json!({"k": k, "ts": crate::time::format(secs * 1000).unwrap_or_default(), "v": v})
    }

    #[tokio::test]
    async fn last_and_mean_close_an_interval_when_a_reading_passes_it() -> Result<()> {
        let readings = [("a", 3, 1.0), ("a", 7, 3.0), ("b", 5, 9.0), ("a", 12, 5.0), ("a", 31, 7.0)];
        let last = run(config(ResampleMethod::Last), &readings).await?;
        assert_eq!(last, vec![point("a", 0, 3.0), point("a", 10, 5.0)], "20 s had nothing and is skipped");
        let mean = run(config(ResampleMethod::Mean), &readings).await?;
        assert_eq!(mean, vec![point("a", 0, 2.0), point("a", 10, 5.0)]);
        Ok(())
    }

    #[tokio::test]
    async fn forward_fill_repeats_the_last_value_up_to_the_gap() -> Result<()> {
        let mut config = config(ResampleMethod::ForwardFill);
        config.max_gap_seconds = Some(25.0);
        let out = run(config, &[("a", 3, 1.0), ("a", 51, 2.0)]).await?;
        // 0 s holds 1.0; 10, 20 and 30 s are within 25 s of the reading at
        // 3 s and repeat it; 40 s is not
        assert_eq!(
            out,
            vec![point("a", 0, 1.0), point("a", 10, 1.0), point("a", 20, 1.0), point("a", 30, 1.0)]
        );
        Ok(())
    }

    #[tokio::test]
    async fn linear_interpolates_at_the_grid_once_the_next_reading_arrives() -> Result<()> {
        let out = run(config(ResampleMethod::Linear), &[("a", 5, 0.0), ("a", 25, 40.0), ("a", 30, 0.0)]).await?;
        assert_eq!(out, vec![point("a", 10, 10.0), point("a", 20, 30.0), point("a", 30, 0.0)]);
        Ok(())
    }

    #[tokio::test]
    async fn as_and_the_time_name_shape_the_message() -> Result<()> {
        let mut config = config(ResampleMethod::Last);
        config.output = Some("value".into());
        config.time = Some("meta.at".into());
        let mut transform = build(config)?;
        let out = transform
            .apply(batch(vec![json!({"k": "a", "meta": {"at": 0}, "v": 1.0}), json!({"k": "a", "meta": {"at": 10_000}, "v": 2.0})]))
            .await?;
        assert_eq!(*out[0][0], json!({"k": "a", "at": "1970-01-01T00:00:00.000Z", "value": 1.0}));
        Ok(())
    }

    /// Arrival time is the one clock the tick may close an interval on.
    #[tokio::test]
    async fn under_arrival_time_the_clock_closes_a_quiet_interval() -> Result<()> {
        let mut config = config(ResampleMethod::ForwardFill);
        config.interval_seconds = 0.05;
        config.time = None;
        let mut transform = build(config)?;
        let first = transform.apply(batch(vec![json!({"k": "a", "v": 4.0})])).await?;
        assert!(first.is_empty(), "the interval the reading fell in is still open");
        tokio::time::sleep(Duration::from_millis(120)).await;
        tokio::time::timeout(Duration::from_millis(50), transform.wakeup())
            .await
            .map_err(|_| anyhow::anyhow!("the wakeup should already be due"))?;
        let flushed = transform.flush().await?;
        let values: Vec<f64> = flushed.iter().flat_map(|b| b.iter().filter_map(|m| m["v"].as_f64())).collect();
        assert!(!values.is_empty() && values.iter().all(|v| *v == 4.0), "{values:?}");
        Ok(())
    }

    #[tokio::test]
    async fn under_field_time_there_is_no_tick() -> Result<()> {
        let mut transform = build(config(ResampleMethod::Last))?;
        transform.apply(batch(vec![json!({"k": "a", "ts": 0, "v": 1.0})])).await?;
        assert!(
            tokio::time::timeout(Duration::from_millis(20), transform.wakeup()).await.is_err(),
            "nothing to wake for"
        );
        Ok(())
    }

    #[test]
    fn contradictions_are_refused_at_build() {
        let mut zero = config(ResampleMethod::Last);
        zero.interval_seconds = 0.0;
        assert!(build(zero).is_err());
        let mut gap = config(ResampleMethod::Mean);
        gap.max_gap_seconds = Some(5.0);
        assert!(build(gap).is_err(), "max_gap is forward_fill's");
    }
}
