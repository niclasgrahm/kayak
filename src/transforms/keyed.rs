//! What every streaming statistics transform shares: a series per key, its
//! state in the pipeline's bucket, its time by the one rule.
//!
//! The declarations are in [`kayak_core::streaming`], and that module's docs
//! carry the argument for the shape. This is the shape made concrete once, so
//! that `deadband`, `derive`, `rolling`, `smooth`, `detect` and `resample` each
//! reduce to "what happens to a number, given the state" and none of them can
//! forget the bucket, the bound or the missing-field rule.
//!
//! Three things are decided here and nowhere else:
//!
//! - **The key is the `group_by` values, rendered.** One field renders as its
//!   bare value (`machine_7`); several render as a JSON array
//!   (`["machine_7","temperature"]`), which is unambiguous and reads in the
//!   state tab. A group field that is missing, null, or an object is a message
//!   with no key, and `on_missing` says what happens to it.
//! - **State is stored under a name of the transform's own** — `<kind>:<the
//!   fields it writes>` — beside whatever else the pipeline remembers under
//!   that key, so a `remember` and a `rolling` in one pipeline share a key
//!   without sharing an entry. Two identical transforms in one chain would
//!   share state; that is a config nobody has a use for, and it is documented
//!   rather than prevented.
//! - **Reading the field is one call with one answer**: a number, "skip this
//!   message", or an error. Absent and null follow `on_missing`; present and
//!   not a number is always an error.

use anyhow::{Context, Result, bail};
use kayak_core::config::MissingFieldPolicy;
use serde_json::Value;
use std::sync::Arc;

use crate::{BuildCtx, buckets::Buckets, fields, time::MessageTime};

/// What one message contributes to its series, or why it doesn't.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Reading {
    /// The number, and the key it belongs under.
    Value(f64),
    /// A message this transform has nothing to say about — no key, or no
    /// field, under `on_missing: skip`. It passes through untouched.
    Skip,
}

/// A series-per-key binding, resolved at build time.
pub struct Series {
    buckets: Arc<Buckets>,
    bucket: String,
    /// The name this transform's state is stored under per key.
    name: String,
    group_by: Vec<String>,
    time: MessageTime,
    on_missing: MissingFieldPolicy,
}

impl Series {
    /// Resolve against the pipeline's `state`, refusing to build without one.
    ///
    /// `name` is what this transform's state is stored under and should be
    /// specific to what it writes (`rolling:temp_avg,temp_max`), so two
    /// different transforms under one key do not read each other's state.
    pub fn resolve(
        ctx: &BuildCtx,
        transform: &str,
        name: String,
        group_by: Vec<String>,
        time: Option<String>,
        on_missing: MissingFieldPolicy,
    ) -> Result<Self> {
        let Some(state) = ctx.state.clone() else {
            bail!(
                "the '{transform}' transform keeps state per key, so the pipeline has to declare \
                 a `state` — add `state: {{ bucket: <name> }}` to the pipeline, and the bucket \
                 itself under `state` at the top of the config"
            );
        };
        if !ctx.buckets.contains(&state.bucket) {
            bail!(
                "the '{transform}' transform names state bucket '{}', which is not declared \
                 under `state` at the top of the config",
                state.bucket
            );
        }
        for field in &group_by {
            if field.trim().is_empty() {
                bail!("the '{transform}' transform has a blank group_by field");
            }
        }
        Ok(Self {
            buckets: Arc::clone(&ctx.buckets),
            bucket: state.bucket,
            name,
            group_by,
            time: MessageTime::new(time),
            on_missing,
        })
    }

    /// The fields the key is made of.
    #[must_use]
    pub fn group_by(&self) -> &[String] {
        &self.group_by
    }

    /// Where time is read from.
    #[must_use]
    pub fn time(&self) -> &MessageTime {
        &self.time
    }

    /// The key a message belongs under, or `None` when it has no key.
    #[must_use]
    pub fn key_of(&self, message: &Value) -> Option<String> {
        let mut parts = Vec::with_capacity(self.group_by.len());
        for field in &self.group_by {
            match fields::get(message, field) {
                Some(Value::String(s)) => parts.push(Value::String(s.clone())),
                Some(v @ (Value::Number(_) | Value::Bool(_))) => parts.push(v.clone()),
                _ => return None,
            }
        }
        Some(match parts.as_slice() {
            [] => String::new(),
            [Value::String(s)] => s.clone(),
            [one] => one.to_string(),
            _ => Value::Array(parts).to_string(),
        })
    }

    /// The key's values as they are written out — under each group field's
    /// leaf name, the reducer's way.
    #[must_use]
    pub fn group_fields_of(&self, message: &Value) -> Vec<(String, Value)> {
        self.group_by
            .iter()
            .filter_map(|field| {
                fields::get(message, field)
                    .map(|v| (fields::leaf(field).to_string(), v.clone()))
            })
            .collect()
    }

    /// The key, or what to do without one.
    pub fn key(&self, message: &Value) -> Result<Option<String>> {
        match self.key_of(message) {
            Some(key) => Ok(Some(key)),
            None if self.on_missing == MissingFieldPolicy::Skip => Ok(None),
            None => bail!(
                "a message is missing a group_by field ({}) or it is not a string, number or \
                 boolean",
                self.group_by.join(", ")
            ),
        }
    }

    /// The field as a number, `Skip` under `on_missing: skip`, or an error.
    pub fn number(&self, message: &Value, field: &str) -> Result<Reading> {
        match fields::get(message, field) {
            None | Some(Value::Null) => match self.on_missing {
                MissingFieldPolicy::Skip => Ok(Reading::Skip),
                MissingFieldPolicy::Error => bail!("field '{field}' is missing from a message"),
            },
            Some(value) => value
                .as_f64()
                .map(Reading::Value)
                .with_context(|| format!("field '{field}' is {}, not a number", fields::describe(value))),
        }
    }

    /// The message's time in milliseconds, by the one rule.
    pub fn millis(&self, message: &Value) -> Result<i64> {
        self.time.millis_of(message)
    }

    /// Edit this transform's state under a key, in place.
    ///
    /// The value is `Null` the first time. The bucket the pipeline named is
    /// resolved at build time, so its being gone at run time is the config
    /// having changed under a running pipeline — an error rather than a
    /// silent restart of the series.
    pub fn update<R>(&self, key: &str, f: impl FnOnce(&mut Value) -> R) -> Result<R> {
        self.buckets
            .update(&self.bucket, key, &self.name, f)
            .with_context(|| format!("state bucket '{}' is no longer declared", self.bucket))
    }
}

/// A run of readings kept as a JSON array under a state name — the window
/// every windowed transform holds, spelled once.
///
/// Stored as `[[millis, value], ...]`, oldest first, so a window can be
/// trimmed by count *and* by age from one representation. Kept as `Value`
/// rather than a typed struct because it is edited where it lies (see
/// [`Buckets::update`]) and a typed round trip would be the clone that
/// facility exists to avoid. The value is any JSON, since the reducer's
/// functions take any — `rolling` holds whatever the field held, `smooth`
/// only ever pushes numbers.
pub struct Window;

impl Window {
    /// Append a point and trim to `size` and, if given, to the last `seconds`
    /// before `millis`.
    pub fn push(state: &mut Value, millis: i64, value: Value, size: usize, seconds: Option<f64>) {
        if !state.is_array() {
            *state = Value::Array(Vec::new());
        }
        let Some(points) = state.as_array_mut() else {
            return;
        };
        points.push(Value::Array(vec![Value::from(millis), value]));
        if let Some(seconds) = seconds {
            #[allow(clippy::cast_possible_truncation, reason = "seconds to millis, well within range")]
            let cutoff = millis - (seconds * 1000.0).round() as i64;
            points.retain(|p| p[0].as_i64().is_none_or(|t| t >= cutoff));
        }
        let excess = points.len().saturating_sub(size.max(1));
        if excess > 0 {
            points.drain(..excess);
        }
    }

    /// The stored points, oldest first — empty for a state that is not yet a
    /// window.
    #[must_use]
    pub fn points(state: &Value) -> &[Value] {
        state.as_array().map_or(&[], Vec::as_slice)
    }

    /// The values of a stored window, oldest first.
    #[must_use]
    pub fn values(points: &[Value]) -> Vec<&Value> {
        points.iter().filter_map(|p| p.get(1)).collect()
    }

    /// The values as numbers, skipping any that aren't — a window that was
    /// only ever pushed numbers loses nothing here.
    #[must_use]
    pub fn numbers(points: &[Value]) -> Vec<f64> {
        points.iter().filter_map(|p| p[1].as_f64()).collect()
    }

    /// The times of a stored window in seconds, oldest first.
    #[must_use]
    pub fn seconds(points: &[Value]) -> Vec<f64> {
        #[allow(clippy::cast_precision_loss, reason = "millis to seconds")]
        points
            .iter()
            .filter_map(|p| p[0].as_i64())
            .map(|t| t as f64 / 1000.0)
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buckets::Buckets;
    use kayak_core::state::{PipelineState, StateBucketConfig, StateBuckets};
    use serde_json::json;

    fn ctx_with_state(
        pipelines: &mut std::collections::HashMap<String, crate::PipelineHandle>,
        state: Option<PipelineState>,
    ) -> BuildCtx<'_> {
        let (events, _) = tokio::sync::broadcast::channel(16);
        let mut ctx = BuildCtx::new(pipelines, "keyed-test".into(), events);
        let mut declared = StateBuckets::new();
        declared.insert("b", StateBucketConfig::default());
        ctx.buckets = Arc::new(Buckets::from_config(&declared));
        ctx.state = state;
        ctx
    }

    fn series(group_by: &[&str], on_missing: MissingFieldPolicy) -> Result<Series> {
        let mut pipelines = std::collections::HashMap::new();
        let ctx = ctx_with_state(
            &mut pipelines,
            Some(PipelineState {
                bucket: "b".into(),
                key: None,
            }),
        );
        Series::resolve(
            &ctx,
            "test",
            "test:x".into(),
            group_by.iter().map(ToString::to_string).collect(),
            None,
            on_missing,
        )
    }

    #[test]
    fn a_pipeline_without_state_cannot_build_one() {
        let mut pipelines = std::collections::HashMap::new();
        let ctx = ctx_with_state(&mut pipelines, None);
        let err = Series::resolve(&ctx, "rolling", "x".into(), vec![], None, MissingFieldPolicy::Error)
            .err()
            .map(|e| e.to_string())
            .unwrap_or_default();
        assert!(err.contains("'rolling'") && err.contains("state"), "{err}");

        let ctx = ctx_with_state(
            &mut pipelines,
            Some(PipelineState {
                bucket: "nope".into(),
                key: None,
            }),
        );
        let err = Series::resolve(&ctx, "rolling", "x".into(), vec![], None, MissingFieldPolicy::Error)
            .err()
            .map(|e| e.to_string())
            .unwrap_or_default();
        assert!(err.contains("'nope'"), "{err}");
    }

    #[test]
    fn the_key_is_the_group_values_rendered() -> Result<()> {
        let one = series(&["machine"], MissingFieldPolicy::Error)?;
        assert_eq!(one.key_of(&json!({"machine": "m7"})), Some("m7".into()));
        assert_eq!(one.key_of(&json!({"machine": 7})), Some("7".into()));
        assert_eq!(one.key_of(&json!({"machine": null})), None);
        assert_eq!(one.key_of(&json!({"machine": {"id": 1}})), None);

        let two = series(&["_meta.machine", "signal"], MissingFieldPolicy::Error)?;
        assert_eq!(
            two.key_of(&json!({"_meta": {"machine": "m7"}, "signal": "temp"})),
            Some(r#"["m7","temp"]"#.into())
        );
        assert_eq!(two.key_of(&json!({"signal": "temp"})), None);

        let none = series(&[], MissingFieldPolicy::Error)?;
        assert_eq!(none.key_of(&json!({})), Some(String::new()));
        Ok(())
    }

    #[test]
    fn absent_follows_on_missing_and_wrong_is_always_an_error() -> Result<()> {
        let strict = series(&["k"], MissingFieldPolicy::Error)?;
        assert!(strict.number(&json!({}), "v").is_err());
        assert!(strict.key(&json!({})).is_err());

        let lax = series(&["k"], MissingFieldPolicy::Skip)?;
        assert_eq!(lax.number(&json!({}), "v")?, Reading::Skip);
        assert_eq!(lax.number(&json!({"v": null}), "v")?, Reading::Skip);
        assert_eq!(lax.key(&json!({}))?, None);
        assert_eq!(lax.number(&json!({"v": 2.5}), "v")?, Reading::Value(2.5));
        let err = lax.number(&json!({"v": "2.5"}), "v").err().map(|e| e.to_string()).unwrap_or_default();
        assert!(err.contains("a string"), "{err}");
        Ok(())
    }

    #[test]
    fn a_window_is_trimmed_by_count_and_by_age() {
        let mut state = Value::Null;
        for (t, v) in [(0, 1.0), (1000, 2.0), (2000, 3.0), (3000, 4.0)] {
            Window::push(&mut state, t, json!(v), 3, None);
        }
        assert_eq!(Window::numbers(Window::points(&state)), vec![2.0, 3.0, 4.0]);

        let mut state = Value::Null;
        for (t, v) in [(0, 1.0), (1000, 2.0), (5000, 3.0)] {
            Window::push(&mut state, t, json!(v), 100, Some(2.5));
        }
        let points = Window::points(&state);
        assert_eq!(Window::numbers(points), vec![3.0]);
        assert_eq!(Window::seconds(points), vec![5.0]);
        assert_eq!(Window::values(points), vec![&json!(3.0)]);
        assert!(Window::points(&Value::Null).is_empty());
    }
}
