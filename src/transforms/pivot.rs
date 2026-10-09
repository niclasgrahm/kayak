//! `pivot`: one reading per message in, the latest of each per key out.
//!
//! The declaration is [`kayak_core::streaming::PivotTransformConfig`]; the
//! shape is [`super::keyed`]. The state per key is an object from name to the
//! latest value — bounded by `names`, which is why that list is required —
//! and a message is written on *after* its own reading is taken, so it always
//! carries itself.

use anyhow::{Result, bail};
use kayak_core::config::MissingFieldPolicy;
use kayak_core::streaming::PivotTransformConfig;
use serde_json::{Map, Value};
use std::collections::HashSet;
use std::sync::Arc;

use super::keyed::Series;
use crate::{
    BuildCtx, fields,
    inputs::MessageBatch,
    transforms::{BuildTransform, Transform},
};

impl BuildTransform for PivotTransformConfig {
    fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn Transform>> {
        for (what, field) in [("name", &self.name), ("value", &self.value)] {
            if field.trim().is_empty() {
                bail!("a pivot needs a '{what}' field");
            }
        }
        if self.names.is_empty() {
            bail!("a pivot needs at least one of 'names' — they are the row it builds, and its bound");
        }
        let mut seen = HashSet::new();
        for name in &self.names {
            if name.trim().is_empty() {
                bail!("a pivot has a blank entry in 'names'");
            }
            if !seen.insert(name.as_str()) {
                bail!("a pivot names '{name}' twice");
            }
        }
        if self.into.as_ref().is_some_and(|into| into.trim().is_empty()) {
            bail!("a pivot's 'into' is blank — leave it out to write at the top level");
        }
        let series = Series::resolve(
            ctx,
            "pivot",
            format!("pivot:{}", self.into.as_deref().unwrap_or("")),
            self.group_by,
            None,
            self.on_missing,
            self.gate,
        )?;
        Ok(Box::new(PivotTransform {
            series,
            name: self.name,
            value: self.value,
            names: self.names,
            into: self.into,
            on_missing: self.on_missing,
        }))
    }
}

pub struct PivotTransform {
    series: Series,
    name: String,
    value: String,
    names: Vec<String>,
    into: Option<String>,
    on_missing: MissingFieldPolicy,
}

impl PivotTransform {
    /// The reading this message contributes, if any: which of `names`, and
    /// its value. `Ok(None)` for a message about something else.
    fn reading(&self, message: &Value) -> Result<Option<(String, Value)>> {
        let Some(name) = fields::get(message, &self.name).and_then(Value::as_str) else {
            return Ok(None);
        };
        if !self.names.iter().any(|n| n == name) {
            return Ok(None);
        }
        match fields::get(message, &self.value) {
            Some(value) if !value.is_null() => Ok(Some((name.to_string(), value.clone()))),
            _ => match self.on_missing {
                MissingFieldPolicy::Skip => Ok(None),
                MissingFieldPolicy::Error => {
                    bail!("a message names '{name}' but carries no '{}'", self.value)
                }
            },
        }
    }

    /// Where one name is written on a message.
    fn path(&self, name: &str) -> String {
        match &self.into {
            Some(into) => format!("{into}.{name}"),
            None => name.to_string(),
        }
    }
}

#[async_trait::async_trait]
impl Transform for PivotTransform {
    async fn apply(&mut self, batch: Arc<MessageBatch>) -> Result<Vec<Arc<MessageBatch>>> {
        let mut out: MessageBatch = Vec::with_capacity(batch.len());
        for message in batch.iter() {
            let Some(key) = self.series.key(message)? else {
                out.push(Arc::clone(message));
                continue;
            };
            let reading = self.reading(message)?;
            let row = self.series.update(&key, |state| {
                if !state.is_object() {
                    *state = Value::Object(Map::new());
                }
                if let (Some((name, value)), Some(row)) = (&reading, state.as_object_mut()) {
                    row.insert(name.clone(), value.clone());
                }
                state.clone()
            })?;
            let mut written = (**message).clone();
            // in `names` order, so the row reads the same way on every message
            for name in &self.names {
                if let Some(value) = row.get(name) {
                    fields::set(&mut written, &self.path(name), value.clone())?;
                }
            }
            out.push(Arc::new(written));
        }
        Ok(vec![Arc::new(out)])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::{batch, ctx_with_bucket};
    use kayak_core::streaming::Gate;
    use serde_json::json;

    fn config(names: &[&str]) -> PivotTransformConfig {
        PivotTransformConfig {
            name: "sensor".into(),
            value: "value".into(),
            names: names.iter().map(ToString::to_string).collect(),
            into: None,
            group_by: vec!["device".into()],
            on_missing: MissingFieldPolicy::Error,
            gate: Gate::default(),
        }
    }

    async fn pivoted(config: PivotTransformConfig, messages: Vec<Value>) -> Result<Vec<Value>> {
        let mut pipelines = std::collections::HashMap::new();
        let mut ctx = ctx_with_bucket(&mut pipelines, None);
        let mut transform = config.build(&mut ctx)?;
        let out = transform.apply(batch(messages)).await?;
        Ok(out.iter().flat_map(|b| b.iter().map(|m| (**m).clone())).collect())
    }

    #[tokio::test]
    async fn each_message_carries_the_latest_of_every_name_so_far() -> Result<()> {
        let out = pivoted(
            config(&["state", "fault", "good"]),
            vec![
                json!({"device": "m1", "sensor": "state", "value": "RUNNING"}),
                json!({"device": "m1", "sensor": "good", "value": 41}),
                json!({"device": "m1", "sensor": "fault", "value": "NONE"}),
                json!({"device": "m1", "sensor": "state", "value": "FAULT"}),
            ],
        )
        .await?;
        assert_eq!(
            out[0],
            json!({"device": "m1", "sensor": "state", "value": "RUNNING", "state": "RUNNING"}),
            "a name not seen yet is left out, not null"
        );
        assert_eq!(out[1]["state"], json!("RUNNING"));
        assert_eq!(out[1]["good"], json!(41), "a message carries its own reading");
        assert_eq!(
            out[3],
            json!({"device": "m1", "sensor": "state", "value": "FAULT",
                   "state": "FAULT", "fault": "NONE", "good": 41})
        );
        Ok(())
    }

    #[tokio::test]
    async fn rows_are_per_key_and_unlisted_names_contribute_nothing() -> Result<()> {
        let out = pivoted(
            config(&["state"]),
            vec![
                json!({"device": "m1", "sensor": "state", "value": "RUNNING"}),
                json!({"device": "m2", "sensor": "state", "value": "OFF"}),
                json!({"device": "m1", "sensor": "temperature", "value": 650}),
            ],
        )
        .await?;
        assert_eq!(out[1]["state"], json!("OFF"));
        assert_eq!(out[2]["state"], json!("RUNNING"), "m2's state did not reach m1");
        assert!(out[2].get("temperature").is_none(), "an unlisted name is not remembered");
        Ok(())
    }

    #[tokio::test]
    async fn into_writes_the_row_under_an_object() -> Result<()> {
        let mut config = config(&["state"]);
        config.into = Some("machine".into());
        let out = pivoted(config, vec![json!({"device": "m1", "sensor": "state", "value": "OFF"})]).await?;
        assert_eq!(out[0]["machine"], json!({"state": "OFF"}));
        Ok(())
    }

    /// A message naming one of `names` but carrying no value is the sparse
    /// stream `on_missing` is about.
    #[tokio::test]
    async fn a_named_reading_without_a_value_follows_on_missing() -> Result<()> {
        let message = || vec![json!({"device": "m1", "sensor": "state"})];
        assert!(pivoted(config(&["state"]), message()).await.is_err());
        let mut lax = config(&["state"]);
        lax.on_missing = MissingFieldPolicy::Skip;
        assert_eq!(pivoted(lax, message()).await?, message());
        Ok(())
    }

    #[test]
    fn a_pivot_that_cannot_mean_anything_is_refused() {
        let mut pipelines = std::collections::HashMap::new();
        let mut ctx = ctx_with_bucket(&mut pipelines, None);
        let mut blank_into = config(&["a"]);
        blank_into.into = Some(" ".into());
        for (what, config) in [
            ("no names", config(&[])),
            ("a duplicate", config(&["a", "a"])),
            ("a blank name", config(&["a", ""])),
            ("a blank into", blank_into),
        ] {
            assert!(config.build(&mut ctx).is_err(), "{what} was accepted");
        }
    }
}
