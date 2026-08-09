//! `remember` and `recall`: the two halves of a pipeline's state.
//!
//! They are two transforms rather than one because **where they sit in the
//! chain is the semantics.** `recall` after `remember` means a message that
//! carries a new unit id comes out carrying it; before, and it comes out
//! carrying the previous one. Neither is wrong, and a single transform with a
//! flag for it would be a worse way of saying which you meant than the order
//! they are written in.
//!
//! Both resolve the bucket at build time and refuse to build without one, so a
//! misconfigured pipeline is an error at the moment it is created rather than a
//! surprise per batch forever.

use anyhow::{Result, bail};
use kayak_core::config::{
    Condition, NumericFilterOperatorKind, RecallMissingPolicy, RecallTransformConfig, Remembered,
    RememberTransformConfig, StringFilterOperatorKind,
};
use std::collections::HashSet;
use std::sync::Arc;

use crate::{
    BuildCtx,
    buckets::Buckets,
    fields,
    inputs::MessageBatch,
    transforms::{BuildTransform, Transform},
};
use serde_json::Value;

/// What both transforms need to reach the state: the buckets, which one, and
/// what this pipeline's messages are keyed by.
///
/// Resolved once at build time. The bucket is looked up by name on every batch
/// rather than held as a reference, which costs one hash lookup and buys the
/// store the freedom to be rebuilt underneath a running pipeline.
struct Binding {
    buckets: Arc<Buckets>,
    bucket: String,
    /// The field the key is read from. `None` is one bucket-wide value, which
    /// is spelled as a fixed key rather than as a second code path.
    key_field: Option<String>,
}

/// The key a bucket-wide binding writes under. A name no field path can
/// produce, so it cannot collide with a real key.
const WHOLE_BUCKET_KEY: &str = "";

impl Binding {
    /// From the pipeline's `state` block, failing if it hasn't got one.
    fn resolve(ctx: &BuildCtx, transform: &str) -> Result<Self> {
        let Some(state) = ctx.state.clone() else {
            bail!(
                "the '{transform}' transform needs the pipeline to declare a `state` — \
                 add `state: {{ bucket: <name>, key: <field> }}` to the pipeline, and the \
                 bucket itself under `state` at the top of the config"
            );
        };
        if !ctx.buckets.contains(&state.bucket) {
            bail!(
                "the '{transform}' transform names state bucket '{}', which is not declared \
                 under `state` at the top of the config",
                state.bucket
            );
        }
        Ok(Self {
            buckets: Arc::clone(&ctx.buckets),
            bucket: state.bucket,
            key_field: state.key,
        })
    }

    /// The key a message belongs under, or `None` when it carries no key at all
    /// — which is a message this transform can say nothing about.
    fn key_of(&self, message: &Value) -> Option<String> {
        let Some(field) = &self.key_field else {
            return Some(WHOLE_BUCKET_KEY.to_string());
        };
        match fields::get(message, field) {
            // a key has to be something that names a thing; an object or an
            // array as a key would compare by its JSON and read as noise
            Some(Value::String(s)) => Some(s.clone()),
            Some(Value::Number(n)) => Some(n.to_string()),
            Some(Value::Bool(b)) => Some(b.to_string()),
            _ => None,
        }
    }
}

/// Whether a message passes every condition. No conditions is "yes" — a
/// `remember` with no `when` remembers from everything.
fn matches(conditions: &[Condition], message: &Value) -> bool {
    conditions.iter().all(|condition| match condition {
        Condition::Numeric {
            field,
            operator,
            value,
        } => fields::get(message, field)
            .and_then(Value::as_f64)
            .is_some_and(|found| match operator {
                NumericFilterOperatorKind::GreaterThan => found > *value,
                NumericFilterOperatorKind::LessThan => found < *value,
                NumericFilterOperatorKind::EqualTo => (found - *value).abs() < f64::EPSILON,
            }),
        Condition::String {
            field,
            operator,
            value,
        } => fields::get(message, field)
            .and_then(Value::as_str)
            .is_some_and(|found| match operator {
                StringFilterOperatorKind::EqualTo => found == value,
                StringFilterOperatorKind::Contains => found.contains(value),
            }),
    })
}

// ── remember ────────────────────────────────────────────────────────────────

impl BuildTransform for RememberTransformConfig {
    fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn Transform>> {
        if self.remember.is_empty() {
            bail!("a remember transform needs at least one thing to remember");
        }
        let mut names = HashSet::new();
        for remembered in &self.remember {
            let name = remembered.output.trim();
            if name.is_empty() {
                bail!(
                    "the entry remembering '{}' needs an 'as' — it is the name `recall` \
                     asks for it by",
                    remembered.field
                );
            }
            if !names.insert(name.to_string()) {
                bail!("two entries would both be remembered as '{name}'");
            }
        }

        Ok(Box::new(RememberTransform {
            binding: Binding::resolve(ctx, "remember")?,
            when: self.when,
            remember: self.remember,
            warned_about_key: false,
        }))
    }
}

pub struct RememberTransform {
    binding: Binding,
    when: Vec<Condition>,
    remember: Vec<Remembered>,
    /// Whether the "no key on this message" warning has been said.
    ///
    /// Said **once per transform, not once per message**, because the thing it
    /// reports is a config mistake and not an event: a `state.key` naming a
    /// field the stream doesn't carry is wrong for every message that will ever
    /// arrive, and a line per message at a thousand a second buries the log it
    /// is trying to appear in. The same rule the UI feed follows.
    warned_about_key: bool,
}

#[async_trait::async_trait]
impl Transform for RememberTransform {
    /// Writes what matches, and hands the batch straight on.
    ///
    /// The batch comes out exactly as it went in — this is a tap on the stream.
    /// A transform called `remember` that also swallowed what it remembered
    /// would be a surprise, and the message is nearly always still wanted:
    /// in the machine case the `unit_id` message is part of the cycle being
    /// windowed.
    async fn apply(&mut self, batch: Arc<MessageBatch>) -> Result<Vec<Arc<MessageBatch>>> {
        for message in batch.iter() {
            if !matches(&self.when, message) {
                continue;
            }
            let Some(key) = self.binding.key_of(message) else {
                if !self.warned_about_key {
                    self.warned_about_key = true;
                    tracing::warn!(
                        "remember: messages carry no usable value at this pipeline's state \
                         key ('{}'), so nothing is being remembered from them. Reported \
                         once; check the pipeline's `state.key` against what its input \
                         actually sends.",
                        self.binding
                            .key_field
                            .as_deref()
                            .unwrap_or("<the whole bucket>")
                    );
                }
                continue;
            };
            let values: Vec<(String, Value)> = self
                .remember
                .iter()
                .filter_map(|r| {
                    fields::get(message, &r.field)
                        .map(|value| (r.output.trim().to_string(), value.clone()))
                })
                .collect();
            if values.is_empty() {
                continue;
            }
            self.binding
                .buckets
                .remember(&self.binding.bucket, &key, values);
        }
        Ok(vec![batch])
    }
}

// ── recall ──────────────────────────────────────────────────────────────────

impl BuildTransform for RecallTransformConfig {
    fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn Transform>> {
        if self.recall.is_empty() {
            bail!("a recall transform needs at least one name to recall");
        }
        let mut names = HashSet::new();
        for name in &self.recall {
            if name.trim().is_empty() {
                bail!("a recall transform cannot recall an empty name");
            }
            if !names.insert(name.trim().to_string()) {
                bail!("'{}' is recalled twice", name.trim());
            }
        }

        Ok(Box::new(RecallTransform {
            binding: Binding::resolve(ctx, "recall")?,
            recall: self.recall.iter().map(|n| n.trim().to_string()).collect(),
            on_missing: self.on_missing,
            warned_about_shape: false,
        }))
    }
}

pub struct RecallTransform {
    binding: Binding,
    recall: Vec<String>,
    on_missing: RecallMissingPolicy,
    /// Said once, for the reason [`RememberTransform::warned_about_key`] is.
    warned_about_shape: bool,
}

#[async_trait::async_trait]
impl Transform for RecallTransform {
    async fn apply(&mut self, batch: Arc<MessageBatch>) -> Result<Vec<Arc<MessageBatch>>> {
        let mut out: MessageBatch = Vec::with_capacity(batch.len());
        for message in batch.iter() {
            let recalled = match self.binding.key_of(message) {
                Some(key) => {
                    self.binding
                        .buckets
                        .recall(&self.binding.bucket, &key, &self.recall)
                }
                // no key is the same situation as an empty bucket as far as
                // this message is concerned: nothing is known about it
                None => self.recall.iter().map(|_| None).collect(),
            };

            if recalled.iter().any(Option::is_none) {
                match self.on_missing {
                    RecallMissingPolicy::Skip => continue,
                    RecallMissingPolicy::Error => {
                        let missing: Vec<&str> = self
                            .recall
                            .iter()
                            .zip(&recalled)
                            .filter(|(_, value)| value.is_none())
                            .map(|(name, _)| name.as_str())
                            .collect();
                        bail!(
                            "nothing is remembered as {:?} in state bucket '{}' for this message",
                            missing,
                            self.binding.bucket
                        );
                    }
                    RecallMissingPolicy::Null => {}
                }
            }

            // written onto the message rather than under a prefix, so a reducer
            // downstream groups by `unit_id` without knowing where it came from
            let mut message = (**message).clone();
            if let Some(object) = message.as_object_mut() {
                for (name, value) in self.recall.iter().zip(recalled) {
                    object.insert(name.clone(), value.unwrap_or(Value::Null));
                }
            } else if !self.warned_about_shape {
                self.warned_about_shape = true;
                tracing::warn!(
                    "recall: messages are not json objects, so there is nowhere to write \
                     the recalled values; they are passed on unchanged. Reported once."
                );
            }
            out.push(Arc::new(message));
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
    use kayak_core::state::{PipelineState, StateBucketConfig, StateBuckets};
    use serde_json::json;

    fn declared() -> StateBuckets {
        let mut buckets = StateBuckets::new();
        buckets.insert("machines", StateBucketConfig::default());
        buckets
    }

    /// A build against a real bucket, the way `AppState` does it.
    fn ctx_parts(key: Option<&str>) -> (Arc<Buckets>, Option<PipelineState>) {
        (
            Arc::new(Buckets::from_config(&declared())),
            Some(PipelineState {
                bucket: "machines".to_string(),
                key: key.map(ToString::to_string),
            }),
        )
    }

    fn build<T: BuildTransform>(
        config: T,
        buckets: &Arc<Buckets>,
        state: Option<PipelineState>,
    ) -> Result<Box<dyn Transform>> {
        let (events, _) = tokio::sync::broadcast::channel(16);
        let mut pipelines = std::collections::HashMap::new();
        let mut ctx = BuildCtx::new(&mut pipelines, "state-test".into(), events)
            .with_buckets(Arc::clone(buckets))
            .with_state(state);
        config.build(&mut ctx)
    }

    fn remember_config(when: Vec<Condition>, field: &str, as_name: &str) -> RememberTransformConfig {
        RememberTransformConfig {
            when,
            remember: vec![Remembered {
                field: field.to_string(),
                output: as_name.to_string(),
            }],
        }
    }

    fn is_signal(name: &str) -> Condition {
        Condition::String {
            field: "signal".to_string(),
            operator: StringFilterOperatorKind::EqualTo,
            value: name.to_string(),
        }
    }

    async fn run(t: &mut Box<dyn Transform>, messages: Vec<Value>) -> Result<Vec<Value>> {
        let out = t.apply(batch(messages)).await?;
        Ok(out
            .iter()
            .flat_map(|b| b.iter().map(|m| (**m).clone()).collect::<Vec<_>>())
            .collect())
    }

    /// The worked case, end to end through both transforms: a slow-moving fact
    /// on one signal reaches the fast readings on another.
    #[tokio::test]
    async fn what_one_message_remembers_the_next_recalls() -> Result<()> {
        let (buckets, state) = ctx_parts(Some("machine_id"));
        let mut remember = build(
            remember_config(vec![is_signal("unit_id")], "value", "unit_id"),
            &buckets,
            state.clone(),
        )?;
        let mut recall = build(
            RecallTransformConfig {
                recall: vec!["unit_id".to_string()],
                on_missing: RecallMissingPolicy::Skip,
            },
            &buckets,
            state,
        )?;

        run(
            &mut remember,
            vec![json!({"machine_id": "m1", "signal": "unit_id", "value": "u-7"})],
        )
        .await?;

        let out = run(
            &mut recall,
            vec![json!({"machine_id": "m1", "signal": "temperature", "value": 21.5})],
        )
        .await?;

        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["unit_id"], json!("u-7"));
        assert_eq!(out[0]["value"], json!(21.5), "the payload is left alone");
        Ok(())
    }

    /// `when` is what makes a stream carrying several kinds of thing usable:
    /// without it every message's `value` would overwrite the remembered one.
    #[tokio::test]
    async fn only_matching_messages_are_remembered_from() -> Result<()> {
        let (buckets, state) = ctx_parts(Some("machine_id"));
        let mut remember = build(
            remember_config(vec![is_signal("unit_id")], "value", "unit_id"),
            &buckets,
            state,
        )?;

        run(
            &mut remember,
            vec![
                json!({"machine_id": "m1", "signal": "unit_id", "value": "u-7"}),
                json!({"machine_id": "m1", "signal": "temperature", "value": 99.0}),
            ],
        )
        .await?;

        assert_eq!(
            buckets.recall("machines", "m1", &["unit_id".to_string()]),
            vec![Some(json!("u-7"))],
            "a temperature reading overwrote the remembered unit id"
        );
        Ok(())
    }

    /// A tap, not a filter — the batch comes out as it went in, boundary
    /// messages and all.
    #[tokio::test]
    async fn remember_passes_every_message_on_unchanged() -> Result<()> {
        let (buckets, state) = ctx_parts(Some("machine_id"));
        let mut remember = build(
            remember_config(vec![is_signal("unit_id")], "value", "unit_id"),
            &buckets,
            state,
        )?;

        let messages = vec![
            json!({"machine_id": "m1", "signal": "unit_id", "value": "u-7"}),
            json!({"machine_id": "m1", "signal": "temperature", "value": 99.0}),
        ];
        assert_eq!(run(&mut remember, messages.clone()).await?, messages);
        Ok(())
    }

    /// The key is what keeps two machines apart.
    #[tokio::test]
    async fn each_key_recalls_its_own_value() -> Result<()> {
        let (buckets, state) = ctx_parts(Some("machine_id"));
        let mut remember = build(
            remember_config(vec![], "unit", "unit_id"),
            &buckets,
            state.clone(),
        )?;
        let mut recall = build(
            RecallTransformConfig {
                recall: vec!["unit_id".to_string()],
                on_missing: RecallMissingPolicy::Skip,
            },
            &buckets,
            state,
        )?;

        run(
            &mut remember,
            vec![
                json!({"machine_id": "m1", "unit": "u-1"}),
                json!({"machine_id": "m2", "unit": "u-2"}),
            ],
        )
        .await?;

        let out = run(
            &mut recall,
            vec![json!({"machine_id": "m2", "reading": 1})],
        )
        .await?;
        assert_eq!(out[0]["unit_id"], json!("u-2"));
        Ok(())
    }

    /// Every stateful pipeline has a warm-up where nothing is remembered yet.
    /// The default drops those messages rather than passing them on
    /// unattributed, which would give a reducer one bogus group.
    #[tokio::test]
    async fn the_default_drops_a_message_with_nothing_remembered_for_it() -> Result<()> {
        let (buckets, state) = ctx_parts(Some("machine_id"));
        let mut recall = build(
            RecallTransformConfig {
                recall: vec!["unit_id".to_string()],
                on_missing: RecallMissingPolicy::default(),
            },
            &buckets,
            state,
        )?;

        let out = run(&mut recall, vec![json!({"machine_id": "m1"})]).await?;
        assert!(out.is_empty(), "an unattributable message was passed on");
        Ok(())
    }

    #[tokio::test]
    async fn null_passes_the_message_on_with_the_gap_showing() -> Result<()> {
        let (buckets, state) = ctx_parts(Some("machine_id"));
        let mut recall = build(
            RecallTransformConfig {
                recall: vec!["unit_id".to_string()],
                on_missing: RecallMissingPolicy::Null,
            },
            &buckets,
            state,
        )?;

        let out = run(&mut recall, vec![json!({"machine_id": "m1"})]).await?;
        assert_eq!(out.len(), 1);
        assert_eq!(out[0]["unit_id"], json!(null));
        Ok(())
    }

    #[tokio::test]
    async fn error_fails_the_batch_and_says_what_was_missing() -> Result<()> {
        let (buckets, state) = ctx_parts(Some("machine_id"));
        let mut recall = build(
            RecallTransformConfig {
                recall: vec!["unit_id".to_string()],
                on_missing: RecallMissingPolicy::Error,
            },
            &buckets,
            state,
        )?;

        let Err(err) = run(&mut recall, vec![json!({"machine_id": "m1"})]).await else {
            panic!("expected the missing value to fail the batch")
        };
        assert!(format!("{err:#}").contains("unit_id"), "got: {err:#}");
        Ok(())
    }

    /// No `key` is one bucket-wide value — right for something there is only
    /// ever one of.
    #[tokio::test]
    async fn without_a_key_the_bucket_holds_one_value() -> Result<()> {
        let (buckets, state) = ctx_parts(None);
        let mut remember =
            build(remember_config(vec![], "v", "latest"), &buckets, state.clone())?;
        let mut recall = build(
            RecallTransformConfig {
                recall: vec!["latest".to_string()],
                on_missing: RecallMissingPolicy::Skip,
            },
            &buckets,
            state,
        )?;

        run(&mut remember, vec![json!({"v": 1})]).await?;
        let out = run(&mut recall, vec![json!({"anything": true})]).await?;
        assert_eq!(out[0]["latest"], json!(1));
        Ok(())
    }

    /// Both transforms are useless without somewhere to put things, and say so
    /// when the pipeline is created rather than once per batch forever.
    #[test]
    fn neither_transform_builds_without_a_state_on_the_pipeline() {
        let (buckets, _) = ctx_parts(None);
        let remember = build(remember_config(vec![], "v", "latest"), &buckets, None);
        let Err(err) = remember else {
            panic!("remember built without a state block")
        };
        assert!(format!("{err:#}").contains("state"), "got: {err:#}");

        assert!(
            build(
                RecallTransformConfig {
                    recall: vec!["latest".to_string()],
                    on_missing: RecallMissingPolicy::Skip,
                },
                &buckets,
                None,
            )
            .is_err()
        );
    }

    /// Naming a bucket nobody declared is a config mistake, and the error says
    /// which name it was.
    #[test]
    fn naming_an_undeclared_bucket_fails_to_build() {
        let (buckets, _) = ctx_parts(None);
        let Err(err) = build(
            remember_config(vec![], "v", "latest"),
            &buckets,
            Some(PipelineState {
                bucket: "nope".to_string(),
                key: None,
            }),
        ) else {
            panic!("built against an undeclared bucket")
        };
        assert!(format!("{err:#}").contains("nope"), "got: {err:#}");
    }

    /// Two entries sharing an `as` would have one silently overwrite the other,
    /// the same way two aggregations sharing one would.
    #[test]
    fn two_entries_remembered_under_one_name_are_refused() {
        let (buckets, state) = ctx_parts(None);
        let config = RememberTransformConfig {
            when: vec![],
            remember: vec![
                Remembered {
                    field: "a".to_string(),
                    output: "same".to_string(),
                },
                Remembered {
                    field: "b".to_string(),
                    output: "same".to_string(),
                },
            ],
        };
        let Err(err) = build(config, &buckets, state) else {
            panic!("expected the collision to be refused")
        };
        assert!(format!("{err:#}").contains("same"), "got: {err:#}");
    }

    #[test]
    fn an_empty_remember_or_recall_is_refused() {
        let (buckets, state) = ctx_parts(None);
        assert!(
            build(
                RememberTransformConfig {
                    when: vec![],
                    remember: vec![],
                },
                &buckets,
                state.clone(),
            )
            .is_err()
        );
        assert!(
            build(
                RecallTransformConfig {
                    recall: vec![],
                    on_missing: RecallMissingPolicy::Skip,
                },
                &buckets,
                state,
            )
            .is_err()
        );
    }

    /// Conditions are read as "all of these", and a message missing the field
    /// simply doesn't match rather than erroring.
    #[tokio::test]
    async fn several_conditions_all_have_to_match() -> Result<()> {
        let (buckets, state) = ctx_parts(Some("machine_id"));
        let mut remember = build(
            remember_config(
                vec![
                    is_signal("cycle_status"),
                    Condition::Numeric {
                        field: "value".to_string(),
                        operator: NumericFilterOperatorKind::EqualTo,
                        value: 1.0,
                    },
                ],
                "value",
                "started",
            ),
            &buckets,
            state,
        )?;

        run(
            &mut remember,
            vec![
                json!({"machine_id": "m1", "signal": "cycle_status", "value": 0}),
                json!({"machine_id": "m1", "signal": "temperature", "value": 1}),
            ],
        )
        .await?;
        assert_eq!(
            buckets.recall("machines", "m1", &["started".to_string()]),
            vec![None],
            "a message matching only one condition was remembered"
        );

        run(
            &mut remember,
            vec![json!({"machine_id": "m1", "signal": "cycle_status", "value": 1})],
        )
        .await?;
        assert_eq!(
            buckets.recall("machines", "m1", &["started".to_string()]),
            vec![Some(json!(1))]
        );
        Ok(())
    }

    /// The key is a dotted path like anywhere else, which is what makes
    /// metadata usable as one — the machine's name lives in the subject.
    #[tokio::test]
    async fn the_key_can_be_a_path_into_the_metadata() -> Result<()> {
        let (buckets, state) = ctx_parts(Some("_meta.machine_id"));
        let mut remember = build(remember_config(vec![], "v", "unit_id"), &buckets, state)?;

        run(
            &mut remember,
            vec![json!({"_meta": {"machine_id": "m9"}, "v": "u-1"})],
        )
        .await?;
        assert_eq!(
            buckets.recall("machines", "m9", &["unit_id".to_string()]),
            vec![Some(json!("u-1"))]
        );
        Ok(())
    }


    /// A `state.key` naming a field the stream doesn't carry is wrong for every
    /// message that will ever arrive. It is reported once — a line per message
    /// at a thousand a second buries the log it is trying to appear in.
    #[tokio::test]
    async fn a_missing_key_is_reported_once_rather_than_per_message() -> Result<()> {
        let (buckets, state) = ctx_parts(Some("machine_id"));
        let mut remember = build(remember_config(vec![], "v", "unit_id"), &buckets, state)?;

        // none of these carry `machine_id`
        let messages = vec![json!({"v": 1}), json!({"v": 2}), json!({"v": 3})];
        assert_eq!(
            run(&mut remember, messages.clone()).await?,
            messages,
            "the batch is still passed on"
        );
        run(&mut remember, messages).await?;

        assert_eq!(buckets.summaries()[0].keys, 0, "nothing should be stored");
        Ok(())
    }

    /// A key that is a number or a bool names a thing just as well as a string
    /// does; an object or an array does not, and is refused rather than
    /// stringified into something that reads as noise.
    #[tokio::test]
    async fn a_key_may_be_any_scalar_but_not_a_structure() -> Result<()> {
        let (buckets, state) = ctx_parts(Some("id"));
        let mut remember = build(remember_config(vec![], "v", "seen"), &buckets, state)?;

        run(
            &mut remember,
            vec![
                json!({"id": 7, "v": "by number"}),
                json!({"id": true, "v": "by bool"}),
                json!({"id": {"nested": 1}, "v": "by object"}),
                json!({"id": ["a"], "v": "by array"}),
            ],
        )
        .await?;

        assert_eq!(
            buckets.recall("machines", "7", &["seen".to_string()]),
            vec![Some(json!("by number"))]
        );
        assert_eq!(
            buckets.recall("machines", "true", &["seen".to_string()]),
            vec![Some(json!("by bool"))]
        );
        assert_eq!(
            buckets.summaries()[0].keys,
            2,
            "an object or array key was stored"
        );
        Ok(())
    }
}
