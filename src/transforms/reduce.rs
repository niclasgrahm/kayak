use anyhow::{Context, bail};
use kayak_core::config::{Aggregation, MissingFieldPolicy, ReduceFnKind, ReduceTransformConfig};
use serde_json::{Map, Value};
use std::collections::HashSet;
use std::sync::Arc;

use crate::{
    BuildCtx,
    inputs::MessageBatch,
    transforms::{BuildTransform, Transform},
};

impl BuildTransform for ReduceTransformConfig {
    fn build(self, _ctx: &mut BuildCtx) -> anyhow::Result<Box<dyn Transform>> {
        // Everything below is a config mistake that would otherwise show up as
        // a strange-looking message rather than as an error: an aggregation
        // with no field can't aggregate, and two sharing a name would have one
        // quietly overwrite the other. Refusing to build says so once, at the
        // point the pipeline is created, instead of once per batch forever.
        if self.aggregations.is_empty() {
            bail!("a reducer needs at least one aggregation");
        }
        // group fields are written out under their leaf name (see `reduce`), so
        // it is the leaves that have to be distinct — `a.id` and `b.id` are two
        // different groupings that would land on one field.
        let mut group_names = HashSet::new();
        for field in &self.group_by {
            let name = crate::fields::leaf(field);
            if !group_names.insert(name.to_string()) {
                bail!(
                    "two group_by fields would both be written out as '{name}' — \
                     rename one, or group by a field whose last segment differs"
                );
            }
        }

        let mut names = HashSet::new();
        for aggregation in &self.aggregations {
            let name = aggregation.output.trim();
            if name.is_empty() {
                bail!(
                    "the '{:?}' aggregation needs an 'as' — it is the field the answer is written to",
                    aggregation.function
                );
            }
            if aggregation.function != ReduceFnKind::Count && aggregation.field.is_none() {
                bail!("the '{name}' aggregation needs a 'field' to aggregate");
            }
            if !names.insert(name.to_string()) {
                bail!("two aggregations are both called '{name}'");
            }
            if group_names.contains(name) {
                bail!("the '{name}' aggregation would overwrite the group_by field of that name");
            }
        }

        Ok(Box::new(ReduceTransform {
            aggregations: self.aggregations,
            group_by: self.group_by,
            on_missing: self.on_missing,
        }))
    }
}

pub struct ReduceTransform {
    aggregations: Vec<Aggregation>,
    group_by: Vec<String>,
    on_missing: MissingFieldPolicy,
}

/// One group being accumulated: the values its key was built from, and the
/// messages that landed in it.
///
/// The messages are kept rather than the running answers because the functions
/// don't share one accumulator — `median` needs every value, `collect` is every
/// value — and a batch is already entirely in memory by the time it gets here.
struct Group {
    key: Vec<Value>,
    messages: Vec<Arc<Value>>,
}

#[async_trait::async_trait]
impl Transform for ReduceTransform {
    async fn apply(
        &mut self,
        message_batch: Arc<MessageBatch>,
    ) -> anyhow::Result<Vec<Arc<MessageBatch>>> {
        // an empty batch is normal — a tumbling window can close without ever
        // receiving a message. There is nothing to reduce, so emit nothing
        // rather than inventing a 0 / NaN.
        if message_batch.is_empty() {
            return Ok(vec![]);
        }

        let groups = self.group(&message_batch)?;
        let messages: Vec<Arc<Value>> = groups
            .into_iter()
            .map(|group| self.reduce(&group).map(Arc::new))
            .collect::<anyhow::Result<_>>()?;

        if messages.is_empty() {
            return Ok(vec![]);
        }
        Ok(vec![Arc::new(messages)])
    }
}

impl ReduceTransform {
    /// Split a batch into its groups, in the order they were first seen.
    ///
    /// First-seen rather than sorted, because a reducer sits in a stream and
    /// the order messages arrived in is the only order it has any claim to. A
    /// linear scan of the keys is what does the lookup: a group count runs to
    /// the handful of distinct values a batch holds, and hashing a `Vec<Value>`
    /// would mean giving `Value` a `Hash` it doesn't have.
    fn group(&self, batch: &MessageBatch) -> anyhow::Result<Vec<Group>> {
        if self.group_by.is_empty() {
            return Ok(vec![Group {
                key: Vec::new(),
                messages: batch.clone(),
            }]);
        }

        let mut groups: Vec<Group> = Vec::new();
        for message in batch {
            let mut key = Vec::with_capacity(self.group_by.len());
            let mut complete = true;
            for name in &self.group_by {
                if let Some(value) = present(message, name) {
                    key.push(value.clone());
                } else {
                    // a message that can't be placed in a group can't be
                    // reduced at all, so this one is about the message rather
                    // than about one aggregation
                    if self.on_missing == MissingFieldPolicy::Error {
                        bail!("group_by field '{name}' is missing from a message");
                    }
                    complete = false;
                    break;
                }
            }
            if !complete {
                continue;
            }
            match groups.iter_mut().find(|group| group.key == key) {
                Some(group) => group.messages.push(Arc::clone(message)),
                None => groups.push(Group {
                    key,
                    messages: vec![Arc::clone(message)],
                }),
            }
        }
        Ok(groups)
    }

    /// One group as the message it comes out as: what it was grouped by, then
    /// what was asked about it.
    fn reduce(&self, group: &Group) -> anyhow::Result<Value> {
        let mut out = Map::new();
        for (name, value) in self.group_by.iter().zip(&group.key) {
            // a path is written out under its leaf: grouping by
            // `_meta.machine_id` emits `machine_id`, because that is the field
            // the messages coming out of here carry and nothing downstream
            // should have to spell this pipeline's input shape. Two paths with
            // the same leaf are refused at build time rather than one quietly
            // overwriting the other.
            out.insert(crate::fields::leaf(name).to_string(), value.clone());
        }
        for aggregation in &self.aggregations {
            let values = self.values_for(aggregation, &group.messages)?;
            let answer = apply_function(aggregation, &values, group.messages.len())
                .with_context(|| format!("aggregation '{}'", aggregation.output))?;
            out.insert(aggregation.output.trim().to_string(), answer);
        }
        Ok(Value::Object(out))
    }

    /// The values one aggregation sees: its field across the group's messages,
    /// with the missing ones handled however the config says.
    fn values_for<'a>(
        &self,
        aggregation: &Aggregation,
        messages: &'a [Arc<Value>],
    ) -> anyhow::Result<Vec<&'a Value>> {
        // `count` without a field counts messages, so it asks for no values at
        // all — and cannot be short of one
        let Some(field) = aggregation.field.as_deref() else {
            return Ok(Vec::new());
        };
        let mut values = Vec::with_capacity(messages.len());
        for message in messages {
            match present(message, field) {
                Some(value) => values.push(value),
                None if self.on_missing == MissingFieldPolicy::Skip => {}
                None => bail!(
                    "field '{field}' is missing from a message (aggregation '{}')",
                    aggregation.output
                ),
            }
        }
        Ok(values)
    }
}

/// A field's value, if the message really carries one.
///
/// An explicit `null` reads as missing: "the field isn't there" and "the field
/// is there and empty" are the same fact for anything being aggregated, and
/// treating them differently would make `sum` fail on one and not the other.
fn present<'a>(message: &'a Value, field: &str) -> Option<&'a Value> {
    match crate::fields::get(message, field) {
        Some(Value::Null) | None => None,
        Some(value) => Some(value),
    }
}

/// One aggregation's answer over the values it was given.
///
/// `messages` is how many messages were in the group, which only `count`
/// without a field has any use for.
fn apply_function(
    aggregation: &Aggregation,
    values: &[&Value],
    messages: usize,
) -> anyhow::Result<Value> {
    let function = aggregation.function;
    match function {
        // with a field it counts the messages that carried one, which is the
        // difference between "how many messages" and "how many readings"
        ReduceFnKind::Count => {
            let count = if aggregation.field.is_some() {
                values.len()
            } else {
                messages
            };
            Ok(Value::from(count))
        }
        ReduceFnKind::CountDistinct => Ok(Value::from(distinct(values))),
        ReduceFnKind::Collect => Ok(Value::Array(values.iter().map(|v| (*v).clone()).collect())),
        ReduceFnKind::First => Ok(values.first().map_or(Value::Null, |v| (*v).clone())),
        ReduceFnKind::Last => Ok(values.last().map_or(Value::Null, |v| (*v).clone())),
        ReduceFnKind::Min | ReduceFnKind::Max => extreme(function, values),
        // a group with nothing to average has no average; a 0 would be a
        // reading nobody took
        _ if values.is_empty() => Ok(Value::Null),
        ReduceFnKind::Sum | ReduceFnKind::Avg | ReduceFnKind::Median | ReduceFnKind::Stddev => {
            let numbers = numbers(values)?;
            Ok(Value::from(match function {
                ReduceFnKind::Sum => numbers.iter().sum::<f64>(),
                ReduceFnKind::Avg => mean(&numbers),
                ReduceFnKind::Median => median(&numbers),
                _ => stddev(&numbers),
            }))
        }
    }
}

/// How many different values there were, compared by the JSON they serialize
/// to. That is a coarser answer than a structural comparison for numbers —
/// `1` and `1.0` are two — but it is the comparison the rest of the pipeline
/// makes too, since this is all untyped JSON that arrived as text.
fn distinct(values: &[&Value]) -> usize {
    let mut seen = HashSet::new();
    for value in values {
        seen.insert(value.to_string());
    }
    seen.len()
}

/// `min` / `max` over values that are all numbers or all strings.
///
/// Strings are included because the useful case is a timestamp: an ISO one
/// compares as text exactly as it compares as a time, so `max` of a `ts` field
/// is the latest reading in the window. Mixed types have no ordering worth
/// guessing at, so they are an error rather than a silent choice.
fn extreme(function: ReduceFnKind, values: &[&Value]) -> anyhow::Result<Value> {
    let Some(first) = values.first() else {
        return Ok(Value::Null);
    };
    let wanted_larger = function == ReduceFnKind::Max;

    let better = |candidate: std::cmp::Ordering| {
        candidate == if wanted_larger { std::cmp::Ordering::Greater } else { std::cmp::Ordering::Less }
    };

    if first.is_number() {
        let numbers = numbers(values)?;
        let mut best = numbers[0];
        for number in &numbers[1..] {
            if better(number.total_cmp(&best)) {
                best = *number;
            }
        }
        return Ok(Value::from(best));
    }
    if let Some(first) = first.as_str() {
        let mut best = first;
        for value in &values[1..] {
            let text = value
                .as_str()
                .context("values are a mix of text and something else")?;
            if better(text.cmp(best)) {
                best = text;
            }
        }
        return Ok(Value::from(best));
    }
    bail!("min and max need numbers or text, not {first}")
}

/// Every value as a number, or an error naming the one that isn't.
fn numbers(values: &[&Value]) -> anyhow::Result<Vec<f64>> {
    values
        .iter()
        .map(|value| value.as_f64().with_context(|| format!("{value} is not a number")))
        .collect()
}

// a batch would have to hold 2^52 messages for this cast to lose anything; it
// can't, they're all in memory at once
#[allow(clippy::cast_precision_loss)]
fn mean(numbers: &[f64]) -> f64 {
    numbers.iter().sum::<f64>() / numbers.len() as f64
}

/// The middle value, or the mean of the two middle ones. Sorted with
/// `total_cmp` so a NaN that arrived as a number has a defined place rather
/// than making the sort itself misbehave.
fn median(numbers: &[f64]) -> f64 {
    let mut sorted = numbers.to_vec();
    sorted.sort_by(f64::total_cmp);
    let middle = sorted.len() / 2;
    if sorted.len().is_multiple_of(2) {
        f64::midpoint(sorted[middle - 1], sorted[middle])
    } else {
        sorted[middle]
    }
}

/// The *population* standard deviation, not the sample one: a window holds
/// every message that arrived in it, so it is the population.
fn stddev(numbers: &[f64]) -> f64 {
    let centre = mean(numbers);
    let squares: Vec<f64> = numbers.iter().map(|n| (n - centre).powi(2)).collect();
    mean(&squares).sqrt()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::batch;
    use serde_json::json;

    fn aggregation(function: ReduceFnKind, field: Option<&str>, output: &str) -> Aggregation {
        Aggregation {
            function,
            output: output.to_string(),
            field: field.map(ToString::to_string),
        }
    }

    /// Everything here goes through `build`, so the validation a config gets is
    /// the validation these tests get.
    fn transform(config: ReduceTransformConfig) -> anyhow::Result<Box<dyn Transform>> {
        let (events, _) = tokio::sync::broadcast::channel(16);
        let mut pipelines = std::collections::HashMap::new();
        let mut ctx = BuildCtx::new(&mut pipelines, "reducer-test".into(), events);
        config.build(&mut ctx)
    }

    fn config(aggregations: Vec<Aggregation>) -> ReduceTransformConfig {
        ReduceTransformConfig {
            aggregations,
            group_by: Vec::new(),
            on_missing: MissingFieldPolicy::Error,
        }
    }

    async fn run(
        config: ReduceTransformConfig,
        messages: Vec<Value>,
    ) -> anyhow::Result<Vec<Value>> {
        let out = transform(config)?.apply(batch(messages)).await?;
        Ok(out
            .iter()
            .flat_map(|b| b.iter().map(|m| (**m).clone()).collect::<Vec<_>>())
            .collect())
    }

    /// One value per function over the same four readings, so the arithmetic
    /// and the shape are both pinned in one place.
    #[tokio::test]
    async fn each_reduce_function_produces_the_expected_value() -> anyhow::Result<()> {
        let cases = [
            (ReduceFnKind::Sum, json!(10.0)),
            (ReduceFnKind::Avg, json!(2.5)),
            (ReduceFnKind::Min, json!(1.0)),
            (ReduceFnKind::Max, json!(4.0)),
            (ReduceFnKind::Count, json!(4)),
            (ReduceFnKind::CountDistinct, json!(4)),
            (ReduceFnKind::First, json!(1.0)),
            (ReduceFnKind::Last, json!(4.0)),
            (ReduceFnKind::Collect, json!([1.0, 2.0, 3.0, 4.0])),
            (ReduceFnKind::Median, json!(2.5)),
            (ReduceFnKind::Stddev, json!(1.118_033_988_749_895)),
        ];
        let messages: Vec<Value> = [1.0, 2.0, 3.0, 4.0]
            .iter()
            .map(|v| json!({"value": v}))
            .collect();
        for (function, expected) in cases {
            let out = run(
                config(vec![aggregation(function, Some("value"), "answer")]),
                messages.clone(),
            )
            .await?;
            assert_eq!(
                out,
                vec![json!({"answer": expected})],
                "unexpected result for {function:?}"
            );
        }
        Ok(())
    }

    /// The name in `as` is the whole point of the field: the answer is written
    /// under it, and nothing else is added alongside.
    #[tokio::test]
    async fn the_answer_is_written_under_the_name_that_was_asked_for() -> anyhow::Result<()> {
        let out = run(
            config(vec![aggregation(ReduceFnKind::Sum, Some("value"), "total")]),
            vec![json!({"value": 2}), json!({"value": 3})],
        )
        .await?;
        assert_eq!(out, vec![json!({"total": 5.0})]);
        Ok(())
    }

    /// Several answers about one batch in one message — which is the reason the
    /// output message is assembled rather than being one fixed shape. Chaining
    /// three reducers cannot do this: each one throws away the fields the next
    /// would need.
    #[tokio::test]
    async fn several_aggregations_land_in_one_message() -> anyhow::Result<()> {
        let out = run(
            config(vec![
                aggregation(ReduceFnKind::Sum, Some("value"), "total"),
                aggregation(ReduceFnKind::Count, None, "n"),
                aggregation(ReduceFnKind::Max, Some("ts"), "latest"),
            ]),
            vec![
                json!({"value": 2, "ts": "2026-01-01T00:00:00Z"}),
                json!({"value": 3, "ts": "2026-01-01T00:00:05Z"}),
            ],
        )
        .await?;
        assert_eq!(
            out,
            vec![json!({"total": 5.0, "n": 2, "latest": "2026-01-01T00:00:05Z"})]
        );
        Ok(())
    }

    /// A group per distinct key, carrying the key fields it was grouped by —
    /// without those the answers couldn't be told apart.
    #[tokio::test]
    async fn grouping_emits_one_message_per_key() -> anyhow::Result<()> {
        let mut config = config(vec![
            aggregation(ReduceFnKind::Sum, Some("value"), "total"),
            aggregation(ReduceFnKind::Count, None, "n"),
        ]);
        config.group_by = vec!["sensor".to_string()];
        let out = run(
            config,
            vec![
                json!({"sensor": "b", "value": 1}),
                json!({"sensor": "a", "value": 2}),
                json!({"sensor": "b", "value": 3}),
            ],
        )
        .await?;
        // first-seen order, not sorted: a reducer sits in a stream, and the
        // order the messages arrived in is the only order it has a claim to
        assert_eq!(
            out,
            vec![
                json!({"sensor": "b", "total": 4.0, "n": 2}),
                json!({"sensor": "a", "total": 2.0, "n": 1}),
            ]
        );
        Ok(())
    }

    /// Several grouping fields are one key, not nested groups.
    #[tokio::test]
    async fn grouping_by_two_fields_keys_on_the_combination() -> anyhow::Result<()> {
        let mut config = config(vec![aggregation(ReduceFnKind::Count, None, "n")]);
        config.group_by = vec!["region".to_string(), "sensor".to_string()];
        let out = run(
            config,
            vec![
                json!({"region": "eu", "sensor": "a"}),
                json!({"region": "us", "sensor": "a"}),
                json!({"region": "eu", "sensor": "a"}),
            ],
        )
        .await?;
        assert_eq!(
            out,
            vec![
                json!({"region": "eu", "sensor": "a", "n": 2}),
                json!({"region": "us", "sensor": "a", "n": 1}),
            ]
        );
        Ok(())
    }

    /// The default is the old behaviour, and it is the default for a reason: a
    /// sum over "whichever messages happened to have the field" is wrong in a
    /// way nothing downstream can see.
    #[tokio::test]
    async fn a_missing_field_is_an_error_by_default() -> anyhow::Result<()> {
        let res = run(
            config(vec![aggregation(ReduceFnKind::Sum, Some("value"), "total")]),
            vec![json!({"value": 1}), json!({"other": 2})],
        )
        .await;
        let Err(err) = res else {
            panic!("expected an error for a message missing the reduced field");
        };
        assert!(
            format!("{err:#}").contains("value"),
            "error should name the offending field: {err:#}"
        );
        Ok(())
    }

    /// `skip` is the other reading, and it has to be asked for. A field present
    /// but `null` is missing too — the same fact said two ways.
    #[tokio::test]
    async fn skip_leaves_the_message_out_of_that_aggregation() -> anyhow::Result<()> {
        let mut config = config(vec![
            aggregation(ReduceFnKind::Sum, Some("value"), "total"),
            aggregation(ReduceFnKind::Count, Some("value"), "readings"),
            aggregation(ReduceFnKind::Count, None, "messages"),
        ]);
        config.on_missing = MissingFieldPolicy::Skip;
        let out = run(
            config,
            vec![
                json!({"value": 1}),
                json!({"other": 2}),
                json!({"value": null}),
            ],
        )
        .await?;
        assert_eq!(out, vec![json!({"total": 1.0, "readings": 1, "messages": 3})]);
        Ok(())
    }

    /// A group every message was skipped out of has no average — and a 0 would
    /// be a reading nobody took. The counts are the exception: none is a real
    /// answer for "how many".
    #[tokio::test]
    async fn an_aggregation_with_no_values_left_reports_null() -> anyhow::Result<()> {
        let mut config = config(vec![
            aggregation(ReduceFnKind::Avg, Some("value"), "mean"),
            aggregation(ReduceFnKind::Count, Some("value"), "n"),
            aggregation(ReduceFnKind::Collect, Some("value"), "all"),
        ]);
        config.on_missing = MissingFieldPolicy::Skip;
        let out = run(config, vec![json!({"other": 1})]).await?;
        assert_eq!(out, vec![json!({"mean": null, "n": 0, "all": []})]);
        Ok(())
    }

    /// A message that can't be placed in a group can't be reduced at all, so
    /// `skip` drops it rather than inventing a null-keyed group.
    #[tokio::test]
    async fn a_message_missing_a_grouping_field_follows_the_same_policy()
    -> anyhow::Result<()> {
        let aggregations = vec![aggregation(ReduceFnKind::Count, None, "n")];
        let mut strict = config(aggregations.clone());
        strict.group_by = vec!["sensor".to_string()];
        let Err(err) = run(strict, vec![json!({"value": 1})]).await else {
            panic!("a message with no group can't be reduced");
        };
        assert!(format!("{err:#}").contains("sensor"), "got: {err:#}");

        let mut lenient = config(aggregations);
        lenient.group_by = vec!["sensor".to_string()];
        lenient.on_missing = MissingFieldPolicy::Skip;
        let out = run(
            lenient,
            vec![json!({"value": 1}), json!({"sensor": "a"})],
        )
        .await?;
        assert_eq!(out, vec![json!({"sensor": "a", "n": 1})]);
        Ok(())
    }

    /// Text compares alphabetically, which for an ISO timestamp is the same
    /// thing as comparing times — the case this exists for.
    #[tokio::test]
    async fn min_and_max_work_on_text_as_well_as_numbers() -> anyhow::Result<()> {
        let out = run(
            config(vec![
                aggregation(ReduceFnKind::Min, Some("ts"), "first_seen"),
                aggregation(ReduceFnKind::Max, Some("ts"), "last_seen"),
            ]),
            vec![
                json!({"ts": "2026-01-01T00:00:05Z"}),
                json!({"ts": "2026-01-01T00:00:01Z"}),
            ],
        )
        .await?;
        assert_eq!(
            out,
            vec![json!({
                "first_seen": "2026-01-01T00:00:01Z",
                "last_seen": "2026-01-01T00:00:05Z"
            })]
        );
        Ok(())
    }

    /// Values of two different kinds have no ordering worth guessing at.
    #[tokio::test]
    async fn min_over_mixed_types_is_an_error() -> anyhow::Result<()> {
        let res = run(
            config(vec![aggregation(ReduceFnKind::Min, Some("v"), "smallest")]),
            vec![json!({"v": "a"}), json!({"v": 1})],
        )
        .await;
        let Err(err) = res else {
            panic!("text and a number have no common order");
        };
        assert!(format!("{err:#}").contains("smallest"), "got: {err:#}");
        Ok(())
    }

    /// A sum over text is a config mistake, and the message has to say which
    /// aggregation it was so a reducer with five of them is debuggable.
    #[tokio::test]
    async fn a_non_numeric_value_names_the_aggregation_that_wanted_a_number()
    -> anyhow::Result<()> {
        let res = run(
            config(vec![aggregation(ReduceFnKind::Sum, Some("name"), "total")]),
            vec![json!({"name": "otter"})],
        )
        .await;
        let Err(err) = res else {
            panic!("'otter' cannot be summed");
        };
        let message = format!("{err:#}");
        assert!(message.contains("total"), "got: {message}");
        assert!(message.contains("otter"), "got: {message}");
        Ok(())
    }

    /// A tumbling window can close without ever receiving a message. Emitting
    /// nothing is correct; a 0 or a NaN would be invented data.
    #[tokio::test]
    async fn an_empty_batch_emits_nothing() -> anyhow::Result<()> {
        let out = transform(config(vec![aggregation(
            ReduceFnKind::Sum,
            Some("value"),
            "total",
        )]))?
        .apply(batch(vec![]))
        .await?;
        assert!(out.is_empty(), "expected no batches, got {out:?}");
        Ok(())
    }

    /// Every group's message goes out in one batch: they are one window's
    /// answer, and splitting them would make the fan-out downstream see a
    /// window arrive in pieces.
    #[tokio::test]
    async fn every_group_leaves_in_a_single_batch() -> anyhow::Result<()> {
        let mut config = config(vec![aggregation(ReduceFnKind::Count, None, "n")]);
        config.group_by = vec!["sensor".to_string()];
        let out = transform(config)?
            .apply(batch(vec![
                json!({"sensor": "a"}),
                json!({"sensor": "b"}),
            ]))
            .await?;
        assert_eq!(out.len(), 1, "one batch");
        assert_eq!(out[0].len(), 2, "two groups in it");
        Ok(())
    }

    /// These are the config mistakes that would otherwise be a strange message
    /// per batch rather than one refusal at the point the pipeline is created.
    #[test]
    fn a_reducer_that_could_not_work_refuses_to_build() {
        let cases = [
            ("at least one", config(Vec::new())),
            (
                "needs a 'field'",
                config(vec![aggregation(ReduceFnKind::Sum, None, "total")]),
            ),
            (
                "both called",
                config(vec![
                    aggregation(ReduceFnKind::Sum, Some("value"), "answer"),
                    aggregation(ReduceFnKind::Avg, Some("value"), "answer"),
                ]),
            ),
            (
                "needs an 'as'",
                config(vec![aggregation(ReduceFnKind::Sum, Some("value"), "  ")]),
            ),
        ];
        for (expected, config) in cases {
            let Err(err) = transform(config) else {
                panic!("expected a build error mentioning '{expected}'");
            };
            assert!(
                format!("{err:#}").contains(expected),
                "expected '{expected}', got: {err:#}"
            );
        }
    }

    /// An aggregation named after a grouping field would overwrite it, leaving
    /// a message whose key is not the key it was grouped under.
    #[test]
    fn an_aggregation_may_not_take_a_group_by_fields_name() {
        let mut config = config(vec![aggregation(
            ReduceFnKind::Count,
            None,
            "sensor",
        )]);
        config.group_by = vec!["sensor".to_string()];
        let Err(err) = transform(config) else {
            panic!("expected the collision to be refused");
        };
        assert!(format!("{err:#}").contains("sensor"), "got: {err:#}");
    }

    /// `count` is the one function with no field to point at, and its two
    /// readings are both useful: messages in the group, or messages that
    /// carried the field.
    #[test]
    fn count_is_the_one_function_that_builds_without_a_field() {
        assert!(
            transform(config(vec![aggregation(ReduceFnKind::Count, None, "n")])).is_ok(),
            "count with no field counts messages"
        );
    }

    /// A field is addressed the same way everywhere, and that includes a path
    /// into a nested object — which is what makes in-band metadata usable
    /// without the reducer knowing metadata exists.
    #[tokio::test]
    async fn a_nested_field_can_be_grouped_by_and_aggregated() -> anyhow::Result<()> {
        let mut config = config(vec![aggregation(
            ReduceFnKind::Avg,
            Some("reading.value"),
            "mean",
        )]);
        config.group_by = vec!["_meta.subject".to_string()];

        let out = run(
            config,
            vec![
                json!({ "reading": { "value": 1.0 }, "_meta": { "subject": "a" } }),
                json!({ "reading": { "value": 3.0 }, "_meta": { "subject": "a" } }),
                json!({ "reading": { "value": 8.0 }, "_meta": { "subject": "b" } }),
            ],
        )
        .await?;

        // one message per subject, in first-seen order, each carrying the group
        // field under its leaf name
        assert_eq!(
            out,
            vec![
                json!({ "subject": "a", "mean": 2.0 }),
                json!({ "subject": "b", "mean": 8.0 }),
            ]
        );
        Ok(())
    }

    /// Group fields come out under their leaf, so two paths sharing one leaf
    /// would silently land on the same field. Refused at build time, like every
    /// other collision here.
    #[test]
    fn two_group_by_paths_with_the_same_leaf_are_refused() {
        let mut config = config(vec![aggregation(ReduceFnKind::Count, None, "n")]);
        config.group_by = vec!["a.id".to_string(), "b.id".to_string()];
        let Err(err) = transform(config) else {
            panic!("expected the collision to be refused");
        };
        assert!(format!("{err:#}").contains("'id'"), "got: {err:#}");
    }

    /// The collision check between an aggregation and a group field is on the
    /// name that is actually written, not on the path that was configured.
    #[test]
    fn an_aggregation_may_not_overwrite_a_group_paths_leaf() {
        let mut config = config(vec![aggregation(ReduceFnKind::Count, None, "subject")]);
        config.group_by = vec!["_meta.subject".to_string()];
        assert!(
            transform(config).is_err(),
            "an aggregation called 'subject' would overwrite the group field"
        );
    }
}
