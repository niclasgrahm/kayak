//! The evaluation half of the `map` transform.
//!
//! [`kayak_core::mapping`] declares the mappings; this turns a list of them
//! into a [`MapTransform`] at build time and applies it to one message at a
//! time. The split is the one [`crate::outputs::columns`] makes, for the same
//! reason: what a mapping *is* has to compile for wasm so the form can render
//! it, and what a mapping *does* needs a clock and a JSON parser.
//!
//! Three properties are load-bearing.
//!
//! **Mappings apply in order, over one working message.** Each one reads what
//! the ones before it wrote, which is what makes an intermediate field work and
//! therefore what makes a two-step arithmetic expressible. There is
//! deliberately *no* build-time check that a mapping doesn't read a field a
//! later mapping writes: a message may perfectly well already carry that field
//! and be having it replaced afterwards, so the check would refuse working
//! configs, and a false refusal is worse than the warning it would have saved.
//!
//! **Absent and `null` are the same fact.** A mapping reading a field that is
//! explicitly `null` takes the `default`, or `on_missing`, exactly as one
//! reading a field that isn't there does — the same reading the reducer and the
//! column mapping already make, and the alternative is two spellings of missing
//! that behave differently in different transforms.
//!
//! **A value that is present and won't convert is an error, always**, whatever
//! `on_missing` says. `on_missing` is about a stream that is sparser than the
//! config expected; a `"twelve"` in a field cast to `float` is a stream that
//! isn't what the config says it is, and treating it as absent would let the
//! difference go unnoticed forever.

use anyhow::{Context, Result, anyhow, bail};
use kayak_core::mapping::{
    ArithmeticOperator, CastType, ConcatPart, KeepPolicy, MapMissingPolicy, MapTransformConfig,
    Mapping, Operand,
};
use serde_json::{Map, Value};
use std::collections::HashSet;
use std::sync::Arc;

use crate::{
    BuildCtx,
    fields,
    inputs::MessageBatch,
    transforms::{BuildTransform, Transform},
};

impl BuildTransform for MapTransformConfig {
    fn build(self, _ctx: &mut BuildCtx) -> Result<Box<dyn Transform>> {
        // Everything below is a config mistake that would otherwise be a
        // strange-looking message once per batch forever rather than an error:
        // the reducer's rule, applied to the other transform that assembles a
        // message rather than passing one on.
        if self.mappings.is_empty() {
            bail!("a map needs at least one mapping");
        }

        let mut targets: Vec<String> = Vec::new();
        let mut seen = HashSet::new();
        for (index, mapping) in self.mappings.iter().enumerate() {
            let position = format!("mapping {} ('{}')", index + 1, mapping.kind());
            check(mapping, &position, self.keep)?;

            if let Some(target) = mapping.target() {
                let target = target.trim();
                if target.is_empty() {
                    bail!("{position} needs an 'as' — it is the field the value is written to");
                }
                if !seen.insert(target.to_string()) {
                    bail!(
                        "{position} writes '{target}', which another mapping already writes — \
                         the first of the two would have no effect"
                    );
                }
                targets.push(target.to_string());
            }
        }

        Ok(Box::new(MapTransform {
            mappings: self.mappings,
            keep: self.keep,
            on_missing: self.on_missing,
            targets,
            warned_about_a_non_object: false,
        }))
    }
}

/// The per-mapping half of the build-time checks: what this kind of mapping
/// needs in order to mean anything at all.
fn check(mapping: &Mapping, position: &str, keep: KeepPolicy) -> Result<()> {
    let blank = |field: &str, what: &str| -> Result<()> {
        if field.trim().is_empty() {
            bail!("{position} has an empty '{what}'");
        }
        Ok(())
    };

    match mapping {
        Mapping::Copy { from, .. } | Mapping::Cast { from, .. } => blank(from, "from")?,
        Mapping::Constant { .. } => {}
        Mapping::Coalesce { from, .. } => {
            // one source is a copy, and spelling it as a coalesce hides that
            // the fallback everyone assumes is there isn't
            if from.len() < 2 {
                bail!(
                    "{position} coalesces {} field(s) — it needs at least two, \
                     or it is a 'copy'",
                    from.len()
                );
            }
            for field in from {
                blank(field, "from")?;
            }
        }
        Mapping::Concat { parts, .. } => {
            if parts.is_empty() {
                bail!("{position} has no parts to join");
            }
            for part in parts {
                if let ConcatPart::Field { field } = part {
                    blank(field, "field")?;
                }
            }
        }
        Mapping::Arithmetic {
            left,
            operator,
            right,
            ..
        } => {
            for (operand, side) in [(left, "left"), (right, "right")] {
                if let Operand::Field { field } = operand {
                    blank(field, side)?;
                }
            }
            // a literal zero divisor cannot ever be anything else, so it is a
            // config mistake rather than a bad message. A *field* that turns
            // out to be zero is the second of those and fails the batch.
            if *operator == ArithmeticOperator::Divide
                && matches!(right, Operand::Value { value } if *value == 0.0)
            {
                bail!("{position} divides by a literal zero");
            }
        }
        Mapping::Drop { from } => {
            if from.is_empty() {
                bail!("{position} names no fields to drop");
            }
            for field in from {
                blank(field, "from")?;
            }
            // under `keep: mapped` only the mapped fields come out anyway, so a
            // drop is either a no-op or a misunderstanding of what `mapped`
            // does. Both are worth saying out loud.
            if keep == KeepPolicy::Mapped {
                bail!(
                    "{position} is a 'drop', but 'keep: mapped' already leaves out \
                     everything no mapping writes"
                );
            }
        }
    }
    Ok(())
}

pub struct MapTransform {
    mappings: Vec<Mapping>,
    keep: KeepPolicy,
    on_missing: MapMissingPolicy,
    /// The fields a `keep: mapped` projection keeps, in the order they were
    /// written. Precomputed because it is the same list for every message.
    targets: Vec<String>,
    /// A message that isn't a JSON object is passed through with one warning,
    /// not one per message — that is a stream whose shape the config didn't
    /// expect, which is a fact worth saying once and worthless said a million
    /// times. Same rule the `remember`/`recall` transforms follow.
    warned_about_a_non_object: bool,
}

#[async_trait::async_trait]
impl Transform for MapTransform {
    async fn apply(&mut self, message_batch: Arc<MessageBatch>) -> Result<Vec<Arc<MessageBatch>>> {
        if message_batch.is_empty() {
            return Ok(vec![message_batch]);
        }

        let mut out = Vec::with_capacity(message_batch.len());
        for message in message_batch.iter() {
            if message.as_object().is_none() {
                // the same call the in-band envelope makes about a non-object
                // payload: there is nowhere to put a named field, so the
                // message goes on untouched rather than failing the pipeline.
                if !self.warned_about_a_non_object {
                    self.warned_about_a_non_object = true;
                    tracing::warn!(
                        "map: a message is not a JSON object, so there is nothing to map on it; \
                         passing it through unchanged (further ones are not reported)"
                    );
                }
                out.push(Arc::clone(message));
                continue;
            }
            out.push(Arc::new(self.map_message(message)?));
        }
        Ok(vec![Arc::new(out)])
    }
}

impl MapTransform {
    /// One message through every mapping, in order.
    fn map_message(&self, message: &Value) -> Result<Value> {
        let mut working = message.clone();
        for (index, mapping) in self.mappings.iter().enumerate() {
            self.apply_mapping(mapping, &mut working)
                .with_context(|| format!("mapping {} ('{}')", index + 1, mapping.kind()))?;
        }
        match self.keep {
            KeepPolicy::All => Ok(working),
            KeepPolicy::Mapped => self.project(&working),
        }
    }

    fn apply_mapping(&self, mapping: &Mapping, working: &mut Value) -> Result<()> {
        match mapping {
            Mapping::Drop { from } => {
                for field in from {
                    fields::remove(working, field);
                }
                return Ok(());
            }
            Mapping::Constant { value, output } => {
                return fields::set(working, output, value.to_value());
            }
            _ => {}
        }

        // every remaining mapping reads something, so all of them share the
        // same three-way answer: a value, a default, or whatever `on_missing`
        // says. Computing it here is what keeps that policy in one place.
        let (target, default, resolved) = match mapping {
            Mapping::Copy {
                from,
                output,
                default,
            } => (
                target_of(output.as_deref(), from),
                default.as_ref(),
                present(working, from).cloned(),
            ),
            Mapping::Cast {
                from,
                to,
                output,
                default,
            } => {
                let value = match present(working, from) {
                    // a present value that won't convert is an error whatever
                    // `on_missing` says — see the module docs
                    Some(value) => Some(cast(value, *to).with_context(|| {
                        format!("field '{from}' cannot be cast to {}", to.as_str())
                    })?),
                    None => None,
                };
                (target_of(output.as_deref(), from), default.as_ref(), value)
            }
            Mapping::Coalesce {
                from,
                output,
                default,
            } => (
                output.as_str(),
                default.as_ref(),
                from.iter().find_map(|field| present(working, field)).cloned(),
            ),
            Mapping::Concat { parts, output } => (
                output.as_str(),
                None,
                concat(working, parts)?.map(Value::String),
            ),
            Mapping::Arithmetic {
                left,
                operator,
                right,
                output,
            } => (
                output.as_str(),
                None,
                arithmetic(working, left, *operator, right)?,
            ),
            Mapping::Drop { .. } | Mapping::Constant { .. } => unreachable!("handled above"),
        };

        let value = match (resolved, default) {
            (Some(value), _) => value,
            (None, Some(default)) => default.to_value(),
            (None, None) => match self.on_missing {
                MapMissingPolicy::Error => bail!(
                    "the message does not carry the field(s) this reads. Give the mapping a \
                     'default', or set the map's 'on_missing' to 'omit' or 'null', to mean it"
                ),
                MapMissingPolicy::Omit => return Ok(()),
                MapMissingPolicy::Null => Value::Null,
            },
        };
        fields::set(working, target, value)
    }

    /// `keep: mapped`: a fresh message holding only what the mappings wrote.
    ///
    /// Read back out of the working message by the same paths they were written
    /// to — [`fields::get`] and [`fields::set`] prefer a literal key in the same
    /// order, so this round-trips whichever of the two shapes each write chose.
    /// A target that `on_missing: omit` left unwritten is simply not there.
    fn project(&self, working: &Value) -> Result<Value> {
        let mut projected = Value::Object(Map::new());
        for target in &self.targets {
            if let Some(value) = fields::get(working, target) {
                fields::set(&mut projected, target, value.clone())?;
            }
        }
        Ok(projected)
    }
}

/// The field a `copy` or a `cast` writes to: its `as`, or the last segment of
/// what it read.
///
/// The leaf reading is what makes promoting a nested value the short spelling —
/// `{"from": "_meta.subject"}` means `subject`, which is the name anything
/// downstream would want to spell.
fn target_of<'a>(output: Option<&'a str>, from: &'a str) -> &'a str {
    output.unwrap_or_else(|| fields::leaf(from))
}

/// A field's value, treating an explicit `null` as absent.
///
/// [`fields::get`] deliberately returns what is there and leaves this reading to
/// the caller; for `map` the two are the same fact, so every read goes through
/// here rather than through `get` directly.
fn present<'a>(message: &'a Value, field: &str) -> Option<&'a Value> {
    match fields::get(message, field) {
        Some(Value::Null) | None => None,
        Some(value) => Some(value),
    }
}

/// The pieces of a `concat`, joined. `None` if any field it reads is missing —
/// half a key is worse than no key.
fn concat(message: &Value, parts: &[ConcatPart]) -> Result<Option<String>> {
    let mut joined = String::new();
    for part in parts {
        match part {
            ConcatPart::Value { value } => joined.push_str(value),
            ConcatPart::Field { field } => {
                let Some(value) = present(message, field) else {
                    return Ok(None);
                };
                match value {
                    Value::String(text) => joined.push_str(text),
                    Value::Number(_) | Value::Bool(_) => joined.push_str(&value.to_string()),
                    // there is no one right way to flatten an object into a
                    // key, so this refuses rather than picking one
                    _ => bail!(
                        "field '{field}' holds {}, which cannot be part of a joined string",
                        describe(value)
                    ),
                }
            }
        }
    }
    Ok(Some(joined))
}

/// One arithmetic operation. `None` if either operand reads a field that is
/// missing.
fn arithmetic(
    message: &Value,
    left: &Operand,
    operator: ArithmeticOperator,
    right: &Operand,
) -> Result<Option<Value>> {
    let (Some(left), Some(right)) = (operand(message, left)?, operand(message, right)?) else {
        return Ok(None);
    };
    if operator == ArithmeticOperator::Divide && right == 0.0 {
        bail!("division by zero: the right-hand operand is 0");
    }
    let answer = match operator {
        ArithmeticOperator::Add => left + right,
        ArithmeticOperator::Subtract => left - right,
        ArithmeticOperator::Multiply => left * right,
        ArithmeticOperator::Divide => left / right,
    };
    let number = serde_json::Number::from_f64(answer).ok_or_else(|| {
        anyhow!("{left} {} {right} is not a number JSON can hold", operator.symbol())
    })?;
    Ok(Some(Value::Number(number)))
}

fn operand(message: &Value, operand: &Operand) -> Result<Option<f64>> {
    match operand {
        Operand::Value { value } => Ok(Some(*value)),
        Operand::Field { field } => match present(message, field) {
            None => Ok(None),
            Some(value) => value.as_f64().map(Some).ok_or_else(|| {
                anyhow!(
                    "field '{field}' holds {}, which cannot be used in arithmetic",
                    describe(value)
                )
            }),
        },
    }
}

/// One value converted to another JSON shape.
///
/// This is the only place in kayak that coerces rather than checks, and it is
/// still conservative about it: every conversion that could go two ways
/// (rounding a fraction, guessing at a truthy string) refuses instead.
fn cast(value: &Value, to: CastType) -> Result<Value> {
    let mismatch =
        || anyhow!("it holds {}, which is not a {}", describe(value), to.as_str());
    Ok(match to {
        CastType::Text => match value {
            Value::String(text) => Value::String(text.clone()),
            Value::Number(_) | Value::Bool(_) => Value::String(value.to_string()),
            _ => return Err(mismatch()),
        },
        CastType::Integer => Value::Number(integer(value)?.into()),
        CastType::Float => match value {
            // kept as it arrived, so a number's own digits survive — the same
            // care the column mapping takes
            Value::Number(_) => value.clone(),
            Value::String(text) => {
                let parsed: f64 = text.trim().parse().map_err(|_| mismatch())?;
                Value::Number(serde_json::Number::from_f64(parsed).ok_or_else(mismatch)?)
            }
            _ => return Err(mismatch()),
        },
        CastType::Boolean => match value {
            Value::Bool(flag) => Value::Bool(*flag),
            Value::String(text) if text.trim().eq_ignore_ascii_case("true") => Value::Bool(true),
            Value::String(text) if text.trim().eq_ignore_ascii_case("false") => Value::Bool(false),
            Value::Number(number) => match number.as_i64() {
                Some(1) => Value::Bool(true),
                Some(0) => Value::Bool(false),
                _ => return Err(mismatch()),
            },
            _ => return Err(mismatch()),
        },
        CastType::Timestamp => Value::String(timestamp(value)?.to_rfc3339()),
        CastType::Date => Value::String(timestamp(value)?.date_naive().to_string()),
        CastType::Uuid => match value {
            Value::String(text) if is_uuid(text.trim()) => {
                Value::String(text.trim().to_ascii_lowercase())
            }
            _ => return Err(mismatch()),
        },
        CastType::Json => match value {
            Value::String(text) => serde_json::from_str(text)
                .map_err(|error| anyhow!("it is a string that is not JSON: {error}"))?,
            // deliberately not a pass-through: this cast exists for a payload
            // that arrived encoded inside another one, and accepting anything
            // would make a config that names the wrong field succeed quietly
            _ => return Err(anyhow!(
                "it holds {}, and a cast to json parses a *string* containing JSON",
                describe(value)
            )),
        },
    })
}

fn integer(value: &Value) -> Result<i64> {
    let mismatch = || anyhow!("it holds {}, which is not an integer", describe(value));
    match value {
        Value::Number(number) => {
            if let Some(whole) = number.as_i64() {
                return Ok(whole);
            }
            // a JSON `12.0` is an integer written with a point; a `12.5` is a
            // number this cast has no unambiguous answer for, so it refuses
            // rather than rounding in a direction nothing asked for
            let float = number.as_f64().ok_or_else(mismatch)?;
            if float.fract() != 0.0 {
                bail!("it has a fractional part, and rounding is not something the config said");
            }
            if !(float >= -(2f64.powi(63)) && float < 2f64.powi(63)) {
                bail!("it is outside the range an integer can hold");
            }
            #[allow(
                clippy::cast_possible_truncation,
                reason = "range- and fraction-checked immediately above"
            )]
            Ok(float as i64)
        }
        Value::String(text) => text.trim().parse().map_err(|_| mismatch()),
        _ => Err(mismatch()),
    }
}

/// A value read as a point in time: an RFC 3339 string, or a number of seconds
/// since the epoch.
///
/// Seconds and not milliseconds for the reason the column mapping gives — a
/// JSON number carries no unit, one of the two has to be picked, and seconds is
/// what `to_timestamp` means everywhere else.
fn timestamp(value: &Value) -> Result<chrono::DateTime<chrono::FixedOffset>> {
    let out_of_range = || anyhow!("{value} is not a time");
    match value {
        Value::String(text) => {
            let text = text.trim();
            if let Ok(parsed) = chrono::DateTime::parse_from_rfc3339(text) {
                return Ok(parsed);
            }
            // a plain calendar date is the other spelling a source uses, and
            // reading it as midnight UTC is the only reading available
            let date = chrono::NaiveDate::parse_from_str(text, "%Y-%m-%d")
                .map_err(|_| anyhow!("'{text}' is not an RFC 3339 time or a YYYY-MM-DD date"))?;
            Ok(date
                .and_hms_opt(0, 0, 0)
                .ok_or_else(out_of_range)?
                .and_utc()
                .fixed_offset())
        }
        Value::Number(_) => {
            let seconds = value.as_f64().ok_or_else(out_of_range)?;
            let micros = (seconds * 1_000_000.0).round();
            if !micros.is_finite() || micros.abs() > 9.0e15 {
                return Err(out_of_range());
            }
            #[allow(
                clippy::cast_possible_truncation,
                reason = "range-checked immediately above"
            )]
            let micros = micros as i64;
            Ok(chrono::DateTime::from_timestamp_micros(micros)
                .ok_or_else(out_of_range)?
                .fixed_offset())
        }
        _ => Err(anyhow!("it holds {}, which is not a time", describe(value))),
    }
}

/// The canonical hyphenated form and nothing else. Deliberately hand-rolled
/// rather than pulling in a uuid crate for one predicate: nothing here needs to
/// take a uuid apart, only to refuse something that isn't one.
fn is_uuid(text: &str) -> bool {
    let groups = [8, 4, 4, 4, 12];
    let mut parts = text.split('-');
    for length in groups {
        match parts.next() {
            Some(part)
                if part.len() == length && part.chars().all(|c| c.is_ascii_hexdigit()) => {}
            _ => return false,
        }
    }
    parts.next().is_none()
}

/// What a value is, for an error that has to say why it wasn't accepted.
fn describe(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "a boolean",
        Value::Number(_) => "a number",
        Value::String(_) => "a string",
        Value::Array(_) => "an array",
        Value::Object(_) => "an object",
    }
}

#[cfg(test)]
mod tests {
    use super::MapTransform;
    use crate::BuildCtx;
    use crate::transforms::{BuildTransform, Transform};
    use kayak_core::mapping::{
        ArithmeticOperator, CastType, ConcatPart, KeepPolicy, Literal, MapMissingPolicy,
        MapTransformConfig, Mapping, Operand,
    };
    use serde_json::{Value, json};
    use std::sync::Arc;

    fn config(mappings: Vec<Mapping>) -> MapTransformConfig {
        MapTransformConfig {
            mappings,
            keep: KeepPolicy::All,
            on_missing: MapMissingPolicy::Error,
        }
    }

    /// A build with nothing on the context — `map` reaches for no connection,
    /// no bucket and no secret, which is most of why it is cheap.
    fn build(config: MapTransformConfig) -> anyhow::Result<Box<dyn Transform>> {
        let (events, _) = tokio::sync::broadcast::channel(16);
        let mut pipelines = std::collections::HashMap::new();
        let mut ctx = BuildCtx::new(&mut pipelines, "map-test".into(), events);
        config.build(&mut ctx)
    }

    /// Build and run one message through, which is what nearly every test here
    /// wants; `map` never changes the number of messages, so one in is one out.
    ///
    /// Sync, with the runtime made here, because `apply` never actually awaits
    /// anything — `map` touches no network — and a table-driven test reads far
    /// better without an `.await` on every row.
    fn run(config: MapTransformConfig, message: Value) -> anyhow::Result<Value> {
        let mut transform = build(config)?;
        let batches = tokio::runtime::Builder::new_current_thread()
            .build()?
            .block_on(transform.apply(Arc::new(vec![Arc::new(message)])))?;
        let [batch] = batches.as_slice() else {
            panic!("expected exactly one batch out, got {}", batches.len());
        };
        let [message] = batch.as_slice() else {
            panic!("expected exactly one message out, got {}", batch.len());
        };
        Ok(message.as_ref().clone())
    }

    fn copy(from: &str, output: Option<&str>) -> Mapping {
        Mapping::Copy {
            from: from.into(),
            output: output.map(Into::into),
            default: None,
        }
    }

    #[test]
    fn a_copy_renames_a_field_and_leaves_the_rest_alone() -> anyhow::Result<()> {
        let out = run(
            config(vec![copy("temp", Some("temperature"))]),
            json!({ "temp": 21.5, "sensor": "a" }),
        )?;
        assert_eq!(out, json!({ "temp": 21.5, "sensor": "a", "temperature": 21.5 }));
        Ok(())
    }

    /// The case in-band metadata created: a `group_by` can reach `_meta.subject`
    /// but nothing downstream should have to spell the upstream's shape.
    #[test]
    fn a_copy_promotes_a_nested_value_under_its_leaf_name() -> anyhow::Result<()> {
        let out = run(
            config(vec![copy("_meta.subject", None)]),
            json!({ "_meta": { "subject": "m1.temperature" }, "value": 1 }),
        )?;
        assert_eq!(out["subject"], json!("m1.temperature"));
        Ok(())
    }

    #[test]
    fn a_dotted_as_writes_into_a_nested_object() -> anyhow::Result<()> {
        let out = run(
            config(vec![copy("id", Some("sensor.id"))]),
            json!({ "id": "s1" }),
        )?;
        assert_eq!(out["sensor"]["id"], json!("s1"));
        Ok(())
    }

    /// The whole reason mappings are an ordered list: the second one reads what
    /// the first wrote, and a `drop` sweeps the intermediate away.
    #[test]
    fn mappings_apply_in_order_so_arithmetic_can_be_chained() -> anyhow::Result<()> {
        let out = run(
            config(vec![
                Mapping::Arithmetic {
                    left: Operand::Field {
                        field: "fahrenheit".into(),
                    },
                    operator: ArithmeticOperator::Subtract,
                    right: Operand::Value { value: 32.0 },
                    output: "_offset".into(),
                },
                Mapping::Arithmetic {
                    left: Operand::Field {
                        field: "_offset".into(),
                    },
                    operator: ArithmeticOperator::Divide,
                    right: Operand::Value { value: 1.8 },
                    output: "celsius".into(),
                },
                Mapping::Drop {
                    from: vec!["_offset".into()],
                },
            ]),
            json!({ "fahrenheit": 212.0 }),
        )?;
        assert_eq!(out["celsius"], json!(100.0));
        assert!(out.get("_offset").is_none(), "the intermediate is gone");
        Ok(())
    }

    #[test]
    fn a_coalesce_takes_the_first_field_that_is_there() -> anyhow::Result<()> {
        let mapping = || Mapping::Coalesce {
            from: vec!["temp_c".into(), "readings.celsius".into()],
            output: "celsius".into(),
            default: None,
        };
        let first = run(config(vec![mapping()]), json!({ "temp_c": 4 }))?;
        assert_eq!(first["celsius"], json!(4));

        let second = run(
            config(vec![mapping()]),
            json!({ "readings": { "celsius": 9 } }),
        )?;
        assert_eq!(second["celsius"], json!(9));
        Ok(())
    }

    /// The same fact said two ways, and the reading `reduce` and the column
    /// mapping already make.
    #[test]
    fn an_explicit_null_counts_as_missing() -> anyhow::Result<()> {
        let out = run(
            config(vec![Mapping::Copy {
                from: "a".into(),
                output: Some("b".into()),
                default: Some(Literal::Number { value: 0.0 }),
            }]),
            json!({ "a": null }),
        )?;
        assert_eq!(out["b"], json!(0.0));
        Ok(())
    }

    #[test]
    fn a_missing_field_is_an_error_by_default() {
        let Err(error) = run(config(vec![copy("nope", Some("b"))]), json!({ "a": 1 })) else {
            panic!("a missing field should be refused");
        };
        assert!(error.to_string().contains("mapping 1"), "{error}");
    }

    #[test]
    fn on_missing_omit_leaves_the_target_unwritten() -> anyhow::Result<()> {
        let mut config = config(vec![copy("nope", Some("b"))]);
        config.on_missing = MapMissingPolicy::Omit;
        let out = run(config, json!({ "a": 1 }))?;
        assert_eq!(out, json!({ "a": 1 }));
        Ok(())
    }

    #[test]
    fn on_missing_null_writes_the_target_as_null() -> anyhow::Result<()> {
        let mut config = config(vec![copy("nope", Some("b"))]);
        config.on_missing = MapMissingPolicy::Null;
        let out = run(config, json!({ "a": 1 }))?;
        assert_eq!(out, json!({ "a": 1, "b": null }));
        Ok(())
    }

    /// A `default` answers before `on_missing` does, which is what makes it the
    /// way to say that *this one* field is expected to be absent without
    /// loosening the policy for every other mapping.
    #[test]
    fn a_default_wins_over_the_policy() -> anyhow::Result<()> {
        let out = run(
            config(vec![Mapping::Copy {
                from: "nope".into(),
                output: Some("b".into()),
                default: Some(Literal::Text {
                    value: "unknown".into(),
                }),
            }]),
            json!({ "a": 1 }),
        )?;
        assert_eq!(out["b"], json!("unknown"));
        Ok(())
    }

    #[test]
    fn keep_mapped_projects_down_to_what_was_written() -> anyhow::Result<()> {
        let mut config = config(vec![
            copy("_meta.subject", None),
            Mapping::Constant {
                value: Literal::Text {
                    value: "line-3".into(),
                },
                output: "line".into(),
            },
        ]);
        config.keep = KeepPolicy::Mapped;
        let out = run(
            config,
            json!({ "_meta": { "subject": "m1" }, "value": 1, "noise": true }),
        )?;
        assert_eq!(out, json!({ "subject": "m1", "line": "line-3" }));
        Ok(())
    }

    /// `keep: mapped` writes and reads a nested target by the same path rule,
    /// so a projection can build a shape rather than only a flat row.
    #[test]
    fn keep_mapped_round_trips_a_nested_target() -> anyhow::Result<()> {
        let mut config = config(vec![copy("id", Some("sensor.id"))]);
        config.keep = KeepPolicy::Mapped;
        let out = run(config, json!({ "id": "s1", "noise": 1 }))?;
        assert_eq!(out, json!({ "sensor": { "id": "s1" } }));
        Ok(())
    }

    #[test]
    fn a_concat_joins_fields_and_literal_text() -> anyhow::Result<()> {
        let out = run(
            config(vec![Mapping::Concat {
                parts: vec![
                    ConcatPart::Field {
                        field: "site".into(),
                    },
                    ConcatPart::Value { value: "/".into() },
                    ConcatPart::Field {
                        field: "machine".into(),
                    },
                ],
                output: "asset".into(),
            }]),
            json!({ "site": "oslo", "machine": 3 }),
        )?;
        assert_eq!(out["asset"], json!("oslo/3"));
        Ok(())
    }

    #[test]
    fn casts_convert_the_shapes_they_claim_to() -> anyhow::Result<()> {
        let cases = [
            (CastType::Float, json!("12.5"), json!(12.5)),
            (CastType::Integer, json!("12"), json!(12)),
            (CastType::Integer, json!(12.0), json!(12)),
            (CastType::Text, json!(3), json!("3")),
            (CastType::Boolean, json!("TRUE"), json!(true)),
            (CastType::Boolean, json!(0), json!(false)),
            (
                CastType::Json,
                json!("{\"a\":1}"),
                json!({ "a": 1 }),
            ),
            (
                CastType::Uuid,
                json!("A1B2C3D4-1111-2222-3333-444455556666"),
                json!("a1b2c3d4-1111-2222-3333-444455556666"),
            ),
            (
                CastType::Timestamp,
                json!(1_754_000_000),
                json!("2025-07-31T22:13:20+00:00"),
            ),
            (CastType::Date, json!("2026-08-10T11:22:33Z"), json!("2026-08-10")),
        ];
        for (to, input, expected) in cases {
            let out = run(
                config(vec![Mapping::Cast {
                    from: "a".into(),
                    to,
                    output: None,
                    default: None,
                }]),
                json!({ "a": input }),
            )?;
            assert_eq!(out["a"], expected, "casting to {}", to.as_str());
        }
        Ok(())
    }

    /// The module's third rule: `on_missing` is about a sparse stream, not
    /// about one carrying the wrong thing, so a present value that won't
    /// convert fails however lenient the policy is.
    #[test]
    fn a_value_that_will_not_convert_is_an_error_whatever_on_missing_says() {
        for policy in [
            MapMissingPolicy::Error,
            MapMissingPolicy::Omit,
            MapMissingPolicy::Null,
        ] {
            let mut config = config(vec![Mapping::Cast {
                from: "a".into(),
                to: CastType::Float,
                output: None,
                default: Some(Literal::Number { value: 0.0 }),
            }]);
            config.on_missing = policy;
            let Err(error) = run(config, json!({ "a": "twelve" })) else {
                panic!("a value that will not convert should be refused under {policy:?}");
            };
            assert!(error.to_string().contains("mapping 1"), "{error}");
        }
    }

    #[test]
    fn an_integer_cast_refuses_to_round() {
        let Err(error) = run(
            config(vec![Mapping::Cast {
                from: "a".into(),
                to: CastType::Integer,
                output: None,
                default: None,
            }]),
            json!({ "a": 12.5 }),
        ) else {
            panic!("rounding should be refused rather than guessed at");
        };
        // `{:#}` rather than `{}`: the mapping's position is the outermost
        // context, and the reason it refused is further down the chain
        assert!(format!("{error:#}").contains("fractional"), "{error:#}");
    }

    /// A message with no fields to map goes on untouched rather than failing
    /// the pipeline — the same call the in-band envelope makes.
    #[test]
    fn a_non_object_message_is_passed_through() -> anyhow::Result<()> {
        let out = run(config(vec![copy("a", Some("b"))]), json!([1, 2, 3]))?;
        assert_eq!(out, json!([1, 2, 3]));
        Ok(())
    }

    #[test]
    fn contradictory_mappings_are_refused_at_build_time() {
        let cases: Vec<(&str, MapTransformConfig)> = vec![
            ("no mappings", config(vec![])),
            (
                "two mappings writing one field",
                config(vec![copy("a", Some("x")), copy("b", Some("x"))]),
            ),
            ("a blank as", config(vec![copy("a", Some("  "))])),
            ("a blank from", config(vec![copy("", Some("x"))])),
            (
                "a coalesce over one field",
                config(vec![Mapping::Coalesce {
                    from: vec!["a".into()],
                    output: "x".into(),
                    default: None,
                }]),
            ),
            (
                "a concat with no parts",
                config(vec![Mapping::Concat {
                    parts: vec![],
                    output: "x".into(),
                }]),
            ),
            (
                "a drop with no fields",
                config(vec![Mapping::Drop { from: vec![] }]),
            ),
            (
                "division by a literal zero",
                config(vec![Mapping::Arithmetic {
                    left: Operand::Field { field: "a".into() },
                    operator: ArithmeticOperator::Divide,
                    right: Operand::Value { value: 0.0 },
                    output: "x".into(),
                }]),
            ),
        ];
        for (what, config) in cases {
            assert!(build(config).is_err(), "{what} should be refused");
        }
    }

    /// `keep: mapped` already leaves out everything no mapping writes, so a
    /// `drop` beside it is a misunderstanding worth saying out loud.
    #[test]
    fn a_drop_is_refused_under_keep_mapped() {
        let mut config = config(vec![
            copy("a", Some("x")),
            Mapping::Drop {
                from: vec!["b".into()],
            },
        ]);
        config.keep = KeepPolicy::Mapped;
        assert!(build(config).is_err());
    }

    /// A transform is `Send` because the run loop owns it across an await; this
    /// is the compile-time check that the map one stays that way.
    #[allow(dead_code, reason = "a compile-time assertion, never called")]
    fn map_transform_is_send(transform: MapTransform) -> impl Send {
        transform
    }
}
