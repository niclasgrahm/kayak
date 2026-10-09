//! The numeric and time builtins a script can call.
//!
//! Every function here is a thin adapter: an array of rhai values becomes a
//! `Vec<f64>`, [`crate::stats`] does the arithmetic, and the answer goes back
//! as a rhai value. Nothing numeric is decided in this file, which is what
//! keeps one test per function in `stats` standing for the script's version
//! too — what is tested here is the *adapting*: what a non-number does, what
//! an undefined answer looks like, and that every function is one the
//! declaration in [`kayak_core::script::builtins`] knows about.
//!
//! Three rules:
//!
//! - **`pluck` is the bridge, and it skips.** `pluck(batch, "value")` is the
//!   values of that field across the batch, *leaving out* the messages that
//!   don't carry it (or carry `null`) — the `skip` half of the reducer's
//!   `on_missing`, because a script is already the place someone reaches for
//!   when the strict version got in the way. A script wanting `error` checks
//!   `pluck(...).len() == batch.len()`.
//! - **Undefined is `()`, never NaN.** The mean of nothing, the slope of one
//!   point, the z-scores of a flat series. `stats` returns `None` for those and
//!   this turns it into the unit, so `if m == ()` is the warm-up check, the
//!   same one `recall` gets.
//! - **A non-number in the array is an error, not a skip.** `pluck` already
//!   dropped the *absent* values; a `"twelve"` that is present is a stream
//!   that isn't what the script claims, and the same rule the `map` transform
//!   follows applies — `on_missing` is about a sparse stream, not a wrong one.
//!
//! Results are floats throughout, even where the input was whole numbers: a
//! `sum` of integers coming back as an integer and a `sum` with one float in
//! it coming back as a float would make a script's arithmetic change type on
//! the data's say-so. The exceptions are counts and positions — `histogram`'s
//! `counts`, `peaks` — which are integers because they are.

use rhai::{Array, Dynamic, Engine, EvalAltResult, FLOAT, INT, Map};
use serde_json::Value;

use super::value::{from_dynamic, to_dynamic};
use crate::stats;

type Fallible<T> = Result<T, Box<EvalAltResult>>;

#[allow(clippy::unnecessary_box_returns, reason = "the box is the type rhai's host functions fail with")]
fn fail(message: impl Into<String>) -> Box<EvalAltResult> {
    Box::new(EvalAltResult::from(message.into()))
}

/// An array of rhai values as numbers, or the error naming the first that
/// isn't one.
fn numbers(array: &Array) -> Fallible<Vec<f64>> {
    array
        .iter()
        .enumerate()
        .map(|(i, value)| {
            if let Ok(f) = value.as_float() {
                return Ok(f);
            }
            #[allow(clippy::cast_precision_loss, reason = "an i64 read as a measurement")]
            if let Ok(n) = value.as_int() {
                return Ok(n as f64);
            }
            Err(fail(format!(
                "element {i} is {}, not a number — pluck() leaves out missing values, but a \
                 present value that isn't a number is an error",
                value.type_name()
            )))
        })
        .collect()
}

fn floats(numbers: Vec<f64>) -> Dynamic {
    Dynamic::from_array(numbers.into_iter().map(Dynamic::from_float).collect())
}

fn maybe(answer: Option<f64>) -> Dynamic {
    answer.map_or(Dynamic::UNIT, Dynamic::from_float)
}

fn maybe_floats(answer: Option<Vec<f64>>) -> Dynamic {
    answer.map_or(Dynamic::UNIT, floats)
}

/// A count or a position as a rhai integer — a batch cannot hold more than
/// `i64::MAX` messages.
#[allow(clippy::cast_possible_wrap)]
fn int(n: usize) -> INT {
    n as INT
}

/// A `#{slope, intercept, r2}` map, or `()`.
fn fit(fit: Option<stats::Fit>) -> Dynamic {
    let Some(fit) = fit else {
        return Dynamic::UNIT;
    };
    let mut map = Map::new();
    map.insert("slope".into(), Dynamic::from_float(fit.slope));
    map.insert("intercept".into(), Dynamic::from_float(fit.intercept));
    map.insert("r2".into(), Dynamic::from_float(fit.r2));
    Dynamic::from_map(map)
}

/// Register every numeric builtin. Called once, from the runner's
/// `build_engine`; the declaration each name has to appear in is
/// [`kayak_core::script::builtins`], and the runner's test says so.
#[allow(clippy::too_many_lines, reason = "one registration per builtin, in the declaration's order")]
pub fn register(engine: &mut Engine) {
    // ── the bridge ──────────────────────────────────────────────────────────
    // Bound to `fields::get`, as `field()` is, so a path here means what it
    // means everywhere — including the rule that an exact key beats a path.
    engine.register_fn("pluck", |batch: Array, path: &str| -> Array {
        batch
            .iter()
            .filter_map(|message| {
                let value = from_dynamic(message).ok()?;
                match crate::fields::get(&value, path) {
                    Some(Value::Null) | None => None,
                    Some(found) => Some(to_dynamic(found)),
                }
            })
            .collect()
    });

    // ── one number out ──────────────────────────────────────────────────────
    engine.register_fn("sum", |a: Array| -> Fallible<FLOAT> { Ok(stats::sum(&numbers(&a)?)) });
    engine.register_fn("mean", |a: Array| -> Fallible<Dynamic> { Ok(maybe(stats::mean(&numbers(&a)?))) });
    engine.register_fn("median", |a: Array| -> Fallible<Dynamic> {
        Ok(maybe(stats::median(&numbers(&a)?)))
    });
    engine.register_fn("min", |a: Array| -> Fallible<Dynamic> { Ok(maybe(stats::min(&numbers(&a)?))) });
    engine.register_fn("max", |a: Array| -> Fallible<Dynamic> { Ok(maybe(stats::max(&numbers(&a)?))) });
    engine.register_fn("std", |a: Array| -> Fallible<Dynamic> { Ok(maybe(stats::stddev(&numbers(&a)?))) });
    engine.register_fn("variance", |a: Array| -> Fallible<Dynamic> { Ok(maybe(stats::variance(&numbers(&a)?))) });
    engine.register_fn("quantile", |a: Array, q: FLOAT| -> Fallible<Dynamic> {
        Ok(maybe(stats::quantile(&numbers(&a)?, q)))
    });
    engine.register_fn("mad", |a: Array| -> Fallible<Dynamic> { Ok(maybe(stats::mad(&numbers(&a)?))) });
    engine.register_fn("skew", |a: Array| -> Fallible<Dynamic> { Ok(maybe(stats::skew(&numbers(&a)?))) });
    engine.register_fn("kurtosis", |a: Array| -> Fallible<Dynamic> {
        Ok(maybe(stats::kurtosis(&numbers(&a)?)))
    });
    engine.register_fn("rms", |a: Array| -> Fallible<Dynamic> { Ok(maybe(stats::rms(&numbers(&a)?))) });
    engine.register_fn("autocorr", |a: Array, lag: INT| -> Fallible<Dynamic> {
        let lag = usize::try_from(lag).map_err(|_| fail("the lag has to be a positive number"))?;
        Ok(maybe(stats::autocorr(&numbers(&a)?, lag)))
    });

    // ── an array out ────────────────────────────────────────────────────────
    engine.register_fn("zscore", |a: Array| -> Fallible<Dynamic> {
        Ok(maybe_floats(stats::zscore(&numbers(&a)?)))
    });
    engine.register_fn("diff", |a: Array| -> Fallible<Dynamic> { Ok(floats(stats::diff(&numbers(&a)?))) });
    engine.register_fn("cumsum", |a: Array| -> Fallible<Dynamic> {
        Ok(floats(stats::cumsum(&numbers(&a)?)))
    });
    engine.register_fn("ewma", |a: Array, alpha: FLOAT| -> Fallible<Dynamic> {
        Ok(maybe_floats(stats::ewma(&numbers(&a)?, alpha)))
    });
    engine.register_fn("peaks", |a: Array| -> Fallible<Dynamic> {
        let found = stats::peaks(&numbers(&a)?);
        Ok(Dynamic::from_array(
            found.into_iter().map(|i| Dynamic::from_int(int(i))).collect(),
        ))
    });

    // ── a map out ───────────────────────────────────────────────────────────
    engine.register_fn("linfit", |ys: Array| -> Fallible<Dynamic> {
        Ok(fit(stats::linfit_indexed(&numbers(&ys)?)))
    });
    engine.register_fn("linfit", |xs: Array, ys: Array| -> Fallible<Dynamic> {
        Ok(fit(stats::linfit(&numbers(&xs)?, &numbers(&ys)?)))
    });
    engine.register_fn("histogram", |a: Array, bins: INT| -> Fallible<Dynamic> {
        let bins = usize::try_from(bins).map_err(|_| fail("bins has to be a positive number"))?;
        let Some(histogram) = stats::histogram(&numbers(&a)?, bins) else {
            return Ok(Dynamic::UNIT);
        };
        let mut map = Map::new();
        map.insert("edges".into(), floats(histogram.edges));
        map.insert(
            "counts".into(),
            Dynamic::from_array(
                histogram
                    .counts
                    .into_iter()
                    .map(|c| Dynamic::from_int(int(c)))
                    .collect(),
            ),
        );
        Ok(Dynamic::from_map(map))
    });

    // ── scalars ─────────────────────────────────────────────────────────────
    engine.register_fn("clamp", |x: FLOAT, low: FLOAT, high: FLOAT| -> FLOAT {
        stats::clamp(x, low, high)
    });
    engine.register_fn("clamp", |x: INT, low: INT, high: INT| -> INT {
        if low > high { x } else { x.max(low).min(high) }
    });
    engine.register_fn("interp", |xs: Array, ys: Array, x: FLOAT| -> Fallible<Dynamic> {
        Ok(maybe(stats::interp(&numbers(&xs)?, &numbers(&ys)?, x)))
    });
    engine.register_fn("dtw", |a: Array, b: Array| -> Fallible<Dynamic> {
        Ok(maybe(stats::dtw(&numbers(&a)?, &numbers(&b)?)))
    });

    // ── time ────────────────────────────────────────────────────────────────
    // The one rule for reading a time is `crate::time`'s, and these are it
    // spelled for a script: a string or a number in, milliseconds out. A `()`
    // passes through as `()`, so a missing field stays missing rather than
    // failing the message — the same distinction the transforms draw between
    // absent and wrong.
    engine.register_fn("parse_time", |value: Dynamic| -> Fallible<Dynamic> {
        if value.is_unit() {
            return Ok(Dynamic::UNIT);
        }
        let value = from_dynamic(&value).map_err(|err| fail(err.to_string()))?;
        crate::time::parse_millis(&value)
            .map(Dynamic::from_int)
            .map_err(|err| fail(format!("parse_time: {err}")))
    });
    engine.register_fn("format_time", |millis: INT| -> Fallible<String> {
        crate::time::format(millis).map_err(|err| fail(format!("format_time: {err}")))
    });
}

#[cfg(test)]
mod tests {
    use super::super::error::ScriptError;
    use super::super::runner::{Bindings, ScriptRunner};
    use kayak_core::script::ScriptScope;
    use serde_json::{Value, json};
    use std::sync::Arc;

    /// One `batch`-scoped run over `batch`, returning what the script's last
    /// expression was — the arrays go in as messages, the answer comes out as
    /// the single message of the single batch.
    fn eval(code: &str, batch: &[Value]) -> Result<Value, ScriptError> {
        let script = format!("let out = {{ {code} }}; emit([#{{ out: out }}]);");
        let mut runner =
            ScriptRunner::compile(&script, ScriptScope::Batch, None, Bindings::default(), None)?;
        let batch: Vec<Arc<Value>> = batch.iter().cloned().map(Arc::new).collect();
        let out = runner.run(&batch)?;
        Ok(out
            .first()
            .and_then(|b| b.first())
            .and_then(|m| m.get("out").cloned())
            .unwrap_or(Value::Null))
    }

    fn readings(values: &[Value]) -> Vec<Value> {
        values.iter().map(|v| json!({"value": v})).collect()
    }

    #[test]
    fn pluck_bridges_a_batch_to_an_array_and_skips_what_is_missing() -> Result<(), ScriptError> {
        let batch = vec![
            json!({"value": 1}),
            json!({"other": 2}),
            json!({"value": null}),
            json!({"value": 2.5}),
            json!({"sensor": {"value": 9}}),
        ];
        assert_eq!(eval(r#"pluck(batch, "value")"#, &batch)?, json!([1, 2.5]));
        assert_eq!(eval(r#"pluck(batch, "sensor.value")"#, &batch)?, json!([9]));
        assert_eq!(eval(r#"pluck(batch, "nothing")"#, &batch)?, json!([]));
        Ok(())
    }

    /// The four-line cycle-features script the roadmap promised.
    #[test]
    fn the_cycle_features_script() -> Result<(), ScriptError> {
        let batch = readings(&[json!(1), json!(3), json!(5), json!(7)]);
        let out = eval(
            r#"
            let t = pluck(batch, "value");
            let fit = linfit(t);
            #{ mean: mean(t), std: std(t), slope: fit.slope, r2: fit.r2, n: t.len() }
            "#,
            &batch,
        )?;
        assert_eq!(out, json!({"mean": 4.0, "std": 5.0_f64.sqrt(), "slope": 2.0, "r2": 1.0, "n": 4}));
        Ok(())
    }

    #[test]
    fn every_summary_function_answers_over_an_array() -> Result<(), ScriptError> {
        let batch = readings(&[json!(2), json!(4), json!(4), json!(4), json!(5), json!(5), json!(7), json!(9)]);
        let out = eval(
            r#"
            let v = pluck(batch, "value");
            #{
                sum: sum(v), mean: mean(v), median: median(v), min: min(v), max: max(v),
                std: std(v), variance: variance(v), q: quantile(v, 0.5), mad: mad(v), rms: rms(v),
                skew: skew(v), kurt: kurtosis(v), ac: autocorr(v, 1),
            }
            "#,
            &batch,
        )?;
        assert_eq!(out["sum"], json!(40.0));
        assert_eq!(out["mean"], json!(5.0));
        assert_eq!(out["median"], json!(4.5));
        assert_eq!(out["min"], json!(2.0));
        assert_eq!(out["max"], json!(9.0));
        assert_eq!(out["std"], json!(2.0));
        assert_eq!(out["variance"], json!(4.0));
        assert_eq!(out["q"], json!(4.5));
        assert_eq!(out["mad"], json!(0.5));
        assert!(out["rms"].is_number() && out["skew"].is_number());
        assert!(out["kurt"].is_number() && out["ac"].is_number());
        Ok(())
    }

    #[test]
    fn the_array_functions_return_arrays() -> Result<(), ScriptError> {
        let batch = readings(&[json!(1), json!(3), json!(2), json!(5), json!(1)]);
        let out = eval(
            r#"
            let v = pluck(batch, "value");
            #{ diff: diff(v), cumsum: cumsum(v), ewma: ewma(v, 1.0), peaks: peaks(v),
               z: zscore([1, 2, 3]).len(), h: histogram(v, 2) }
            "#,
            &batch,
        )?;
        assert_eq!(out["diff"], json!([2.0, -1.0, 3.0, -4.0]));
        assert_eq!(out["cumsum"], json!([1.0, 4.0, 6.0, 11.0, 12.0]));
        assert_eq!(out["ewma"], json!([1.0, 3.0, 2.0, 5.0, 1.0]));
        assert_eq!(out["peaks"], json!([1, 3]));
        assert_eq!(out["z"], json!(3));
        assert_eq!(out["h"], json!({"edges": [1.0, 3.0, 5.0], "counts": [3, 2]}));
        Ok(())
    }

    #[test]
    fn scalars_and_pairs() -> Result<(), ScriptError> {
        let out = eval(
            "
            #{ c: clamp(7.0, 0.0, 5.0), ci: clamp(-2, 0, 5),
               i: interp([0.0, 10.0], [0.0, 100.0], 2.5),
               d: dtw([0, 1, 2], [0, 0, 1, 1, 2, 2]),
               f: linfit([0.0, 2.0], [1.0, 5.0]).intercept }
            ",
            &[],
        )?;
        assert_eq!(out, json!({"c": 5.0, "ci": 0, "i": 25.0, "d": 0.0, "f": 1.0}));
        Ok(())
    }

    /// The rule that makes the warm-up check possible: `()` and never NaN.
    #[test]
    fn undefined_is_unit_never_nan() -> Result<(), ScriptError> {
        let out = eval(
            r#"
            let none = pluck(batch, "value");
            #{ mean: mean(none) == (), median: median(none) == (), std: std(none) == (),
               fit: linfit([1.0]) == (), z: zscore([2, 2, 2]) == (), sum: sum(none),
               t: parse_time(()) == () }
            "#,
            &[],
        )?;
        assert_eq!(
            out,
            json!({"mean": true, "median": true, "std": true, "fit": true, "z": true, "sum": 0.0, "t": true})
        );
        Ok(())
    }

    #[test]
    fn a_present_non_number_is_an_error_naming_it() {
        let batch = readings(&[json!(1), json!("twelve")]);
        let err = eval(r#"mean(pluck(batch, "value"))"#, &batch)
            .err()
            .map(|e| e.to_string())
            .unwrap_or_default();
        assert!(err.contains("element 1 is string"), "{err}");
    }

    #[test]
    fn parse_and_format_time_are_the_one_time_rule() -> Result<(), ScriptError> {
        let out = eval(
            r#"
            #{ s: parse_time("1970-01-01T00:00:01Z"), n: parse_time(1500),
               back: format_time(1500), rt: parse_time(format_time(1700000000123)) }
            "#,
            &[],
        )?;
        assert_eq!(
            out,
            json!({"s": 1000, "n": 1500, "back": "1970-01-01T00:00:01.500Z", "rt": 1_700_000_000_123_i64})
        );

        let err = eval(r#"parse_time("noon")"#, &[])
            .err()
            .map(|e| e.to_string())
            .unwrap_or_default();
        assert!(err.contains("noon") && err.contains("RFC 3339"), "{err}");
        Ok(())
    }
}
