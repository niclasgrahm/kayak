//! Reading a point in time off a message — the one rule for it.
//!
//! Nearly everything statistical wants to know *when* a reading was taken: a
//! rate is a delta over a time, a slope is per second, a window is so many
//! seconds wide. Until this existed kayak had no rule for that — `now()` and
//! `now_millis()` could write a time, and nothing could read one back. This
//! module is that rule, and every component that reads a time is meant to go
//! through it so that "what is a time" has one answer.
//!
//! **A time is an RFC 3339 string or a number of milliseconds since the
//! epoch.** Milliseconds rather than seconds because `now_millis()` is what a
//! script writes, and a script reading its own field back with the other unit
//! is the kind of wrong that is out by a factor of a thousand and visible only
//! on a chart. (The `map` transform's `cast: timestamp` reads a bare number as
//! seconds, for the column mapping's reason; that is a *conversion* of the
//! field, and this is a reading of it. The two are documented against each
//! other on the site.) A string that is not RFC 3339 and a number that is not
//! finite are errors naming the value, never a guess: a `"2024-01-01 12:00"`
//! read as midnight would be a reading silently moved by twelve hours.
//!
//! [`MessageTime`] is the component-side half: a component with a `time`
//! setting holds one, and asks it for each message's time. Absent, the time is
//! **arrival** — the moment the component was asked — which is the right
//! default for a stream read live and the wrong one for a replay, and the
//! setting is what a replay sets.

use anyhow::{Context, Result, anyhow, bail};
use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::Value;

/// A time read off a JSON value — see the module docs for what one is.
pub fn parse(value: &Value) -> Result<DateTime<Utc>> {
    match value {
        Value::String(text) => DateTime::parse_from_rfc3339(text.trim())
            .map(|t| t.with_timezone(&Utc))
            .map_err(|err| anyhow!("'{text}' is not an RFC 3339 time ({err})")),
        Value::Number(_) => {
            let millis = value
                .as_f64()
                .filter(|m| m.is_finite())
                .ok_or_else(|| anyhow!("{value} is not a number of milliseconds"))?;
            // `f64 → i64` here is the same range check `from_timestamp_millis`
            // makes, done first so an absurd number is an error rather than a
            // saturated cast that lands on the last representable instant.
            if millis.abs() >= 9.0e15 {
                bail!("{value} is out of range for a time in milliseconds");
            }
            #[allow(
                clippy::cast_possible_truncation,
                reason = "range-checked immediately above; fractional millis are dropped on purpose"
            )]
            let millis = millis.round() as i64;
            DateTime::from_timestamp_millis(millis)
                .ok_or_else(|| anyhow!("{value} is out of range for a time in milliseconds"))
        }
        Value::Null => bail!("it is null"),
        other => bail!(
            "{} is not a time — a time is an RFC 3339 string or milliseconds since the epoch",
            crate::fields::describe(other)
        ),
    }
}

/// [`parse`], as the milliseconds a script does arithmetic in.
pub fn parse_millis(value: &Value) -> Result<i64> {
    parse(value).map(|t| t.timestamp_millis())
}

/// Milliseconds since the epoch as an RFC 3339 string in UTC, to the
/// millisecond — the spelling `now()` uses and `parse` reads back exactly.
pub fn format(millis: i64) -> Result<String> {
    DateTime::from_timestamp_millis(millis)
        .map(|t| t.to_rfc3339_opts(SecondsFormat::Millis, true))
        .ok_or_else(|| anyhow!("{millis} is out of range for a time in milliseconds"))
}

/// Where a component reads a message's time from: a field, or the clock.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct MessageTime {
    field: Option<String>,
}

impl MessageTime {
    /// From a component's `time` setting. Blank is the same as absent, so a
    /// form's empty box means what leaving the field out means.
    #[must_use]
    pub fn new(field: Option<String>) -> Self {
        Self {
            field: field.map(|f| f.trim().to_string()).filter(|f| !f.is_empty()),
        }
    }

    /// Whether the time is the clock rather than a field.
    #[must_use]
    pub fn is_arrival(&self) -> bool {
        self.field.is_none()
    }

    /// The field, when one is configured.
    #[must_use]
    pub fn field(&self) -> Option<&str> {
        self.field.as_deref()
    }

    /// This message's time. A configured field that is missing, null or not
    /// a time is an error naming the field — a reading with no time is not
    /// one that can be placed, and quietly substituting arrival would put it
    /// at the wrong place with nothing to say so.
    pub fn of(&self, message: &Value) -> Result<DateTime<Utc>> {
        let Some(field) = &self.field else {
            return Ok(Utc::now());
        };
        let value = crate::fields::get(message, field)
            .filter(|v| !v.is_null())
            .ok_or_else(|| anyhow!("the time field '{field}' is missing from a message"))?;
        parse(value).with_context(|| format!("the time field '{field}'"))
    }

    /// [`MessageTime::of`], in milliseconds since the epoch.
    pub fn millis_of(&self, message: &Value) -> Result<i64> {
        self.of(message).map(|t| t.timestamp_millis())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn a_time_is_rfc_3339_or_epoch_millis() -> Result<()> {
        assert_eq!(parse_millis(&json!("1970-01-01T00:00:01Z"))?, 1000);
        assert_eq!(parse_millis(&json!(" 1970-01-01T01:00:00+01:00 "))?, 0, "offsets are honoured");
        assert_eq!(parse_millis(&json!(1_700_000_000_000_i64))?, 1_700_000_000_000);
        assert_eq!(parse_millis(&json!(1500.7))?, 1501, "fractional millis round");
        assert_eq!(parse_millis(&json!(-1000))?, -1000, "before the epoch is a time too");
        Ok(())
    }

    /// The one that matters: a near-miss is refused, not read as something.
    #[test]
    fn anything_else_is_an_error_naming_the_value() {
        for wrong in [
            json!("2024-01-01 12:00"),
            json!("2024-01-01"),
            json!("yesterday"),
            json!(true),
            json!(null),
            json!([1]),
            json!(1e300),
        ] {
            let err = parse(&wrong).err().map(|e| e.to_string()).unwrap_or_default();
            assert!(!err.is_empty(), "{wrong} should not parse as a time");
        }
        let err = parse(&json!("noon")).err().map(|e| e.to_string()).unwrap_or_default();
        assert!(err.contains("noon"), "{err}");
    }

    #[test]
    fn format_and_parse_round_trip_to_the_millisecond() -> Result<()> {
        let text = format(1_700_000_000_123)?;
        assert_eq!(text, "2023-11-14T22:13:20.123Z");
        assert_eq!(parse_millis(&Value::String(text))?, 1_700_000_000_123);
        assert!(format(i64::MAX).is_err());
        Ok(())
    }

    #[test]
    fn a_field_is_read_and_arrival_is_the_default() -> Result<()> {
        let message = json!({"ts": "1970-01-01T00:00:02Z", "sensor": {"at": 3000}});
        assert_eq!(MessageTime::new(Some("ts".into())).millis_of(&message)?, 2000);
        assert_eq!(
            MessageTime::new(Some("sensor.at".into())).millis_of(&message)?,
            3000,
            "a dotted path, as everywhere else"
        );

        let arrival = MessageTime::new(Some("  ".into()));
        assert!(arrival.is_arrival(), "blank is absent");
        let before = Utc::now();
        let read = arrival.of(&message)?;
        assert!(read >= before && read <= Utc::now());
        Ok(())
    }

    #[test]
    fn a_configured_field_that_is_missing_is_an_error_not_arrival() {
        let time = MessageTime::new(Some("ts".into()));
        for message in [json!({}), json!({"ts": null}), json!({"ts": "soon"})] {
            let err = time.of(&message).err().map(|e| format!("{e:#}")).unwrap_or_default();
            assert!(err.contains("'ts'"), "{message}: {err}");
        }
    }
}
