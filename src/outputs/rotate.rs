//! Rotation policy, file naming and message encoding — the parts of a
//! file-shaped output that have nothing to do with *where* the bytes land.
//!
//! Split out from [`crate::outputs::file`] deliberately: the object-store
//! output is the same writer with a different destination. Everything here
//! takes no filesystem at all, which is both what makes it shareable and what
//! makes it unit-testable without a temporary directory.
//!
//! The division is: this module decides *when* a part is finished, *what* it is
//! called and *how* the messages are laid out inside it; the destination
//! decides where to put it and how to open it.

use chrono::{DateTime, Utc};
use kayak_core::config::{FileFormat, RotationConfig};
use std::time::Duration;

/// When to close the current part and start the next.
///
/// The config type with its options resolved: absent means "never on this
/// trigger", which is a policy that answers `false` rather than a branch every
/// caller has to remember.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Rotation {
    max_rows: Option<usize>,
    interval: Option<Duration>,
}

impl Rotation {
    #[must_use]
    pub fn new(config: Option<RotationConfig>) -> Self {
        let config = config.unwrap_or_default();
        Self {
            max_rows: config.max_rows,
            interval: config.interval_secs.map(Duration::from_secs),
        }
    }

    /// Whether this policy ever closes a part.
    ///
    /// "No triggers" is a fine answer on a filesystem — one file for the run —
    /// and not one on an object store, where a part that never closes is a part
    /// that is never uploaded. The s3 output asks this at build time rather than
    /// discovering it by growing.
    #[must_use]
    pub fn rotates(&self) -> bool {
        self.max_rows.is_some() || self.interval.is_some()
    }

    /// Whether a part holding `rows` messages, opened at `opened_at`, is
    /// finished as of `now`.
    ///
    /// Asked *after* a batch is written rather than before, so a batch is never
    /// split across two files. That means `max_rows` is a floor and not a
    /// ceiling — a batch of 500 arriving at 999 rows produces a file of 1499 —
    /// which is the right way round for a format where a message is a record:
    /// the alternative is either splitting a batch or buffering it, and both
    /// cost more than an oversized part.
    ///
    /// A zero `max_rows` would otherwise rotate on every check including the
    /// empty one, producing an unbounded stream of empty files; it is treated
    /// as "every batch" instead.
    #[must_use]
    pub fn is_full(&self, rows: usize, opened_at: DateTime<Utc>, now: DateTime<Utc>) -> bool {
        if rows == 0 {
            // nothing has been written, so there is nothing worth closing —
            // and rotating here would spin on the interval trigger alone
            return false;
        }
        let by_rows = self.max_rows.is_some_and(|max| rows >= max.max(1));
        let by_time = self.interval.is_some_and(|interval| {
            now.signed_duration_since(opened_at)
                .to_std()
                .is_ok_and(|elapsed| elapsed >= interval)
        });
        by_rows || by_time
    }
}

/// The name of one part.
///
/// Generated rather than configured, and the two halves are why. The open
/// timestamp makes the parts of a run sort chronologically under any plain
/// lexical listing (`ls`, an object-store prefix scan), which is the order
/// anyone reading them wants. The sequence number makes them unique when the
/// timestamp doesn't: rotating twice inside one second is entirely possible on
/// a row trigger, and two parts that overwrote each other would lose data
/// silently.
///
/// Colons are deliberately not in it. A timestamp's natural `14:00:00Z` is
/// illegal in a Windows file name and awkward in a shell everywhere else, so
/// the time separators are hyphens.
#[must_use]
pub fn part_name(opened_at: DateTime<Utc>, sequence: u64, format: FileFormat) -> String {
    format!(
        "{}-{sequence:06}.{}",
        opened_at.format("%Y-%m-%dT%H-%M-%SZ"),
        extension(format)
    )
}

/// The file extension a format is written under.
#[must_use]
pub fn extension(format: FileFormat) -> &'static str {
    match format {
        FileFormat::Ndjson => "ndjson",
        FileFormat::JsonArray => "json",
    }
}

/// Turns messages into the bytes of a part, one format at a time.
///
/// Stateful because the formats differ in whether they can be: ndjson appends
/// and is complete after every batch, a JSON array has to know whether it is
/// writing the first element and has to be closed. That difference is the whole
/// reason this is a type rather than a function.
#[derive(Clone, Debug)]
pub struct Encoder {
    format: FileFormat,
    /// Whether anything has been written to the current part. Only the array
    /// format cares — it decides the leading `[` and the separating commas.
    started: bool,
}

impl Encoder {
    #[must_use]
    pub fn new(format: FileFormat) -> Self {
        Self {
            format,
            started: false,
        }
    }

    /// Forget everything about the current part. Called on rotation, so the
    /// next part opens its own array rather than continuing the last one's.
    pub fn reset(&mut self) {
        self.started = false;
    }

    /// The bytes for `messages`, to be appended to the current part.
    ///
    /// Serializing every message separately rather than the batch as a whole is
    /// what makes the two formats one code path, and it is also the honest
    /// shape: a batch is a unit of *transport*, not something a reader of the
    /// file should be able to see. Two runs that batched differently must
    /// produce the same file.
    pub fn encode(
        &mut self,
        messages: &[std::sync::Arc<serde_json::Value>],
    ) -> anyhow::Result<Vec<u8>> {
        let mut out = Vec::new();
        for message in messages {
            let rendered = serde_json::to_vec(message.as_ref())?;
            match self.format {
                FileFormat::Ndjson => {
                    out.extend_from_slice(&rendered);
                    out.push(b'\n');
                }
                FileFormat::JsonArray => {
                    out.extend_from_slice(if self.started { b"," } else { b"[" });
                    out.extend_from_slice(&rendered);
                }
            }
            self.started = true;
        }
        Ok(out)
    }

    /// The bytes that finish a part, if the format needs any.
    ///
    /// An array that was never written to still has to come out as `[]` rather
    /// than an empty file, or a reader that parses every part fails on one that
    /// happened to be closed empty.
    #[must_use]
    pub fn finish(&self) -> Vec<u8> {
        match self.format {
            FileFormat::Ndjson => Vec::new(),
            FileFormat::JsonArray if self.started => b"]".to_vec(),
            FileFormat::JsonArray => b"[]".to_vec(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::sync::Arc;

    fn at(seconds: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(seconds, 0).unwrap_or_default()
    }

    fn messages(values: &[serde_json::Value]) -> Vec<Arc<serde_json::Value>> {
        values.iter().cloned().map(Arc::new).collect()
    }

    #[test]
    fn no_triggers_never_rotates() {
        let rotation = Rotation::new(None);
        assert!(!rotation.is_full(1_000_000, at(0), at(1_000_000)));
    }

    #[test]
    fn a_row_count_rotates_once_it_is_reached() {
        let rotation = Rotation::new(Some(RotationConfig {
            max_rows: Some(10),
            interval_secs: None,
        }));
        assert!(!rotation.is_full(9, at(0), at(0)));
        assert!(rotation.is_full(10, at(0), at(0)));
        // a batch can overshoot rather than being split
        assert!(rotation.is_full(1500, at(0), at(0)));
    }

    #[test]
    fn an_interval_rotates_once_it_has_elapsed_since_the_open() {
        let rotation = Rotation::new(Some(RotationConfig {
            max_rows: None,
            interval_secs: Some(60),
        }));
        assert!(!rotation.is_full(1, at(0), at(59)));
        assert!(rotation.is_full(1, at(0), at(60)));
    }

    /// Whichever comes first — that is what makes the two triggers combinable
    /// rather than a choice.
    #[test]
    fn either_trigger_is_enough() {
        let rotation = Rotation::new(Some(RotationConfig {
            max_rows: Some(10),
            interval_secs: Some(60),
        }));
        assert!(rotation.is_full(10, at(0), at(0)), "rows alone");
        assert!(rotation.is_full(1, at(0), at(60)), "time alone");
    }

    /// The trap this guards: an interval trigger on an idle pipeline would
    /// otherwise close and reopen a part every time it was asked, filling the
    /// directory with empty files while nothing was flowing.
    #[test]
    fn an_empty_part_is_never_full() {
        let rotation = Rotation::new(Some(RotationConfig {
            max_rows: Some(1),
            interval_secs: Some(1),
        }));
        assert!(!rotation.is_full(0, at(0), at(10_000)));
    }

    /// Zero would mean "rotate before writing anything", which is an unbounded
    /// stream of empty files rather than a configuration anyone wants.
    #[test]
    fn a_zero_row_limit_rotates_per_batch_rather_than_forever() {
        let rotation = Rotation::new(Some(RotationConfig {
            max_rows: Some(0),
            interval_secs: None,
        }));
        assert!(!rotation.is_full(0, at(0), at(0)));
        assert!(rotation.is_full(1, at(0), at(0)));
    }

    #[test]
    fn a_part_is_named_for_when_it_opened_and_which_one_it_is() {
        assert_eq!(
            part_name(at(1_770_000_000), 7, FileFormat::Ndjson),
            "2026-02-02T02-40-00Z-000007.ndjson"
        );
        assert_eq!(
            part_name(at(1_770_000_000), 7, FileFormat::JsonArray),
            "2026-02-02T02-40-00Z-000007.json"
        );
    }

    /// Two parts opened in the same second must not be the same file, or a row
    /// trigger on a fast pipeline loses data by overwriting.
    #[test]
    fn parts_opened_in_the_same_second_are_still_distinct() {
        let first = part_name(at(10), 1, FileFormat::Ndjson);
        let second = part_name(at(10), 2, FileFormat::Ndjson);
        assert_ne!(first, second);
    }

    /// Lexical order is chronological order — which is what makes `ls` and an
    /// object-store prefix listing return the parts in the order they happened.
    #[test]
    fn part_names_sort_chronologically() {
        let mut names = vec![
            part_name(at(2_000), 3, FileFormat::Ndjson),
            part_name(at(1_000), 1, FileFormat::Ndjson),
            part_name(at(1_000), 2, FileFormat::Ndjson),
        ];
        names.sort();
        assert_eq!(
            names,
            vec![
                part_name(at(1_000), 1, FileFormat::Ndjson),
                part_name(at(1_000), 2, FileFormat::Ndjson),
                part_name(at(2_000), 3, FileFormat::Ndjson),
            ]
        );
    }

    /// Illegal on Windows and a nuisance to quote everywhere else.
    #[test]
    fn a_part_name_has_no_colons_in_it() {
        assert!(!part_name(at(1_770_000_000), 1, FileFormat::Ndjson).contains(':'));
    }

    #[test]
    fn ndjson_is_one_message_per_line() -> anyhow::Result<()> {
        let mut encoder = Encoder::new(FileFormat::Ndjson);
        let bytes = encoder.encode(&messages(&[json!({"a": 1}), json!({"a": 2})]))?;
        assert_eq!(String::from_utf8(bytes)?, "{\"a\":1}\n{\"a\":2}\n");
        assert!(encoder.finish().is_empty());
        Ok(())
    }

    /// The point of ndjson: a file that is still being written is already a
    /// valid one, so a run that is halfway through — or that died — is readable.
    #[test]
    fn ndjson_is_complete_after_every_batch() -> anyhow::Result<()> {
        let mut encoder = Encoder::new(FileFormat::Ndjson);
        let mut file = encoder.encode(&messages(&[json!({"a": 1})]))?;
        for line in String::from_utf8(file.clone())?.lines() {
            serde_json::from_str::<serde_json::Value>(line)?;
        }
        file.extend(encoder.encode(&messages(&[json!({"a": 2})]))?);
        assert_eq!(String::from_utf8(file)?.lines().count(), 2);
        Ok(())
    }

    #[test]
    fn a_json_array_brackets_and_separates_across_batches() -> anyhow::Result<()> {
        let mut encoder = Encoder::new(FileFormat::JsonArray);
        let mut file = encoder.encode(&messages(&[json!({"a": 1})]))?;
        file.extend(encoder.encode(&messages(&[json!({"a": 2})]))?);
        file.extend(encoder.finish());
        assert_eq!(String::from_utf8(file.clone())?, "[{\"a\":1},{\"a\":2}]");
        let parsed: Vec<serde_json::Value> = serde_json::from_slice(&file)?;
        assert_eq!(parsed.len(), 2);
        Ok(())
    }

    /// A part closed without ever being written to still has to parse, or a
    /// reader that walks every part in a directory falls over on one of them.
    #[test]
    fn an_empty_json_array_part_is_still_valid_json() -> anyhow::Result<()> {
        let encoder = Encoder::new(FileFormat::JsonArray);
        let parsed: Vec<serde_json::Value> = serde_json::from_slice(&encoder.finish())?;
        assert!(parsed.is_empty());
        Ok(())
    }

    /// Rotation must not leave the next part continuing the last one's array.
    #[test]
    fn resetting_starts_a_new_array() -> anyhow::Result<()> {
        let mut encoder = Encoder::new(FileFormat::JsonArray);
        encoder.encode(&messages(&[json!({"a": 1})]))?;
        encoder.reset();
        let mut bytes = encoder.encode(&messages(&[json!({"a": 2})]))?;
        // it opens its own array rather than continuing the last part's
        assert_eq!(String::from_utf8(bytes.clone())?, "[{\"a\":2}");
        bytes.extend(encoder.finish());
        assert_eq!(String::from_utf8(bytes)?, "[{\"a\":2}]");
        Ok(())
    }

    /// A batch is a unit of transport and must not be visible in the file: the
    /// same messages arriving as one batch or as three are the same bytes.
    #[test]
    fn batching_does_not_show_up_in_the_output() -> anyhow::Result<()> {
        let values = [json!({"a": 1}), json!({"a": 2}), json!({"a": 3})];
        for format in [FileFormat::Ndjson, FileFormat::JsonArray] {
            let mut whole = Encoder::new(format);
            let mut at_once = whole.encode(&messages(&values))?;
            at_once.extend(whole.finish());

            let mut split = Encoder::new(format);
            let mut one_by_one = Vec::new();
            for value in &values {
                one_by_one.extend(split.encode(&messages(std::slice::from_ref(value)))?);
            }
            one_by_one.extend(split.finish());

            assert_eq!(at_once, one_by_one, "{format:?}");
        }
        Ok(())
    }
}
