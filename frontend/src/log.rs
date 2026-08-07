//! The per-card message log: what a `UiEvent` looks like as lines of text.
//!
//! Pure so it can be tested without a DOM — the component in `app.rs` only
//! appends what this produces and renders it.

use kayak_core::{EventPayload, UiEvent};
use std::collections::VecDeque;

/// How many lines a card keeps. Older lines are dropped from the front, so the
/// log reads like a tail.
pub const LOG_CAPACITY: usize = 10;

/// A rendered log line. `error` is what the card styles differently — the
/// distinction is worth more than the exact wording.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Line {
    pub text: String,
    pub error: bool,
}

/// The lines an event contributes to the log of the pipeline it names.
///
/// A batch logs one line per message, which is what makes the log a view of
/// data rather than of batches. A failure logs one line naming the stage that
/// failed — without it the card just stops updating, and the reason for that
/// only exists in the server log.
#[must_use]
pub fn lines_for(event: &UiEvent) -> Vec<Line> {
    match &event.payload {
        EventPayload::Batch(batch) => batch
            .iter()
            .map(|msg| Line {
                text: msg.to_string(),
                error: false,
            })
            .collect(),
        EventPayload::Error(message) => vec![Line {
            text: format!("{stage} error: {message}", stage = event.stage),
            error: true,
        }],
    }
}

/// Append lines to a log, dropping the oldest to stay within [`LOG_CAPACITY`].
pub fn append(log: &mut VecDeque<(u64, Line)>, next_id: &mut u64, lines: Vec<Line>) {
    for line in lines {
        if log.len() == LOG_CAPACITY {
            log.pop_front();
        }
        log.push_back((*next_id, line));
        *next_id += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::{LOG_CAPACITY, Line, append, lines_for};
    use kayak_core::{UiEvent, stage};
    use serde_json::json;
    use std::collections::VecDeque;
    use std::sync::Arc;

    #[test]
    fn a_batch_logs_one_line_per_message() {
        let event = UiEvent::batch(
            "witty-crab".to_string(),
            stage::OUTPUT,
            Arc::new(vec![Arc::new(json!({"n": 1})), Arc::new(json!({"n": 2}))]),
        );

        let lines = lines_for(&event);

        assert_eq!(lines.len(), 2);
        assert!(lines.iter().all(|l| !l.error));
        assert_eq!(lines[0].text, r#"{"n":1}"#);
    }

    #[test]
    fn a_failure_logs_one_line_naming_the_stage_that_failed() {
        let event = UiEvent::error(
            "witty-crab".to_string(),
            stage::TRANSFORM,
            &"http request failed: connection refused",
        );

        assert_eq!(
            lines_for(&event),
            vec![Line {
                text: "transform error: http request failed: connection refused".to_string(),
                error: true,
            }]
        );
    }

    #[test]
    fn the_log_keeps_the_newest_lines_only() {
        let mut log = VecDeque::new();
        let mut next_id = 0;

        for n in 0..(LOG_CAPACITY + 3) {
            append(
                &mut log,
                &mut next_id,
                vec![Line {
                    text: n.to_string(),
                    error: false,
                }],
            );
        }

        assert_eq!(log.len(), LOG_CAPACITY);
        assert_eq!(log.front().map(|(_, l)| l.text.as_str()), Some("3"));
        assert_eq!(log.back().map(|(_, l)| l.text.as_str()), Some("12"));
    }

    /// Keys have to stay unique across the whole run, or `<For>` reuses a pipeline
    /// for a different line and the log shows stale text.
    #[test]
    fn every_line_gets_its_own_key_even_after_lines_are_dropped() {
        let mut log = VecDeque::new();
        let mut next_id = 0;
        let line = |n: usize| Line {
            text: n.to_string(),
            error: false,
        };

        for n in 0..(LOG_CAPACITY * 2) {
            append(&mut log, &mut next_id, vec![line(n)]);
        }

        let mut keys: Vec<_> = log.iter().map(|(k, _)| *k).collect();
        let count = keys.len();
        keys.dedup();
        assert_eq!(keys.len(), count);
    }
}
