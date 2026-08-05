use std::sync::Arc;

use crate::{
    BuildCtx,
    inputs::MessageBatch,
    outputs::{BuildOutput, OutputDestination},
    state::StreamerId,
};
use anyhow::Result;
use chrono::{DateTime, SecondsFormat, Utc};
use serde::Serialize;
use streamer_core::config::StdoutOutputConfig;

impl BuildOutput for StdoutOutputConfig {
    fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn OutputDestination>> {
        Ok(Box::new(StdoutOutput {
            streamer_id: ctx.streamer_id.clone(),
        }))
    }
}

pub struct StdoutOutput {
    streamer_id: StreamerId,
}

/// What a batch looks like on the terminal: the batch itself plus enough
/// context to tell rows apart once several streamers are printing to the same
/// stdout. Serialized compact, so one batch is one row.
#[derive(Serialize)]
struct Row<'a> {
    /// RFC 3339, millisecond precision — the time the batch was emitted, not
    /// the time any message in it was produced.
    ts: String,
    streamer: &'a StreamerId,
    /// Number of messages in the batch, so a long row can be read at a glance.
    count: usize,
    batch: &'a MessageBatch,
}

fn format_batch(
    streamer_id: &StreamerId,
    at: DateTime<Utc>,
    message_batch: &MessageBatch,
) -> Result<String> {
    let row = Row {
        ts: at.to_rfc3339_opts(SecondsFormat::Millis, true),
        streamer: streamer_id,
        count: message_batch.len(),
        batch: message_batch,
    };
    Ok(serde_json::to_string(&row)?)
}

#[async_trait::async_trait]
impl OutputDestination for StdoutOutput {
    async fn emit(&mut self, message_batch: Arc<MessageBatch>) -> anyhow::Result<()> {
        println!(
            "{}",
            format_batch(&self.streamer_id, Utc::now(), &message_batch)?
        );
        Ok(())
    }
    async fn init(&mut self) -> Result<()> {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::format_batch;
    use chrono::{DateTime, Utc};
    use serde_json::json;
    use std::sync::Arc;

    fn at(rfc3339: &str) -> anyhow::Result<DateTime<Utc>> {
        Ok(DateTime::parse_from_rfc3339(rfc3339)?.with_timezone(&Utc))
    }

    #[test]
    fn a_batch_is_one_row_labelled_with_its_streamer_and_time() -> anyhow::Result<()> {
        let batch = vec![
            Arc::new(json!({"a": 1, "nested": {"b": [1, 2]}})),
            Arc::new(json!({"a": 2})),
        ];

        let line = format_batch(
            &"witty-crab".to_string(),
            at("2026-08-04T09:30:00.123Z")?,
            &batch,
        )?;

        assert!(!line.contains('\n'), "expected a single row, got: {line}");
        assert_eq!(
            line,
            r#"{"ts":"2026-08-04T09:30:00.123Z","streamer":"witty-crab","count":2,"batch":[{"a":1,"nested":{"b":[1,2]}},{"a":2}]}"#
        );
        Ok(())
    }

    #[test]
    fn an_empty_batch_is_still_one_row() -> anyhow::Result<()> {
        let line = format_batch(
            &"witty-crab".to_string(),
            at("2026-08-04T09:30:00Z")?,
            &Vec::new(),
        )?;
        assert_eq!(
            line,
            r#"{"ts":"2026-08-04T09:30:00.000Z","streamer":"witty-crab","count":0,"batch":[]}"#
        );
        Ok(())
    }
}
