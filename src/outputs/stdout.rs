use std::io::{self, Write};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::{
    BuildCtx,
    inputs::MessageBatch,
    outputs::{BuildOutput, OutputDestination},
    state::PipelineId,
};
use anyhow::Result;
use chrono::{DateTime, SecondsFormat, Utc};
use kayak_core::config::StdoutOutputConfig;
use serde::Serialize;

/// How long a row may sit in the buffer before it is pushed out, and how much
/// may accumulate before it goes early.
///
/// A quarter of a second is under the threshold where a terminal stops feeling
/// live, and the size cap is what keeps a firehose from holding a quarter of a
/// second of *its* output — which at these rates is a great deal of memory.
const FLUSH_INTERVAL: Duration = Duration::from_millis(250);
const FLUSH_BYTES: usize = 64 * 1024;

impl BuildOutput for StdoutOutputConfig {
    fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn OutputDestination>> {
        Ok(Box::new(StdoutOutput {
            pipeline_id: ctx.pipeline_id.clone(),
            buffer: String::new(),
            last_flush: Instant::now(),
        }))
    }
}

/// Writes each batch to stdout as one JSON row.
///
/// **Buffered, not `println!` per batch.** `println!` locks stdout and makes a
/// blocking write syscall every time, on a thread that is supposed to be
/// driving async work — measured at roughly an eight-fold cost to a pipeline's
/// throughput against an output that does nothing. Rows accumulate here instead
/// and go out in one write every [`FLUSH_INTERVAL`], or sooner if
/// [`FLUSH_BYTES`] builds up first.
///
/// The time bound is what keeps it honest as a *diagnostic* output: the
/// heartbeat pipeline in `example_config/` emits once a second, and a purely
/// size-triggered buffer would leave its rows invisible for minutes. Whatever is
/// still held is flushed by `finish`, so nothing is lost when a pipeline stops.
pub struct StdoutOutput {
    pipeline_id: PipelineId,
    buffer: String,
    last_flush: Instant,
}

impl StdoutOutput {
    /// Push what's buffered, if anything.
    fn flush(&mut self) -> Result<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let mut out = io::stdout().lock();
        out.write_all(self.buffer.as_bytes())?;
        out.flush()?;
        self.buffer.clear();
        self.last_flush = Instant::now();
        Ok(())
    }

    /// Whether what's buffered has waited long enough, or grown big enough.
    fn due(&self) -> bool {
        self.buffer.len() >= FLUSH_BYTES || self.last_flush.elapsed() >= FLUSH_INTERVAL
    }
}

/// What a batch looks like on the terminal: the batch itself plus enough
/// context to tell rows apart once several pipelines are printing to the same
/// stdout. Serialized compact, so one batch is one row.
#[derive(Serialize)]
struct Row<'a> {
    /// RFC 3339, millisecond precision — the time the batch was emitted, not
    /// the time any message in it was produced.
    ts: String,
    pipeline: &'a PipelineId,
    /// Number of messages in the batch, so a long row can be read at a glance.
    count: usize,
    batch: &'a MessageBatch,
}

fn format_batch(
    pipeline_id: &PipelineId,
    at: DateTime<Utc>,
    message_batch: &MessageBatch,
) -> Result<String> {
    let row = Row {
        ts: at.to_rfc3339_opts(SecondsFormat::Millis, true),
        pipeline: pipeline_id,
        count: message_batch.len(),
        batch: message_batch,
    };
    Ok(serde_json::to_string(&row)?)
}

#[async_trait::async_trait]
impl OutputDestination for StdoutOutput {
    async fn emit(&mut self, message_batch: Arc<MessageBatch>) -> anyhow::Result<()> {
        self.buffer
            .push_str(&format_batch(&self.pipeline_id, Utc::now(), &message_batch)?);
        self.buffer.push('\n');
        if self.due() {
            self.flush()?;
        }
        Ok(())
    }
    async fn init(&mut self) -> Result<()> {
        Ok(())
    }
    /// Whatever is still buffered when the pipeline stops. Without this the
    /// last rows of a short run would never be printed at all.
    async fn finish(&mut self) -> Result<()> {
        self.flush()
    }
}

#[cfg(test)]
mod tests {
    use super::{FLUSH_BYTES, StdoutOutput, format_batch};
    use crate::outputs::OutputDestination;
    use chrono::{DateTime, Utc};
    use serde_json::json;
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    fn output() -> StdoutOutput {
        StdoutOutput {
            pipeline_id: "witty-crab".to_string(),
            buffer: String::new(),
            last_flush: Instant::now(),
        }
    }

    /// The point of the buffer: a batch does not cost a write syscall. Without
    /// this the emit path is a `println!` again and the eight-fold throughput
    /// cost comes back.
    #[tokio::test]
    async fn a_batch_is_buffered_rather_than_written_immediately() -> anyhow::Result<()> {
        let mut out = output();
        out.emit(Arc::new(vec![Arc::new(json!({"n": 1}))])).await?;

        assert!(
            !out.buffer.is_empty(),
            "the row should still be held, not already written"
        );
        Ok(())
    }

    /// Buffering must never turn into losing. A pipeline that stops with rows
    /// still held has to write them, which is what `finish` is for.
    #[tokio::test]
    async fn finishing_writes_out_what_is_still_held() -> anyhow::Result<()> {
        let mut out = output();
        out.emit(Arc::new(vec![Arc::new(json!({"n": 1}))])).await?;
        out.finish().await?;

        assert!(
            out.buffer.is_empty(),
            "finish should have flushed the held rows"
        );
        Ok(())
    }

    /// A firehose must not be able to hold a whole flush interval's output in
    /// memory, so size triggers a write before time does.
    #[test]
    fn a_large_buffer_is_due_before_the_interval_elapses() {
        let mut out = output();
        out.buffer = "x".repeat(FLUSH_BYTES);

        assert!(
            out.due(),
            "a buffer past the size cap should be due regardless of the clock"
        );
    }

    /// And the other way round: a trickle still reaches the terminal, or the
    /// once-a-second heartbeat pipeline would look dead.
    #[test]
    fn a_small_buffer_is_due_once_the_interval_has_passed() {
        let mut out = output();
        out.buffer = "one row\n".to_string();
        assert!(!out.due(), "a fresh, small buffer should wait");

        let Some(a_second_ago) = Instant::now().checked_sub(Duration::from_secs(1)) else {
            panic!("the clock is less than a second past its origin");
        };
        out.last_flush = a_second_ago;
        assert!(out.due(), "a row that has waited should go out");
    }

    fn at(rfc3339: &str) -> anyhow::Result<DateTime<Utc>> {
        Ok(DateTime::parse_from_rfc3339(rfc3339)?.with_timezone(&Utc))
    }

    #[test]
    fn a_batch_is_one_row_labelled_with_its_pipeline_and_time() -> anyhow::Result<()> {
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
            r#"{"ts":"2026-08-04T09:30:00.123Z","pipeline":"witty-crab","count":2,"batch":[{"a":1,"nested":{"b":[1,2]}},{"a":2}]}"#
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
            r#"{"ts":"2026-08-04T09:30:00.000Z","pipeline":"witty-crab","count":0,"batch":[]}"#
        );
        Ok(())
    }
}
