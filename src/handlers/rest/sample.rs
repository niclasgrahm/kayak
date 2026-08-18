//! Taking a few real messages off an input, without creating a pipeline.
//!
//! See `Operation::SampleInput` in `kayak_core::api_docs` for what the endpoint
//! is, and [`kayak_core::sample`] for the declaration of what sampling each
//! kind of input costs. What is worth knowing here is the four rules the
//! handler is built out of:
//!
//! - **It builds the production input**, through the same `BuildCtx`
//!   `create_pipeline` uses, for the reason the script dry run compiles with
//!   the production runner: a sample that can disagree with the pipeline is
//!   worse than no sample, because it will be believed.
//! - **What it changes, it says.** The adjustments are not hidden fixes —
//!   they come back in `notes`, out of the same declaration that decided the
//!   input was samplable at all.
//! - **Nothing is acknowledged.** The `Ack` a delivery carries is dropped
//!   rather than fired: a sample has not delivered anything anywhere, and
//!   telling a broker otherwise is the one way a read-only operation could
//!   lose somebody's message.
//! - **It is bounded on both axes and holds nothing.** At most
//!   `MAX_MESSAGES` within at most `MAX_TIMEOUT_MS`, then the input is
//!   dropped, which is what ends the subscription.

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::{Json, extract::State, response::IntoResponse};
use kayak_core::config::{InputConfig, InputKind};
use kayak_core::sample::{SampleRequest, SampleResponse, SampleStage, Sampling};
use serde_json::Value;

use crate::handlers::error::AppError;
use crate::state::{AppState, PipelineId};

/// Fetch a few messages from an input — see `Operation::SampleInput`.
pub async fn sample_input(
    State(state): State<Arc<AppState>>,
    Json(request): Json<SampleRequest>,
) -> Result<impl IntoResponse, AppError> {
    let kind = kind_of(&request.input);
    // Not knowing is a 400 rather than a 500: the only way to get here is a
    // kind nobody has declared, which for a request that deserialized means a
    // kind added without an arm — and `every_input_says_whether_it_can_be_sampled`
    // is what stops that reaching a release.
    let sampling = kayak_core::sample::for_input(&kind).ok_or_else(|| {
        AppError::bad_request(anyhow::anyhow!("a '{kind}' input cannot be sampled"))
    })?;
    if let Sampling::Refused { reason } = &sampling {
        return Err(AppError::bad_request(anyhow::anyhow!("{reason}")));
    }

    // read before the config is taken apart, since both are bounded by what
    // the request asked for rather than by what it carried
    let wanted = request.messages_wanted();
    let wait = Duration::from_millis(request.wait_ms());
    let mut notes: Vec<String> = sampling.note().map(ToString::to_string).into_iter().collect();
    let input = prepare(request.input, &mut notes);

    // A throwaway id, and a random one: an input derives what it announces
    // itself as from it (the mqtt client id), so it has to be unlike any
    // pipeline's — including one created while this sample is in flight.
    let id: PipelineId = petname::petname(3, "-")
        .map_or_else(|| "sample".to_string(), |name| format!("sample-{name}"));

    let mut source = match state.build_sample_input(&id, input) {
        Ok(source) => source,
        Err(err) => {
            return Ok(Json(SampleResponse::Failed {
                stage: SampleStage::Build,
                message: format!("{err:#}"),
            }));
        }
    };

    let started = Instant::now();
    let mut messages: Vec<Value> = Vec::new();
    let read = tokio::time::timeout(wait, async {
        while messages.len() < wanted {
            // the `Ack` inside goes out of scope here, unfired, which is the
            // point — see the module docs
            let delivery = source.next().await?;
            for message in delivery.batch.iter() {
                messages.push((**message).clone());
                if messages.len() == wanted {
                    break;
                }
            }
        }
        anyhow::Ok(())
    })
    .await;

    // A timeout is not a failure: it is "that is all there was", which is an
    // answer. A read error is a failure even when some messages arrived first,
    // because the reason it stopped is the thing worth showing.
    if let Ok(Err(err)) = read {
        return Ok(Json(SampleResponse::Failed {
            stage: SampleStage::Read,
            message: format!("{err:#}"),
        }));
    }

    Ok(Json(SampleResponse::Sampled {
        messages,
        notes,
        #[allow(clippy::cast_possible_truncation)]
        waited_ms: started.elapsed().as_millis() as u64,
    }))
}

/// The kind's wire name — the key [`kayak_core::sample::for_input`] answers by.
///
/// Read off the serialized tag rather than matched here, so that adding an
/// input kind doesn't need an arm in this file that could disagree with the
/// declaration. The tag *is* the name in both places.
fn kind_of(input: &InputConfig) -> String {
    serde_json::to_value(input)
        .ok()
        .and_then(|value| value["type"].as_str().map(ToString::to_string))
        .unwrap_or_default()
}

/// The config as the sample will build it, and what that changed.
///
/// Everything this touches is something that would otherwise make the sample
/// either useless or a nuisance to the running system. Note what it does *not*
/// touch: the envelope, the batch cap, where the input starts. Those are what
/// the pipeline will do, and showing something else would be showing the wrong
/// stream.
fn prepare(mut input: InputConfig, notes: &mut Vec<String>) -> InputConfig {
    // A buffer's whole job is to make the pipeline wait — for a count, or for
    // a window to close. A sample that honoured it would report "nothing
    // arrived" for a stream that is publishing perfectly well.
    if input.buffer.take().is_some() {
        notes.push(
            "the input's buffer was ignored — a sample shows messages as they arrive".to_string(),
        );
    }
    // The adjustment the kafka note in `sample::for_input` promises. A group
    // id nobody else uses is what makes reading harmless: no rebalance of the
    // pipeline's group, and the offsets committed under this name are the
    // sample's own and are never read again.
    if let InputKind::Kafka(kafka) = &mut input.kind {
        kafka.group = petname::petname(3, "-").map_or_else(
            || format!("kayak-sample-{}", kafka.group),
            |name| format!("kayak-sample-{name}"),
        );
    }
    input
}

#[cfg(test)]
mod tests {
    use super::*;

    fn input(json: serde_json::Value) -> InputConfig {
        match serde_json::from_value(json) {
            Ok(config) => config,
            Err(err) => panic!("not a valid input config: {err}"),
        }
    }

    #[test]
    fn the_kind_is_read_off_the_wire_tag() {
        assert_eq!(
            kind_of(&input(serde_json::json!({"type": "dummy", "duration": 1}))),
            "dummy"
        );
    }

    #[test]
    fn a_buffer_is_dropped_and_said_so() {
        let mut notes = Vec::new();
        let prepared = prepare(
            input(serde_json::json!({
                "type": "dummy",
                "duration": 1,
                "buffer": {"type": "static", "size": 100},
            })),
            &mut notes,
        );
        assert!(prepared.buffer.is_none());
        assert_eq!(notes.len(), 1);
    }

    #[test]
    fn an_input_with_no_buffer_is_left_alone_and_says_nothing() {
        let mut notes = Vec::new();
        prepare(
            input(serde_json::json!({"type": "dummy", "duration": 1})),
            &mut notes,
        );
        assert!(notes.is_empty());
    }

    /// The pipeline's own group is what must not be joined — see the note in
    /// `sample::for_input`.
    #[test]
    fn a_kafka_sample_reads_under_a_group_of_its_own() {
        let mut notes = Vec::new();
        let prepared = prepare(
            input(serde_json::json!({
                "type": "kafka",
                "connection": "prod",
                "topic": "orders",
                "group": "the-pipelines-group",
            })),
            &mut notes,
        );
        let InputKind::Kafka(kafka) = &prepared.kind else {
            panic!("still a kafka input");
        };
        assert_ne!(kafka.group, "the-pipelines-group");
        assert!(kafka.group.starts_with("kayak-sample-"));
        // the rest of the config is what the pipeline would use
        assert_eq!(kafka.topic, "orders");
    }
}
