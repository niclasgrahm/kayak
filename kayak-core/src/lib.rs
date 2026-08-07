use std::sync::Arc;

use crate::config::Config;
use serde::{Deserialize, Serialize};

pub mod config;
pub mod connections;
pub mod docs;
pub mod format;
pub mod layout;

pub use connections::{ConnectionId, ConnectionKind, Connections};
pub use format::ConfigFormat;
pub use layout::{EdgeEnd, LayoutFile, PipelineLayout, PortLayout, Side};

#[derive(Serialize, Deserialize, Clone)]
pub struct PipelineDto {
    pub id: String,
    pub config: Config,
}

/// How the server was started, and whether what it is running still matches
/// the file it started from.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct SettingsDto {
    /// Name of the config file the server is working against — the `--config`
    /// one, or the one a save has since created. Its absence doesn't mean edits
    /// can't be saved: it means there is no file *yet*, so the UI offers to
    /// create one rather than to overwrite one.
    pub config_file: Option<String>,
    /// The directory a save writes into. Shown so "create a config file" can
    /// say where the file will appear, which is the one thing the file name on
    /// its own doesn't tell you.
    ///
    /// Defaults to empty when a client is talking to an older server, which
    /// reads the same as "unknown" — the UI just leaves the location out.
    #[serde(default)]
    pub save_directory: String,
    /// The running graph has diverged from what was last loaded or saved.
    /// Edits apply to the runtime immediately and the file is left alone, so
    /// without this the divergence would be invisible until a restart lost it.
    pub unsaved_changes: bool,
}

/// What `POST /api/config/save` takes: a bare file name, saved beside the
/// config the server was started from. Not a path — see `persist::save_path`.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct SaveConfigRequest {
    pub name: String,
    /// JSON or YAML. Omitted means "whatever `name` says it is", which is what
    /// keeps a client that predates the choice — and a hand-written `curl` —
    /// writing the format the file is named for.
    #[serde(default)]
    pub format: Option<ConfigFormat>,
}

/// Where a save actually landed, so the UI can name it rather than guess.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct SaveConfigResponse {
    pub path: String,
}

pub type PipelineId = String;
pub type MessageBatch = Vec<Arc<serde_json::Value>>;

/// The stage of a run loop an event came from. Also what the frontend matches
/// on to decide whether an edge lights up and which badge a log line gets, so
/// it is a type rather than a string: both ends match on it exhaustively, and a
/// fourth stage would fail to compile at every site that has to handle it.
///
/// The serialized spellings are wire format — `/events` carries them and the
/// frontend's filter chips are named after them. `stage_round_trips` pins them.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum Stage {
    Input,
    Transform,
    Output,
}

impl Stage {
    /// The wire spelling, for anything that needs it as text — a log badge, an
    /// error message. Same string `serde` produces.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Input => "input",
            Self::Transform => "transform",
            Self::Output => "output",
        }
    }
}

impl std::fmt::Display for Stage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// What a run loop is reporting: a batch that passed through, or something that
/// went wrong while handling one.
#[derive(Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum EventPayload {
    Batch(Arc<MessageBatch>),
    /// A failure at this stage. The batch that caused it is not carried: a
    /// transform that failed has no output to show, and the input that did
    /// arrive was already reported by its own event.
    Error(String),
}

#[derive(Clone, Serialize, Deserialize, PartialEq)]
pub struct UiEvent {
    pub pipeline_id: PipelineId,
    pub stage: Stage,
    /// When the run loop reported this, in milliseconds since the epoch.
    ///
    /// The *server's* clock, stamped where the event is published rather than
    /// where it is built: this type compiles for wasm, where `SystemTime::now`
    /// panics. Zero means "no time" — an event from a server that predates the
    /// field, which the log renders as blank rather than as 1970.
    #[serde(default)]
    pub ts: u64,
    /// Which pass through the run loop this belongs to — one batch in, its
    /// transforms, and everything that left. Counted per pipeline from one.
    ///
    /// `None` for anything that happened outside a pass: an output that failed
    /// to initialise before the loop started, or an input source dying in its
    /// own task while the loop waits. Those are real events with no pass to
    /// belong to, not a missing number.
    ///
    /// The frontend groups the log by this, and a *gap* in it is information
    /// too: the UI feed is a broadcast channel that drops rather than blocks,
    /// so a jump from 8 to 12 is three passes the browser never saw and should
    /// say so instead of drawing the survivors as if they were consecutive.
    #[serde(default)]
    pub seq: Option<u64>,
    /// Which component of the stage, indexed into that stage's array in the
    /// config — the second of two outputs is `Some(1)`.
    ///
    /// `None` where it isn't known rather than where there is only one: input
    /// batches carry no index because several inputs are merged before the run
    /// loop sees them, and by then which one produced the batch is gone.
    #[serde(default)]
    pub component: Option<usize>,
    pub payload: EventPayload,
}

impl UiEvent {
    pub fn batch(pipeline_id: PipelineId, stage: Stage, batch: Arc<MessageBatch>) -> Self {
        Self {
            pipeline_id,
            stage,
            ts: 0,
            seq: None,
            component: None,
            payload: EventPayload::Batch(batch),
        }
    }

    /// `error` is rendered with `{:#}`, so an `anyhow` chain arrives as the
    /// same "context: cause" line the server log shows.
    pub fn error(pipeline_id: PipelineId, stage: Stage, error: &impl std::fmt::Display) -> Self {
        Self {
            pipeline_id,
            stage,
            ts: 0,
            seq: None,
            component: None,
            payload: EventPayload::Error(format!("{error:#}")),
        }
    }

    /// Stamp the event with a wall-clock time. Called by the publisher, which
    /// is the one place in the server that reads a clock — see `events::publish`.
    #[must_use]
    pub fn at(mut self, ts: u64) -> Self {
        self.ts = ts;
        self
    }

    /// Attach the run-loop pass this came from. See [`UiEvent::seq`].
    #[must_use]
    pub fn seq(mut self, seq: u64) -> Self {
        self.seq = Some(seq);
        self
    }

    /// Attach which component of the stage this came from. See
    /// [`UiEvent::component`].
    #[must_use]
    pub fn component(mut self, component: usize) -> Self {
        self.component = Some(component);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::{EventPayload, Stage, UiEvent};
    use serde_json::json;
    use std::sync::Arc;

    /// The spellings are wire format: `/events` carries them, and the frontend
    /// matches on them after a round trip through JSON. Renaming a variant must
    /// not silently rename what goes over the socket.
    #[test]
    fn stage_round_trips_through_its_wire_spelling() {
        for (stage, spelling) in [
            (Stage::Input, "input"),
            (Stage::Transform, "transform"),
            (Stage::Output, "output"),
        ] {
            assert_eq!(serde_json::to_value(stage).ok(), Some(json!(spelling)));
            assert_eq!(
                serde_json::from_value::<Stage>(json!(spelling)).ok(),
                Some(stage)
            );
            assert_eq!(stage.as_str(), spelling);
        }
    }

    #[test]
    fn an_event_carries_the_time_it_was_stamped_with() {
        let event = UiEvent::batch(
            "witty-crab".to_string(),
            Stage::Output,
            Arc::new(vec![Arc::new(json!({"n": 1}))]),
        );

        assert_eq!(event.ts, 0, "unstamped until published");
        assert_eq!(event.at(1_754_573_021_220).ts, 1_754_573_021_220);
    }

    /// A frontend built against a newer core must still read a server that
    /// predates `ts`, which is what the `serde(default)` is for — the log shows
    /// no time rather than failing to parse the event at all.
    #[test]
    fn an_event_without_a_timestamp_still_parses() {
        let Ok(event) = serde_json::from_value::<UiEvent>(json!({
            "pipeline_id": "witty-crab",
            "stage": "input",
            "payload": {"error": "upstream went away"},
        })) else {
            panic!("an event without `ts` should still parse");
        };

        assert_eq!(event.ts, 0);
        assert_eq!(event.stage, Stage::Input);
        assert!(matches!(event.payload, EventPayload::Error(_)));
    }
}
