use std::sync::Arc;

use crate::config::Config;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub mod api_docs;
pub mod columns;
pub mod config;
pub mod connections;
pub mod docs;
pub mod dry_run;
pub mod format;
pub mod history;
pub mod layout;
pub mod mapping;
pub mod metadata;
pub mod sample;
pub mod schema;
pub mod script;
pub mod sql;
pub mod server_config;
pub mod state;
pub mod streaming;

pub use columns::{ColumnMapping, ColumnType, ExtraFieldPolicy, MissingColumnPolicy, TableIndex};
pub use connections::{ConnectionId, ConnectionKind, Connections};
pub use format::{ConfigFormat, PipelineSource};
pub use history::{ErrorSignature, HistoryBucket, PipelineHistory, Resolution};
pub use layout::{EdgeEnd, LayoutFile, PipelineLayout, PortLayout, Side};
pub use schema::{InferredField, InferredType, MessageSchema, TextFormat};
pub use state::{PipelineState, StateBucketConfig, StateBuckets};

/// One pipeline as the API reports it: the id that it runs under, the config
/// that kayak built it from, and the status of its run loop.
#[derive(Serialize, Deserialize, Clone, JsonSchema)]
pub struct PipelineDto {
    pub id: String,
    pub config: Config,
    /// The status of the run loop. When it is absent, read it as `running`.
    #[serde(default)]
    pub status: RunStatus,
}

/// The status of the run loop of a pipeline.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, Default, PartialEq, Eq, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    /// The run loop started, and an output is not initialized yet. For
    /// example, the database of an output is not available. The pipeline tries
    /// again with a backoff. When the output initializes, the status changes
    /// to `running`.
    Starting,
    /// All outputs are initialized, and the loop runs. Messages move only in
    /// this status.
    #[default]
    Running,
    /// The loop stopped because it was cancelled, by a delete, a revert or a
    /// shutdown. This status is rare in a response, because kayak usually
    /// removes the pipeline at the same time.
    Stopped,
    /// The loop stopped because the last input failed. The pipeline sends
    /// nothing more until kayak builds it again.
    Failed,
}

impl RunStatus {
    /// The wire spelling, for anything that needs it as text — a badge, a log
    /// line. Same string `serde` produces.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Starting => "starting",
            Self::Running => "running",
            Self::Stopped => "stopped",
            Self::Failed => "failed",
        }
    }

    /// Whether this is the state a healthy pipeline is in. What the UI hangs
    /// "say something" off, so that a state added later is shown rather than
    /// silently treated as fine.
    #[must_use]
    pub fn is_running(self) -> bool {
        matches!(self, Self::Running)
    }
}

impl std::fmt::Display for RunStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The body of `POST /api/pipelines/{pipeline_id}/messages`: one message, or an
/// array of messages. An array is always many messages, not one message.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(untagged)]
pub enum IngestRequest {
    /// Many messages, sent as one batch.
    Many(Vec<serde_json::Value>),
    /// One message, sent as a batch of one.
    One(serde_json::Value),
}

impl IngestRequest {
    /// The messages, however they were spelled.
    #[must_use]
    pub fn into_messages(self) -> Vec<serde_json::Value> {
        match self {
            Self::Many(messages) => messages,
            Self::One(message) => vec![message],
        }
    }
}

/// The response to a post: the number of messages that the pipeline accepted.
///
/// Accepted means that the batch is in the queue of the run loop. It does not
/// mean that the outputs wrote the messages.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
pub struct IngestResponse {
    pub accepted: usize,
}

/// How the server started, and whether the running graph is the same as the
/// config file.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
pub struct SettingsDto {
    /// The name of the config file of the server: the `--config` file, or the
    /// file that a save made. When it is absent, there is no file yet. A save
    /// makes one.
    pub config_file: Option<String>,
    /// The directory that a save writes to. An empty value means that the
    /// directory is not known.
    #[serde(default)]
    pub save_directory: String,
    /// True when the running graph is different from the last load or save.
    /// A change has an immediate effect on the server, but does not change the
    /// file. A restart discards the unsaved changes.
    pub unsaved_changes: bool,
}

/// The body of `POST /api/config/save`. `name` is a file name with no path.
/// kayak writes the file to the save directory of the server.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
pub struct SaveConfigRequest {
    pub name: String,
    /// JSON or YAML. When it is absent, the extension of `name` sets the
    /// format.
    #[serde(default)]
    pub format: Option<ConfigFormat>,
    /// Whether the save can replace an existing file. The default is `true`.
    /// With `false`, the save only makes new files. If a file with the name
    /// exists, the response is a 409 and kayak writes nothing.
    #[serde(default = "overwrite_default")]
    pub overwrite: bool,
}

fn overwrite_default() -> bool {
    true
}

/// The path of the file that the save wrote.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
pub struct SaveConfigResponse {
    pub path: String,
}

/// The body of `POST /api/auth/login`.
///
/// The password is the real password, not a `${NAME}` reference. kayak does
/// not store it, send it back or write it to the log.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}

/// The body of `POST /api/auth/token`.
///
/// The token comes from the identity provider of the host application. kayak
/// changes it into a session cookie. kayak does not store it, send it back or
/// write it to the log.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct TokenLoginRequest {
    pub token: String,
}

/// The caller, and whether the server checks credentials.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
pub struct AuthDto {
    /// Whether the server checks credentials. It is `false` on a server
    /// started without `--server-config`, or with `auth: {type: none}`.
    pub authentication_required: bool,
    /// The signed-in user, or `null` for a caller with no credentials.
    pub username: Option<String>,
    /// The role of the caller. `null` means signed out. This is different
    /// from `read`: a `read` user can see the graph, and a signed-out caller
    /// cannot.
    pub role: Option<crate::server_config::Role>,
}

impl AuthDto {
    /// What a server that authenticates nobody says about every caller.
    #[must_use]
    pub fn open() -> Self {
        Self {
            authentication_required: false,
            username: None,
            role: None,
        }
    }

    /// Whether the caller may change the graph.
    ///
    /// The one place the "authentication is off" case and the "signed in as an
    /// admin" case are folded together, so that neither the navbar nor the
    /// canvas has to know there are two ways to be allowed. A server with no
    /// accounts hands everyone the edit button, which is what it did before
    /// roles existed.
    #[must_use]
    pub fn may_edit(&self) -> bool {
        !self.authentication_required
            || matches!(self.role, Some(crate::server_config::Role::Admin))
    }

    /// Whether the UI has to ask for credentials before it can show anything.
    #[must_use]
    pub fn needs_login(&self) -> bool {
        self.authentication_required && self.role.is_none()
    }
}

pub type PipelineId = String;
pub type MessageBatch = Vec<Arc<serde_json::Value>>;

/// The stage of the run loop that an event comes from.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, PartialEq, Eq, Hash, JsonSchema)]
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

/// How many of a batch's messages the feed carries. A batch can be thousands
/// of messages wide — a tumbling buffer over a busy subject is exactly that —
/// and the rest are counted rather than sent: a card shows a handful at a time,
/// and the count is what says how much it isn't showing.
pub const MESSAGES_PER_BATCH: usize = 100;

/// How much of one message the feed carries, in bytes. Enough to recognise a
/// payload, far short of what a card can render.
pub const MAX_MESSAGE_BYTES: usize = 2048;

/// A batch in the event feed: some of its messages as JSON text, and the
/// counts of the messages that the feed does not carry.
///
/// The feed carries a maximum of 100 messages for each batch. It cuts each
/// message to 2048 bytes.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Debug, JsonSchema)]
pub struct BatchPreview {
    /// The messages as compact JSON text. There are a maximum of 100. kayak
    /// cuts each one to 2048 bytes and adds `…` at the end of a cut message.
    pub messages: Vec<String>,
    /// The number of messages in the batch. It is larger than the length of
    /// `messages` when the batch has more than 100 messages.
    pub total: usize,
    /// The number of messages that passed this stage in passes that the feed
    /// **did not report**, since the last reported event.
    ///
    /// The feed is a sample. To calculate the throughput, add this number to
    /// `total`.
    #[serde(default)]
    pub skipped_messages: u64,
}

impl BatchPreview {
    /// Render a batch down to what the feed carries.
    #[must_use]
    pub fn of(batch: &MessageBatch, skipped_messages: u64) -> Self {
        Self {
            messages: batch
                .iter()
                .take(MESSAGES_PER_BATCH)
                .map(|message| truncate(&message.to_string()))
                .collect(),
            total: batch.len(),
            skipped_messages,
        }
    }

    /// How many messages the batch held that this preview doesn't carry.
    #[must_use]
    pub fn dropped(&self) -> usize {
        self.total.saturating_sub(self.messages.len())
    }

    /// What this event is worth to a throughput readout: its own messages plus
    /// everything the feed skipped to get here. Counting `total` alone would
    /// report a sampled fraction of what the pipeline is really doing.
    #[must_use]
    pub fn counted(&self) -> usize {
        let skipped = usize::try_from(self.skipped_messages).unwrap_or(usize::MAX);
        self.total.saturating_add(skipped)
    }
}

/// Cut `text` to [`MAX_MESSAGE_BYTES`], on a character boundary, marking that
/// it was cut. Strings that fit are returned unchanged.
#[must_use]
pub fn truncate(text: &str) -> String {
    if text.len() <= MAX_MESSAGE_BYTES {
        return text.to_string();
    }
    let mut end = MAX_MESSAGE_BYTES;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…", &text[..end])
}

/// The contents of an event: a batch that passed, or an error.
#[derive(Clone, Serialize, Deserialize, PartialEq, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum EventPayload {
    Batch(BatchPreview),
    /// A failure at this stage, as text. The event does not carry the batch.
    Error(String),
}

/// One event in the `/events` stream.
#[derive(Clone, Serialize, Deserialize, PartialEq, JsonSchema)]
pub struct UiEvent {
    pub pipeline_id: PipelineId,
    pub stage: Stage,
    /// The time of the event on the server clock, in milliseconds since the
    /// epoch. Zero means that the time is not known.
    #[serde(default)]
    pub ts: u64,
    /// The number of the pass through the run loop. A pass is one batch in, its
    /// transforms, and its outputs. The count is per pipeline and starts at 1.
    ///
    /// It is `null` for an event outside a pass. For example, an output that
    /// failed to initialize before the loop started, or an input that failed
    /// while the loop waited.
    ///
    /// A gap shows missed passes. For example, a change from 8 to 12 means
    /// that the client did not get three passes.
    #[serde(default)]
    pub seq: Option<u64>,
    /// The index of the component in the array of its stage in the config,
    /// from zero. For example, the second of two outputs is `1`.
    ///
    /// It is `null` when the index is not known. Input events have no index,
    /// because kayak merges the inputs before the run loop gets a batch.
    #[serde(default)]
    pub component: Option<usize>,
    pub payload: EventPayload,
}

impl UiEvent {
    /// Report a batch, cut down to what the feed carries. `skipped_messages` is
    /// what passed this stage since the last reported event — zero unless the
    /// throttle has been dropping passes.
    pub fn batch(
        pipeline_id: PipelineId,
        stage: Stage,
        batch: &MessageBatch,
        skipped_messages: u64,
    ) -> Self {
        Self {
            pipeline_id,
            stage,
            ts: 0,
            seq: None,
            component: None,
            payload: EventPayload::Batch(BatchPreview::of(batch, skipped_messages)),
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

    /// Whether this is a failure rather than a batch that went through.
    #[must_use]
    pub fn is_error(&self) -> bool {
        matches!(self.payload, EventPayload::Error(_))
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
    use super::{
        AuthDto, BatchPreview, EventPayload, MAX_MESSAGE_BYTES, MESSAGES_PER_BATCH, PipelineDto,
        RunStatus, Stage, UiEvent, truncate,
    };
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
    fn run_status_round_trips_through_its_wire_spelling() {
        for (status, spelling) in [
            (RunStatus::Starting, "starting"),
            (RunStatus::Running, "running"),
            (RunStatus::Stopped, "stopped"),
            (RunStatus::Failed, "failed"),
        ] {
            assert_eq!(serde_json::to_value(status).ok(), Some(json!(spelling)));
            assert_eq!(
                serde_json::from_value::<RunStatus>(json!(spelling)).ok(),
                Some(status)
            );
            assert_eq!(status.as_str(), spelling);
        }
    }

    /// A pipeline body from a server that predates the field reads as running
    /// — the state every reader assumed when there was nothing else it could
    /// be. See [`PipelineDto::status`].
    #[test]
    fn a_pipeline_without_a_status_reads_as_running() {
        let dto: PipelineDto = match serde_json::from_value(json!({
            "id": "witty-crab",
            "config": {"inputs": [], "transforms": [], "outputs": []},
        })) {
            Ok(dto) => dto,
            Err(e) => panic!("deserializing a status-less pipeline: {e}"),
        };
        assert_eq!(dto.status, RunStatus::Running);
        assert!(dto.status.is_running());
    }

    #[test]
    fn an_event_carries_the_time_it_was_stamped_with() {
        let event = UiEvent::batch(
            "witty-crab".to_string(),
            Stage::Output,
            &vec![Arc::new(json!({"n": 1}))],
            0,
        );

        assert_eq!(event.ts, 0, "unstamped until published");
        assert_eq!(event.at(1_754_573_021_220).ts, 1_754_573_021_220);
    }

    /// The cap is the reason the type exists: a batch wider than it must cross
    /// the wire as a preview plus a count, never whole.
    #[test]
    fn a_wide_batch_is_cut_down_to_the_cap_and_counted() {
        let batch: Vec<_> = (0..MESSAGES_PER_BATCH * 3)
            .map(|n| Arc::new(json!({ "n": n })))
            .collect();

        let preview = BatchPreview::of(&batch, 0);

        assert_eq!(preview.messages.len(), MESSAGES_PER_BATCH);
        assert_eq!(preview.total, MESSAGES_PER_BATCH * 3);
        assert_eq!(preview.dropped(), MESSAGES_PER_BATCH * 2);
    }

    #[test]
    fn a_batch_that_fits_drops_nothing() {
        let batch = vec![Arc::new(json!({"n": 1})), Arc::new(json!({"n": 2}))];

        let preview = BatchPreview::of(&batch, 0);

        assert_eq!(preview.total, 2);
        assert_eq!(preview.dropped(), 0);
        assert_eq!(preview.messages.len(), 2);
    }

    /// A long message is cut on a character boundary — slicing a multi-byte
    /// character in half would panic rather than produce a shorter string.
    #[test]
    fn a_long_message_is_cut_without_splitting_a_character() {
        let text = "é".repeat(MAX_MESSAGE_BYTES);

        let cut = truncate(&text);

        assert!(cut.len() <= MAX_MESSAGE_BYTES + "…".len());
        assert!(cut.ends_with('…'), "expected a marked cut, got {cut}");
    }

    #[test]
    fn a_short_message_is_left_alone() {
        assert_eq!(truncate("{\"n\":1}"), "{\"n\":1}");
    }

    /// The skip count is what keeps the throughput readout honest once the feed
    /// is being sampled, so it has to survive the round trip.
    #[test]
    fn a_preview_carries_what_the_feed_skipped() {
        let event = UiEvent::batch(
            "witty-crab".to_string(),
            Stage::Input,
            &vec![Arc::new(json!({"n": 1}))],
            4_096,
        );

        let Ok(json) = serde_json::to_string(&event) else {
            panic!("an event should serialize");
        };
        let Ok(round_tripped) = serde_json::from_str::<UiEvent>(&json) else {
            panic!("an event should round trip");
        };
        let EventPayload::Batch(preview) = round_tripped.payload else {
            panic!("expected a batch payload");
        };

        assert_eq!(preview.skipped_messages, 4_096);
        assert_eq!(preview.total, 1);
    }

    /// An older server sends no `skipped_messages`; that has to read as "nothing
    /// was skipped" rather than fail the whole event.
    #[test]
    fn a_preview_without_a_skip_count_still_parses() {
        let Ok(preview) = serde_json::from_value::<BatchPreview>(json!({
            "messages": ["{\"n\":1}"],
            "total": 1,
        })) else {
            panic!("a preview without `skipped_messages` should still parse");
        };

        assert_eq!(preview.skipped_messages, 0);
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

    /// A server with no accounts hands everyone the edit button, which is what
    /// it did before roles existed. The one place the "authentication is off"
    /// case and the "signed in as an admin" case are folded together.
    #[test]
    fn an_open_server_lets_everybody_edit_and_asks_nobody_to_log_in() {
        let open = AuthDto::open();
        assert!(open.may_edit());
        assert!(!open.needs_login());
    }

    #[test]
    fn a_reader_may_look_but_not_edit() {
        let reader = AuthDto {
            authentication_required: true,
            username: Some("watcher".to_string()),
            role: Some(crate::server_config::Role::Read),
        };
        assert!(!reader.may_edit());
        assert!(!reader.needs_login(), "a reader is signed in");
    }

    #[test]
    fn an_admin_may_edit() {
        let admin = AuthDto {
            authentication_required: true,
            username: Some("root".to_string()),
            role: Some(crate::server_config::Role::Admin),
        };
        assert!(admin.may_edit());
        assert!(!admin.needs_login());
    }

    /// The state the login page exists for: this server asks, and nobody has
    /// answered yet.
    #[test]
    fn a_guarded_server_with_no_session_needs_a_login() {
        let anonymous = AuthDto {
            authentication_required: true,
            username: None,
            role: None,
        };
        assert!(anonymous.needs_login());
        assert!(!anonymous.may_edit());
    }
}
