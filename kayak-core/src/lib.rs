use std::sync::Arc;

use crate::config::Config;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

pub mod api_docs;
pub mod columns;
pub mod config;
pub mod connections;
pub mod docs;
pub mod format;
pub mod history;
pub mod layout;
pub mod mapping;
pub mod metadata;
pub mod script;
pub mod server_config;
pub mod state;

pub use columns::{ColumnMapping, ColumnType, ExtraFieldPolicy, MissingColumnPolicy, TableIndex};
pub use connections::{ConnectionId, ConnectionKind, Connections};
pub use format::ConfigFormat;
pub use history::{ErrorSignature, HistoryBucket, PipelineHistory, Resolution};
pub use layout::{EdgeEnd, LayoutFile, PipelineLayout, PortLayout, Side};
pub use state::{PipelineState, StateBucketConfig, StateBuckets};

/// One pipeline as the API reports it: the id it is running under, and the
/// config it was built from.
///
/// The same wire shape the run loop's `PipelineView` serializes to — this is
/// the owned spelling of it, and the one the schema is generated from.
#[derive(Serialize, Deserialize, Clone, JsonSchema)]
pub struct PipelineDto {
    pub id: String,
    pub config: Config,
}

/// What `POST /api/pipelines/{id}/messages` takes: one message, or an array of
/// them.
///
/// Untagged, and the array arm comes first on purpose — a JSON array would
/// otherwise deserialize as [`IngestRequest::One`] holding an array, and posting
/// ten messages would put one message into the pipeline. There is no envelope
/// around the messages because there is nothing to put in one: the pipeline is
/// named by the path, and kayak has no schema to declare.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
#[serde(untagged)]
pub enum IngestRequest {
    /// Several messages, delivered as one batch.
    Many(Vec<serde_json::Value>),
    /// A single message, delivered as a batch of one.
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

/// What came back from a post: how many messages were handed to the pipeline.
///
/// It says *accepted*, not *processed* — the batch is queued for the run loop
/// and the response doesn't wait for it, so a 202 means the pipeline has the
/// messages, not that the outputs have written them.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
pub struct IngestResponse {
    pub accepted: usize,
}

/// How the server was started, and whether what it is running still matches
/// the file it started from.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
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
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
pub struct SaveConfigRequest {
    pub name: String,
    /// JSON or YAML. Omitted means "whatever `name` says it is", which is what
    /// keeps a client that predates the choice — and a hand-written `curl` —
    /// writing the format the file is named for.
    #[serde(default)]
    pub format: Option<ConfigFormat>,
}

/// Where a save actually landed, so the UI can name it rather than guess.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
pub struct SaveConfigResponse {
    pub path: String,
}

/// What `POST /api/auth/login` takes.
///
/// The password is a plain `String` and not a
/// [`Secret`](crate::config::Secret), which is the opposite of every other
/// password field in kayak and deliberately so: a `Secret` holds a `${NAME}`
/// *reference* to a credential, and this is the credential itself, typed into a
/// login box a moment ago. It exists for the length of one request and is never
/// stored, serialized back or logged.
#[derive(Serialize, Deserialize, Clone, Debug, JsonSchema)]
pub struct LoginRequest {
    pub username: String,
    pub password: String,
}

/// Who the caller is, and whether this server cares.
///
/// The frontend asks for this before it draws anything: it decides between the
/// login page and the canvas, and between a canvas that can be edited and one
/// that can only be read.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq, JsonSchema)]
pub struct AuthDto {
    /// Whether this server checks credentials at all. `false` is a server
    /// started without a `--server-config`, or with one that sets
    /// `auth: {type: none}` — see [`crate::server_config`] for why that is the
    /// default.
    pub authentication_required: bool,
    /// The signed-in user, or `None` for a caller who presented nothing.
    pub username: Option<String>,
    /// What the caller may do. `None` means signed out — which is a different
    /// thing from [`Role::Read`], and worth keeping different: a reader may see
    /// the graph, and a signed-out caller may not.
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

/// The stage of a run loop an event came from. Also what the frontend matches
/// on to decide whether an edge lights up and which badge a log line gets, so
/// it is a type rather than a string: both ends match on it exhaustively, and a
/// fourth stage would fail to compile at every site that has to handle it.
///
/// The serialized spellings are wire format — `/events` carries them and the
/// frontend's filter chips are named after them. `stage_round_trips` pins them.
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

/// A batch as the UI feed carries it: a few of its messages, already rendered
/// and cut to size, plus the counts that say what was left out.
///
/// **The truncation happens on the server**, which is the whole point of the
/// type. An earlier version sent `Arc<MessageBatch>` — the entire batch — and
/// left the browser to throw all but a hundred of them away, so a wide batch
/// was serialized whole, pushed across the wire whole and parsed whole before
/// anything decided it wasn't wanted. At a kafka-shaped 50k messages a second
/// that measured 22 MB/s of JSON nobody ever read.
#[derive(Clone, Serialize, Deserialize, PartialEq, Eq, Debug, JsonSchema)]
pub struct BatchPreview {
    /// Compact JSON, at most [`MESSAGES_PER_BATCH`] of them, each cut to
    /// [`MAX_MESSAGE_BYTES`]. Compact rather than pretty because this is what a
    /// collapsed row shows; expanding one re-parses it.
    pub messages: Vec<String>,
    /// How many messages the batch actually held. Larger than `messages` is
    /// long whenever the batch was wider than the cap.
    pub total: usize,
    /// Messages that passed this stage in passes the feed **did not report**,
    /// counted since the last one it did — see `kayak::pipeline::UiThrottle`.
    ///
    /// It exists so the throughput readout stays honest. The feed is sampled
    /// under load, so counting only the batches that arrive would report a
    /// fraction of what the pipeline is really doing, and a card reading `40/s`
    /// under a pipeline running at 40,000 says the wrong thing more loudly than
    /// no number at all would.
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

/// What a run loop is reporting: a batch that passed through, or something that
/// went wrong while handling one.
#[derive(Clone, Serialize, Deserialize, PartialEq, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum EventPayload {
    Batch(BatchPreview),
    /// A failure at this stage. The batch that caused it is not carried: a
    /// transform that failed has no output to show, and the input that did
    /// arrive was already reported by its own event.
    Error(String),
}

#[derive(Clone, Serialize, Deserialize, PartialEq, JsonSchema)]
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
        AuthDto, BatchPreview, EventPayload, MAX_MESSAGE_BYTES, MESSAGES_PER_BATCH, Stage, UiEvent,
        truncate,
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
