//! Fetching a few real messages from an input that isn't a pipeline yet.
//!
//! The point of it is the form: configuring a stream you cannot see is
//! guesswork, and every field reference downstream — a column's `field`, a
//! filter's comparison — is a name someone had to already know. A handful of
//! actual messages turns that into reading. What is done with them afterwards
//! is [`crate::schema`]'s job; this module is the declaration and the wire
//! shapes, and `kayak::handlers::rest::sample` is the half that talks to a
//! broker.
//!
//! # Sampling is not free, and each input has to say so
//!
//! The reason this is a declaration rather than a loop over `InputSource`:
//! reading a message means *being a consumer*, and for some brokers a consumer
//! is a thing with consequences for the pipeline already running. A kafka
//! consumer joining a group rebalances it and commits offsets; an MQTT client
//! reconnecting under an existing client id disconnects the client that was
//! there. So [`for_input`] answers "what would sampling this cost", per kind,
//! and `every_input_says_whether_it_can_be_sampled` in [`crate::docs`] fails
//! for a kind that hasn't answered — the same bargain
//! [`crate::metadata::for_input`] makes.
//!
//! [`Sampling::Adjusted`] is the interesting arm and the one to reach for when
//! adding an input: it means *sampling is safe, but only because the sampler
//! changes something*, and the change is named so the UI can say what it did.
//! A kafka sample runs under a throwaway group id precisely so that committing
//! is harmless; a note saying so is the difference between a sample the user
//! can reason about and a magic button.
//!
//! # What a sample is not
//!
//! It is not a subscription and it is not a preview of the pipeline: it takes
//! at most [`MAX_MESSAGES`] messages inside at most [`MAX_TIMEOUT_MS`] and
//! then drops the input. Nothing is kept, nothing is published, and an empty
//! result is an ordinary answer — a quiet subject has nothing to show, and
//! saying "nothing arrived in five seconds" is more useful than waiting
//! forever for something that would say the same thing.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::config::InputConfig;

/// How many messages a sample takes when the request doesn't say.
///
/// Enough to tell a field that is always there from one that is sometimes
/// there, which is the question [`crate::schema`] exists to answer, and few
/// enough that a slow stream still answers quickly.
pub const DEFAULT_MAX_MESSAGES: usize = 5;

/// The most any one sample will take. A bound rather than a limit anyone
/// should reach: past a handful this stops being a look at the stream and
/// starts being a consumer of it.
pub const MAX_MESSAGES: usize = 50;

/// How long a sample waits when the request doesn't say.
pub const DEFAULT_TIMEOUT_MS: u64 = 5_000;

/// The longest any one sample will wait. It holds an HTTP request open and a
/// connection to somebody's broker, so it is bounded here rather than by the
/// caller's patience.
pub const MAX_TIMEOUT_MS: u64 = 30_000;

/// What sampling an input of some kind would involve.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(tag = "sampling", rename_all = "snake_case")]
pub enum Sampling {
    /// The input can be built and read exactly as configured, and doing so
    /// costs the running system nothing: it subscribes, takes what arrives and
    /// goes away.
    Ready,
    /// The same, but only because the sampler changed something first. The
    /// note says what, in the user's words rather than the implementation's —
    /// it is shown beside the messages.
    Adjusted { note: String },
    /// Sampling this kind is refused, and the reason says what to do instead.
    /// A refusal is not a gap to be filled in later by trying harder: an
    /// `http` input has nothing to fetch *from*, because it is the thing being
    /// posted to.
    Refused { reason: String },
}

impl Sampling {
    /// Whether a sample can be taken at all — the question a button asks.
    #[must_use]
    pub fn allowed(&self) -> bool {
        !matches!(self, Self::Refused { .. })
    }

    /// What the user should be told before or beside the messages, if
    /// anything.
    #[must_use]
    pub fn note(&self) -> Option<&str> {
        match self {
            Self::Ready => None,
            Self::Adjusted { note } | Self::Refused { reason: note } => Some(note),
        }
    }

    fn adjusted(note: &str) -> Self {
        Self::Adjusted {
            note: note.to_string(),
        }
    }

    fn refused(reason: &str) -> Self {
        Self::Refused {
            reason: reason.to_string(),
        }
    }
}

/// What sampling an input of this kind involves, or `None` for a kind that has
/// not said — which is a kind that has not been added here, and which the test
/// in [`crate::docs`] fails for.
///
/// Keyed by the kind's wire name for the reason [`crate::metadata::for_input`]
/// is: the form holds a draft, not a typed config, and the button has to know
/// before anything is built.
#[must_use]
pub fn for_input(kind: &str) -> Option<Sampling> {
    Some(match kind {
        // Nothing to reach: it generates its own messages, so a sample is
        // exactly what the pipeline would see.
        "dummy" => Sampling::Ready,
        // Both are fan-out subscriptions: a second subscriber gets a copy and
        // costs the first one nothing. What they cannot do is show a message
        // published before the sample started — there is no replay in either,
        // so a quiet subject samples empty however long it waits.
        "nats" | "redis" => Sampling::Ready,
        // A subscription with a monitored item per node, the same as the
        // pipeline's. Reading a value does not consume it.
        "opcua" => Sampling::Ready,
        // A tap on another pipeline's output, and a fan-out like the two
        // above: every downstream gets every batch, so listening in takes
        // nothing away from the ones already there. Like them it starts from
        // now, so an upstream that is between messages samples empty.
        "pipeline" => Sampling::Ready,
        // A server-sent-events subscription, fanned out per subscriber like
        // nats: the pipeline's own stream is untouched. Better than the
        // fan-outs above, the platform backfills each series' latest reading
        // on subscribe, so a quiet sensor still samples its last value.
        "indu" => Sampling::Ready,
        // A query, and the one kind of input that *can* show what was
        // published before the sample started: the rows are still in the
        // table. Reading them takes nothing away from a running pipeline — a
        // second reader is a second query. When the input starts from the
        // newest rows the sampler starts it from the oldest instead, and says
        // so at the time, since a quiet table would otherwise sample empty
        // with a table full of rows behind it.
        "postgres" | "clickhouse" => Sampling::Ready,
        // A consumer group is shared state: joining the pipeline's would
        // rebalance it and commit offsets on its behalf, i.e. take messages
        // away from the thing it is supposed to be showing.
        "kafka" => Sampling::adjusted(
            "read under a throwaway consumer group, so the pipeline's own \
             offsets are untouched — which also means the sample starts \
             where this input's `start` says rather than where the pipeline \
             has got to",
        ),
        // A broker allows one connection per client id and disconnects the
        // older one, so sampling under the id a running pipeline uses would
        // knock that pipeline off its broker.
        "mqtt" => Sampling::adjusted("read under a client id of its own, so a running pipeline keeps its connection"),
        // It receives; there is nothing to fetch from. Sampling it would mean
        // claiming its endpoint, which is the running pipeline's.
        "http" => Sampling::refused(
            "an http input is posted to rather than read from — create the \
             pipeline and post a message to its endpoint",
        ),
        _ => return None,
    })
}

/// Take a few messages from an input, without creating a pipeline.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[schemars(title = "sample request")]
pub struct SampleRequest {
    /// the input to read from, exactly as it would be written in a pipeline —
    /// including its `envelope`, so the metadata fields the messages will
    /// really carry are in the sample too.
    pub input: InputConfig,
    /// how many messages to take. Defaults to [`DEFAULT_MAX_MESSAGES`], capped
    /// at [`MAX_MESSAGES`].
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_messages: Option<usize>,
    /// how long to wait for them, in milliseconds. Defaults to
    /// [`DEFAULT_TIMEOUT_MS`], capped at [`MAX_TIMEOUT_MS`]. The wait ends as
    /// soon as enough messages have arrived.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
}

impl SampleRequest {
    /// How many messages this request actually gets, within the bound.
    #[must_use]
    pub fn messages_wanted(&self) -> usize {
        self.max_messages
            .unwrap_or(DEFAULT_MAX_MESSAGES)
            .clamp(1, MAX_MESSAGES)
    }

    /// How long it actually waits, within the bound.
    #[must_use]
    pub fn wait_ms(&self) -> u64 {
        self.timeout_ms.unwrap_or(DEFAULT_TIMEOUT_MS).min(MAX_TIMEOUT_MS)
    }
}

/// Where a sample went wrong, when it did.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum SampleStage {
    /// The input could not be built at all — a connection that isn't
    /// configured, a secret that doesn't resolve. The same failure creating
    /// the pipeline would have given, which is the point of building the real
    /// thing.
    Build,
    /// It was built, and reading from it failed — the broker refused the
    /// subscription, the host is unreachable.
    Read,
}

/// What a sample found.
///
/// Note there is no separate "nothing arrived" arm: an empty `messages` is
/// exactly that, and it is an ordinary answer rather than a failure. A stream
/// nobody is publishing to is a real state of the world, and one the user
/// often needs to be shown rather than protected from.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema)]
#[serde(tag = "outcome", rename_all = "snake_case")]
#[schemars(title = "sample response")]
pub enum SampleResponse {
    Sampled {
        /// the messages, exactly as the pipeline's first transform would have
        /// seen them.
        messages: Vec<Value>,
        /// what the sample did differently from the pipeline it is standing in
        /// for — a throwaway consumer group, an ignored buffer. Empty when it
        /// did nothing differently.
        #[serde(default, skip_serializing_if = "Vec::is_empty")]
        notes: Vec<String>,
        /// how long it waited, in milliseconds.
        waited_ms: u64,
    },
    Failed {
        stage: SampleStage,
        message: String,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_request_that_says_nothing_gets_the_defaults() {
        let request = SampleRequest {
            input: serde_json::from_value(serde_json::json!({"type": "dummy", "duration": 1}))
                .expect("a dummy input"),
            max_messages: None,
            timeout_ms: None,
        };
        assert_eq!(request.messages_wanted(), DEFAULT_MAX_MESSAGES);
        assert_eq!(request.wait_ms(), DEFAULT_TIMEOUT_MS);
    }

    #[test]
    fn a_request_is_bounded_at_both_ends() {
        let mut request = SampleRequest {
            input: serde_json::from_value(serde_json::json!({"type": "dummy", "duration": 1}))
                .expect("a dummy input"),
            max_messages: Some(10_000),
            timeout_ms: Some(10 * MAX_TIMEOUT_MS),
        };
        assert_eq!(request.messages_wanted(), MAX_MESSAGES);
        assert_eq!(request.wait_ms(), MAX_TIMEOUT_MS);
        // and a nonsensical zero is one message rather than a request that
        // does nothing at all
        request.max_messages = Some(0);
        assert_eq!(request.messages_wanted(), 1);
    }

    #[test]
    fn a_refusal_is_not_allowed_and_says_why() {
        let http = for_input("http").expect("the http input has an answer");
        assert!(!http.allowed());
        assert!(http.note().is_some_and(|reason| reason.contains("posted to")));
    }

    #[test]
    fn an_adjustment_is_allowed_and_says_what_it_changed() {
        let kafka = for_input("kafka").expect("the kafka input has an answer");
        assert!(kafka.allowed());
        assert!(kafka.note().is_some());
        assert!(for_input("nats").expect("the nats input has an answer").note().is_none());
    }

    #[test]
    fn an_input_nobody_has_thought_about_has_no_answer() {
        assert_eq!(for_input("something-new"), None);
    }
}
