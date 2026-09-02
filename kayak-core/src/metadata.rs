//! What each input knows about a message besides the message.
//!
//! This is a **declaration, not prose**. The metadata an input attaches is
//! listed here, `/docs` renders it under that input alongside its fields, and
//! [`for_input`] returning `None` is what fails
//! `every_input_declares_its_metadata` in [`crate::docs`] — so an input added
//! without saying what it attaches doesn't get past the test suite. It is the
//! same bargain the component reference already makes with doc comments: the
//! documentation is the source, so it cannot rot.
//!
//! The fields are attached *in band*, as ordinary fields on the message, under
//! whatever [`crate::config::EnvelopeConfig`] names — `_meta` by default. That
//! is why nothing here needs a type: everything a transform can do to a field
//! it can do to these.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// One metadata field an input attaches, and what it holds.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MetaFieldDoc {
    /// The field's name inside the metadata object.
    pub name: String,
    pub description: String,
}

impl MetaFieldDoc {
    fn new(name: &str, description: &str) -> Self {
        Self {
            name: name.to_string(),
            description: description.to_string(),
        }
    }
}

/// The fields every input attaches, whatever its kind.
fn common() -> Vec<MetaFieldDoc> {
    vec![
        MetaFieldDoc::new("pipeline", "id of the pipeline that read the message"),
        MetaFieldDoc::new("input", "kind of input it was read by, e.g. `nats`"),
        MetaFieldDoc::new(
            "received_at",
            "when kayak read it, RFC 3339. This is an arrival time and not an \
             event time: it says when the message reached this pipeline, not \
             when whatever it describes happened.",
        ),
    ]
}

/// Every metadata field an input of this kind attaches, common ones first.
///
/// `None` for a kind that hasn't declared any — which is a kind that has not
/// been added here, since even an input with nothing of its own to say gets the
/// common fields. That is what the test in [`crate::docs`] checks.
#[must_use]
pub fn for_input(kind: &str) -> Option<Vec<MetaFieldDoc>> {
    let own: Vec<MetaFieldDoc> = match kind {
        "dummy" => Vec::new(),
        "http" => vec![
            MetaFieldDoc::new("method", "http method the messages were posted with"),
            MetaFieldDoc::new(
                "remote_addr",
                "address the request came from, when the server can see one",
            ),
            MetaFieldDoc::new(
                "headers",
                "request headers, **restricted to a fixed list** — \
                 `content-type`, `user-agent`, `x-request-id`, \
                 `x-correlation-id` and `traceparent`. Everything else is \
                 dropped rather than passed on: a header carrying a \
                 credential (`authorization`, `x-api-key`) written into a \
                 file or an object store is a leak that outlives the request \
                 by years, and no allow-list-by-prefix is safe enough to \
                 offer instead.",
            ),
        ],
        "kafka" => vec![
            MetaFieldDoc::new(
                "connection",
                "name of the connection it was consumed through",
            ),
            MetaFieldDoc::new("topic", "topic the record came from"),
            MetaFieldDoc::new("partition", "partition within that topic"),
            MetaFieldDoc::new(
                "offset",
                "the record's offset in the partition. Together with `topic` \
                 and `partition` this identifies the record exactly.",
            ),
            MetaFieldDoc::new(
                "key",
                "the record's key as a string, or `null` when it was sent \
                 without one",
            ),
            MetaFieldDoc::new(
                "timestamp",
                "the record's own timestamp, RFC 3339, when kafka reports one",
            ),
        ],
        "nats" => vec![
            MetaFieldDoc::new("connection", "name of the connection it was received on"),
            MetaFieldDoc::new(
                "subject",
                "the subject this message was published to. This is the \
                 *concrete* subject rather than the pattern subscribed to, \
                 which is what makes a wildcard subscription usable: \
                 subscribe to `*.temperature` and the machine's name is here.",
            ),
            MetaFieldDoc::new(
                "reply",
                "the reply subject, when the publisher set one, else `null`",
            ),
            MetaFieldDoc::new("headers", "nats headers, as an object of arrays"),
        ],
        "indu" => vec![
            MetaFieldDoc::new("connection", "name of the connection it was read through"),
            MetaFieldDoc::new(
                "event",
                "which platform event carried it: `reading` for a sensor, \
                 `stream_reading` for a stream",
            ),
        ],
        "mqtt" => vec![
            MetaFieldDoc::new("connection", "name of the connection it was received on"),
            MetaFieldDoc::new(
                "topic",
                "the concrete topic this message arrived on — useful when the \
                 input subscribes to a filter containing `+` or `#` wildcards",
            ),
            MetaFieldDoc::new("qos", "the quality of service it was delivered at"),
            MetaFieldDoc::new(
                "retain",
                "whether the broker sent this as a topic's retained message \
                 rather than a live publish",
            ),
        ],
        "redis" => vec![
            MetaFieldDoc::new("connection", "name of the connection it was received on"),
            MetaFieldDoc::new("channel", "the channel this message was published to"),
        ],
        "opcua" => vec![MetaFieldDoc::new(
            "connection",
            "name of the connection the session was opened through. Which \
             *node* the reading came from is deliberately not here: it is on \
             the message itself, as `node` and `name`, because a value without \
             its tag is not a reading and metadata is opt-in.",
        )],
        "pipeline" => vec![MetaFieldDoc::new(
            "upstream",
            "id of the pipeline this batch came from. Note that metadata \
             already attached upstream arrives with the message and is not \
             replaced — being in band, it flows through the graph like any \
             other field.",
        )],
        _ => return None,
    };

    let mut fields = common();
    fields.extend(own);
    Some(fields)
}
