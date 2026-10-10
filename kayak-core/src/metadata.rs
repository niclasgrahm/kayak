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

/// One metadata field that an input attaches, and its contents.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct MetaFieldDoc {
    /// The name of the field in the metadata object.
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
        MetaFieldDoc::new("pipeline", "The id of the pipeline that read the message."),
        MetaFieldDoc::new("input", "The type of the input that read the message, for example `nats`."),
        MetaFieldDoc::new(
            "received_at",
            "The time when kayak read the message, as RFC 3339. This is the \
             arrival time at this pipeline. It is not the time of the event \
             in the message.",
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
            MetaFieldDoc::new("method", "The HTTP method of the request."),
            MetaFieldDoc::new(
                "remote_addr",
                "The address that sent the request, when the server knows it.",
            ),
            MetaFieldDoc::new(
                "headers",
                "The request headers from a **fixed list**: `content-type`, \
                 `user-agent`, `x-request-id`, `x-correlation-id` and \
                 `traceparent`. The input drops all other headers. Thus, a \
                 credential header such as `authorization` or `x-api-key` \
                 never goes into the message.",
            ),
        ],
        "kafka" => vec![
            MetaFieldDoc::new(
                "connection",
                "The name of the connection that the input consumed from.",
            ),
            MetaFieldDoc::new("topic", "The topic of the record."),
            MetaFieldDoc::new("partition", "The partition of the record in the topic."),
            MetaFieldDoc::new(
                "offset",
                "The offset of the record in the partition. With `topic` and \
                 `partition`, it identifies the record.",
            ),
            MetaFieldDoc::new(
                "key",
                "The key of the record as a string, or `null` when the record \
                 has no key.",
            ),
            MetaFieldDoc::new(
                "timestamp",
                "The timestamp of the record, as RFC 3339, when kafka gives one.",
            ),
        ],
        "nats" => vec![
            MetaFieldDoc::new("connection", "The name of the connection that received the message."),
            MetaFieldDoc::new(
                "subject",
                "The subject that the message was published to. This is the \
                 full subject, not the subscription pattern. For example, \
                 when you subscribe to `*.temperature`, this field holds the \
                 name of the machine.",
            ),
            MetaFieldDoc::new(
                "reply",
                "The reply subject, when the publisher set one. Otherwise `null`.",
            ),
            MetaFieldDoc::new("headers", "The nats headers, as an object of arrays."),
        ],
        "indu" => vec![
            MetaFieldDoc::new("connection", "The name of the connection that the input read from."),
            MetaFieldDoc::new(
                "event",
                "The platform event of the message: `reading` for a sensor, \
                 `stream_reading` for a stream.",
            ),
        ],
        "mqtt" => vec![
            MetaFieldDoc::new("connection", "The name of the connection that received the message."),
            MetaFieldDoc::new(
                "topic",
                "The full topic of the message. Use it when the input \
                 subscribes to a filter with `+` or `#` wildcards.",
            ),
            MetaFieldDoc::new("qos", "The quality of service of the delivery."),
            MetaFieldDoc::new(
                "retain",
                "True when the broker sent the retained message of the topic. \
                 False for a live publish.",
            ),
        ],
        "redis" => vec![
            MetaFieldDoc::new("connection", "The name of the connection that received the message."),
            MetaFieldDoc::new("channel", "The channel that the message was published to."),
        ],
        "opcua" => vec![MetaFieldDoc::new(
            "connection",
            "The name of the connection of the session. The node of the \
             reading is not metadata. It is always in the message, as `node` \
             and `name`.",
        )],
        "http_poll" => vec![
            MetaFieldDoc::new(
                "url",
                "The url that the input read the message from, with no \
                 username and no password.",
            ),
            MetaFieldDoc::new(
                "polled_at",
                "The start time of the read that returned the message, as RFC \
                 3339. All messages of one read have the same value. Use it to \
                 tell one snapshot from the next.",
            ),
        ],
        "postgres" | "clickhouse" => vec![
            MetaFieldDoc::new("connection", "The name of the connection that the input read from."),
            MetaFieldDoc::new(
                "polled_at",
                "The start time of the read that returned the row, as RFC \
                 3339. All rows of one read have the same value. Use it to \
                 tell one snapshot from the next. The time comes from the \
                 clock of kayak, not from the database server.",
            ),
        ],
        "pipeline" => vec![MetaFieldDoc::new(
            "upstream",
            "The id of the pipeline that sent the batch. Metadata from an \
             upstream input stays in the message. This input does not \
             replace it.",
        )],
        _ => return None,
    };

    let mut fields = common();
    fields.extend(own);
    Some(fields)
}
