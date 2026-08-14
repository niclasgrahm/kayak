//! Reading an OPC UA server's variables through a subscription.
//!
//! The one input that is *told* rather than asking. OPC UA's own streaming
//! shape is a subscription with a monitored item per node: the server samples
//! at its end and publishes what changed, so a tag that hasn't moved costs
//! nothing and a plant with ten thousand tags is one session rather than ten
//! thousand reads. Polling is the other half of this component and is not
//! built (see `docs/roadmap.md`); everything here assumes the server pushes.
//!
//! Three decisions are worth knowing before changing anything in here.
//!
//! **The tag is part of the message, not of the envelope.** Every other input
//! puts what it knows about a message — a subject, an offset — behind the
//! opt-in envelope, because the message is meaningful without it. A reading is
//! not: `21.5` with no node and no name is not data. So `node`, `name`,
//! `value`, `status` and the two timestamps are the message, always, and the
//! envelope adds only the connection.
//!
//! **`status` is always present, and that is the point.** A failed sensor does
//! not go quiet — it reports `BadDeviceFailure` with no value, once, and then
//! nothing. Dropping those would make a broken instrument look like a steady
//! one, so they are passed on with `value: null` for a `filter` downstream to
//! act on. A `Good` status is normally *absent* on the wire (it is the default
//! the encoding leaves out), which is why [`status_name`] reads a missing
//! status as `Good` rather than as unknown.
//!
//! **The session's reconnects are the library's, the outages are ours.** The
//! client is built with an unlimited session retry limit and subscription
//! recreation, so a blip in the plant network is healed underneath us with the
//! monitored items put back — much better than tearing the session down and
//! browsing again. What this module owns is the case that cannot be healed:
//! the event loop *ending*, or a connect that never completes. Both are
//! reported once per outage and retried on [`Backoff`]'s schedule, exactly as
//! the mqtt and redis inputs do it.
//!
//! Known noise, so nobody goes looking for the bug: the client logs two ERRORs
//! about a missing application instance certificate every time a session is
//! opened. There is no certificate because there is no encryption, so the
//! warning is true and irrelevant. `main.rs`'s `QUIET` silences the one module
//! that is *only* about reading those files; the other two lines come from
//! modules that log real failures as well and are left alone. All of it goes
//! away when this connection grows a security policy.

use crate::{
    BuildCtx,
    backoff::Backoff,
    events::publish,
    inputs::{
        BuildInput, InputSource, MessageBatch,
        ack::{self, Delivery},
        envelope::{Envelope, Meta},
    },
    secrets::Resolved,
    state::{PipelineId, UiEvent},
};
use anyhow::{Context, Result};
use kayak_core::{
    Stage,
    config::{OpcuaBrowseConfig, OpcuaConfig, OpcuaNodeConfig, Secret},
    connections::OpcuaConnection,
};
use opcua::client::browser::BrowseFilter;
use opcua::client::{Client, ClientBuilder, DataChangeCallback, IdentityToken, Session};
use opcua::types::{
    AttributeId, DataChangeFilter, DataChangeTrigger, DataValue, DeadbandType, EndpointDescription,
    ExtensionObject, MessageSecurityMode, MonitoredItemCreateRequest, MonitoringMode,
    MonitoringParameters, NodeClass, NodeClassMask, NodeId, ReadValueId, StatusCode,
    TimestampsToReturn, UserTokenPolicy, Variant,
};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::{broadcast, mpsc};
use tokio::task::JoinHandle;

/// What this client calls itself to a server. Servers log it and some list it
/// in their session table, which is the whole reason it is a fixed string
/// rather than something derived: an operator looking at a plant server should
/// see one recognisable name, not one per pipeline.
const APPLICATION_URI: &str = "urn:kayak";

/// How long to give a connect before calling it failed.
///
/// There has to be one. `Session::wait_for_connection` never returns while the
/// event loop is retrying, and with an unlimited retry limit that is forever —
/// so a server that is simply not there would leave this input waiting with
/// nothing on the pipeline's card to say why.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// Ceilings on what the client will accept in one message from the server.
///
/// The library's defaults (5 chunks, ~320 kB) are too small for a browse of any
/// real address space — the first one attempted here came back
/// `BadResponseTooLarge`. These are generous rather than unlimited, which is
/// available and deliberately not used: the size of what a server sends should
/// not be the server's alone to decide.
const MAX_MESSAGE_BYTES: usize = 16 * 1024 * 1024;
const MAX_CHUNK_COUNT: usize = 256;

/// Publishing interval when the config doesn't name one.
const DEFAULT_PUBLISH_INTERVAL_MS: u64 = 1000;

/// How far a `browse` follows the address space when the config doesn't say.
const DEFAULT_BROWSE_DEPTH: usize = 3;

/// How many notifications may wait between the subscription's callback and the
/// run loop.
///
/// The callback is synchronous — the server's own publish handling calls it —
/// so it cannot wait for a slow pipeline, and there is nothing to push back on
/// in any case: the server publishes whether or not anyone is keeping up. So
/// the queue is bounded and overflow is *counted and reported* rather than
/// hidden. See [`OpcuaInput::report_drops`].
const QUEUE_CAPACITY: usize = 10_000;

/// Subscription lifetime, in publishing intervals. The keep-alive count is how
/// long the server waits before sending an empty publish to prove it is still
/// there; the lifetime is how long it keeps the subscription with no publish
/// requests from us. The spec requires the lifetime to be at least three
/// keep-alives.
const KEEP_ALIVE_COUNT: u32 = 10;
const LIFETIME_COUNT: u32 = 60;

/// How many nodes one monitored-item request carries. Servers cap the size of a
/// request, and a thousand-tag browse would otherwise be one request no server
/// would accept.
const ITEMS_PER_REQUEST: usize = 500;

impl BuildInput for OpcuaConfig {
    fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn InputSource>> {
        // an OPC UA subscription has no client-withheld acknowledgement: the
        // publish acks the library sends are about notification *messages* and
        // are answered before this pipeline has seen anything. The same rule
        // the nats and redis inputs follow.
        ack::require_receipt_only(ctx.ack_mode(), "opcua")?;

        let server = ctx
            .opcua_connection(&self.connection)
            .context("the opcua input cannot be built")?
            .clone();
        let endpoint = ctx.resolve(&server.endpoint).with_context(|| {
            format!(
                "failed to resolve secrets in the endpoint of connection '{}'",
                self.connection
            )
        })?;
        let identity = identity_of(ctx, &self.connection, &server)?;

        anyhow::ensure!(
            !self.nodes.is_empty() || self.browse.is_some(),
            "an opcua input needs `nodes`, `browse`, or both — with neither there is nothing \
             to subscribe to and the pipeline would sit silent forever"
        );

        let nodes = parse_nodes(self.nodes)?;
        let browse = self.browse.as_ref().map(parse_browse).transpose()?;

        if let Some(deadband) = self.deadband {
            anyhow::ensure!(
                deadband.is_finite() && deadband >= 0.0,
                "the opcua input's `deadband` must be a number of the value's own units and at \
                 least zero, not {deadband}"
            );
        }

        Ok(Box::new(OpcuaInput {
            endpoint,
            identity,
            session_name: format!("kayak-{}", ctx.pipeline_id),
            nodes,
            browse,
            publish_interval: Duration::from_millis(
                self.publish_interval_ms.unwrap_or(DEFAULT_PUBLISH_INTERVAL_MS),
            ),
            // absent is -1, which is OPC UA's own spelling of "sample at the
            // publishing interval" and what a server does when left alone
            sampling_interval: match self.sampling_interval_ms {
                Some(ms) => millis_as_f64(ms, "sampling_interval_ms")?,
                None => -1.0,
            },
            queue_size: self.queue_size.unwrap_or(1),
            deadband: self.deadband,
            max_batch: crate::inputs::batch_cap(self.max_batch),
            envelope: ctx.envelope("opcua", Some(&self.connection)),
            connection_name: self.connection,
            pipeline_id: ctx.pipeline_id.clone(),
            events: ctx.events.clone(),
            backoff: Backoff::new(),
            session: None,
            event_loop: None,
            rx: None,
            dropped: Arc::new(AtomicU64::new(0)),
            reported_drops: 0,
        }))
    }
}

/// How the session should sign in, from what the connection carries.
///
/// Both halves or neither: a username with no password is a config that would
/// otherwise sign in anonymously and look like it had authenticated.
fn identity_of(ctx: &BuildCtx, connection: &str, server: &OpcuaConnection) -> Result<Identity> {
    let resolve = |secret: &Secret| {
        ctx.resolve(secret).with_context(|| {
            format!("failed to resolve secrets in the credentials of connection '{connection}'")
        })
    };
    match (&server.username, &server.password) {
        (None, None) => Ok(Identity::Anonymous),
        (Some(user), Some(pass)) => Ok(Identity::UserName(resolve(user)?, resolve(pass)?)),
        _ => anyhow::bail!(
            "opcua connection '{connection}' sets `username` or `password` without the other; \
             they must be set together or not at all"
        ),
    }
}

/// The nodes a config named, as ids the client can use.
///
/// Parsed at build time rather than at subscribe time so a typo is "pipeline
/// 'x' failed to start" and not a monitored item the server quietly rejects an
/// hour later.
fn parse_nodes(configured: Vec<OpcuaNodeConfig>) -> Result<Vec<(NodeId, String)>> {
    let mut nodes: Vec<(NodeId, String)> = Vec::with_capacity(configured.len());
    for node in configured {
        let id = NodeId::from_str(&node.node_id).map_err(|status| {
            anyhow::anyhow!(
                "'{}' is not a node id ({status}). OPC UA writes them as `ns=2;s=Name`, \
                 `ns=2;i=1042`, `g=<guid>` or `b=<base64>`, and a node in the server's own \
                 namespace 0 may leave the `ns=` off",
                node.node_id
            )
        })?;
        anyhow::ensure!(
            !nodes.iter().any(|(seen, _)| seen == &id),
            "the opcua input names node '{}' twice",
            node.node_id
        );
        let name = node.name.unwrap_or(node.node_id);
        nodes.push((id, name));
    }
    Ok(nodes)
}

/// The root of a `browse`, and how far under it to look.
fn parse_browse(browse: &OpcuaBrowseConfig) -> Result<Browse> {
    let root = NodeId::from_str(&browse.root)
        .map_err(|status| anyhow::anyhow!("'{}' is not a node id ({status})", browse.root))?;
    let depth = browse.depth.unwrap_or(DEFAULT_BROWSE_DEPTH);
    anyhow::ensure!(
        depth > 0,
        "the opcua input's `browse.depth` is 0, which would find nothing. Leave it out for the \
         default of {DEFAULT_BROWSE_DEPTH}"
    );
    Ok(Browse { root, depth })
}

/// Milliseconds as the `f64` OPC UA states intervals in, refusing one that
/// wouldn't survive the conversion. `u32` is 49 days of milliseconds, which is
/// past any interval that means anything.
fn millis_as_f64(ms: u64, field: &str) -> Result<f64> {
    let ms = u32::try_from(ms)
        .map_err(|_| anyhow::anyhow!("the opcua input's `{field}` of {ms} ms is not a interval"))?;
    Ok(f64::from(ms))
}

/// How the session signs in. Anonymous unless the connection carries both a
/// username and a password.
enum Identity {
    Anonymous,
    UserName(Resolved, Resolved),
}

impl Identity {
    fn token(&self) -> IdentityToken {
        match self {
            Self::Anonymous => IdentityToken::Anonymous,
            Self::UserName(user, pass) => {
                IdentityToken::UserName(user.expose().to_string(), pass.expose().into())
            }
        }
    }
}

/// A node to browse under, and how far.
struct Browse {
    root: NodeId,
    depth: usize,
}

pub struct OpcuaInput {
    endpoint: Resolved,
    identity: Identity,
    /// What the server's session table will call this client. Derived from the
    /// pipeline id, so an operator can tell two pipelines apart on the server.
    session_name: String,
    /// The nodes named in the config, with what messages from each are to call
    /// them. Parsed at build time.
    nodes: Vec<(NodeId, String)>,
    browse: Option<Browse>,
    publish_interval: Duration,
    /// Milliseconds, or `-1.0` for "whatever the publishing interval is".
    sampling_interval: f64,
    queue_size: u32,
    deadband: Option<f64>,
    /// Most messages in one batch. One unless the config says otherwise.
    max_batch: usize,
    /// What this input attaches to each message, if the config asked for any.
    envelope: Envelope,
    connection_name: String,
    pipeline_id: PipelineId,
    events: broadcast::Sender<UiEvent>,
    /// Paces reconnect attempts, and decides which of them is worth reporting.
    backoff: Backoff,
    /// The live session. Held so the subscription — and with it the callback
    /// holding the sending half of `rx` — stays alive.
    session: Option<Arc<Session>>,
    /// The task driving that session. Watched in `next`, because a session
    /// whose event loop has ended goes quiet rather than failing.
    event_loop: Option<JoinHandle<StatusCode>>,
    rx: Option<mpsc::Receiver<Value>>,
    /// Notifications the queue had no room for, counted by the callback and
    /// read by the run loop.
    dropped: Arc<AtomicU64>,
    reported_drops: u64,
}

/// What this input knows about a message besides the message: only which
/// connection it came through. The node, the value, its quality and its
/// timestamps are on the message itself — see the module docs.
fn meta_of(connection_name: &str) -> Meta {
    vec![("connection", Value::String(connection_name.to_string()))]
}

/// A status code's name, reading an absent one as `Good`.
///
/// Absent really does mean good: the encoding leaves the status out when it is
/// `Good`, so a reading that arrives without one is the ordinary case and not
/// an unknown quality.
fn status_name(status: Option<StatusCode>) -> &'static str {
    status.unwrap_or(StatusCode::Good).sub_code().name()
}

/// One reading, as the message that carries it.
///
/// Rendered here — on the session's side of the queue — rather than in the run
/// loop, for the reason `BatchPreview` is rendered on the server: the work
/// belongs to whatever produced the data, not to the loop everything else in
/// the pipeline is waiting behind.
fn message_of(node: &NodeId, name: &str, value: &DataValue) -> Option<Value> {
    let mut out = Map::new();
    out.insert("node".to_string(), Value::String(node.to_string()));
    out.insert("name".to_string(), Value::String(name.to_string()));
    out.insert(
        "value".to_string(),
        match &value.value {
            Some(variant) => json_of(variant)?,
            None => Value::Null,
        },
    );
    out.insert(
        "status".to_string(),
        Value::String(status_name(value.status).to_string()),
    );
    for (field, stamp) in [
        ("source_timestamp", value.source_timestamp.as_ref()),
        ("server_timestamp", value.server_timestamp.as_ref()),
    ] {
        out.insert(
            field.to_string(),
            stamp.map_or(Value::Null, |t| Value::String(t.to_rfc3339())),
        );
    }
    Some(Value::Object(out))
}

/// One OPC UA value as JSON, or `None` when it has no JSON form.
///
/// The scalar types map straight across. What doesn't is the handful that are
/// structures in their own right — an extension object holding a server's
/// custom type, a nested data value, diagnostic information — and those return
/// `None` rather than a guess: a message whose `value` was the `Debug` output
/// of a Rust struct would be worse than one that says it could not be read.
/// The caller skips the reading with a warning, the same answer every input
/// gives a payload it cannot parse.
///
/// **Floats go through their own shortest decimal form.** `f64::from(0.1f32)`
/// is `0.10000000149011612`, which is arithmetically right and reads as
/// nonsense in a log line; a `Float` node holding `0.1` should say `0.1`, and
/// the digits the source declared are the ones to keep. Same rule the postgres
/// output's `$n::text::NUMERIC` cast follows.
fn json_of(variant: &Variant) -> Option<Value> {
    let number = |n: Option<serde_json::Number>| Some(n.map_or(Value::Null, Value::Number));
    match variant {
        Variant::Empty => Some(Value::Null),
        Variant::Boolean(v) => Some(Value::Bool(*v)),
        Variant::SByte(v) => Some(Value::from(*v)),
        Variant::Byte(v) => Some(Value::from(*v)),
        Variant::Int16(v) => Some(Value::from(*v)),
        Variant::UInt16(v) => Some(Value::from(*v)),
        Variant::Int32(v) => Some(Value::from(*v)),
        Variant::UInt32(v) => Some(Value::from(*v)),
        Variant::Int64(v) => Some(Value::from(*v)),
        Variant::UInt64(v) => Some(Value::from(*v)),
        // a non-finite float has no JSON spelling at all, so it is a null
        // rather than a skipped message: the reading happened, and its status
        // is what says whether it meant anything
        Variant::Float(v) => number(format!("{v}").parse().ok()),
        Variant::Double(v) => number(serde_json::Number::from_f64(*v)),
        Variant::String(v) => Some(v.value().as_ref().map_or(Value::Null, |s| Value::from(s.as_str()))),
        Variant::DateTime(v) => Some(Value::String(v.to_rfc3339())),
        Variant::Guid(v) => Some(Value::String(v.to_string())),
        Variant::StatusCode(v) => Some(Value::String(v.sub_code().name().to_string())),
        // an opaque blob is base64 in JSON, the same spelling OPC UA's own JSON
        // encoding gives it
        Variant::ByteString(v) => Some(Value::String(v.as_base64())),
        Variant::XmlElement(v) => Some(Value::String(v.to_string())),
        Variant::QualifiedName(v) => Some(Value::String(v.to_string())),
        Variant::LocalizedText(v) => Some(Value::String(v.to_string())),
        Variant::NodeId(v) => Some(Value::String(v.to_string())),
        Variant::ExpandedNodeId(v) => Some(Value::String(v.to_string())),
        Variant::Variant(v) => json_of(v),
        Variant::Array(array) => {
            let mut out = Vec::with_capacity(array.values.len());
            for value in &array.values {
                out.push(json_of(value)?);
            }
            Some(Value::Array(out))
        }
        Variant::ExtensionObject(_) | Variant::DataValue(_) | Variant::DiagnosticInfo(_) => None,
    }
}

impl OpcuaInput {
    /// Connects, works out which nodes to monitor and subscribes to them,
    /// retrying forever — a plant server coming back after maintenance is
    /// exactly what this is for. Returns the queue the callback feeds.
    async fn reconnect(&mut self) -> mpsc::Receiver<Value> {
        loop {
            match self.try_connect().await {
                Ok(rx) => {
                    if self.backoff.is_failing() {
                        tracing::info!(
                            "opcua input reconnected to {} on connection '{}'",
                            self.endpoint,
                            self.connection_name
                        );
                    }
                    self.backoff.succeeded();
                    return rx;
                }
                Err(e) => {
                    self.teardown();
                    if !self.backoff.is_failing() {
                        tracing::error!(
                            "opcua input on connection '{}' cannot subscribe, retrying: {e:?}",
                            self.connection_name
                        );
                        publish(&self.events, || {
                            UiEvent::error(self.pipeline_id.clone(), Stage::Input, &e)
                        });
                    }
                    tokio::time::sleep(self.backoff.failed()).await;
                }
            }
        }
    }

    async fn try_connect(&mut self) -> Result<mpsc::Receiver<Value>> {
        let mut client = self.client()?;
        let endpoint: EndpointDescription = (
            self.endpoint.expose(),
            // the session is unencrypted — see `OpcuaConnection`'s doc comment
            // for why there is no other choice here yet
            "None",
            MessageSecurityMode::None,
            UserTokenPolicy::anonymous(),
        )
            .into();

        // *directly*: no `GetEndpoints` round trip first. Discovery is how this
        // is usually written and how it usually fails — a server behind docker
        // or NAT hands back the hostname it knows itself by, which is not one
        // this client can dial. What the connection says is what is dialled.
        let (session, event_loop) = client
            .connect_to_endpoint_directly(endpoint, self.identity.token())
            .with_context(|| format!("failed to open an opcua session at {}", self.endpoint))?;
        let mut handle = event_loop.spawn();

        let connected = tokio::select! {
            connected = tokio::time::timeout(CONNECT_TIMEOUT, session.wait_for_connection()) => connected,
            // the event loop giving up is the one way `wait_for_connection`
            // never returns, so it is watched rather than waited out
            ended = &mut handle => {
                let status = ended.map_or_else(|e| e.to_string(), |status| status.to_string());
                anyhow::bail!("the opcua session at {} ended before it connected: {status}", self.endpoint)
            }
        };
        anyhow::ensure!(
            connected.unwrap_or(false),
            "no opcua session at {} within {CONNECT_TIMEOUT:?}",
            self.endpoint
        );
        self.session = Some(session.clone());
        self.event_loop = Some(handle);

        let names = self.monitored_nodes(&session).await?;
        anyhow::ensure!(
            !names.is_empty(),
            "the opcua input's `browse` found no variables under its root at {}",
            self.endpoint
        );
        let monitored: Vec<NodeId> = names.keys().cloned().collect();
        let count = monitored.len();

        let (tx, rx) = mpsc::channel(QUEUE_CAPACITY);
        let dropped = self.dropped.clone();
        let subscription = session
            .create_subscription(
                self.publish_interval,
                LIFETIME_COUNT,
                KEEP_ALIVE_COUNT,
                // 0 is "as many as fit", which is what a batch of tag changes
                // wants: capping it only spreads one interval over several
                // publishes
                0,
                0,
                true,
                DataChangeCallback::new(move |value, item| {
                    let node = &item.item_to_monitor().node_id;
                    let name = names.get(node).map_or_else(|| node.to_string(), Clone::clone);
                    let Some(message) = message_of(node, &name, &value) else {
                        tracing::warn!(
                            "skipping a reading of opcua node '{node}': its value is a \
                             structure with no json form"
                        );
                        return;
                    };
                    if tx.try_send(message).is_err() {
                        dropped.fetch_add(1, Ordering::Relaxed);
                    }
                }),
            )
            .await
            .with_context(|| format!("failed to create an opcua subscription at {}", self.endpoint))?;

        self.monitor(&session, subscription, &monitored).await?;
        tracing::info!(
            "opcua input subscribed to {count} node(s) at {} on connection '{}'",
            self.endpoint,
            self.connection_name
        );
        Ok(rx)
    }

    fn client(&self) -> Result<Client> {
        ClientBuilder::new()
            .application_name("kayak")
            .application_uri(APPLICATION_URI)
            .product_uri(APPLICATION_URI)
            .session_name(self.session_name.clone())
            // -1 is "retry forever": a short outage is healed under us, with
            // the subscription and its monitored items recreated, and never
            // reaches this input at all
            .session_retry_limit(-1)
            .recreate_subscriptions(true)
            // no client certificate is written or read: the session is
            // unencrypted, so there is nothing to sign with, and a server that
            // needs one is a server this connection cannot describe yet
            .create_sample_keypair(false)
            // the client insists on a pki directory and creates the empty
            // trusted/rejected halves of one wherever it is pointed. Nothing
            // ever lands in them — no certificate is written, and server certs
            // are trusted rather than filed — so it is pointed away from the
            // working directory, which is where a pipeline would otherwise
            // leave two empty directories in whatever repository it was started
            // from.
            .pki_dir(std::env::temp_dir().join("kayak-opcua-pki"))
            .trust_server_certs(true)
            .verify_server_certs(false)
            .max_message_size(MAX_MESSAGE_BYTES)
            .max_chunk_count(MAX_CHUNK_COUNT)
            .client()
            .map_err(|errors| anyhow::anyhow!("invalid opcua client config: {}", errors.join(", ")))
    }

    /// Every node this input should monitor, and what to call each: the ones
    /// the config named, plus whatever a `browse` finds under its root.
    ///
    /// A browse names its finds after the server's own display name, which is
    /// the whole convenience of it — `Temperature` rather than
    /// `ns=2;i=1042`. A node found by both keeps the config's name: what the
    /// file says wins over what the server happens to call it.
    async fn monitored_nodes(&self, session: &Session) -> Result<HashMap<NodeId, String>> {
        let mut names: HashMap<NodeId, String> = HashMap::new();
        if let Some(browse) = &self.browse {
            let filter = BrowseFilter::new_hierarchical()
                // objects are followed but never monitored: a folder has no
                // value. Both classes have to be asked for, since a class left
                // out of the mask is not returned and so is not recursed into
                // either.
                .node_class_mask(NodeClassMask::OBJECT | NodeClassMask::VARIABLE)
                .max_depth(browse.depth);
            let found = session
                .browser()
                .handler(filter.clone())
                .run_into_result(vec![filter.new_description_from_node(browse.root.clone())])
                .await
                .with_context(|| {
                    format!("failed to browse opcua node '{}'", browse.root)
                })?;
            for (id, node) in found.nodes {
                if node.node_class == NodeClass::Variable {
                    names.insert(id, node.display_name.to_string());
                }
            }
        }
        for (id, name) in &self.nodes {
            names.insert(id.clone(), name.clone());
        }
        Ok(names)
    }

    /// Creates a monitored item per node, in requests a server will accept.
    ///
    /// A node the server refuses is reported and the rest are kept: one tag
    /// renamed in the plant should cost that tag, not the pipeline.
    async fn monitor(
        &self,
        session: &Session,
        subscription: u32,
        nodes: &[NodeId],
    ) -> Result<()> {
        let filter = self.deadband.map_or_else(ExtensionObject::null, |deadband| {
            ExtensionObject::from_message(DataChangeFilter {
                trigger: DataChangeTrigger::StatusValue,
                deadband_type: DeadbandType::Absolute as u32,
                deadband_value: deadband,
            })
        });
        let items: Vec<MonitoredItemCreateRequest> = nodes
            .iter()
            .map(|node| MonitoredItemCreateRequest {
                item_to_monitor: ReadValueId {
                    node_id: node.clone(),
                    attribute_id: AttributeId::Value as u32,
                    ..Default::default()
                },
                monitoring_mode: MonitoringMode::Reporting,
                requested_parameters: MonitoringParameters {
                    // left at zero for the library to fill in: it is the handle
                    // the callback's `MonitoredItem` is found by
                    client_handle: 0,
                    sampling_interval: self.sampling_interval,
                    filter: filter.clone(),
                    queue_size: self.queue_size,
                    discard_oldest: true,
                },
            })
            .collect();

        for chunk in items.chunks(ITEMS_PER_REQUEST) {
            let created = session
                .create_monitored_items(subscription, TimestampsToReturn::Both, chunk.to_vec())
                .await
                .context("failed to create opcua monitored items")?;
            for (item, created) in chunk.iter().zip(created) {
                if created.result.status_code.is_bad() {
                    tracing::warn!(
                        "opcua node '{}' cannot be monitored ({}); the other nodes are unaffected",
                        item.item_to_monitor.node_id,
                        created.result.status_code
                    );
                }
            }
        }
        Ok(())
    }

    /// Drops the session and the task driving it. The subscription goes with
    /// it, and so does the callback holding the queue's sending half — which is
    /// what makes a closed queue mean "the session is gone" in `next`.
    fn teardown(&mut self) {
        self.session = None;
        if let Some(handle) = self.event_loop.take() {
            handle.abort();
        }
    }

    /// Says so when the queue has overflowed since last time.
    ///
    /// Once per run of drops rather than once per drop: a pipeline that cannot
    /// keep up drops thousands, and a line each would bury the log with the one
    /// message that matters in it. The count is the true one — it is kept by
    /// the callback, which sees every notification.
    fn report_drops(&mut self) {
        let dropped = self.dropped.load(Ordering::Relaxed);
        if dropped > self.reported_drops {
            tracing::warn!(
                "opcua input on connection '{}' dropped {} reading(s): the pipeline is not \
                 keeping up with what the server is publishing. Raise `max_batch`, lengthen \
                 `publish_interval_ms`, or set a `deadband`",
                self.connection_name,
                dropped - self.reported_drops
            );
            self.reported_drops = dropped;
        }
    }
}

#[async_trait::async_trait]
impl InputSource for OpcuaInput {
    async fn next(&mut self) -> Result<Delivery> {
        loop {
            if self.rx.is_none() {
                self.rx = Some(self.reconnect().await);
            }
            let (Some(rx), Some(event_loop)) = (self.rx.as_mut(), self.event_loop.as_mut()) else {
                // reconnect only ever returns with both set
                self.rx = None;
                continue;
            };

            let mut batch: MessageBatch = Vec::new();
            let received = tokio::select! {
                received = rx.recv() => received,
                // a session whose event loop has ended never publishes again
                // and never closes the queue either, so it has to be watched
                // for rather than waited on
                ended = event_loop => {
                    let status = ended.map_or_else(|e| e.to_string(), |status| status.to_string());
                    tracing::warn!(
                        "the opcua session on connection '{}' ended ({status}), reconnecting",
                        self.connection_name
                    );
                    None
                }
            };
            let Some(first) = received else {
                self.rx = None;
                self.teardown();
                continue;
            };
            batch.push(Arc::new(first));

            // whatever else has *already* arrived, up to the cap — never a wait
            // for one to fill, so a quiet plant still yields batches of one
            // however high `max_batch` is. With the default of 1 this doesn't
            // run at all.
            while batch.len() < self.max_batch {
                match rx.try_recv() {
                    Ok(value) => batch.push(Arc::new(value)),
                    Err(_) => break,
                }
            }

            self.report_drops();

            // applied here rather than in the callback because the envelope is
            // the wrapper's, not the session's. A reading is always an object,
            // so `merge` never has nowhere to attach and `apply` never skips.
            if self.envelope.is_enabled() {
                let own = meta_of(&self.connection_name);
                batch = batch
                    .into_iter()
                    .filter_map(|message| {
                        let value = Arc::try_unwrap(message).unwrap_or_else(|arc| (*arc).clone());
                        self.envelope.apply(value, own.clone()).map(Arc::new)
                    })
                    .collect();
            }

            return Ok(Delivery::new(Arc::new(batch)));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kayak_core::config::{AckMode, EnvelopeConfig};
    use kayak_core::connections::{ConnectionKind, Connections};
    use opcua::types::{Array, ByteString, UAString, VariantScalarTypeId};

    fn connections() -> Connections {
        [(
            "plant".to_string(),
            ConnectionKind::Opcua(OpcuaConnection {
                endpoint: "opc.tcp://localhost:50000".into(),
                username: None,
                password: None,
            }),
        )]
        .into_iter()
        .collect()
    }

    fn config(json: serde_json::Value) -> OpcuaConfig {
        match serde_json::from_value(json) {
            Ok(config) => config,
            Err(e) => panic!("the sample config should parse: {e}"),
        }
    }

    fn build_with(config: OpcuaConfig, ack_mode: Option<AckMode>) -> Result<Box<dyn InputSource>> {
        let mut pipelines = HashMap::new();
        let (events, _rx) = tokio::sync::broadcast::channel(4);
        let mut ctx = BuildCtx::new(&mut pipelines, "p".to_string(), events)
            .with_connections(Arc::new(connections()));
        ctx.ack_mode = ack_mode;
        config.build(&mut ctx)
    }

    fn build(json: serde_json::Value) -> Result<Box<dyn InputSource>> {
        build_with(config(json), None)
    }

    fn refusal(json: serde_json::Value) -> String {
        match build(json) {
            Ok(_) => panic!("an opcua input built that should have been refused"),
            Err(e) => format!("{e:#}"),
        }
    }

    #[test]
    fn a_named_node_builds() {
        assert!(
            build(serde_json::json!({
                "connection": "plant",
                "nodes": [{"node_id": "ns=2;s=Machine1.Temperature", "name": "temperature"}]
            }))
            .is_ok()
        );
    }

    /// An input with neither `nodes` nor `browse` has nothing to monitor. It
    /// would connect, subscribe to nothing and sit silent forever, which is
    /// indistinguishable from a plant that isn't running — so it is refused at
    /// build time instead.
    #[test]
    fn an_input_that_monitors_nothing_is_refused() {
        let err = refusal(serde_json::json!({"connection": "plant"}));
        assert!(err.contains("nodes"), "{err}");
    }

    /// A node id is parsed while the pipeline is being built, so a typo fails
    /// the pipeline visibly instead of becoming a monitored item the server
    /// rejects once, silently, an hour later.
    #[test]
    fn a_node_id_that_is_not_one_is_refused() {
        let err = refusal(serde_json::json!({
            "connection": "plant",
            "nodes": [{"node_id": "Machine1.Temperature"}]
        }));
        assert!(err.contains("is not a node id"), "{err}");
    }

    /// Namespace 0 is the server's own and `ns=` may be left off there — the
    /// notation OPC UA itself uses, so it has to be accepted.
    #[test]
    fn a_node_id_without_a_namespace_is_accepted() {
        assert!(
            build(serde_json::json!({
                "connection": "plant",
                "nodes": [{"node_id": "i=2258"}]
            }))
            .is_ok()
        );
    }

    /// Two monitored items on one node is never what was meant, and the
    /// duplicate would double every reading of it downstream.
    #[test]
    fn the_same_node_twice_is_refused() {
        let err = refusal(serde_json::json!({
            "connection": "plant",
            "nodes": [
                {"node_id": "ns=2;i=1042", "name": "a"},
                {"node_id": "ns=2;i=1042", "name": "b"}
            ]
        }));
        assert!(err.contains("twice"), "{err}");
    }

    /// A browse with no depth would find nothing at all, so the zero someone
    /// meant as "no limit" is refused rather than obeyed.
    #[test]
    fn a_browse_of_depth_zero_is_refused() {
        let err = refusal(serde_json::json!({
            "connection": "plant",
            "browse": {"root": "ns=2;s=Machine1", "depth": 0}
        }));
        assert!(err.contains("depth"), "{err}");
    }

    #[test]
    fn a_negative_deadband_is_refused() {
        let err = refusal(serde_json::json!({
            "connection": "plant",
            "nodes": [{"node_id": "ns=2;i=1042"}],
            "deadband": -1.0
        }));
        assert!(err.contains("deadband"), "{err}");
    }

    /// An OPC UA subscription has no acknowledgement this pipeline could
    /// withhold: the publish acks are the library's and are answered before a
    /// transform has seen anything. Refused rather than silently behaving like
    /// `on_receipt`, the same rule the nats and redis inputs follow.
    #[test]
    fn on_delivery_is_refused() {
        let config = config(serde_json::json!({
            "connection": "plant",
            "nodes": [{"node_id": "ns=2;i=1042"}]
        }));
        let Err(err) = build_with(config, Some(AckMode::OnDelivery)) else {
            panic!("an opcua input built with `ack: on_delivery`, which it cannot honour");
        };
        assert!(format!("{err:#}").contains("opcua"), "{err:#}");
    }

    /// The wrong kind of connection is caught here rather than at the first
    /// read, and says which kind it actually is.
    #[test]
    fn a_connection_of_another_kind_is_refused() {
        let mut pipelines = HashMap::new();
        let (events, _rx) = tokio::sync::broadcast::channel(4);
        let connections: Connections = [(
            "plant".to_string(),
            ConnectionKind::Redis(kayak_core::connections::RedisConnection {
                url: "redis://localhost:6379".into(),
            }),
        )]
        .into_iter()
        .collect();
        let mut ctx = BuildCtx::new(&mut pipelines, "p".to_string(), events)
            .with_connections(Arc::new(connections));
        let built = config(serde_json::json!({
            "connection": "plant",
            "nodes": [{"node_id": "ns=2;i=1042"}]
        }))
        .build(&mut ctx);
        let Err(err) = built else {
            panic!("an opcua input built on a redis connection");
        };
        assert!(format!("{err:#}").contains("redis"), "{err:#}");
    }

    /// A named node keeps its name; one that wasn't named answers to its id,
    /// which is exact and always available.
    #[test]
    fn a_node_without_a_name_is_called_by_its_id() {
        let nodes = match parse_nodes(vec![
            OpcuaNodeConfig {
                node_id: "ns=2;i=1042".to_string(),
                name: Some("temperature".to_string()),
            },
            OpcuaNodeConfig {
                node_id: "ns=2;i=1043".to_string(),
                name: None,
            },
        ]) {
            Ok(nodes) => nodes,
            Err(e) => panic!("both nodes should parse: {e:#}"),
        };
        assert_eq!(nodes[0].1, "temperature");
        assert_eq!(nodes[1].1, "ns=2;i=1043");
    }

    fn message(value: &DataValue) -> Value {
        let node = match NodeId::from_str("ns=2;s=Temp") {
            Ok(node) => node,
            Err(e) => panic!("the sample node id should parse: {e}"),
        };
        match message_of(&node, "temperature", value) {
            Some(message) => message,
            None => panic!("the sample reading should render"),
        }
    }

    /// The message carries the tag as well as the value: a reading with no node
    /// and no name is not data, which is why none of this is behind the
    /// envelope.
    #[test]
    fn a_reading_carries_its_node_and_its_name() {
        let out = message(&DataValue {
            value: Some(Variant::Double(21.5)),
            status: Some(StatusCode::Good),
            source_timestamp: None,
            source_picoseconds: None,
            server_timestamp: None,
            server_picoseconds: None,
        });
        assert_eq!(out["node"], serde_json::json!("ns=2;s=Temp"));
        assert_eq!(out["name"], serde_json::json!("temperature"));
        assert_eq!(out["value"], serde_json::json!(21.5));
        assert_eq!(out["status"], serde_json::json!("Good"));
        assert_eq!(out["source_timestamp"], Value::Null);
    }

    /// A `Good` status is left off the wire, so an absent one is the ordinary
    /// case and must not read as unknown — a message whose quality was missing
    /// would be filtered out by every `status == "Good"` there is.
    #[test]
    fn a_missing_status_reads_as_good() {
        assert_eq!(status_name(None), "Good");
        assert_eq!(status_name(Some(StatusCode::BadDeviceFailure)), "BadDeviceFailure");
    }

    /// A failed sensor reports a bad status with no value at all. That is a
    /// fact about the plant and is passed on rather than dropped: a `filter`
    /// downstream is what decides what to do about it.
    #[test]
    fn a_bad_reading_keeps_its_status_and_a_null_value() {
        let out = message(&DataValue {
            value: None,
            status: Some(StatusCode::BadDeviceFailure),
            source_timestamp: None,
            source_picoseconds: None,
            server_timestamp: None,
            server_picoseconds: None,
        });
        assert_eq!(out["value"], Value::Null);
        assert_eq!(out["status"], serde_json::json!("BadDeviceFailure"));
    }

    #[test]
    fn the_scalar_types_convert() {
        for (variant, expected) in [
            (Variant::Empty, serde_json::json!(null)),
            (Variant::Boolean(true), serde_json::json!(true)),
            (Variant::SByte(-3), serde_json::json!(-3)),
            (Variant::Byte(3), serde_json::json!(3)),
            (Variant::Int16(-300), serde_json::json!(-300)),
            (Variant::UInt16(300), serde_json::json!(300)),
            (Variant::Int32(-70000), serde_json::json!(-70000)),
            (Variant::UInt32(70000), serde_json::json!(70000)),
            (Variant::Int64(-5_000_000_000), serde_json::json!(-5_000_000_000_i64)),
            (Variant::UInt64(5_000_000_000), serde_json::json!(5_000_000_000_u64)),
            (Variant::Double(1.25), serde_json::json!(1.25)),
            (
                Variant::String(UAString::from("hello")),
                serde_json::json!("hello"),
            ),
            (Variant::String(UAString::null()), serde_json::json!(null)),
            (
                Variant::StatusCode(StatusCode::BadNodeIdUnknown),
                serde_json::json!("BadNodeIdUnknown"),
            ),
            (
                Variant::ByteString(ByteString::from(vec![1_u8, 2, 3])),
                serde_json::json!("AQID"),
            ),
        ] {
            assert_eq!(json_of(&variant), Some(expected), "{variant:?}");
        }
    }

    /// A `Float` node holding `0.1` should say `0.1`. Widening the f32 gives
    /// `0.10000000149011612`, which is arithmetically right and reads as
    /// nonsense — the digits the source declared are the ones to keep.
    #[test]
    fn a_float_keeps_the_digits_it_was_sent_with() {
        assert_eq!(json_of(&Variant::Float(0.1)), Some(serde_json::json!(0.1)));
    }

    /// A value JSON has no spelling for is a null rather than a skipped
    /// message: the reading happened, and its status is what says whether it
    /// meant anything.
    #[test]
    fn a_non_finite_float_is_null() {
        assert_eq!(json_of(&Variant::Double(f64::NAN)), Some(Value::Null));
        assert_eq!(json_of(&Variant::Float(f32::INFINITY)), Some(Value::Null));
    }

    #[test]
    fn an_array_converts_element_by_element() {
        let array = match Array::new(
            VariantScalarTypeId::Int32,
            vec![Variant::Int32(1), Variant::Int32(2)],
        ) {
            Ok(array) => array,
            Err(e) => panic!("the sample array should build: {e}"),
        };
        assert_eq!(
            json_of(&Variant::Array(Box::new(array))),
            Some(serde_json::json!([1, 2]))
        );
    }

    /// A structure has no honest JSON form, and the `Debug` output of a Rust
    /// struct would be a worse answer than none. The caller skips the reading
    /// with a warning, exactly as every input does with a payload it cannot
    /// parse.
    #[test]
    fn a_structured_value_has_no_json_form() {
        assert_eq!(json_of(&Variant::DiagnosticInfo(Box::default())), None);
        let node = match NodeId::from_str("ns=2;s=Temp") {
            Ok(node) => node,
            Err(e) => panic!("the sample node id should parse: {e}"),
        };
        assert_eq!(
            message_of(
                &node,
                "temperature",
                &DataValue {
                    value: Some(Variant::DiagnosticInfo(Box::default())),
                    status: None,
                    source_timestamp: None,
                    source_picoseconds: None,
                    server_timestamp: None,
                    server_picoseconds: None,
                }
            ),
            None
        );
    }

    /// The envelope adds the connection and nothing else — the node and the
    /// value are on the message itself, so this input's metadata is the
    /// smallest of any of them.
    #[test]
    fn the_envelope_carries_only_the_connection() {
        let envelope = Envelope::new(
            Some(&EnvelopeConfig::Merge { meta: None }),
            vec![("input", Value::String("opcua".to_string()))],
        );
        let Some(out) = envelope.apply(serde_json::json!({"value": 1}), meta_of("plant")) else {
            panic!("a reading is an object, so a merge envelope always attaches");
        };
        assert_eq!(out["_meta"]["connection"], serde_json::json!("plant"));
        assert_eq!(out["value"], serde_json::json!(1));
    }
}
