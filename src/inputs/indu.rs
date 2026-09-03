//! Reads sensors and streams out of Indu Cloud — `GET /api/v1/live/sse`.
//!
//! kayak's first server-sent-events input, and `indu`-only rather than a
//! generic `sse` input with a preset over it: the protocol on the wire is
//! Indu's — `ready`, `subscribed`, `reading`, `stream_reading`, `lagged` —
//! and the half of this file that is not SSE framing is that protocol. A
//! second SSE source would be the moment to split the framing out.
//!
//! # Names, not UUIDs
//!
//! The config names a sensor as `<device>/<sensor>` and a stream by the name
//! it was written under, because those are the names a person knows and the
//! names the platform's own screens show. The platform subscribes by id, so
//! the names are resolved through `/api/v1` on the first read, and every
//! message carries the name *and* the ids — the name is what a pipeline
//! reduces over, the ids are for anything that needs to go back to the
//! platform.
//!
//! Resolution happens on the first read rather than at build time because
//! building is synchronous and holds the pipelines lock, and because a name
//! that cannot be found is usually a stream another pipeline is about to
//! write: it is reported on the card and looked for again after a pause,
//! the way a broker that is down is.
//!
//! # What the connection does when it breaks
//!
//! The same thing the nats input does: one error on the card on the attempt
//! that starts an outage, reconnects paced by [`Backoff`], a log line on the
//! attempt that ends it. A `lagged` message — the platform dropped readings
//! this connection could not keep up with — is an error on the card too,
//! because a live view that silently skips is a chart with an invisible hole.

use std::collections::{BTreeMap, VecDeque};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use bytes::Bytes;
use futures_util::{Stream, StreamExt};
use kayak_core::{Stage, config::InduInputConfig};
use reqwest::header::{ACCEPT, AUTHORIZATION, HeaderValue};
use reqwest::{Client, Url};
use serde_json::{Value, json};
use tokio::sync::broadcast;

use crate::{
    BuildCtx,
    backoff::Backoff,
    events::publish,
    inputs::{
        BuildInput, InputSource, MessageBatch,
        ack::{self, Delivery},
        envelope::{Envelope, Meta},
    },
    state::{PipelineId, UiEvent},
};

const SSE_PATH: &str = "/api/v1/live/sse";
const DEVICES_PATH: &str = "/api/v1/devices";
const STREAMS_PATH: &str = "/api/v1/streams";

/// How long one catalogue request may take. The subscription itself has no
/// deadline — it is meant to stay open — but is dropped if the platform goes
/// quiet for longer than [`READ_TIMEOUT`], which is several of its heartbeats.
const CATALOGUE_TIMEOUT: Duration = Duration::from_secs(30);
const READ_TIMEOUT: Duration = Duration::from_secs(90);

/// Most devices and sensors asked for in one catalogue page. A device with
/// more sensors than this is not one a pipeline names sensors on by hand.
const PAGE: usize = 5_000;

impl BuildInput for InduInputConfig {
    fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn InputSource>> {
        // A live subscription has no redelivery: a reading not read while the
        // connection was down is in the historian, not on the wire.
        ack::require_receipt_only(ctx.ack_mode(), "indu")?;

        let sensors: Vec<String> = self
            .sensors
            .iter()
            .map(|name| name.trim().to_string())
            .filter(|name| !name.is_empty())
            .collect();
        let streams: Vec<String> = self
            .streams
            .iter()
            .map(|name| name.trim().to_string())
            .filter(|name| !name.is_empty())
            .collect();
        anyhow::ensure!(
            !(sensors.is_empty() && streams.is_empty()),
            "the indu input needs at least one entry in `sensors` or `streams`: what to read"
        );
        for name in &sensors {
            anyhow::ensure!(
                name.split_once('/')
                    .is_some_and(|(device, sensor)| !device.is_empty() && !sensor.is_empty()),
                "the indu input's sensor {name:?} is not `<device>/<sensor>` — the device's id, \
                 a slash, the sensor's id, as the platform names them"
            );
        }

        let connection = ctx.indu_connection(&self.connection)?;
        let origin = Url::parse(&connection.url).with_context(|| {
            format!(
                "the indu connection '{}' has a url that is not one: {:?}",
                self.connection, connection.url
            )
        })?;
        anyhow::ensure!(
            matches!(origin.scheme(), "http" | "https"),
            "the indu connection '{}' needs an http(s) url; got '{}'",
            self.connection,
            origin.scheme()
        );

        let resolved = ctx.resolve(&connection.api_key)?;
        anyhow::ensure!(
            !resolved.expose().is_empty(),
            "the indu connection '{}' resolved to an empty api key; check that '{resolved}' is \
             set in the secret store",
            self.connection
        );
        let credential = credential(resolved.expose())?;
        let client = Client::builder()
            .read_timeout(READ_TIMEOUT)
            .build()
            .context("failed to build the indu input's client")?;

        Ok(Box::new(InduInput {
            api: Api {
                described: origin.to_string(),
                origin,
                credential,
                client,
                sensors,
                streams,
                backfill: self.backfill.unwrap_or(true),
            },
            max_batch: crate::inputs::batch_cap(self.max_batch),
            envelope: ctx.envelope("indu", Some(&self.connection)),
            pipeline_id: ctx.pipeline_id.clone(),
            events: ctx.events.clone(),
            backoff: Backoff::new(),
            catalogue: None,
            subscription: None,
            pending: VecDeque::new(),
        }))
    }
}

/// The `Authorization` header the key becomes — sensitive, so a dumped
/// request prints `Sensitive` rather than the key.
fn credential(key: &str) -> Result<HeaderValue> {
    let mut value = HeaderValue::try_from(format!("Bearer {key}"))
        .context("the indu api key cannot be sent as a header")?;
    value.set_sensitive(true);
    Ok(value)
}

/// One sensor, as the platform knows it and as the config named it.
#[derive(Debug, Clone, PartialEq)]
struct SensorEntry {
    /// `<device>/<sensor>`, as configured.
    name: String,
    device: String,
    sensor: String,
    label: String,
    unit: String,
    device_id: String,
}

#[derive(Debug, Clone, PartialEq)]
struct StreamEntry {
    /// As configured.
    name: String,
    label: String,
    unit: String,
}

/// The configured names, resolved to the platform's ids.
#[derive(Debug, Default, Clone, PartialEq)]
struct Catalogue {
    sensors: BTreeMap<String, SensorEntry>,
    streams: BTreeMap<String, StreamEntry>,
}

type ByteStream = Pin<Box<dyn Stream<Item = reqwest::Result<Bytes>> + Send>>;

/// An open subscription: the body as it arrives, and the bytes of the event
/// that has not finished arriving.
struct Subscription {
    body: ByteStream,
    buffer: String,
}

/// The platform side of the input: what it asks, with what. Its own type
/// so the requests can borrow it across an await while the subscription —
/// a byte stream that is `Send` but not `Sync` — lives beside it.
struct Api {
    origin: Url,
    described: String,
    credential: HeaderValue,
    client: Client,
    sensors: Vec<String>,
    streams: Vec<String>,
    backfill: bool,
}

pub struct InduInput {
    api: Api,
    max_batch: usize,
    envelope: Envelope,
    pipeline_id: PipelineId,
    events: broadcast::Sender<UiEvent>,
    backoff: Backoff,
    /// `None` until the first read resolves the names.
    catalogue: Option<Catalogue>,
    subscription: Option<Subscription>,
    /// Messages decoded from the last chunk and not yet emitted — what
    /// `max_batch` coalesces.
    pending: VecDeque<Value>,
}

/// One server-sent event, framed.
#[derive(Debug, Default, PartialEq)]
struct SseEvent {
    name: String,
    data: String,
}

/// Cut complete events off the front of `buffer`, leaving a partial one.
///
/// The framing and nothing else: `event:` and `data:` lines, a blank line
/// between events, `:` for a comment (the platform's heartbeat), `id:` and
/// `retry:` read and ignored. Both line endings.
fn take_events(buffer: &mut String) -> Vec<SseEvent> {
    // One line ending, so a blank line is `\n\n` whatever the platform sent —
    // and a `\r` left dangling at a chunk boundary joins its `\n` next time.
    if buffer.contains("\r\n") {
        *buffer = buffer.replace("\r\n", "\n");
    }
    let mut events = Vec::new();
    while let Some(end) = buffer.find("\n\n") {
        let block: String = buffer.drain(..end).collect();
        buffer.drain(..2);
        let mut event = SseEvent::default();
        let mut data: Vec<&str> = Vec::new();
        for line in block.lines() {
            if let Some(value) = line.strip_prefix("event:") {
                event.name = value.trim().to_string();
            } else if let Some(value) = line.strip_prefix("data:") {
                data.push(value.strip_prefix(' ').unwrap_or(value));
            }
            // `id:`, `retry:` and comments carry nothing this input uses
        }
        if data.is_empty() && event.name.is_empty() {
            continue; // a heartbeat comment on its own
        }
        event.data = data.join("\n");
        events.push(event);
    }
    events
}

/// Epoch milliseconds as the RFC 3339 string a person reads.
fn rfc3339(ts: i64) -> Value {
    chrono::DateTime::<chrono::Utc>::from_timestamp_millis(ts)
        .map_or(Value::Null, |at| Value::String(at.to_rfc3339()))
}

/// What a `reading` or `stream_reading` event becomes: one named message.
///
/// `None` for an id the catalogue does not know — a subscription answers
/// only what it was asked for, so this is a platform sending something it
/// should not, skipped rather than fatal.
fn message_of(catalogue: &Catalogue, event: &SseEvent, data: &Value) -> Option<Value> {
    match event.name.as_str() {
        "reading" => {
            let id = data.get("sensor_id")?.as_str()?;
            let entry = catalogue.sensors.get(id)?;
            let ts = data.get("ts")?.as_i64()?;
            Some(json!({
                "kind": "sensor",
                "name": entry.name,
                "device": entry.device,
                "sensor": entry.sensor,
                "label": entry.label,
                "unit": entry.unit,
                "sensor_id": id,
                "device_id": entry.device_id,
                "at": rfc3339(ts),
                "ts": ts,
                "value": data.get("v").cloned().unwrap_or(Value::Null),
            }))
        }
        "stream_reading" => {
            let id = data.get("stream_id")?.as_str()?;
            let entry = catalogue.streams.get(id)?;
            let ts = data.get("ts")?.as_i64()?;
            Some(json!({
                "kind": "stream",
                "name": entry.name,
                "label": entry.label,
                "unit": entry.unit,
                "stream_id": id,
                "at": rfc3339(ts),
                "ts": ts,
                "value": data.get("v").cloned().unwrap_or(Value::Null),
            }))
        }
        _ => None,
    }
}

/// The ids in a JSON array of strings, as the platform's `subscribed` lists
/// them.
fn ids_in(value: Option<&Value>) -> Vec<&str> {
    value
        .and_then(Value::as_array)
        .map(|items| items.iter().filter_map(Value::as_str).collect())
        .unwrap_or_default()
}

/// The items of a list endpoint's body, whether it is `{"data": […]}` or a
/// bare array.
fn items_of(body: Value) -> Vec<Value> {
    match body {
        Value::Array(items) => items,
        Value::Object(mut object) => match object.remove("data") {
            Some(Value::Array(items)) => items,
            _ => Vec::new(),
        },
        _ => Vec::new(),
    }
}

fn str_of<'a>(value: &'a Value, field: &str) -> &'a str {
    value.get(field).and_then(Value::as_str).unwrap_or("")
}

impl Api {
    fn api(&self, path: &str) -> Result<Url> {
        self.origin
            .join(path)
            .with_context(|| format!("joining {path} onto the indu origin"))
    }

    /// One authenticated `GET`, as JSON. A status that is not success is an
    /// error naming it: a `401` here is the key, a `403` is what it may see.
    async fn get(&self, url: Url) -> Result<Value> {
        let response = self
            .client
            .get(url.clone())
            .header(AUTHORIZATION, self.credential.clone())
            .timeout(CATALOGUE_TIMEOUT)
            .send()
            .await
            .with_context(|| format!("asking indu at {url}"))?;
        let status = response.status();
        if !status.is_success() {
            let detail = response
                .json::<Value>()
                .await
                .ok()
                .and_then(|problem| problem.get("detail")?.as_str().map(str::to_string))
                .unwrap_or_default();
            bail!(
                "indu answered {status} to {}{}",
                url.path(),
                if detail.is_empty() {
                    String::new()
                } else {
                    format!(": {detail}")
                }
            );
        }
        response
            .json::<Value>()
            .await
            .with_context(|| format!("reading indu's answer to {}", url.path()))
    }

    /// Every configured name, resolved — or the names that could not be.
    async fn resolve(&self) -> Result<Catalogue> {
        let mut catalogue = Catalogue::default();
        let mut missing: Vec<String> = Vec::new();

        // Sensors, grouped by device so a device is asked about once.
        let mut by_device: BTreeMap<&str, Vec<(&str, &str)>> = BTreeMap::new();
        for name in &self.sensors {
            if let Some((device, sensor)) = name.split_once('/') {
                by_device.entry(device).or_default().push((sensor, name));
            }
        }
        for (device, wanted) in by_device {
            let mut url = self.api(DEVICES_PATH)?;
            url.query_pairs_mut()
                .append_pair("q", device)
                .append_pair("limit", &PAGE.to_string());
            let devices = items_of(self.get(url).await?);
            let Some(found) = devices
                .iter()
                .find(|candidate| str_of(candidate, "external_id") == device)
            else {
                missing.extend(wanted.iter().map(|(_, name)| (*name).to_string()));
                continue;
            };
            let device_id = str_of(found, "id").to_string();
            let mut url = self.api(&format!("{DEVICES_PATH}/{device_id}/sensors"))?;
            url.query_pairs_mut()
                .append_pair("limit", &PAGE.to_string());
            let sensors = items_of(self.get(url).await?);
            for (sensor, name) in wanted {
                let Some(found) = sensors
                    .iter()
                    .find(|candidate| str_of(candidate, "external_id") == sensor)
                else {
                    missing.push(name.to_string());
                    continue;
                };
                let label = str_of(found, "name");
                catalogue.sensors.insert(
                    str_of(found, "id").to_string(),
                    SensorEntry {
                        name: name.to_string(),
                        device: device.to_string(),
                        sensor: sensor.to_string(),
                        label: if label.is_empty() { sensor } else { label }.to_string(),
                        unit: str_of(found, "unit").to_string(),
                        device_id: device_id.clone(),
                    },
                );
            }
        }

        if !self.streams.is_empty() {
            let streams = items_of(self.get(self.api(STREAMS_PATH)?).await?);
            for name in &self.streams {
                // Written under `external_id`; a stream the platform computes
                // has none, and goes by its display name.
                let Some(found) = streams
                    .iter()
                    .find(|candidate| str_of(candidate, "external_id") == name)
                    .or_else(|| {
                        streams
                            .iter()
                            .find(|candidate| str_of(candidate, "name") == name)
                    })
                else {
                    missing.push(name.clone());
                    continue;
                };
                let label = str_of(found, "name");
                catalogue.streams.insert(
                    str_of(found, "id").to_string(),
                    StreamEntry {
                        name: name.clone(),
                        label: if label.is_empty() { name } else { label }.to_string(),
                        unit: str_of(found, "unit").to_string(),
                    },
                );
            }
        }

        if !missing.is_empty() {
            bail!(
                "the indu input could not find {} on {}: {} — a sensor is `<device>/<sensor>` by \
                 the platform's ids, a stream by the name it was written under, and the key must \
                 be allowed to see them",
                if missing.len() == 1 {
                    "this series"
                } else {
                    "these series"
                },
                self.described,
                missing.join(", ")
            );
        }
        Ok(catalogue)
    }

    /// Open the subscription for everything in the catalogue.
    async fn subscribe(&self, catalogue: &Catalogue) -> Result<Subscription> {
        let mut url = self.api(SSE_PATH)?;
        {
            let mut query = url.query_pairs_mut();
            if !catalogue.sensors.is_empty() {
                query.append_pair(
                    "sensors",
                    &catalogue
                        .sensors
                        .keys()
                        .cloned()
                        .collect::<Vec<_>>()
                        .join(","),
                );
            }
            if !catalogue.streams.is_empty() {
                query.append_pair(
                    "streams",
                    &catalogue
                        .streams
                        .keys()
                        .cloned()
                        .collect::<Vec<_>>()
                        .join(","),
                );
            }
            query.append_pair("backfill", if self.backfill { "true" } else { "false" });
        }
        let response = self
            .client
            .get(url)
            .header(AUTHORIZATION, self.credential.clone())
            .header(ACCEPT, HeaderValue::from_static("text/event-stream"))
            .send()
            .await
            .with_context(|| format!("subscribing to indu at {}", self.described))?;
        let status = response.status();
        if !status.is_success() {
            let detail = response
                .json::<Value>()
                .await
                .ok()
                .and_then(|problem| problem.get("detail")?.as_str().map(str::to_string))
                .unwrap_or_default();
            bail!(
                "indu refused the subscription with {status}{}",
                if detail.is_empty() {
                    String::new()
                } else {
                    format!(": {detail}")
                }
            );
        }
        Ok(Subscription {
            body: Box::pin(response.bytes_stream()),
            buffer: String::new(),
        })
    }
}

impl InduInput {
    /// Resolve and subscribe, or report why not and wait before trying
    /// again — never gives up. One error on the card per outage, on the
    /// attempt that starts it; a log line on the one that ends it.
    async fn connect(&mut self) -> Subscription {
        loop {
            match self.try_connect().await {
                Ok(subscription) => {
                    if self.backoff.is_failing() {
                        tracing::info!("indu input reconnected to {}", self.api.described);
                    }
                    self.backoff.succeeded();
                    return subscription;
                }
                Err(e) => {
                    if !self.backoff.is_failing() {
                        tracing::error!(
                            "indu input on {} is not subscribed, retrying: {e:?}",
                            self.api.described
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

    async fn try_connect(&mut self) -> Result<Subscription> {
        if self.catalogue.is_none() {
            self.catalogue = Some(self.api.resolve().await?);
        }
        let catalogue = self
            .catalogue
            .as_ref()
            .ok_or_else(|| anyhow!("the indu input has no catalogue"))?;
        self.api.subscribe(catalogue).await
    }

    /// The name a platform id was configured as, for a complaint a person
    /// can act on.
    fn name_of(&self, id: &str) -> String {
        self.catalogue
            .as_ref()
            .and_then(|catalogue| {
                catalogue
                    .sensors
                    .get(id)
                    .map(|entry| entry.name.clone())
                    .or_else(|| catalogue.streams.get(id).map(|entry| entry.name.clone()))
            })
            .unwrap_or_else(|| id.to_string())
    }

    /// Act on one platform event: queue a reading, report what deserves
    /// reporting. `false` when the connection is no longer good.
    fn handle(&mut self, event: &SseEvent) -> bool {
        let data: Value = match serde_json::from_str(&event.data) {
            Ok(data) => data,
            Err(e) => {
                tracing::warn!("skipping an indu event that is not json: {e}");
                return true;
            }
        };
        match event.name.as_str() {
            "reading" | "stream_reading" => {
                let Some(catalogue) = self.catalogue.as_ref() else {
                    return true;
                };
                let Some(message) = message_of(catalogue, event, &data) else {
                    tracing::warn!(
                        "skipping an indu {} for a series not subscribed",
                        event.name
                    );
                    return true;
                };
                let own: Meta = if self.envelope.is_enabled() {
                    vec![("event", Value::String(event.name.clone()))]
                } else {
                    Vec::new()
                };
                // The message is an object this input built, so a `merge`
                // envelope always has somewhere to go.
                if let Some(enveloped) = self.envelope.apply(message, own) {
                    self.pending.push_back(enveloped);
                }
                true
            }
            "subscribed" => {
                let mut rejected: Vec<String> = ids_in(data.get("rejected"))
                    .into_iter()
                    .chain(ids_in(data.get("rejected_streams")))
                    .map(|id| self.name_of(id))
                    .collect();
                if !rejected.is_empty() {
                    rejected.sort();
                    let e = anyhow!(
                        "indu did not subscribe {}: {} — outside what the key may see, or no \
                         longer there",
                        if rejected.len() == 1 {
                            "this series"
                        } else {
                            "these series"
                        },
                        rejected.join(", ")
                    );
                    tracing::warn!("{e}");
                    publish(&self.events, || {
                        UiEvent::error(self.pipeline_id.clone(), Stage::Input, &e)
                    });
                }
                true
            }
            "lagged" => {
                let dropped = data.get("dropped").and_then(Value::as_u64).unwrap_or(0);
                let e = anyhow!(
                    "indu dropped {dropped} readings this input could not keep up with; the \
                     historian has them, this pipeline does not"
                );
                tracing::warn!("{e}");
                publish(&self.events, || {
                    UiEvent::error(self.pipeline_id.clone(), Stage::Input, &e)
                });
                true
            }
            "error" => {
                let fatal = data.get("fatal").and_then(Value::as_bool).unwrap_or(false);
                let message = str_of(&data, "message").to_string();
                let e = anyhow!("indu reported an error on the subscription: {message}");
                tracing::warn!("{e}");
                publish(&self.events, || {
                    UiEvent::error(self.pipeline_id.clone(), Stage::Input, &e)
                });
                !fatal
            }
            // `ready`, `pong`, `alert_event`, `device_status`: nothing to read
            _ => true,
        }
    }
}

#[async_trait::async_trait]
impl InputSource for InduInput {
    async fn next(&mut self) -> Result<Delivery> {
        loop {
            // Whatever the last chunk left over goes first; a chunk that
            // carried several readings is several passes, not one.
            if !self.pending.is_empty() {
                let mut batch: MessageBatch = Vec::new();
                while batch.len() < self.max_batch {
                    let Some(message) = self.pending.pop_front() else {
                        break;
                    };
                    batch.push(Arc::new(message));
                }
                return Ok(Delivery::new(Arc::new(batch)));
            }

            if self.subscription.is_none() {
                self.subscription = Some(self.connect().await);
            }
            let Some(subscription) = self.subscription.as_mut() else {
                continue;
            };

            match subscription.body.next().await {
                Some(Ok(chunk)) => {
                    subscription
                        .buffer
                        .push_str(&String::from_utf8_lossy(&chunk));
                    let events = take_events(&mut subscription.buffer);
                    for event in events {
                        if !self.handle(&event) {
                            self.subscription = None;
                            break;
                        }
                    }
                }
                Some(Err(e)) => {
                    tracing::warn!(
                        "indu input on {} lost its subscription: {e}",
                        self.api.described
                    );
                    self.subscription = None;
                    // An outage starts here, not on the reconnect attempt: the
                    // next connect reports it if it fails, and the backoff
                    // paces it either way.
                    tokio::time::sleep(self.backoff.failed()).await;
                }
                None => {
                    tracing::warn!("indu input on {} was disconnected", self.api.described);
                    self.subscription = None;
                    tokio::time::sleep(self.backoff.failed()).await;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::MapSecretStore;
    use axum::extract::{Path, Query, State};
    use axum::http::{HeaderMap, StatusCode};
    use axum::routing::get;
    use axum::{Json, Router};
    use kayak_core::EventPayload;
    use kayak_core::config::Secret;
    use kayak_core::connections::{ConnectionKind, Connections, InduConnection};
    use std::collections::HashMap;
    use std::fmt::Write as _;
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn ok<T, E: std::fmt::Display>(result: std::result::Result<T, E>) -> T {
        result.unwrap_or_else(|e| panic!("the test could not get this far: {e}"))
    }

    fn connections(url: &str) -> Arc<Connections> {
        Arc::new(
            [(
                "indu".to_string(),
                ConnectionKind::Indu(InduConnection {
                    url: url.to_string(),
                    ingest_url: None,
                    api_key: Secret::new("${INDU_API_KEY}"),
                }),
            )]
            .into_iter()
            .collect(),
        )
    }

    fn build(
        config: InduInputConfig,
        url: &str,
    ) -> (Result<Box<dyn InputSource>>, broadcast::Receiver<UiEvent>) {
        let mut pipelines = HashMap::new();
        let (events, rx) = tokio::sync::broadcast::channel(16);
        let secrets = Arc::new(MapSecretStore::new(
            "a test store",
            &[("INDU_API_KEY", "indu.ak.test.secret"), ("BLANK", "")],
        ));
        let mut ctx = BuildCtx::with_secrets(&mut pipelines, "p".to_string(), events, secrets)
            .with_connections(connections(url));
        (config.build(&mut ctx), rx)
    }

    fn config() -> InduInputConfig {
        InduInputConfig {
            connection: "indu".to_string(),
            sensors: vec!["press-3/temperature".to_string()],
            streams: vec!["press-3/oee".to_string()],
            backfill: None,
            max_batch: None,
        }
    }

    const SENSOR: &str = "11111111-1111-4111-8111-111111111111";
    const DEVICE: &str = "22222222-2222-4222-8222-222222222222";
    const STREAM: &str = "33333333-3333-4333-8333-333333333333";
    const OTHER: &str = "44444444-4444-4444-8444-444444444444";

    /// A stand-in for the platform: the three catalogue endpoints and the
    /// subscription, which answers one scripted body per connection.
    #[derive(Default)]
    struct Platform {
        /// The bodies to serve, one per connection, in order. The last is
        /// repeated.
        bodies: Mutex<Vec<String>>,
        connections: AtomicUsize,
        /// The query string each subscription arrived with.
        queries: Mutex<Vec<String>>,
        authorizations: Mutex<Vec<String>>,
        /// Whether the catalogue knows the stream at all.
        has_stream: bool,
    }

    fn sse(events: &[(&str, Value)]) -> String {
        let mut out = String::from(": heartbeat\n\n");
        for (index, (name, data)) in events.iter().enumerate() {
            let _ = write!(out, "id: {}\nevent: {name}\ndata: {data}\n\n", index + 1);
        }
        out
    }

    async fn devices(
        State(platform): State<Arc<Platform>>,
        Query(query): Query<HashMap<String, String>>,
        headers: HeaderMap,
    ) -> Json<Value> {
        ok(platform.authorizations.lock()).push(
            headers
                .get(AUTHORIZATION)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_string(),
        );
        let q = query.get("q").cloned().unwrap_or_default();
        let all = vec![
            json!({"id": DEVICE, "external_id": "press-3", "name": "Press 3"}),
            json!({"id": OTHER, "external_id": "press-30", "name": "Press 30"}),
        ];
        Json(
            json!({"data": all.into_iter().filter(|d| str_of(d, "external_id").contains(&q)).collect::<Vec<_>>()}),
        )
    }

    async fn sensors(Path(id): Path<String>) -> (StatusCode, Json<Value>) {
        if id != DEVICE {
            return (
                StatusCode::NOT_FOUND,
                Json(json!({"detail": "no such device"})),
            );
        }
        (
            StatusCode::OK,
            Json(json!({"data": [
                {"id": SENSOR, "external_id": "temperature", "name": "Temperature", "unit": "°C", "data_type": "number", "status": "active"},
                {"id": OTHER, "external_id": "pressure", "name": "Pressure", "unit": "bar", "data_type": "number", "status": "active"}
            ]})),
        )
    }

    async fn streams(State(platform): State<Arc<Platform>>) -> Json<Value> {
        let mut all =
            vec![json!({"id": OTHER, "name": "press-3 smoothed", "external_id": null, "unit": ""})];
        if platform.has_stream {
            all.push(json!({"id": STREAM, "name": "press-3/oee", "external_id": "press-3/oee", "unit": "%"}));
        }
        Json(Value::Array(all))
    }

    async fn live(
        State(platform): State<Arc<Platform>>,
        Query(query): Query<Vec<(String, String)>>,
        headers: HeaderMap,
    ) -> (StatusCode, [(&'static str, &'static str); 1], String) {
        let n = platform.connections.fetch_add(1, Ordering::SeqCst);
        ok(platform.queries.lock()).push(
            query
                .iter()
                .map(|(k, v)| format!("{k}={v}"))
                .collect::<Vec<_>>()
                .join("&"),
        );
        if headers.get(AUTHORIZATION).and_then(|v| v.to_str().ok())
            != Some("Bearer indu.ak.test.secret")
        {
            return (
                StatusCode::UNAUTHORIZED,
                [("content-type", "application/json")],
                json!({"detail": "the credential was not accepted"}).to_string(),
            );
        }
        let bodies = ok(platform.bodies.lock());
        let body = bodies
            .get(n)
            .or_else(|| bodies.last())
            .cloned()
            .unwrap_or_default();
        (
            StatusCode::OK,
            [("content-type", "text/event-stream")],
            body,
        )
    }

    async fn serve(platform: Platform) -> (String, Arc<Platform>) {
        let platform = Arc::new(platform);
        let app = Router::new()
            .route(DEVICES_PATH, get(devices))
            .route("/api/v1/devices/{id}/sensors", get(sensors))
            .route(STREAMS_PATH, get(streams))
            .route(SSE_PATH, get(live))
            .with_state(Arc::clone(&platform));
        let listener = ok(tokio::net::TcpListener::bind("127.0.0.1:0").await);
        let addr = ok(listener.local_addr());
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (format!("http://{addr}"), platform)
    }

    fn one(delivery: Delivery) -> Value {
        let batch = delivery.batch;
        assert_eq!(batch.len(), 1, "{batch:?}");
        (*batch[0]).clone()
    }

    fn error_text(event: &UiEvent) -> String {
        match &event.payload {
            EventPayload::Error(text) => text.clone(),
            EventPayload::Batch(_) => panic!("expected an error event"),
        }
    }

    #[test]
    fn events_are_framed_across_chunks_and_line_endings() {
        let mut buffer = String::from(": hb\n\nevent: reading\ndata: {\"a\":");
        assert!(take_events(&mut buffer).is_empty(), "a partial event waits");
        buffer.push_str("1}\n\r\nevent: pong\r\ndata: {}\r\n\r\nid: 9\n");
        let events = take_events(&mut buffer);
        assert_eq!(
            events,
            vec![
                SseEvent {
                    name: "reading".into(),
                    data: "{\"a\":1}".into()
                },
                SseEvent {
                    name: "pong".into(),
                    data: "{}".into()
                },
            ]
        );
        assert_eq!(buffer, "id: 9\n", "the tail of the next event stays");
    }

    #[test]
    fn nothing_to_read_a_bad_name_and_an_empty_key_are_refused_at_build() {
        let (built, _) = build(
            InduInputConfig {
                sensors: vec![],
                streams: vec![" ".to_string()],
                ..config()
            },
            "http://127.0.0.1:59999",
        );
        let Err(err) = built else {
            panic!("an indu input with nothing to read built");
        };
        let text = format!("{err:#}");
        assert!(text.contains("at least one"), "{text}");

        let (built, _) = build(
            InduInputConfig {
                sensors: vec!["temperature".to_string()],
                ..config()
            },
            "http://127.0.0.1:59999",
        );
        let Err(err) = built else {
            panic!("an indu input with a bare sensor name built");
        };
        let text = format!("{err:#}");
        assert!(text.contains("<device>/<sensor>"), "{text}");

        let mut pipelines = HashMap::new();
        let (events, _rx) = tokio::sync::broadcast::channel(4);
        let secrets = Arc::new(MapSecretStore::new("a test store", &[("BLANK", "")]));
        let mut ctx = BuildCtx::with_secrets(&mut pipelines, "p".to_string(), events, secrets)
            .with_connections(Arc::new(
                [(
                    "indu".to_string(),
                    ConnectionKind::Indu(InduConnection {
                        url: "http://127.0.0.1:59999".to_string(),
                        ingest_url: None,
                        api_key: Secret::new("${BLANK}"),
                    }),
                )]
                .into_iter()
                .collect(),
            ));
        let Err(err) = config().build(&mut ctx) else {
            panic!("an indu input with an empty key built");
        };
        let text = format!("{err:#}");
        assert!(text.contains("empty api key"), "{text}");
    }

    #[tokio::test]
    async fn names_resolve_and_readings_become_named_messages() {
        let (url, platform) = serve(Platform {
            bodies: Mutex::new(vec![sse(&[
                ("ready", json!({"tenant_id": "t", "user_id": "k"})),
                ("subscribed", json!({"sensors": [SENSOR], "streams": [STREAM]})),
                ("reading", json!({"sensor_id": SENSOR, "device_id": DEVICE, "ts": 1_756_800_000_000_i64, "v": 71.2})),
                ("stream_reading", json!({"stream_id": STREAM, "ts": 1_756_800_001_000_i64, "v": 0.83})),
                ("reading", json!({"sensor_id": OTHER, "device_id": DEVICE, "ts": 1, "v": 0})),
            ])]),
            has_stream: true,
            ..Platform::default()
        })
        .await;
        let (built, _rx) = build(config(), &url);
        let mut input = ok(built);

        let first = one(ok(input.next().await));
        assert_eq!(first["kind"], "sensor");
        assert_eq!(first["name"], "press-3/temperature");
        assert_eq!(first["device"], "press-3");
        assert_eq!(first["sensor"], "temperature");
        assert_eq!(first["label"], "Temperature");
        assert_eq!(first["unit"], "°C");
        assert_eq!(first["sensor_id"], SENSOR);
        assert_eq!(first["device_id"], DEVICE);
        assert_eq!(first["ts"], 1_756_800_000_000_i64);
        assert_eq!(first["at"], "2025-09-02T08:00:00+00:00");
        assert_eq!(first["value"], 71.2);

        let second = one(ok(input.next().await));
        assert_eq!(second["kind"], "stream");
        assert_eq!(second["name"], "press-3/oee");
        assert_eq!(second["label"], "press-3/oee");
        assert_eq!(second["unit"], "%");
        assert_eq!(second["stream_id"], STREAM);
        assert_eq!(second["value"], 0.83);

        // The subscription asked for exactly the resolved ids, with backfill
        // on by default, under the key.
        let queries = ok(platform.queries.lock()).clone();
        assert_eq!(
            queries[0],
            format!("sensors={SENSOR}&streams={STREAM}&backfill=true")
        );
        assert!(
            ok(platform.authorizations.lock())
                .iter()
                .all(|a| a == "Bearer indu.ak.test.secret")
        );
        // The reading for a sensor nobody asked for was skipped, not emitted:
        // the next thing to arrive is a reconnect, which is the body again.
        drop(input);
    }

    #[tokio::test]
    async fn an_unknown_name_is_reported_with_the_names_missing_and_retried() {
        let (url, platform) = serve(Platform {
            bodies: Mutex::new(vec![sse(&[])]),
            has_stream: false,
            ..Platform::default()
        })
        .await;
        let (built, mut rx) = build(
            InduInputConfig {
                sensors: vec![
                    "press-3/temperature".to_string(),
                    "press-3/humidity".to_string(),
                    "press-9/temperature".to_string(),
                ],
                ..config()
            },
            &url,
        );
        let mut input = ok(built);
        let reader = tokio::spawn(async move { input.next().await });

        let event = ok(tokio::time::timeout(Duration::from_secs(5), rx.recv()).await);
        let text = error_text(&ok(event));
        assert!(text.contains("could not find these series"), "{text}");
        assert!(text.contains("press-3/humidity"), "{text}");
        assert!(text.contains("press-9/temperature"), "{text}");
        assert!(text.contains("press-3/oee"), "{text}");
        assert!(
            !text.contains("press-3/temperature,"),
            "the found one is not listed: {text}"
        );
        // No subscription was opened over a half-resolved catalogue.
        assert_eq!(platform.connections.load(Ordering::SeqCst), 0);
        reader.abort();
    }

    #[tokio::test]
    async fn backfill_false_is_asked_for_and_a_rejected_series_is_reported() {
        let (url, platform) = serve(Platform {
            bodies: Mutex::new(vec![sse(&[
                (
                    "subscribed",
                    json!({"sensors": [SENSOR], "rejected_streams": [STREAM]}),
                ),
                (
                    "reading",
                    json!({"sensor_id": SENSOR, "device_id": DEVICE, "ts": 5, "v": "RUNNING"}),
                ),
            ])]),
            has_stream: true,
            ..Platform::default()
        })
        .await;
        let (built, mut rx) = build(
            InduInputConfig {
                backfill: Some(false),
                ..config()
            },
            &url,
        );
        let mut input = ok(built);
        let message = one(ok(input.next().await));
        assert_eq!(message["value"], "RUNNING", "a string value passes through");
        assert!(ok(platform.queries.lock())[0].ends_with("backfill=false"));

        let text = error_text(&ok(rx.try_recv()));
        assert!(
            text.contains("did not subscribe this series: press-3/oee"),
            "{text}"
        );
    }

    #[tokio::test]
    async fn a_dropped_connection_reconnects_and_lagged_is_an_error() {
        let (url, platform) = serve(Platform {
            bodies: Mutex::new(vec![
                sse(&[(
                    "reading",
                    json!({"sensor_id": SENSOR, "device_id": DEVICE, "ts": 1, "v": 1}),
                )]),
                sse(&[
                    ("lagged", json!({"dropped": 12})),
                    (
                        "reading",
                        json!({"sensor_id": SENSOR, "device_id": DEVICE, "ts": 2, "v": 2}),
                    ),
                ]),
            ]),
            has_stream: true,
            ..Platform::default()
        })
        .await;
        let (built, mut rx) = build(config(), &url);
        let mut input = ok(built);

        assert_eq!(one(ok(input.next().await))["value"], 1);
        // The first body ends there; the second connection carries on.
        let second = ok(tokio::time::timeout(Duration::from_secs(10), input.next()).await);
        assert_eq!(one(ok(second))["value"], 2);
        assert_eq!(platform.connections.load(Ordering::SeqCst), 2);

        let text = error_text(&ok(rx.try_recv()));
        assert!(text.contains("dropped 12 readings"), "{text}");
    }

    #[tokio::test]
    async fn max_batch_coalesces_what_one_chunk_carried() {
        let (url, _platform) = serve(Platform {
            bodies: Mutex::new(vec![sse(&[
                (
                    "reading",
                    json!({"sensor_id": SENSOR, "device_id": DEVICE, "ts": 1, "v": 1}),
                ),
                (
                    "reading",
                    json!({"sensor_id": SENSOR, "device_id": DEVICE, "ts": 2, "v": 2}),
                ),
                (
                    "reading",
                    json!({"sensor_id": SENSOR, "device_id": DEVICE, "ts": 3, "v": 3}),
                ),
            ])]),
            has_stream: true,
            ..Platform::default()
        })
        .await;
        let (built, _rx) = build(
            InduInputConfig {
                max_batch: Some(2),
                ..config()
            },
            &url,
        );
        let mut input = ok(built);
        let batch = ok(input.next().await);
        assert_eq!(batch.batch.len(), 2);
        let batch = ok(input.next().await);
        assert_eq!(batch.batch.len(), 1);
    }

    #[tokio::test]
    async fn a_refused_key_is_an_error_on_the_card_not_a_panic() {
        let (url, _platform) = serve(Platform {
            bodies: Mutex::new(vec![sse(&[])]),
            has_stream: true,
            ..Platform::default()
        })
        .await;
        let mut pipelines = HashMap::new();
        let (events, mut rx) = tokio::sync::broadcast::channel(16);
        let secrets = Arc::new(MapSecretStore::new(
            "a test store",
            &[("INDU_API_KEY", "indu.ak.wrong.secret")],
        ));
        let mut ctx = BuildCtx::with_secrets(&mut pipelines, "p".to_string(), events, secrets)
            .with_connections(connections(&url));
        let mut input = ok(config().build(&mut ctx));
        let reader = tokio::spawn(async move { input.next().await });
        let event = ok(tokio::time::timeout(Duration::from_secs(5), rx.recv()).await);
        let text = error_text(&ok(event));
        assert!(text.contains("401"), "{text}");
        assert!(text.contains("the credential was not accepted"), "{text}");
        reader.abort();
    }
}
