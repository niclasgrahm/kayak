use crate::{
    BuildCtx,
    backoff::Backoff,
    events::publish,
    inputs::{
        BuildInput, InputSource, MessageBatch,
        ack::{Ack, Delivery},
        envelope::{Envelope, Meta},
    },
    secrets::Resolved,
    state::{PipelineId, UiEvent},
};
use anyhow::{Context, Result};
use futures_util::FutureExt;
use kayak_core::{
    Stage,
    config::{AckMode, MqttConfig, MqttQos},
};
use rumqttc::{AsyncClient, Event, EventLoop, MqttOptions, Packet, Publish, QoS};
use serde_json::Value;
use std::sync::Arc;
use tokio::sync::broadcast;

fn to_rumqttc_qos(qos: MqttQos) -> QoS {
    match qos {
        MqttQos::AtMostOnce => QoS::AtMostOnce,
        MqttQos::AtLeastOnce => QoS::AtLeastOnce,
        MqttQos::ExactlyOnce => QoS::ExactlyOnce,
    }
}

impl BuildInput for MqttConfig {
    fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn InputSource>> {
        let broker = ctx
            .mqtt_connection(&self.connection)
            .context("the mqtt input cannot be built")?;
        let qos = self.qos.unwrap_or(MqttQos::AtMostOnce);
        let ack_mode = ctx.ack_mode();
        // a qos-0 subscription has no broker-side "received" vs "delivered" —
        // the broker never resends and there is nothing to ack — so refuse
        // rather than silently behaving like `on_receipt`, the same rule
        // `ack::require_receipt_only` enforces for inputs with no ack at all
        anyhow::ensure!(
            !(ack_mode == AckMode::OnDelivery && qos == MqttQos::AtMostOnce),
            "the mqtt input's `ack: on_delivery` needs at least `at_least_once` qos on the \
             subscription — a qos `at_most_once` message is never resent and has nothing for \
             an ack to hold open"
        );
        let username = broker
            .username
            .as_ref()
            .map(|u| ctx.resolve(u))
            .transpose()
            .with_context(|| {
                format!(
                    "failed to resolve secrets in the username of connection '{}'",
                    self.connection
                )
            })?;
        let password = broker
            .password
            .as_ref()
            .map(|p| ctx.resolve(p))
            .transpose()
            .with_context(|| {
                format!(
                    "failed to resolve secrets in the password of connection '{}'",
                    self.connection
                )
            })?;
        anyhow::ensure!(
            username.is_some() == password.is_some(),
            "mqtt connection '{}' sets `username` or `password` without the other; they must \
             be set together or not at all",
            self.connection
        );
        Ok(Box::new(MqttInput {
            host: broker.host.clone(),
            port: broker.port.unwrap_or(1883),
            username,
            password,
            // stable across restarts and unique to this pipeline/topic pair —
            // not configurable, see the doc comment on `MqttConfig`
            client_id: format!("kayak-{}-{}", ctx.pipeline_id, self.topic),
            topic: self.topic,
            qos: to_rumqttc_qos(qos),
            max_batch: crate::inputs::batch_cap(self.max_batch),
            envelope: ctx.envelope("mqtt", Some(&self.connection)),
            ack_mode,
            connection_name: self.connection,
            client_eventloop: None,
            pipeline_id: ctx.pipeline_id.clone(),
            events: ctx.events.clone(),
            backoff: Backoff::new(),
        }))
    }
}

pub struct MqttInput {
    host: String,
    port: u16,
    username: Option<Resolved>,
    password: Option<Resolved>,
    client_id: String,
    topic: String,
    qos: QoS,
    /// Most messages in one batch. One unless the config says otherwise — see
    /// [`MqttInput::next`].
    max_batch: usize,
    /// What this input attaches to each message, if the config asked for any.
    envelope: Envelope,
    /// `on_receipt` (the default) or `on_delivery` — decides both whether the
    /// client is told to leave acking to us and what `next` hands back as an
    /// [`Ack`]. See the `ack` module docs.
    ack_mode: AckMode,
    connection_name: String,
    /// Built on the first read, like the nats and kafka inputs: `build()` must
    /// not block on a broker that isn't up yet.
    client_eventloop: Option<(AsyncClient, EventLoop)>,
    pipeline_id: PipelineId,
    events: broadcast::Sender<UiEvent>,
    /// Paces reconnect attempts after the eventloop fails — see
    /// [`MqttInput::next`].
    backoff: Backoff,
}

impl MqttInput {
    fn connect(&self) -> (AsyncClient, EventLoop) {
        let mut options = MqttOptions::new(self.client_id.clone(), self.host.clone(), self.port);
        if let (Some(username), Some(password)) = (&self.username, &self.password) {
            options.set_credentials(username.expose(), password.expose());
        }
        // `on_receipt` leaves this false: the client acks a qos>0 message the
        // moment it hands it to us, before this pipeline has done anything
        // with it, same as kafka's auto-commit. `on_delivery` turns that off,
        // which is what makes `MqttAck::ack` the only thing that acks at all.
        if self.ack_mode == AckMode::OnDelivery {
            options.set_manual_acks(true);
        }
        AsyncClient::new(options, 100)
    }

    /// Connects and subscribes, retrying on failure — never gives up.
    ///
    /// This alone does **not** prove the broker is reachable: `AsyncClient`
    /// only ever queues requests onto a channel the eventloop drains, so
    /// `connect`/`subscribe` succeed at once whether or not anything is
    /// listening on the other end. The real handshake happens the first time
    /// the eventloop is polled, in `next` — which is why that path has its
    /// own call into [`MqttInput::report_failure`] rather than trusting this
    /// one to have already paced things. This still retries a subscribe that
    /// fails outright (the channel itself gone, say), for whatever reason
    /// that can happen.
    async fn reconnect(&mut self) -> (AsyncClient, EventLoop) {
        loop {
            let (client, eventloop) = self.connect();
            match client.subscribe(self.topic.clone(), self.qos).await {
                Ok(()) => return (client, eventloop),
                Err(e) => {
                    let e = anyhow::Error::new(e).context(format!(
                        "failed to subscribe to mqtt topic '{}'",
                        self.topic
                    ));
                    let delay = self.report_failure(&e);
                    tokio::time::sleep(delay).await;
                }
            }
        }
    }

    /// Records a failure — logging and publishing a [`UiEvent::error`] once
    /// per outage, on the attempt that starts it, not on every retry — and
    /// returns how long to wait before trying again. Doesn't sleep itself: a
    /// caller that already has a batch in hand (the opportunistic second
    /// loop in `next`) wants the failure recorded without delaying what it's
    /// about to return; the blocking first loop awaits the delay directly.
    fn report_failure(&mut self, e: &anyhow::Error) -> std::time::Duration {
        if !self.backoff.is_failing() {
            tracing::error!(
                "mqtt input on topic '{}' lost its connection, retrying: {e:?}",
                self.topic
            );
            publish(&self.events, || {
                UiEvent::error(self.pipeline_id.clone(), Stage::Input, e)
            });
        }
        self.backoff.failed()
    }
}

/// What this input knows about one message: the concrete topic it arrived
/// on — the whole reason a wildcard subscription is worth anything — plus the
/// qos it was delivered at and whether it was a retained message.
fn meta_of(connection_name: &str, publish: &Publish) -> Meta {
    vec![
        ("connection", Value::String(connection_name.to_string())),
        ("topic", Value::String(publish.topic.clone())),
        (
            "qos",
            Value::String(
                match publish.qos {
                    QoS::AtMostOnce => "at_most_once",
                    QoS::AtLeastOnce => "at_least_once",
                    QoS::ExactlyOnce => "exactly_once",
                }
                .to_string(),
            ),
        ),
        ("retain", Value::Bool(publish.retain)),
    ]
}

/// The next record's JSON. `None` means the record was skipped — not JSON —
/// and the caller should ask again.
///
/// A free function taking its inputs explicitly, rather than a `&self`
/// method: `MqttInput::next` holds a mutable borrow of `client_eventloop`
/// across the loop this is called from, and a `&self` method call there would
/// need the whole of `self` borrowed immutably at the same time — the same
/// reason [`crate::inputs::nats::NatsInput::next`] builds its decoder as a
/// closure over the specific fields it needs instead of calling back into
/// `self`.
fn decode(envelope: &Envelope, connection_name: &str, publish: &Publish) -> Option<Value> {
    let value = match serde_json::from_slice(&publish.payload) {
        Ok(value) => value,
        Err(e) => {
            tracing::warn!(
                "skipping non-json message on mqtt topic '{}': {}",
                publish.topic,
                e
            );
            return None;
        }
    };
    let own = if envelope.is_enabled() {
        meta_of(connection_name, publish)
    } else {
        Vec::new()
    };
    let enveloped = envelope.apply(value, own);
    if enveloped.is_none() {
        tracing::warn!(
            "skipping a message on mqtt topic '{}': its payload is not a json object, so a \
             `merge` envelope has nowhere to attach metadata — use a `wrap` envelope for a \
             topic carrying bare values",
            publish.topic
        );
    }
    enveloped
}

/// [`Ack`] for `AckMode::OnDelivery`: acks every message this delivery
/// carried once the run loop says the batch has cleared this pipeline.
///
/// Every message read gets an entry, including ones `decode` skipped as
/// unparseable — those are handled too, just not forwarded, and leaving them
/// unacked would have the broker resend a message this input will only skip
/// again. `try_ack` rather than `ack`, the same rule the http input's
/// backpressure and `Inboxes::send` follow: a non-blocking, best-effort call
/// from a synchronous [`Ack::ack`] rather than something that could await.
struct MqttAck {
    client: AsyncClient,
    publishes: Vec<Publish>,
}

impl Ack for MqttAck {
    fn ack(&self) {
        for publish in &self.publishes {
            if let Err(e) = self.client.try_ack(publish) {
                tracing::warn!(
                    "failed to ack mqtt message on topic '{}': {}",
                    publish.topic,
                    e
                );
            }
        }
    }
}

#[async_trait::async_trait]
impl InputSource for MqttInput {
    /// Wait for one message, then take whatever else is *already* waiting, up
    /// to `max_batch` — the same shape [`crate::inputs::kafka::KafkaInput`]
    /// and [`crate::inputs::nats::NatsInput`] follow, for the same reason:
    /// this never waits for a batch to fill, so a quiet topic still yields
    /// batches of one however high `max_batch` is.
    ///
    /// Every event that isn't an incoming `Publish` — the connect/subscribe
    /// handshake, `PingResp`, an `Outgoing` record of what this client just
    /// sent — is silently passed over rather than ended the wait on: the
    /// eventloop has to be polled continuously for the connection to work at
    /// all, including to drive its own keepalive, and `Event` is how it
    /// reports every one of those alongside the messages this input actually
    /// wants.
    async fn next(&mut self) -> Result<Delivery> {
        // The outer loop is the reconnect path: an eventloop failure drops
        // the client and goes round again instead of returning an error, so
        // a broker outage costs this input a wait, not its life — see
        // `reconnect`.
        loop {
            if self.client_eventloop.is_none() {
                self.client_eventloop = Some(self.reconnect().await);
            }
            // Captured before the mutable borrow of `client_eventloop` below —
            // disjoint fields, so the borrow checker is fine with both being
            // alive at once, same ordering `NatsInput::next` uses.
            let envelope = &self.envelope;
            let connection_name = &self.connection_name;
            let ack_mode = self.ack_mode;
            let max_batch = self.max_batch;
            let host = &self.host;
            let (client, eventloop) = self
                .client_eventloop
                .as_mut()
                .ok_or_else(|| anyhow::anyhow!("mqtt client not initialized"))?;

            let mut batch: MessageBatch = Vec::new();
            let mut publishes: Vec<Publish> = Vec::new();
            // `rumqttc`'s `connect`/`subscribe` only ever queue a request —
            // see `reconnect` — so this first `poll()` is the actual
            // handshake, and its failure is the one this input mostly
            // retries on. Carried out of the loop as a value rather than
            // handled inline: reporting it needs `&mut self`, which the
            // borrow of `client_eventloop` below doesn't allow while it's
            // still in scope.
            let mut lost: Option<anyhow::Error> = None;
            while batch.is_empty() {
                match eventloop.poll().await {
                    Ok(Event::Incoming(Packet::Publish(publish))) => {
                        if ack_mode == AckMode::OnDelivery {
                            publishes.push(publish.clone());
                        }
                        if let Some(value) = decode(envelope, connection_name, &publish) {
                            batch.push(Arc::new(value));
                        }
                    }
                    Ok(_) => {}
                    Err(e) => {
                        lost = Some(
                            anyhow::Error::new(e)
                                .context(format!("mqtt connection to '{host}' failed")),
                        );
                        break;
                    }
                }
            }
            if let Some(e) = lost {
                self.client_eventloop = None;
                let delay = self.report_failure(&e);
                tokio::time::sleep(delay).await;
                continue;
            }
            // At least one message got through, which is the connection
            // proving itself alive — same rule `NatsInput`/`KafkaInput`
            // follow for their own first loop.
            self.backoff.succeeded();

            // Whatever else has *already* arrived, up to the cap. A failure
            // here doesn't cost the batch already collected — it's returned
            // now, and the reconnect happens on the next call, same as the
            // opportunistic loops in the nats and kafka inputs. The failure
            // is still recorded, so the next call's backoff continues this
            // outage's schedule rather than announcing a fresh one.
            let mut lost_after_batch: Option<anyhow::Error> = None;
            while batch.len() < max_batch {
                let Some(ready) = eventloop.poll().now_or_never() else {
                    break;
                };
                match ready {
                    Ok(Event::Incoming(Packet::Publish(publish))) => {
                        if ack_mode == AckMode::OnDelivery {
                            publishes.push(publish.clone());
                        }
                        if let Some(value) = decode(envelope, connection_name, &publish) {
                            batch.push(Arc::new(value));
                        }
                    }
                    Ok(_) => {}
                    Err(e) => {
                        lost_after_batch = Some(
                            anyhow::Error::new(e)
                                .context(format!("mqtt connection to '{host}' failed")),
                        );
                        break;
                    }
                }
            }

            let batch = Arc::new(batch);
            let delivery = match ack_mode {
                AckMode::OnReceipt => Delivery::new(batch),
                AckMode::OnDelivery => Delivery::with_ack(
                    batch,
                    Box::new(MqttAck {
                        client: client.clone(),
                        publishes,
                    }) as Box<dyn Ack>,
                ),
            };
            if let Some(e) = lost_after_batch {
                self.client_eventloop = None;
                self.report_failure(&e);
            }
            return Ok(delivery);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kayak_core::connections::{ConnectionKind, Connections, MqttConnection};
    use std::collections::HashMap;
    use std::time::Duration;

    fn build(qos: Option<MqttQos>, ack_mode: Option<AckMode>) -> Result<Box<dyn InputSource>> {
        let mut pipelines = HashMap::new();
        let (events, _rx) = tokio::sync::broadcast::channel(4);
        let connections: Connections = [(
            "broker".to_string(),
            ConnectionKind::Mqtt(MqttConnection {
                host: "localhost".to_string(),
                port: None,
                username: None,
                password: None,
            }),
        )]
        .into_iter()
        .collect();
        let mut ctx = BuildCtx::new(&mut pipelines, "p".to_string(), events)
            .with_connections(Arc::new(connections));
        ctx.ack_mode = ack_mode;
        MqttConfig {
            connection: "broker".to_string(),
            topic: "sensors/+/temperature".to_string(),
            qos,
            max_batch: None,
        }
        .build(&mut ctx)
    }

    /// The default mode over the default qos, and the common case: nothing to
    /// refuse.
    #[test]
    fn the_default_mode_and_qos_build_together() {
        assert!(build(None, None).is_ok());
    }

    /// A qos-0 subscription has no broker-side redelivery, so there is
    /// nothing `on_delivery` could hold open — refused rather than silently
    /// behaving like `on_receipt`.
    #[test]
    fn on_delivery_over_at_most_once_is_refused() {
        let Err(err) = build(Some(MqttQos::AtMostOnce), Some(AckMode::OnDelivery)) else {
            panic!("on_delivery built over an at_most_once subscription, which cannot ack");
        };
        assert!(format!("{err:#}").contains("at_least_once"), "{err:#}");
    }

    /// The whole point of allowing it: at qos 1 or 2 there is something for
    /// `on_delivery` to hold open.
    #[test]
    fn on_delivery_over_at_least_once_or_exactly_once_builds() {
        assert!(build(Some(MqttQos::AtLeastOnce), Some(AckMode::OnDelivery)).is_ok());
        assert!(build(Some(MqttQos::ExactlyOnce), Some(AckMode::OnDelivery)).is_ok());
    }

    #[test]
    fn a_username_without_a_password_is_refused() {
        let mut pipelines = HashMap::new();
        let (events, _rx) = tokio::sync::broadcast::channel(4);
        let connections: Connections = [(
            "broker".to_string(),
            ConnectionKind::Mqtt(MqttConnection {
                host: "localhost".to_string(),
                port: None,
                username: Some("kayak".into()),
                password: None,
            }),
        )]
        .into_iter()
        .collect();
        let mut ctx = BuildCtx::new(&mut pipelines, "p".to_string(), events)
            .with_connections(Arc::new(connections));
        let Err(err) = (MqttConfig {
            connection: "broker".to_string(),
            topic: "test".to_string(),
            qos: None,
            max_batch: None,
        })
        .build(&mut ctx) else {
            panic!("a username with no password built");
        };
        assert!(format!("{err:#}").contains("together"), "{err:#}");
    }

    /// The bug that shipped first: `connect`/`subscribe` queue a request
    /// without ever touching the network (see `MqttInput::reconnect`'s
    /// docs), so a naive read of "did reconnecting succeed?" says yes on
    /// every attempt and the backoff guarding it never engages — `next`
    /// spins as fast as the refused connection can be re-dialed. This pins
    /// that the *eventloop* failure, not the queue-only connect, is what
    /// paces the retry.
    ///
    /// Needs a real refused socket — a fake `EventLoop` isn't reachable
    /// through `rumqttc`'s types — so this is the network test the project's
    /// "genuinely can't be tested offline" carve-out covers; everything else
    /// about the pacing (the schedule itself) is covered by `backoff::tests`.
    ///
    /// Deliberately **not** `start_paused`: the bound has to be real wall
    /// time. A paused clock only auto-advances once the runtime is fully
    /// idle, and a regression to a tight loop never is — it would hang this
    /// test rather than fail it. Each iteration's real `connect` still
    /// yields at its `.await`, so a one-second real timeout fires whether
    /// the loop inside is paced or not.
    #[tokio::test]
    async fn a_refused_connection_is_retried_on_a_backoff_not_a_tight_loop() {
        // bound to port 0 to get one nothing else is using, then closed
        // immediately: connecting to it afterwards is refused deterministically
        // and fast, without depending on any particular port being free
        let listener = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap_or_else(|e| panic!("binding a throwaway listener: {e}"));
        let port = listener
            .local_addr()
            .unwrap_or_else(|e| panic!("reading the throwaway listener's port: {e}"))
            .port();
        drop(listener);

        let (events, _rx) = tokio::sync::broadcast::channel(4);
        let mut input = MqttInput {
            host: "127.0.0.1".to_string(),
            port,
            username: None,
            password: None,
            client_id: "kayak-test".to_string(),
            topic: "test".to_string(),
            qos: QoS::AtMostOnce,
            max_batch: 1,
            envelope: Envelope::none(),
            ack_mode: AckMode::OnReceipt,
            connection_name: "broker".to_string(),
            client_eventloop: None,
            pipeline_id: "test".to_string(),
            events,
            backoff: Backoff::new(),
        };

        // give it a bounded stretch of real time to retry in — it never
        // succeeds, since nothing is listening, so this always times out
        let _ = tokio::time::timeout(Duration::from_secs(1), input.next()).await;

        // a tight loop would have run this thousands of times over in one
        // second; the first backoff step alone is 250ms, so a paced retry
        // manages only a handful
        assert!(
            input.backoff.attempts() <= 10,
            "expected a handful of paced attempts in one second, got {} — the retry is not \
             backing off",
            input.backoff.attempts()
        );
    }
}
