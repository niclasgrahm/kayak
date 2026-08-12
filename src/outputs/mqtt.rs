use anyhow::Context;
use kayak_core::config::{MqttOutputConfig, MqttQos};
use rumqttc::{AsyncClient, MqttOptions, QoS};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use crate::{
    BuildCtx,
    backoff::Gate,
    inputs::MessageBatch,
    outputs::{BuildOutput, OutputDestination},
    secrets::Resolved,
};

fn to_rumqttc_qos(qos: MqttQos) -> QoS {
    match qos {
        MqttQos::AtMostOnce => QoS::AtMostOnce,
        MqttQos::AtLeastOnce => QoS::AtLeastOnce,
        MqttQos::ExactlyOnce => QoS::ExactlyOnce,
    }
}

impl BuildOutput for MqttOutputConfig {
    fn build(self, ctx: &mut BuildCtx) -> anyhow::Result<Box<dyn OutputDestination>> {
        let broker = ctx
            .mqtt_connection(&self.connection)
            .context("the mqtt output cannot be built")?;
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
        Ok(Box::new(MqttOutput {
            host: broker.host.clone(),
            port: broker.port.unwrap_or(1883),
            username,
            password,
            // stable across restarts and unique to this pipeline/topic pair —
            // not configurable, same as the mqtt input's
            client_id: format!("kayak-{}-{}-out", ctx.pipeline_id, self.topic),
            topic: self.topic,
            qos: to_rumqttc_qos(self.qos.unwrap_or(MqttQos::AtMostOnce)),
            retain: self.retain.unwrap_or(false),
            client: None,
            poller: None,
            alive: None,
            gate: Gate::new(),
        }))
    }
}

/// Publishes every message in a batch to one mqtt topic.
///
/// Unlike [`crate::outputs::nats::NatsOutput`], connecting is not enough on
/// its own: rumqttc splits a broker connection into an [`AsyncClient`] (a
/// handle that only ever *queues* requests) and an `EventLoop` that has to be
/// polled continuously for anything — the handshake, a publish actually
/// reaching the wire, its PUBACK coming back — to happen at all. `init`
/// therefore does two things `NatsOutput::init` doesn't need to: it polls once
/// itself so a broker that is down fails `init` (and so the pipeline) rather
/// than being discovered on the first `emit`, then hands the eventloop to a
/// background task that keeps polling for as long as this output lives.
pub struct MqttOutput {
    host: String,
    port: u16,
    username: Option<Resolved>,
    password: Option<Resolved>,
    client_id: String,
    topic: String,
    qos: QoS,
    retain: bool,
    client: Option<AsyncClient>,
    /// Kept so the poller dies with this output rather than outliving it.
    poller: Option<tokio::task::JoinHandle<()>>,
    /// Flipped to `false` by the poller when its eventloop ends — the only
    /// way `emit` can find out the connection died, since `publish` itself
    /// just queues onto a channel the (now-dead) poller would have drained
    /// and won't fail on its own. `None` before the first connect.
    alive: Option<Arc<AtomicBool>>,
    /// Paces reconnect attempts once `alive` says the connection is gone —
    /// see `emit`. `init` never consults it: a pipeline that can't reach its
    /// broker at startup still fails to build, same as always.
    gate: Gate,
}

impl Drop for MqttOutput {
    fn drop(&mut self) {
        if let Some(poller) = &self.poller {
            poller.abort();
        }
    }
}

impl MqttOutput {
    /// Connects, subscribes the eventloop to a background poller, and hands
    /// back the pieces `init` and `emit`'s reconnect both need. See the
    /// struct docs: the initial `poll()` here is what makes a down broker
    /// fail outright rather than being discovered on the first `emit`.
    async fn connect(
        &self,
    ) -> anyhow::Result<(AsyncClient, tokio::task::JoinHandle<()>, Arc<AtomicBool>)> {
        let mut options = MqttOptions::new(self.client_id.clone(), self.host.clone(), self.port);
        if let (Some(username), Some(password)) = (&self.username, &self.password) {
            options.set_credentials(username.expose(), password.expose());
        }
        let (client, mut eventloop) = AsyncClient::new(options, 100);
        eventloop.poll().await.with_context(|| {
            format!(
                "failed to connect to mqtt broker at {}:{}",
                self.host, self.port
            )
        })?;
        let alive = Arc::new(AtomicBool::new(true));
        let poller_alive = Arc::clone(&alive);
        let host = self.host.clone();
        let poller = tokio::spawn(async move {
            loop {
                if let Err(e) = eventloop.poll().await {
                    tracing::warn!("mqtt output connection to '{host}' ended: {e}");
                    break;
                }
            }
            poller_alive.store(false, Ordering::Relaxed);
        });
        Ok((client, poller, alive))
    }
}

#[async_trait::async_trait]
impl OutputDestination for MqttOutput {
    async fn init(&mut self) -> anyhow::Result<()> {
        let (client, poller, alive) = self.connect().await?;
        self.client = Some(client);
        self.poller = Some(poller);
        self.alive = Some(alive);
        Ok(())
    }

    async fn emit(&mut self, message_batch: Arc<MessageBatch>) -> anyhow::Result<()> {
        // The poller died (broker gone) since the last `emit` — the client
        // handle is still technically there, but nothing is driving its
        // eventloop any more, so treat it the same as never having connected.
        let dead = self
            .alive
            .as_ref()
            .is_some_and(|alive| !alive.load(Ordering::Relaxed));
        if dead {
            if let Some(poller) = self.poller.take() {
                poller.abort();
            }
            self.client = None;
            self.alive = None;
        }

        if self.client.is_none() {
            // Gated the same way the nats and postgres outputs are: a
            // reconnect is only attempted once the backoff window has
            // passed, so a downed broker gets one attempt every few seconds
            // rather than one on every batch.
            let now = Instant::now();
            if !self.gate.ready(now) {
                anyhow::bail!(
                    "mqtt output for topic '{}' at {}:{} is still unreachable; not retrying yet",
                    self.topic,
                    self.host,
                    self.port
                );
            }
            match self.connect().await {
                Ok((client, poller, alive)) => {
                    self.gate.record_success();
                    self.client = Some(client);
                    self.poller = Some(poller);
                    self.alive = Some(alive);
                }
                Err(e) => {
                    self.gate.record_failure(now);
                    return Err(e);
                }
            }
        }

        // silently doing nothing when init() never ran would look like the
        // messages were published, so make it an error instead
        let client = self.client.as_ref().ok_or_else(|| {
            anyhow::anyhow!("mqtt output is not connected; init() was not called")
        })?;
        for msg in message_batch.iter() {
            let payload =
                serde_json::to_vec(msg).context("failed to serialize message for mqtt")?;
            client
                .publish(self.topic.clone(), self.qos, self.retain, payload)
                .await
                .with_context(|| format!("failed to publish to mqtt topic '{}'", self.topic))?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kayak_core::connections::{ConnectionKind, Connections, MqttConnection};
    use std::collections::HashMap;

    fn build(username: Option<&str>, password: Option<&str>) -> anyhow::Result<Box<dyn OutputDestination>> {
        let mut pipelines = HashMap::new();
        let (events, _rx) = tokio::sync::broadcast::channel(4);
        let connections: Connections = [(
            "broker".to_string(),
            ConnectionKind::Mqtt(MqttConnection {
                host: "localhost".to_string(),
                port: None,
                username: username.map(Into::into),
                password: password.map(Into::into),
            }),
        )]
        .into_iter()
        .collect();
        let mut ctx = BuildCtx::new(&mut pipelines, "p".to_string(), events)
            .with_connections(Arc::new(connections));
        MqttOutputConfig {
            connection: "broker".to_string(),
            topic: "out/topic".to_string(),
            qos: None,
            retain: None,
        }
        .build(&mut ctx)
    }

    /// Building must not talk to the broker — a pipeline that starts is one
    /// whose settings parse, not one whose broker happened to be up. Nothing
    /// here connects until `init()`.
    #[test]
    fn building_does_not_require_a_broker() {
        assert!(build(None, None).is_ok());
    }

    #[test]
    fn credentials_are_refused_unless_both_are_set() {
        assert!(build(Some("kayak"), None).is_err());
        assert!(build(None, Some("hunter2")).is_err());
        assert!(build(Some("kayak"), Some("hunter2")).is_ok());
    }
}
