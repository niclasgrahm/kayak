//! Named connections to the systems pipelines talk to.
//!
//! A kafka cluster or a nats server is usually shared: one pipeline per topic on
//! the same brokers is the normal shape, and repeating the broker list — and its
//! `${NAME}` secret references — in every one of them is both tedious and a way
//! for them to drift apart. So the connection is declared *once*, under a name,
//! and a component refers to it:
//!
//! ```json
//! // connections.json
//! { "prod-kafka": { "type": "kafka", "brokers": "${KAFKA_BROKERS}" } }
//!
//! // config.json
//! { "type": "kafka", "connection": "prod-kafka", "topic": "orders", "group": "kayak" }
//! ```
//!
//! The split between the two is "what does the *system* need" against "what does
//! *this pipeline* want from it": brokers and credentials belong to the
//! connection, the topic and consumer group to the component. There is no inline
//! form — a component names a connection or it doesn't build.
//!
//! Connections hold [`Secret`]s, never resolved values, for the same reason
//! configs do: this crate compiles to wasm for the frontend, and the file is
//! meant to be committed. Resolution happens in the root crate at build time.
//!
//! A connection is *not* a runtime object. Nothing here opens a socket; two
//! pipelines naming one connection each get their own client, built from the
//! same settings. Pooling would be a separate change, and this is what it would
//! be built on.

use crate::config::Secret;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// What a connection is called. The name is the user's, and it is what a
/// component's `connection` field holds.
pub type ConnectionId = String;

/// A kafka cluster.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[schemars(title = "kafka")]
pub struct KafkaConnection {
    /// The brokers, as a list with commas, for example `localhost:9092`. You
    /// can use `${NAME}` secret references.
    pub brokers: Secret,
}

/// A nats server, or a cluster of nats servers.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[schemars(title = "nats")]
pub struct NatsConnection {
    /// The url of the server, for example `nats://localhost:4222`. You can use
    /// `${NAME}` secret references.
    pub urls: Secret,
}

/// A redis server, or a server that uses the same protocol.
///
/// kayak uses the pub/sub commands `SUBSCRIBE` and `PUBLISH`. It does not use
/// the key-value store. Thus, a redis input has the same delivery guarantees
/// as a nats input.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[schemars(title = "redis")]
pub struct RedisConnection {
    /// The url of the server, for example `redis://localhost:6379` or
    /// `redis://:${REDIS_PASSWORD}@localhost:6379/0`. You can use `${NAME}`
    /// secret references.
    pub url: Secret,
}

/// An mqtt broker.
///
/// The connection uses plain TCP. TLS is not available.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[schemars(title = "mqtt")]
pub struct MqttConnection {
    /// The hostname of the broker, for example `localhost`.
    pub host: String,
    /// The port of the broker. The default is 1883.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
    /// The username, if the broker requires one. Set `username` and
    /// `password` together, or set neither.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<Secret>,
    /// The password of the user. Use a `${NAME}` secret reference for this
    /// value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password: Option<Secret>,
}

/// An OPC UA server. The connection holds the endpoint. The `opcua` input
/// selects the nodes to read.
///
/// **The session is not signed and not encrypted.** kayak connects with the
/// security policy `None`, as an anonymous user or with a username and
/// password. The credentials go over the network as plain text. Use this
/// connection only on a network that you trust.
///
/// When a session opens, the OPC UA client writes two errors about a missing
/// application instance certificate to the log. This is not a fault. A session
/// with no encryption needs no certificate.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[schemars(title = "opcua")]
pub struct OpcuaConnection {
    /// The url of the endpoint, for example `opc.tcp://localhost:50000`. You
    /// can use `${NAME}` secret references.
    ///
    /// kayak connects directly to this url. It does not ask the server for its
    /// list of endpoints first. Thus, a server behind docker, NAT or a load
    /// balancer works when this url is correct.
    pub endpoint: Secret,
    /// The username, if the server requires one. Set `username` and
    /// `password` together, or set neither. Without them, the session is
    /// anonymous.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<Secret>,
    /// The password of the user. Use a `${NAME}` secret reference for this
    /// value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password: Option<Secret>,
}

/// A postgres database and the role that kayak connects as. The output or
/// the input sets the table.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[schemars(title = "postgres")]
pub struct PostgresConnection {
    /// The hostname of the server, for example `localhost`.
    pub host: String,
    /// The database to connect to.
    pub database: String,
    /// The role to connect as.
    pub user: String,
    /// The password of the role. Use a `${NAME}` secret reference for this
    /// value.
    pub password: Secret,
    /// The port of the server. The default is 5432.
    // omitted rather than written as `null` when absent, so a connection saved
    // back out is the file someone hand-wrote — same rule as an input's buffer
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
}

/// A directory on the filesystem of the server. A `file` output writes under
/// it, to a `path` relative to this directory.
///
/// The directory must be inside the `--data-dir` of the server. kayak checks
/// this when it builds the output. A server started without `--data-dir` has
/// no file output.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[schemars(title = "file")]
pub struct FileConnection {
    /// The directory that file outputs write under, for example
    /// `./out/events`. It must be inside the `--data-dir` of the server. If the
    /// directory does not exist, kayak makes it.
    pub root: String,
}

/// A bucket on an S3-compatible object store, and its credentials. An `s3`
/// output writes under a prefix in the bucket.
///
/// There is no limit like `--data-dir` for a bucket. The credentials set what
/// kayak can write. Give kayak a key that can write only to this bucket.
///
/// Set `endpoint` for rustfs, minio or another S3-compatible server. Without
/// `endpoint`, kayak uses AWS S3 in `region`.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[schemars(title = "s3")]
pub struct S3Connection {
    /// The bucket to write to. The bucket must exist. The output makes
    /// objects. It does not make buckets.
    pub bucket: String,
    /// The access key id. Use a `${NAME}` secret reference for this value.
    pub access_key_id: Secret,
    /// The secret access key. Use a `${NAME}` secret reference for this value.
    pub secret_access_key: Secret,
    /// The url of an S3-compatible server, for example `http://localhost:9000`
    /// for the rustfs in `docker-compose.yaml`. Leave it out to use AWS S3 in
    /// `region`.
    // omitted rather than written as `null` when absent, so a connection saved
    // back out is the file someone hand-wrote — same rule as a postgres port
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    /// The region of the bucket. The default is `us-east-1`. S3-compatible
    /// servers with no regions accept this value.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    /// Permit an `http://` endpoint with no TLS. The default is false. The
    /// credentials then go over the network as plain text. Use it only for a
    /// local server, for example the rustfs in `docker-compose.yaml`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allow_http: Option<bool>,
}

/// A ClickHouse server and the user that kayak connects as. kayak uses the
/// HTTP interface of the server. The output or the input sets the table.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[schemars(title = "clickhouse")]
pub struct ClickhouseConnection {
    /// The url of the HTTP interface, for example `http://localhost:8123` for
    /// the server in `docker-compose.yaml`, or `https://<host>:8443` for
    /// ClickHouse Cloud.
    pub url: String,
    /// The database to use. The database must exist. The output makes tables.
    /// It does not make databases.
    pub database: String,
    /// The user to connect as.
    pub user: String,
    /// The password of the user. Use a `${NAME}` secret reference for this
    /// value.
    pub password: Secret,
    /// Permit an `http://` url with no TLS. The default is false. The
    /// credentials go with every request, as plain text. Use it only for a
    /// local server, for example the server in `docker-compose.yaml`.
    // omitted rather than written as `null` when absent, so a connection saved
    // back out is the file someone hand-wrote — same rule as a postgres port
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allow_http: Option<bool>,
}

impl ClickhouseConnection {
    /// Whether a plaintext url is allowed here. Not unless it says so.
    #[must_use]
    pub fn allows_http(&self) -> bool {
        self.allow_http.unwrap_or(false)
    }
}

/// A Tidepool server and its ingest token. The `tidepool` output sets the
/// table. You declare the tables in the Tidepool project. kayak does not make
/// them.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[schemars(title = "tidepool")]
pub struct TidepoolConnection {
    /// The url of the server, for example `http://localhost:7070`.
    pub url: String,
    /// The ingest token, as a `${NAME}` secret reference. Use the
    /// `TIDEPOOL_INGEST_TOKEN` of the server, or its admin token. Leave it out
    /// for a server with open ingest.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub token: Option<Secret>,
    /// Permit an `http://` url with no TLS when `token` is set. The default is
    /// false. The token goes with every batch, as plain text. Without a token,
    /// an `http://` url is always permitted.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allow_http: Option<bool>,
}

impl TidepoolConnection {
    /// Whether a plaintext url is allowed with a token. Not unless it says so.
    #[must_use]
    pub fn allows_http(&self) -> bool {
        self.allow_http.unwrap_or(false)
    }
}

/// An Indu Cloud deployment: the API and ingest endpoints, and the API key.
///
/// The `indu` output and the `indu` input use the same connection. The output
/// writes streams through `/ingest/v1/streams`. The input reads sensors and
/// streams through `/api/v1`. Make the key in Indu, on the `/keys` page or with
/// `indud apps register --kind kayak`. Give the key a role in Indu.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[schemars(title = "indu")]
pub struct InduConnection {
    /// The origin of the deployment, for example `https://app.acme.indu.cloud`.
    /// The ingest endpoint is `/ingest/v1/…` under this url, unless you set
    /// `ingest_url`.
    pub url: String,
    /// The origin of `/ingest/v1/…` when it is not under `url`, for example
    /// `https://ingest.acme.indu.cloud`. A single-server installation serves
    /// ingest on its own host.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ingest_url: Option<String>,
    /// The API key (`indu.ak.…`), as a `${NAME}` secret reference.
    pub api_key: Secret,
}

impl InduConnection {
    /// The origin ingest requests go to: `ingest_url` when set, else `url`.
    #[must_use]
    pub fn ingest_origin(&self) -> &str {
        self.ingest_url.as_deref().unwrap_or(&self.url)
    }
}

/// The types of system that a connection can describe. The `type` field
/// selects the type.
///
/// Inputs and outputs use the same connection types. For example, a kafka
/// input consumes from a `kafka` connection, and a kafka output publishes to
/// it.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ConnectionKind {
    Kafka(KafkaConnection),
    Nats(NatsConnection),
    Postgres(PostgresConnection),
    Clickhouse(ClickhouseConnection),
    File(FileConnection),
    S3(S3Connection),
    Mqtt(MqttConnection),
    Redis(RedisConnection),
    Opcua(OpcuaConnection),
    Indu(InduConnection),
    Tidepool(TidepoolConnection),
}

impl ConnectionKind {
    /// The `type` tag that selects this kind — the same string a component's
    /// `connection` field is matched against.
    #[must_use]
    pub fn type_name(&self) -> &'static str {
        match self {
            Self::Kafka(_) => KAFKA,
            Self::Nats(_) => NATS,
            Self::Postgres(_) => POSTGRES,
            Self::Clickhouse(_) => CLICKHOUSE,
            Self::File(_) => FILE,
            Self::S3(_) => S3,
            Self::Mqtt(_) => MQTT,
            Self::Redis(_) => REDIS,
            Self::Opcua(_) => OPCUA,
            Self::Indu(_) => INDU,
            Self::Tidepool(_) => TIDEPOOL,
        }
    }
}

pub const KAFKA: &str = "kafka";
pub const NATS: &str = "nats";
pub const POSTGRES: &str = "postgres";
pub const CLICKHOUSE: &str = "clickhouse";
pub const FILE: &str = "file";
pub const S3: &str = "s3";
pub const MQTT: &str = "mqtt";
pub const REDIS: &str = "redis";
pub const OPCUA: &str = "opcua";
pub const INDU: &str = "indu";
pub const TIDEPOOL: &str = "tidepool";

/// The body of `POST /api/connections`: the name in `id`, and the fields of
/// the connection beside it. The body has the same shape as one entry in the
/// connections file.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, JsonSchema)]
pub struct CreateConnectionRequest {
    pub id: ConnectionId,
    #[serde(flatten)]
    pub connection: ConnectionKind,
}

/// All connections in the connections file, as an object from name to
/// connection. kayak writes the names in alphabetical order.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq, JsonSchema)]
#[serde(transparent)]
pub struct Connections(BTreeMap<ConnectionId, ConnectionKind>);

impl Connections {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn get(&self, id: &str) -> Option<&ConnectionKind> {
        self.0.get(id)
    }

    #[must_use]
    pub fn contains(&self, id: &str) -> bool {
        self.0.contains_key(id)
    }

    /// Returns the connection that was there before, if any.
    pub fn insert(
        &mut self,
        id: ConnectionId,
        connection: ConnectionKind,
    ) -> Option<ConnectionKind> {
        self.0.insert(id, connection)
    }

    pub fn remove(&mut self, id: &str) -> Option<ConnectionKind> {
        self.0.remove(id)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&ConnectionId, &ConnectionKind)> {
        self.0.iter()
    }

    #[must_use]
    pub fn ids(&self) -> Vec<ConnectionId> {
        self.0.keys().cloned().collect()
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.0.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// The kafka cluster called `id`, or an error naming what went wrong. One
    /// accessor per kind, so a component asks for the kind it can actually use
    /// and a mismatch is reported where it can say which kind was wanted.
    pub fn kafka(&self, id: &str) -> Result<&KafkaConnection, ConnectionError> {
        match self.lookup(id, KAFKA)? {
            ConnectionKind::Kafka(c) => Ok(c),
            other => Err(ConnectionError::wrong_kind(id, KAFKA, other)),
        }
    }

    pub fn indu(&self, id: &str) -> Result<&InduConnection, ConnectionError> {
        match self.lookup(id, INDU)? {
            ConnectionKind::Indu(c) => Ok(c),
            other => Err(ConnectionError::wrong_kind(id, INDU, other)),
        }
    }

    pub fn tidepool(&self, id: &str) -> Result<&TidepoolConnection, ConnectionError> {
        match self.lookup(id, TIDEPOOL)? {
            ConnectionKind::Tidepool(c) => Ok(c),
            other => Err(ConnectionError::wrong_kind(id, TIDEPOOL, other)),
        }
    }

    pub fn nats(&self, id: &str) -> Result<&NatsConnection, ConnectionError> {
        match self.lookup(id, NATS)? {
            ConnectionKind::Nats(c) => Ok(c),
            other => Err(ConnectionError::wrong_kind(id, NATS, other)),
        }
    }

    pub fn postgres(&self, id: &str) -> Result<&PostgresConnection, ConnectionError> {
        match self.lookup(id, POSTGRES)? {
            ConnectionKind::Postgres(c) => Ok(c),
            other => Err(ConnectionError::wrong_kind(id, POSTGRES, other)),
        }
    }

    pub fn clickhouse(&self, id: &str) -> Result<&ClickhouseConnection, ConnectionError> {
        match self.lookup(id, CLICKHOUSE)? {
            ConnectionKind::Clickhouse(c) => Ok(c),
            other => Err(ConnectionError::wrong_kind(id, CLICKHOUSE, other)),
        }
    }

    pub fn file(&self, id: &str) -> Result<&FileConnection, ConnectionError> {
        match self.lookup(id, FILE)? {
            ConnectionKind::File(c) => Ok(c),
            other => Err(ConnectionError::wrong_kind(id, FILE, other)),
        }
    }

    pub fn s3(&self, id: &str) -> Result<&S3Connection, ConnectionError> {
        match self.lookup(id, S3)? {
            ConnectionKind::S3(c) => Ok(c),
            other => Err(ConnectionError::wrong_kind(id, S3, other)),
        }
    }

    pub fn mqtt(&self, id: &str) -> Result<&MqttConnection, ConnectionError> {
        match self.lookup(id, MQTT)? {
            ConnectionKind::Mqtt(c) => Ok(c),
            other => Err(ConnectionError::wrong_kind(id, MQTT, other)),
        }
    }

    pub fn redis(&self, id: &str) -> Result<&RedisConnection, ConnectionError> {
        match self.lookup(id, REDIS)? {
            ConnectionKind::Redis(c) => Ok(c),
            other => Err(ConnectionError::wrong_kind(id, REDIS, other)),
        }
    }

    pub fn opcua(&self, id: &str) -> Result<&OpcuaConnection, ConnectionError> {
        match self.lookup(id, OPCUA)? {
            ConnectionKind::Opcua(c) => Ok(c),
            other => Err(ConnectionError::wrong_kind(id, OPCUA, other)),
        }
    }

    /// An unknown name lists the ones that do exist: the usual cause is a typo
    /// or a connection that was never added, and both are answered by the list.
    fn lookup(&self, id: &str, wanted: &'static str) -> Result<&ConnectionKind, ConnectionError> {
        self.0.get(id).ok_or_else(|| ConnectionError::Unknown {
            id: id.to_string(),
            wanted,
            known: self.ids(),
        })
    }
}

impl FromIterator<(ConnectionId, ConnectionKind)> for Connections {
    fn from_iter<T: IntoIterator<Item = (ConnectionId, ConnectionKind)>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}

/// Why a component could not get the connection it asked for.
///
/// Both cases are the user's mistake in a config file rather than a runtime
/// failure, so they carry enough to fix it without going and reading the other
/// file.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ConnectionError {
    Unknown {
        id: ConnectionId,
        wanted: &'static str,
        known: Vec<ConnectionId>,
    },
    WrongKind {
        id: ConnectionId,
        wanted: &'static str,
        found: &'static str,
    },
}

impl ConnectionError {
    fn wrong_kind(id: &str, wanted: &'static str, found: &ConnectionKind) -> Self {
        Self::WrongKind {
            id: id.to_string(),
            wanted,
            found: found.type_name(),
        }
    }
}

impl std::fmt::Display for ConnectionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unknown { id, wanted, known } => {
                write!(f, "there is no connection called '{id}'")?;
                if known.is_empty() {
                    write!(f, "; no connections are configured")
                } else {
                    write!(f, "; configured connections are {}", known.join(", "))?;
                    write!(f, " (a {wanted} connection is wanted here)")
                }
            }
            Self::WrongKind { id, wanted, found } => write!(
                f,
                "connection '{id}' is a {found} connection, but a {wanted} connection is wanted here"
            ),
        }
    }
}

impl std::error::Error for ConnectionError {}

#[cfg(test)]
mod tests {
    use super::*;

    fn connections() -> Connections {
        [
            (
                "prod-kafka".to_string(),
                ConnectionKind::Kafka(KafkaConnection {
                    brokers: "localhost:9092".into(),
                }),
            ),
            (
                "local-nats".to_string(),
                ConnectionKind::Nats(NatsConnection {
                    urls: "nats://localhost:4222".into(),
                }),
            ),
        ]
        .into_iter()
        .collect()
    }

    #[test]
    fn a_connection_is_found_by_name_and_kind() -> Result<(), ConnectionError> {
        assert_eq!(
            connections().kafka("prod-kafka")?.brokers.template(),
            "localhost:9092"
        );
        assert_eq!(
            connections().nats("local-nats")?.urls.template(),
            "nats://localhost:4222"
        );
        Ok(())
    }

    /// The whole point of naming the kind on both sides: a nats connection in a
    /// kafka input is a mistake that has to be caught where it can be explained,
    /// not passed to a broker as a broker list.
    #[test]
    fn asking_for_the_wrong_kind_says_which_kind_it_actually_is() {
        let Err(err) = connections().kafka("local-nats") else {
            panic!("a nats connection was accepted as a kafka one");
        };
        assert_eq!(
            err,
            ConnectionError::WrongKind {
                id: "local-nats".to_string(),
                wanted: "kafka",
                found: "nats",
            }
        );
        let message = err.to_string();
        assert!(message.contains("is a nats connection"), "{message}");
    }

    /// A typo is the common case, and the fix is in the *other* file — so the
    /// error carries the names that do exist rather than sending someone to go
    /// and look.
    #[test]
    fn an_unknown_connection_lists_the_ones_that_exist() {
        let Err(err) = connections().kafka("prod-kafk") else {
            panic!("an unknown connection was accepted");
        };
        let message = err.to_string();
        assert!(
            message.contains("no connection called 'prod-kafk'"),
            "{message}"
        );
        assert!(message.contains("local-nats"), "{message}");
        assert!(message.contains("prod-kafka"), "{message}");
    }

    #[test]
    fn an_unknown_connection_on_an_empty_file_says_so() {
        let Err(err) = Connections::new().kafka("prod-kafka") else {
            panic!("an unknown connection was accepted");
        };
        let message = err.to_string();
        assert!(
            message.contains("no connections are configured"),
            "{message}"
        );
    }

    /// The file is a map of name to connection, and a connection is tagged the
    /// same way a component is. This is the wire format the UI posts and the
    /// file holds.
    #[test]
    fn the_file_is_a_map_of_name_to_tagged_connection() -> Result<(), serde_json::Error> {
        let raw = serde_json::json!({
            "prod-kafka": {"type": "kafka", "brokers": "${KAFKA_BROKERS}"},
            "warehouse": {
                "type": "postgres",
                "host": "db",
                "database": "kayak",
                "user": "kayak",
                "password": "${POSTGRES_PASSWORD}"
            }
        });
        let parsed: Connections = serde_json::from_value(raw.clone())?;
        assert_eq!(parsed.len(), 2);
        assert_eq!(
            parsed.get("warehouse").map(ConnectionKind::type_name),
            Some("postgres")
        );
        // and back out unchanged: the file is committed, so a load-and-save
        // must not rewrite it
        assert_eq!(serde_json::to_value(&parsed)?, raw);
        Ok(())
    }

    /// Names come out in order whatever order they went in, which is what makes
    /// the written file deterministic.
    #[test]
    fn names_are_iterated_in_order() {
        let mut connections = Connections::new();
        for id in ["z", "a", "m"] {
            connections.insert(
                id.to_string(),
                ConnectionKind::Nats(NatsConnection {
                    urls: "nats://localhost:4222".into(),
                }),
            );
        }
        assert_eq!(connections.ids(), ["a", "m", "z"]);
    }
}
