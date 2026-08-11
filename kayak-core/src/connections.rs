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

/// A kafka cluster: the brokers, and eventually whatever it takes to
/// authenticate against them.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[schemars(title = "kafka")]
pub struct KafkaConnection {
    /// comma-separated broker list, e.g. `localhost:9092`. May reference
    /// secrets as `${NAME}` — see "secrets" in the readme.
    pub brokers: Secret,
}

/// A nats server, or a cluster of them.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[schemars(title = "nats")]
pub struct NatsConnection {
    /// connection url, e.g. `nats://localhost:4222`. May reference secrets as
    /// `${NAME}` — see "secrets" in the readme.
    pub urls: Secret,
}

/// A postgres database, as one role connects to it.
///
/// The database and the role are part of the connection; the *table* is not —
/// that is what a particular output writes into, so it stays on the output.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[schemars(title = "postgres")]
pub struct PostgresConnection {
    /// server hostname, e.g. `localhost`
    pub host: String,
    /// the database to connect to
    pub database: String,
    /// the role to connect as
    pub user: String,
    /// that role's password. May reference secrets as `${NAME}` — see
    /// "secrets" in the readme, and prefer a reference to a literal here.
    pub password: Secret,
    /// server port. Defaults to 5432.
    // omitted rather than written as `null` when absent, so a connection saved
    // back out is the file someone hand-wrote — same rule as an input's buffer
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
}

/// A directory on the server's filesystem that file outputs write under.
///
/// The odd one out among the kinds: there is no host, no credentials, nothing
/// to authenticate against. It earns its place as a connection anyway because
/// it holds the same thing the others do — *what the system is*, as against
/// what one pipeline wants from it. A file output names a `path` relative to
/// this root exactly as a kafka output names a topic on those brokers, and the
/// object-store connection that replaces it later swaps the root for a bucket
/// without any component changing.
///
/// The root is **not** a boundary on its own. It arrives from `POST
/// /api/connections` like any other connection, so a browser could name `/`
/// here; what actually confines writes is the server's `--data-dir`, which this
/// root has to resolve under. See `Root::resolve` in the root crate.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[schemars(title = "file")]
pub struct FileConnection {
    /// directory that file outputs write under, e.g. `./out/events`. Created if
    /// it does not exist, and it must resolve inside the server's `--data-dir`
    /// — a server started without that flag has file output turned off.
    pub root: String,
}

/// A bucket on an S3-compatible object store, and the credentials that reach
/// it.
///
/// The `bucket` is where [`FileConnection`]'s `root` is: the thing the *system*
/// gives you, against which an output names a prefix of its own. What is not
/// here is any equivalent of `--data-dir`. There cannot be one — the server has
/// no view of a remote namespace to confine writes within, so the boundary is
/// the credentials, and giving a deployment a key that can only write one bucket
/// is the thing that does what the sandbox does locally.
///
/// `endpoint` is what makes this work against rustfs, minio or any other
/// S3-compatible server; left out, it is real AWS S3 in `region`.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[schemars(title = "s3")]
pub struct S3Connection {
    /// the bucket to write into. It has to exist already — an output creates
    /// objects, never buckets.
    pub bucket: String,
    /// access key id. May reference secrets as `${NAME}` — see "secrets" in the
    /// readme, and prefer a reference to a literal here.
    pub access_key_id: Secret,
    /// secret access key. May reference secrets as `${NAME}` — see "secrets" in
    /// the readme, and prefer a reference to a literal here.
    pub secret_access_key: Secret,
    /// url of an S3-compatible server, e.g. `http://localhost:9000` for the
    /// rustfs in `docker-compose.yaml`. Leave it out for real AWS S3, which is
    /// then addressed through `region`.
    // omitted rather than written as `null` when absent, so a connection saved
    // back out is the file someone hand-wrote — same rule as a postgres port
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub endpoint: Option<String>,
    /// the bucket's region. Defaults to `us-east-1`, which is also what an
    /// S3-compatible server that does not care about regions will accept.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    /// allow a plaintext `http://` endpoint. Defaults to false: credentials
    /// over http is a mistake worth having to write down, and the local rustfs
    /// is the case that legitimately wants it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub allow_http: Option<bool>,
}

/// A ClickHouse server, as one user connects to it over its HTTP interface.
///
/// The same split [`PostgresConnection`] makes: the server, the database and
/// the user are the connection's; the *table* belongs to the output that writes
/// it.
///
/// The HTTP interface rather than the native protocol because it is what every
/// ClickHouse deployment exposes — including ClickHouse Cloud, where 8443 is the
/// only port there is — and because it takes an insert as a body in a named
/// format, which is exactly the shape a batch of messages already has.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[schemars(title = "clickhouse")]
pub struct ClickhouseConnection {
    /// url of the HTTP interface, e.g. `http://localhost:8123` for the server in
    /// `docker-compose.yaml`, or `https://<host>:8443` for ClickHouse Cloud.
    pub url: String,
    /// the database to write into. It has to exist already — an output creates
    /// tables, never databases.
    pub database: String,
    /// the user to connect as
    pub user: String,
    /// that user's password. May reference secrets as `${NAME}` — see "secrets"
    /// in the readme, and prefer a reference to a literal here.
    pub password: Secret,
    /// allow a plaintext `http://` url. Defaults to false: the credentials
    /// above go with every insert, so sending them in the clear is a decision
    /// worth writing down. The local server in `docker-compose.yaml` is the
    /// case that legitimately wants it.
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

/// Every kind of system a connection can describe.
///
/// Tagged the same way the component enums are, so a connection reads like the
/// components that use it. One kind serves both directions: a `kafka`
/// connection is what a kafka input consumes from *and* what a kafka output
/// publishes to.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ConnectionKind {
    Kafka(KafkaConnection),
    Nats(NatsConnection),
    Postgres(PostgresConnection),
    Clickhouse(ClickhouseConnection),
    File(FileConnection),
    S3(S3Connection),
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
        }
    }
}

pub const KAFKA: &str = "kafka";
pub const NATS: &str = "nats";
pub const POSTGRES: &str = "postgres";
pub const CLICKHOUSE: &str = "clickhouse";
pub const FILE: &str = "file";
pub const S3: &str = "s3";

/// What `POST /api/connections` takes: a name, and the connection itself
/// flattened alongside it.
///
/// The name is a field here rather than a path segment because it is part of
/// what is being created, and because the body then reads exactly like one
/// entry of the file it will be written to.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, JsonSchema)]
pub struct CreateConnectionRequest {
    pub id: ConnectionId,
    #[serde(flatten)]
    pub connection: ConnectionKind,
}

/// Everything in the connections file, by name.
///
/// A `BTreeMap` rather than a list of `{id, ...}` objects: the name is the
/// identity, duplicates are impossible to express, and iteration is in name
/// order — which is what makes the file deterministic to write, the same
/// property the config file depends on.
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
