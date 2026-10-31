//! The `indu_namespace` input: Indu's unified namespace, read whole —
//! [`super::poll::Poller`] over a [`NamespaceReader`].
//!
//! The platform keeps its org tree, devices and sensors as one namespace and
//! serves it at `GET /api/v1/namespace/nodes` as a flat list carrying
//! `parent_id`. It has no change feed for it (moves and renames overwrite in
//! place), so reading it whole on a timer is how a move, a rename or a
//! deletion is noticed at all. That makes this the `http_poll` case — a
//! snapshot, reference data, wanted whole — with two differences that are
//! why it is a reader of its own:
//!
//! - **A reply is capped, so the tree is read a branch at a time.** The
//!   platform answers at most 5 000 nodes and at most five levels below the
//!   node asked about, and says `truncated` when it cut. The reader starts at
//!   the roots, asks again under every node that has children the reply
//!   didn't include, and asks a branch one level at a time when it came back
//!   cut. A single node with more children than one reply holds can't be
//!   read this way, and is a failed read rather than a partial tree.
//! - **A snapshot is one batch.** The schedule's `max_batch` is unbounded, so
//!   the poller hands a read on as it came, whole and alone. That is what a
//!   `tidepool` output's `write: replace` relies on: each batch is the tree,
//!   and what isn't in it is gone.
//!
//! Every node becomes `{id, parent_id, label, kind, unit}` with the platform's
//! prefixed ids (`org:…`, `device:…`), which never collide across kinds. A
//! parent outside the kinds asked for leaves its children pointing at a node
//! that isn't there, which a hierarchy reads as a root.

use std::collections::{BTreeSet, HashSet, VecDeque};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use kayak_core::config::{InduNamespaceInputConfig, NamespaceKind};
use reqwest::header::{ACCEPT, AUTHORIZATION, HeaderValue};
use reqwest::{Client, Url};
use serde::Deserialize;
use serde_json::{Value, json};

use crate::{
    BuildCtx,
    inputs::{
        BuildInput, InputSource, ack,
        poll::{Fetched, Poller, Reader, Schedule},
    },
    outbound::truncate,
};

const NODES_PATH: &str = "api/v1/namespace/nodes";
/// The most levels the platform answers below a node.
const DEPTH: u8 = 5;
/// Requests one snapshot may make. A tree that needs more is not one this
/// input can read on a timer.
const MAX_REQUESTS: usize = 10_000;
/// Nodes one snapshot may hold: the whole tree is held, so it is bounded.
const MAX_NODES: usize = 500_000;
const DEFAULT_INTERVAL: Duration = Duration::from_secs(60);
const TIMEOUT: Duration = Duration::from_secs(30);

impl BuildInput for InduNamespaceInputConfig {
    fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn InputSource>> {
        // a snapshot is handed on whatever the outputs then do with it
        ack::require_receipt_only(ctx.ack_mode(), "indu_namespace")?;
        anyhow::ensure!(
            self.interval_secs != Some(0),
            "`interval_secs` is 0; a read that never waits is a loop against the platform"
        );
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
        let credential = super::indu::credential(resolved.expose())?;
        let nodes = origin.join(NODES_PATH).with_context(|| {
            format!("the indu connection '{}' can't be joined", self.connection)
        })?;
        let client = Client::builder()
            .timeout(TIMEOUT)
            .build()
            .context("failed to build the indu_namespace input's client")?;
        let kinds: BTreeSet<NamespaceKind> = if self.kinds.is_empty() {
            [
                NamespaceKind::Org,
                NamespaceKind::Device,
                NamespaceKind::Sensor,
            ]
            .into()
        } else {
            self.kinds.iter().copied().collect()
        };

        let described = format!("the namespace at {}", crate::outbound::describe(&origin));
        let schedule = Schedule {
            interval: self
                .interval_secs
                .map_or(DEFAULT_INTERVAL, Duration::from_secs),
            start_from: None,
            page_size: 1,
            // a snapshot is one batch: see the module docs
            max_batch: usize::MAX,
            source: described.clone(),
        };
        let envelope = ctx.envelope("indu_namespace", None);
        Ok(Box::new(Poller::new(
            schedule,
            Box::new(NamespaceReader {
                nodes,
                described,
                credential,
                client,
                kinds,
            }),
            vec![("connection", Value::String(self.connection.clone()))],
            envelope,
            ctx.pipeline_id.clone(),
            ctx.events.clone(),
        )))
    }
}

/// One node as the platform sends it; what this input doesn't hand on is
/// ignored.
#[derive(Debug, Clone, Deserialize)]
struct Node {
    id: String,
    kind: NamespaceKind,
    label: String,
    #[serde(default)]
    parent_id: Option<String>,
    #[serde(default)]
    has_children: bool,
    #[serde(default)]
    unit: Option<String>,
}

#[derive(Debug, Deserialize)]
struct Reply {
    nodes: Vec<Node>,
    #[serde(default)]
    truncated: bool,
}

/// Reads the whole namespace, a branch at a time.
pub struct NamespaceReader {
    nodes: Url,
    described: String,
    credential: HeaderValue,
    client: Client,
    kinds: BTreeSet<NamespaceKind>,
}

impl NamespaceReader {
    /// The nodes under `parent` (the roots for `None`), `depth` levels down.
    async fn get(&self, parent: Option<&str>, depth: u8) -> Result<Reply> {
        let mut url = self.nodes.clone();
        url.query_pairs_mut()
            .append_pair("depth", &depth.to_string());
        if let Some(parent) = parent {
            url.query_pairs_mut().append_pair("parent", parent);
        }
        let response = self
            .client
            .get(url)
            .header(ACCEPT, "application/json")
            .header(AUTHORIZATION, self.credential.clone())
            .send()
            .await
            .with_context(|| format!("failed to reach {}", self.described))?;
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        if !status.is_success() {
            bail!(
                "{} answered {status} for {}: {}",
                self.described,
                parent.unwrap_or("the roots"),
                truncate(text.trim())
            );
        }
        serde_json::from_str(&text).with_context(|| {
            format!(
                "{} answered in a shape this kayak doesn't read: {}",
                self.described,
                truncate(text.trim())
            )
        })
    }

    /// Whether a node's children are worth asking for: they're of a kind
    /// asked for, or lead to one.
    fn expands(&self, kind: NamespaceKind) -> bool {
        let wanted = |k| self.kinds.contains(&k);
        match kind {
            // an org node holds org nodes, devices and streams
            NamespaceKind::Org => true,
            NamespaceKind::Device => wanted(NamespaceKind::Sensor) || wanted(NamespaceKind::Stream),
            NamespaceKind::Sensor => wanted(NamespaceKind::Stream),
            NamespaceKind::Stream => false,
        }
    }

    /// The whole tree: see the module docs.
    async fn walk(&self) -> Result<Vec<Value>> {
        let mut out = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        let mut frontier: VecDeque<(Option<String>, u8)> = VecDeque::from([(None, DEPTH)]);
        let mut requests = 0;
        while let Some((parent, depth)) = frontier.pop_front() {
            requests += 1;
            if requests > MAX_REQUESTS {
                bail!(
                    "{} needed more than {MAX_REQUESTS} requests for one read; narrow `kinds`",
                    self.described
                );
            }
            let reply = self.get(parent.as_deref(), depth).await?;
            if reply.truncated {
                if depth > 1 {
                    // the same branch, a level at a time
                    frontier.push_back((parent, 1));
                    continue;
                }
                bail!(
                    "{} has more children under {} than one reply holds, so the namespace can't \
                     be read whole",
                    self.described,
                    parent.as_deref().unwrap_or("the roots")
                );
            }
            let with_children: HashSet<&str> = reply
                .nodes
                .iter()
                .filter_map(|n| n.parent_id.as_deref())
                .collect();
            let mut next = Vec::new();
            for node in &reply.nodes {
                if !seen.insert(node.id.clone()) {
                    continue;
                }
                if node.has_children
                    && !with_children.contains(node.id.as_str())
                    && self.expands(node.kind)
                {
                    next.push(node.id.clone());
                }
                if self.kinds.contains(&node.kind) {
                    if out.len() == MAX_NODES {
                        bail!(
                            "{} holds more than {MAX_NODES} nodes; narrow `kinds`",
                            self.described
                        );
                    }
                    out.push(row(node));
                }
            }
            frontier.extend(next.into_iter().map(|id| (Some(id), DEPTH)));
        }
        Ok(out)
    }
}

/// A node as a message.
fn row(node: &Node) -> Value {
    json!({
        "id": node.id,
        "parent_id": node.parent_id,
        "label": node.label,
        "kind": node.kind,
        "unit": node.unit,
    })
}

#[async_trait::async_trait]
impl Reader for NamespaceReader {
    async fn snapshot(&mut self) -> Result<Vec<Value>> {
        self.walk().await
    }

    async fn page(&mut self, _after: Option<&str>, _limit: usize) -> Result<Vec<Fetched>> {
        Err(anyhow!(
            "an indu_namespace input reads whole snapshots and has no pages"
        ))
    }

    async fn newest(&mut self) -> Result<Option<String>> {
        Err(anyhow!(
            "an indu_namespace input reads whole snapshots and has no watermark"
        ))
    }

    fn describe(&self) -> String {
        self.described.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::MapSecretStore;
    use axum::Router;
    use axum::extract::{Query, State};
    use axum::http::{HeaderMap, StatusCode};
    use axum::response::IntoResponse;
    use axum::routing::get;
    use kayak_core::config::Secret;
    use kayak_core::connections::{ConnectionKind, Connections, InduConnection};
    use std::collections::{BTreeMap, HashMap};
    use std::sync::{Arc, Mutex, PoisonError};

    fn ok<T, E: std::fmt::Display>(result: std::result::Result<T, E>) -> T {
        result.unwrap_or_else(|e| panic!("the test could not get this far: {e}"))
    }

    /// A tree as the platform keeps it: (id, kind, parent).
    type Tree = Vec<(&'static str, &'static str, Option<&'static str>)>;

    /// A platform serving a tree the way the real one does: `depth` levels
    /// below `parent`, at most `cap` nodes, `truncated` past that.
    #[derive(Default)]
    struct Fake {
        tree: Mutex<Tree>,
        cap: Mutex<usize>,
        /// (parent, depth) per request, and the authorization it carried.
        asked: Mutex<Vec<(String, String, String)>>,
    }

    fn lock<T>(m: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
        m.lock().unwrap_or_else(PoisonError::into_inner)
    }

    async fn nodes(
        State(fake): State<Arc<Fake>>,
        headers: HeaderMap,
        Query(query): Query<BTreeMap<String, String>>,
    ) -> axum::response::Response {
        let parent = query.get("parent").cloned();
        let depth: usize = query.get("depth").and_then(|d| d.parse().ok()).unwrap_or(2);
        let auth = headers
            .get(AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_string();
        lock(&fake.asked).push((
            parent.clone().unwrap_or_default(),
            depth.to_string(),
            auth.clone(),
        ));
        if auth != "Bearer indu.ak.test" {
            return (StatusCode::UNAUTHORIZED, "no").into_response();
        }
        let tree = lock(&fake.tree).clone();
        let cap = *lock(&fake.cap);
        let children = |of: Option<&str>| -> Vec<(&str, &str, Option<&str>)> {
            tree.iter().filter(|(_, _, p)| *p == of).copied().collect()
        };
        // breadth first from the parent, `depth` levels
        let mut out = Vec::new();
        let mut level: Vec<(&str, &str, Option<&str>)> = children(parent.as_deref());
        for d in 0..depth {
            let mut below = Vec::new();
            for node in &level {
                let kids = children(Some(node.0));
                out.push(json!({
                    "id": node.0, "kind": node.1, "label": node.0.to_uppercase(),
                    "parent_id": node.2, "has_children": !kids.is_empty(),
                    "selectable": node.1 == "sensor",
                }));
                if d + 1 < depth {
                    below.extend(kids);
                }
            }
            level = below;
        }
        let truncated = out.len() > cap;
        out.truncate(cap);
        axum::Json(json!({"nodes": out, "truncated": truncated, "total_device_count": 0}))
            .into_response()
    }

    async fn platform(tree: Tree, cap: usize) -> (Arc<Fake>, String) {
        let fake = Arc::new(Fake::default());
        *lock(&fake.tree) = tree;
        *lock(&fake.cap) = cap;
        let app = Router::new()
            .route("/api/v1/namespace/nodes", get(nodes))
            .with_state(Arc::clone(&fake));
        let listener = ok(tokio::net::TcpListener::bind("127.0.0.1:0").await);
        let addr = ok(listener.local_addr());
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        (fake, format!("http://{addr}"))
    }

    fn build(url: &str, kinds: Vec<NamespaceKind>) -> Result<Box<dyn InputSource>> {
        let mut pipelines = HashMap::new();
        let (events, _rx) = tokio::sync::broadcast::channel(16);
        let secrets = Arc::new(MapSecretStore::new(
            "a test store",
            &[("INDU_API_KEY", "indu.ak.test"), ("BLANK", "")],
        ));
        let connections: Connections = [(
            "indu".to_string(),
            ConnectionKind::Indu(InduConnection {
                url: url.to_string(),
                ingest_url: None,
                api_key: Secret::new("${INDU_API_KEY}"),
            }),
        )]
        .into_iter()
        .collect();
        let mut ctx = BuildCtx::with_secrets(&mut pipelines, "p".to_string(), events, secrets)
            .with_connections(Arc::new(connections));
        InduNamespaceInputConfig {
            connection: "indu".into(),
            interval_secs: Some(3600),
            kinds,
        }
        .build(&mut ctx)
    }

    /// A plant seven levels deep, deeper than one reply reaches:
    /// site ─ hall ─ line ─ cell ─ station ─ press (device) ─ temp (sensor),
    /// and a second device right under the site.
    fn plant() -> Tree {
        vec![
            ("org:site", "org", None),
            ("org:hall", "org", Some("org:site")),
            ("org:line", "org", Some("org:hall")),
            ("org:cell", "org", Some("org:line")),
            ("org:station", "org", Some("org:cell")),
            ("device:press", "device", Some("org:station")),
            ("sensor:temp", "sensor", Some("device:press")),
            ("device:robot", "device", Some("org:site")),
            ("sensor:torque", "sensor", Some("device:robot")),
            ("org:unassigned", "org", None),
        ]
    }

    async fn one_read(input: &mut Box<dyn InputSource>) -> Vec<Value> {
        let delivery = ok(input.next().await);
        delivery.batch.iter().map(|m| (**m).clone()).collect()
    }

    fn ids(batch: &[Value]) -> Vec<String> {
        let mut ids: Vec<String> = batch
            .iter()
            .map(|m| m["id"].as_str().unwrap_or("?").to_string())
            .collect();
        ids.sort();
        ids
    }

    #[tokio::test]
    async fn the_whole_tree_comes_as_one_batch_however_deep() {
        let (fake, url) = platform(plant(), 5_000).await;
        let mut input = ok(build(&url, Vec::new()));
        let batch = one_read(&mut input).await;
        assert_eq!(
            ids(&batch),
            [
                "device:press",
                "device:robot",
                "org:cell",
                "org:hall",
                "org:line",
                "org:site",
                "org:station",
                "org:unassigned",
                "sensor:temp",
                "sensor:torque",
            ]
        );
        let press = batch.iter().find(|m| m["id"] == "device:press");
        assert_eq!(
            press,
            Some(&json!({"id": "device:press", "parent_id": "org:station",
                         "label": "DEVICE:PRESS", "kind": "device", "unit": null}))
        );
        // the roots five deep, then the station's branch, which the first
        // reply stopped at
        let asked: Vec<(String, String)> = lock(&fake.asked)
            .iter()
            .map(|(p, d, _)| (p.clone(), d.clone()))
            .collect();
        assert_eq!(
            asked,
            [
                (String::new(), "5".to_string()),
                ("org:station".to_string(), "5".to_string()),
            ]
        );
        assert!(
            lock(&fake.asked)
                .iter()
                .all(|(_, _, auth)| auth == "Bearer indu.ak.test")
        );
    }

    #[tokio::test]
    async fn a_cut_reply_is_read_again_a_level_at_a_time() {
        let (fake, url) = platform(plant(), 3).await;
        let mut input = ok(build(&url, Vec::new()));
        let batch = one_read(&mut input).await;
        assert_eq!(batch.len(), 10, "{batch:?}");
        let depths: Vec<String> = lock(&fake.asked)
            .iter()
            .map(|(_, d, _)| d.clone())
            .collect();
        assert!(depths.contains(&"1".to_string()), "{depths:?}");
    }

    #[tokio::test]
    async fn kinds_left_out_are_neither_handed_on_nor_read() {
        let (fake, url) = platform(plant(), 5_000).await;
        let mut input = ok(build(&url, vec![NamespaceKind::Org, NamespaceKind::Device]));
        let batch = one_read(&mut input).await;
        assert!(ids(&batch).iter().all(|id| !id.starts_with("sensor:")));
        assert_eq!(batch.len(), 8);
        // the station still is (the press is under it), but with no sensors
        // wanted the press isn't
        let parents: Vec<String> = lock(&fake.asked)
            .iter()
            .map(|(p, _, _)| p.clone())
            .collect();
        assert_eq!(parents, [String::new(), "org:station".to_string()]);
    }

    #[tokio::test]
    async fn a_node_too_wide_for_one_reply_fails_the_read() {
        let (_fake, url) = platform(plant(), 1).await;
        // the poller reports and retries a failed read; the reader says why
        let reader = NamespaceReader {
            nodes: ok(ok(Url::parse(&url)).join(NODES_PATH)),
            described: "the fake".into(),
            credential: ok(super::super::indu::credential("indu.ak.test")),
            client: Client::new(),
            kinds: [NamespaceKind::Org].into(),
        };
        let Err(e) = reader.walk().await else {
            panic!("two roots don't fit in a reply of one");
        };
        assert!(
            format!("{e:#}").contains("more children under the roots"),
            "{e:#}"
        );
    }

    #[tokio::test]
    async fn a_removed_node_is_missing_from_the_next_snapshot() {
        let (fake, url) = platform(plant(), 5_000).await;
        let reader = NamespaceReader {
            nodes: ok(ok(Url::parse(&url)).join(NODES_PATH)),
            described: "the fake".into(),
            credential: ok(super::super::indu::credential("indu.ak.test")),
            client: Client::new(),
            kinds: [NamespaceKind::Org, NamespaceKind::Device].into(),
        };
        assert!(ids(&ok(reader.walk().await)).contains(&"device:robot".to_string()));
        lock(&fake.tree).retain(|(id, _, _)| *id != "device:robot");
        assert!(!ids(&ok(reader.walk().await)).contains(&"device:robot".to_string()));
    }

    #[test]
    fn a_config_that_cannot_work_is_refused_at_build_time() {
        assert!(build("http://127.0.0.1:59999", Vec::new()).is_ok());
        assert!(build("ftp://127.0.0.1:59999", Vec::new()).is_err());
    }
}
