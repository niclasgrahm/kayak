//! Writes batches to objects under a prefix in an S3-compatible bucket.
//!
//! The other half of the split [`crate::outputs::rotate`] was made for: rotation
//! policy, part naming and encoding are shared verbatim with
//! [`crate::outputs::file`], and what is written here is a destination. Reading
//! the two side by side, the only interesting difference is the one the object
//! store forces.
//!
//! **An object store has no append.** A file output opens a file and writes each
//! batch into it as it arrives, so a part is on disk and readable while it is
//! still being filled. A bucket has no such thing: an object exists or it does
//! not, and `PUT` writes it whole. So a part is accumulated in memory and
//! uploaded at the moment it rotates, which turns rotation from a filing
//! convenience into the thing that decides both how soon data is visible and how
//! much of it this process is holding. That is why `rotate` is required on this
//! output and optional on the file one — see [`Buffered::rotation`].
//!
//! Multipart upload is the obvious alternative and is deliberately not used yet:
//! S3 requires every part but the last to be at least 5 MiB, so "one multipart
//! part per batch" does not work for the batch sizes a pipeline actually
//! produces, and doing it properly means the same in-memory accumulation with a
//! flush threshold on top. This is that accumulation without the second layer.
//!
//! There is no `--data-dir` here and there cannot be. The local sandbox works
//! because the server can ask the filesystem where a path really landed; nothing
//! equivalent exists for a remote namespace. The boundary for this output is the
//! credentials on its connection — a key that can write one bucket is what does
//! the job `--data-dir` does locally.

use anyhow::{Context, Result, bail};
use chrono::{DateTime, Utc};
use kayak_core::config::{FileFormat, S3OutputConfig};
use kayak_core::connections::S3Connection;
use object_store::aws::{AmazonS3, AmazonS3Builder};
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStoreExt, PutPayload};
use std::sync::Arc;

use crate::BuildCtx;
use crate::inputs::MessageBatch;
use crate::outputs::rotate::{Encoder, Rotation, part_name};
use crate::outputs::{BuildOutput, OutputDestination};

impl BuildOutput for S3OutputConfig {
    fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn OutputDestination>> {
        let connection = ctx
            .s3_connection(&self.connection)
            .context("the s3 output cannot be built")?
            .clone();
        let context = || {
            format!(
                "the s3 output cannot write through connection '{}'",
                self.connection
            )
        };
        // both resolved at build time rather than on the first write, for the
        // reason the file output resolves its directory then: a prefix that
        // isn't a key or a bucket nobody can address should fail the pipeline
        // that owns it, not surface an hour into a run
        let prefix = parse_prefix(&self.prefix).with_context(context)?;
        let store = build_store(&connection, ctx).with_context(context)?;

        let rotation = Rotation::new(Some(self.rotate));
        if !rotation.rotates() {
            bail!(
                "the s3 output needs a rotation trigger: set `rotate.max_rows`, `rotate.\
                 interval_secs` or both. Unlike a file, an object cannot be appended to, so a part \
                 is held in memory until it rotates — without a trigger this pipeline would buffer \
                 its entire run and upload it once, when it stops"
            );
        }

        let format = self.format.unwrap_or_default();
        Ok(Box::new(S3Output {
            store,
            prefix,
            format,
            buffered: Buffered {
                rotation,
                encoder: Encoder::new(format),
                open: None,
            },
            sequence: 0,
        }))
    }
}

/// The client for one connection.
///
/// Built per output rather than pooled, which is the rule connections already
/// state: two pipelines naming one connection get their own client from the same
/// settings.
fn build_store(connection: &S3Connection, ctx: &BuildCtx) -> Result<AmazonS3> {
    let access_key_id = ctx.resolve(&connection.access_key_id)?;
    let secret_access_key = ctx.resolve(&connection.secret_access_key)?;
    let allow_http = connection.allow_http.unwrap_or(false);

    let mut builder = AmazonS3Builder::new()
        .with_bucket_name(&connection.bucket)
        .with_access_key_id(access_key_id.expose())
        .with_secret_access_key(secret_access_key.expose())
        .with_allow_http(allow_http);
    if let Some(region) = &connection.region {
        builder = builder.with_region(region);
    }
    if let Some(endpoint) = &connection.endpoint {
        // caught here rather than left to object_store, whose own error for this
        // does not say which knob turns it off
        if !allow_http && endpoint.starts_with("http://") {
            bail!(
                "endpoint '{endpoint}' is plaintext http, and the connection does not set \
                 `allow_http`. Use an https endpoint, or set `allow_http: true` if this is a local \
                 server such as the rustfs in docker-compose.yaml"
            );
        }
        builder = builder.with_endpoint(endpoint);
    }
    // no endpoint and no explicit virtual-hosted setting means object_store
    // addresses real AWS as `https://s3.<region>.amazonaws.com/<bucket>`, which
    // is also the shape every S3-compatible server accepts — so one code path
    // serves both and there is no knob here for it
    Ok(builder.build()?)
}

/// `prefix` as an object-store key prefix.
///
/// Empty is legal and means the root of the bucket — unlike the file output,
/// where an empty path is refused because "the connection's root itself" is
/// already spelled by pointing the connection there. Here the connection is a
/// bucket, and writing at the top of one is an ordinary thing to want.
fn parse_prefix(prefix: &str) -> Result<Option<ObjectPath>> {
    let trimmed = prefix.trim().trim_matches('/');
    if trimmed.is_empty() {
        return Ok(None);
    }
    let path = ObjectPath::parse(trimmed)
        .with_context(|| format!("'{prefix}' is not a usable key prefix"))?;
    Ok(Some(path))
}

/// The part currently being accumulated.
#[derive(Debug)]
struct OpenPart {
    /// The encoded bytes so far. This is the whole reason rotation is mandatory:
    /// it is the part, in memory, until it is uploaded.
    bytes: Vec<u8>,
    key: ObjectPath,
    opened_at: DateTime<Utc>,
    rows: usize,
}

/// Everything about the current part that isn't the bucket.
///
/// Grouped so the three move together, the same reason
/// [`crate::outputs::file::FileOutput`] groups its open file with its counters:
/// a rotation replaces all of them at once.
#[derive(Debug)]
struct Buffered {
    /// Required rather than optional here — see the module docs.
    rotation: Rotation,
    encoder: Encoder,
    open: Option<OpenPart>,
}

/// Uploads batches as rotating objects under one prefix.
#[derive(Debug)]
pub struct S3Output {
    store: AmazonS3,
    prefix: Option<ObjectPath>,
    format: FileFormat,
    buffered: Buffered,
    /// Which part the next one is. Never reset, so two parts opened in the same
    /// second are still distinct keys.
    sequence: u64,
}

impl S3Output {
    /// The key a part opened now would land at.
    fn key(&mut self, opened_at: DateTime<Utc>) -> ObjectPath {
        self.sequence += 1;
        let name = part_name(opened_at, self.sequence, self.format);
        match &self.prefix {
            Some(prefix) => prefix.clone().join(name),
            None => ObjectPath::from(name),
        }
    }

    /// Upload the current part, if there is one with anything in it.
    ///
    /// An empty part is dropped rather than uploaded: on the local filesystem an
    /// empty `json_array` file still has to parse, because a reader walks every
    /// file in the directory — but here nothing was ever created, so the honest
    /// result is no object at all rather than one holding `[]`.
    async fn upload(&mut self) -> Result<()> {
        let Some(part) = self.buffered.open.take() else {
            return Ok(());
        };
        let trailer = self.buffered.encoder.finish();
        if part.rows == 0 {
            return Ok(());
        }
        let mut bytes = part.bytes;
        bytes.extend_from_slice(&trailer);

        let size = bytes.len();
        self.store
            .put(&part.key, PutPayload::from(bytes))
            .await
            .with_context(|| format!("failed to upload {}", part.key))?;
        tracing::debug!(key = %part.key, rows = part.rows, bytes = size, "uploaded s3 part");
        Ok(())
    }
}

#[async_trait::async_trait]
impl OutputDestination for S3Output {
    async fn init(&mut self) -> Result<()> {
        // Nothing is uploaded until the first part rotates, so a pipeline that
        // never produces anything leaves no objects behind — the same rule the
        // file output has about not creating an empty file. The client is
        // already built; there is no connect step to fail here.
        Ok(())
    }

    async fn emit(&mut self, message_batch: Arc<MessageBatch>) -> Result<()> {
        if message_batch.is_empty() {
            return Ok(());
        }
        let bytes = self.buffered.encoder.encode(&message_batch)?;

        if self.buffered.open.is_none() {
            let opened_at = Utc::now();
            let key = self.key(opened_at);
            self.buffered.open = Some(OpenPart {
                bytes: Vec::new(),
                key,
                opened_at,
                rows: 0,
            });
        }
        let part = self
            .buffered
            .open
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("the s3 output has no open part"))?;
        part.bytes.extend_from_slice(&bytes);
        part.rows += message_batch.len();

        // asked after the batch is appended, so a batch is never split across
        // two objects — the same order, and for the same reason, as the file
        // output's
        let (rows, opened_at) = (part.rows, part.opened_at);
        if self.buffered.rotation.is_full(rows, opened_at, Utc::now()) {
            self.upload().await?;
            self.buffered.encoder.reset();
        }
        Ok(())
    }

    /// The part in memory is uploaded or it is lost — there is no half-written
    /// object on the other side to recover from, which makes this hook the
    /// difference between "stopped" and "dropped the last part".
    async fn finish(&mut self) -> Result<()> {
        self.upload().await?;
        self.buffered.encoder.reset();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use kayak_core::config::RotationConfig;
    use kayak_core::connections::{ConnectionKind, Connections};

    fn connection() -> S3Connection {
        S3Connection {
            bucket: "events".to_string(),
            access_key_id: "rustfsadmin".into(),
            secret_access_key: "rustfsadmin".into(),
            endpoint: Some("http://localhost:9000".to_string()),
            region: None,
            allow_http: Some(true),
        }
    }

    fn build(
        connection: S3Connection,
        config: S3OutputConfig,
    ) -> Result<Box<dyn OutputDestination>> {
        let mut pipelines = std::collections::HashMap::new();
        let (events, _) = tokio::sync::broadcast::channel(4);
        let connections: Connections = [("store".to_string(), ConnectionKind::S3(connection))]
            .into_iter()
            .collect();
        let mut ctx = BuildCtx::new(&mut pipelines, "p".to_string(), events)
            .with_connections(Arc::new(connections));
        config.build(&mut ctx)
    }

    fn config() -> S3OutputConfig {
        S3OutputConfig {
            connection: "store".to_string(),
            prefix: "orders".to_string(),
            format: None,
            rotate: RotationConfig {
                max_rows: Some(10),
                interval_secs: None,
            },
        }
    }

    /// Building must not talk to the bucket — a pipeline that starts is one
    /// whose settings parse, not one whose object store happened to be up.
    #[test]
    fn it_builds_against_a_server_that_is_not_running() -> Result<()> {
        build(connection(), config())?;
        Ok(())
    }

    /// The rule the module exists to explain: without a trigger this output
    /// would hold the whole run in memory, so it refuses to be built rather
    /// than doing it.
    #[test]
    fn a_rotation_trigger_is_required() {
        let config = S3OutputConfig {
            rotate: RotationConfig {
                max_rows: None,
                interval_secs: None,
            },
            ..config()
        };
        let Err(err) = build(connection(), config) else {
            panic!("an s3 output was built with no rotation trigger");
        };
        let message = err.to_string();
        assert!(message.contains("rotation trigger"), "{message}");
    }

    /// Credentials over plaintext http is a thing to have written down, and the
    /// error has to name the flag that allows it or nobody finds it.
    #[test]
    fn a_plaintext_endpoint_needs_allow_http() {
        let connection = S3Connection {
            allow_http: None,
            ..connection()
        };
        let Err(err) = build(connection, config()) else {
            panic!("a plaintext endpoint was accepted without allow_http");
        };
        let message = format!("{err:#}");
        assert!(message.contains("allow_http"), "{message}");
    }

    /// The kind is checked as well as the name, same as everywhere else — a
    /// file connection here is a mistake worth naming.
    #[test]
    fn the_connection_has_to_be_an_s3_one() {
        let mut pipelines = std::collections::HashMap::new();
        let (events, _) = tokio::sync::broadcast::channel(4);
        let connections: Connections = [(
            "store".to_string(),
            ConnectionKind::File(kayak_core::connections::FileConnection {
                root: "out".to_string(),
            }),
        )]
        .into_iter()
        .collect();
        let mut ctx = BuildCtx::new(&mut pipelines, "p".to_string(), events)
            .with_connections(Arc::new(connections));
        let Err(err) = config().build(&mut ctx) else {
            panic!("a file connection was accepted by an s3 output");
        };
        let message = format!("{err:#}");
        assert!(message.contains("is a file connection"), "{message}");
    }

    #[test]
    fn a_prefix_becomes_a_key_under_it() -> Result<()> {
        let prefix = parse_prefix("orders/eu")?.context("expected a prefix")?;
        assert_eq!(
            prefix.join("part.ndjson").to_string(),
            "orders/eu/part.ndjson"
        );
        Ok(())
    }

    /// Writing at the top of a bucket is ordinary — the connection *is* the
    /// bucket, so unlike the file output's path there is nothing to insist on.
    #[test]
    fn an_empty_prefix_writes_at_the_root_of_the_bucket() -> Result<()> {
        for prefix in ["", "  ", "/"] {
            assert!(parse_prefix(prefix)?.is_none(), "'{prefix}'");
        }
        Ok(())
    }

    /// Surrounding slashes are how people write prefixes and mean nothing here;
    /// keeping them would produce an empty leading key segment.
    #[test]
    fn surrounding_slashes_are_ignored() -> Result<()> {
        let prefix = parse_prefix("/orders/")?.context("expected a prefix")?;
        assert_eq!(prefix.to_string(), "orders");
        Ok(())
    }
}
