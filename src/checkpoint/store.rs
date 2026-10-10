//! The checkpoint directory on disk.
//!
//! ```text
//! <state-dir>/
//!   manifest.json             which file is current for each pipeline and bucket
//!   pipelines/<hex id>-<epoch>.json
//!   buckets/<hex name>-<epoch>.json
//!   http/<hex id>/…           the http input's write-ahead log (see `inputs::wal`)
//! ```
//!
//! **The manifest is the commit point.** Snapshot files are written first and
//! fsynced, then the manifest naming them is written beside its old self,
//! fsynced and renamed over it, and the directory is fsynced so the rename
//! survives a power cut. A crash anywhere before the rename leaves the old
//! manifest pointing at old files, which are still there — garbage collection
//! only ever runs *after* a commit, and only removes what the committed
//! manifest does not name. So the directory always holds one complete,
//! consistent checkpoint, and the most it can hold besides is unreferenced
//! files from a write that did not finish.
//!
//! File names carry the id **hex-encoded**: a pipeline id is whatever the
//! config said, and nothing a config says may pick a path.

use anyhow::Context;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};

use super::PipelineSnapshot;
use crate::buckets::BucketSnapshot;

/// The manifest's own format. A manifest of another format is refused at
/// startup rather than guessed at.
pub const FORMAT: u32 = 1;

const MANIFEST: &str = "manifest.json";
const PIPELINES: &str = "pipelines";
const BUCKETS: &str = "buckets";

/// Which snapshot is current, for everything that has one.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Manifest {
    pub format: u32,
    #[serde(default)]
    pub pipelines: BTreeMap<String, Entry>,
    #[serde(default)]
    pub buckets: BTreeMap<String, Entry>,
}

impl Default for Manifest {
    fn default() -> Self {
        Self {
            format: FORMAT,
            pipelines: BTreeMap::new(),
            buckets: BTreeMap::new(),
        }
    }
}

/// One current snapshot: the epoch it was taken at, and the file holding it.
/// `file: None` is a snapshot with nothing in it — a stateless pipeline with
/// no positions — which is recorded so the epoch is known without a file
/// being written for it.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    pub epoch: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub file: Option<String>,
}

/// The directory, opened — and locked: see [`Store::open`].
#[derive(Debug)]
pub struct Store {
    dir: PathBuf,
    /// Held for as long as this store lives; the OS releases it if the
    /// process dies.
    _lock: std::fs::File,
}

const LOCK: &str = "lock";

/// Hex, so any id is a safe file name.
fn encode(id: &str) -> String {
    use std::fmt::Write as _;
    id.bytes().fold(String::with_capacity(id.len() * 2), |mut out, b| {
        let _ = write!(out, "{b:02x}");
        out
    })
}

impl Store {
    /// Open (creating it if need be) the state directory and read its
    /// manifest. A missing manifest is a first start; an unreadable one, one of
    /// another format, or one naming a file that is not there is an error —
    /// restoring part of a checkpoint is worse than refusing to start, because
    /// the inputs would then rewind to positions whose state was not restored.
    pub fn open(dir: &Path) -> anyhow::Result<(Self, Manifest)> {
        for sub in [PIPELINES, BUCKETS] {
            std::fs::create_dir_all(dir.join(sub)).with_context(|| {
                format!("failed to create the state directory {}", dir.join(sub).display())
            })?;
        }
        // Exclusive, and refused rather than waited for: two processes
        // checkpointing into one directory overwrite each other's snapshots
        // and garbage-collect each other's files, and the damage is silent
        // until the next restart restores a mixture. The likeliest way to get
        // there is a rolling deploy that starts the new process before the
        // old one has stopped, and failing to start says so.
        let lock_path = dir.join(LOCK);
        let lock = std::fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .write(true)
            .open(&lock_path)
            .with_context(|| format!("failed to open {}", lock_path.display()))?;
        match lock.try_lock() {
            Ok(()) => {}
            Err(std::fs::TryLockError::WouldBlock) => anyhow::bail!(
                "{} is in use by another kayak process; two servers cannot share a state \
                 directory",
                dir.display()
            ),
            Err(std::fs::TryLockError::Error(e)) => {
                return Err(e).with_context(|| format!("failed to lock {}", lock_path.display()));
            }
        }
        let store = Self {
            dir: dir.to_path_buf(),
            _lock: lock,
        };
        let path = dir.join(MANIFEST);
        let manifest = match std::fs::read(&path) {
            Ok(bytes) => serde_json::from_slice::<Manifest>(&bytes)
                .with_context(|| format!("{} is not a readable checkpoint manifest", path.display()))?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Manifest::default(),
            Err(e) => {
                return Err(e).with_context(|| format!("failed to read {}", path.display()));
            }
        };
        anyhow::ensure!(
            manifest.format == FORMAT,
            "{} is a format {} checkpoint manifest, and this kayak reads format {FORMAT}",
            path.display(),
            manifest.format
        );
        for (sub, entries) in [(PIPELINES, &manifest.pipelines), (BUCKETS, &manifest.buckets)] {
            for (name, entry) in entries {
                if let Some(file) = &entry.file {
                    anyhow::ensure!(
                        dir.join(sub).join(file).is_file(),
                        "the checkpoint manifest names {sub}/{file} for '{name}', and it is not \
                         there — the state directory is damaged"
                    );
                }
            }
        }
        Ok((store, manifest))
    }

    #[must_use]
    pub fn dir(&self) -> &Path {
        &self.dir
    }

    pub fn read_pipeline(&self, entry: &Entry) -> anyhow::Result<Option<PipelineSnapshot>> {
        self.read(PIPELINES, entry)
    }

    pub fn read_bucket(&self, entry: &Entry) -> anyhow::Result<Option<BucketSnapshot>> {
        self.read(BUCKETS, entry)
    }

    fn read<T: serde::de::DeserializeOwned>(
        &self,
        sub: &str,
        entry: &Entry,
    ) -> anyhow::Result<Option<T>> {
        let Some(file) = &entry.file else {
            return Ok(None);
        };
        let path = self.dir.join(sub).join(file);
        let bytes =
            std::fs::read(&path).with_context(|| format!("failed to read {}", path.display()))?;
        serde_json::from_slice(&bytes)
            .map(Some)
            .with_context(|| format!("{} is not a readable snapshot", path.display()))
    }

    /// Write one pipeline's snapshot and return the file name to put in the
    /// manifest. Durable when this returns.
    pub fn write_pipeline(
        &self,
        id: &str,
        epoch: u64,
        snapshot: &PipelineSnapshot,
    ) -> anyhow::Result<String> {
        self.write(PIPELINES, id, epoch, snapshot)
    }

    pub fn write_bucket(
        &self,
        name: &str,
        epoch: u64,
        snapshot: &BucketSnapshot,
    ) -> anyhow::Result<String> {
        self.write(BUCKETS, name, epoch, snapshot)
    }

    fn write<T: Serialize>(
        &self,
        sub: &str,
        id: &str,
        epoch: u64,
        value: &T,
    ) -> anyhow::Result<String> {
        let file = format!("{}-{epoch}.json", encode(id));
        let bytes = serde_json::to_vec(value).context("failed to render a snapshot")?;
        write_durably(&self.dir.join(sub).join(&file), &bytes)?;
        Ok(file)
    }

    /// Make `manifest` the current checkpoint. The commit point — see the
    /// module docs.
    pub fn commit(&self, manifest: &Manifest) -> anyhow::Result<()> {
        // the snapshot files were each fsynced; their directory entries have
        // to be durable before a manifest naming them is
        for sub in [PIPELINES, BUCKETS] {
            sync_dir(&self.dir.join(sub))?;
        }
        let bytes = serde_json::to_vec_pretty(manifest).context("failed to render the manifest")?;
        write_durably(&self.dir.join(MANIFEST), &bytes)
    }

    /// Remove every snapshot file the committed manifest does not name, and
    /// any temporary a crashed write left behind. Only ever called after
    /// [`Store::commit`]; a failure here costs disk, never a checkpoint, so it
    /// is logged rather than returned.
    pub fn collect_garbage(&self, manifest: &Manifest) {
        for (sub, entries) in [(PIPELINES, &manifest.pipelines), (BUCKETS, &manifest.buckets)] {
            let keep: std::collections::HashSet<&str> =
                entries.values().filter_map(|e| e.file.as_deref()).collect();
            let dir = self.dir.join(sub);
            let Ok(listing) = std::fs::read_dir(&dir) else {
                continue;
            };
            for entry in listing.flatten() {
                let name = entry.file_name();
                let Some(name) = name.to_str() else { continue };
                if keep.contains(name) {
                    continue;
                }
                if let Err(e) = std::fs::remove_file(entry.path()) {
                    tracing::warn!("could not remove stale snapshot {}: {e}", entry.path().display());
                }
            }
        }
    }
}

/// Write `bytes` to `path` so that after this returns the file is either the
/// whole of `bytes` or — if the process died first — whatever it was before:
/// a sibling temporary, fsynced, renamed over the target, and the directory
/// fsynced so the rename itself survives a power cut.
pub fn write_durably(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    let temporary = crate::persist::temporary_path(path);
    {
        let mut file = std::fs::File::create(&temporary)
            .with_context(|| format!("failed to create {}", temporary.display()))?;
        file.write_all(bytes)
            .with_context(|| format!("failed to write {}", temporary.display()))?;
        file.sync_all()
            .with_context(|| format!("failed to sync {}", temporary.display()))?;
    }
    std::fs::rename(&temporary, path)
        .with_context(|| format!("failed to move {} into place", path.display()))?;
    if let Some(parent) = path.parent() {
        sync_dir(parent)?;
    }
    Ok(())
}

/// Make a directory's entries durable. A no-op where directories cannot be
/// opened for that (Windows), which is the platform's own guarantee instead.
pub fn sync_dir(dir: &Path) -> anyhow::Result<()> {
    #[cfg(unix)]
    {
        std::fs::File::open(dir)
            .and_then(|d| d.sync_all())
            .with_context(|| format!("failed to sync the directory {}", dir.display()))?;
    }
    #[cfg(not(unix))]
    let _ = dir;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::ComponentState;
    use serde_json::json;

    fn snapshot(n: u64) -> PipelineSnapshot {
        PipelineSnapshot {
            inputs: vec![ComponentState {
                config: json!({"type": "test"}),
                state: Some(json!({"offset": n})),
            }],
            ..PipelineSnapshot::default()
        }
    }

    #[test]
    fn a_first_start_has_an_empty_manifest() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let (_, manifest) = Store::open(dir.path())?;
        assert_eq!(manifest, Manifest::default());
        Ok(())
    }

    /// The round trip: what was committed is what the next open reads.
    #[test]
    fn a_committed_snapshot_is_read_back() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let (store, mut manifest) = Store::open(dir.path())?;
        let file = store.write_pipeline("a/../b", 7, &snapshot(42))?;
        manifest.pipelines.insert(
            "a/../b".into(),
            Entry {
                epoch: 7,
                file: Some(file.clone()),
            },
        );
        store.commit(&manifest)?;
        assert!(!file.contains('/'), "an id chose a path: {file}");
        drop(store);

        let (store, read) = Store::open(dir.path())?;
        assert_eq!(read, manifest);
        let entry = read.pipelines.get("a/../b").cloned().unwrap_or(Entry { epoch: 0, file: None });
        assert_eq!(store.read_pipeline(&entry)?, Some(snapshot(42)));
        Ok(())
    }

    /// The crash before the commit point: a snapshot file was written, the
    /// manifest was not, and the previous checkpoint is still the one read.
    #[test]
    fn a_snapshot_written_but_not_committed_is_not_the_checkpoint() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let (store, mut manifest) = Store::open(dir.path())?;
        let first = store.write_pipeline("p", 1, &snapshot(1))?;
        manifest.pipelines.insert("p".into(), Entry { epoch: 1, file: Some(first) });
        store.commit(&manifest)?;
        // epoch 2's file lands, and then the process dies
        store.write_pipeline("p", 2, &snapshot(2))?;
        drop(store);

        let (store, read) = Store::open(dir.path())?;
        let entry = read.pipelines.get("p").cloned().unwrap_or(Entry { epoch: 0, file: None });
        assert_eq!(entry.epoch, 1);
        assert_eq!(store.read_pipeline(&entry)?, Some(snapshot(1)));
        Ok(())
    }

    /// Garbage collection removes what the manifest does not name — the stale
    /// epoch and an orphan from a write that never committed — and nothing
    /// else.
    #[test]
    fn garbage_collection_keeps_exactly_what_the_manifest_names() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let (store, mut manifest) = Store::open(dir.path())?;
        let old = store.write_pipeline("p", 1, &snapshot(1))?;
        let new = store.write_pipeline("p", 2, &snapshot(2))?;
        let orphan = store.write_pipeline("q", 3, &snapshot(3))?;
        manifest.pipelines.insert("p".into(), Entry { epoch: 2, file: Some(new.clone()) });
        store.commit(&manifest)?;
        store.collect_garbage(&manifest);

        let pipelines = dir.path().join(PIPELINES);
        assert!(pipelines.join(&new).is_file());
        assert!(!pipelines.join(&old).exists());
        assert!(!pipelines.join(&orphan).exists());
        Ok(())
    }

    /// A manifest naming a file that is gone is a damaged directory, and
    /// restoring part of a checkpoint is refused rather than attempted.
    #[test]
    fn a_manifest_naming_a_missing_file_is_refused() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let (store, mut manifest) = Store::open(dir.path())?;
        let file = store.write_pipeline("p", 1, &snapshot(1))?;
        manifest.pipelines.insert("p".into(), Entry { epoch: 1, file: Some(file.clone()) });
        store.commit(&manifest)?;
        drop(store);
        std::fs::remove_file(dir.path().join(PIPELINES).join(&file))?;

        let Err(err) = Store::open(dir.path()) else {
            panic!("a damaged state directory was opened");
        };
        assert!(format!("{err:#}").contains("damaged"), "{err:#}");
        Ok(())
    }

    #[test]
    fn a_manifest_of_another_format_is_refused() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        std::fs::write(dir.path().join(MANIFEST), r#"{"format": 99}"#)?;
        let Err(err) = Store::open(dir.path()) else {
            panic!("a format 99 manifest was read");
        };
        assert!(format!("{err:#}").contains("format 99"), "{err:#}");
        Ok(())
    }

    /// Two servers on one state directory would overwrite each other's
    /// snapshots, so the second is refused — and the directory is free again
    /// the moment the first lets go.
    #[test]
    fn a_state_directory_has_one_owner_at_a_time() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let (first, _) = Store::open(dir.path())?;
        let Err(err) = Store::open(dir.path()) else {
            panic!("two stores opened one directory");
        };
        assert!(format!("{err:#}").contains("in use"), "{err:#}");
        drop(first);
        Store::open(dir.path())?;
        Ok(())
    }
}
