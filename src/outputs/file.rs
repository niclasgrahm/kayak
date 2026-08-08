//! Writes batches to files in a directory on the server.
//!
//! Two halves, and the split is the interesting part. Everything about *when* a
//! part is finished, what it is called and how messages are laid out inside it
//! lives in [`crate::outputs::rotate`], because the object-store output wants
//! exactly those and none of this file's. What lives here is the destination:
//! opening a real file, and — the part worth reading carefully — deciding which
//! files this output is allowed to open at all.

use anyhow::{Context, Result, bail, ensure};
use chrono::{DateTime, Utc};
use kayak_core::config::{FileFormat, FileOutputConfig};
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;

use crate::BuildCtx;
use crate::inputs::MessageBatch;
use crate::outputs::rotate::{Encoder, Rotation, part_name};
use crate::outputs::{BuildOutput, OutputDestination};

impl BuildOutput for FileOutputConfig {
    fn build(self, ctx: &mut BuildCtx) -> Result<Box<dyn OutputDestination>> {
        let connection = ctx
            .file_connection(&self.connection)
            .context("the file output cannot be built")?;
        // resolved at build time rather than on first write, and it is the
        // build that creates the directory: a path that escapes the sandbox, or
        // a root nobody can write to, should fail the pipeline that owns it
        // rather than surface an hour into a run
        let data_dir = ctx.data_dir.as_deref().map(PathBuf::as_path);
        let directory =
            Root::resolve(data_dir, &connection.root, &self.path).with_context(|| {
                format!(
                    "the file output cannot write under connection '{}'",
                    self.connection
                )
            })?;
        let format = self.format.unwrap_or_default();
        Ok(Box::new(FileOutput {
            directory,
            format,
            rotation: Rotation::new(self.rotate),
            encoder: Encoder::new(format),
            open: None,
            sequence: 0,
        }))
    }
}

/// A directory this output is allowed to write in.
///
/// Constructing one is the whole security story, so it is the only way to get a
/// path into [`FileOutput`].
///
/// **The reason this is a boundary and not a convenience** is the same one
/// [`crate::persist::save_path`] gives: the browser does not write to the
/// server's disk, the *server* does, on request. `POST /api/pipelines` and
/// `POST /api/connections` both take their contents from an HTTP body, so an
/// unconstrained path in either would turn a pipeline editor into an
/// arbitrary-write primitive — and this one writes attacker-influenced *content*
/// too, at whatever volume the pipeline carries.
///
/// So there are two layers, and neither is sufficient alone:
///
/// 1. `--data-dir`, fixed when the process starts and reachable by no request.
///    Without it there is no file output at all — the closed default matters,
///    because this is a component almost nobody wants in production and leaving
///    it on by default would make every deployment opt *out* of a disk writer.
/// 2. The connection's `root`, which arrives over HTTP like anything else and
///    is therefore checked against layer 1 rather than trusted. It is what lets
///    an operator hand different pipelines different subtrees.
///
/// Paths are **refused, never normalised**. Trimming a `..` away would leave
/// someone who wrote one believing it meant something, and a normaliser is
/// exactly the kind of code that is one edge case away from being the hole it
/// was written to close.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Root {
    directory: PathBuf,
}

impl Root {
    /// The directory `path` names under `root`, if the server is allowed to
    /// write there. Creates it, since a pipeline that cannot write until
    /// someone runs `mkdir` is a pipeline that fails at runtime for no reason.
    ///
    /// `data_dir` is expected already canonicalized — `main` does it once at
    /// startup, so a symlinked `--data-dir` is compared against what it really
    /// points at rather than failing every check below.
    pub fn resolve(data_dir: Option<&Path>, root: &str, path: &str) -> Result<Self> {
        let Some(data_dir) = data_dir else {
            bail!(
                "file output is turned off: the server was started without --data-dir, so it has \
                 nowhere it is allowed to write. Start it with --data-dir <path> to enable file \
                 outputs under that directory"
            )
        };

        // checked before anything is created, so a rejected path leaves no
        // directories behind as a side effect of having been refused
        let relative = Self::relative_path(path)?;

        let root_directory = Self::prepare(Path::new(root))
            .with_context(|| format!("the connection's root '{root}' is not usable"))?;
        ensure!(
            root_directory.starts_with(data_dir),
            "the connection's root '{}' is outside the server's --data-dir ({}), which is the \
             only place this server may write. Point the connection somewhere under it, or start \
             the server with a --data-dir that contains it",
            root_directory.display(),
            data_dir.display()
        );

        let directory = Self::prepare(&root_directory.join(&relative))
            .with_context(|| format!("'{path}' is not usable under '{root}'"))?;
        // re-checked after canonicalizing rather than trusting the join: every
        // component of `relative` is a plain name, but a *symlink* planted
        // inside the root can still point out of it, and only asking the
        // filesystem where we actually landed catches that. Checked against
        // both layers, since the root passing layer 1 says nothing about where
        // a link under it goes
        ensure!(
            directory.starts_with(&root_directory) && directory.starts_with(data_dir),
            "'{path}' resolves to {}, which is outside the connection's root ({}). A symlink \
             pointing out of the root is not a way through it",
            directory.display(),
            root_directory.display()
        );

        Ok(Self { directory })
    }

    /// The path a part of this output lands at.
    #[must_use]
    pub fn join(&self, name: &str) -> PathBuf {
        self.directory.join(name)
    }

    /// `path` as a relative path of ordinary names, or an error saying why it
    /// isn't one.
    ///
    /// Every rejection here is a path that *could* have been made to work by
    /// rewriting it, and none of them are. See the type's documentation.
    fn relative_path(path: &str) -> Result<PathBuf> {
        let trimmed = path.trim();
        ensure!(
            !trimmed.is_empty(),
            "a path is required: name a directory under the connection's root, e.g. 'orders'"
        );
        let path = Path::new(trimmed);
        ensure!(
            path.is_relative(),
            "'{trimmed}' is an absolute path. A file output writes under its connection's root, \
             so its path has to be relative to it"
        );
        for component in path.components() {
            match component {
                Component::Normal(_) => {}
                Component::ParentDir => bail!(
                    "'{trimmed}' contains '..'. A file output cannot write outside its \
                     connection's root, and the path is refused rather than trimmed"
                ),
                Component::CurDir => bail!(
                    "'{trimmed}' contains '.'. Write the path as a plain relative path, e.g. \
                     'orders' or 'orders/eu'"
                ),
                Component::RootDir | Component::Prefix(_) => {
                    bail!("'{trimmed}' is not a relative path: it names a filesystem root")
                }
            }
        }
        Ok(path.to_path_buf())
    }

    /// Create `directory` if needed and return where it actually is.
    ///
    /// Canonicalizing is what makes the checks above mean anything — it is also
    /// why the directory has to exist first, since a path that isn't there
    /// cannot be resolved.
    fn prepare(directory: &Path) -> Result<PathBuf> {
        std::fs::create_dir_all(directory)
            .with_context(|| format!("failed to create {}", directory.display()))?;
        std::fs::canonicalize(directory)
            .with_context(|| format!("failed to resolve {}", directory.display()))
    }
}

/// The part currently being written.
///
/// Held together in one struct because the three move as a unit — a rotation
/// replaces all of them at once, and a half-rotated writer counting rows into
/// the wrong file is exactly the bug this shape prevents.
#[derive(Debug)]
struct OpenPart {
    file: File,
    path: PathBuf,
    opened_at: DateTime<Utc>,
    rows: usize,
}

/// Appends batches to rotating files in one directory.
#[derive(Debug)]
pub struct FileOutput {
    directory: Root,
    format: FileFormat,
    rotation: Rotation,
    encoder: Encoder,
    open: Option<OpenPart>,
    /// Which part the next one is. Never reset, so two parts opened in the same
    /// second are still distinct names.
    sequence: u64,
}

impl FileOutput {
    /// Open the next part.
    async fn open(&mut self) -> Result<()> {
        let opened_at = Utc::now();
        self.sequence += 1;
        let path = self
            .directory
            .join(&part_name(opened_at, self.sequence, self.format));
        // `create_new` rather than `create`: a name collision means the naming
        // scheme failed, and silently truncating someone's data is the worst
        // available response to that
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .await
            .with_context(|| format!("failed to create {}", path.display()))?;
        self.encoder.reset();
        self.open = Some(OpenPart {
            file,
            path,
            opened_at,
            rows: 0,
        });
        Ok(())
    }

    /// Finish the current part, if there is one.
    async fn close(&mut self) -> Result<()> {
        let Some(mut part) = self.open.take() else {
            return Ok(());
        };
        let trailer = self.encoder.finish();
        if !trailer.is_empty() {
            part.file
                .write_all(&trailer)
                .await
                .with_context(|| format!("failed to finish writing {}", part.path.display()))?;
        }
        part.file
            .flush()
            .await
            .with_context(|| format!("failed to flush {}", part.path.display()))?;
        tracing::debug!(path = %part.path.display(), rows = part.rows, "closed file part");
        Ok(())
    }
}

#[async_trait::async_trait]
impl OutputDestination for FileOutput {
    async fn init(&mut self) -> Result<()> {
        // the directory was created and checked at build time; the first part
        // waits for the first batch, so a pipeline that never produces anything
        // does not litter the directory with an empty file
        Ok(())
    }

    async fn emit(&mut self, message_batch: Arc<MessageBatch>) -> Result<()> {
        if message_batch.is_empty() {
            return Ok(());
        }
        if self.open.is_none() {
            self.open().await?;
        }
        let bytes = self.encoder.encode(&message_batch)?;

        let part = self
            .open
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("the file output has no open part"))?;
        part.file
            .write_all(&bytes)
            .await
            .with_context(|| format!("failed to write to {}", part.path.display()))?;
        // flushed per batch rather than buffered across them: this output's
        // whole purpose is being read while it runs, and an unflushed buffer is
        // also what would be lost if the pipeline were cancelled mid-part
        part.file
            .flush()
            .await
            .with_context(|| format!("failed to flush {}", part.path.display()))?;
        part.rows += message_batch.len();

        // asked after the write, so a batch is never split across two parts
        let (rows, opened_at) = (part.rows, part.opened_at);
        if self.rotation.is_full(rows, opened_at, Utc::now()) {
            self.close().await?;
        }
        Ok(())
    }

    /// A part left open when the pipeline stops still has to be finished. It
    /// only matters for `json_array` — ndjson is complete after every batch —
    /// but that is exactly the case where not doing it leaves an unparseable
    /// file behind, with no `]` and no rotation coming to add one.
    async fn finish(&mut self) -> Result<()> {
        self.close().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A sandbox two levels deep, so a test can tell "outside the root" from
    /// "outside the data dir" — they are different failures.
    struct Sandbox {
        _directory: tempfile::TempDir,
        data_dir: PathBuf,
        root: PathBuf,
    }

    fn sandbox() -> Result<Sandbox> {
        let directory = tempfile::tempdir()?;
        let data_dir = std::fs::canonicalize(directory.path())?;
        let root = data_dir.join("events");
        std::fs::create_dir_all(&root)?;
        Ok(Sandbox {
            _directory: directory,
            data_dir,
            root,
        })
    }

    fn resolve(sandbox: &Sandbox, path: &str) -> Result<Root> {
        Root::resolve(
            Some(&sandbox.data_dir),
            &sandbox.root.to_string_lossy(),
            path,
        )
    }

    #[test]
    fn a_relative_path_resolves_under_the_root() -> Result<()> {
        let sandbox = sandbox()?;
        let resolved = resolve(&sandbox, "orders")?;
        assert_eq!(resolved.directory, sandbox.root.join("orders"));
        // and it was created, so the first write does not have to
        assert!(sandbox.root.join("orders").is_dir());
        Ok(())
    }

    #[test]
    fn a_nested_path_resolves_too() -> Result<()> {
        let sandbox = sandbox()?;
        let resolved = resolve(&sandbox, "orders/eu")?;
        assert_eq!(resolved.directory, sandbox.root.join("orders").join("eu"));
        Ok(())
    }

    /// The closed default. A server that was never told where it may write must
    /// not be talked into writing anywhere.
    #[test]
    fn without_a_data_dir_there_is_no_file_output() -> Result<()> {
        let sandbox = sandbox()?;
        let Err(err) = Root::resolve(None, &sandbox.root.to_string_lossy(), "orders") else {
            panic!("a file output built on a server with no --data-dir");
        };
        let message = err.to_string();
        assert!(message.contains("--data-dir"), "{message}");
        Ok(())
    }

    /// The reason the path is refused rather than normalised: someone who wrote
    /// this meant to leave the root, and quietly deciding they didn't is worse
    /// than telling them.
    #[test]
    fn a_parent_directory_component_is_refused() -> Result<()> {
        let sandbox = sandbox()?;
        for path in ["../escaped", "orders/../../escaped", ".."] {
            let Err(err) = resolve(&sandbox, path) else {
                panic!("'{path}' was accepted");
            };
            assert!(err.to_string().contains(".."), "{path}: {err}");
        }
        // and nothing was created on the way to refusing
        assert!(!sandbox.data_dir.join("escaped").exists());
        Ok(())
    }

    #[test]
    fn an_absolute_path_is_refused() -> Result<()> {
        let sandbox = sandbox()?;
        let Err(err) = resolve(&sandbox, "/etc/kayak") else {
            panic!("an absolute path was accepted");
        };
        assert!(err.to_string().contains("absolute"), "{err}");
        Ok(())
    }

    #[test]
    fn an_empty_path_is_refused() -> Result<()> {
        let sandbox = sandbox()?;
        for path in ["", "   "] {
            assert!(resolve(&sandbox, path).is_err(), "'{path}' was accepted");
        }
        Ok(())
    }

    /// Layer 2 checked against layer 1. A connection arrives over HTTP, so a
    /// root outside the sandbox is a request that has to be refused rather than
    /// an operator's decision to be honoured.
    #[test]
    fn a_root_outside_the_data_dir_is_refused() -> Result<()> {
        let sandbox = sandbox()?;
        let outside = tempfile::tempdir()?;
        let Err(err) = Root::resolve(
            Some(&sandbox.data_dir),
            &outside.path().to_string_lossy(),
            "orders",
        ) else {
            panic!("a root outside the data dir was accepted");
        };
        let message = err.to_string();
        assert!(message.contains("--data-dir"), "{message}");
        Ok(())
    }

    /// The data dir itself is a legal root — the flag alone is a complete
    /// configuration, without a connection having to name a subdirectory.
    #[test]
    fn the_data_dir_itself_is_a_legal_root() -> Result<()> {
        let sandbox = sandbox()?;
        let resolved = Root::resolve(
            Some(&sandbox.data_dir),
            &sandbox.data_dir.to_string_lossy(),
            "orders",
        )?;
        assert_eq!(resolved.directory, sandbox.data_dir.join("orders"));
        Ok(())
    }

    /// The component check passes every one of these — they are ordinary names.
    /// Only asking the filesystem where the path actually landed catches it,
    /// which is why the check is after the canonicalize and not instead of it.
    #[cfg(unix)]
    #[test]
    fn a_symlink_pointing_out_of_the_root_is_not_a_way_through_it() -> Result<()> {
        let sandbox = sandbox()?;
        let outside = tempfile::tempdir()?;
        std::os::unix::fs::symlink(outside.path(), sandbox.root.join("escape"))?;

        let Err(err) = resolve(&sandbox, "escape") else {
            panic!("a symlink out of the root was followed");
        };
        assert!(err.to_string().contains("symlink"), "{err}");

        // and the same one step further in, where the link is not the last
        // component
        assert!(resolve(&sandbox, "escape/orders").is_err());
        Ok(())
    }

    /// A link that stays inside the sandbox is not an escape and must keep
    /// working — the rule is about where you land, not about links.
    #[cfg(unix)]
    #[test]
    fn a_symlink_staying_inside_the_root_is_fine() -> Result<()> {
        let sandbox = sandbox()?;
        std::fs::create_dir_all(sandbox.root.join("real"))?;
        std::os::unix::fs::symlink(sandbox.root.join("real"), sandbox.root.join("link"))?;
        let resolved = resolve(&sandbox, "link")?;
        assert_eq!(resolved.directory, sandbox.root.join("real"));
        Ok(())
    }
}
