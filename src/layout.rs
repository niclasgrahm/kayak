//! Reading and writing the canvas layout file.
//!
//! A sibling of [`crate::persist`], and deliberately not part of it: the config
//! file describes what the server *runs*, this one describes where someone
//! dragged the cards. Keeping them apart is what lets the config file stay
//! reviewable, and what lets this one be written far more freely — moving a
//! node is not a change to the system, so it does not wait for an explicit save
//! and does not count as an unsaved change.
//!
//! Two things are shared with `persist` on purpose. The write is atomic, for
//! the same reason: a crash mid-write must not lose an arrangement. And the
//! render is deterministic, because the file is meant to be committed — see
//! [`streamer_core::layout`].
//!
//! Always JSON, whatever the config file is written in. Nobody hand-writes this
//! one, so there is nothing for a second format to buy.

use std::path::{Path, PathBuf};

use anyhow::Context;
use streamer_core::LayoutFile;

/// The layout file that belongs to a given config file: `config.json` is
/// arranged by `config.layout.json`, `pipelines.yaml` by
/// `pipelines.layout.json`.
///
/// Derived rather than configured, so the pair travels together — copy the
/// config file into a new directory and the arrangement is simply not there
/// yet, which is exactly right.
#[must_use]
pub fn layout_path(config_path: &Path) -> PathBuf {
    let stem = config_path
        .file_stem()
        .map_or_else(|| "config".to_string(), |s| s.to_string_lossy().into_owned());
    config_path.with_file_name(format!("{stem}.layout.json"))
}

/// The arrangement in `path`.
///
/// **A missing file is not an error**: it is the ordinary state of a graph
/// nobody has arranged yet, and the answer — "no node has been placed" — is the
/// same one an empty file gives. A file that exists but won't parse *is* an
/// error, because silently discarding someone's arrangement is worse than
/// refusing to start.
pub fn read(path: &Path) -> anyhow::Result<LayoutFile> {
    let contents = match std::fs::read_to_string(path) {
        Ok(contents) => contents,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => return Ok(LayoutFile::default()),
        Err(err) => {
            return Err(anyhow::Error::new(err))
                .with_context(|| format!("failed to open layout file {}", path.display()));
        }
    };
    serde_json::from_str(&contents)
        .with_context(|| format!("failed to parse layout file {}", path.display()))
}

/// The file's contents: pretty-printed with a trailing newline, because it is
/// committed and diffed by a human even though it is written by the program.
pub fn render(layout: &LayoutFile) -> anyhow::Result<String> {
    let mut json = serde_json::to_string_pretty(layout).context("failed to serialize the layout")?;
    json.push('\n');
    Ok(json)
}

/// Replace `path` with `layout`, staged through a sibling temporary and renamed
/// into place.
pub fn write(path: &Path, layout: &LayoutFile) -> anyhow::Result<()> {
    let contents = render(layout)?;
    let temporary = crate::persist::temporary_path(path);
    std::fs::write(&temporary, contents)
        .with_context(|| format!("failed to write {}", temporary.display()))?;
    std::fs::rename(&temporary, path).with_context(|| {
        format!(
            "failed to move {} into place at {}",
            temporary.display(),
            path.display()
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use streamer_core::NodeLayout;

    fn placed(x: f64, y: f64) -> NodeLayout {
        NodeLayout {
            x,
            y,
            width: 360.0,
            height: None,
        }
    }

    /// The pair has to be findable from the config path alone, in both formats,
    /// and the layout is always JSON.
    #[test]
    fn the_layout_file_sits_beside_the_config_it_arranges() {
        assert_eq!(
            layout_path(Path::new("/srv/kayak/config.json")),
            Path::new("/srv/kayak/config.layout.json")
        );
        assert_eq!(
            layout_path(Path::new("/srv/kayak/pipelines.yaml")),
            Path::new("/srv/kayak/pipelines.layout.json")
        );
    }

    /// The common case on a fresh checkout: there is no layout file, and that
    /// means "nothing has been arranged", not "startup failed".
    #[test]
    fn a_missing_file_reads_as_an_empty_layout() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let layout = read(&dir.path().join("config.layout.json"))?;
        assert!(layout.is_empty());
        Ok(())
    }

    /// A file that exists but is broken must not be silently replaced with an
    /// empty one — the next write would then wipe the arrangement it holds.
    #[test]
    fn a_broken_file_is_an_error_rather_than_an_empty_layout() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("config.layout.json");
        std::fs::write(&path, "{ not json")?;
        assert!(read(&path).is_err());
        Ok(())
    }

    #[test]
    fn a_layout_round_trips_through_a_file() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("config.layout.json");

        let mut layout = LayoutFile::default();
        layout.nodes.insert("a".to_string(), placed(0.0, 0.0));
        layout.nodes.insert("b".to_string(), placed(400.0, 200.0));

        write(&path, &layout)?;
        assert_eq!(read(&path)?, layout);
        assert!(
            !crate::persist::temporary_path(&path).exists(),
            "the staging file was left behind"
        );
        Ok(())
    }

    /// Committed file, so the same arrangement has to render the same bytes —
    /// otherwise re-saving an untouched graph shows up as a diff.
    #[test]
    fn the_same_arrangement_renders_the_same_bytes() -> anyhow::Result<()> {
        let one = {
            let mut l = LayoutFile::default();
            l.nodes.insert("z".to_string(), placed(1.0, 2.0));
            l.nodes.insert("a".to_string(), placed(3.0, 4.0));
            render(&l)?
        };
        let two = {
            let mut l = LayoutFile::default();
            l.nodes.insert("a".to_string(), placed(3.0, 4.0));
            l.nodes.insert("z".to_string(), placed(1.0, 2.0));
            render(&l)?
        };
        assert_eq!(one, two);
        assert!(one.ends_with("}\n"), "got: {one}");
        Ok(())
    }
}
