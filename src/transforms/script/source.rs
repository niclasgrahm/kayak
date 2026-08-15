//! Getting a script's text, from the config or from a file beside it.
//!
//! The boundary here is the same kind of thing [`crate::outputs::file::Root`]
//! and [`crate::persist::save_path`] are, and it is written the same way: a
//! path arrives from a config file that may itself have arrived over HTTP, so
//! it is **refused, never normalised**. Trimming a `..` away would leave
//! whoever wrote it believing it meant something.
//!
//! Where it differs from the file output's sandbox is which directory bounds
//! it, and that is worth being precise about because this server now has three:
//!
//! - `--data-dir` bounds where pipelines write **data**,
//! - `AppState`'s `save_dir` bounds where the server writes **configs**,
//! - and this one bounds where it *reads* **scripts**.
//!
//! This third one is not a flag. It is the directory the config file is in,
//! derived the way the connections and layout files' paths are derived, because
//! a script is part of the description of the graph in exactly the way those
//! are — the set travels together or it stops working. The consequence is that
//! a server started **without** a config file refuses a file-sourced script:
//! there is no directory to resolve against, and the working directory would be
//! a boundary that moved depending on where someone happened to launch the
//! server from. Inline scripts still work there, which is what the HTTP API and
//! the UI use anyway.

use std::path::{Component, Path, PathBuf};

use anyhow::{Context, Result, bail, ensure};
use kayak_core::script::ScriptSource;

/// The script's text, wherever the config said to find it.
pub fn read(source: &ScriptSource, script_dir: Option<&Path>) -> Result<String> {
    match source {
        ScriptSource::Inline { code } => {
            ensure!(
                !code.trim().is_empty(),
                "an inline script is empty. A transform that does nothing is better spelled by \
                 leaving it out than by leaving it blank"
            );
            Ok(code.clone())
        }
        ScriptSource::File { path } => read_file(path, script_dir),
    }
}

fn read_file(path: &str, script_dir: Option<&Path>) -> Result<String> {
    let Some(script_dir) = script_dir else {
        bail!(
            "this script names the file '{path}', but the server has no config file to resolve \
             it against — a file-sourced script lives beside the config, and this server was \
             started without one. Write the script inline instead, or start the server with \
             --config"
        )
    };

    let relative = relative_path(path)?;
    let file = script_dir.join(&relative);
    let text = std::fs::read_to_string(&file)
        .with_context(|| format!("failed to read the script file '{path}'"))?;

    // Re-checked after canonicalizing rather than trusting the join, for the
    // reason the file output re-checks its landing directory: every component
    // of `relative` is a plain name, but a *symlink* planted inside the
    // directory can still point out of it, and only asking the filesystem where
    // we actually landed catches that.
    let landed = std::fs::canonicalize(&file)
        .with_context(|| format!("failed to resolve the script file '{path}'"))?;
    let root = std::fs::canonicalize(script_dir)
        .with_context(|| format!("failed to resolve {}", script_dir.display()))?;
    ensure!(
        landed.starts_with(&root),
        "the script file '{path}' resolves to {}, which is outside the config's directory ({}). \
         A symlink pointing out of it is not a way through it",
        landed.display(),
        root.display()
    );

    ensure!(
        !text.trim().is_empty(),
        "the script file '{path}' is empty"
    );
    Ok(text)
}

/// `path` as a relative path of ordinary names, or an error saying why it
/// isn't one. Every rejection here is a path that *could* have been made to
/// work by rewriting it, and none of them are — see the module docs.
fn relative_path(path: &str) -> Result<PathBuf> {
    let trimmed = path.trim();
    ensure!(
        !trimmed.is_empty(),
        "a script file needs a path, e.g. 'scripts/enrich.rhai'"
    );
    let path = Path::new(trimmed);
    ensure!(
        path.is_relative(),
        "'{trimmed}' is an absolute path. A script file is resolved against the config file's \
         directory, so its path has to be relative to it"
    );
    for component in path.components() {
        match component {
            Component::Normal(_) => {}
            Component::ParentDir => bail!(
                "'{trimmed}' contains '..'. A script has to live under the config file's \
                 directory, and the path is refused rather than trimmed"
            ),
            Component::CurDir => bail!(
                "'{trimmed}' contains '.'. Write the path as a plain relative path, e.g. \
                 'scripts/enrich.rhai'"
            ),
            Component::RootDir | Component::Prefix(_) => {
                bail!("'{trimmed}' is not a relative path: it names a filesystem root")
            }
        }
    }
    Ok(path.to_path_buf())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn inline(code: &str) -> ScriptSource {
        ScriptSource::Inline {
            code: code.to_string(),
        }
    }

    fn file(path: &str) -> ScriptSource {
        ScriptSource::File {
            path: path.to_string(),
        }
    }

    #[test]
    fn an_inline_script_is_its_own_text() -> Result<()> {
        assert_eq!(read(&inline("emit(msg);"), None)?, "emit(msg);");
        Ok(())
    }

    /// A blank script is a transform that silently drops every message. That is
    /// almost certainly a half-finished edit rather than an intention.
    #[test]
    fn a_blank_script_is_refused() {
        assert!(read(&inline("   \n  "), None).is_err());
    }

    #[test]
    fn a_file_script_is_read_from_beside_the_config() -> Result<()> {
        let dir = tempfile::tempdir()?;
        std::fs::create_dir(dir.path().join("scripts"))?;
        std::fs::write(dir.path().join("scripts/enrich.rhai"), "emit(msg);")?;
        assert_eq!(
            read(&file("scripts/enrich.rhai"), Some(dir.path()))?,
            "emit(msg);"
        );
        Ok(())
    }

    /// The closed default: no config file means no directory to resolve
    /// against, and the working directory is not a boundary — see the module
    /// docs.
    #[test]
    fn a_file_script_needs_a_config_file_to_resolve_against() {
        let Err(err) = read(&file("scripts/enrich.rhai"), None) else {
            panic!("a file script cannot resolve without a config");
        };
        assert!(
            format!("{err:#}").contains("--config"),
            "the error should say how to fix it: {err:#}"
        );
    }

    /// Refused, not trimmed. Each of these is a path that a normaliser would
    /// have quietly turned into a working one.
    #[test]
    fn a_path_that_climbs_out_is_refused_rather_than_trimmed() -> Result<()> {
        let dir = tempfile::tempdir()?;
        for path in ["../outside.rhai", "scripts/../../outside.rhai", "/etc/passwd"] {
            assert!(
                read(&file(path), Some(dir.path())).is_err(),
                "'{path}' should have been refused"
            );
        }
        Ok(())
    }

    /// The check the component-wise one cannot make: every component of the
    /// path is an ordinary name, and it still lands outside.
    #[cfg(unix)]
    #[test]
    fn a_symlink_pointing_out_of_the_directory_is_refused() -> Result<()> {
        let outside = tempfile::tempdir()?;
        std::fs::write(outside.path().join("secret.rhai"), "emit(msg);")?;
        let dir = tempfile::tempdir()?;
        std::os::unix::fs::symlink(outside.path().join("secret.rhai"), dir.path().join("link.rhai"))?;

        let Err(err) = read(&file("link.rhai"), Some(dir.path())) else {
            panic!("a symlink out of the config directory is not a way through it");
        };
        assert!(
            format!("{err:#}").contains("outside the config's directory"),
            "the error should say what happened: {err:#}"
        );
        Ok(())
    }

    #[test]
    fn a_missing_script_file_names_itself() -> Result<()> {
        let dir = tempfile::tempdir()?;
        let Err(err) = read(&file("scripts/absent.rhai"), Some(dir.path())) else {
            panic!("a script file that is not there cannot be read");
        };
        assert!(
            format!("{err:#}").contains("scripts/absent.rhai"),
            "the error should name the file: {err:#}"
        );
        Ok(())
    }
}
