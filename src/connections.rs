//! Reading and writing the connections file.
//!
//! The third file the server takes, after the config and the secrets: the
//! systems pipelines talk to, declared once under a name. The types are in
//! [`kayak_core::connections`] — this is the file IO and the rules around it.
//!
//! It follows the **config file's** rules rather than the layout file's, and
//! deliberately: adding a connection changes what the server can build, so it is
//! a load source and a save target, never a mirror. Nothing here writes except
//! an explicit save, the write is atomic, and the render is deterministic
//! because the file is committed. The format is JSON or YAML, decided by the
//! extension at the two edges and nowhere else — the same rule, and the same
//! reason, as [`crate::persist`].
//!
//! Secrets are as safe here as in a config file: a connection only ever holds
//! the unresolved `${NAME}` template, and resolution happens when a component
//! is built.

use std::path::{Path, PathBuf};

use anyhow::Context;
use kayak_core::{ConfigFormat, Connections};

/// The connections file that belongs to a config file, when the server wasn't
/// told about one: `config.json` → `config.connections.json`,
/// `pipelines.yaml` → `pipelines.connections.yaml`.
///
/// Derived the same way the layout file's path is, so the set travels together;
/// unlike the layout, it keeps the config's *format*, because this one is
/// hand-written and someone who chose YAML meant it for both.
#[must_use]
pub fn connections_path(config_path: &Path) -> PathBuf {
    let stem = config_path.file_stem().map_or_else(
        || "config".to_string(),
        |s| s.to_string_lossy().into_owned(),
    );
    let extension = crate::persist::format_of(config_path).extension();
    config_path.with_file_name(format!("{stem}.connections.{extension}"))
}

/// The connections in a file's contents.
pub fn parse(contents: &str, format: ConfigFormat) -> anyhow::Result<Connections> {
    match format {
        ConfigFormat::Json => serde_json::from_str(contents).map_err(Into::into),
        ConfigFormat::Yaml => serde_norway::from_str(contents).map_err(Into::into),
    }
}

/// The file's contents, pretty-printed with a trailing newline.
///
/// Deterministic, because [`Connections`] iterates by name: the same
/// connections render the same bytes, which is what lets "are there unsaved
/// changes" be answered by comparing renders.
pub fn render(connections: &Connections, format: ConfigFormat) -> anyhow::Result<String> {
    match format {
        ConfigFormat::Json => {
            let mut json = serde_json::to_string_pretty(connections)
                .context("failed to serialize the connections")?;
            json.push('\n');
            Ok(json)
        }
        // serde_norway already ends its output in a newline
        ConfigFormat::Yaml => {
            serde_norway::to_string(connections).context("failed to serialize the connections")
        }
    }
}

/// Read and parse the connections file at `path`, in the format its name
/// implies.
///
/// **A missing file is not an error**, for the same reason a missing layout file
/// isn't: a server whose config names no connections has nothing to read, and
/// "no connections" is the honest answer. A path the operator named explicitly
/// is a different matter — see [`read_required`].
pub fn read(path: &Path) -> anyhow::Result<Connections> {
    match std::fs::read_to_string(path) {
        Ok(contents) => parse_named(&contents, path),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(Connections::new()),
        Err(err) => Err(anyhow::Error::new(err))
            .with_context(|| format!("failed to open connections file {}", path.display())),
    }
}

/// As [`read`], but a missing file is an error.
///
/// This is what `--connections <path>` gets: someone who named a file meant it,
/// and starting with no connections would fail later, further from the cause,
/// as every pipeline that names one refuses to build.
pub fn read_required(path: &Path) -> anyhow::Result<Connections> {
    let contents = std::fs::read_to_string(path)
        .with_context(|| format!("failed to open connections file {}", path.display()))?;
    parse_named(&contents, path)
}

fn parse_named(contents: &str, path: &Path) -> anyhow::Result<Connections> {
    let format = crate::persist::format_of(path);
    parse(contents, format).with_context(|| {
        format!(
            "failed to parse connections file {} as {format}",
            path.display()
        )
    })
}

/// Replace `path` with `connections`, staged through a sibling temporary and
/// renamed into place — same as a config save, and for the same reason.
pub fn write(path: &Path, connections: &Connections, format: ConfigFormat) -> anyhow::Result<()> {
    let contents = render(connections, format)?;
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
    use kayak_core::connections::{ConnectionKind, KafkaConnection, NatsConnection};

    fn sample() -> Connections {
        [
            (
                "prod-kafka".to_string(),
                ConnectionKind::Kafka(KafkaConnection {
                    brokers: "${KAFKA_BROKERS}".into(),
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

    /// The set travels together, and the connections file keeps the config's
    /// format rather than defaulting to JSON the way the layout does — this one
    /// is written by hand.
    #[test]
    fn the_connections_file_sits_beside_the_config_that_uses_it() {
        assert_eq!(
            connections_path(Path::new("/srv/kayak/config.json")),
            Path::new("/srv/kayak/config.connections.json")
        );
        assert_eq!(
            connections_path(Path::new("/srv/kayak/pipelines.yaml")),
            Path::new("/srv/kayak/pipelines.connections.yaml")
        );
    }

    /// The ordinary state of a config that names no connections. Failing here
    /// would mean a server couldn't start without a file it doesn't need.
    #[test]
    fn a_missing_file_reads_as_no_connections() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        assert!(read(&dir.path().join("config.connections.json"))?.is_empty());
        Ok(())
    }

    /// ...but a file the operator named explicitly has to be there. Silently
    /// starting without it would fail later, as every pipeline that names a
    /// connection refuses to build.
    #[test]
    fn a_missing_file_that_was_asked_for_by_name_is_an_error() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        assert!(read_required(&dir.path().join("nowhere.json")).is_err());
        Ok(())
    }

    #[test]
    fn a_broken_file_is_an_error_rather_than_no_connections() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("config.connections.json");
        std::fs::write(&path, "{ not json")?;
        let Err(err) = read(&path) else {
            panic!("a broken connections file was read as an empty one");
        };
        assert!(format!("{err:#}").contains("as json"), "{err:#}");
        Ok(())
    }

    #[test]
    fn connections_round_trip_through_a_file_in_either_format() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        for (name, format) in [
            ("config.connections.json", ConfigFormat::Json),
            ("config.connections.yaml", ConfigFormat::Yaml),
            ("config.connections.yml", ConfigFormat::Yaml),
        ] {
            let path = dir.path().join(name);
            write(&path, &sample(), format)?;
            assert_eq!(read(&path)?, sample(), "{name}");
            assert!(
                !crate::persist::temporary_path(&path).exists(),
                "{name}: the staging file was left behind"
            );
        }
        Ok(())
    }

    /// The file is committed, so the same connections have to render the same
    /// bytes — that is also what `has_unsaved_changes` compares.
    #[test]
    fn the_same_connections_render_the_same_bytes() -> anyhow::Result<()> {
        let mut reversed = Connections::new();
        for (id, connection) in sample().iter().collect::<Vec<_>>().into_iter().rev() {
            reversed.insert(id.clone(), connection.clone());
        }
        for format in [ConfigFormat::Json, ConfigFormat::Yaml] {
            assert_eq!(
                render(&sample(), format)?,
                render(&reversed, format)?,
                "{format}"
            );
        }
        Ok(())
    }

    /// A secret reference is written back exactly as it came in: the file holds
    /// the template, never the value, which is what makes it committable.
    #[test]
    fn a_secret_reference_survives_a_round_trip_unresolved() -> anyhow::Result<()> {
        let rendered = render(&sample(), ConfigFormat::Json)?;
        assert!(rendered.contains("${KAFKA_BROKERS}"), "{rendered}");
        Ok(())
    }
}
