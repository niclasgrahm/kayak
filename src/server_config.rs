//! Reading the server settings file.
//!
//! The types are in [`kayak_core::server_config`] — this is the file IO, and it
//! is the shortest of the four: the file is **read and never written**. The
//! config and the connections are edited from the UI and so have a save path;
//! this one describes the process rather than the work, nothing in the running
//! server may change who is allowed to reach it, and a settings file that could
//! be rewritten over HTTP by whoever is already logged in would be a way to
//! grant yourself a role. So there is no `write` here, and adding one is a
//! decision rather than an omission to tidy up.
//!
//! Format follows the same rule as everywhere else: JSON or YAML, decided by
//! the extension at this edge and nowhere past it.
//!
//! [`read_required`] is the only entry point, because there is no derived path
//! to fall back on — the file is named with `--server-config` or there isn't
//! one, and a server started without the flag runs
//! [`ServerConfig::default`](kayak_core::server_config::ServerConfig::default).

use std::path::Path;

use anyhow::Context;
use kayak_core::ConfigFormat;
use kayak_core::server_config::ServerConfig;

/// The settings in a file's contents.
pub fn parse(contents: &str, format: ConfigFormat) -> anyhow::Result<ServerConfig> {
    match format {
        ConfigFormat::Json => serde_json::from_str(contents).map_err(Into::into),
        ConfigFormat::Yaml => serde_norway::from_str(contents).map_err(Into::into),
    }
}

/// Read, parse and check the settings file at `path`.
///
/// A missing file is an error, unlike the connections file's — that one has a
/// derived path that may simply not exist, whereas this one is only ever read
/// because someone named it on the command line. Starting without it would run
/// an unauthenticated server for someone who asked for an authenticated one,
/// which is the one failure mode worth being loudest about.
pub fn read_required(path: &Path) -> anyhow::Result<ServerConfig> {
    let contents = std::fs::read_to_string(path)
        .with_context(|| format!("failed to open server config file {}", path.display()))?;
    let format = crate::persist::format_of(path);
    let config = parse(&contents, format).with_context(|| {
        format!(
            "failed to parse server config file {} as {format}",
            path.display()
        )
    })?;
    config
        .validate()
        .with_context(|| format!("{} is not a usable server config", path.display()))?;
    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;
    use kayak_core::server_config::{AuthConfig, Role};

    const YAML: &str = "
auth:
  type: basic
  users:
    niclas:
      password: ${KAYAK_NICLAS_PASSWORD}
      role: admin
    grafana:
      password: hunter2
";

    #[test]
    fn the_documented_yaml_shape_parses() -> anyhow::Result<()> {
        let config = parse(YAML, ConfigFormat::Yaml)?;
        assert!(config.requires_auth());
        let niclas = config.user("niclas").context("niclas is declared")?;
        assert_eq!(niclas.role, Role::Admin);
        assert_eq!(niclas.password.template(), "${KAYAK_NICLAS_PASSWORD}");
        // no role written, so the safe one
        assert_eq!(
            config.user("grafana").map(|u| u.role),
            Some(Role::Read),
            "an account with no role should read as read-only"
        );
        Ok(())
    }

    #[test]
    fn the_same_settings_parse_from_either_format() -> anyhow::Result<()> {
        let json = r#"{"auth": {"type": "basic", "users": {
            "niclas": {"password": "${KAYAK_NICLAS_PASSWORD}", "role": "admin"},
            "grafana": {"password": "hunter2"}
        }}}"#;
        assert_eq!(
            parse(json, ConfigFormat::Json)?,
            parse(YAML, ConfigFormat::Yaml)?
        );
        Ok(())
    }

    #[test]
    fn a_file_naming_no_auth_at_all_is_a_server_that_asks_nobody() -> anyhow::Result<()> {
        let config = parse("auth:\n  type: none\n", ConfigFormat::Yaml)?;
        assert_eq!(config.auth, AuthConfig::None);
        assert!(!config.requires_auth());
        Ok(())
    }

    /// An empty settings file is legal and means the defaults — the file exists
    /// to hold sections, and holding none is a fine thing for it to do.
    #[test]
    fn an_empty_file_reads_as_the_defaults() -> anyhow::Result<()> {
        assert_eq!(parse("{}", ConfigFormat::Yaml)?, ServerConfig::default());
        Ok(())
    }

    #[test]
    fn a_file_is_read_in_the_format_its_name_implies() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        for name in ["server.yaml", "server.yml"] {
            let path = dir.path().join(name);
            std::fs::write(&path, YAML)?;
            assert!(read_required(&path)?.requires_auth(), "{name}");
        }
        let path = dir.path().join("server.json");
        std::fs::write(&path, r#"{"auth": {"type": "none"}}"#)?;
        assert!(!read_required(&path)?.requires_auth());
        Ok(())
    }

    /// Someone who named a file meant it. Falling back to the defaults would
    /// silently run an open server for an operator who asked for a closed one.
    #[test]
    fn a_missing_file_is_an_error_rather_than_the_defaults() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let Err(err) = read_required(&dir.path().join("nowhere.yaml")) else {
            panic!("a missing server config was read as the defaults");
        };
        assert!(format!("{err:#}").contains("nowhere.yaml"), "{err:#}");
        Ok(())
    }

    /// The validation runs at startup, not at the first request.
    #[test]
    fn a_file_that_parses_but_locks_everyone_out_fails_to_load() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("server.yaml");
        std::fs::write(&path, "auth:\n  type: basic\n  users: {}\n")?;
        let Err(err) = read_required(&path) else {
            panic!("a server config with no users was accepted");
        };
        assert!(
            format!("{err:#}").contains("nobody could log in"),
            "{err:#}"
        );
        Ok(())
    }

    #[test]
    fn a_broken_file_names_itself_and_its_format() -> anyhow::Result<()> {
        let dir = tempfile::tempdir()?;
        let path = dir.path().join("server.json");
        std::fs::write(&path, "{ not json")?;
        let Err(err) = read_required(&path) else {
            panic!("a broken server config was accepted");
        };
        let message = format!("{err:#}");
        assert!(message.contains("server.json"), "{message}");
        assert!(message.contains("as json"), "{message}");
        Ok(())
    }
}
