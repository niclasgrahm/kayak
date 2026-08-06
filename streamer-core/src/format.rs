//! How a config file is spelled on disk.
//!
//! JSON and YAML describe exactly the same pipelines — [`Config`] doesn't know
//! which one it came from, and nothing downstream of parsing does either. The
//! format is a property of the *file*, so the rules for reading it off a file
//! name live here, shared by the server (which infers the format of `--config`,
//! and of a save target) and the frontend (whose save dialog offers the choice).
//!
//! [`Config`]: crate::config::Config

use serde::{Deserialize, Serialize};

/// The two ways a config file can be written.
///
/// JSON is the default because it is what every existing file and every example
/// in the repository uses; a file only gets read as YAML if it says so.
#[derive(Serialize, Deserialize, Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum ConfigFormat {
    #[default]
    Json,
    Yaml,
}

impl ConfigFormat {
    /// The format a file name implies: `.yaml` or `.yml` is YAML, anything else
    /// is JSON.
    ///
    /// The extension is the only signal, and it is deliberately the *whole*
    /// rule rather than a fallback behind sniffing the contents. Someone who
    /// names a file `pipelines.yaml` should get a YAML parse error out of a file
    /// that isn't YAML, not a silent second guess.
    #[must_use]
    pub fn of_file_name(name: &str) -> Self {
        let name = name.trim();
        let extension = match name.rsplit_once('.') {
            // a leading dot is the whole name of a hidden file, not an
            // extension — the same reading `Path::extension` takes
            Some((stem, ext)) if !stem.is_empty() => Some(ext.to_ascii_lowercase()),
            _ => None,
        };
        match extension.as_deref() {
            Some("yaml" | "yml") => Self::Yaml,
            _ => Self::Json,
        }
    }

    /// The extension a file in this format is named with, without the dot.
    #[must_use]
    pub fn extension(self) -> &'static str {
        match self {
            Self::Json => "json",
            Self::Yaml => "yaml",
        }
    }

    /// The name a user typed, renamed to be a file of this format.
    ///
    /// Used by the save dialog, where picking a format has to move the name
    /// with it — offering "yaml" and then writing `config.json` would put the
    /// two halves of one decision at odds. A name that already ends in an
    /// extension for the chosen format is left exactly as it is, so `.yml`
    /// doesn't get rewritten to `.yaml` under someone's cursor.
    #[must_use]
    pub fn rename(self, name: &str) -> String {
        let name = name.trim();
        if Self::of_file_name(name) == self && name.contains('.') {
            return name.to_string();
        }
        // a leading dot is the whole name of a hidden file, not an extension
        match name.rsplit_once('.') {
            Some((stem, _)) if !stem.is_empty() => format!("{stem}.{}", self.extension()),
            _ => format!("{name}.{}", self.extension()),
        }
    }
}

impl std::fmt::Display for ConfigFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Json => "json",
            Self::Yaml => "yaml",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_yaml_extension_names_a_yaml_file() {
        assert_eq!(ConfigFormat::of_file_name("config.yaml"), ConfigFormat::Yaml);
        assert_eq!(ConfigFormat::of_file_name("config.yml"), ConfigFormat::Yaml);
        assert_eq!(ConfigFormat::of_file_name("CONFIG.YAML"), ConfigFormat::Yaml);
        assert_eq!(
            ConfigFormat::of_file_name("  spaced.yaml  "),
            ConfigFormat::Yaml
        );
    }

    /// Everything else is JSON, including a name with no extension at all —
    /// that's what keeps every file that predates YAML support loading. A bare
    /// `.yaml` is a hidden file's whole name rather than an extension, which is
    /// how `Path::extension` reads it too.
    #[test]
    fn anything_else_names_a_json_file() {
        for name in ["config.json", "config", "config.txt", "yaml", ".yaml"] {
            assert_eq!(
                ConfigFormat::of_file_name(name),
                ConfigFormat::Json,
                "{name}"
            );
        }
    }

    #[test]
    fn renaming_swaps_the_extension() {
        assert_eq!(ConfigFormat::Yaml.rename("config.json"), "config.yaml");
        assert_eq!(ConfigFormat::Json.rename("config.yaml"), "config.json");
        assert_eq!(ConfigFormat::Yaml.rename("config"), "config.yaml");
        assert_eq!(ConfigFormat::Json.rename("  config  "), "config.json");
    }

    /// `.yml` is already YAML, so switching to YAML must not retype it: the
    /// name is the user's, and the selector only exists to keep it honest.
    #[test]
    fn renaming_leaves_a_name_that_already_matches_alone() {
        assert_eq!(ConfigFormat::Yaml.rename("config.yml"), "config.yml");
        assert_eq!(ConfigFormat::Json.rename("config.json"), "config.json");
        assert_eq!(
            ConfigFormat::Yaml.rename("pipelines.staging.yaml"),
            "pipelines.staging.yaml"
        );
    }

    /// A dotted stem is a name, not an extension boundary to be preserved:
    /// only the last segment is the extension.
    #[test]
    fn renaming_only_replaces_the_last_segment() {
        assert_eq!(
            ConfigFormat::Yaml.rename("pipelines.staging.json"),
            "pipelines.staging.yaml"
        );
    }

    #[test]
    fn it_round_trips_through_serde_as_a_lowercase_string() -> Result<(), serde_json::Error> {
        assert_eq!(serde_json::to_string(&ConfigFormat::Yaml)?, "\"yaml\"");
        assert_eq!(
            serde_json::from_str::<ConfigFormat>("\"json\"")?,
            ConfigFormat::Json
        );
        Ok(())
    }
}
