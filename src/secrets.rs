//! Resolving `${NAME}` references in config into real secret values.
//!
//! Config files are version controlled, so they carry *references* to secrets
//! rather than the secrets themselves (see [`Secret`] in `streamer-core`).
//! Where those references resolve from is a deployment concern, not a config
//! one, which is why it lives behind the [`SecretStore`] trait: environment
//! variables suit a local `cargo leptos watch`, a mounted JSON file suits
//! Docker and Kubernetes, and neither choice reaches the config format.
//!
//! Resolution happens once, while a streamer is being built, and yields a
//! [`Resolved`] — which keeps the original template alongside the real value so
//! that logs and error messages can name a connection without printing its
//! password.

use anyhow::{Context, Result, bail};
use std::collections::HashMap;
use std::path::Path;
use streamer_core::config::Secret;

/// Where resolved secret values come from.
///
/// Implementations are consulted by name; the name is the text between `${` and
/// `}` in a [`Secret`]. Names are not themselves sensitive — they appear in
/// config files and in error messages — but values must never be logged.
pub trait SecretStore: Send + Sync {
    fn get(&self, name: &str) -> Option<String>;

    /// How this store is described when a lookup fails, e.g. "the environment".
    /// Only ever names the source, never its contents.
    fn describe(&self) -> String;
}

/// Reads secrets from the process environment.
pub struct EnvStore;

impl SecretStore for EnvStore {
    fn get(&self, name: &str) -> Option<String> {
        std::env::var(name).ok()
    }

    fn describe(&self) -> String {
        "the environment".to_string()
    }
}

/// Reads secrets from a flat JSON object of `"NAME": "value"` pairs, loaded
/// once at startup from a file the developer keeps out of version control.
pub struct FileStore {
    values: HashMap<String, String>,
    path: String,
}

impl FileStore {
    /// Nested objects and non-string values are rejected rather than coerced —
    /// a secrets file is small and hand-written, so a typo should be loud.
    pub fn from_path(path: &Path) -> Result<Self> {
        let contents = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read secrets file {}", path.display()))?;
        let parsed: HashMap<String, serde_json::Value> = serde_json::from_str(&contents)
            .with_context(|| {
                format!(
                    "failed to parse secrets file {} (expected a JSON object of name/string pairs)",
                    path.display()
                )
            })?;

        let mut values = HashMap::with_capacity(parsed.len());
        for (name, value) in parsed {
            let serde_json::Value::String(value) = value else {
                bail!(
                    "secret '{name}' in {} is not a string; secrets files hold name/string pairs only",
                    path.display()
                );
            };
            values.insert(name, value);
        }

        Ok(Self {
            values,
            path: path.display().to_string(),
        })
    }
}

impl SecretStore for FileStore {
    fn get(&self, name: &str) -> Option<String> {
        self.values.get(name).cloned()
    }

    fn describe(&self) -> String {
        format!("secrets file {}", self.path)
    }
}

/// Consults several stores in order and takes the first hit.
///
/// The server chains the environment ahead of the secrets file, so a single
/// secret can be overridden for one run without editing the file. That does
/// mean an unrelated environment variable with a colliding name shadows the
/// file, which is why a shadowed lookup is logged.
pub struct ChainStore {
    stores: Vec<Box<dyn SecretStore>>,
}

impl ChainStore {
    #[must_use]
    pub fn new(stores: Vec<Box<dyn SecretStore>>) -> Self {
        Self { stores }
    }
}

impl SecretStore for ChainStore {
    fn get(&self, name: &str) -> Option<String> {
        let mut found: Option<(usize, String)> = None;
        for (i, store) in self.stores.iter().enumerate() {
            match (&found, store.get(name)) {
                (None, Some(value)) => found = Some((i, value)),
                (Some((winner, _)), Some(_)) => tracing::debug!(
                    "secret '{}' found in {} as well; using the one from {}",
                    name,
                    store.describe(),
                    self.stores[*winner].describe()
                ),
                (_, None) => {}
            }
        }
        found.map(|(_, value)| value)
    }

    fn describe(&self) -> String {
        let sources: Vec<String> = self.stores.iter().map(|s| s.describe()).collect();
        sources.join(" or ")
    }
}

/// A [`Secret`] with its `${NAME}` references filled in.
///
/// The real value is only reachable through [`Resolved::expose`]. `Display` and
/// `Debug` print the *template* instead, so the usual `format!("connecting to
/// {url}")` prints `nats://app:${NATS_PASSWORD}@broker:4222` — enough to debug a
/// connection failure, with nothing worth leaking. (A password written inline in
/// the config rather than referenced does still print; avoiding that is the
/// whole point of the reference syntax.)
#[derive(Clone)]
pub struct Resolved {
    template: String,
    value: String,
}

impl Resolved {
    /// The real value, for handing to a client library. Every call site is a
    /// place a secret could escape, so keep them few and obvious.
    #[must_use]
    pub fn expose(&self) -> &str {
        &self.value
    }
}

impl std::fmt::Display for Resolved {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.template)
    }
}

impl std::fmt::Debug for Resolved {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.template)
    }
}

/// Characters allowed in a `${NAME}` reference. Environment variables want
/// letters, digits and `_`; file-backed stores tend to want dots and dashes too.
fn is_name_char(c: char) -> bool {
    c.is_ascii_alphanumeric() || matches!(c, '_' | '.' | '-')
}

/// Replace every `${NAME}` in `secret` with its value from `store`.
///
/// An unknown name is an error rather than an empty string: a pipeline that
/// silently connects without a password is worse than one that refuses to
/// start. There is no escape for a literal `${` — a value that needs one has to
/// come from the store rather than from the config file.
pub fn resolve(secret: &Secret, store: &dyn SecretStore) -> Result<Resolved> {
    let template = secret.template();
    let mut value = String::with_capacity(template.len());
    let mut rest = template;

    while let Some(start) = rest.find("${") {
        value.push_str(&rest[..start]);
        let after = &rest[start + 2..];
        let Some(end) = after.find('}') else {
            bail!("unterminated '${{' in secret reference '{template}'");
        };
        let name = &after[..end];
        if name.is_empty() || !name.chars().all(is_name_char) {
            bail!(
                "invalid secret reference '${{{name}}}' in '{template}'; \
                 names may contain letters, digits, '_', '.' and '-'"
            );
        }
        let Some(secret_value) = store.get(name) else {
            bail!("secret '{name}' is not set in {}", store.describe());
        };
        value.push_str(&secret_value);
        rest = &after[end + 1..];
    }
    value.push_str(rest);

    Ok(Resolved {
        template: template.to_string(),
        value,
    })
}
