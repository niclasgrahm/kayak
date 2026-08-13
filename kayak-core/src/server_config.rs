//! How the *server* is run, as against what the graph is.
//!
//! Every file kayak takes so far describes the work: the config is the
//! pipelines, the connections are the systems they talk to, the layout is how
//! the cards are arranged. This one describes the process serving them — who is
//! allowed to reach it, and later whatever else belongs to a deployment rather
//! than to a graph. It is named with `--server-config` and, unlike the
//! connections file, is **not derived from anything**: it belongs to the
//! deployment, not to the config file, and two configs served by one process
//! share it by construction.
//!
//! **The whole file is optional and its absence is a working server.** That is
//! the load-bearing property here: a `just dev` on a laptop should not need a
//! settings file to exist, so [`ServerConfig::default`] is what a server with no
//! `--server-config` runs, and it authenticates nobody. Turning a security
//! control off by default is a real cost, paid deliberately — the alternative
//! is a first run that can't reach its own UI, and an upgrade that locks every
//! existing deployment out of its own server. What takes the edge off it is that
//! `main` logs a warning when an unauthenticated server binds anything but
//! loopback.
//!
//! # The shape
//!
//! ```yaml
//! auth:
//!   type: basic
//!   users:
//!     niclas:
//!       password: ${KAYAK_NICLAS_PASSWORD}
//!       role: admin
//!     grafana:
//!       password: ${KAYAK_DASHBOARD_PASSWORD}
//!       role: read
//! ```
//!
//! [`AuthConfig`] is a **tagged enum rather than a bool beside a map**, and that
//! is the point of it. `auth: false` sitting above a populated `users:` — an
//! operator believing they are protected and not being — is not expressible
//! here, because there is nowhere to write it. The two states are the two
//! variants.
//!
//! Passwords are [`Secret`]s, so the file holds `${NAME}` references and stays
//! committable, exactly as a connection's password does. They are resolved once
//! at startup against the same store `--secrets` and the environment feed.

use std::collections::BTreeMap;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::config::Secret;
use crate::history::{DEFAULT_RETENTION_SECS, MAX_RETENTION_SECS};

/// Everything the server is told about itself, as opposed to about its
/// pipelines.
///
/// One field so far. New sections go beside `auth` and want the same property
/// it has: a default that is what the server does today, so that adding a
/// section doesn't change an existing deployment.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct ServerConfig {
    /// Who is allowed to reach the server. Absent means nobody is asked.
    pub auth: AuthConfig,
    /// How much of what the pipelines did is kept for the UI to show later.
    pub history: HistoryConfig,
}

impl ServerConfig {
    /// Whether requests are authenticated at all.
    #[must_use]
    pub fn requires_auth(&self) -> bool {
        matches!(self.auth, AuthConfig::Basic { .. })
    }

    /// The contradictions that are spellable but meaningless, refused at
    /// startup rather than at the first request.
    ///
    /// Both arms are servers nobody can use: one where every login fails
    /// because there are no accounts, and one where an account can't be typed
    /// into a login box. Failing here costs a restart; failing at the first
    /// request costs whoever is locked out working out why.
    pub fn validate(&self) -> Result<(), ServerConfigError> {
        self.history.validate()?;
        let AuthConfig::Basic { users } = &self.auth else {
            return Ok(());
        };
        if users.is_empty() {
            return Err(ServerConfigError::NoUsers);
        }
        for name in users.keys() {
            if name.trim().is_empty() {
                return Err(ServerConfigError::BlankUsername);
            }
        }
        Ok(())
    }

    /// The user of that name, if authentication is on at all.
    ///
    /// `None` from an [`AuthConfig::None`] server is the honest answer rather
    /// than an oversight: with no accounts configured there is no such user,
    /// and the caller is the middleware, which never asks in that case.
    #[must_use]
    pub fn user(&self, username: &str) -> Option<&UserConfig> {
        match &self.auth {
            AuthConfig::None => None,
            AuthConfig::Basic { users } => users.get(username),
        }
    }
}

/// How the server decides who is asking.
///
/// Internally tagged, so `type` selects the variant the way it does throughout
/// the component config. One scheme so far; an `oidc` variant is the shape the
/// next one takes, and the reason this is an enum rather than a struct of
/// optional fields.
#[derive(Clone, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum AuthConfig {
    /// Anyone who can reach the server can do anything to it. The default, and
    /// what every deployment before this feature was.
    #[default]
    None,
    /// A fixed set of accounts declared in this file. Requests carry either
    /// HTTP Basic credentials or a session cookie issued by
    /// `POST /api/auth/login`; both resolve to one of these users.
    Basic {
        /// The accounts, by username. A `BTreeMap` so the file round-trips in
        /// name order, the same reason `Connections` is one.
        users: BTreeMap<String, UserConfig>,
    },
}

/// One account.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct UserConfig {
    /// The password, as a `${NAME}` reference to whatever the server's secret
    /// store holds. A literal works and is what a throwaway deployment will
    /// write, but it puts a real credential in a file that gets committed —
    /// which is the thing every other password field in kayak avoids.
    pub password: Secret,
    /// What this account may do. Omitted means [`Role::Read`], because the
    /// field someone forgets to write should be the harmless one.
    #[serde(default)]
    pub role: Role,
}

/// What an account is allowed to do.
///
/// Two, and deliberately only two: the split that matters first is "can change
/// what the server is running" against "can watch it". Anything finer — per
/// pipeline, per connection — needs a model of *which* resources, which is a
/// much larger feature than a second role.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    /// May do anything: create and delete pipelines and connections, save and
    /// revert the config file, rearrange the canvas.
    Admin,
    /// May see everything and change nothing. The default for an account whose
    /// `role` is left out.
    #[default]
    Read,
}

impl Role {
    /// How the role is spelled in the file, for error messages and the UI.
    #[must_use]
    pub fn label(self) -> &'static str {
        match self {
            Self::Admin => "admin",
            Self::Read => "read",
        }
    }
}

/// A settings file that parses but says something meaningless.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ServerConfigError {
    NoUsers,
    BlankUsername,
    /// A retention past [`MAX_RETENTION_SECS`]. Refused rather than clamped:
    /// the store is in memory, so this is an allocation, and silently keeping
    /// a tenth of what a config asked for is worse than saying no.
    RetentionTooLong(u64),
}

impl std::fmt::Display for ServerConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NoUsers => write!(
                f,
                "auth is 'basic' but no users are declared, so nobody could log in; \
                 add a user or set auth type to 'none'"
            ),
            Self::BlankUsername => write!(f, "a username is blank"),
            Self::RetentionTooLong(secs) => write!(
                f,
                "history.retention_secs is {secs}, which is longer than the {MAX_RETENTION_SECS} \
                 second maximum; history is kept in memory, so this is an allocation rather \
                 than a disk budget"
            ),
        }
    }
}

impl std::error::Error for ServerConfigError {}

#[cfg(test)]
mod tests {
    use super::*;

    /// The property the whole file rests on: no file at all is a server that
    /// runs, and runs the way it did before this existed.
    #[test]
    fn the_default_server_asks_nobody_for_anything() {
        let config = ServerConfig::default();
        assert_eq!(config.auth, AuthConfig::None);
        assert!(!config.requires_auth());
        assert!(config.validate().is_ok());
    }

    /// An account with no `role` is a reader. The field someone forgets is the
    /// one that can't hand out delete buttons.
    #[test]
    fn an_account_without_a_role_is_read_only() {
        let config: ServerConfig = serde_json::from_str(
            r#"{"auth": {"type": "basic", "users": {"sam": {"password": "hunter2"}}}}"#,
        )
        .expect("the sample parses");
        let user = config.user("sam").expect("sam is declared");
        assert_eq!(user.role, Role::Read);
    }

    #[test]
    fn a_role_is_read_from_the_file_when_it_is_written() {
        let config: ServerConfig = serde_json::from_str(
            r#"{"auth": {"type": "basic", "users": {
                "sam": {"password": "a", "role": "admin"},
                "kim": {"password": "b", "role": "read"}
            }}}"#,
        )
        .expect("the sample parses");
        assert_eq!(config.user("sam").map(|u| u.role), Some(Role::Admin));
        assert_eq!(config.user("kim").map(|u| u.role), Some(Role::Read));
        assert_eq!(config.user("nobody"), None);
    }

    /// The password is a `Secret`, so the file holds the reference and never
    /// the value — the same promise a connection's password makes.
    #[test]
    fn a_password_is_a_secret_reference_and_survives_a_round_trip() {
        let config: ServerConfig = serde_json::from_str(
            r#"{"auth": {"type": "basic", "users": {"sam": {"password": "${KAYAK_SAM}"}}}}"#,
        )
        .expect("the sample parses");
        assert_eq!(
            config.user("sam").map(|u| u.password.template()),
            Some("${KAYAK_SAM}")
        );
        let rendered = serde_json::to_string(&config).expect("it serializes");
        assert!(rendered.contains("${KAYAK_SAM}"), "{rendered}");
    }

    /// A server nobody can log into is a mistake, not a configuration.
    #[test]
    fn basic_auth_with_no_users_is_refused() {
        let config: ServerConfig =
            serde_json::from_str(r#"{"auth": {"type": "basic", "users": {}}}"#)
                .expect("the sample parses");
        assert_eq!(config.validate(), Err(ServerConfigError::NoUsers));
    }

    #[test]
    fn a_blank_username_is_refused() {
        let config: ServerConfig = serde_json::from_str(
            r#"{"auth": {"type": "basic", "users": {"  ": {"password": "a"}}}}"#,
        )
        .expect("the sample parses");
        assert_eq!(config.validate(), Err(ServerConfigError::BlankUsername));
    }

    /// `auth: true` beside a users map is the misconfiguration this shape
    /// exists to make unspellable — there is no bool to get out of step with
    /// the accounts. A file that tries fails to parse rather than picking one.
    #[test]
    fn there_is_no_boolean_spelling_of_auth() {
        assert!(serde_json::from_str::<ServerConfig>(r#"{"auth": true}"#).is_err());
    }

    /// A typo in a field name is a security bug when the field is `role`:
    /// silently defaulting `rol: admin` to read would be merely annoying, but
    /// silently defaulting a mistyped `users` key to "no users" would not.
    #[test]
    fn an_unknown_field_is_refused_rather_than_ignored() {
        assert!(
            serde_json::from_str::<ServerConfig>(
                r#"{"auth": {"type": "basic", "users": {"sam": {"password": "a", "rol": "admin"}}}}"#
            )
            .is_err()
        );
        assert!(serde_json::from_str::<ServerConfig>(r#"{"users": {}}"#).is_err());
    }
}

/// How much of what the pipelines did the server keeps for the UI to show
/// later — the knob on [`crate::history`].
///
/// **One duration, and the buffer sizes are derived from it.** Retention is
/// what an operator actually has an opinion about; "how many buckets" is an
/// implementation detail they would have to multiply out to reason about, and
/// exposing it would let the two rings be configured into disagreement. The
/// fine ring is not configurable at all — it is sized by what a card can
/// display, which is not a deployment's business.
///
/// This is the one section whose default is *not* what the server did before it
/// existed, and the deviation is deliberate. Off by default would mean the
/// feature is missing for everyone who doesn't know to look for it, which is
/// precisely the person it is for — someone finding out at 08:00 that something
/// broke at 02:14. What makes that affordable is the bound: a pipeline costs
/// one `HistoryBucket` per minute of retention plus half an hour of fine ones,
/// which is about 58 kB a day, flat in throughput.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct HistoryConfig {
    /// How far back the coarse ring reaches, in seconds. A day by default,
    /// capped at [`MAX_RETENTION_SECS`].
    ///
    /// **Zero turns history off** — the rings are never allocated and the
    /// counters are never sampled. That is the off switch rather than an
    /// `enabled` flag beside a duration, for the reason [`AuthConfig`] is an
    /// enum rather than a bool beside a map: `enabled: false` above
    /// `retention_secs: 86400` is a contradiction someone would write and then
    /// misread, and here there is nowhere to write it.
    pub retention_secs: u64,
}

impl Default for HistoryConfig {
    fn default() -> Self {
        Self {
            retention_secs: DEFAULT_RETENTION_SECS,
        }
    }
}

impl HistoryConfig {
    /// Whether anything is kept at all.
    #[must_use]
    pub fn enabled(&self) -> bool {
        self.retention_secs > 0
    }

    /// How many coarse buckets the ring holds — the retention divided by the
    /// bucket width, which is the derivation this type exists to keep in one
    /// place.
    #[must_use]
    pub fn coarse_capacity(&self) -> usize {
        let width = crate::history::COARSE_BUCKET_SECS.max(1);
        usize::try_from(self.retention_secs.div_ceil(width)).unwrap_or(usize::MAX)
    }

    /// How many fine buckets the ring holds. Fixed — see the type's docs.
    ///
    /// Never more than the retention asks for: a server told to keep five
    /// minutes should not hold half an hour of fine buckets just because that
    /// ring's window is a constant.
    #[must_use]
    pub fn fine_capacity(&self) -> usize {
        let width = crate::history::FINE_BUCKET_SECS.max(1);
        let window = crate::history::FINE_WINDOW_SECS.min(self.retention_secs);
        usize::try_from(window.div_ceil(width)).unwrap_or(usize::MAX)
    }

    fn validate(&self) -> Result<(), ServerConfigError> {
        if self.retention_secs > MAX_RETENTION_SECS {
            return Err(ServerConfigError::RetentionTooLong(self.retention_secs));
        }
        Ok(())
    }
}
