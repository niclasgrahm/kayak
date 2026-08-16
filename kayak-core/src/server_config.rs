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
//! or, for a server embedded in a host application whose identity provider
//! mints the tokens:
//!
//! ```yaml
//! auth:
//!   type: jwt
//!   jwks_url: https://cognito-idp.eu-central-1.amazonaws.com/pool/.well-known/jwks.json
//!   issuer: https://cognito-idp.eu-central-1.amazonaws.com/pool
//!   username_claim: cognito:username
//!   roles:
//!     claim: cognito:groups
//!     admin: [Admin]
//!   service_accounts:
//!     provisioner:
//!       password: ${KAYAK_PROVISIONER_PASSWORD}
//!       role: admin
//! ```
//!
//! The issuer's coordinates are ordinary strings, not `${NAME}` references —
//! a pool id and a client id are addresses, not credentials, and belong in
//! the file. Only passwords resolve.
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
        !matches!(self.auth, AuthConfig::None)
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
        match &self.auth {
            AuthConfig::None => Ok(()),
            AuthConfig::Basic { users } => {
                if users.is_empty() {
                    return Err(ServerConfigError::NoUsers);
                }
                usernames_are_spellable(users)
            }
            AuthConfig::Jwt(jwt) => jwt.validate(),
        }
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
            AuthConfig::Jwt(jwt) => jwt.service_accounts.get(username),
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
    /// Tokens minted by an external identity provider — Cognito, Keycloak,
    /// anything that publishes a JWKS — validated against its published keys.
    /// This is the embedding scheme: a host application puts a token it
    /// already holds on the iframe URL, and kayak exchanges it for its own
    /// session cookie. See [`JwtConfig`] for the fields.
    Jwt(JwtConfig),
}

/// The `jwt` scheme's settings.
///
/// Everything here describes the *issuer's* side of the contract: where its
/// keys are published, what its tokens claim, and how those claims map onto
/// kayak's two roles. Nothing is kayak-issued — the server holds no signing
/// key and mints no tokens, it only checks what arrives and hands out its
/// ordinary session cookie in exchange.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
pub struct JwtConfig {
    /// Where the issuer publishes its signing keys — for Cognito,
    /// `https://cognito-idp.<region>.amazonaws.com/<pool>/.well-known/jwks.json`.
    /// Fetched once at startup (a server that can't reach it refuses to start,
    /// the same way a `${NAME}` that isn't set does) and re-fetched when a
    /// token names a key id the cache doesn't hold, which is how rotation is
    /// followed.
    pub jwks_url: String,
    /// The exact `iss` claim a token must carry. A token from any other
    /// issuer is refused however validly it is signed.
    pub issuer: String,
    /// The `aud` claim a token must carry, when set. Left out, the audience
    /// is not checked — which is what a Cognito *access* token needs, since
    /// those carry `client_id` rather than `aud`; an ID token wants this set
    /// to the app client id.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub audience: Option<String>,
    /// The claim the username is read from. `sub` by default; Cognito's
    /// human-readable one is `cognito:username`.
    #[serde(default = "default_username_claim")]
    pub username_claim: String,
    /// How claims decide what the caller may do. Left out, every valid token
    /// is a reader and only `service_accounts` can be admins — the safe
    /// reading of a section someone didn't write.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub roles: Option<RoleMapping>,
    /// Accounts checked as HTTP Basic beside the tokens, for callers that
    /// cannot do an identity-provider login — a provisioning script, CI.
    /// Same shape as the `basic` scheme's `users`, resolved the same way.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub service_accounts: BTreeMap<String, UserConfig>,
}

impl JwtConfig {
    fn validate(&self) -> Result<(), ServerConfigError> {
        if !(self.jwks_url.starts_with("https://") || self.jwks_url.starts_with("http://")) {
            return Err(ServerConfigError::JwksUrlNotHttp(self.jwks_url.clone()));
        }
        if self.issuer.trim().is_empty() {
            return Err(ServerConfigError::BlankJwtField("issuer"));
        }
        if self.username_claim.trim().is_empty() {
            return Err(ServerConfigError::BlankJwtField("username_claim"));
        }
        if let Some(roles) = &self.roles {
            if roles.claim.trim().is_empty() {
                return Err(ServerConfigError::BlankJwtField("roles.claim"));
            }
            if roles.admin.is_empty() {
                return Err(ServerConfigError::RoleMappingGrantsNothing);
            }
        }
        usernames_are_spellable(&self.service_accounts)
    }
}

/// The claim → role rule: one claim, and the values of it that mean admin.
///
/// Deliberately not an expression language — Grafana answers this with
/// JMESPath, and kayak consistently refuses that trade (`Condition` has no
/// `or`, `map` doesn't compute). A claim that is a string matches by equality,
/// one that is an array (Cognito's `cognito:groups`) matches if any element
/// is listed. Every valid token that doesn't match is a reader.
#[derive(Clone, Debug, Deserialize, Serialize, JsonSchema, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RoleMapping {
    /// The claim the role is read from, `cognito:groups` for Cognito.
    pub claim: String,
    /// The values of it that make the caller an admin.
    pub admin: Vec<String>,
}

fn default_username_claim() -> String {
    "sub".to_string()
}

/// The rule both account maps share: a name someone couldn't type into a
/// login box is a mistake, not an account.
fn usernames_are_spellable(
    users: &BTreeMap<String, UserConfig>,
) -> Result<(), ServerConfigError> {
    for name in users.keys() {
        if name.trim().is_empty() {
            return Err(ServerConfigError::BlankUsername);
        }
    }
    Ok(())
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
    /// A `jwks_url` that isn't an http(s) URL — a file path or a bare host
    /// can't be fetched, and finding that out at startup beats finding it out
    /// when the first token arrives.
    JwksUrlNotHttp(String),
    /// A required jwt field left blank.
    BlankJwtField(&'static str),
    /// A `roles` section whose `admin` list is empty can never grant a role;
    /// leaving the section out says the same thing on purpose.
    RoleMappingGrantsNothing,
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
            Self::JwksUrlNotHttp(url) => write!(
                f,
                "jwks_url '{url}' is not an http(s) URL, so the signing keys could never \
                 be fetched"
            ),
            Self::BlankJwtField(field) => write!(f, "jwt auth needs a non-blank {field}"),
            Self::RoleMappingGrantsNothing => write!(
                f,
                "the roles section has an empty admin list, so it can never grant a role; \
                 add a value or remove the section (every valid token then reads)"
            ),
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

    /// The documented jwt shape parses, and the fields someone leaves out
    /// land on the safe answers: username from `sub`, nobody an admin, no
    /// audience check, no service accounts.
    #[test]
    fn the_jwt_shape_parses_with_its_defaults() -> serde_json::Result<()> {
        let config: ServerConfig = serde_json::from_str(
            r#"{"auth": {"type": "jwt",
                "jwks_url": "https://issuer.example/.well-known/jwks.json",
                "issuer": "https://issuer.example"}}"#,
        )?;
        assert!(config.requires_auth());
        assert!(config.validate().is_ok());
        let AuthConfig::Jwt(jwt) = &config.auth else {
            panic!("the jwt variant should have been read");
        };
        assert_eq!(jwt.username_claim, "sub");
        assert_eq!(jwt.audience, None);
        assert_eq!(jwt.roles, None);
        assert!(jwt.service_accounts.is_empty());
        Ok(())
    }

    /// The full embedding shape — role mapping and a service account — and
    /// `user()` finds the service account exactly as it finds a basic user.
    #[test]
    fn a_jwt_service_account_is_an_account() -> serde_json::Result<()> {
        let config: ServerConfig = serde_json::from_str(
            r#"{"auth": {"type": "jwt",
                "jwks_url": "https://issuer.example/jwks.json",
                "issuer": "https://issuer.example",
                "audience": "client-id",
                "username_claim": "cognito:username",
                "roles": {"claim": "cognito:groups", "admin": ["Admin"]},
                "service_accounts": {
                    "provisioner": {"password": "${KAYAK_PROVISIONER}", "role": "admin"}
                }}}"#,
        )?;
        assert!(config.validate().is_ok());
        let account = config.user("provisioner").expect("the account is declared");
        assert_eq!(account.role, Role::Admin);
        assert_eq!(account.password.template(), "${KAYAK_PROVISIONER}");
        assert_eq!(config.user("nobody"), None);
        Ok(())
    }

    /// Round-trips without growing nulls or empty maps: what someone wrote is
    /// what is rendered back, the same promise the pipeline config makes.
    #[test]
    fn a_minimal_jwt_config_round_trips_minimally() -> serde_json::Result<()> {
        let config: ServerConfig = serde_json::from_str(
            r#"{"auth": {"type": "jwt", "jwks_url": "https://a/jwks", "issuer": "https://a"}}"#,
        )?;
        let rendered = serde_json::to_string(&config)?;
        assert!(!rendered.contains("audience"), "{rendered}");
        assert!(!rendered.contains("service_accounts"), "{rendered}");
        assert!(!rendered.contains("roles"), "{rendered}");
        let back: ServerConfig = serde_json::from_str(&rendered)?;
        assert_eq!(back, config);
        Ok(())
    }

    /// The contradictions the validator refuses: a JWKS that could never be
    /// fetched, blank required fields, a roles section that can never grant,
    /// and a service account nobody could type.
    #[test]
    fn meaningless_jwt_configs_are_refused() -> serde_json::Result<()> {
        let issuer_blank = r#""jwks_url": "https://a/jwks", "issuer": " ""#;
        let claim_blank =
            r#""jwks_url": "https://a/jwks", "issuer": "https://a", "username_claim": " ""#;
        let cases: &[(&str, ServerConfigError)] = &[
            (
                r#""jwks_url": "/etc/jwks.json", "issuer": "https://a""#,
                ServerConfigError::JwksUrlNotHttp("/etc/jwks.json".to_string()),
            ),
            (issuer_blank, ServerConfigError::BlankJwtField("issuer")),
            (claim_blank, ServerConfigError::BlankJwtField("username_claim")),
            (
                r#""jwks_url": "https://a/jwks", "issuer": "https://a",
                   "roles": {"claim": "groups", "admin": []}"#,
                ServerConfigError::RoleMappingGrantsNothing,
            ),
            (
                r#""jwks_url": "https://a/jwks", "issuer": "https://a",
                   "service_accounts": {" ": {"password": "x"}}"#,
                ServerConfigError::BlankUsername,
            ),
        ];
        for (body, expected) in cases {
            let json = format!(r#"{{"auth": {{"type": "jwt", {body}}}}}"#);
            let config: ServerConfig = serde_json::from_str(&json)?;
            assert_eq!(config.validate(), Err(expected.clone()), "{json}");
        }
        Ok(())
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
