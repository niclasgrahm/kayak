//! Who is asking, and whether they may.
//!
//! The declaration is [`kayak_core::server_config`]; this is the live half —
//! the accounts with their passwords resolved, the sessions handed out to
//! browsers, and the middleware the router wraps each endpoint in.
//!
//! # Two schemes, one identity
//!
//! A request arrives with either HTTP Basic credentials or a session cookie,
//! and both land on the same [`Identity`]. That is deliberate: the
//! authorization check is then one check, and a role means the same thing
//! however you got in.
//!
//! The cookie is not a convenience. `EventSource` — which is what the whole
//! canvas is fed by, through `GET /events` — cannot set request headers at all,
//! so a browser has no way to present Basic credentials on the one endpoint it
//! needs most. The alternatives are a token in the query string, which ends up
//! in every access log the request passes through, or a cookie. Basic stays for
//! everything that isn't a browser, where it is the least ceremony possible.
//!
//! # Sessions are in memory and die with the process
//!
//! A `HashMap` here rather than a signed, stateless cookie, and the tradeoff is
//! worth knowing: a stateless cookie survives a restart and cannot be revoked,
//! a stored one is revoked the moment [`Auth::log_out`] runs and does not
//! survive. Logout that genuinely logs you out is worth more than sessions that
//! outlive a deploy, and it means there is no signing key to invent, store or
//! rotate. A restart logging everyone out of a dev tool is a shrug.
//!
//! # What is *not* here
//!
//! Password hashing. Passwords are [`Secret`](kayak_core::config::Secret)s
//! resolved from the secret store, so the settings file holds `${NAME}` and the
//! value comes from the environment or the secrets file — the same shape every
//! other credential in kayak has. Hashes would let the file be safe *without* a
//! secret store, and are worth doing, but they need a `kayak hash-password`
//! helper to be usable at all. The readme's TODO carries it.
//!
//! Rate limiting. Nothing here slows a caller down after a wrong password, so
//! basic credentials on a public network are only as good as the password. The
//! readme says so.

use std::collections::HashMap;
use std::sync::Mutex;

use anyhow::Context;
use axum::extract::Request;
use axum::http::{StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use base64::Engine;
use kayak_core::api_docs::Access;
use kayak_core::server_config::{AuthConfig, Role, ServerConfig};
use rand::RngExt;
use subtle::ConstantTimeEq;

use crate::secrets::{Resolved, SecretStore};

/// The name of the session cookie. One name, used by the middleware that reads
/// it and the two handlers that set and clear it.
pub const SESSION_COOKIE: &str = "kayak_session";

/// Who a request is from.
///
/// Put into the request extensions by [`authorize`] once the credentials check
/// out, so a handler that wants to know who is calling can ask without
/// re-parsing anything. Absent on a server with authentication turned off,
/// which is the honest answer there: nobody was identified.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Identity {
    pub username: String,
    pub role: Role,
}

/// One account, with its password resolved.
struct Account {
    password: Resolved,
    role: Role,
}

/// A handed-out session.
struct Session {
    username: String,
    role: Role,
}

/// The live authentication state: the accounts, and who is currently logged in.
pub struct Auth {
    /// `None` when the server authenticates nobody, which is both the default
    /// and an explicit `auth: {type: none}`. An `Option` rather than an empty
    /// map because those are different servers — an empty map would be one
    /// where every login fails, which `ServerConfig::validate` refuses.
    accounts: Option<HashMap<String, Account>>,
    sessions: Mutex<HashMap<String, Session>>,
}

impl Default for Auth {
    fn default() -> Self {
        Self::disabled()
    }
}

impl Auth {
    /// A server that asks nobody for anything — the default, and what every
    /// deployment that predates this ran.
    #[must_use]
    pub fn disabled() -> Self {
        Self {
            accounts: None,
            sessions: Mutex::new(HashMap::new()),
        }
    }

    /// Resolve the accounts in a settings file against the secret store.
    ///
    /// Once, at startup, for the same reason a connection's credentials are
    /// resolved when a pipeline is built: a `${NAME}` that isn't set should
    /// stop the server rather than turn into a login that mysteriously never
    /// succeeds.
    pub fn from_config(config: &ServerConfig, secrets: &dyn SecretStore) -> anyhow::Result<Self> {
        config.validate()?;
        let AuthConfig::Basic { users } = &config.auth else {
            return Ok(Self::disabled());
        };
        let mut accounts = HashMap::with_capacity(users.len());
        for (username, user) in users {
            let password = crate::secrets::resolve(&user.password, secrets)
                .with_context(|| format!("failed to resolve the password for user '{username}'"))?;
            accounts.insert(
                username.clone(),
                Account {
                    password,
                    role: user.role,
                },
            );
        }
        tracing::info!(
            "authentication is on, with {} account(s): {}",
            accounts.len(),
            users
                .iter()
                .map(|(name, user)| format!("{name} ({})", user.role.label()))
                .collect::<Vec<_>>()
                .join(", ")
        );
        Ok(Self {
            accounts: Some(accounts),
            sessions: Mutex::new(HashMap::new()),
        })
    }

    /// Whether anything is checked at all.
    #[must_use]
    pub fn is_enabled(&self) -> bool {
        self.accounts.is_some()
    }

    /// Check a username and password.
    ///
    /// Two properties here are the whole point of the function. The comparison
    /// is constant-time, so the response time doesn't leak how much of the
    /// password was right. And an **unknown username still costs a
    /// comparison** — against a fixed dummy of the same shape — so a wrong name
    /// and a wrong password take the same time, and the endpoint can't be used
    /// to enumerate who has an account.
    #[must_use]
    pub fn authenticate(&self, username: &str, password: &str) -> Option<Identity> {
        let accounts = self.accounts.as_ref()?;
        let account = accounts.get(username);
        // `expose` is one of the few places a real secret value is reached; it
        // goes straight into the comparison and is not held, logged or copied.
        let expected = account.map_or(DUMMY_PASSWORD, |account| account.password.expose());
        let matched: bool = expected.as_bytes().ct_eq(password.as_bytes()).into();
        match account {
            Some(account) if matched => Some(Identity {
                username: username.to_string(),
                role: account.role,
            }),
            _ => None,
        }
    }

    /// Start a session for an identity that has already been checked, and give
    /// back the token that names it.
    pub fn log_in(&self, identity: &Identity) -> anyhow::Result<String> {
        let token = session_token();
        let mut sessions = self
            .sessions
            .lock()
            .map_err(|_| anyhow::anyhow!("the session store is poisoned"))?;
        sessions.insert(
            token.clone(),
            Session {
                username: identity.username.clone(),
                role: identity.role,
            },
        );
        Ok(token)
    }

    /// Forget a session. Idempotent: logging out twice, or with a token that
    /// was never valid, is not an error — there is nothing a caller could do
    /// differently, and saying "that token wasn't real" answers a question
    /// nobody should be able to ask.
    pub fn log_out(&self, token: &str) {
        if let Ok(mut sessions) = self.sessions.lock() {
            sessions.remove(token);
        }
    }

    /// Who a session token belongs to, if it is still live.
    #[must_use]
    fn session(&self, token: &str) -> Option<Identity> {
        let sessions = self.sessions.lock().ok()?;
        sessions.get(token).map(|session| Identity {
            username: session.username.clone(),
            role: session.role,
        })
    }

    /// Who this request is from, by whichever scheme it used.
    ///
    /// The cookie is tried first because it is the cheaper check and the one a
    /// browser will be using on nearly every request; a request carrying both
    /// is not a case worth having an opinion about.
    #[must_use]
    pub fn identify(&self, request: &Request) -> Option<Identity> {
        if !self.is_enabled() {
            return None;
        }
        if let Some(token) = cookie(request, SESSION_COOKIE)
            && let Some(identity) = self.session(&token)
        {
            return Some(identity);
        }
        let (username, password) = basic_credentials(request)?;
        self.authenticate(&username, &password)
    }
}

/// Compared against when the username is unknown, so that the wrong-name and
/// wrong-password paths do the same work. Its contents don't matter; that it is
/// never a real password does.
const DUMMY_PASSWORD: &str = "$kayak$no-such-account$";

/// 256 bits of randomness, hex encoded. Long enough that guessing is not a
/// consideration and short enough to be an ordinary cookie value.
fn session_token() -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut rng = rand::rng();
    let bytes: [u8; 32] = rng.random();
    let mut token = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        token.push(char::from(HEX[(byte >> 4) as usize]));
        token.push(char::from(HEX[(byte & 0x0f) as usize]));
    }
    token
}

/// The username and password out of an `Authorization: Basic` header.
///
/// Anything malformed reads as "no credentials" rather than as an error: the
/// outcome is a 401 either way, and a header that failed to decode tells the
/// caller nothing more useful than that they are not signed in.
fn basic_credentials(request: &Request) -> Option<(String, String)> {
    let header = request
        .headers()
        .get(header::AUTHORIZATION)?
        .to_str()
        .ok()?;
    let encoded = header
        .strip_prefix("Basic ")
        .or_else(|| header.strip_prefix("basic "))?;
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(encoded.trim())
        .ok()?;
    let decoded = String::from_utf8(decoded).ok()?;
    // a password may contain a colon; a username may not, so split once
    let (username, password) = decoded.split_once(':')?;
    Some((username.to_string(), password.to_string()))
}

/// One cookie's value out of the `Cookie` header.
///
/// Parsed here rather than with a cookie crate because one name is read and one
/// is written, and neither needs attributes, signing or a jar.
fn cookie(request: &Request, name: &str) -> Option<String> {
    let header = request.headers().get(header::COOKIE)?.to_str().ok()?;
    header.split(';').find_map(|pair| {
        let (key, value) = pair.split_once('=')?;
        (key.trim() == name).then(|| value.trim().to_string())
    })
}

/// The middleware every route is wrapped in, at the access its table entry
/// declares.
///
/// Applied with `route_layer` in [`crate::endpoints`], so it runs only for
/// requests that matched a route — an unknown path is the router's own 404 and
/// never a 401, which is what keeps "this endpoint does not exist" and "you may
/// not have it" from being the same answer.
pub async fn authorize(access: Access, auth: &Auth, mut request: Request, next: Next) -> Response {
    if !auth.is_enabled() {
        return next.run(request).await;
    }
    let identity = auth.identify(&request);
    if !access.permits(identity.as_ref().map(|i| i.role)) {
        return refuse(access, identity.as_ref());
    }
    if let Some(identity) = identity {
        request.extensions_mut().insert(identity);
    }
    next.run(request).await
}

/// The 401 or the 403.
///
/// **No `WWW-Authenticate` header on the 401**, and that is a decision rather
/// than an oversight. Sending one makes a browser throw its own credential
/// dialog over the top of the app, which is exactly what the login page is
/// for — and there is no way to send it to `curl` and not to the browser, since
/// both are just requests. Nothing is lost: `curl -u` sends its credentials
/// preemptively, so it never needs the challenge to know to.
fn refuse(access: Access, identity: Option<&Identity>) -> Response {
    let (status, message) = match identity {
        // signed in, but not enough — saying which role is wanted is the
        // difference between a dead end and a thing to go and ask for
        Some(identity) => (
            StatusCode::FORBIDDEN,
            format!(
                "user '{}' has the '{}' role; this endpoint needs '{}'",
                identity.username,
                identity.role.label(),
                access.label()
            ),
        ),
        None => (
            StatusCode::UNAUTHORIZED,
            "authentication required: sign in, or send HTTP Basic credentials".to_string(),
        ),
    };
    (status, axum::Json(serde_json::json!({ "error": message }))).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::testing::MapSecretStore;
    use kayak_core::server_config::UserConfig;
    use std::collections::BTreeMap;

    fn config(users: &[(&str, &str, Role)]) -> ServerConfig {
        let users: BTreeMap<String, UserConfig> = users
            .iter()
            .map(|(name, password, role)| {
                (
                    (*name).to_string(),
                    UserConfig {
                        password: (*password).into(),
                        role: *role,
                    },
                )
            })
            .collect();
        ServerConfig {
            history: kayak_core::server_config::HistoryConfig::default(),
            auth: AuthConfig::Basic { users },
        }
    }

    fn auth() -> anyhow::Result<Auth> {
        let secrets = MapSecretStore::new("the test secrets", &[("SAM_PASSWORD", "hunter2")]);
        Auth::from_config(
            &config(&[
                ("sam", "${SAM_PASSWORD}", Role::Admin),
                ("kim", "correct horse", Role::Read),
            ]),
            &secrets,
        )
    }

    #[test]
    fn a_server_with_no_auth_section_identifies_nobody() {
        let auth = Auth::disabled();
        assert!(!auth.is_enabled());
        assert_eq!(auth.authenticate("sam", "hunter2"), None);
    }

    /// The password lives in the secret store and the settings file holds only
    /// the reference — the same promise a connection's password makes.
    #[test]
    fn a_password_reference_is_resolved_against_the_secret_store() -> anyhow::Result<()> {
        let auth = auth()?;
        assert_eq!(
            auth.authenticate("sam", "hunter2"),
            Some(Identity {
                username: "sam".to_string(),
                role: Role::Admin
            })
        );
        // the template itself is not the password
        assert_eq!(auth.authenticate("sam", "${SAM_PASSWORD}"), None);
        Ok(())
    }

    /// A `${NAME}` nobody set is a login that would never succeed, so it stops
    /// the server instead.
    #[test]
    fn an_unset_password_reference_fails_at_startup() {
        let secrets = MapSecretStore::empty();
        let result = Auth::from_config(&config(&[("sam", "${NOPE}", Role::Admin)]), &secrets);
        let Err(err) = result else {
            panic!("a server with an unresolvable password started");
        };
        let message = format!("{err:#}");
        assert!(message.contains("sam"), "{message}");
        assert!(message.contains("NOPE"), "{message}");
    }

    #[test]
    fn a_wrong_password_and_an_unknown_user_are_both_just_no() -> anyhow::Result<()> {
        let auth = auth()?;
        assert_eq!(auth.authenticate("sam", "wrong"), None);
        assert_eq!(auth.authenticate("nobody", "hunter2"), None);
        assert_eq!(auth.authenticate("", ""), None);
        Ok(())
    }

    #[test]
    fn a_role_comes_from_the_account_that_was_matched() -> anyhow::Result<()> {
        let auth = auth()?;
        assert_eq!(
            auth.authenticate("kim", "correct horse").map(|i| i.role),
            Some(Role::Read)
        );
        Ok(())
    }

    /// A session names the identity that was checked when it was handed out,
    /// and logging out takes it away — the thing a signed cookie could not do.
    #[test]
    fn a_session_lasts_until_it_is_logged_out() -> anyhow::Result<()> {
        let auth = auth()?;
        let identity = auth
            .authenticate("kim", "correct horse")
            .context("kim signs in")?;
        let token = auth.log_in(&identity)?;
        assert_eq!(auth.session(&token), Some(identity));
        auth.log_out(&token);
        assert_eq!(auth.session(&token), None);
        // and again, harmlessly
        auth.log_out(&token);
        Ok(())
    }

    #[test]
    fn two_sessions_are_different_tokens() -> anyhow::Result<()> {
        let auth = auth()?;
        let identity = auth
            .authenticate("sam", "hunter2")
            .context("sam signs in")?;
        assert_ne!(auth.log_in(&identity)?, auth.log_in(&identity)?);
        Ok(())
    }

    #[test]
    fn a_token_that_was_never_issued_is_nobody() -> anyhow::Result<()> {
        assert_eq!(auth()?.session("deadbeef"), None);
        Ok(())
    }

    fn request_with(header_name: header::HeaderName, value: &str) -> anyhow::Result<Request> {
        Ok(Request::builder()
            .header(header_name, value)
            .body(axum::body::Body::empty())?)
    }

    #[test]
    fn basic_credentials_are_read_out_of_the_authorization_header() -> anyhow::Result<()> {
        let encoded = base64::engine::general_purpose::STANDARD.encode("sam:hunter2");
        let request = request_with(header::AUTHORIZATION, &format!("Basic {encoded}"))?;
        assert_eq!(
            basic_credentials(&request),
            Some(("sam".to_string(), "hunter2".to_string()))
        );
        Ok(())
    }

    /// A colon is legal in a password and not in a username, so the split is
    /// on the first one. Getting this wrong would silently truncate passwords.
    #[test]
    fn a_password_may_contain_a_colon() -> anyhow::Result<()> {
        let encoded = base64::engine::general_purpose::STANDARD.encode("sam:hunter:2:3");
        let request = request_with(header::AUTHORIZATION, &format!("Basic {encoded}"))?;
        assert_eq!(
            basic_credentials(&request),
            Some(("sam".to_string(), "hunter:2:3".to_string()))
        );
        Ok(())
    }

    #[test]
    fn a_header_that_is_not_basic_credentials_is_no_credentials() -> anyhow::Result<()> {
        for value in ["Bearer abc", "Basic !!!not base64!!!", "Basic", ""] {
            let request = request_with(header::AUTHORIZATION, value)?;
            assert_eq!(basic_credentials(&request), None, "{value}");
        }
        Ok(())
    }

    #[test]
    fn a_cookie_is_picked_out_of_the_header_by_name() -> anyhow::Result<()> {
        let request = request_with(
            header::COOKIE,
            "theme=dark; kayak_session=abc123; other=value",
        )?;
        assert_eq!(cookie(&request, SESSION_COOKIE), Some("abc123".to_string()));
        assert_eq!(cookie(&request, "nothing"), None);
        Ok(())
    }

    /// Both schemes have to land on the same identity, because the
    /// authorization check downstream of them is one check.
    #[test]
    fn a_cookie_and_basic_credentials_identify_the_same_person() -> anyhow::Result<()> {
        let auth = auth()?;
        let identity = auth
            .authenticate("sam", "hunter2")
            .context("sam signs in")?;
        let token = auth.log_in(&identity)?;

        let by_cookie = auth.identify(&request_with(
            header::COOKIE,
            &format!("{SESSION_COOKIE}={token}"),
        )?);
        let encoded = base64::engine::general_purpose::STANDARD.encode("sam:hunter2");
        let by_basic = auth.identify(&request_with(
            header::AUTHORIZATION,
            &format!("Basic {encoded}"),
        )?);

        assert_eq!(by_cookie, Some(identity.clone()));
        assert_eq!(by_basic, Some(identity));
        Ok(())
    }

    #[test]
    fn a_stale_cookie_falls_through_to_the_credentials_that_are_there() -> anyhow::Result<()> {
        let auth = auth()?;
        let identity = auth
            .authenticate("sam", "hunter2")
            .context("sam signs in")?;
        let token = auth.log_in(&identity)?;
        auth.log_out(&token);

        let encoded = base64::engine::general_purpose::STANDARD.encode("kim:correct horse");
        let mut request = request_with(header::COOKIE, &format!("{SESSION_COOKIE}={token}"))?;
        request
            .headers_mut()
            .insert(header::AUTHORIZATION, format!("Basic {encoded}").parse()?);
        assert_eq!(
            auth.identify(&request).map(|i| i.username),
            Some("kim".to_string())
        );
        Ok(())
    }
}
