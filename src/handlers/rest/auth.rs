//! Signing in, signing out, and asking who you are.
//!
//! The three endpoints a browser needs and a `curl` user does not — everything
//! here exists so that the UI can have a login page instead of the browser's
//! own credential dialog. See [`crate::auth`] for why a cookie is required at
//! all rather than being a nicety.

use std::sync::Arc;

use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::{Extension, Json, extract::State};
use kayak_core::{AuthDto, LoginRequest};

use crate::auth::{Identity, SESSION_COOKIE};
use crate::{handlers::error::AppError, state::AppState};

/// Who the caller is, and whether this server asks.
///
/// Public, and it has to be: this is the endpoint that answers "should I be
/// showing a login page", which by definition is asked by someone who has not
/// logged in. It gives away only whether authentication is on — which anyone
/// can discover by making one other request anyway.
///
/// The identity comes from the request extensions rather than being worked out
/// here, because the middleware has already done it for every endpoint.
#[allow(clippy::unused_async)]
pub async fn whoami(
    State(state): State<Arc<AppState>>,
    identity: Option<Extension<Identity>>,
) -> impl IntoResponse {
    Json(dto(&state, identity.map(|Extension(identity)| identity)))
}

/// Exchange a username and password for a session.
///
/// A 401 on a wrong password says only that it was wrong — not whether the
/// username exists, which [`crate::auth::Auth::authenticate`] takes some care
/// not to reveal in its timing either.
///
/// On a server with authentication turned off this is a 404-shaped situation
/// rather than a 401: there is nothing to log into. It answers with the same
/// `AuthDto` the caller would get from `whoami`, so the UI's "am I signed in"
/// logic has one shape rather than two.
#[allow(clippy::unused_async)]
pub async fn login(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(payload): Json<LoginRequest>,
) -> Result<Response, AppError> {
    let auth = state.auth();
    if !auth.is_enabled() {
        return Ok(Json(AuthDto::open()).into_response());
    }
    let Some(identity) = auth.authenticate(&payload.username, &payload.password) else {
        tracing::info!("failed login for '{}'", payload.username);
        return Ok((
            StatusCode::UNAUTHORIZED,
            Json(serde_json::json!({"error": "wrong username or password"})),
        )
            .into_response());
    };
    let token = auth.log_in(&identity)?;
    tracing::info!(
        "'{}' signed in as {}",
        identity.username,
        identity.role.label()
    );
    let body = dto(&state, Some(identity));
    Ok((
        [(header::SET_COOKIE, set_cookie(&token, &headers))],
        Json(body),
    )
        .into_response())
}

/// End the session this request is carrying.
///
/// Both halves matter: the cookie is cleared in the browser *and* the session
/// is dropped from the server, so a copy of the cookie taken from somewhere
/// else stops working too. That is the thing a signed stateless cookie could
/// not do, and the reason sessions are stored.
///
/// Idempotent, and it answers 204 whether or not there was a session to end —
/// there is nothing a caller could do differently, and "that token wasn't real"
/// answers a question nobody should be able to ask.
#[allow(clippy::unused_async)]
pub async fn logout(State(state): State<Arc<AppState>>, headers: HeaderMap) -> impl IntoResponse {
    if let Some(token) = cookie_from(&headers, SESSION_COOKIE) {
        state.auth().log_out(&token);
    }
    (
        StatusCode::NO_CONTENT,
        [(header::SET_COOKIE, clear_cookie(&headers))],
    )
}

/// What this server says about a caller.
fn dto(state: &AppState, identity: Option<Identity>) -> AuthDto {
    AuthDto {
        authentication_required: state.auth().is_enabled(),
        username: identity.as_ref().map(|i| i.username.clone()),
        role: identity.map(|i| i.role),
    }
}

/// The `Set-Cookie` that starts a session.
///
/// `HttpOnly` so a script on the page can't read it, `SameSite=Strict` so
/// another site can't cause a request that carries it, `Path=/` because the
/// canvas and the API are the same origin. No `Max-Age`: it is a session
/// cookie, so closing the browser ends it, and the server's own copy is what
/// actually decides how long it lives.
fn set_cookie(token: &str, headers: &HeaderMap) -> String {
    let mut cookie = format!("{SESSION_COOKIE}={token}; HttpOnly; SameSite=Strict; Path=/");
    if is_https(headers) {
        cookie.push_str("; Secure");
    }
    cookie
}

/// The `Set-Cookie` that ends one. Same attributes — a browser matches on them
/// to decide which cookie is being replaced — with an expiry in the past.
fn clear_cookie(headers: &HeaderMap) -> String {
    let mut cookie = format!("{SESSION_COOKIE}=; HttpOnly; SameSite=Strict; Path=/; Max-Age=0");
    if is_https(headers) {
        cookie.push_str("; Secure");
    }
    cookie
}

/// Whether the request reached us over TLS, as far as we can tell.
///
/// `Secure` is set when it did and not when it didn't, rather than always:
/// a `Secure` cookie is never sent back over plain http, so setting it
/// unconditionally would make logging in silently fail on the plain-http
/// deployment that the readme already tells people not to run. The header is
/// what a reverse proxy in front of kayak sets, which is the deployment TLS
/// actually happens in — see the readme's note that terminating TLS in front of
/// this is the expectation rather than an option.
fn is_https(headers: &HeaderMap) -> bool {
    headers
        .get("x-forwarded-proto")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|proto| proto.split(',').next().is_some_and(|p| p.trim() == "https"))
}

/// One cookie's value out of a header map. The middleware has its own copy of
/// this over a `Request`; a handler only ever sees the headers.
fn cookie_from(headers: &HeaderMap, name: &str) -> Option<String> {
    let header = headers.get(header::COOKIE)?.to_str().ok()?;
    header.split(';').find_map(|pair| {
        let (key, value) = pair.split_once('=')?;
        (key.trim() == name).then(|| value.trim().to_string())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn headers(proto: Option<&str>) -> anyhow::Result<HeaderMap> {
        let mut headers = HeaderMap::new();
        if let Some(proto) = proto {
            headers.insert("x-forwarded-proto", proto.parse()?);
        }
        Ok(headers)
    }

    /// The three attributes that make the cookie worth having. `HttpOnly` is
    /// the one that matters most — without it an injected script reads the
    /// session straight out of `document.cookie`.
    #[test]
    fn the_session_cookie_is_httponly_samesite_and_scoped_to_the_site() -> anyhow::Result<()> {
        let cookie = set_cookie("abc123", &headers(None)?);
        assert!(cookie.starts_with("kayak_session=abc123;"), "{cookie}");
        assert!(cookie.contains("HttpOnly"), "{cookie}");
        assert!(cookie.contains("SameSite=Strict"), "{cookie}");
        assert!(cookie.contains("Path=/"), "{cookie}");
        Ok(())
    }

    /// `Secure` when the request came over TLS, and *not* when it didn't: a
    /// `Secure` cookie is never sent back over plain http, so setting it
    /// unconditionally makes logging in fail with no error anywhere.
    #[test]
    fn secure_follows_the_scheme_the_request_arrived_on() -> anyhow::Result<()> {
        assert!(set_cookie("t", &headers(Some("https"))?).contains("Secure"));
        // a proxy chain sends a list; the first entry is the client's
        assert!(set_cookie("t", &headers(Some("https, http"))?).contains("Secure"));
        assert!(!set_cookie("t", &headers(Some("http"))?).contains("Secure"));
        assert!(!set_cookie("t", &headers(None)?).contains("Secure"));
        Ok(())
    }

    /// A browser matches a replacement cookie on its attributes, so clearing
    /// one has to look like the one it is clearing.
    #[test]
    fn clearing_the_cookie_matches_the_one_it_replaces() -> anyhow::Result<()> {
        let set = set_cookie("abc123", &headers(Some("https"))?);
        let clear = clear_cookie(&headers(Some("https"))?);
        for attribute in ["HttpOnly", "SameSite=Strict", "Path=/", "Secure"] {
            assert!(clear.contains(attribute), "{clear} is missing {attribute}");
            assert!(set.contains(attribute), "{set} is missing {attribute}");
        }
        assert!(clear.contains("Max-Age=0"), "{clear}");
        assert!(clear.starts_with("kayak_session=;"), "{clear}");
        Ok(())
    }

    #[test]
    fn a_session_token_is_read_out_of_the_cookie_header() -> anyhow::Result<()> {
        let mut map = HeaderMap::new();
        map.insert(header::COOKIE, "theme=dark; kayak_session=abc123".parse()?);
        assert_eq!(
            cookie_from(&map, SESSION_COOKIE),
            Some("abc123".to_string())
        );
        assert_eq!(cookie_from(&HeaderMap::new(), SESSION_COOKIE), None);
        Ok(())
    }
}
