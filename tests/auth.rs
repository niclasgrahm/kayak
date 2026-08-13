//! What the server lets through, and what it doesn't.
//!
//! Driven through the real router with `tower::ServiceExt::oneshot`, like
//! `tests/api.rs` — the middleware is applied by `api_router`, so a test that
//! called handlers directly would prove nothing about whether anything is
//! actually guarded.
//!
//! The first test in here is the one that matters most: a server started with
//! no settings file behaves exactly as it did before any of this existed. Every
//! other test is about what happens once someone opts in.

use std::sync::Arc;

use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use base64::Engine;
use http_body_util::BodyExt;
use kayak::api_router;
use kayak::auth::Auth;
use kayak::state::AppState;
use kayak_core::server_config::{AuthConfig, HistoryConfig, Role, ServerConfig, UserConfig};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use tower::ServiceExt;

/// A server that asks nobody for anything — the default, and what a process
/// with no `--server-config` runs.
fn open_server() -> Router {
    api_router(Arc::new(AppState::new()))
}

/// A server with one admin and one reader, both with literal passwords (the
/// `${NAME}` path is covered by the unit tests in `src/auth.rs`).
fn guarded_server() -> anyhow::Result<Router> {
    let users: BTreeMap<String, UserConfig> = [
        (
            "root".to_string(),
            UserConfig {
                password: "hunter2".into(),
                role: Role::Admin,
            },
        ),
        (
            "watcher".to_string(),
            UserConfig {
                password: "correct horse".into(),
                role: Role::Read,
            },
        ),
    ]
    .into_iter()
    .collect();
    let config = ServerConfig {
        history: HistoryConfig::default(),
        auth: AuthConfig::Basic { users },
    };
    let auth = Arc::new(Auth::from_config(&config, &kayak::secrets::EnvStore)?);
    Ok(api_router(Arc::new(AppState::new().with_auth(auth))))
}

fn basic(username: &str, password: &str) -> String {
    let encoded =
        base64::engine::general_purpose::STANDARD.encode(format!("{username}:{password}"));
    format!("Basic {encoded}")
}

struct Sent {
    status: StatusCode,
    body: Value,
    set_cookie: Option<String>,
    headers: axum::http::HeaderMap,
}

async fn send(app: &Router, request: Request<Body>) -> anyhow::Result<Sent> {
    let response = app.clone().oneshot(request).await?;
    let status = response.status();
    let headers = response.headers().clone();
    let set_cookie = headers
        .get(header::SET_COOKIE)
        .and_then(|value| value.to_str().ok())
        .map(ToString::to_string);
    let bytes = response.into_body().collect().await?.to_bytes();
    let body = if bytes.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice(&bytes)
            .unwrap_or_else(|_| Value::String(String::from_utf8_lossy(&bytes).into_owned()))
    };
    Ok(Sent {
        status,
        body,
        set_cookie,
        headers,
    })
}

/// A request builder that takes an optional credential header, so a test can
/// say "the same call, signed in as this person".
fn request(
    method: &str,
    uri: &str,
    credential: Option<&str>,
    body: Option<&Value>,
) -> Request<Body> {
    let mut builder = Request::builder().method(method).uri(uri);
    if let Some(credential) = credential {
        builder = builder.header(header::AUTHORIZATION, credential);
    }
    let body = match body {
        Some(value) => {
            builder = builder.header(header::CONTENT_TYPE, "application/json");
            Body::from(serde_json::to_vec(value).unwrap_or_default())
        }
        None => Body::empty(),
    };
    builder
        .body(body)
        .unwrap_or_else(|_| Request::new(Body::empty()))
}

fn with_cookie(method: &str, uri: &str, cookie: &str) -> Request<Body> {
    Request::builder()
        .method(method)
        .uri(uri)
        .header(header::COOKIE, cookie)
        .body(Body::empty())
        .unwrap_or_else(|_| Request::new(Body::empty()))
}

fn idle_config(id: &str) -> Value {
    json!({
        "id": id,
        "inputs": [{ "type": "dummy", "duration": 3600 }],
        "transforms": [],
        "outputs": [{ "type": "stdout" }]
    })
}

/// **The promise this whole feature is built around.** A server started without
/// a settings file authenticates nobody, and every endpoint — read, write and
/// public alike — behaves as it did before roles existed. If this test ever
/// fails, an upgrade has locked somebody out of their own server.
#[tokio::test]
async fn a_server_with_no_settings_file_guards_nothing() -> anyhow::Result<()> {
    let app = open_server();

    let created = send(
        &app,
        request("POST", "/api/pipelines", None, Some(&idle_config("open"))),
    )
    .await?;
    assert_eq!(created.status, StatusCode::CREATED);

    for (method, uri) in [
        ("GET", "/api/pipelines"),
        ("GET", "/api/connections"),
        ("GET", "/api/settings"),
        ("GET", "/api/layout"),
        ("GET", "/api/state"),
        ("GET", "/api/docs"),
    ] {
        let sent = send(&app, request(method, uri, None, None)).await?;
        assert_eq!(sent.status, StatusCode::OK, "{method} {uri}");
    }

    let deleted = send(&app, request("DELETE", "/api/pipelines/open", None, None)).await?;
    assert_eq!(deleted.status, StatusCode::NO_CONTENT);
    Ok(())
}

/// ...and it says so, so the UI knows not to draw a login page.
#[tokio::test]
async fn an_open_server_reports_that_it_needs_no_login() -> anyhow::Result<()> {
    let sent = send(&open_server(), request("GET", "/api/auth/me", None, None)).await?;
    assert_eq!(sent.status, StatusCode::OK);
    assert_eq!(sent.body["authentication_required"], json!(false));
    assert_eq!(sent.body["role"], Value::Null);
    Ok(())
}

#[tokio::test]
async fn an_unauthenticated_request_is_refused_once_accounts_exist() -> anyhow::Result<()> {
    let app = guarded_server()?;
    for (method, uri) in [
        ("GET", "/api/pipelines"),
        ("GET", "/api/settings"),
        ("POST", "/api/pipelines"),
        ("DELETE", "/api/pipelines/whatever"),
    ] {
        let sent = send(&app, request(method, uri, None, None)).await?;
        assert_eq!(sent.status, StatusCode::UNAUTHORIZED, "{method} {uri}");
        assert!(
            sent.body["error"].is_string(),
            "{method} {uri} answered without an error body"
        );
    }
    Ok(())
}

/// **No `WWW-Authenticate`.** Sending it makes the browser throw its own
/// credential dialog over the app, which is the thing the login page exists to
/// replace — and there is no way to send it to curl and not to a browser.
/// `curl -u` sends its credentials preemptively, so nothing needs the challenge.
#[tokio::test]
async fn a_401_does_not_challenge_the_browser() -> anyhow::Result<()> {
    let app = guarded_server()?;
    let sent = send(&app, request("GET", "/api/pipelines", None, None)).await?;
    assert_eq!(sent.status, StatusCode::UNAUTHORIZED);
    assert!(
        sent.headers.get(header::WWW_AUTHENTICATE).is_none(),
        "the 401 carried a WWW-Authenticate header"
    );
    Ok(())
}

#[tokio::test]
async fn basic_credentials_get_an_admin_in() -> anyhow::Result<()> {
    let app = guarded_server()?;
    let credential = basic("root", "hunter2");

    let created = send(
        &app,
        request(
            "POST",
            "/api/pipelines",
            Some(&credential),
            Some(&idle_config("mine")),
        ),
    )
    .await?;
    assert_eq!(created.status, StatusCode::CREATED);

    let listed = send(
        &app,
        request("GET", "/api/pipelines", Some(&credential), None),
    )
    .await?;
    assert_eq!(listed.status, StatusCode::OK);

    let deleted = send(
        &app,
        request("DELETE", "/api/pipelines/mine", Some(&credential), None),
    )
    .await?;
    assert_eq!(deleted.status, StatusCode::NO_CONTENT);
    Ok(())
}

/// The point of having two roles: a reader sees everything and changes nothing.
#[tokio::test]
async fn a_reader_may_look_but_not_touch() -> anyhow::Result<()> {
    let app = guarded_server()?;
    let credential = basic("watcher", "correct horse");

    for (method, uri) in [
        ("GET", "/api/pipelines"),
        ("GET", "/api/connections"),
        ("GET", "/api/settings"),
        ("GET", "/api/layout"),
        ("GET", "/api/state"),
    ] {
        let sent = send(&app, request(method, uri, Some(&credential), None)).await?;
        assert_eq!(sent.status, StatusCode::OK, "{method} {uri}");
    }

    let refused = send(
        &app,
        request(
            "POST",
            "/api/pipelines",
            Some(&credential),
            Some(&idle_config("nope")),
        ),
    )
    .await?;
    assert_eq!(refused.status, StatusCode::FORBIDDEN);

    // and the 403 says what would be needed, rather than just "no"
    let message = refused.body["error"].as_str().unwrap_or_default();
    assert!(message.contains("watcher"), "{message}");
    assert!(message.contains("admin"), "{message}");

    for (method, uri) in [
        ("DELETE", "/api/pipelines/anything"),
        ("POST", "/api/config/revert"),
    ] {
        let sent = send(&app, request(method, uri, Some(&credential), None)).await?;
        assert_eq!(sent.status, StatusCode::FORBIDDEN, "{method} {uri}");
    }
    Ok(())
}

/// Arranging the canvas writes a file that gets committed, so it is an admin
/// act — a reader can look at the canvas, they just can't rearrange it.
#[tokio::test]
async fn rearranging_the_canvas_is_an_admin_act() -> anyhow::Result<()> {
    let app = guarded_server()?;
    let layout = json!({"pipelines": {}});

    let reader = send(
        &app,
        request(
            "PUT",
            "/api/layout",
            Some(&basic("watcher", "correct horse")),
            Some(&layout),
        ),
    )
    .await?;
    assert_eq!(reader.status, StatusCode::FORBIDDEN);

    let admin = send(
        &app,
        request(
            "PUT",
            "/api/layout",
            Some(&basic("root", "hunter2")),
            Some(&layout),
        ),
    )
    .await?;
    assert_ne!(admin.status, StatusCode::FORBIDDEN);
    Ok(())
}

#[tokio::test]
async fn a_wrong_password_gets_nowhere() -> anyhow::Result<()> {
    let app = guarded_server()?;
    for credential in [
        basic("root", "wrong"),
        basic("nobody", "hunter2"),
        // the reader's password against the admin's name
        basic("root", "correct horse"),
    ] {
        let sent = send(
            &app,
            request("GET", "/api/pipelines", Some(&credential), None),
        )
        .await?;
        assert_eq!(sent.status, StatusCode::UNAUTHORIZED, "{credential}");
    }
    Ok(())
}

/// The data plane is deliberately not behind the operators' credentials — a
/// device posting readings is not an operator. It gets its own mechanism later;
/// until then this endpoint is open even on a guarded server, and the readme
/// says so.
#[tokio::test]
async fn the_ingest_endpoint_is_not_behind_the_login() -> anyhow::Result<()> {
    let app = guarded_server()?;
    let sent = send(
        &app,
        request(
            "POST",
            "/api/pipelines/nothing/messages",
            None,
            Some(&json!({"a": 1})),
        ),
    )
    .await?;
    // 404 because there is no such pipeline — the point is that it is not a 401
    assert_eq!(sent.status, StatusCode::NOT_FOUND);
    Ok(())
}

/// The reference describes kayak rather than this deployment, so it stays
/// readable — you can look up how to use a server you have no account on.
#[tokio::test]
async fn the_component_reference_stays_public() -> anyhow::Result<()> {
    let app = guarded_server()?;
    for uri in ["/api/docs", "/api/openapi.json"] {
        let sent = send(&app, request("GET", uri, None, None)).await?;
        assert_eq!(sent.status, StatusCode::OK, "{uri}");
    }
    Ok(())
}

/// An unknown path is the router's own 404 and not a 401. Keeping those
/// different is what `route_layer` buys, and it means a typo teaches you
/// something rather than looking like a permissions problem.
#[tokio::test]
async fn an_unknown_path_is_still_a_404() -> anyhow::Result<()> {
    let app = guarded_server()?;
    let sent = send(&app, request("GET", "/api/nonsense", None, None)).await?;
    assert_eq!(sent.status, StatusCode::NOT_FOUND);
    Ok(())
}

/// The browser's path in full: log in, get a cookie, use it on an endpoint that
/// could not have carried an `Authorization` header — which is the whole reason
/// the cookie exists.
#[tokio::test]
async fn logging_in_yields_a_cookie_that_authenticates() -> anyhow::Result<()> {
    let app = guarded_server()?;
    let sent = send(
        &app,
        request(
            "POST",
            "/api/auth/login",
            None,
            Some(&json!({"username": "root", "password": "hunter2"})),
        ),
    )
    .await?;
    assert_eq!(sent.status, StatusCode::OK);
    assert_eq!(sent.body["username"], json!("root"));
    assert_eq!(sent.body["role"], json!("admin"));
    assert_eq!(sent.body["authentication_required"], json!(true));

    let cookie = sent.set_cookie.unwrap_or_default();
    assert!(cookie.contains("HttpOnly"), "{cookie}");
    let token = cookie
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_string();

    let listed = send(&app, with_cookie("GET", "/api/pipelines", &token)).await?;
    assert_eq!(listed.status, StatusCode::OK);

    let me = send(&app, with_cookie("GET", "/api/auth/me", &token)).await?;
    assert_eq!(me.body["username"], json!("root"));
    Ok(())
}

/// A wrong password at the login endpoint is a 401 that says nothing about
/// whether the username exists.
#[tokio::test]
async fn a_failed_login_does_not_say_which_half_was_wrong() -> anyhow::Result<()> {
    let app = guarded_server()?;
    let mut messages = Vec::new();
    for credentials in [
        json!({"username": "root", "password": "wrong"}),
        json!({"username": "ghost", "password": "wrong"}),
    ] {
        let sent = send(
            &app,
            request("POST", "/api/auth/login", None, Some(&credentials)),
        )
        .await?;
        assert_eq!(sent.status, StatusCode::UNAUTHORIZED, "{credentials}");
        assert!(sent.set_cookie.is_none(), "a failed login set a cookie");
        messages.push(sent.body["error"].as_str().unwrap_or_default().to_string());
    }
    assert_eq!(
        messages[0], messages[1],
        "the two failures read differently"
    );
    Ok(())
}

/// The thing a signed stateless cookie could not do: logging out invalidates
/// the session on the server, so a copy of the cookie taken from anywhere else
/// stops working too.
#[tokio::test]
async fn logging_out_revokes_the_session_everywhere() -> anyhow::Result<()> {
    let app = guarded_server()?;
    let logged_in = send(
        &app,
        request(
            "POST",
            "/api/auth/login",
            None,
            Some(&json!({"username": "watcher", "password": "correct horse"})),
        ),
    )
    .await?;
    let token = logged_in
        .set_cookie
        .unwrap_or_default()
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_string();

    assert_eq!(
        send(&app, with_cookie("GET", "/api/pipelines", &token))
            .await?
            .status,
        StatusCode::OK
    );

    let out = send(&app, with_cookie("POST", "/api/auth/logout", &token)).await?;
    assert_eq!(out.status, StatusCode::NO_CONTENT);
    assert!(
        out.set_cookie.unwrap_or_default().contains("Max-Age=0"),
        "logging out did not clear the cookie"
    );

    // the same cookie value, now worthless
    assert_eq!(
        send(&app, with_cookie("GET", "/api/pipelines", &token))
            .await?
            .status,
        StatusCode::UNAUTHORIZED
    );
    Ok(())
}

/// A cookie carries the role it was issued with, so the two schemes really do
/// land on one identity rather than on two nearly-identical ones.
#[tokio::test]
async fn a_readers_cookie_is_still_a_readers() -> anyhow::Result<()> {
    let app = guarded_server()?;
    let logged_in = send(
        &app,
        request(
            "POST",
            "/api/auth/login",
            None,
            Some(&json!({"username": "watcher", "password": "correct horse"})),
        ),
    )
    .await?;
    assert_eq!(logged_in.body["role"], json!("read"));
    let token = logged_in
        .set_cookie
        .unwrap_or_default()
        .split(';')
        .next()
        .unwrap_or_default()
        .trim()
        .to_string();

    let request = Request::builder()
        .method("POST")
        .uri("/api/pipelines")
        .header(header::COOKIE, &token)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&idle_config("nope"))?))?;
    assert_eq!(send(&app, request).await?.status, StatusCode::FORBIDDEN);
    Ok(())
}

/// Logging into a server with no accounts is not an error — there is nothing to
/// sign into, and the UI's "am I signed in" logic gets one shape rather than
/// two.
#[tokio::test]
async fn logging_into_an_open_server_says_there_is_nothing_to_log_into() -> anyhow::Result<()> {
    let sent = send(
        &open_server(),
        request(
            "POST",
            "/api/auth/login",
            None,
            Some(&json!({"username": "anyone", "password": "anything"})),
        ),
    )
    .await?;
    assert_eq!(sent.status, StatusCode::OK);
    assert_eq!(sent.body["authentication_required"], json!(false));
    assert!(sent.set_cookie.is_none());
    Ok(())
}
