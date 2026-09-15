//! What the two components that *send* http requests share.
//!
//! The `http` output and the `http` transform both take a url, a verb, an
//! optional credential and a timeout, and both quote an endpoint's complaint
//! when it refuses them. This is that half, spelled once: the parsing, the
//! credential, the naming of a url in an error, and the cutting of a reply
//! body down to something a log line can hold. What differs — an output
//! discards the reply, a transform carries on with it — stays in each.

use std::time::Duration;

use anyhow::{Context, Result};
use kayak_core::config::{HttpAuthConfig, HttpVerb};
use reqwest::header::{HeaderName, HeaderValue};
use reqwest::{Method, Url};

use crate::BuildCtx;

/// How long a request may take before it is given up on, when the config
/// doesn't say. Thirty seconds is generous for a webhook and is still a bound:
/// without one, an endpoint that accepts a connection and then never answers
/// holds the pipeline's run loop for as long as it likes.
pub const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// How much of a rejecting endpoint's response body is quoted in the error.
///
/// The body is the only thing that says *why* a request was refused, which is
/// why it is read at all — but an html error page is megabytes, and this text
/// becomes an [`crate::history::ErrorSignature`] key as well as a line in the
/// UI. Bounded here rather than at either of those.
pub const MAX_DETAIL_BYTES: usize = 300;

/// A url parsed and checked at build time — a typo in a url is a config
/// mistake, and a pipeline that starts and then fails once a second says so
/// much less clearly than one that refuses to build. `what` names the
/// component in the error.
pub fn parse_url(what: &str, url: &str) -> Result<Url> {
    let parsed = Url::parse(url).with_context(|| format!("the {what}'s url '{url}' is not a url"))?;
    anyhow::ensure!(
        matches!(parsed.scheme(), "http" | "https"),
        "the {what}'s url '{url}' is not an http url; the scheme is '{}'",
        parsed.scheme()
    );
    Ok(parsed)
}

/// The method for a verb, refusing the ones that carry no body. Both
/// components exist to send the messages somewhere, and a method with no body
/// has nowhere to put them — a round trip per batch delivering nothing.
pub fn method_with_body(what: &str, verb: HttpVerb) -> Result<Method> {
    Ok(match verb {
        HttpVerb::Post => Method::POST,
        HttpVerb::Put => Method::PUT,
        HttpVerb::Patch => Method::PATCH,
        HttpVerb::Get | HttpVerb::Delete => anyhow::bail!(
            "the {what} cannot use {verb}: a request with no body would send none of the \
             messages. Use POST, PUT or PATCH."
        ),
    })
}

/// How a url is named in an error and in a log.
///
/// Userinfo is stripped, because `https://kayak:hunter2@example.com/hook` is a
/// perfectly ordinary way to write a webhook url and an error message is
/// exactly the place a password should not turn up. The rest is left alone —
/// a query string is part of what someone needs to see to recognise which
/// endpoint failed.
#[must_use]
pub fn describe(url: &Url) -> String {
    let mut clean = url.clone();
    if clean.password().is_some() {
        let _ = clean.set_password(None);
    }
    if !clean.username().is_empty() {
        let _ = clean.set_username("");
    }
    clean.to_string()
}

/// The header a component presents on every request, already resolved.
///
/// The value is marked sensitive, so anything that dumps a request's headers
/// prints it as `Sensitive` rather than as the token. That is the outbound
/// twin of the rule the input's [`crate::inputs::http::Credentials`] follows:
/// the credential is held in exactly one place and never travels anywhere it
/// could be written down.
pub struct Credential {
    pub name: HeaderName,
    pub value: HeaderValue,
}

impl Credential {
    pub fn build(what: &str, config: &HttpAuthConfig, ctx: &BuildCtx) -> Result<Self> {
        let (name, prefix, secret) = match config {
            HttpAuthConfig::Bearer { token } => ("authorization", "Bearer ", token),
            HttpAuthConfig::Header { name, value } => {
                let trimmed = name.trim();
                anyhow::ensure!(!trimmed.is_empty(), "an {what}'s `auth` header needs a name");
                (trimmed, "", value)
            }
        };
        // unlike the input's, this name is not checked against ALLOWED_HEADERS:
        // that rule exists because an input's `envelope` copies headers into the
        // messages, and nothing here reads a header at all
        let name = HeaderName::try_from(name.to_ascii_lowercase())
            .with_context(|| format!("'{name}' is not a valid http header name"))?;

        let resolved = ctx.resolve(secret)?;
        anyhow::ensure!(
            !resolved.expose().is_empty(),
            "the credential for an {what}'s `auth` is empty, so the requests would carry an \
             empty header; check that '{resolved}' is set in the secret store"
        );
        // `expose` is one of the few places a real secret is reached; it goes
        // straight into the header and is not held, logged or copied
        let mut value = HeaderValue::try_from(format!("{prefix}{}", resolved.expose()))
            .context("the credential for an `auth` cannot be sent as a header")?;
        value.set_sensitive(true);

        Ok(Self { name, value })
    }
}

/// The first [`MAX_DETAIL_BYTES`] of an endpoint's complaint, cut on a
/// character boundary.
#[must_use]
pub fn truncate(detail: &str) -> String {
    if detail.len() <= MAX_DETAIL_BYTES {
        return detail.to_string();
    }
    let end = (0..=MAX_DETAIL_BYTES)
        .rev()
        .find(|i| detail.is_char_boundary(*i))
        .unwrap_or(0);
    format!("{}…", &detail[..end])
}
