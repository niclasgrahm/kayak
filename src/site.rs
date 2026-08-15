//! The frontend's static files — the WASM bundle, the stylesheet, the
//! vendored API-reference renderer — served out of the binary rather than off
//! the disk.
//!
//! # Why this exists
//!
//! Until this module, a production server was **two artifacts**: the binary,
//! and a `target/site` directory beside it that `LEPTOS_SITE_ROOT` pointed at.
//! `leptos_axum::file_and_error_handler` read every request for a static file
//! off that directory through a `ServeDir`. That is why the `Dockerfile` had
//! to copy `/out/site` as well as the binary, and why running the release
//! binary from anywhere else served a page whose WASM 404'd — a failure that
//! looks like a blank canvas rather than like a missing file.
//!
//! With `--features embed-assets` the site directory is compiled *into* the
//! binary, and the binary is the whole deployment: `scp` it somewhere and run
//! it. That is the property worth having, and it is why the feature is what
//! the release build uses.
//!
//! # It is a feature, and it is off by default
//!
//! `target/site` is a *build output*. Embedding it means the root crate cannot
//! compile until the frontend has been built, which would make `cargo check`,
//! `cargo test` and `just ci` all depend on a WASM toolchain and a
//! `cargo leptos` run. So the embed is behind `embed-assets`, which nothing in
//! the development loop turns on and which the `Dockerfile` passes to
//! `cargo leptos build --release --bin-features embed-assets`.
//!
//! That ordering is safe because cargo-leptos builds the client before the
//! server — it has to, since the server's shell links to files the client
//! build names. Building the two halves separately (`--frontend-only` then
//! `--server-only`) is the same thing said explicitly.
//!
//! # The fallback is layered, not replaced
//!
//! [`fallback`] looks in the embedded files first and hands everything else to
//! `leptos_axum::file_and_error_handler`, which is what renders the shell with
//! a 404 for a path that is neither a route nor a file. Reimplementing that
//! arm would mean owning a copy of leptos' SSR response builder; delegating
//! means this module only has to know about files. It also means the two
//! spellings degrade into each other: without the feature the fallback *is*
//! leptos', byte for byte, so a build without it behaves exactly as the server
//! did before this module existed.
//!
//! # The serving half is generic over where the bytes come from
//!
//! [`Assets`] is a trait and [`respond`] takes `&dyn Assets`, which is what
//! makes the whole HTTP surface — content types, `ETag`, `304`, precompressed
//! variants, `index.html` — testable with an in-memory map under a plain
//! `cargo test`. The alternative was a module whose only test needed a WASM
//! build first, i.e. a module with no test that ever ran in CI.
//!
//! # Known limits
//!
//! Request paths are **not percent-decoded**. Every name under `assets-dir`
//! and every name cargo-leptos emits is ASCII with no reserved characters, so
//! there is nothing to decode; a file named with a space would need this to
//! grow one, and would be refused rather than mis-served in the meantime.
//!
//! Responses are `cache-control: no-cache`, meaning "revalidate", not "do not
//! store". The asset names are stable across releases (`hash-files` is off),
//! so anything longer-lived would serve a stale bundle after a deploy; the
//! `ETag` is what makes the revalidation cost a 304 rather than a download.

use std::borrow::Cow;
use std::future::Future;
use std::pin::Pin;

use axum::body::Body;
use axum::extract::{FromRef, State};
use axum::http::{HeaderMap, Method, Request, Response, StatusCode, Uri, header};
use bytes::Bytes;
use leptos::config::LeptosOptions;
use leptos::prelude::IntoView;

/// One file, as it is about to be written onto the wire.
///
/// `bytes` is a `Cow` because the embedded case borrows `&'static [u8]`
/// straight out of the binary and copies nothing; the test double owns its
/// bytes.
pub struct Asset {
    pub bytes: Cow<'static, [u8]>,
    /// The entity tag, *unquoted* — [`respond`] adds the quotes.
    pub etag: String,
}

/// Where the static files come from.
///
/// Keys are site-root-relative paths with no leading slash: `index.html`,
/// `pkg/kayak.wasm`, `scalar.js`. A precompressed variant is a key of its own
/// (`pkg/kayak.wasm.br`), which is what lets [`respond`] look one up without
/// the source knowing anything about content encoding.
pub trait Assets {
    fn get(&self, key: &str) -> Option<Asset>;
}

/// A content encoding this server can answer with.
///
/// Only the two cargo-leptos' `--precompress` produces. `Identity` is the file
/// as it stands and is always available, which is why it has no suffix.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Encoding {
    Br,
    Gzip,
}

impl Encoding {
    /// What a precompressed variant of a file is called.
    #[must_use]
    pub fn suffix(self) -> &'static str {
        match self {
            Encoding::Br => ".br",
            Encoding::Gzip => ".gz",
        }
    }

    /// The `content-encoding` value that names it.
    #[must_use]
    pub fn token(self) -> &'static str {
        match self {
            Encoding::Br => "br",
            Encoding::Gzip => "gzip",
        }
    }
}

/// The precompressed variants this client will take, best first.
///
/// Brotli is preferred over gzip whenever both are acceptable, regardless of
/// the q-values' ordering — it is smaller on every asset here, and honouring a
/// client's stated preference between two encodings it accepts is not a choice
/// worth the parsing. A `q=0` is a refusal and *is* honoured, since that is
/// the only way a client has of saying "not this one".
#[must_use]
pub fn accepted_encodings(accept_encoding: Option<&str>) -> Vec<Encoding> {
    let Some(header) = accept_encoding else {
        return Vec::new();
    };
    let mut accepted = Vec::new();
    for wanted in [Encoding::Br, Encoding::Gzip] {
        if accepts(header, wanted.token()) {
            accepted.push(wanted);
        }
    }
    accepted
}

/// Whether an `accept-encoding` header takes one named encoding.
///
/// `*` counts, and a `q=0` on either the name or the wildcard does not.
fn accepts(header: &str, token: &str) -> bool {
    let mut wildcard = None;
    for part in header.split(',') {
        let mut fields = part.split(';').map(str::trim);
        let Some(name) = fields.next() else { continue };
        let acceptable = !fields.any(|field| {
            field
                .strip_prefix("q=")
                .is_some_and(|q| q.parse::<f32>().is_ok_and(|q| q <= 0.0))
        });
        if name.eq_ignore_ascii_case(token) {
            return acceptable;
        }
        if name == "*" {
            wildcard = Some(acceptable);
        }
    }
    wildcard.unwrap_or(false)
}

/// The asset key a request path names, or `None` if it names nothing this
/// server will look up.
///
/// A directory — `/` or anything ending in `/` — means that directory's
/// `index.html`, which is how the SSR shell's sibling files are reached. Empty
/// and `.`/`..` segments are **refused rather than normalised**, the same rule
/// `persist::save_path` follows: the path arrives from an HTTP request, and a
/// path that is cleaned up is a path whose rules live in two places. (Nothing
/// could escape the embed anyway — the keys are an exact map — but the check
/// costs nothing and outlives whatever backs [`Assets`] next.)
#[must_use]
pub fn key_for(path: &str) -> Option<String> {
    let trimmed = path.strip_prefix('/').unwrap_or(path);
    let mut key = if trimmed.is_empty() || trimmed.ends_with('/') {
        format!("{trimmed}index.html")
    } else {
        trimmed.to_string()
    };
    if key.starts_with('/') || key.contains('\\') {
        return None;
    }
    if key
        .split('/')
        .any(|segment| segment.is_empty() || segment == "." || segment == "..")
    {
        return None;
    }
    key.shrink_to_fit();
    Some(key)
}

/// The `content-type` for an asset key.
///
/// The one entry that is load-bearing rather than tidy is `.wasm`: a browser
/// only streams-compiles a bundle served as `application/wasm`, and anything
/// else makes the page work while quietly taking the slow path.
#[must_use]
pub fn content_type(key: &str) -> &'static str {
    let extension = key.rsplit_once('.').map_or("", |(_, ext)| ext);
    match extension {
        "html" | "htm" => "text/html; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "js" | "mjs" => "text/javascript; charset=utf-8",
        "wasm" => "application/wasm",
        "json" | "map" | "webmanifest" => "application/json",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "ico" => "image/x-icon",
        "woff" => "font/woff",
        "woff2" => "font/woff2",
        "ttf" => "font/ttf",
        "otf" => "font/otf",
        "txt" => "text/plain; charset=utf-8",
        "xml" => "application/xml",
        _ => "application/octet-stream",
    }
}

/// Serve one request out of `assets`, or `None` if they hold nothing for it.
///
/// `None` is the whole contract with [`fallback`]: it means "not a file",
/// which is the case leptos' handler exists to answer, and it must not be
/// confused with an error.
#[must_use]
pub fn respond(assets: &dyn Assets, path: &str, headers: &HeaderMap) -> Option<Response<Body>> {
    let key = key_for(path)?;
    let accept_encoding = headers
        .get(header::ACCEPT_ENCODING)
        .and_then(|value| value.to_str().ok());

    // A precompressed variant first, then the file itself. The content type
    // always comes from the *uncompressed* key: `kayak.wasm.br` is a brotli
    // encoding of a wasm file, not a file of type `br`.
    let (asset, encoding) = accepted_encodings(accept_encoding)
        .into_iter()
        .find_map(|enc| {
            assets
                .get(&format!("{key}{}", enc.suffix()))
                .map(|asset| (asset, Some(enc)))
        })
        .or_else(|| assets.get(&key).map(|asset| (asset, None)))?;

    let etag = format!("\"{}\"", asset.etag);
    let mut response = Response::builder()
        .header(header::CONTENT_TYPE, content_type(&key))
        .header(header::ETAG, &etag)
        .header(header::CACHE_CONTROL, "no-cache")
        // The answer depends on the request header even when the file was
        // served as-is, because *that* is what a shared cache would replay to
        // a client that would have taken the brotli one.
        .header(header::VARY, "accept-encoding");
    if let Some(encoding) = encoding {
        response = response.header(header::CONTENT_ENCODING, encoding.token());
    }

    if if_none_match(headers, &etag) {
        return response
            .status(StatusCode::NOT_MODIFIED)
            .body(Body::empty())
            .ok();
    }

    let bytes = match asset.bytes {
        Cow::Borrowed(bytes) => Bytes::from_static(bytes),
        Cow::Owned(bytes) => Bytes::from(bytes),
    };
    response
        .header(header::CONTENT_LENGTH, bytes.len())
        .body(Body::from(bytes))
        .ok()
}

/// Whether the client already holds this exact entity.
///
/// `*` matches anything, and the comparison is the weak one (a `W/` prefix is
/// ignored) because nothing here ever serves a byte range.
fn if_none_match(headers: &HeaderMap, etag: &str) -> bool {
    let Some(value) = headers
        .get(header::IF_NONE_MATCH)
        .and_then(|value| value.to_str().ok())
    else {
        return false;
    };
    value.split(',').map(str::trim).any(|candidate| {
        candidate == "*" || candidate.strip_prefix("W/").unwrap_or(candidate) == etag
    })
}

/// The site directory, compiled in.
///
/// `debug-embed` is on so that the feature means one thing in every profile:
/// without it rust-embed reads the folder off disk in debug builds, which
/// would make a `--features embed-assets` test binary depend on the absolute
/// path of the machine it was built on.
#[cfg(feature = "embed-assets")]
#[derive(rust_embed::Embed)]
#[folder = "target/site"]
// wasm-bindgen's TypeScript declarations. Nothing ever requests them over
// HTTP, and they are the only files cargo-leptos puts in the site directory
// that a browser has no use for.
#[exclude = "*.d.ts"]
struct SiteFiles;

#[cfg(feature = "embed-assets")]
struct Embedded;

#[cfg(feature = "embed-assets")]
impl Assets for Embedded {
    fn get(&self, key: &str) -> Option<Asset> {
        let file = SiteFiles::get(key)?;
        Some(Asset {
            etag: hex(&file.metadata.sha256_hash()),
            bytes: file.data,
        })
    }
}

/// The content hash as an entity tag. Hand-rolled because pulling a hex crate
/// in for sixteen characters of table lookup is not a trade.
#[cfg(feature = "embed-assets")]
fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;

    bytes.iter().fold(String::new(), |mut out, byte| {
        // `write!` into a String cannot fail, and the fold is what keeps this
        // free of the `unwrap` the lints forbid.
        let _ = write!(out, "{byte:02x}");
        out
    })
}

/// Whether this binary carries the frontend.
///
/// Read once at startup for the log line, so that "the canvas is blank" has an
/// answer in the first four lines of the server's output rather than in the
/// browser's network tab.
#[must_use]
pub const fn is_embedded() -> bool {
    cfg!(feature = "embed-assets")
}

/// The embedded answer for a request, if there is one. Always `None` without
/// the feature, which is what makes [`fallback`] collapse to leptos' own
/// handler.
fn embedded(path: &str, headers: &HeaderMap) -> Option<Response<Body>> {
    #[cfg(feature = "embed-assets")]
    {
        respond(&Embedded, path, headers)
    }
    #[cfg(not(feature = "embed-assets"))]
    {
        let _ = (path, headers);
        None
    }
}

/// The response a fallback hands back, boxed because leptos' own handler
/// boxes its own and the two arms have to agree on one type.
type BoxedResponse = Pin<Box<dyn Future<Output = Response<Body>> + Send>>;

/// The router's fallback: embedded static files, then whatever
/// `leptos_axum::file_and_error_handler` would have done.
///
/// Drop-in for that function — same signature, same behaviour for everything
/// it still handles, which is why `main.rs` reads as one word changed.
pub fn fallback<S, IV>(
    shell: impl Fn(LeptosOptions) -> IV + Clone + Send + 'static,
) -> impl Fn(Uri, State<S>, Request<Body>) -> BoxedResponse + Clone + Send + 'static
where
    IV: IntoView + 'static,
    S: Send + Sync + Clone + 'static,
    LeptosOptions: FromRef<S>,
{
    let files = leptos_axum::file_and_error_handler::<S, IV>(shell);
    move |uri: Uri, state: State<S>, request: Request<Body>| {
        if let Some(mut response) = embedded(uri.path(), request.headers()) {
            // A HEAD is the GET's headers and none of its body. Content-Length
            // stays: it is what the request was asking for.
            if request.method() == Method::HEAD {
                *response.body_mut() = Body::empty();
            }
            return Box::pin(async move { response });
        }
        files(uri, state, request)
    }
}

/// The tests that need a built `target/site`, and therefore only run under
/// `cargo test --features embed-assets` after a `cargo leptos build`. They are
/// what proves the embed is wired to the right folder — everything else in
/// this module is tested against the in-memory double below, which by
/// construction cannot notice a wrong path.
#[cfg(all(test, feature = "embed-assets"))]
mod embedded_tests {
    use super::*;

    fn assert_served(path: &str, content_type: &str) {
        let Some(response) = respond(&Embedded, path, &HeaderMap::new()) else {
            panic!("{path} is not in the embedded site — was the frontend built?");
        };
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response
                .headers()
                .get(header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok()),
            Some(content_type)
        );
    }

    /// What the SSR shell links to. A page whose bundle 404s is a blank
    /// canvas, which is the whole failure this module exists to remove.
    #[test]
    fn the_shells_own_files_are_embedded() {
        assert_served("/pkg/kayak.js", "text/javascript; charset=utf-8");
        assert_served("/pkg/kayak.css", "text/css; charset=utf-8");
        // cargo-leptos renames wasm-bindgen's `kayak_bg.wasm` to this, which
        // is the name the shell's hydration script asks for.
        assert_served("/pkg/kayak.wasm", "application/wasm");
    }

    /// The `assets-dir` half: the vendored reference renderer `/api/reference`
    /// loads from this server rather than from a CDN.
    #[test]
    fn the_assets_directory_travels_too() {
        assert_served("/scalar.js", "text/javascript; charset=utf-8");
    }

    #[test]
    fn the_typescript_declarations_are_left_out() {
        assert!(SiteFiles::get("pkg/kayak.d.ts").is_none());
    }

    /// The tag has to be the file's own, or a browser holding last release's
    /// bundle would be told it is current.
    #[test]
    fn every_embedded_file_has_a_distinct_etag() {
        let js = Embedded.get("pkg/kayak.js").map(|asset| asset.etag);
        let css = Embedded.get("pkg/kayak.css").map(|asset| asset.etag);
        assert!(js.is_some() && css.is_some());
        assert_ne!(js, css);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    /// The stand-in for the embed, which is what lets every rule below be
    /// tested under a plain `cargo test` — no WASM toolchain, no
    /// `cargo leptos` run, no `target/site`.
    struct MapAssets(HashMap<&'static str, &'static [u8]>);

    impl MapAssets {
        fn new(files: &[(&'static str, &'static [u8])]) -> Self {
            Self(files.iter().copied().collect())
        }
    }

    impl Assets for MapAssets {
        fn get(&self, key: &str) -> Option<Asset> {
            self.0.get(key).map(|bytes| Asset {
                bytes: Cow::Borrowed(bytes),
                // Length is a fine stand-in for a hash here: the tests care
                // that the tag round-trips, not what it is derived from.
                etag: format!("len{}", bytes.len()),
            })
        }
    }

    fn site() -> MapAssets {
        MapAssets::new(&[
            ("index.html", b"<!DOCTYPE html>"),
            ("pkg/kayak.wasm", b"\0asm plain"),
            ("pkg/kayak.wasm.br", b"brotli"),
            ("pkg/kayak.wasm.gz", b"gzipped"),
            ("scalar.js", b"renderer"),
            ("docs/index.html", b"docs"),
        ])
    }

    fn headers(pairs: &[(&str, &str)]) -> HeaderMap {
        let mut headers = HeaderMap::new();
        for (name, value) in pairs {
            if let (Ok(name), Ok(value)) = (
                header::HeaderName::from_bytes(name.as_bytes()),
                header::HeaderValue::from_str(value),
            ) {
                headers.insert(name, value);
            }
        }
        headers
    }

    fn header_of(response: &Response<Body>, name: header::HeaderName) -> Option<&str> {
        response.headers().get(name).and_then(|v| v.to_str().ok())
    }

    #[test]
    fn a_directory_means_its_index() {
        assert_eq!(key_for("/").as_deref(), Some("index.html"));
        assert_eq!(key_for("").as_deref(), Some("index.html"));
        assert_eq!(key_for("/docs/").as_deref(), Some("docs/index.html"));
    }

    #[test]
    fn a_file_is_its_own_key() {
        assert_eq!(key_for("/scalar.js").as_deref(), Some("scalar.js"));
        assert_eq!(
            key_for("/pkg/kayak.wasm").as_deref(),
            Some("pkg/kayak.wasm")
        );
    }

    /// Refused, never normalised — the rule `persist::save_path` follows, for
    /// the same reason: the path came in over HTTP.
    #[test]
    fn traversal_and_empty_segments_are_refused() {
        assert_eq!(key_for("/../Cargo.toml"), None);
        assert_eq!(key_for("/pkg/../../etc/passwd"), None);
        assert_eq!(key_for("/pkg/./kayak.wasm"), None);
        assert_eq!(key_for("//pkg//kayak.wasm"), None);
        assert_eq!(key_for("/pkg\\kayak.wasm"), None);
    }

    /// The one content type that changes how the browser *loads* the file
    /// rather than how it labels it.
    #[test]
    fn wasm_is_served_as_wasm() {
        assert_eq!(content_type("pkg/kayak.wasm"), "application/wasm");
    }

    #[test]
    fn content_types_cover_what_the_site_holds() {
        assert_eq!(content_type("index.html"), "text/html; charset=utf-8");
        assert_eq!(content_type("pkg/kayak.css"), "text/css; charset=utf-8");
        assert_eq!(content_type("scalar.js"), "text/javascript; charset=utf-8");
        assert_eq!(content_type("favicon.svg"), "image/svg+xml");
        assert_eq!(content_type("noextension"), "application/octet-stream");
    }

    #[test]
    fn brotli_wins_when_both_are_offered() {
        assert_eq!(
            accepted_encodings(Some("gzip, deflate, br")),
            vec![Encoding::Br, Encoding::Gzip]
        );
    }

    /// A `q=0` is the only way a client can refuse one encoding while taking
    /// another, so it has to be read.
    #[test]
    fn a_zero_q_value_is_a_refusal() {
        assert_eq!(accepted_encodings(Some("br;q=0, gzip")), vec![Encoding::Gzip]);
        assert_eq!(accepted_encodings(Some("gzip;q=0")), vec![]);
        assert_eq!(accepted_encodings(None), vec![]);
    }

    #[test]
    fn a_wildcard_accepts_both() {
        assert_eq!(
            accepted_encodings(Some("*")),
            vec![Encoding::Br, Encoding::Gzip]
        );
        assert_eq!(accepted_encodings(Some("*;q=0")), vec![]);
    }

    #[test]
    fn a_file_is_served_with_its_type_and_an_etag() {
        let Some(response) = respond(&site(), "/scalar.js", &headers(&[])) else {
            panic!("scalar.js should be served");
        };
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            header_of(&response, header::CONTENT_TYPE),
            Some("text/javascript; charset=utf-8")
        );
        assert_eq!(header_of(&response, header::ETAG), Some("\"len8\""));
        assert_eq!(header_of(&response, header::CONTENT_LENGTH), Some("8"));
        assert_eq!(
            header_of(&response, header::VARY),
            Some("accept-encoding"),
            "the answer depends on accept-encoding even when nothing was encoded"
        );
        assert_eq!(header_of(&response, header::CONTENT_ENCODING), None);
    }

    #[test]
    fn a_directory_request_serves_the_index() {
        let Some(response) = respond(&site(), "/", &headers(&[])) else {
            panic!("/ should be served");
        };
        assert_eq!(
            header_of(&response, header::CONTENT_TYPE),
            Some("text/html; charset=utf-8")
        );
    }

    /// The content type comes from the *uncompressed* name: `kayak.wasm.br` is
    /// a brotli-encoded wasm file, not a file of type `br`.
    #[test]
    fn a_precompressed_variant_keeps_the_real_content_type() {
        let Some(response) = respond(
            &site(),
            "/pkg/kayak.wasm",
            &headers(&[("accept-encoding", "gzip, br")]),
        ) else {
            panic!("the wasm bundle should be served");
        };
        assert_eq!(header_of(&response, header::CONTENT_ENCODING), Some("br"));
        assert_eq!(
            header_of(&response, header::CONTENT_TYPE),
            Some("application/wasm")
        );
        assert_eq!(header_of(&response, header::CONTENT_LENGTH), Some("6"));
    }

    #[test]
    fn gzip_is_used_when_brotli_is_not_accepted() {
        let Some(response) = respond(
            &site(),
            "/pkg/kayak.wasm",
            &headers(&[("accept-encoding", "gzip")]),
        ) else {
            panic!("the wasm bundle should be served");
        };
        assert_eq!(header_of(&response, header::CONTENT_ENCODING), Some("gzip"));
        assert_eq!(header_of(&response, header::CONTENT_LENGTH), Some("7"));
    }

    #[test]
    fn a_client_taking_no_encoding_gets_the_file_itself() {
        let Some(response) = respond(&site(), "/pkg/kayak.wasm", &headers(&[])) else {
            panic!("the wasm bundle should be served");
        };
        assert_eq!(header_of(&response, header::CONTENT_ENCODING), None);
        assert_eq!(header_of(&response, header::CONTENT_LENGTH), Some("10"));
    }

    /// A file with no precompressed variant is served as-is however much the
    /// client would have taken one.
    #[test]
    fn a_file_with_no_variant_is_served_plain() {
        let Some(response) = respond(
            &site(),
            "/scalar.js",
            &headers(&[("accept-encoding", "br, gzip")]),
        ) else {
            panic!("scalar.js should be served");
        };
        assert_eq!(header_of(&response, header::CONTENT_ENCODING), None);
    }

    #[test]
    fn a_matching_etag_is_a_304_with_no_body() {
        let etag = "\"len8\"";
        let Some(response) = respond(&site(), "/scalar.js", &headers(&[("if-none-match", etag)]))
        else {
            panic!("scalar.js should be served");
        };
        assert_eq!(response.status(), StatusCode::NOT_MODIFIED);
        assert_eq!(header_of(&response, header::CONTENT_LENGTH), None);
        assert_eq!(header_of(&response, header::ETAG), Some(etag));
    }

    #[test]
    fn a_stale_etag_gets_the_file() {
        let Some(response) = respond(
            &site(),
            "/scalar.js",
            &headers(&[("if-none-match", "\"len999\"")]),
        ) else {
            panic!("scalar.js should be served");
        };
        assert_eq!(response.status(), StatusCode::OK);
    }

    /// The 304 has to be decided *per encoding*: the brotli variant and the
    /// plain file are different entities and carry different tags.
    #[test]
    fn the_etag_is_the_served_variants_own() {
        let Some(response) = respond(
            &site(),
            "/pkg/kayak.wasm",
            &headers(&[("accept-encoding", "br")]),
        ) else {
            panic!("the wasm bundle should be served");
        };
        assert_eq!(header_of(&response, header::ETAG), Some("\"len6\""));
    }

    /// `None` is the contract with `fallback` — "not a file", so that leptos'
    /// handler renders the shell. Anything else here would turn every unknown
    /// route into a 404 from this module.
    #[test]
    fn an_unknown_path_is_not_this_modules_business() {
        assert!(respond(&site(), "/nope.js", &headers(&[])).is_none());
        assert!(respond(&site(), "/api/pipelines", &headers(&[])).is_none());
        assert!(respond(&site(), "/../Cargo.toml", &headers(&[])).is_none());
    }
}
