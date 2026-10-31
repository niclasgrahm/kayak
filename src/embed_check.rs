//! Whether a directory holds a built site — the check `build.rs` makes before
//! `embed-assets` is allowed to compile it into the binary.
//!
//! It lives in its own file because it has two users: `build.rs` includes it
//! by path (a build script cannot depend on the crate it builds), and
//! `site.rs`' tests include it the same way, which is what gets it tested
//! under a plain `cargo test`. It uses `std` and nothing else for the first
//! reason.
//!
//! The check exists because an empty directory embeds without complaint: the
//! derive compiles, the binary starts, `is_embedded()` says yes, and every
//! request for the bundle 404s into a blank canvas. A release build that
//! raced the frontend did exactly that.

use std::path::Path;

/// What the SSR shell links to. Without any one of them the page does not
/// hydrate, so a site missing one is not worth shipping.
pub const REQUIRED: [&str; 3] = ["pkg/kayak.js", "pkg/kayak.wasm", "pkg/kayak.css"];

/// The [`REQUIRED`] files `dir` lacks. An empty file counts as missing: it is
/// what a write still in progress looks like.
pub fn missing(dir: &Path) -> Vec<&'static str> {
    REQUIRED
        .iter()
        .copied()
        .filter(|file| {
            !std::fs::metadata(dir.join(file)).is_ok_and(|meta| meta.is_file() && meta.len() > 0)
        })
        .collect()
}
