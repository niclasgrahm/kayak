//! Decides which directory `embed-assets` compiles into the binary, and
//! refuses to build if it holds no site. See `src/site.rs` and
//! `src/embed_check.rs`.
//!
//! Without the feature this does nothing but tell cargo when to ask again.

use std::path::PathBuf;

#[path = "src/embed_check.rs"]
mod embed_check;

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=src/embed_check.rs");
    println!("cargo:rerun-if-env-changed=KAYAK_SITE_DIR");
    if std::env::var_os("CARGO_FEATURE_EMBED_ASSETS").is_none() {
        return;
    }

    let manifest = PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").unwrap_or_default());
    // `join` with an absolute path is that path, so `KAYAK_SITE_DIR` may be
    // either; relative is relative to this crate, as rust-embed's own
    // `folder` would be.
    let dir =
        manifest.join(std::env::var_os("KAYAK_SITE_DIR").unwrap_or_else(|| "target/site".into()));
    // A directory here is scanned recursively, so a rebuilt bundle re-embeds.
    println!("cargo:rerun-if-changed={}", dir.display());

    let missing = embed_check::missing(&dir);
    assert!(
        missing.is_empty(),
        "embed-assets: {} has no built site (missing {}). Build the frontend first and \
         point KAYAK_SITE_DIR at a copy of it — `just build` does both. A single \
         `cargo leptos build --bin-features embed-assets` compiles the server while the \
         frontend is still being built, so it cannot be relied on to fill the directory.",
        dir.display(),
        missing.join(", "),
    );
    // What `#[folder = "$KAYAK_SITE_DIR"]` in `src/site.rs` reads: the
    // directory just checked, never one resolved a second time.
    println!("cargo:rustc-env=KAYAK_SITE_DIR={}", dir.display());
}
