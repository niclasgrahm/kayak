//! What keeps `website/` from drifting away from the source it is generated
//! from.
//!
//! Two things are checked, and they are the two ways this site can lie. The
//! reference tables are committed rather than built on a docs machine, so
//! something has to say when they no longer match the schemas — that is
//! [`the_committed_reference_matches_the_schemas`], and `just docs` is its
//! fix. And the config sample on the front page is the first thing anyone
//! copies, so it is deserialized here with the same types the server uses.

use std::path::{Path, PathBuf};

/// The site root, relative to this crate.
fn website() -> PathBuf {
    let crate_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
    let Some(workspace) = crate_dir.parent() else {
        panic!("{} has no parent directory", crate_dir.display())
    };
    workspace.join("website")
}

#[test]
fn the_committed_reference_matches_the_schemas() {
    let root = website();
    let mut stale = Vec::new();

    for file in kayak_docsgen::files() {
        let path = root.join(&file.path);
        match std::fs::read_to_string(&path) {
            Ok(committed) if committed == file.contents => {}
            Ok(_) => stale.push(format!("{} has drifted", file.path)),
            Err(_) => stale.push(format!("{} is missing", file.path)),
        }
    }

    assert!(
        stale.is_empty(),
        "the doc site's generated reference is out of date — run `just docs`:\n  {}",
        stale.join("\n  ")
    );
}

/// The config on the front page is the first one anyone copies, so it is
/// deserialized with the types the server itself deserializes into. A sample
/// that no longer parses is a broken first five minutes.
#[test]
fn the_getting_started_config_is_one_kayak_would_accept() {
    let path = website().join("getting-started.md");
    let Ok(page) = std::fs::read_to_string(&path) else {
        panic!("{} is missing", path.display())
    };

    let sample = json_blocks(&page);
    assert!(!sample.is_empty(), "no json sample on the getting started page");

    for block in sample {
        // the same parser the server loads a config file with, so both
        // spellings of the file are accepted here exactly as they are there.
        let parsed = kayak::persist::parse(&block, kayak_core::ConfigFormat::Json);
        assert!(
            parsed.is_ok(),
            "a json sample on the getting started page is not a config kayak accepts: {:?}\n{block}",
            parsed.err()
        );
    }
}

/// Every fenced `json` block in a page. Deliberately not a markdown parser:
/// the pages are ours, and a fence is a fence.
fn json_blocks(page: &str) -> Vec<String> {
    let mut blocks = Vec::new();
    let mut current: Option<String> = None;

    for line in page.lines() {
        match (&mut current, line.trim_end()) {
            (None, "```json") => current = Some(String::new()),
            (Some(_), "```") => {
                if let Some(block) = current.take() {
                    blocks.push(block);
                }
            }
            (Some(block), line) => {
                block.push_str(line);
                block.push('\n');
            }
            (None, _) => {}
        }
    }

    blocks
}
