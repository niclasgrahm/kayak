//! The licence split, pinned.
//!
//! kayak is Apache-2.0 in `kayak-core` and AGPL-3.0-or-later everywhere else,
//! and the reasoning is in `licensing.md`. What can go wrong silently is a
//! *new* workspace member: it inherits nothing, so a crate added without a
//! `license` field ships unlicensed, and one added with the wrong one puts
//! copyleft on the crate clients are meant to build against. Neither shows up
//! in a build, so it shows up here instead.

/// The permissive half — the shared vocabulary, deliberately not copyleft.
const PERMISSIVE: &str = "Apache-2.0";

/// Everything that is the engine, the UI compiled into it, or built against
/// it.
const COPYLEFT: &str = "AGPL-3.0-or-later";

/// The one crate on the permissive side. Adding another is a decision, not an
/// oversight — which is why it is spelled out here rather than inferred.
const PERMISSIVE_CRATES: [&str; 1] = ["kayak-core"];

fn read(path: &str) -> String {
    match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(err) => panic!("{path} should be readable: {err}"),
    }
}

/// Every quoted string on a line — enough to read `members = [...]` and a
/// `license = "..."` without a toml parser. The manifests are ours, so the
/// shapes this can't read are shapes we don't write.
fn quoted(line: &str) -> Vec<&str> {
    line.split('"').skip(1).step_by(2).collect()
}

/// The `license` of a manifest's `[package]` table, or `None` if it declares
/// no licence at all. Scoped to that table so a `license` under some other
/// section can't stand in for the real one.
fn declared_license(manifest: &str) -> Option<&str> {
    let mut in_package = false;
    for line in manifest.lines() {
        let line = line.trim();
        if line.starts_with('[') {
            in_package = line == "[package]";
        } else if in_package
            && line.starts_with("license")
            && let [value] = quoted(line).as_slice()
        {
            return Some(value);
        }
    }
    None
}

/// The workspace members, read from the root manifest rather than listed here
/// — the point of the test is to catch a member nobody thought about.
fn members() -> Vec<String> {
    let root = read("Cargo.toml");
    let Some(line) = root.lines().find(|line| line.trim_start().starts_with("members")) else {
        panic!("the root manifest should declare workspace members");
    };
    quoted(line).into_iter().map(String::from).collect()
}

fn manifest_of(member: &str) -> String {
    read(&format!("{member}/Cargo.toml"))
}

#[test]
fn every_workspace_member_declares_a_licence() {
    let mut manifests = vec![("kayak".to_string(), read("Cargo.toml"))];
    for member in members() {
        let manifest = manifest_of(&member);
        manifests.push((member, manifest));
    }

    assert!(
        manifests.len() > 1,
        "the members list should not have come back empty"
    );

    for (name, manifest) in manifests {
        assert!(
            declared_license(&manifest).is_some(),
            "{name} declares no `license` — see licensing.md"
        );
    }
}

#[test]
fn the_split_is_core_permissive_and_everything_else_copyleft() {
    for member in members() {
        let manifest = manifest_of(&member);
        let expected = if PERMISSIVE_CRATES.contains(&member.as_str()) {
            PERMISSIVE
        } else {
            COPYLEFT
        };
        assert_eq!(
            declared_license(&manifest),
            Some(expected),
            "{member} is on the wrong side of the licence split — see licensing.md"
        );
    }

    assert_eq!(
        declared_license(&read("Cargo.toml")),
        Some(COPYLEFT),
        "the server crate is the copyleft side"
    );
}

#[test]
fn the_licence_texts_are_beside_the_crates_that_claim_them() {
    assert!(
        read("LICENSE").contains("GNU AFFERO GENERAL PUBLIC LICENSE"),
        "the root LICENSE should be the AGPL text"
    );
    assert!(
        read("kayak-core/LICENSE").contains("Apache License"),
        "kayak-core should carry its own Apache text rather than inheriting the root's"
    );
}

/// MIT requires the notice to travel with the code, and an `embed-assets`
/// build puts `assets/` inside the binary — so the licence has to be in
/// `assets/`, not merely somewhere in the repo.
#[test]
fn the_vendored_renderer_carries_its_notice() {
    assert!(
        std::path::Path::new("assets/scalar.js").exists(),
        "the vendored renderer should still be here"
    );
    assert!(
        read("assets/scalar.LICENSE").contains("MIT License"),
        "assets/scalar.LICENSE should hold Scalar's MIT notice"
    );
}
