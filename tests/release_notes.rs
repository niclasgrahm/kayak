//! The GitHub Release notes, pinned.
//!
//! The release workflow runs `scripts/release-notes.sh` for a pushed tag, and
//! that is the only place it would otherwise run: a heading the script can't
//! read, or a release PR that bumped the version without filing the changelog,
//! would surface as a failed job after the tag is already public. So the
//! script is run here, against every version the changelog has.

use std::process::{Command, Output};

use anyhow::{Context, Result, ensure};

fn notes(version: &str) -> Result<Output> {
    Command::new("sh")
        .args(["scripts/release-notes.sh", version, "owner/repo"])
        .output()
        .context("sh should run the release-notes script")
}

/// Every `## X.Y.Z — date` heading, newest first, as the file has them.
fn changelog_versions() -> Result<Vec<String>> {
    let changelog = std::fs::read_to_string("CHANGELOG.md")?;
    Ok(changelog
        .lines()
        .filter_map(|line| line.strip_prefix("## "))
        .filter_map(|rest| rest.split_whitespace().next())
        .filter(|version| version.split('.').all(|n| n.parse::<u32>().is_ok()))
        .map(str::to_string)
        .collect())
}

#[test]
fn every_released_version_makes_notes() -> Result<()> {
    let versions = changelog_versions()?;
    ensure!(versions.len() > 1, "the changelog should list released versions");
    for (index, version) in versions.iter().enumerate() {
        let out = notes(version)?;
        ensure!(
            out.status.success(),
            "{version}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        let text = String::from_utf8(out.stdout)?;

        // the section's own subheadings are `###`; a `##` other than the one
        // the script adds means the next version's section leaked in
        let headings: Vec<_> = text.lines().filter(|l| l.starts_with("## ")).collect();
        ensure!(headings == ["## Images"], "{version}: headings {headings:?}");
        ensure!(
            !text.starts_with('\n') && text.contains(&format!("ghcr.io/owner/repo:{version}")),
            "{version}: {text}"
        );

        // a compare link to the version below, and none under the oldest
        match versions.get(index + 1) {
            Some(older) => {
                let link = format!("/compare/v{older}...v{version}");
                ensure!(text.contains(&link), "{version}: no {link} in {text}");
            }
            None => ensure!(!text.contains("/compare/"), "{version}: {text}"),
        }
    }
    Ok(())
}

/// What a release PR has to do besides bumping the version: file the
/// changelog under it. Without this the tag would be pushed and only then
/// fail to produce notes.
#[test]
fn the_crate_version_has_a_changelog_section() -> Result<()> {
    let version = env!("CARGO_PKG_VERSION");
    let out = notes(&format!("v{version}"))?;
    ensure!(
        out.status.success(),
        "CHANGELOG.md has no `## {version}` section: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    Ok(())
}

#[test]
fn unreleased_and_unknown_versions_are_refused() -> Result<()> {
    for version in ["Unreleased", "9.9.9", "0.2.", ""] {
        let out = notes(version)?;
        ensure!(!out.status.success(), "{version:?} should be refused");
        ensure!(out.stdout.is_empty(), "{version:?} printed notes");
    }
    Ok(())
}
