#!/bin/sh
# The notes of a GitHub Release, written out of CHANGELOG.md: the version's
# section as it stands, then the image tags it publishes, then a compare link
# to the version below it in the changelog. The release workflow runs this for
# a pushed tag; `tests/release_notes.rs` runs it against every version the
# changelog has, so a heading this can't read fails `just ci` rather than a
# release.
#
#   scripts/release-notes.sh 0.2.4 [owner/repo] [CHANGELOG.md]
#
# The version may carry the tag's `v`. A version with no `## <version> — date`
# heading is an error, not an empty release: a release PR that forgot to file
# its changelog should fail loudly.
#
# POSIX sh and awk only — it runs on a bare GitHub runner and on macOS.
set -eu

version=${1:?usage: release-notes.sh VERSION [REPOSITORY] [CHANGELOG]}
version=${version#v}
# `Unreleased` is a heading too, and not something to publish
case $version in
  *[!0-9.]* | .* | *. | *..* | "")
    echo "release-notes: '$version' is not a version (X.Y.Z)" >&2
    exit 1
    ;;
esac
repository=${2:-niclasgrahm/kayak}
changelog=${3:-CHANGELOG.md}

awk -v version="$version" -v repository="$repository" '
  # `## 0.2.4 — 2026-10-31`: the second field is the version.
  /^## / {
    if (found) { previous = $2; exit }
    if ($2 == version) { found = 1; next }
  }
  found { lines[++n] = $0 }
  END {
    if (!found) {
      print "release-notes: CHANGELOG has no section for " version > "/dev/stderr"
      exit 1
    }
    # the blank lines either side of the section are the file'\''s, not its
    first = 1
    while (first <= n && lines[first] ~ /^[[:space:]]*$/) first++
    last = n
    while (last >= first && lines[last] ~ /^[[:space:]]*$/) last--
    for (i = first; i <= last; i++) print lines[i]

    split(version, part, ".")
    print ""
    print "## Images"
    print ""
    print "    ghcr.io/" repository ":" version
    print "    ghcr.io/" repository ":" part[1] "." part[2]
    print ""
    print "`linux/amd64` and `linux/arm64`, built natively."
    if (previous != "") {
      print ""
      print "**Full changelog**: https://github.com/" repository "/compare/v" previous "...v" version
    }
  }
' "$changelog"
