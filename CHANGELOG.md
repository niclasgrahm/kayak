# changelog

Notable changes, newest first. Versions are the git tags the container images
are published under — see "running it" in the readme.

kayak is pre-1.0, so the middle number is the one that moves when something
breaks: a change that stops an existing config file loading is a `0.x` bump,
and everything else is a `0.x.y`. What "notable" means here is *what an
operator would want to know before upgrading* — a behaviour change, a new
component, a default that moved. Refactors and internal work are in the git
history, not here.

## 0.1.1 — 2026-08-18

### Fixed

- **The server now shuts down when it is asked to.** `SIGTERM` and `SIGINT`
  were both unhandled, so nothing ran on the way out. That mattered most for
  outputs holding an unfinished part: a `file` output never closed its
  `json_array`, leaving a file no reader could parse, and the `s3` output lost
  its buffered part **outright** — an object store has no append, so a part
  that has not rotated yet exists nowhere but in memory.

  Under docker it was worse. The image's `ENTRYPOINT` is the binary, so kayak
  runs as pid 1, and pid 1 has no default action for a signal it has not
  handled: `docker stop` was ignored, and every container stop was a
  ten-second wait for a `SIGKILL`.

  Stopping is now ordered — new connections refused, `/events` streams ended,
  open requests drained, then the pipelines cancelled and awaited so every
  output gets its `finish`. Bounded at ten seconds for the drain and five for
  the run loops, after which it says so in the log and carries on stopping. A
  second signal still kills it immediately. A shutdown never writes the config
  file: unsaved changes are still unsaved after a restart.

  See "shutting down" in the deployment guide.

### Security

- `h2` bumped to 0.4.16 for [RUSTSEC-2026-0258], a transitive dependency (via
  hyper) that queued empty HTTP/2 DATA frames without limit — unbounded memory,
  or a panic on length overflow, against a server whose streams are not being
  drained. Low severity upstream.

[RUSTSEC-2026-0258]: https://rustsec.org/advisories/RUSTSEC-2026-0258

### Documentation

- The readme is written for people running kayak rather than for people working
  on it; the contributor material moved to the doc site.

## 0.1.0 — 2026-08-18

First public release. The graph-based stream processor, its canvas UI and the
generated reference, published as `ghcr.io/niclasgrahm/kayak` for both
`linux/amd64` and `linux/arm64`.
