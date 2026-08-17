# security policy

## reporting a vulnerability

**Please don't open a public issue.**

Use GitHub's private vulnerability reporting, which is enabled on this
repository: go to the [Security tab](https://github.com/niclasgrahm/kayak/security)
and choose **Report a vulnerability**. It opens a private thread visible only to
the maintainers, and it works even if you have no other way to reach me.

Useful things to include: what an attacker can do, how to reproduce it, the
version or commit you saw it on, and whether you've published anything about it
already.

kayak is a small project with one maintainer. Expect an acknowledgement within a
week; a fix depends on what it is. You'll be credited in the release notes
unless you'd rather not be.

## what is in scope

kayak is a server that runs pipelines described by a config file and exposes an
HTTP API and a web UI. In scope is anything that lets someone:

- reach the API or the UI without the credentials the config requires
- escape one of the two directory boundaries — `--data-dir` for pipeline output,
  the config file's directory for saved configs
- read a secret out of an API response, a log line, an error message or the UI
- post to a pipeline's ingest endpoint without the credential that endpoint
  declares
- reach or crash the server through a config that a legitimate operator would
  reasonably write

## what is not

Some things look like vulnerabilities and are documented decisions. Reporting
them is welcome — you may well have found a case that isn't covered — but expect
them to be closed with a pointer:

- **An operator can make the server do things.** Someone who can write the
  config file can already run pipelines that read and write files, connect to
  databases and execute a `script` transform. The config file is trusted input;
  the boundary is who can supply one, not what one can say.
- **`auth` on the http input is not the server's sign-in**, deliberately. See
  the input's section in `CLAUDE.md`.
- **The ingest endpoint's status codes leak whether a pipeline exists and
  whether it is guarded** (401 / 202 / 404). Known and accepted while the
  credential is per-pipeline.
- **There is no rate limiting** anywhere.
- **`SecurityPolicy::None` on the OPC UA client.** The connection says so rather
  than pretending otherwise.
- **Advisories in the doc site's build toolchain.** `website/` is a VitePress
  site built to static HTML; its dev server never runs in CI or production.

## supported versions

Pre-1.0 and moving quickly: only the latest release gets fixes. `latest` on the
container image is the tip of `main`, not a release — pin a version tag for
anything you depend on.
