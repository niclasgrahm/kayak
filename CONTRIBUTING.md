# contributing to kayak

Thanks for looking. kayak is early and the surface is still moving, so the most
useful things right now are bug reports, "I tried to do X and couldn't" issues,
and small focused pull requests.

## before you write code

**Open an issue first for anything non-trivial.** Not bureaucracy — a fair
amount of this codebase is built the way it is on purpose, and the reasoning is
written down in [CLAUDE.md](CLAUDE.md) rather than being obvious from the code.
A five-minute conversation will save you from a rewrite of something whose shape
was a decision.

For a new component (an input, transform, output or connection kind), read the
"adding a component" notes in `CLAUDE.md` first. Each one touches about five
places and the compiler names two of them.

## getting set up

You need [Rust](https://rustup.rs), [`just`](https://github.com/casey/just) and
[`cargo-leptos`](https://github.com/leptos-rs/cargo-leptos):

```bash
cargo install cargo-leptos
just hooks     # once per checkout — installs the repo's git hooks
just dev       # the server on :6767 against example_config/
```

Docker is optional and only needed for the pipelines in the sample graph that
talk to real systems (`docker compose up`).

## the two rules

These are not negotiable and they apply to every change, however small.

**1. New code ships with tests.** Any new or changed behaviour — a component, a
handler, a config field, a bug fix — needs a test that fails without the change.
A bug fix without a regression test is not a fix. If something genuinely can't
be tested offline, say so in the pull request and explain why, rather than
skipping it quietly.

[The testing guide](https://propell.dev/kayak/contributing/testing) is worth
reading before you write the first one — the runtime lives in `src/lib.rs` so
`tests/` can reach it, `src/testing.rs` holds the test doubles, and the HTTP
surface is tested through `tower::oneshot` with no socket involved.

**2. `just ci` must be green.** That's `just lint` (clippy `pedantic`, with
`-D warnings`) plus `just test` — the whole suite, not just your new test.

Never disable, `#[ignore]` or weaken an existing test to get to green. If a test
turns out to encode the wrong behaviour, that's a conversation to have in the
issue, not something to edit away.

Lints are strict deliberately: `unwrap_used` and `expect_used` are warnings, and
`clippy.toml` makes them apply in tests too.

## sending a pull request

`main` only moves by merging a pull request — the git hook `just hooks` installs
refuses a direct push, and a branch rule refuses it on the server too.

```bash
just branch feat/what-it-does
# ... work, with tests ...
just ci
just pr
```

Keep the commit message about *why*. The existing history is the house style:
first line short and in the imperative, then prose explaining the reasoning and
anything that was found the hard way.

If you touched a config struct's doc comments, run `just docs` — the doc site's
reference tables are generated from them and committed, and `docsgen`'s tests
fail when the two drift.

## how contributions are licensed

kayak is split: `kayak-core` is Apache-2.0, everything else is
AGPL-3.0-or-later. See [licensing.md](licensing.md) for why.

Contributions are inbound = outbound — a patch is licensed under the licence of
the crate it lands in — **and additionally require signing a
[contributor licence agreement](cla.md)**. The CLA lets kayak be offered under a
commercial licence alongside the AGPL, which is what funds the project; without
it every future licensing decision would need every past contributor's
agreement.

You keep the copyright in your work. You are granting a licence, not signing it
away.

## reporting a security issue

Don't open a public issue. [SECURITY.md](SECURITY.md) has the process.
