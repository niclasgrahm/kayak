# The sample everything is tried against: pipelines, the connections they name,
# and the secrets those resolve against. One directory so the set travels
# together — the connections and layout files are *derived* from the config's
# path, so they only find each other when they sit side by side.
example := "example_config"

# Where file outputs are allowed to write in dev. The server refuses to build a
# file output without this, on purpose — see "file output" in the readme — so a
# dev server passes one to make the component usable without ceremony. It is
# gitignored: the sample writes pipeline data, not fixtures.
data := "dev_data"

# `cargo leptos watch` is what sets LEPTOS_SITE_ADDR and builds the WASM, which
# is why this doesn't just `cargo run` — that binds :3000 and serves no
# frontend. Everything the sample connects to comes up with `docker compose up`;
# without it the nats, kafka and postgres pipelines report connection errors on
# their cards and the rest still runs.
#
# It runs **with authentication on** — sign in as `niclas` / `hunter2`, or as
# `viewer` / `hunter2` to see what a read-only account gets. The password comes
# from the secrets file the same recipe creates, so there is nothing to set up.
# Developing against the open path would leave the login page and the role
# checks as the one part of the UI nobody ever looks at; `dev-yaml` below is
# the escape hatch when a login is in the way. See {{example}}/server.yaml.

# Kills whatever is bound to :6767. `cargo leptos watch` sometimes gets left
# running detached (e.g. after a terminal closes without Ctrl-C), and a
# leftover holding the port makes the next `dev` fail with a bind error
# instead of a useful message — so both dev recipes clear it first.

# free :6767 by killing whatever process is bound to it
kill-dev-server:
  #!/usr/bin/env sh
  pid=$(lsof -ti tcp:6767)
  if [ -n "$pid" ]; then
    echo "killing process on :6767 ($pid)"
    kill -9 $pid
  fi

# dev server on :6767, hot reload, against example_config/ — asks for a login
dev: kill-dev-server secrets
  cargo leptos watch -- --config {{example}}/config.json --secrets {{example}}/secrets.json --data-dir {{data}} --server-config {{example}}/server.yaml

# The same graph in its other spelling, and deliberately *without* a
# `--server-config`: it is what this recipe is for (the YAML config path) plus
# the one way to get at the canvas without signing in, which is worth having
# when the thing being worked on is not the login.

# the same graph in its other spelling, no login — worth running now and then so the YAML path doesn't rot
dev-yaml: kill-dev-server secrets
  cargo leptos watch -- --config {{example}}/config.yaml --secrets {{example}}/secrets.json --data-dir {{data}}

# It is gitignored — nothing named secrets.json is committed, whatever is in it
# — so a fresh checkout has the example and not the file. Copying it here is
# what keeps `just dev` a single command anyway.
#
# Filling in *missing* keys rather than only creating the file is the part that
# earns its keep: a checkout that ran `just dev` before a new secret was added to
# the sample has a file that no longer starts the sample, and the failure names
# the secret without saying that the fix is to go and diff two files by hand.
# Values already in the file always win — someone may have put a real credential
# there, and clobbering it back to "hunter2" would be a worse bug than the one
# this fixes.

# create or top up example_config/secrets.json from the example
secrets:
  #!/usr/bin/env python3
  import json, pathlib
  example = pathlib.Path("{{example}}")
  target, source = example / "secrets.json", example / "secrets.example.json"
  wanted = json.loads(source.read_text())
  if not target.exists():
      target.write_text(json.dumps(wanted, indent=2) + "\n")
      print(f"created {target} from the example")
  else:
      have = json.loads(target.read_text())
      missing = {k: v for k, v in wanted.items() if k not in have}
      if missing:
          target.write_text(json.dumps({**have, **missing}, indent=2) + "\n")
          print(f"added {', '.join(missing)} to {target} from the example")

lint:
  cargo clippy --workspace --all-targets -- -D warnings

# unit + integration tests; no network, no NATS, no running server needed
# --workspace so the frontend's canvas-geometry tests run too
test:
  cargo test --workspace --all-targets

# what CI runs — run this before pushing
ci: lint test

# `main` is only moved by merging a pull request. GitHub won't enforce that on a
# private repo on this plan, so the rule lives in a hook instead — see
# `.githooks/pre-push`. The hooks are committed rather than left in one clone's
# `.git`, which is what `core.hooksPath` is for; run this once per checkout.

# install the repo's git hooks (once per checkout)
hooks:
  git config core.hooksPath .githooks
  @echo "hooks installed: $(git config core.hooksPath)"

# start a branch for a change — `just branch feat/http-hmac`
branch NAME:
  git switch -c {{NAME}}

# push the current branch and open its pull request
pr *ARGS:
  git push -u origin HEAD
  gh pr create --fill {{ARGS}}

# smoke test against a server that is already running on :6767
test-http:
  hurl --test hurl/tests/*.hurl

start-baseline:
  hurl hurl/create_baseline.hurl

# Deliberately *not* part of `ci` — a minute-long sweep in the pre-push loop is
# a minute-long sweep people learn to skip. It is a release build for the same
# reason a baseline refuses to save a debug one: a debug number measures the
# optimiser's absence, and some of the hot paths here inline away entirely
# under --release.
#
#   just bench                      the suite, as a table
#   just bench --compare            ... and the deltas against this machine's baseline
#   just bench --save               ... and record this run as that baseline
#   just bench --filter pipelines   just the multi-pipeline rows
#   just bench --duration 20        longer windows, less noise
#
# See website/contributing/benchmarking.md for what the numbers mean.

# throughput sweep over the run loop — no server, no broker, no filesystem
bench *ARGS:
  cargo run --release -p kayak-bench -- {{ARGS}}

# The doc site under `website/`: prose written by hand, every reference table
# generated. `just docs` regenerates the tables — from the config schemas, the
# metadata declarations and the endpoint table — and is what you run after
# adding a component, a field or an endpoint. Nothing about the site is edited
# to make a new component appear; the sidebar comes from the same run.
#
# The generated files are committed, so the site builds on a machine with no
# Rust toolchain, and `kayak-docsgen`'s test suite fails when they have drifted
# from the source — which is how a stale reference becomes a red `just ci`
# rather than something someone notices in six months.

# regenerate the doc site's reference tables from the schemas
docs:
  cargo run -p kayak-docsgen -- website

# the doc site on :5173, hot reload (needs `npm install` in website/ once)
docs-dev: docs
  cd website && npm run dev

# production build of the doc site into website/.vitepress/dist
docs-build: docs
  cd website && npm ci && npm run build
