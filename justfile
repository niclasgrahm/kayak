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

# dev server on :6767, hot reload, against example_config/
dev: secrets
  cargo leptos watch -- --config {{example}}/config.json --secrets {{example}}/secrets.json --data-dir {{data}}

# the same graph in its other spelling — worth running now and then so the YAML path doesn't rot
dev-yaml: secrets
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

# smoke test against a server that is already running on :6767
test-http:
  hurl --test hurl/tests/*.hurl

start-baseline:
  hurl hurl/create_baseline.hurl
