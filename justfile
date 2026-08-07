# The sample everything is tried against: pipelines, the connections they name,
# and the secrets those resolve against. One directory so the set travels
# together — the connections and layout files are *derived* from the config's
# path, so they only find each other when they sit side by side.
example := "example_config"

# `cargo leptos watch` is what sets LEPTOS_SITE_ADDR and builds the WASM, which
# is why this doesn't just `cargo run` — that binds :3000 and serves no
# frontend. Everything the sample connects to comes up with `docker compose up`;
# without it the nats, kafka and postgres pipelines report connection errors on
# their cards and the rest still runs.

# dev server on :6767, hot reload, against example_config/
dev: secrets
  cargo leptos watch -- --config {{example}}/config.json --secrets {{example}}/secrets.json

# the same graph in its other spelling — worth running now and then so the YAML path doesn't rot
dev-yaml: secrets
  cargo leptos watch -- --config {{example}}/config.yaml --secrets {{example}}/secrets.json

# It is gitignored — nothing named secrets.json is committed, whatever is in it
# — so a fresh checkout has the example and not the file. Copying it here is
# what keeps `just dev` a single command anyway.

# create example_config/secrets.json from the example, if it isn't there yet
secrets:
  #!/usr/bin/env sh
  if [ ! -f {{example}}/secrets.json ]; then
    cp {{example}}/secrets.example.json {{example}}/secrets.json
    echo "created {{example}}/secrets.json from the example"
  fi

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
