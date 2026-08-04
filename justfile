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
