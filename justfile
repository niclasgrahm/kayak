lint:
  cargo clippy --all-targets -- -D warnings

# unit + integration tests; no network, no NATS, no running server needed
test:
  cargo test --all-targets

# what CI runs — run this before pushing
ci: lint test

# smoke test against a server that is already running on :6767
test-http:
  hurl --test hurl/tests/*.hurl

start-baseline:
  hurl hurl/create_baseline.hurl
