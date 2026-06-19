lint:
  cargo clippy --all-targets -- -D warnings

test-http:
  hurl --test hurl/tests/*.hurl

start-baseline:
  hurl hurl/create_baseline.hurl
