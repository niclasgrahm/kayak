from rust:1-bookworm as builder
run rustup target add wasm32-unknown-unknown

RUN rustup target add wasm32-unknown-unknown
RUN curl -L --proto '=https' --tlsv1.2 -sSf \
  https://raw.githubusercontent.com/cargo-bins/cargo-binstall/main/install-from-binary-release.sh | bash \
  && cargo install cargo-leptos -y

workdir /app
copy . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
  --mount=type=cache,target=/app/target \
  cargo leptos build --release \
  && cp target/release/kayak /app/kayak-bin \
  && cp -r target/site /app/site

# ---- Stage 2: runtime ----
FROM debian:bookworm-slim AS runtime

RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates \
  && rm -rf /var/lib/apt/lists/*

RUN useradd -m app
USER app
WORKDIR /app

COPY --from=builder /app/kayak-bin /app/kayak
COPY --from=builder /app/site /app/site
# the sample graph and the connections it names; the pair has to keep its
# derived-name relationship, so both land in /app with the same stem
COPY example_config/config.json /app/config.json
COPY example_config/config.connections.json /app/config.connections.json

ENV LEPTOS_SITE_ROOT="/app/site" \
  LEPTOS_SITE_PKG_DIR="pkg" \
  LEPTOS_SITE_ADDR="0.0.0.0:6767" \
  LEPTOS_ENV="PROD"

EXPOSE 6767
# --data-dir because the sample has a file output, and without the flag that one
# pipeline refuses to build and takes the whole load down with it. It resolves
# against WORKDIR, as does the `dev_data/events` root the connection names, so
# the pair lands the same way here as it does under `just dev`. Mount a volume
# over /app/dev_data to keep what it writes.
CMD ["/app/kayak", "--config", "/app/config.json", "--data-dir", "/app/dev_data"]
