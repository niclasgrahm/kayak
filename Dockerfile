from rust:1-bookworm as builder
run rustup target add wasm32-unknown-unknown

RUN rustup target add wasm32-unknown-unknown
RUN curl -L --proto '=https' --tlsv1.2 -sSf \
  https://raw.githubusercontent.com/cargo-bins/cargo-binstall/main/install-from-binary-release.sh | bash \
  && cargo binstall cargo-leptos -y

workdir /app
copy . .
RUN --mount=type=cache,target=/usr/local/cargo/registry \
  --mount=type=cache,target=/app/target \
  cargo leptos build --release \
  && cp target/release/streamer /app/streamer-bin \
  && cp -r target/site /app/site

# ---- Stage 2: runtime ----
FROM debian:bookworm-slim AS runtime

RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates \
  && rm -rf /var/lib/apt/lists/*

RUN useradd -m app
USER app
WORKDIR /app

COPY --from=builder /app/streamer-bin /app/streamer
COPY --from=builder /app/site /app/site
COPY config.json /app/config.json

ENV LEPTOS_SITE_ROOT="/app/site" \
  LEPTOS_SITE_PKG_DIR="pkg" \
  LEPTOS_SITE_ADDR="0.0.0.0:6767" \
  LEPTOS_ENV="PROD"

EXPOSE 6767
CMD ["/app/streamer", "--config", "/app/config.json"]
