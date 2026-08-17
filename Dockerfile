# syntax=docker/dockerfile:1.7
#
# Two stages: a cargo-leptos build (server binary + WASM + assets) and a
# debian-slim runtime that holds the binary and nothing else. The frontend is
# *inside* the binary — see `--bin-features embed-assets` below and
# `src/site.rs` for why — so there is no site directory to copy, and the
# runtime image is one file plus a CA bundle.
#
# The image is the *runtime*, not a deployment: no config is baked in. Started
# bare it serves the UI with an empty graph, which is a working container to
# `docker run` and a working k8s Deployment with no volume. Mount a config and
# name it:
#
#   docker run -p 6767:6767 -v ./pipelines:/kayak ghcr.io/OWNER/kayak \
#     --config /kayak/config.json
#
# The sample graph travels along at /usr/share/kayak/example for a one-command
# tour — see the readme.

ARG RUST_VERSION=1.95
ARG DEBIAN_VERSION=bookworm

# ---- build ----------------------------------------------------------------
FROM rust:${RUST_VERSION}-${DEBIAN_VERSION} AS builder

# rdkafka-sys compiles librdkafka from source; cmake is its build driver and
# the rest is what any C dependency in the tree needs. zlib and TLS are
# vendored (libz-sys, rustls), so there is nothing else to install.
RUN apt-get update \
  && apt-get install -y --no-install-recommends cmake \
  && rm -rf /var/lib/apt/lists/*

RUN rustup target add wasm32-unknown-unknown

# Pinned so a rebuild months from now is the same build. cargo-binstall fetches
# the prebuilt cargo-leptos rather than compiling it, which is minutes saved on
# every cold build.
ARG CARGO_LEPTOS_VERSION=0.3.6
RUN curl -L --proto '=https' --tlsv1.2 -sSf \
  https://raw.githubusercontent.com/cargo-bins/cargo-binstall/main/install-from-binstall-release.sh | bash \
  && cargo binstall --no-confirm cargo-leptos --version ${CARGO_LEPTOS_VERSION}

WORKDIR /app
COPY . .

# Symbols are half the binary and none of them are read in a container; a
# backtrace still carries frame names from the panic handler's own metadata.
ENV RUSTFLAGS="-C strip=symbols"
# The cache mounts make an incremental rebuild cheap locally. They are scoped
# to this Dockerfile, so `target/` never enters a layer — the artifacts are
# copied out to /out while the mount is still there.
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
  --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
  --mount=type=cache,target=/app/target,sharing=locked \
# `--bin-features embed-assets` is what compiles `target/site` into the
# binary. It is safe in one command because cargo-leptos builds the client
# before the server, so the site directory is complete by the time the server
# crate is compiled — `--frontend-only` then `--server-only` is the same thing
# said explicitly, if that ordering ever stops holding.
#
# `--precompress` would work too (the server negotiates br and gzip out of the
# embed), at the cost of carrying three copies of the WASM bundle in the
# binary. Left off: the trade is a deployment's to make.
  cargo leptos build --release --bin-features embed-assets \
  && mkdir -p /out \
  && cp target/release/kayak /out/kayak

# ---- runtime --------------------------------------------------------------
FROM debian:${DEBIAN_VERSION}-slim AS runtime

# Only for outbound TLS (an http input against an https url). Everything else
# the binary needs is in the base image.
RUN apt-get update \
  && apt-get install -y --no-install-recommends ca-certificates \
  && rm -rf /var/lib/apt/lists/*

# A fixed uid, so a k8s `runAsNonRoot` pod and a `docker run --user` land on
# the same identity as the image's default, and a mounted volume can be
# chowned to a number that is stable across rebuilds.
RUN groupadd --gid 10001 kayak \
  && useradd --uid 10001 --gid 10001 --home-dir /kayak --no-create-home kayak

COPY --from=builder /out/kayak /usr/local/bin/kayak
# The AGPL asks for its text to travel with the binary, and this image is the
# binary. Scalar's MIT notice needs no line here: `assets/` is copied into the
# site directory by cargo-leptos and compiled into the binary along with the
# bundle it covers.
COPY LICENSE /usr/share/kayak/LICENSE
# The sample graph and the connections it names. The pair is found by *derived*
# name, so both keep the same stem and the same directory; the layout file
# beside them is what the canvas comes up arranged by.
COPY example_config/config.json example_config/config.connections.json \
  example_config/config.layout.json /usr/share/kayak/example/

# There is no LEPTOS_SITE_ROOT: the site directory does not exist in this
# image, because the binary carries it. What is left is the URL prefix the
# rendered page links its bundle under, which is a path in the page rather than
# a path on disk. The address is 0.0.0.0 because the default (127.0.0.1, from
# Cargo.toml) reaches nothing from outside a container.
ENV LEPTOS_SITE_PKG_DIR="pkg" \
  LEPTOS_SITE_ADDR="0.0.0.0:6767" \
  LEPTOS_ENV="PROD"

# Where a config is expected to be mounted, and what relative paths in one
# resolve against — including a `file` connection's root under --data-dir.
# Owned by the run user because it is also a *write* target: `save config` puts
# the file back beside the one it was loaded from.
RUN install -d -o 10001 -g 10001 /kayak
WORKDIR /kayak
USER 10001:10001

EXPOSE 6767

# ENTRYPOINT rather than CMD so the flags are the container's arguments:
# `docker run kayak --config /kayak/config.json --data-dir /data`. With none of
# them the server starts an empty graph and serves the UI, which is the
# smallest thing that works.
ENTRYPOINT ["/usr/local/bin/kayak"]

# `image.source` is also what links the package to this repository on GHCR,
# so it has to be the real url rather than a description of one.
LABEL org.opencontainers.image.title="kayak" \
  org.opencontainers.image.description="Graph-based stream processing with a web UI" \
  org.opencontainers.image.source="https://github.com/niclasgrahm/kayak" \
  org.opencontainers.image.licenses="AGPL-3.0-or-later"
