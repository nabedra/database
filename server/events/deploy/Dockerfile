# syntax=docker/dockerfile:1
FROM rust:1.94.1-bookworm AS build
ARG BUILD_JOBS=2
# Build context: `server`. Events builds alone with `--manifest-path`; it needs only its
# own sources and the in-tree storage engine.
WORKDIR /server

# Dependency layer: manifests and lock file only, with stub crate roots, so a source
# change reuses it (and a `type=gha` cache keeps it between CI runs). The workspace
# crates' stub outputs are cleaned so the real sources always rebuild. The dev-only
# `nabedra-check` path dev-dependency is never built here; Cargo needs only its manifest
# and an empty crate root, which stay in place.
COPY kv/Cargo.toml ./kv/Cargo.toml
COPY check/Cargo.toml ./check/Cargo.toml
COPY events/Cargo.toml events/Cargo.lock ./events/
RUN mkdir -p kv/src check/src events/src /out/timings && \
    : > kv/src/lib.rs && : > check/src/lib.rs && : > events/src/lib.rs && echo 'fn main() {}' > events/src/main.rs && \
    cargo build --locked --release --jobs "${BUILD_JOBS}" --timings \
        --manifest-path events/Cargo.toml --bin nabedra-events && \
    cargo clean --offline --locked --release --manifest-path events/Cargo.toml -p nabedra-events -p nabedra-kv && \
    mv events/target/cargo-timings/cargo-timing.html /out/timings/dependencies.html && \
    rm -rf events/target/cargo-timings kv/src events/src

# The real sources. The layer keeps only the binary and the timing report.
COPY kv/src ./kv/src
COPY events/src ./events/src
RUN cargo build --locked --release --jobs "${BUILD_JOBS}" --timings \
        --manifest-path events/Cargo.toml --bin nabedra-events && \
    install -D -m 0755 events/target/release/nabedra-events /opt/events/bin/nabedra-events && \
    mv events/target/cargo-timings/cargo-timing.html /out/timings/events.html && \
    rm -rf events/target

# `docker buildx build --target timings --output type=local,dest=<dir>` exports the
# Cargo `--timings` reports of both build steps.
FROM scratch AS timings
COPY --from=build /out/timings/ /

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 events \
    && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin events \
    && install -d -o events -g events /var/lib/nabedra-events
COPY --from=build /opt/events/bin/nabedra-events /usr/local/bin/nabedra-events
EXPOSE 8080 8081
USER 10001:10001
ENTRYPOINT ["/usr/local/bin/nabedra-events"]
CMD ["serve", "--config", "/etc/nabedra-events/broker.json"]
