# syntax=docker/dockerfile:1
FROM rust:1.94.1-bookworm AS build
ARG BUILD_JOBS=2
# Build context: `server`. Events builds alone with `--manifest-path`; it needs only its
# own sources and the in-tree storage engine.
WORKDIR /server
COPY kv/Cargo.toml ./kv/Cargo.toml
COPY kv/src ./kv/src
COPY events/Cargo.toml events/Cargo.lock ./events/
COPY events/src ./events/src
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/server/events/target,sharing=locked \
    cargo build --locked --release --jobs "${BUILD_JOBS}" \
        --manifest-path events/Cargo.toml --bin nabedra-events \
    && install -D -m 0755 events/target/release/nabedra-events /opt/events/bin/nabedra-events

FROM debian:bookworm-slim
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
