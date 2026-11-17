# syntax=docker/dockerfile:1
FROM rust:1.94.1-bookworm AS build
ARG BUILD_JOBS=2
WORKDIR /events
RUN apt-get update \
    && apt-get install -y --no-install-recommends clang libclang-dev cmake pkg-config \
    && rm -rf /var/lib/apt/lists/*
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/events/target,sharing=locked \
    cargo build --locked --release --jobs "${BUILD_JOBS}" --bin nabedra-events \
    && install -D -m 0755 target/release/nabedra-events /opt/events/bin/nabedra-events

FROM debian:bookworm-slim
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libstdc++6 \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 events \
    && useradd --uid 10001 --gid 10001 --no-create-home --shell /usr/sbin/nologin events \
    && install -d -o events -g events /var/lib/nabedra-events
COPY --from=build /opt/events/bin/nabedra-events /usr/local/bin/nabedra-events
EXPOSE 8080 8081
USER 10001:10001
ENTRYPOINT ["/usr/local/bin/nabedra-events"]
CMD ["serve", "--config", "/etc/nabedra-events/broker.json"]
