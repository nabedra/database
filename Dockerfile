# syntax=docker/dockerfile:1
ARG NABEDRA_ENGINE=olap
FROM rust:1.94.1-bookworm AS build
ARG NABEDRA_ENGINE=olap
ARG NABEDRA_S3=auto
# Optional Cargo job limit for memory-constrained builders; unset uses every CPU.
ARG BUILD_JOBS=
# Storage is the in-tree nabedra-kv engine (Rust only). The only C code is `ring`,
# the only TLS crypto provider, which builds with the base image's gcc through `cc`.
WORKDIR /src/server
RUN case "$NABEDRA_ENGINE" in olap|oltp|cache) ;; *) exit 2 ;; esac && \
    if [ "$NABEDRA_S3" = auto ]; then if [ "$NABEDRA_ENGINE" = olap ]; then NABEDRA_S3=true; else NABEDRA_S3=false; fi; fi && \
    case "$NABEDRA_ENGINE:$NABEDRA_S3" in oltp:true) selected_features=oltp-backup-s3 ;; olap:true) selected_features=olap,s3 ;; *:false) selected_features="$NABEDRA_ENGINE" ;; *) exit 2 ;; esac && \
    echo "$selected_features" > /src/features

# Dependency layer: it depends only on the manifests and the lock file, so a source
# change reuses it (and a `type=gha` cache keeps it between CI runs). Cargo refuses a
# manifest whose declared targets are missing, so every declared test and example gets
# an empty stub. The stubs stay: the image build never compiles them, and keeping them
# means test-only changes do not rebuild the image. The workspace crates' stub outputs
# are cleaned so the real sources always rebuild, whatever their timestamps. The
# dev-only `nabedra-check` member is never built here; it needs only its manifest and an
# empty crate root, which stay in place so the workspace keeps loading.
COPY server/Cargo.toml server/Cargo.lock ./
COPY server/kv/Cargo.toml ./kv/
COPY server/check/Cargo.toml ./check/
COPY server/rt/Cargo.toml ./rt/
RUN mkdir -p src kv/src check/src rt/src tests examples /out/timings && \
    echo 'fn main() {}' > src/main.rs && : > src/lib.rs && : > kv/src/lib.rs && : > check/src/lib.rs && : > rt/src/lib.rs && \
    awk -F'"' '/^\[\[(test|example)\]\]/ { dir = ($0 ~ /test/) ? "tests" : "examples"; next } \
               /^\[/ { dir = "" } \
               dir != "" && /^name *=/ { print dir "/" $2 ".rs" }' Cargo.toml | xargs -r touch && \
    cargo build --locked --release ${BUILD_JOBS:+--jobs "$BUILD_JOBS"} --timings \
        --bin nabedra-db --no-default-features --features "$(cat /src/features)" && \
    cargo clean --offline --locked --release -p nabedra-database -p nabedra-kv -p nabedra-rt && \
    mv target/cargo-timings/cargo-timing.html /out/timings/dependencies.html && \
    rm -rf target/cargo-timings src kv/src rt/src

# The real sources. The layer below keeps only the binary and the timing report, so a
# `mode=max` cache export does not carry the server crate's build outputs.
COPY server/kv/src ./kv/src
COPY server/rt/src ./rt/src
COPY server/src ./src
RUN cargo build --locked --release ${BUILD_JOBS:+--jobs "$BUILD_JOBS"} --timings \
        --bin nabedra-db --no-default-features --features "$(cat /src/features)" && \
    ./target/release/nabedra-db --version | grep -w "$NABEDRA_ENGINE" && \
    install -D -m 0755 target/release/nabedra-db /out/nabedra-db && \
    mv target/cargo-timings/cargo-timing.html /out/timings/server.html && \
    rm -rf target

# `docker buildx build --target timings --output type=local,dest=<dir>` exports the
# Cargo `--timings` reports of both build steps.
FROM scratch AS timings
COPY --from=build /out/timings/ /

# The OLTP image keeps Python: scripts/benchmark_oltp_nabedra.py and the OLTP Docker
# acceptance scripts run their API and driver with `python3` inside it.
FROM python:3.11-slim-bookworm AS runtime
ARG NABEDRA_ENGINE=olap
ARG NABEDRA_S3=auto
LABEL io.nabedra.engine=$NABEDRA_ENGINE io.nabedra.s3=$NABEDRA_S3
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY orm/python/ /opt/nabedra/orm/python/
RUN python -m pip install --no-cache-dir --disable-pip-version-check /opt/nabedra/orm/python && useradd --create-home nabedra
COPY --from=build /out/nabedra-db /usr/local/bin/nabedra-db
USER nabedra
WORKDIR /home/nabedra
FROM runtime AS olap
RUN nabedra-db --version | grep -w olap || { echo 'The olap target requires --build-arg NABEDRA_ENGINE=olap' >&2; exit 2; }
ENTRYPOINT ["nabedra"]
CMD ["database", "serve", "--config", "/etc/nabedra/nabedra-database.json"]

FROM runtime AS oltp
RUN nabedra-db --version | grep -w oltp || { echo 'The oltp target requires --build-arg NABEDRA_ENGINE=oltp' >&2; exit 2; }
ENTRYPOINT ["nabedra-db"]
CMD ["serve"]

FROM debian:bookworm-slim AS cache
LABEL io.nabedra.engine=cache io.nabedra.s3=false
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/* \
    && useradd --create-home nabedra
COPY --from=build /out/nabedra-db /usr/local/bin/nabedra-db
RUN nabedra-db --version | grep -w cache || { echo 'The cache target requires --build-arg NABEDRA_ENGINE=cache' >&2; exit 2; }
USER nabedra
WORKDIR /home/nabedra
ENTRYPOINT ["nabedra-db"]
CMD ["serve", "--config", "/etc/nabedra/cache.toml"]

FROM scratch AS cache-release
COPY --from=cache /usr/local/bin/nabedra-db /nabedra-db

FROM ${NABEDRA_ENGINE} AS final
