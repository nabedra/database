ARG NABEDRA_ENGINE=olap
FROM rust:1.94.1-bookworm AS build
ARG NABEDRA_ENGINE=olap
ARG NABEDRA_S3=auto
# Storage is the in-tree nabedra-kv engine (safe Rust). The only C code is TLS
# (ring, aws-lc-sys), which builds with the base image's gcc through `cc`.
WORKDIR /src
COPY server/ ./server/
RUN case "$NABEDRA_ENGINE" in olap|oltp|cache) ;; *) exit 2 ;; esac && \
    if [ "$NABEDRA_S3" = auto ]; then if [ "$NABEDRA_ENGINE" = olap ]; then NABEDRA_S3=true; else NABEDRA_S3=false; fi; fi && \
    case "$NABEDRA_ENGINE:$NABEDRA_S3" in oltp:true) selected_features=oltp-backup-s3 ;; olap:true) selected_features=olap,s3 ;; *:false) selected_features="$NABEDRA_ENGINE" ;; *) exit 2 ;; esac && \
    cargo build --manifest-path server/Cargo.toml --locked --release --bin nabedra-db --no-default-features --features "$selected_features" && \
    ./server/target/release/nabedra-db --version | grep -w "$NABEDRA_ENGINE"

FROM python:3.11-slim-bookworm AS runtime
ARG NABEDRA_ENGINE=olap
ARG NABEDRA_S3=auto
LABEL io.nabedra.engine=$NABEDRA_ENGINE io.nabedra.s3=$NABEDRA_S3
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY orm/python/ /opt/nabedra/orm/python/
RUN python -m pip install --no-cache-dir /opt/nabedra/orm/python && useradd --create-home nabedra
COPY --from=build /src/server/target/release/nabedra-db /usr/local/bin/nabedra-db
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
COPY --from=build /src/server/target/release/nabedra-db /usr/local/bin/nabedra-db
RUN nabedra-db --version | grep -w cache || { echo 'The cache target requires --build-arg NABEDRA_ENGINE=cache' >&2; exit 2; }
USER nabedra
WORKDIR /home/nabedra
ENTRYPOINT ["nabedra-db"]
CMD ["serve", "--config", "/etc/nabedra/cache.toml"]

FROM scratch AS cache-release
COPY --from=cache /usr/local/bin/nabedra-db /nabedra-db

FROM ${NABEDRA_ENGINE} AS final
