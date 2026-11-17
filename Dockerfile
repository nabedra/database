ARG NABEDRA_ENGINE=olap
FROM rust:1.94.1-bookworm AS build
ARG NABEDRA_ENGINE=olap
ARG NABEDRA_S3=auto
# RocksDB's Rust binding uses libclang and a C++ compiler.
RUN apt-get update && apt-get install -y --no-install-recommends cmake clang libclang-dev && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY server/ ./server/
RUN case "$NABEDRA_ENGINE" in olap|oltp) ;; *) exit 2 ;; esac && \
    if [ "$NABEDRA_S3" = auto ]; then if [ "$NABEDRA_ENGINE" = olap ]; then NABEDRA_S3=true; else NABEDRA_S3=false; fi; fi && \
    case "$NABEDRA_S3" in true) selected_features="$NABEDRA_ENGINE,s3" ;; false) selected_features="$NABEDRA_ENGINE" ;; *) exit 2 ;; esac && \
    cargo build --manifest-path server/Cargo.toml --locked --release --bin nabedra-db --no-default-features --features "$selected_features" && \
    ./server/target/release/nabedra-db --version | grep -w "$NABEDRA_ENGINE"

FROM python:3.11-slim-bookworm AS runtime
ARG NABEDRA_ENGINE=olap
ARG NABEDRA_S3=auto
LABEL io.nabedra.engine=$NABEDRA_ENGINE io.nabedra.s3=$NABEDRA_S3
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libstdc++6 && rm -rf /var/lib/apt/lists/*
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

FROM ${NABEDRA_ENGINE} AS final
