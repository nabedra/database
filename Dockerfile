FROM rust:1.94.1-bookworm AS build
WORKDIR /src
COPY server/ ./server/
RUN cargo build --manifest-path server/Cargo.toml --locked --release --bin meshdb-server

FROM python:3.11-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY orm/ /opt/meshdb/orm/
RUN python -m pip install --no-cache-dir /opt/meshdb/orm && useradd --create-home meshdb
COPY --from=build /src/server/target/release/meshdb-server /usr/local/bin/meshdb-server
USER meshdb
WORKDIR /home/meshdb
ENTRYPOINT ["meshdb"]
CMD ["serve", "--config", "/etc/meshdb/meshdb.json"]
