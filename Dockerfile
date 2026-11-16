FROM rust:1.94.1-bookworm AS build
WORKDIR /src
COPY server/ ./server/
RUN cargo build --manifest-path server/Cargo.toml --locked --release --bin nabedra-db

FROM python:3.11-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY orm/ /opt/nabedra/orm/
RUN python -m pip install --no-cache-dir /opt/nabedra/orm && useradd --create-home nabedra
COPY --from=build /src/server/target/release/nabedra-db /usr/local/bin/nabedra-db
USER nabedra
WORKDIR /home/nabedra
ENTRYPOINT ["nabedra"]
CMD ["database", "serve", "--config", "/etc/nabedra/nabedra-database.json"]
