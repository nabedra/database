# Valkey reference server and installed clients for the paired benchmark.
FROM rust:1.94.1-bookworm AS valkey
ARG VALKEY_MAKE_JOBS=2
WORKDIR /valkey
RUN set -eux; \
    curl -fLsS https://codeload.github.com/valkey-io/valkey/tar.gz/refs/tags/8.1.11 -o /tmp/valkey.tar.gz; \
    echo 'db3642dfc2fb24e42313b9e9c1557cf0160acce3bf2cd366ce654ce291133b50  /tmp/valkey.tar.gz' | sha256sum -c; \
    tar xzf /tmp/valkey.tar.gz --strip-components=1; \
    make -j"$VALKEY_MAKE_JOBS" BUILD_TLS=yes

FROM rust:1.94.1-bookworm AS orm
ARG TARGETARCH
RUN set -eux; \
    case "$TARGETARCH" in \
      amd64) arch=x64; sha=c0649af18e6a24f6fe5535a3e86b341dd49a8e71117c8b68bde973ef834f16f2 ;; \
      arm64) arch=arm64; sha=0b2d9f564b6594222a62c82e1df2efe119dd4a4aff29644f4dd325bf360b6bcc ;; \
      *) exit 2 ;; \
    esac; \
    curl -fLsS https://nodejs.org/dist/v22.19.0/node-v22.19.0-linux-"$arch".tar.xz -o /tmp/node.tar.xz; \
    echo "$sha  /tmp/node.tar.xz" | sha256sum -c; \
    mkdir /node; tar xJf /tmp/node.tar.xz -C /node --strip-components=1
ENV PATH=/node/bin:$PATH
WORKDIR /tmp/orm
COPY orm/typescript ./
RUN npm ci && mkdir /packages && npm pack --pack-destination /packages
WORKDIR /drivers
RUN npm install --omit=optional /packages/nabedra-*.tgz

FROM python:3.11-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends openssl util-linux && rm -rf /var/lib/apt/lists/*
COPY --from=valkey /valkey/src/valkey-server /usr/local/bin/valkey-server
COPY --from=orm /node/bin/node /usr/local/bin/node
COPY --from=orm /drivers/node_modules /drivers/node_modules
WORKDIR /drivers
COPY orm/python /tmp/orm-python
RUN python -m pip install --no-cache-dir 'redis==6.4.0' 'hiredis==3.2.1' /tmp/orm-python \
    && rm -rf /tmp/orm-python
COPY scripts/cache-benchmark/driver.py scripts/cache-benchmark/driver.mjs scripts/cache-benchmark/run.py ./
COPY scripts/cache-benchmark/cluster.py ./
ENTRYPOINT ["python", "/drivers/run.py", "--valkey", "/usr/local/bin/valkey-server"]
