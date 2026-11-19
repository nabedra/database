# Standalone Cache acceptance in Docker

The runner starts each server in a Docker container with no external network, a 256 MiB cgroup limit, and a private loopback namespace. It uses TLS on both engines for the stock-client and installed ORM comparisons. The JSON report records the actual versions, CPU affinity, cgroup limit, assertion groups, and peak RSS of each server. A failed group exits nonzero after the other groups run.

Build or supply Linux artifacts for the running Docker platform. From the repository root, these commands prepare the paths used by the runner. They can reuse an existing binary, reference server, or installed-package directory.

```sh
work="$PWD/.nabedra/workflows/native-cache"
mkdir -p "$work"/{benchmark,orm-artifacts-final,orm-docker-final}

docker run --rm -v "$PWD:/source:ro" -v nabedra-native-cache-target:/target \
  -e CARGO_TARGET_DIR=/target -w /source rust:1.94.1-bookworm sh -ec '
    apt-get update
    apt-get install -y --no-install-recommends cmake
    cargo build --locked --release --manifest-path server/Cargo.toml \
      --no-default-features --features cache --bin nabedra-db
  '

docker run --rm -v "$work/benchmark:/output" rust:1.94.1-bookworm sh -ec '
  cd "$(mktemp -d)"
  curl -fsSLO https://codeload.github.com/valkey-io/valkey/tar.gz/refs/tags/8.1.11
  echo "db3642dfc2fb24e42313b9e9c1557cf0160acce3bf2cd366ce654ce291133b50  8.1.11" | sha256sum -c
  tar -xzf 8.1.11 --strip-components=1
  make -j2 BUILD_TLS=yes
  cp src/valkey-server /output/
'

docker run --rm -v "$PWD:/source:ro" -v "$work/orm-artifacts-final:/output" \
  python:3.11-slim-bookworm sh -ec '
    cp -R /source/orm/python /tmp/orm
    python -m pip wheel --no-deps --wheel-dir /output /tmp/orm redis==6.4.0
  '

docker run --rm -v "$PWD:/source:ro" -v "$work/orm-docker-final:/output" \
  node:22.13.1-bookworm-slim sh -ec '
    cp -R /source/orm/typescript /tmp/orm
    cd /tmp/orm
    npm ci --no-audit --no-fund
    mkdir /tmp/packs
    npm pack --pack-destination /tmp/packs
    npm install --prefix /output --omit=optional --no-audit --no-fund \
      /tmp/packs/nabedra-*.tgz redis@5.8.2
    cp "$(command -v node)" /output/node
  '

NABEDRA_CACHE_TEST_CPUS=0-1 scripts/cache-acceptance/standalone.sh
```

Set `NABEDRA_CACHE_TEST_CPUS` to available Docker CPU IDs or omit it. Override an artifact location with `NABEDRA_CACHE_TARGET_VOLUME`, `NABEDRA_CACHE_VALKEY_DIR`, `NABEDRA_CACHE_ORM_ARTIFACTS`, or `NABEDRA_CACHE_ORM_BUNDLE`. The default report is `.nabedra/workflows/native-cache/standalone-acceptance/standalone.json`. Pass an output directory to `standalone.sh` to retain separate runs.

K01 runs redis-py 6.4.0 and node-redis 5.8.2 in RESP2 mode against both servers. The stock Node client requests signed integers as exact decimal strings. The installed Python and TypeScript ORMs run their shared byte, TTL, and signed-integer vectors on both servers. Separate Nabedra groups check TLS hostname verification, authenticated prefixes and commands, ordered fragmented input, connection and handshake admission, bounded output, atomic OOM, and LRU victim behavior. Valkey's eviction test verifies preservation of acknowledged new data and eviction of older data under its own `maxmemory` accounting.

K02 applies concurrent overwrite and delete churn while a client leaves a pipelined reply unread. It checks progress on an independent connection, physical RSS headroom, and pressure recovery after a burst of expirations. Deterministic saturation of the actor queue and cluster-specific assertions require their own fixtures. The standalone run supplies local evidence for these assertions and does not qualify any EC2 release gate.
