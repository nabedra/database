# Run Nabedra Cache on one host

Nabedra Cache keeps values in RAM. A successful write takes effect on the serving process. Restarting a standalone process starts with empty data. Keep irreplaceable data in an authoritative database.

Build the native cache binary with the cache-only Cargo feature.

```sh
cargo build --locked --release --manifest-path server/Cargo.toml \
  --no-default-features --features cache
server/target/release/nabedra-db --version
server/target/release/nabedra-db serve --config cache-local.toml
```

An explicit plaintext loopback configuration is available for development.

```toml
listen = "127.0.0.1:6379"
plaintext_loopback = true
eviction = "sampled_lru"
```

For a TLS listener, leave `plaintext_loopback` disabled. Set the advertised host name in the certificate and have clients verify it using the issuing CA. Keep the TOML and private key readable only by the service account. The cache resolves relative certificate and key paths beside the configuration file.

```toml
listen = "0.0.0.0:6379"
eviction = "noeviction"

[tls]
certificate = "client-server-chain.pem"
private_key = "client-server-key.pem"

[[users]]
username = "catalog"
password = "replace-with-a-generated-secret"
key_prefixes = ["nb:catalog:"]
commands = ["get", "set", "del", "exists", "mget", "pttl", "pexpire", "persist", "incrby"]
max_connections = 16
```

Use the `[limits]` table to change these defaults.

| Parameter | Default |
| --- | --- |
| `memory_bytes` | 512 MiB of charged budget |
| `data_bytes` | 128 MiB of retained store budget |
| `max_connections` | 64 |
| `max_handshakes` | 16 |
| `max_pending` | 32 |
| `max_frame_bytes`, `max_pipeline_bytes`, `max_reply_bytes` | 1088 KiB each |
| `max_arguments` | 128 |
| `handshake_timeout_ms`, `io_timeout_ms` | 5,000 and 60,000 |

The server checks that the worker budget covers all bounded connections, retained data and a turn reserve before it starts. Set a higher container or systemd RSS limit to allow for allocator fragmentation and process overhead. Supply a container memory limit and disable swap in production. Values can use up to 1 MiB and raw keys up to 512 bytes, subject to the configured frame limit. If a complete command cannot fit the actor queue, the server returns `BUSY` before executing it. In the no-eviction profile, a write that would exceed the store budget returns `OOM`.

```sh
docker build --build-arg NABEDRA_ENGINE=cache --target cache -t nabedra-cache .
docker run --rm --memory=640m --memory-swap=640m \
  -v "$PWD/cache-local.toml:/etc/nabedra/cache.toml:ro" nabedra-cache
```

With the loopback configuration, exercise the endpoint from the container's network namespace. To expose the TLS listener, use the TLS configuration above, mount the certificate material, and limit port access to authorized clients.
