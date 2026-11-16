# crates.io release validation

The first Rust package release is `nabedra-database` 0.1.0 under MIT. The
executable is `nabedra-database-server`; the library is `nabedra_database_server`.
The default feature set includes S3. No runtime source code changed.
`axum-server` was updated from the yanked
0.9.1 release to 0.9.2 so fresh dependency resolution remains possible. All
other dependency versions and checksums are unchanged.

Validation ran on 2026-11-16 with Cargo 1.99.1 and rustc 1.99.1 on macOS x86_64.
Cargo commands reused the existing target directory with two build jobs, debug
information disabled, and incremental compilation disabled.

```sh
cargo fmt --manifest-path server/Cargo.toml --all -- --check
cargo test --manifest-path server/Cargo.toml --locked \
  --test cli --test startup --test rename_compatibility
cargo publish --manifest-path server/Cargo.toml --registry crates-io \
  --locked --dry-run --allow-dirty
```

After the dependency update, the targeted test run passed all six tests.
Compilation took 48.49 seconds and the test execution took 4.05 seconds.
These cover command parsing,
invalid startup without AWS access, and persisted storage/template identity
compatibility. The extracted package also built with default S3 features.

The package allowlist includes source, tests, examples, the lockfile, README,
and license. The final archive was checked for dotenv files, build artifacts,
private keys, and the supplied Cargo/AWS credential values. None were present.
The lockfile changes the root package name and ordering, plus the
`axum-server` patch version and checksum. The upstream [0.9.2 changelog](https://github.com/programatik29/axum-server/blob/master/CHANGELOG.md)
records the 0.9.1 yank and removal of its optional ACME feature, which this
package does not enable.

No scheduler acceptance scenarios were rerun for this metadata and packaging
change. This evidence makes no new runtime, cloud, load, or scaling claim.

## Publication status

The authenticated `cargo publish --locked --registry crates-io` attempt built
and verified the committed package in 59.24 seconds. crates.io rejected the
upload with HTTP 400 because the publishing account needs a verified email
address. No version was published and no release tag was created. Set and
verify the account email at <https://crates.io/settings/profile>, then retry
the upload from the release commit.
