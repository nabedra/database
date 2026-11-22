#!/bin/sh
set -eu

cd "$(dirname "$0")/.."
# Keep these pins equal to the fuzz job in .github/workflows/nabedra-build.yml.
toolchain=${RUST_NIGHTLY:-nightly-2026-11-20}
cargo_fuzz_version=0.13.2
seconds=${FUZZ_SECONDS:-300}

if ! cargo "+$toolchain" fuzz --version 2>/dev/null | grep -qx "cargo-fuzz $cargo_fuzz_version"; then
    cargo "+$toolchain" install --locked --version "$cargo_fuzz_version" cargo-fuzz
fi
targets=$(cargo "+$toolchain" fuzz list)
test -n "$targets"

result=0
for target in $targets; do
    case "$target" in
        cache_*) features=cache,libfuzzer ;;
        events_*|olap_*) features=events,olap,libfuzzer ;;
        *) echo "Unknown fuzz target: $target" >&2; exit 1 ;;
    esac
    cargo "+$toolchain" fuzz run "$target" --no-default-features --features "$features" -- "-max_total_time=$seconds" || result=1
done
exit "$result"
