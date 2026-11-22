#!/bin/sh
set -eu

cd "$(dirname "$0")/.."
toolchain=${RUST_NIGHTLY:-nightly}
seconds=${FUZZ_SECONDS:-300}
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
