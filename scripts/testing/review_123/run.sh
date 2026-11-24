#!/usr/bin/env bash
# Scratch, review of #123, never merged. Prints results; the exit code says only that it ran to the end.
set -uo pipefail
cd "$(git rev-parse --show-toplevel)"
ROOT=$PWD
export RUSTFLAGS="--cfg tokio_unstable" CARGO_TARGET_DIR=$PWD/server/target/sim
case $1 in
own)
    cp scripts/testing/review_123/review_head.rs server/rt/src/sim/review123.rs
    printf '\n#[cfg(test)]\nmod review123;\n' >> server/rt/src/sim.rs
    cd server
    cargo test --locked -j 4 -p nabedra-rt --features sim --lib --no-run 2>&1 | grep -E "^error|-->|Executable"
    B=$(ls -t target/sim/debug/deps/nabedra_rt-* | grep -v '\.d$' | head -1)
    for t in own_a_rename own_b_job own_c_floor own_e_mkfs own_off_by_default; do timeout -k 5 900 "$B" $t --nocapture 2>&1 | grep -E "OWN|test result|panicked|left:|right:|assertion" | cut -c1-400; done ;;
mut) python3 scripts/testing/review_123/mut.py "$ROOT" ;;
p5)
    test_all() { ( cd "$1/server"; export CARGO_TARGET_DIR=$ROOT/server/target/p5
        find rt oltp-storage -name '*.rs' -exec touch {} +
        cargo test --locked -j 4 -p nabedra-oltp-storage --features sim --lib oltp::pool::sim_tests -- --test-threads=1 2>&1 | grep -E "FAILED|^test |test result|^error|panicked|accepted|only a real" | cut -c1-250 ); }
    echo "## head, clean"; test_all "$ROOT"
    python3 - "$ROOT" <<'PY'
import sys
p=sys.argv[1]+"/server/oltp-storage/src/oltp/pool/mount.rs"; s=open(p).read()
a="            if uuid::Uuid::parse_str(&expected.filesystem_uuid).ok() != Some(volume.filesystem_uuid)\n                || (major, minor)"; assert s.count(a)==1
open(p,"w").write(s.replace(a,"            if (major, minor)"))
PY
    echo "## head with the filesystem uuid comparison of the simulated mount removed"; test_all "$ROOT" ;;
esac
echo "END $1"
