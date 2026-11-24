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
    BASE=$2
    git worktree add -q "$ROOT/target/base" "$BASE"
    test_all() { ( cd "$1/server"; export CARGO_TARGET_DIR=$ROOT/server/target/p5
        cargo test --locked -j 4 -p nabedra-rt --features sim --lib sim:: 2>&1 | grep -E "FAILED|must not|test result|^error|panicked|inode" | cut -c1-250
        cargo test --locked -j 4 -p nabedra-oltp-storage --features sim --lib oltp::pool::sim_tests -- --test-threads=1 2>&1 | grep -E "FAILED|test result|^error|panicked|accepted|only a real" | cut -c1-250 ); }
    echo "## main $BASE, clean"; test_all "$ROOT/target/base"
    python3 - "$ROOT/target/base" <<'PY'
import sys
p=sys.argv[1]+"/server/rt/src/sim/volumes.rs"; s=open(p).read()
a="        if base == old_fs.ino_base() {\n            base += 1 << 20;\n        }\n"; assert s.count(a)==1
open(p,"w").write(s.replace(a,"        if matches!(how, Wipe::Reformat) {\n            base = old_fs.ino_base();\n        }\n"))
PY
    echo "## main with P5 planted"; test_all "$ROOT/target/base"
    python3 - "$ROOT/target/base" <<'PY'
import sys,re
p=sys.argv[1]+"/server/rt/src/sim/volumes/tests.rs"; s=open(p).read()
a='        assert_ne!(\n            recreated.1, anchor.1,\n            "a reformat must not reuse inode numbers (P5)"\n        );\n'; assert s.count(a)==1
open(p,"w").write(s.replace(a,""))
p=sys.argv[1]+"/server/oltp-storage/src/oltp/pool/sim_tests.rs"; s=open(p).read()
a='                assert_ne!(mount.observation().root_inode, old_inode);\n'; assert s.count(a)==1
open(p,"w").write(s.replace(a,"                let _ = mount;\n"))
PY
    echo "## main with P5 planted and the two inode-number assertions removed"; test_all "$ROOT/target/base"
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
