#!/usr/bin/env bash
# Scratch, never merged: review of #123. Default-run trace hashes with server/rt of current main and of the PR merged into it.
set -euo pipefail
what=$1
base=$2
cd "$(git rev-parse --show-toplevel)"
root=$PWD
out=$PWD/target/testing-logs/review-123-hash
mkdir -p "$out"
python3 - <<'PY'
p = "server/sim/src/sweep.rs"
s = open(p).read()
a = '    eprintln!("{}", report.summary());\n'
assert s.count(a) == 1
s = s.replace(a, a + '''    for seed in &report.seeds {
        eprintln!("HASH {} {} {:016x} {} {}", config.label, seed.seed, seed.trace_hash, seed.trace_events, seed.passed);
    }
''')
open(p, "w").write(s)
p = "server/events/src/sim/tests.rs"
s = open(p).read()
a = "        if count == 1 {\n            eprintln!("
assert s.count(a) == 1
s = s.replace(a, "        {\n            eprintln!(")
open(p, "w").write(s)
PY
export RUSTFLAGS="--cfg tokio_unstable"
run() {
    cd "$root/server"
    case $what in
    events)
        cd events
        export CARGO_TARGET_DIR=$PWD/target/sim
        for first in 1 90001; do
            count=500; [ $first = 1 ] || count=200
            NABEDRA_SEED=$first NABEDRA_SEEDS=$count cargo test --locked -j 4 -q --features sim --lib sim::tests::seed_sweep -- --exact --nocapture 2>&1 | grep -E "seed [0-9]+ trace hash|events sim:|^error|test result"
        done
        cargo test --locked --features sim --lib sim:: -- --nocapture --test-threads=1 2>&1 | grep -oE "planted bug .* trace hash [0-9a-f]+ \([0-9]+ events\)"
        ;;
    cache)
        export CARGO_TARGET_DIR=$PWD/target/sim-cache CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS=true CARGO_PROFILE_RELEASE_OVERFLOW_CHECKS=true
        for first in 1 90001; do
            count=250; [ $first = 1 ] || count=100
            NABEDRA_SEED=$first NABEDRA_SEEDS=$count NABEDRA_SIM_JOBS=4 NABEDRA_SIM_VERIFY_REPLAY=0 NABEDRA_SIM_SHRINK=0 NABEDRA_SIM_ARTIFACTS=$out/cache \
                cargo test --locked -j 4 --release --no-default-features --features cache,sim --lib cache::cluster::sim::tests::seed_sweep -- --exact --nocapture --test-threads=1 2>&1 | grep -E "^HASH|^error|test result"
        done
        ;;
    oltp)
        export CARGO_TARGET_DIR=$PWD/target/sim RUST_MIN_STACK=16777216
        for first in 1 501 90001; do
            count=50; [ $first = 1 ] || count=25
            NABEDRA_SIM_WORKLOAD=smoke NABEDRA_SEED=$first NABEDRA_SEEDS=$count NABEDRA_SIM_JOBS=3 NABEDRA_SIM_VERIFY_REPLAY=0 NABEDRA_SIM_TEST_DEFECTS=0 \
                NABEDRA_SIM_CHECK_ID=review-123-hash NABEDRA_SIM_GUARANTEES=OLTP-G02 NABEDRA_SIM_ARTIFACTS=$out/oltp \
                cargo test --locked -j 4 --release -p nabedra-sim-oltp --lib sweep::tests::sweep -- --exact --nocapture --test-threads=1 2>&1 | grep -E "^HASH|^error|test result" || true
        done
        ;;
    esac
    cd "$root"
}
git checkout -q "$base" -- server/rt
git rm -q --cached -r --ignore-unmatch server/rt/src/sim/suspend server/rt/src/sim/suspend.rs; rm -rf server/rt/src/sim/suspend server/rt/src/sim/suspend.rs
git diff --quiet "$base" -- server/rt && echo "BASE server/rt equals $base"
grep -c "StoragePause" server/rt/src/sim.rs || true
(run) | sed 's/finished in .*//' > "$out/$what-base.txt"
git checkout -q HEAD -- server/rt
grep -c "StoragePause" server/rt/src/sim.rs
find server/rt -name '*.rs' -exec touch {} +
(run) | sed 's/finished in .*//' > "$out/$what-head.txt"
cat "$out/$what-head.txt"
lines=$(grep -cE "HASH|trace hash" "$out/$what-base.txt")
echo "compared $lines trace hashes of $what: base $base, head $(git rev-parse HEAD)"
test "$lines" -ge 90
diff "$out/$what-base.txt" "$out/$what-head.txt"
echo "IDENTICAL: $lines $what trace hashes"
