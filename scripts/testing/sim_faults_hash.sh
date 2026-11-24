#!/usr/bin/env bash
# Scratch, never merged: compares default-run trace hashes of the base and head server/rt.
set -euo pipefail
what=$1
base=$2
cd "$(git rev-parse --show-toplevel)"
out=$PWD/target/testing-logs/sim-faults-hash
mkdir -p "$out"
python3 - <<'EOF'
p = "server/sim/src/sweep.rs"
s = open(p).read()
a = '    eprintln!("{}", report.summary());\n'
assert s.count(a) == 1
s = s.replace(a, a + '''    for seed in &report.seeds {
        eprintln!("HASH {} {} {:016x} {} {}", config.label, seed.seed, seed.trace_hash, seed.trace_events, seed.passed);
    }
''')
open(p, "w").write(s)
EOF
export RUSTFLAGS="--cfg tokio_unstable"
run() {
    cd server
    case $what in
    events)
        cd events
        export CARGO_TARGET_DIR=$PWD/target/sim
        cargo test --locked -j 4 --features sim --lib --no-run
        for seed in $(seq 1 150); do
            NABEDRA_SEED=$seed NABEDRA_SEEDS=1 cargo test --locked -q --features sim --lib sim::tests::seed_sweep -- --exact --nocapture 2>&1 | grep -E "seed $seed trace hash"
        done
        cargo test --locked --features sim --lib sim:: -- --nocapture --test-threads=1 2>&1 | grep -oE "planted bug .* trace hash [0-9a-f]+ \([0-9]+ events\)"
        cd ..
        ;;
    cache)
        export CARGO_TARGET_DIR=$PWD/target/sim-cache CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS=true CARGO_PROFILE_RELEASE_OVERFLOW_CHECKS=true
        NABEDRA_SEEDS=150 NABEDRA_SIM_JOBS=4 NABEDRA_SIM_VERIFY_REPLAY=0.05 NABEDRA_SIM_SHRINK=0 NABEDRA_SIM_ARTIFACTS=$out/cache \
            cargo test --locked -j 4 --release --no-default-features --features cache,sim --lib cache::cluster::sim::tests::seed_sweep -- --exact --nocapture --test-threads=1 2>&1 | grep -E "^HASH|^error|test result"
        ;;
    oltp)
        export CARGO_TARGET_DIR=$PWD/target/sim RUST_MIN_STACK=16777216
        NABEDRA_SIM_WORKLOAD=smoke NABEDRA_SEED=1 NABEDRA_SEEDS=50 NABEDRA_SIM_JOBS=4 NABEDRA_SIM_VERIFY_REPLAY=0 NABEDRA_SIM_TEST_DEFECTS=0 \
            NABEDRA_SIM_CHECK_ID=sim-faults-hash NABEDRA_SIM_GUARANTEES=OLTP-G02 NABEDRA_SIM_ARTIFACTS=$out/oltp \
            cargo test --locked -j 4 --release -p nabedra-sim-oltp --lib sweep::tests::sweep -- --exact --nocapture --test-threads=1 2>&1 | grep -E "^HASH|^error|test result"
        ;;
    esac
    cd ..
}
git checkout -q "$base" -- server/rt
(run) | sed 's/finished in .*//' > "$out/$what-base.txt"
git checkout -q HEAD -- server/rt
(run) | sed 's/finished in .*//' > "$out/$what-head.txt"
cat "$out/$what-head.txt"
lines=$(grep -cE "HASH|trace hash" "$out/$what-base.txt")
echo "compared $lines trace hashes of $what: base $base, head $(git rev-parse HEAD)"
test "$lines" -ge 50
diff "$out/$what-base.txt" "$out/$what-head.txt"
echo "IDENTICAL: $lines $what trace hashes"
