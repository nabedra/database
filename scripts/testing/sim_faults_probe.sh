#!/usr/bin/env bash
# Scratch, never merged: cluster sweeps with the opt-in faults on.
set -uo pipefail
cd "$(git rev-parse --show-toplevel)/server"
export RUSTFLAGS="--cfg tokio_unstable"
sweep() {
    echo "=== $1: pause $2 freeze $3 ms $4"
    export NABEDRA_PAUSE=$2 NABEDRA_FREEZE=$3 NABEDRA_FREEZE_MS=$4
    if [ "$1" = oltp ]; then
        CARGO_TARGET_DIR=$PWD/target/sim RUST_MIN_STACK=16777216 NABEDRA_SIM_WORKLOAD=smoke NABEDRA_SEED=1 NABEDRA_SEEDS=200 NABEDRA_SIM_JOBS=4 NABEDRA_SIM_VERIFY_REPLAY=1 NABEDRA_SIM_TEST_DEFECTS=0 \
            NABEDRA_SIM_CHECK_ID=sim-faults-probe NABEDRA_SIM_GUARANTEES=OLTP-G02 NABEDRA_SIM_ARTIFACTS=$PWD/../target/testing-logs/probe-oltp \
            timeout 900 cargo test --locked -j 4 --release -p nabedra-sim-oltp --lib sweep::tests::sweep -- --exact --nocapture --test-threads=1 > /tmp/probe.txt 2>&1
    else
        CARGO_TARGET_DIR=$PWD/target/sim-cache CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS=true CARGO_PROFILE_RELEASE_OVERFLOW_CHECKS=true \
            NABEDRA_SEED=1 NABEDRA_SEEDS=40 NABEDRA_SIM_JOBS=4 NABEDRA_SIM_VERIFY_REPLAY=1 NABEDRA_SIM_SHRINK=0 NABEDRA_SIM_ARTIFACTS=$PWD/../target/testing-logs/probe-cache \
            timeout 900 cargo test --locked -j 4 --release --no-default-features --features cache,sim --lib cache::cluster::sim::tests::seed_sweep -- --exact --nocapture --test-threads=1 > /tmp/probe.txt 2>&1
    fi
    echo "rc $?"
    grep FAULTS /tmp/probe.txt | awk '{p+=$5; f+=$7; n++} END {print n " runs, pauses " p ", freezes " f}'
    grep -vE "FAULTS|Compiling" /tmp/probe.txt | grep -E "^error|violation|seed [0-9]+ (failed|is not)|t=[0-9]+ms|panicked|holds a lock|suspended|nondeterministic|failing|test result|trace:" | cut -c1-300 | head -60
}
export NABEDRA_LATE=1
sweep "$1" 0 0 1
grep -E "seed\":(66|185)" /tmp/probe.txt
for d in "$PWD"/../target/testing-logs/probe-oltp/*; do echo "$d"; done
sweep "$1" 20 0 1
for f in "$PWD"/../target/testing-logs/probe-oltp/*seed-66/history.jsonl; do tail -c 3000 "$f"; done
for f in "$PWD"/../target/testing-logs/probe-oltp/*seed-66/trace-tail.txt; do tail -n 60 "$f" | cut -c1-300; done
echo done
