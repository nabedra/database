#!/usr/bin/env bash
# Scratch, review of #123, never merged: which failures under long freezes depend on the harness.
# Exits 1 if any sweep failed, so its record never reads as a pass.
set -uo pipefail
cd "$(git rev-parse --show-toplevel)/server"
export RUSTFLAGS="--cfg tokio_unstable"
bad=0
sweep() { # kind pause freeze ms disarm long seeds
    echo "=== $1: pause $2 freeze $3 ms $4 disarm $5 long $6 seeds $7"
    export NABEDRA_PAUSE=$2 NABEDRA_FREEZE=$3 NABEDRA_FREEZE_MS=$4 NABEDRA_DISARM=$5 NABEDRA_LONG=$6
    if [ "$1" = oltp ]; then
        CARGO_TARGET_DIR=$PWD/target/sim RUST_MIN_STACK=16777216 NABEDRA_SIM_WORKLOAD=smoke NABEDRA_SEED=1 NABEDRA_SEEDS=$7 NABEDRA_SIM_JOBS=3 NABEDRA_SIM_VERIFY_REPLAY=0 NABEDRA_SIM_TEST_DEFECTS=0 \
            NABEDRA_SIM_CHECK_ID=review-123-probe NABEDRA_SIM_GUARANTEES=OLTP-G02 NABEDRA_SIM_ARTIFACTS=$PWD/../target/testing-logs/probe-oltp-$2-$4-$5-$6 \
            timeout 1200 cargo test --locked -j 4 --release -p nabedra-sim-oltp --lib sweep::tests::sweep -- --exact --nocapture --test-threads=1 > /tmp/probe.txt 2>&1
    else
        CARGO_TARGET_DIR=$PWD/target/sim-cache CARGO_PROFILE_RELEASE_DEBUG_ASSERTIONS=true CARGO_PROFILE_RELEASE_OVERFLOW_CHECKS=true \
            NABEDRA_SEED=1 NABEDRA_SEEDS=$7 NABEDRA_SIM_JOBS=4 NABEDRA_SIM_VERIFY_REPLAY=0 NABEDRA_SIM_SHRINK=0 NABEDRA_SIM_ARTIFACTS=$PWD/../target/testing-logs/probe-cache-$5 \
            timeout 1500 cargo test --locked -j 4 --release --no-default-features --features cache,sim --lib cache::cluster::sim::tests::seed_sweep -- --exact --nocapture --test-threads=1 > /tmp/probe.txt 2>&1
    fi
    rc=$?; echo "rc $rc"; [ $rc = 0 ] || bad=1
    grep -vE "FAULTS|Compiling" /tmp/probe.txt | grep -E "^error|seed [0-9]+ (failed|is not)|t=[0-9]+ms|ENROLL-PROBE|panicked|holds a lock|nondeterministic|test result" | grep -v "violation " | cut -c1-260 | head -90
}
if [ "$1" = oltp ]; then
    sweep oltp 0 1 2000 0 0 12
    sweep oltp 20 1 2000 0 0 12
    sweep oltp 0 1 2000 0 0 60
    sweep oltp 0 1 2000 1 0 60
    sweep oltp 0 1 2000 0 1 60
    sweep oltp 0 1 2000 1 1 60
    sweep oltp 20 1 2000 1 1 60
else
    sweep cache 0 1 2000 0 0 60
    sweep cache 0 1 2000 1 0 60
fi
echo "done bad=$bad"
exit $bad
