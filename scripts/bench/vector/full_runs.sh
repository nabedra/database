#!/usr/bin/env bash
# The measured runs of contract-w2 6.3 (local index) and 6.2 (competitors) at 1M, by name.
#
#   full_runs.sh <run>      runs: see the case below; `list` prints them
#
# prep-* only build and save an index (no measurement): they may run on a busy machine, and the
# lines they write carry "contended" in meta. Every other run refuses unless no cargo runs or
# /home/dev/logs/oltp-vector/bench/WINDOW-GRANTED exists (run_local.sh, run_compete.sh).
# Results: /home/dev/logs/oltp-vector/bench/<DAY>/<run>.jsonl and .summary.md.
set -euo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
CK=${BENCH_DATA:-/home/dev/bench-data}/checkpoints
export DAY=${DAY:-$(date +%F)}
L=$HERE/run_local.sh
C=$HERE/run_compete.sh
COHERE=(cohere-1m gt-1000000-1000)
GLOVE=(glove-100 gt-1183514-1000)
CURVE=(--recalls 0.9,0.95,0.99 --units 32,64,128,256,512 --limits 10,40 --threads 1,4,8 --qps-seconds 3)
FILTERS=all,u10,u1,u01,ab,id99,id1,label_label_1p,label_label_10p,label_label_50p

case "${1:-list}" in
  # Build Cohere 1M through the feed and keep its checkpoint (~3.2 GB). Also the first build-rate
  # and checkpoint write/recover numbers at 1M (contended unless in a window).
  prep-cohere) SMOKE=1 REPS=1 "$L" "${COHERE[@]}" prep-cohere --no-sweep --save-checkpoint "$CK/cohere-1m" ;;
  prep-glove) SMOKE=1 REPS=1 "$L" "${GLOVE[@]}" prep-glove --no-sweep --save-checkpoint "$CK/glove-100" ;;
  # Recall/latency curves per operation, unfiltered (gate: Operations).
  local-cohere-curve) REPS=${REPS:-1} "$L" "${COHERE[@]}" local-cohere-curve --from-checkpoint "$CK/cohere-1m" \
      --ops cosine,dot,l2 --cases all "${CURVE[@]}" ;;
  # Filter cases (gate: Filters), cosine, at the settings that reach 0.95 unfiltered.
  local-cohere-filters) REPS=${REPS:-1} "$L" "${COHERE[@]}" local-cohere-filters --from-checkpoint "$CK/cohere-1m" \
      --ops cosine --cases "$FILTERS" --recalls 0.95 --units 128,256 --limits 40 --threads 1,4,8 --qps-seconds 3 ;;
  # Unequal lengths: GloVe under dot and l2 against cosine (plan section 5 decision).
  local-glove) REPS=${REPS:-1} "$L" "${GLOVE[@]}" local-glove --from-checkpoint "$CK/glove-100" \
      --ops cosine,dot,l2 --cases all,u10 --recalls 0.95,0.99 --units 128,512,2048 --limits 10,40 --threads 1,4,8 --qps-seconds 3 ;;
  # Build rate in a window, 1M through the feed with checkpoint write and recover (gate: Rebuild).
  local-cohere-build) REPS=1 "$L" "${COHERE[@]}" local-cohere-build --no-sweep --checkpoint ;;
  # Competitors on Cohere 1M, cosine, swept to recall@10 >= 0.95.
  qdrant-cohere) "$C" qdrant "${COHERE[@]}" qdrant-cohere --op cosine --cases all,u10,u1,ab,id1,label_label_1p \
      --params 16,32,64,128,256,512 --threads 1,4,8 --qps-seconds 10 ;;
  pgvector-cohere) "$C" pgvector "${COHERE[@]}" pgvector-cohere --op cosine --cases all,u10,u1,ab,id1,label_label_1p \
      --params 40,100,200,400,800 --threads 1,4,8 --qps-seconds 10 ;;
  milvus-cohere) "$C" milvus "${COHERE[@]}" milvus-cohere --op cosine --cases all,u10,u1,ab,id1,label_label_1p \
      --params 16,32,64,128,256,512 --threads 1,4,8 --qps-seconds 10 ;;
  list) sed -n 's/^  \([a-z0-9-]*\)) .*/\1/p' "$0" | grep -v '^list$' ;;
  *) echo "unknown run $1" >&2; exit 2 ;;
esac
