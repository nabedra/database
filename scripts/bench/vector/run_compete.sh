#!/usr/bin/env bash
# Starts one competitor in docker (pinned by digest, --cpus=8 --memory=12g on CPUs 0-7), runs
# compete.py pinned to CPUs 8-15, appends JSON lines, then removes the container and its volume.
#
#   run_compete.sh <qdrant|pgvector|milvus> <set> <gt dir name> <run name> [compete.py options]
#   e.g. run_compete.sh qdrant cohere-1m gt-1000000-1000 qdrant-cohere --cases all,u1,u10
#
# Env: SMOKE=1 (contended run allowed, name gets -smoke), DAY, SERVER_CPUS (0-7), CLIENT_CPUS (8-15).
# Images: tags at least two weeks old on 2026-11-27, pinned by the digest of the tag's index.
set -euo pipefail
SYS=${1:?system}; SET=${2:?set}; GT=${3:?gt dir}; NAME=${4:?run name}; shift 4
HERE=$(cd "$(dirname "$0")" && pwd)
REPO=$(cd "$HERE/../../.." && pwd)
DATA=${BENCH_DATA:-/home/dev/bench-data}
BENCH=/home/dev/logs/oltp-vector/bench
OUT=$BENCH/${DAY:-$(date +%F)}
PY=${BENCH_PY:-/home/dev/bench-venv/bin/python}
SERVER_CPUS=${SERVER_CPUS:-0-7}
CLIENT_CPUS=${CLIENT_CPUS:-8-15}
VOL=$DATA/docker/$SYS
C=bench-$SYS

case "$SYS" in
  # pushed 2026-10-21 (v1.20.1 of 2026-11-13 is not two weeks old yet)
  qdrant) IMAGE=qdrant/qdrant:v1.20.0@sha256:9bfc89f9914fc14876a88c05270685921a55b8877b254a98bcc1cc2f0a424cd2 ;;
  # pushed 2026-11-12
  pgvector) IMAGE=pgvector/pgvector:0.8.7-pg17@sha256:b513c48566e4777cd3370451400000c78eda6a08131d68f4c31d4a76f27bd8fd ;;
  # pushed 2026-11-10; standalone with embedded etcd and local storage (no MinIO)
  milvus) IMAGE=milvusdb/milvus:v3.0.6@sha256:6c606011b3d9124487344b80434d98094624ef359e8a33715263fd1206772069 ;;
  *) echo "unknown system $SYS" >&2; exit 2 ;;
esac

free=$(df -BG --output=avail /mnt/c | tail -1 | tr -dc 0-9)
if (( free < 25 )); then echo "STOP: ${free} GB free on /mnt/c" >&2; exit 3; fi
cargo=$(pgrep -c cargo || true)
if [[ "${SMOKE:-0}" != 1 && "$cargo" -gt 0 && ! -f $BENCH/WINDOW-GRANTED ]]; then
  echo "refusing a measured run: $cargo cargo processes and no $BENCH/WINDOW-GRANTED" >&2
  exit 4
fi
[[ "${SMOKE:-0}" == 1 ]] && NAME="$NAME-smoke"
mkdir -p "$OUT"

cleanup() {
  docker rm -f "$C" >/dev/null 2>&1 || true
  # Files written by the container belong to its user; remove them from inside one.
  [[ -d $VOL ]] && docker run --rm -v "$VOL:/v" --entrypoint sh "$IMAGE" -c 'rm -rf /v/* /v/.[!.]*' >/dev/null 2>&1 || true
  rm -rf "$VOL" 2>/dev/null || true
  docker volume rm -f "$C" >/dev/null 2>&1 || true
}
trap 'rc=$?; cleanup; exit $rc' EXIT
docker pull -q "$IMAGE" >/dev/null
cleanup
mkdir -p "$VOL"
LIMITS=(--cpus=8 --cpuset-cpus="$SERVER_CPUS" --memory=12g --memory-swap=12g)
case "$SYS" in
  qdrant)
    docker run -d --name "$C" "${LIMITS[@]}" -p 127.0.0.1:6333:6333 -p 127.0.0.1:6334:6334 \
      -v "$VOL:/qdrant/storage" "$IMAGE" >/dev/null
    ready() { curl -sf http://127.0.0.1:6333/readyz >/dev/null; } ;;
  pgvector)
    # shared_buffers and work memory sized for a 12 GB container.
    docker run -d --name "$C" "${LIMITS[@]}" --shm-size=6g -p 127.0.0.1:5432:5432 -e POSTGRES_PASSWORD=bench \
      -v "$VOL:/var/lib/postgresql/data" "$IMAGE" -c shared_buffers=3GB -c effective_cache_size=8GB \
      -c max_parallel_workers=8 -c max_worker_processes=16 -c max_connections=50 >/dev/null
    ready() { docker exec "$C" pg_isready -U postgres -h 127.0.0.1 >/dev/null 2>&1; } ;;
  milvus)
    cat > "$VOL/embedEtcd.yaml" <<'EOF'
listen-client-urls: http://0.0.0.0:2379
advertise-client-urls: http://0.0.0.0:2379
quota-backend-bytes: 4294967296
auto-compaction-mode: revision
auto-compaction-retention: '1000'
EOF
    # v3 needs DEPLOY_MODE=STANDALONE for embedded etcd, and runs as a user that cannot write a
    # bind mount: data goes to a named volume (removed in cleanup), only the config is bound.
    docker volume rm -f "$C" >/dev/null 2>&1 || true
    docker run -d --name "$C" "${LIMITS[@]}" --security-opt seccomp:unconfined -e DEPLOY_MODE=STANDALONE \
      -e ETCD_USE_EMBED=true -e ETCD_DATA_DIR=/var/lib/milvus/etcd -e ETCD_CONFIG_PATH=/milvus/configs/embedEtcd.yaml \
      -e COMMON_STORAGETYPE=local -v "$C:/var/lib/milvus" -v "$VOL/embedEtcd.yaml:/milvus/configs/embedEtcd.yaml:ro" \
      -p 127.0.0.1:19530:19530 -p 127.0.0.1:9091:9091 "$IMAGE" milvus run standalone >/dev/null
    ready() { curl -sf http://127.0.0.1:9091/healthz >/dev/null; } ;;
esac
for _ in $(seq 1 120); do ready && break; sleep 2; done
ready || { docker logs --tail 50 "$C"; echo "$SYS did not start" >&2; exit 5; }
sleep 3

digest=$(docker image inspect --format '{{join .RepoDigests " "}}' "$IMAGE")
"$HERE/meta.sh" "$REPO" | IMAGE="$IMAGE" DIGEST="$digest" "$PY" -I -c '
import json, os, sys
m = json.loads(sys.stdin.read()); m["image"] = os.environ["IMAGE"]; m["image_digests"] = os.environ["DIGEST"]
m["limits"] = "--cpus=8 --cpuset-cpus=0-7 --memory=12g"; print(json.dumps(m, separators=(",", ":")))' > "$OUT/$NAME.meta.json"

echo "run $NAME: $SYS on CPUs $SERVER_CPUS, client on $CLIENT_CPUS"
OPENBLAS_NUM_THREADS=1 taskset -c "$CLIENT_CPUS" "$PY" -I "$HERE/compete.py" "$SYS" --data "$DATA/$SET" \
  --gt "$DATA/$SET/$GT" --meta "$OUT/$NAME.meta.json" --out "$OUT/$NAME.jsonl" --label "$NAME" \
  --container "$C" "$@" 2> >(tee -a "$OUT/$NAME.stderr" >&2)
docker logs --tail 200 "$C" > "$OUT/$NAME.container.log" 2>&1 || true
"$PY" -I "$HERE/summarize.py" "$OUT/$NAME.jsonl" | tee "$OUT/$NAME.summary.md"
