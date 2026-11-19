#!/usr/bin/env bash
# Run the cache process histories on one Docker kernel.
set -euo pipefail
repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
work=${NABEDRA_CACHE_WORK:-"$repo/.nabedra/workflows/native-cache"}
binary=${NABEDRA_CACHE_BINARY:-"$work/native-target/release/nabedra-db"}
packages=${NABEDRA_CACHE_ORM_ARTIFACTS:-"$work/orm-artifacts-final"}
result=${1:-"$work/cluster-acceptance"}
mkdir -p "$result"
result=$(cd "$result" && pwd)
cpu=()
if [[ -n ${NABEDRA_CACHE_TEST_CPUS:-} ]]; then
  cpu=(--cpuset-cpus "$NABEDRA_CACHE_TEST_CPUS")
fi
docker run --rm --network none --memory 3g --memory-swap 3g ${cpu[@]+"${cpu[@]}"} \
  --mount "type=bind,source=$repo,target=/source,readonly" \
  --mount "type=bind,source=$result,target=/result" \
  --mount "type=bind,source=$binary,target=/native,readonly" \
  --mount "type=bind,source=$packages,target=/packages,readonly" \
  --env PYTHONDONTWRITEBYTECODE=1 --env PIP_ROOT_USER_ACTION=ignore \
  python:3.11-slim-bookworm sh -ec '
    python -m pip install --no-index --no-deps --disable-pip-version-check /packages/redis-6.4.0-py3-none-any.whl
    cp /native /tmp/nabedra-db
    exec python /source/scripts/cache-acceptance/cluster.py --binary /tmp/nabedra-db --output /result
  '
