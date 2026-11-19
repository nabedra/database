#!/usr/bin/env bash
# Run K01/K02 local process assertions in a dedicated Docker network namespace.
set -euo pipefail

repo=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
work=${NABEDRA_CACHE_WORK:-"$repo/.nabedra/workflows/native-cache"}
output=${1:-"$work/standalone-acceptance"}
reference=${NABEDRA_CACHE_VALKEY_DIR:-"$work/benchmark"}
packages=${NABEDRA_CACHE_ORM_ARTIFACTS:-"$work/orm-artifacts-final"}
node_packages=${NABEDRA_CACHE_ORM_BUNDLE:-"$work/orm-docker-final"}
image=${NABEDRA_CACHE_TEST_IMAGE:-python:3.11-slim-bookworm}
target=${NABEDRA_CACHE_TARGET_VOLUME:-nabedra-native-cache-target}

for path in "$reference/valkey-server" "$packages/redis-6.4.0-py3-none-any.whl" "$node_packages/node" "$node_packages/node_modules/redis" "$node_packages/node_modules/nabedra"; do
  if [[ ! -e "$path" ]]; then
    printf 'Missing acceptance dependency %s\n' "$path" >&2
    exit 2
  fi
done
mkdir -p "$output"
output=$(cd "$output" && pwd)

cpu=()
if [[ -n ${NABEDRA_CACHE_TEST_CPUS:-} ]]; then
  cpu=(--cpuset-cpus "$NABEDRA_CACHE_TEST_CPUS")
fi

docker run --rm ${cpu[@]+"${cpu[@]}"} --network none --memory=256m --memory-swap=256m \
  --mount "type=bind,source=$repo,target=/source,readonly" \
  --mount "type=bind,source=$output,target=/results" \
  --mount "type=bind,source=$reference,target=/reference,readonly" \
  --mount "type=bind,source=$packages,target=/packages,readonly" \
  --mount "type=bind,source=$node_packages,target=/node,readonly" \
  --mount "type=volume,source=$target,target=/target,readonly" \
  --env PYTHONDONTWRITEBYTECODE=1 --env PIP_ROOT_USER_ACTION=ignore \
  --entrypoint sh "$image" -ec '
    python -m pip install --no-index --no-deps --disable-pip-version-check \
      /packages/redis-6.4.0-py3-none-any.whl /packages/nabedra-*.whl
    cp /target/release/nabedra-db /tmp/nabedra-db
    exec python /source/scripts/cache-acceptance/standalone.py \
      --nabedra /tmp/nabedra-db --valkey /reference/valkey-server \
      --node /node/node --node-modules /node/node_modules --output /results
  '
