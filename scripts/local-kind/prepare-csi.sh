#!/usr/bin/env bash
set -euo pipefail
output=${1:?usage: prepare-csi.sh OUTPUT_DIRECTORY}
mkdir -p "$output"
src="$output/csi-hostpath"
revision=6fab18ae4fc4f486193b618114eeb817d9929242
if [[ ! -d "$src/.git" ]]; then
  git clone --depth 1 --branch v1.19.0 https://github.com/kubernetes-csi/csi-driver-host-path.git "$src"
fi
test "$(git -C "$src" rev-parse HEAD)" = "$revision"
manifest="$src/deploy/kubernetes-latest/hostpath/csi-hostpath-plugin.yaml"
for sidecar in csi-attacher csi-provisioner csi-resizer; do
  image=$(awk -v sidecar="/$sidecar:" '$1 == "image:" && index($2, sidecar) { print $2 }' "$manifest")
  version=${image##*:}
  curl --fail --location --silent --show-error --retry 2 --max-time 25 \
    "https://raw.githubusercontent.com/kubernetes-csi/external-${sidecar#csi-}/$version/deploy/kubernetes/rbac.yaml" \
    --output "$output/$sidecar-rbac.yaml"
done
