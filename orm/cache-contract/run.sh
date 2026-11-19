#!/usr/bin/env bash
# Build and install both packages in temporary directories before K01 assertions.
set -euo pipefail
root=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")/../.." && pwd)
python_bin=${NABEDRA_TEST_PYTHON:-python3}
work=$(mktemp -d)
trap 'rm -rf -- "$work"' EXIT
cp "$root"/orm/cache-contract/{fixture.py,python_driver.py,typescript_driver.mjs,vectors.json} "$work"
openssl req -x509 -newkey rsa:2048 -sha256 -days 1 -nodes \
  -subj /CN=localhost -addext subjectAltName=DNS:localhost \
  -keyout "$work/fixture.key" -out "$work/fixture.crt" 2>/dev/null

"$python_bin" -m venv "$work/venv"
"$work/venv/bin/python" -m pip wheel --no-deps --wheel-dir "$work" "$root/orm/python"
"$work/venv/bin/python" -m pip install --no-index --no-deps "$work"/nabedra-*.whl
(
  cd "$root/orm/typescript"
  npm ci --no-audit --no-fund
  npm pack --pack-destination "$work" --quiet
)
npm install --prefix "$work" --no-audit --no-fund "$work"/nabedra-*.tgz

(
  cd "$work"
  export PYTHONNOUSERSITE=1
  unset PYTHONPATH
  "$work/venv/bin/python" fixture.py "$work/venv/bin/python" python_driver.py
  "$work/venv/bin/python" fixture.py node typescript_driver.mjs
)
