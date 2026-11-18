# Run local OLTP with one worker replica

Use Docker with Linux AMD64 support and Python 3.11. The local handoff directory,
`artifacts/local-oltp-ef213dcd/`, contains the Docker archive, the matching Python
wheel and `SHA256SUMS`. The image contains the development-profile binary
built with `--no-default-features --features oltp`. The CLI reports
`nabedra-db 0.1.1 oltp` and serves the `local` database on a retained volume.

From the handoff directory, verify the downloads and install the wheel into a
fresh environment.

```sh
shasum -a 256 -c SHA256SUMS
docker load -i nabedra-oltp-local-ef213dcd-linux-amd64.docker.tar.gz
python3.11 -m venv .venv
.venv/bin/python -m pip install --no-index --no-deps ./nabedra-0.1.0-py3-none-any.whl
```

Create the token once and save the path for later starts. The sample uses a new
named volume on its first run and binds the HTTP API to host loopback.

```sh
local_env=$(mktemp "${TMPDIR:-/tmp}/nabedra-oltp.XXXXXXXX")
chmod 600 "$local_env"
.venv/bin/python -c 'import secrets; print("NABEDRA_TOKEN=" + secrets.token_hex(32))' > "$local_env"
data_volume="nabedra-oltp-data-${local_env##*.}"
docker volume create "$data_volume"

start_oltp() {
  docker run -d --pull never --platform linux/amd64 --name nabedra-oltp \
    --env-file "$local_env" \
    --mount "type=volume,source=$data_volume,target=/var/lib/nabedra" \
    --publish 127.0.0.1:8765:8765 nabedra-oltp-local:ef213dcd "$@" || return
  curl --fail --silent --show-error --retry 30 --retry-connrefused \
    --retry-delay 1 --max-time 2 http://127.0.0.1:8765/health/ready
}

start_oltp &&
export NABEDRA_TOKEN="$(cut -d= -f2- "$local_env")" &&
.venv/bin/python - <<'PY'
import os
from nabedra import Database

db = Database(url='http://127.0.0.1:8765', token=os.environ['NABEDRA_TOKEN'],
              database='local', mode='oltp')
notes = db.create_table('notes', {'id': int, 'body': str}, key='id')
notes.insert({'id': 1, 'body': 'survives a restart'})
assert notes.get(1) == {'id': 1, 'body': 'survives a restart'}
PY
```

After the container stops, remove it and start again with the same volume and
`--cold-start`.

```sh
docker stop --time 30 nabedra-oltp &&
docker rm nabedra-oltp &&
start_oltp serve --data-dir /var/lib/nabedra/data --database local \
  --host 0.0.0.0 --port 8765 --workers 4 --cold-start &&
.venv/bin/python - <<'PY'
import os
from nabedra import Database

db = Database(url='http://127.0.0.1:8765', token=os.environ['NABEDRA_TOKEN'],
              database='local', mode='oltp')
assert db.table('notes').get(1) == {'id': 1, 'body': 'survives a restart'}
PY
unset NABEDRA_TOKEN
```

The packaged Linux smoke exercised the standalone O01 workload with all 13
ORM operations and batch sizes 1, 100 and 1,000. It also exercised SIGTERM and
SIGKILL restarts. Each restart started a
new run on the same volume and recovered the prior table ID, committed rows and
committed receipt. The smoke also checked a new write after each restart. This
covers committed-data recovery in R04. See the handoff's `evidence/` directory.
