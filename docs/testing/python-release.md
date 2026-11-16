# Python package release validation

The Python distribution is `nabedra` 0.1.0 under MIT. Users install it with
`python3.11 -m pip install "nabedra[database]"` and import `nabedra`.
The `database` extra has no dependencies; `gateway` adds HTTPX and `aws` adds
Boto3. The package includes the Python CLI and requires a separately installed
Rust server for local execution. No ORM runtime source changed.

Validation ran on 2026-11-16 with Python 3.11.10 on macOS x86_64.

```sh
python3.11 -m venv orm/python/.venv
orm/python/.venv/bin/python -m pip install build twine \
  'httpx>=0.27,<1' 'boto3>=1.34,<2'
orm/python/.venv/bin/python -m build orm/python
orm/python/.venv/bin/python -m twine check --strict orm/python/dist/*
orm/python/.venv/bin/python -m pip install --no-deps \
  orm/python/dist/nabedra-0.1.0-py3-none-any.whl
PYTHONDONTWRITEBYTECODE=1 orm/python/.venv/bin/python -m unittest discover -s orm/python/tests
```

Both artifacts passed strict Twine checks. All 177 ORM tests passed in 73.418
seconds with the wheel installed. Tests cover the client, queries, transport,
receipts, CLI, gateway, providers and process lifecycle. Subprocess fixtures
may explicitly select checkout source; the separate integration below verifies
the installed package in actual CLI and client processes.

A separate temporary virtual environment installed `nabedra[database]==0.1.0`
from the wheel with `--no-index --find-links orm/python/dist`. Imports, inert client
construction, installed metadata, `nabedra database --help`,
`python -m nabedra database --help`, and `pip check` passed without HTTPX or
Boto3 installed. Environment creation, installation and smoke checks took
12.842 seconds; import and client construction took 206.260 ms in that one run.
These are local smoke timings, not load or throughput benchmarks. Another
temporary environment built and installed the source distribution and imported
the client successfully.

The archive inspection verified all 25 Python modules against source, the MIT
license in both artifacts, console entry point, extras, and conditional optional
dependencies. It found no dotenv files, virtual environments, build outputs,
private keys, or supplied Cargo/AWS credential values. The example AWS region
in the documentation is public configuration. Artifact sizes and SHA-256
checksums are recorded in [artifacts.json](python-release/artifacts.json).

## Installed CLI and client integration

```sh
PYTHONDONTWRITEBYTECODE=1 orm/python/.venv/bin/python scripts/acceptance_nabedra.py \
  --binary /path/to/nabedra-database-server \
  --cli orm/python/.venv/bin/nabedra --python orm/python/.venv/bin/python \
  --output artifacts/python-release/integration.json
```

All eight assertions passed in 44.000 seconds against the existing compatible
0.1.0 server binary. The [integration report](python-release/integration.json)
records the binary hash and installed source hashes. It verifies CLI01 and
U01/C01 subsets by starting one API and one two-thread worker, then writing
and reading exact batches of 10, 100 and 1,000 rows through the installed ORM.
An R04 subset cold restart retains all 1,110 committed rows. F03 subsets verify
that both CLI shutdowns reap their API and worker children.

This uses trusted loopback SQLite object storage. It does not assert the full
scheduler contract, AWS integration, cluster failure tolerance, or scaling.

## Publishing

After verification, upload the exact checked files using a PyPI API token
supplied privately through `TWINE_PASSWORD`:

```sh
orm/python/.venv/bin/python -m twine upload --non-interactive \
  --repository-url https://upload.pypi.org/legacy/ \
  orm/python/dist/nabedra-0.1.0-py3-none-any.whl orm/python/dist/nabedra-0.1.0.tar.gz
```

Never put tokens in command arguments, tracked files, or release evidence.
After upload, verify both registry file hashes and install
`nabedra[database]==0.1.0` from PyPI in a fresh environment outside the checkout.

## Publication result

Published [nabedra 0.1.0](https://pypi.org/project/nabedra/0.1.0/) on
2026-11-16 under MIT. Both the wheel and source distribution were uploaded.
The [registry verification](python-release/pypi.json) confirms that both file
sizes and SHA-256 checksums match the tested artifacts, with Python 3.11+ and
the `database`, `gateway` and `aws` extras declared.

A fresh temporary environment outside the checkout installed
`nabedra[database]==0.1.0` from `https://pypi.org/simple/` with caching disabled.
Version, import path, client construction, absence of optional dependencies,
both CLI help entry points, and `pip check` passed. The environment setup,
download, installation and smoke checks took 12.677 seconds locally.
