# Monorepo layout verification

Checked on 2026-11-16 against base `25d53e6` plus this PR's layout changes.
Commands below ran from the repository root.

The Python package moved from `orm/` to `orm/python/`. The database remains
at `server/`. Component READMEs now cover getting started; their existing
references remain under `server/docs/` and `orm/python/docs/`. Shared testing
contracts and recorded artifacts remain under `docs/testing/`.

```text
Before                         After
orm/src/nabedra/                orm/python/src/nabedra/
orm/tests/                      orm/python/tests/
orm/README.md                  orm/python/docs/reference.md
README.md                      server/docs/overview.md
server/README.md               server/docs/reference.md
TUTORIAL.md                    server/TUTORIAL.md
docs/cli-and-serverless.md      server/docs/cli-and-serverless.md
                               README.md, component READMEs, CONTRIBUTION.md
```

## Executed checks

- Editable install with `orm/python/.venv/bin/python -m pip install -e
  './orm/python[gateway]'` succeeded. `pip check` found no broken requirements.
- `PYTHONDONTWRITEBYTECODE=1 orm/python/.venv/bin/python -m unittest discover
  -s orm/python/tests -v` passed 177 tests in 71.799 seconds. This includes
  relocated TLS and gateway fixture imports and the installer package path.
- `PYTHONDONTWRITEBYTECODE=1 orm/python/.venv/bin/python -m unittest discover
  -s scripts -p 'test_*.py' -v` passed 16 tests in 4.972 seconds.
- `orm/python/.venv/bin/python -m build orm/python --outdir
  artifacts/monorepo/dist` built a wheel and source archive.
- A separate environment installed that wheel with `--no-index --no-deps`.
  Isolated imports resolved inside that environment; client construction worked
  without importing HTTPX or Boto3. Console and module `database --help` worked.
- The four modified acceptance entry points, `acceptance_nabedra.py`,
  `acceptance_providers.py`, `acceptance_serverless_s3.py` and
  `acceptance_cli_serverless.py`, loaded and displayed `--help`.
- `cargo fmt --manifest-path server/Cargo.toml --all -- --check` passed.
  `cargo package --manifest-path server/Cargo.toml --list --allow-dirty`
  completed successfully.
- All 180 Python and Rust runtime source files matched their base bytes after
  relocation. Historical evidence artifacts also remained unchanged.
- Nine component and repository guides had no missing local link targets.
  Ignore checks passed for 18 generated/private paths and nine trackable paths,
  including lockfiles, source files and example environment files.

Logs and package artifacts are local under ignored `artifacts/monorepo/`.
The Docker package source and installer now select `orm/python/`; no Docker
image was built. No runtime throughput or latency claim follows from these
checks. No native server build, Clippy run, Rust test run, cloud acceptance or
full scheduler acceptance ran. In particular, C01/U01/F03/R04 are not certified
by this change's checks. Installed CLI `--version` needs a native server binary,
which was unavailable for this verification.
