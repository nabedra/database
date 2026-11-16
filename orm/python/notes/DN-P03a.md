# DN-P03a serve argument validation

Implemented 2026-11-10 in shared main. No applicable AGENTS.md was found in the repository or ancestor directories. Existing uncommitted work was preserved. The authoritative source is `plans/distributed-contracts.md`, with the accepted protocol in `DN-A04.md`. Its parent override removes completed callback caching. No callback cache is implemented here.

## Private API

`meshdb._serve_args._validate_serve_args(name, nodes=1, start_node=None, stop_node=None, list_nodes=None)` returns a plain dictionary:

- `name`: validated by existing `validation.validate_identifier`.
- `initial_policy`: `{mode, desired, min_nodes, max_nodes}`. Fixed exact integers accept 0..1024 and set all counts equal. Exact two-element integer tuples require 1 <= min <= max <= 1024 and initialize desired to min. Bool and integer/tuple subclasses are rejected.
- `provisioning`: `managed` when all three callbacks are synchronous callables, otherwise `external` when all are None. External mode rejects tuple policy locally.
- `callbacks`: the original objects in start/stop/list order, retained by identity. This tuple is private runtime state and must never be serialized, logged or rendered.

Coroutine functions, async callable objects and partial-wrapped async callbacks are rejected using inspect facilities. Signature introspection is not required, so native callables remain valid. No callback is invoked. Synchronous callbacks returning awaitables remain the bridge's validation responsibility.

`meshdb._serve_args._resolve_config_path()` reads only `MESHDB_CONFIG` when called and returns an absolute path after regular-file and read-access checks. Relative paths use the current directory. Spaces and literal shell, variable and tilde spellings are supported as filenames without expansion. Missing, empty, directory, nonexistent, unreadable and NUL-containing paths fail with one static existing ValidationError mentioning MESHDB_CONFIG. Errors do not include the input path, OS diagnostic or chained exception. No config content is opened, and invalid JSON in an existing readable file passes.

## Validation evidence

Initial test command before implementation:

```text
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=orm/src orm/.venv/bin/python -m unittest discover -s orm/tests -p test_serve_args.py
```

It failed during collection because the new `_serve_args` module did not exist. After implementation, the same command passed all three grouped tests in 0.011 seconds. A test fixture was corrected to inject a NUL value through a dictionary because the OS itself forbids storing NUL in an actual environment variable.

The tests cover accepted policy normalization and original callback identities; exact types, boundaries, mixed callback sets and async declarations; and real temporary config paths, literal filenames, sanitized failures and invalid JSON acceptance. Callback fixtures fail if invoked or rendered. Native callable acceptance does not depend on inspectable signatures. The unreadable test chmods a real file to zero permissions, restoring permissions before cleanup. On hosts that can still read it, such as root, it emulates access denial for portability.

Syntax and direct trailing-whitespace checks use the existing interpreter, compiling the two owned Python files in memory and checking all three owned files. No full suite, package installation or Cargo command was run.

## Scope limits

Only the new `_serve_args.py`, `test_serve_args.py` and this note belong to this assignment. The module adds declarations only at import and is not exported from the package. No public serve function, supervisor, callback bridge or process integration is implemented. No network, threads, subprocesses, callbacks, backend/credential/config-content reads, dotenv lookup or executable resolution occur. Rust owns JSON/schema and secret-file validation. Later integration must keep callback objects out of wire records and diagnostics and validate awaitable callback results. Independent specification and code-quality reviews passed without actionable findings, reusing the recorded checks without redundant tests.
