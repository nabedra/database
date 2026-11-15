# DN-P04a worker console launcher

Implemented 2026-11-10. Specification and quality re-check passed after the
fixture ownership correction. This slice adds only the Python launcher; it
does not establish actual Rust worker startup.

## Scope and contract

Owned changes are new `orm/src/meshdb/cli.py`, new
`orm/src/meshdb/_binary.py`, the `[project.scripts]` stanza in
`orm/pyproject.toml`, new `orm/tests/test_cli.py`, and this note. Existing shared
uncommitted work was left in place. No applicable AGENTS.md files were found.
The test-driven-development and unslop skills were read and applied.

The console entry is `meshdb = "meshdb.cli:main"`. Its only command is
`meshdb worker --bootstrap PATH`; source-tree execution also supports
`python -m meshdb.cli worker --bootstrap PATH`. Imports and both help commands
are inert. Arguments use argparse with required subcommand/bootstrap, no option
abbreviations, and standard usage/error output with exit 2. Error text is static
to bound output and avoid echoing arbitrary arguments or bootstrap paths. Help
describes the required file and Rust's validation ownership.

`main(argv: list[str] | None = None) -> int` parses either explicit arguments or
the process arguments. It invokes `os.execv(binary, [binary, "worker",
"--bootstrap", bootstrap])`. Successful invocation replaces Python, preserving
the PID, signal ownership and Rust's exit status. Resolution or invocation
failure prints one static message advising `MESHDB_SERVER_BINARY` or PATH and
returns 1, compatible with the setuptools console wrapper. No shell, Python
worker engine, supervisor thread or bootstrap preflight is involved. Python
never opens or prints the bootstrap file.

Private shared resolver API is
`meshdb._binary.resolve_server_binary() -> str`. Each call returns an absolute
executable filesystem path or raises `RuntimeError` with the static
`BINARY_FAILURE` message and no chained raw OS error. A present
`MESHDB_SERVER_BINARY` is used literally as a filesystem path, relative to the
current directory when appropriate, including spaces. Empty, missing,
nonexecutable and directory overrides fail without PATH fallback. Without an
override, resolution uses `shutil.which("meshdb-server")`. The resolver checks
file/executable status only when called, never at import/help. It does not
download, expand shell expressions, read bootstrap/configuration contents or
search repository build directories. Executable replacement can still fail
after resolution; the CLI reports the same static failure.

Configuration background was read in `plans/distributed-contracts.md`, section
"Configuration and commands", and `scripts/notes/DN-A03.md`. The parent contract
uses one `storage` binding instead of the older control/objects fields. This
launcher forwards only the filename and does not interpret either schema.

## Focused evidence

Before implementation, ran:

```text
PYTHONPATH=orm/src orm/.venv/bin/python -m unittest discover -s orm/tests -p test_cli.py -v
```

All four scenarios failed because `meshdb.cli` did not exist. After implementing
the launcher, the same command passed all four tests, with the final run taking
5.090 seconds. An intermediate assertion was corrected for macOS's `/var` and
`/private/var` alias by resolving the temporary test directory.

The tests execute real subprocesses and an owned temporary executable Python
fixture. They prove literal argv forwarding with spaces and shell metacharacters,
relative explicit override precedence over PATH, PATH selection, rejection of
empty/missing/nonexecutable/directory overrides without fallback, static handling
of executable-format failure, and missing-PATH failure. They observe the
replacement PID and exit 23. The signal case waits for an atomic fixture-ready
record, sends SIGTERM to that same PID, observes signal termination and verifies
the PID is gone. Help, imports, missing arguments, unsupported commands/options
and oversized invalid input do not invoke the fixture.

Fixture state contains only synthetic argv/PID/executable identity. Children
receive an explicit environment with temporary PATH and state paths, source
PYTHONPATH and bytecode suppression. No host credential environment is inherited.
Child startup/communication and cleanup are bounded at five seconds. Cleanup
kills the owned process group if necessary and reaps the child. Fixtures use no
bootstrap contents or secret/log output.

Additional validation used the existing virtualenv interpreter to compile all
three owned Python sources via `compile`, without creating bytecode, and parsed
`orm/pyproject.toml` with `tomllib` to verify the console target. Syntax and metadata
checks passed. `git diff --check -- orm/pyproject.toml` passed, and all five owned
files passed an explicit trailing-whitespace/final-newline check.

## Limits

No dependency installation, packaging build, broad Python suite, Cargo command,
Rust edit/build, cloud call, dotenv read, delegate, branch/worktree or commit was
performed. Public `serve`, package exports, control/client transports and shared
plans are outside this slice. The Rust worker command remains unwired; real
worker registration/startup and P04b integration remain later work. Process
tests establish launcher behavior on POSIX using the Python fixture, not Rust
runtime or distributed execution behavior. Console metadata was inspected
without installing or rebuilding the package.

## Quality correction: cleanup after reaping

Independent quality review found that fixture cleanup unconditionally signaled
the old process group after `finish()` had already reaped its child. The
coordinator added a `process.returncode is None` guard before `killpg`, retaining
ProcessLookupError handling and final communicate/reap. The fixture creates no
descendants needing cleanup after the leader is reaped. This mechanical test
ownership fix does not change launcher behavior or add a duplicate helper test.

```text
PYTHONPATH=orm/src:orm/tests orm/.venv/bin/python -B -m unittest test_cli.CliTests.test_literal_arguments_override_pid_and_exit_status test_cli.CliTests.test_sigterm_reaches_replacement_without_orphan -v
```

Both existing process-lifecycle scenarios passed. Syntax and direct trailing-
whitespace/final-newline checks passed. A fresh independent quality re-check is
required; no production changes were needed.
