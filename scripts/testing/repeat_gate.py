#!/usr/bin/env python3
"""Repeat each test selected by catalog.py new --json."""

import argparse
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import tomllib


ROOT = Path(__file__).resolve().parents[2]
LANGUAGES = ("rust", "python", "node")
RUST_PASS = re.compile(r"^test result: ok\. 1 passed; 0 failed; 0 ignored;", re.M)
RUST_TEST_NAME = re.compile(r"^(.+): test$", re.M)
PYTHON_PASS = re.compile(r"^Ran 1 test in .+\n\nOK\s*$", re.M)


def rust_command(file, name, root, features):
    manifest = next((parent / "Cargo.toml" for parent in file.parents
                     if (root / parent / "Cargo.toml").is_file()), None)
    if manifest is None:
        raise ValueError(f"no Cargo.toml found for {file}")
    source = file.relative_to(manifest.parent)
    command = ["cargo", "test", "--locked", "--manifest-path", str(manifest)]
    if manifest == Path("server/Cargo.toml"):
        if features is None:
            with (root / manifest).open("rb") as stream:
                tests = tomllib.load(stream).get("test", [])
            if source.parts[0] == "tests":
                target = Path(source.parts[1]).stem
                features = next(
                    (",".join(test.get("required-features", [])) for test in tests if test["name"] == target),
                    "",
                ) or ("oltp" if target.startswith(("oltp_", "kv_")) else None)
            elif name.startswith("cache::") or source.parts[1] in {"cache", "cache.rs"}:
                features = "cache"
            elif name.startswith("oltp") or source.parts[1].startswith("oltp"):
                features = "oltp"
        if features:
            command.extend(["--no-default-features", "--features", features])
    if source.parts[0] == "tests":
        command.extend(["--test", Path(source.parts[1]).stem])
    elif source.parts[0] == "examples":
        command.extend(["--example", Path(source.parts[1]).stem])
    elif source.parts[:2] == ("src", "bin"):
        command.extend(["--bin", Path(source.parts[2]).stem])
    elif source == Path("src/main.rs"):
        with (root / manifest).open("rb") as stream:
            package = tomllib.load(stream)
        target = next(
            (binary["name"] for binary in package.get("bin", []) if binary.get("path") == str(source)),
            package["package"]["name"],
        )
        command.extend(["--bin", target])
    else:
        command.append("--lib")
    return [*command, name, "--", "--exact", "--include-ignored", "--test-threads=1"]


def test_command(test, root=ROOT, features=None):
    test_id, kind, file = test["id"], language(test), Path(test["file"])
    if file.is_absolute() or ".." in file.parts:
        raise ValueError(f"catalog file must be relative to the repository: {file}")
    real = test_id.startswith(("R:", "real:"))
    name = test_id.split(":", 1)[1] if real else test_id
    name = name.removeprefix(f"{file.as_posix()}::")
    if kind not in LANGUAGES or not name.strip() or name.startswith("-"):
        raise ValueError(f"invalid catalog test: {test}")
    if kind == "rust":
        return rust_command(file, name, root, features), root, real
    if kind == "python":
        name = name.replace("::", ".")
        if not name.startswith(file.stem + "."):
            name = f"{file.stem}.{name}"
        return [sys.executable, "-m", "unittest", name], root / file.parent, real
    escaped = re.sub(r"([\\^$.*+?()[\]{}|])", r"\\\1", name)
    return ["node", "--test", "--test-reporter=tap", "--test-name-pattern", f"^{escaped}$",
            "--", str(file)], root, real


def language(test):
    # Early catalog.py versions used lang and returned a bare array.
    return test.get("kind", test.get("lang"))


def resolve_rust_test(file, selector, names):
    leaf, _, ordinal = selector.partition("#")
    if selector in names:
        return selector
    # A catalog may give a path relative to the file's module, such as tests::name.
    matches = [name for name in names if name == leaf or name.endswith("::" + leaf)]
    if len(matches) > 1:
        parts = file.with_suffix("").parts
        if "src" in parts:
            module = parts[parts.index("src") + 1:]
        elif "tests" in parts:
            module = parts[parts.index("tests") + 2:]
        else:
            module = ()
        if module and module[-1] in {"lib", "main", "mod"}:
            module = module[:-1]
        prefix = "::".join(module)
        matches = [name for name in matches if not prefix or name.startswith(prefix + "::")]
    if len(matches) == 1 and not ordinal:
        return matches[0]
    available = f" (matches: {', '.join(matches)})" if matches else ""
    raise ValueError(f"{file}::{selector}: could not resolve one Rust test{available}; supply the full test name")


def assertion_ran(kind, output):
    if kind == "rust":
        return bool(RUST_PASS.search(output))
    if kind == "python":
        return bool(PYTHON_PASS.search(output))
    # Node can report the test file itself as a pass when no test name matched.
    return bool(re.search(r"^# pass [1-9]\d*$", output, re.M)
                and not re.search(r"^1\.\.0$", output, re.M))


def repeat(tests, *, root=ROOT, real_env=False, dry_run=False, features=None, run=subprocess.run):
    failed = 0
    listed = {}
    for test in tests:
        command, cwd, real = test_command(test, root, features)
        count = 5 if real_env or real else 50
        kind = language(test)
        if kind == "rust":
            name_index = command.index("--") - 1
            selector = command[name_index]
            # Resolve every selector against the listing: --exact needs the full
            # path, and a partial path would silently match zero tests.
            listing = (*command[:name_index], "--", "--list", "--format", "terse")
            if listing not in listed:
                if dry_run:
                    print(f"Resolve Rust test names with {shlex.join(listing)}")
                else:
                    process = run(listing, cwd=cwd, text=True,
                                  stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                    if process.returncode:
                        raise ValueError(f"could not list Rust tests for {test['file']}:\n{process.stdout.rstrip()}")
                    listed[listing] = RUST_TEST_NAME.findall(process.stdout)
            if dry_run:
                command[name_index] = f"<resolved {selector}>"
                listed[listing] = []
            else:
                command[name_index] = resolve_rust_test(Path(test["file"]), selector, listed[listing])
        if dry_run:
            print(f"{test['id']}: {count} × (cd {shlex.quote(str(cwd))} && {shlex.join(command)})")
            continue
        passed = 0
        first_failure = None
        for attempt in range(1, count + 1):
            process = run(command, cwd=cwd, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
            if process.returncode == 0 and assertion_ran(kind, process.stdout):
                passed += 1
            elif first_failure is None:
                reason = f"exit {process.returncode}" if process.returncode else "no matching test passed"
                first_failure = f"attempt {attempt}: {reason}\n{process.stdout.rstrip()}"
        print(f"{test['id']}: {passed}/{count} passed, {count - passed} failed")
        if first_failure is not None:
            print(first_failure, file=sys.stderr)
            failed += 1
    if not dry_run:
        print(f"{len(tests) - failed}/{len(tests)} tests passed the repetition gate.")
    return int(bool(failed))


def main(argv=None):
    parser = argparse.ArgumentParser(
        description=__doc__,
        epilog="Use an R: prefix on a catalog ID to repeat only that test five times.",
    )
    parser.add_argument("catalog", nargs="?", default="-", help="catalog JSON file, or - for stdin")
    parser.add_argument("--real-env", action="store_true", help="run each test 5 times")
    parser.add_argument("--dry-run", action="store_true", help="print commands without running them")
    parser.add_argument("--features", help="override engine features for tests in server/Cargo.toml")
    args = parser.parse_args(argv)
    os.environ.setdefault("CARGO_BUILD_JOBS", "2")  # as run_check.py does
    try:
        if args.catalog == "-":
            catalog = json.load(sys.stdin)
        else:
            catalog = json.loads(Path(args.catalog).read_text(encoding="utf-8"))
        tests = catalog["tests"] if isinstance(catalog, dict) else catalog
        if not isinstance(tests, list):
            raise ValueError("catalog JSON must contain a tests array")
        return repeat(tests, real_env=args.real_env, dry_run=args.dry_run, features=args.features)
    except (OSError, ValueError, KeyError, TypeError, IndexError, AttributeError) as error:
        print(f"repeat_gate: {error}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    sys.exit(main())
