#!/usr/bin/env python3
"""Test catalog for the repetition gate.

  catalog.py build [--out docs/testing/catalog.json] [--check]
  catalog.py new --base SHA --head SHA [--json]

`build` scans the tracked tree and writes every test with its file and a content hash of its source (the first 16 hex digits of its SHA-256):
- Rust: each fn under `#[test]` or `#[tokio::test(...)]`, as `<path>::<fn>` (a regex scan; nested
  module names are not resolved, and a repeated name in one file gets a `#2` suffix);
- Python: each `test_*` function or method in a `test_*.py` file, as `<path>::<Class>::<name>`;
- Node: each `test(...)` or `it(...)` name in a `*.test.{mjs,js,ts,cjs}` file, as `<path>::<name>`.

`new` lists tests that are new or whose source changed between two commits, which T1 repeats
(50 runs for unit tests) before they count.
"""

from __future__ import annotations

import argparse
import ast
import hashlib
import json
import re
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUT = REPO_ROOT / "docs" / "testing" / "catalog.json"
SKIP_PREFIXES = ("scripts/testing/testdata/",)

_RUST_TEST = re.compile(
    r"#\[\s*(?:tokio::)?test\s*(?:\([^\]]*\))?\s*\]"   # the test attribute
    r'(?:\s*#\[(?:[^\]"]|"(?:\\.|[^"\\])*")*\])*'    # further attributes, such as #[ignore = "..."]
    r"\s*(?:pub(?:\([^)]*\))?\s+)?(?:async\s+)?(?:unsafe\s+)?fn\s+([A-Za-z_][A-Za-z0-9_]*)"
)
_NODE_TEST = re.compile(r"""(?<![\w.$])(?:test|it)(?:\.only|\.skip|\.todo)?\(\s*(['"`])((?:\\.|(?!\1).)*)\1""")


def lang_of(path: str) -> str | None:
    name = path.rsplit("/", 1)[-1]
    if path.startswith(SKIP_PREFIXES):
        return None
    if name.endswith(".rs"):
        return "rust"
    if name.startswith("test_") and name.endswith(".py"):
        return "python"
    if re.search(r"\.test\.(mjs|cjs|js|ts)$", name):
        return "node"
    return None


def _hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]  # enough to detect a change


def _rust_body(src: str, start: int) -> str:
    """The text from `start` to the brace closing the first `{`. Skips strings, chars and comments."""
    i, depth, n = src.find("{", start), 0, len(src)
    if i < 0:
        return src[start:]
    while i < n:
        ch = src[i]
        if ch == "/" and src.startswith("//", i):
            i = src.find("\n", i)
            i = n if i < 0 else i
            continue
        if ch == "/" and src.startswith("/*", i):
            j = src.find("*/", i + 2)
            i = n if j < 0 else j + 2
            continue
        if ch == '"':
            i += 1
            while i < n and src[i] != '"':
                i += 2 if src[i] == "\\" else 1
        elif ch == "'":
            m = re.match(r"'(?:\\.|[^\\'])'", src[i:i + 12])
            if m:
                i += m.end() - 1
        elif ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return src[start:i + 1]
        i += 1
    return src[start:]


def scan_rust(path: str, src: str) -> list[dict]:
    out = []
    for m in _RUST_TEST.finditer(src):
        out.append({"id": f"{path}::{m.group(1)}", "body": _rust_body(src, m.start())})
    return out


def scan_python(path: str, src: str) -> list[dict]:
    try:
        tree = ast.parse(src)
    except SyntaxError:
        return []
    out = []
    lines = src.encode("utf-8").splitlines(keepends=True)

    def visit(node: ast.AST, prefix: str) -> None:
        for child in ast.iter_child_nodes(node):
            if isinstance(child, ast.ClassDef):
                visit(child, f"{prefix}::{child.name}")
            elif isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef)) and child.name.startswith("test"):
                first = child.decorator_list[0].lineno if child.decorator_list else child.lineno
                segment = lines[first - 1:child.end_lineno]
                segment[-1] = segment[-1][:child.end_col_offset]
                segment[0] = segment[0][child.col_offset:]
                out.append({"id": f"{prefix}::{child.name}", "body": b"".join(segment).decode("utf-8")})

    visit(tree, path)
    return out


def scan_node(path: str, src: str) -> list[dict]:
    matches = list(_NODE_TEST.finditer(src))
    out = []
    for i, m in enumerate(matches):
        end = matches[i + 1].start() if i + 1 < len(matches) else len(src)
        out.append({"id": f"{path}::{m.group(2)}", "body": src[m.start():end]})
    return out


SCANNERS = {"rust": scan_rust, "python": scan_python, "node": scan_node}


def scan_file(path: str, src: str) -> list[dict]:
    lang = lang_of(path)
    if lang is None:
        return []
    entries, seen = [], {}
    for raw in SCANNERS[lang](path, src):
        count = seen.get(raw["id"], 0) + 1
        seen[raw["id"]] = count
        tid = raw["id"] if count == 1 else f"{raw['id']}#{count}"
        entries.append({"id": tid, "file": path, "lang": lang, "hash": _hash(raw["body"])})
    return entries


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(["git", *args], cwd=repo, check=True, capture_output=True, text=True).stdout


def build(repo: Path = REPO_ROOT) -> dict:
    files = [f for f in _git(repo, "ls-files").splitlines() if lang_of(f)]
    tests = []
    for f in files:
        p = repo / f
        if p.is_file():
            tests.extend(scan_file(f, p.read_text(encoding="utf-8", errors="replace")))
    tests.sort(key=lambda t: t["id"])
    return {"schema": "nabedra.catalog/1", "count": len(tests), "tests": tests}


def _show(repo: Path, rev: str, path: str) -> str | None:
    out = subprocess.run(["git", "show", f"{rev}:{path}"], cwd=repo, capture_output=True)
    return out.stdout.decode("utf-8", errors="replace") if out.returncode == 0 else None


def new_tests(base: str, head: str, repo: Path = REPO_ROOT) -> list[dict]:
    """Tests added or changed from base to head, scanned only in the files the diff touches."""
    changed = _git(repo, "diff", "--name-only", "--no-renames", base, head).splitlines()
    out = []
    for f in changed:
        if not lang_of(f):
            continue
        after = _show(repo, head, f)
        if after is None:
            continue
        before = _show(repo, base, f)
        old = {t["id"]: t["hash"] for t in scan_file(f, before)} if before is not None else {}
        for t in scan_file(f, after):
            if t["id"] not in old:
                out.append(dict(t, status="new"))
            elif old[t["id"]] != t["hash"]:
                out.append(dict(t, status="changed"))
    return out


def dumps(catalog: dict) -> str:
    """One test per line, so a regenerated catalog diffs by test."""
    lines = [json.dumps(t, sort_keys=True, separators=(",", ":")) for t in catalog["tests"]]
    head = f'{{"schema":{json.dumps(catalog["schema"])},"count":{catalog["count"]},"tests":[\n'
    return head + ",\n".join(lines) + "\n]}\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="cmd", required=True)
    b = sub.add_parser("build")
    b.add_argument("--repo", type=Path, default=REPO_ROOT)
    b.add_argument("--out", type=Path, default=DEFAULT_OUT)
    b.add_argument("--check", action="store_true", help="fail if the file on disk is out of date")
    n = sub.add_parser("new")
    n.add_argument("--repo", type=Path, default=REPO_ROOT)
    n.add_argument("--base", required=True)
    n.add_argument("--head", required=True)
    n.add_argument("--json", action="store_true")
    args = parser.parse_args(argv)
    if args.cmd == "build":
        text = dumps(build(args.repo))
        if args.check:
            current = args.out.read_text(encoding="utf-8") if args.out.exists() else ""
            if current != text:
                print(f"{args.out} is out of date: run catalog.py build")
                return 1
            print(f"{args.out} is up to date")
            return 0
        args.out.write_text(text, encoding="utf-8")
        print(f"wrote {args.out} ({json.loads(text)['count']} tests)")
        return 0
    found = new_tests(args.base, args.head, args.repo)
    if args.json:
        print(json.dumps(found, indent=2))
    else:
        for t in found:
            print(f"{t['status']}\t{t['lang']}\t{t['id']}")
        print(f"{len(found)} new or changed tests")
    return 0


if __name__ == "__main__":
    sys.exit(main())
