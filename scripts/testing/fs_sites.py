#!/usr/bin/env python3
"""Inventory Rust filesystem sites and enforce the OLTP ambient filesystem seam."""

import argparse
from dataclasses import dataclass
from pathlib import Path
import re


ROOT = Path(__file__).resolve().parents[2]
# Include the destinations of phase 8's mechanical crate split.
POOL_PREFIXES = ("server/src/oltp/pool", "server/oltp-storage/src/oltp/pool")
LOCAL_PREFIXES = (
    "server/src/oltp_replication/manager/local",
    "server/oltp-replication/src/oltp_replication/manager/local",
)
PATH_METHODS = {"exists", "try_exists", "symlink_metadata", "read_dir", "canonicalize", "read_link"}
PROBES = PATH_METHODS | {"is_dir", "is_file", "metadata"}
# openssl's development TLS subprocess needs a host descriptor for stderr.
HOST_OPENSSL_LOG = "let mut options = std::fs::OpenOptions::new();"
# Skip literals and comments, including nested block comments. Preserve line numbers.
RUST = re.compile(
    r'//[^\n]*|/\*|(?:br|cr|r)(?P<hash>\#*)"(?s:.*?)"(?P=hash)'
    r'|"(?:\\[\s\S]|[^"\\])*"|\'(?:\\.|[^\'\\\n])\''
    r'|[a-zA-Z_][a-zA-Z_0-9]*|::|[^\s]'
)
BLOCK = re.compile(r"/\*|\*/")


@dataclass(frozen=True)
class Site:
    line: int
    kind: str
    name: str


def tokens(source):
    offset = 0
    line = 1
    while match := RUST.search(source, offset):
        line += source.count("\n", offset, match.start())
        value = match[0]
        end = match.end()
        if value == "/*":
            depth = 1
            while depth and (marker := BLOCK.search(source, end)):
                depth += 1 if marker[0] == "/*" else -1
                end = marker.end()
            if depth:
                end = len(source)
        elif not value.startswith(("//", '"', "'")) and '"' not in value:
            yield value, line
        line += source.count("\n", match.start(), end)
        offset = end


def std_group_members(values, start):
    depth = 0
    for index, value in enumerate(values[start:], start):
        if value == "{":
            depth += 1
        elif value == "}":
            depth -= 1
            if depth == 0:
                return
        elif depth == 1 and value in {"fs", "*"} and values[index - 1] in {"{", ","}:
            yield value


def skip_test_item(values, start):
    parentheses = brackets = braces = 0
    for index in range(start, len(values)):
        value = values[index]
        if not (parentheses or brackets or braces) and value in {";", ",", "}"}:
            return index
        if value == "(":
            parentheses += 1
        elif value == ")":
            parentheses -= 1
        elif value == "[":
            brackets += 1
        elif value == "]":
            brackets -= 1
        elif value == "{":
            braces += 1
        elif value == "}":
            braces -= 1
            if not (parentheses or brackets or braces):
                return index + 1
    return len(values)


def sites(source):
    located = list(tokens(source))
    values = [value for value, _ in located]
    index = 0
    while index < len(values):
        value, line = located[index]
        if values[index:index + 7] == ["#", "[", "cfg", "(", "test", ")", "]"]:
            index = skip_test_item(values, index + 7)
            continue
        following = values[index + 1:index + 4]
        if value == "std" and following[:1] == ["::"]:
            if following[1:2] == ["fs"] or (
                following[1:2] == ["{"] and any(std_group_members(values, index + 2))
            ) or following[1:2] == ["*"]:
                yield Site(line, "direct std::fs", "std::fs")
        if value in {"fs", "File", "OpenOptions"} and len(following) == 3:
            if following[0] == "::" and following[2] == "(":
                yield Site(line, "call", f"{value}::{following[1]}")
        if value == "." and len(following) >= 2 and following[1] == "(":
            method = following[0]
            if method in PROBES:
                kind = "path method" if method in PATH_METHODS else "probe"
                yield Site(line, kind, method)
        index += 1


def in_scope(path):
    for prefix in POOL_PREFIXES:
        if path == f"{prefix}.rs" or (
            path.startswith(f"{prefix}/") and path != f"{prefix}/resources.rs"
        ):
            return True
    return any(
        path in {f"{prefix}.rs", f"{prefix}/registered.rs"}
        or path.startswith(f"{prefix}/registered/")
        for prefix in LOCAL_PREFIXES
    )


def is_test(path):
    return path.stem in {"tests", "sim_tests"} or "tests" in path.parts or path.stem.endswith("_tests")


def inventory(root):
    # Scan both current crate homes and the phase 8 extraction homes.
    for path in sorted((root / "server").glob("*/src/**/*.rs")) + sorted(
        (root / "server/src").rglob("*.rs")
    ):
        relative = path.relative_to(root)
        if is_test(relative) or relative.parts[:4] in {
            ("server", "kv", "src", "fs"), ("server", "rt", "src", "fs")
        }:
            continue
        source = path.read_text(encoding="utf-8")
        lines = source.splitlines()
        for site in sites(source):
            if (
                relative.as_posix() in {f"{prefix}.rs" for prefix in LOCAL_PREFIXES}
                and site.line > 1
                and lines[site.line - 2].strip() == "// The host child needs a real descriptor for stderr."
                and lines[site.line - 1].strip() == HOST_OPENSSL_LOG
                and site.kind == "direct std::fs"
            ):
                site = Site(site.line, "host filesystem exception", site.name)
            yield relative.as_posix(), site


def main(argv=None, root=ROOT):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="fail on direct filesystem access in converted paths")
    args = parser.parse_args(argv)
    violations = 0
    for path, site in inventory(root):
        scoped = in_scope(path)
        forbidden = scoped and site.kind in {"direct std::fs", "path method"}
        violations += forbidden
        status = "FAIL" if forbidden and args.check else "converted scope" if scoped else "outside scope"
        print(f"{path}:{site.line}: {status}: {site.kind} {site.name}")
    return int(args.check and violations > 0)


if __name__ == "__main__":
    raise SystemExit(main())
