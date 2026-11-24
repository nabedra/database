#!/usr/bin/env python3
"""Check the server's crate graph and count module edges between future crates.

Commands:
  crate_graph.py layers   [--manifest-dir server]      every workspace dependency points down
  crate_graph.py edges    [--expanded] [--json]        module edge counts (phase 8 design, 2.1)
  crate_graph.py ratchet  [--write]                    upward module edges stay at or below the record
  crate_graph.py owners   --edge SRC:DST [--json]      files on both ends of one module edge
  crate_graph.py proofs                                every `impl ...Proof for` sits in the allowed crate

The rules live in docs/testing/crate-graph.toml. Python standard library only.
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
from dataclasses import dataclass
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tomllib


ROOT = Path(__file__).resolve().parents[2]
CONFIG = ROOT / "docs" / "testing" / "crate-graph.toml"
ANY = "*"
RATCHET_BEGIN = "# BEGIN ratchet"
RATCHET_END = "# END ratchet"
# The literal method of the phase 8 design, 2.1: `git grep -hoE "crate::<dst>\b"`, comments
# included, no boundary before `crate` (so `$crate::` counts too).
LITERAL = re.compile(r"crate::([A-Za-z_][A-Za-z0-9_]*)")
SKIP_DIRS = {"target", ".git", "node_modules"}
METHODS = ("literal", "expanded")


class GraphError(Exception):
    pass


# Configuration.


@dataclass
class Config:
    layers: list[tuple[str, list[str]]]
    root: str
    modules: dict[str, str]
    default_package: str
    planned: dict[str, dict[str, list[str]]]
    proofs: dict
    plant: dict
    ratchet: dict[str, dict[str, int]]
    ratchet_sha: str = ""
    path: Path = CONFIG

    @property
    def ranks(self) -> dict[str, int]:
        return {name: index for index, (_, names) in enumerate(self.layers) for name in names}


def load_config(path: Path = CONFIG) -> Config:
    try:
        with path.open("rb") as source:
            data = tomllib.load(source)
    except (OSError, tomllib.TOMLDecodeError) as error:
        raise GraphError(f"{path}: {error}") from error
    layers = []
    seen = set()
    for row in data.get("layer", []):
        name, packages = row.get("name"), row.get("packages")
        if not isinstance(name, str) or not isinstance(packages, list) or not packages:
            raise GraphError(f"{path}: each [[layer]] needs a name and a non-empty packages list")
        for package in packages:
            if package in seen:
                raise GraphError(f"{path}: package {package} is in two layers")
            seen.add(package)
        layers.append((name, list(packages)))
    if not layers:
        raise GraphError(f"{path}: no [[layer]] tables")
    modules_table = data.get("modules", {})
    root = modules_table.get("root", "")
    if not isinstance(root, str) or not root:
        raise GraphError(f"{path}: [modules] needs root, the server crate's source directory")
    mapping = dict(modules_table.get("layer", {}))
    default = modules_table.get("default", "")
    for package in [default, *mapping.values()]:
        if package not in seen:
            raise GraphError(f"{path}: package {package!r} has no layer")
    planned = {}
    for name, row in data.get("planned", {}).items():
        if name not in seen:
            raise GraphError(f"{path}: planned package {name!r} has no layer")
        planned[name] = {kind: list(row.get(kind, [])) for kind in ("normal", "build", "dev")}
    ratchet = data.get("ratchet", {})
    counts = {method: dict(ratchet.get(method, {})) for method in METHODS}
    for table in counts.values():
        if any(not isinstance(value, int) or isinstance(value, bool) or value < 0 for value in table.values()):
            raise GraphError(f"{path}: ratchet counts must be nonnegative integers")
    return Config(
        layers=layers,
        root=root,
        modules=mapping,
        default_package=default,
        planned=planned,
        proofs=data.get("proofs", {}),
        plant=data.get("plant", {}),
        ratchet=counts,
        ratchet_sha=ratchet.get("sha", ""),
        path=path,
    )


# Case 1: layers over `cargo metadata`.


def cargo() -> str | None:
    return shutil.which("cargo")


def cargo_metadata(manifest_dir: Path, *, resolve: bool = False) -> subprocess.CompletedProcess:
    """Run `cargo metadata` offline. `resolve` also resolves the graph, which rejects cycles."""
    command = [cargo() or "cargo", "metadata", "--format-version", "1", "--offline"]
    if not resolve:
        command.append("--no-deps")
    return subprocess.run(
        command, cwd=manifest_dir, text=True, encoding="utf-8", errors="replace",
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=300,
    )


def workspace_metadata(manifest_dir: Path) -> dict:
    result = cargo_metadata(manifest_dir)
    if result.returncode:
        raise GraphError(f"cargo metadata failed in {manifest_dir}:\n{result.stderr.strip()}")
    return json.loads(result.stdout)


def layer_violations(metadata: dict, ranks: dict[str, int]) -> list[str]:
    """Every dependency of every kind between workspace packages must point to a lower layer.

    Packages in one layer are siblings and may not depend on each other.
    """
    members = set(metadata.get("workspace_members", []))
    packages = [p for p in metadata["packages"] if p["id"] in members] if members else metadata["packages"]
    names = {p["name"] for p in packages}
    problems = []
    for package in sorted(packages, key=lambda p: p["name"]):
        name = package["name"]
        if name not in ranks:
            problems.append(f"{name}: workspace package has no layer in crate-graph.toml")
            continue
        for dep in package.get("dependencies", []):
            target = dep["name"]
            if target not in names and target not in ranks and not dep.get("path"):
                continue  # a registry crate
            kind = dep.get("kind") or "normal"
            if target not in ranks:
                problems.append(f"{name} -> {target} ({kind}): dependency has no layer in crate-graph.toml")
            elif ranks[target] >= ranks[name]:
                relation = "the same layer" if ranks[target] == ranks[name] else "a higher layer"
                problems.append(
                    f"{name} -> {target} ({kind}): points to {relation} "
                    f"({rank_name(ranks, target)} from {rank_name(ranks, name)})"
                )
    return problems


def rank_name(ranks: dict[str, int], package: str) -> str:
    return f"layer {ranks[package]}"


# Cases 3 and 4: stub workspaces with the planned package names and path edges.


def write_stub_workspace(directory: Path, planned: dict[str, dict[str, list[str]]],
                         extra: dict[tuple[str, str], str] | None = None) -> None:
    """Write one stub package per planned crate, with path dependencies and no registry crates.

    `extra` maps (package, dependency) to a dependency kind to plant.
    """
    edges = {name: {kind: list(deps) for kind, deps in row.items()} for name, row in planned.items()}
    for (package, dependency), kind in (extra or {}).items():
        edges[package].setdefault(kind, []).append(dependency)
    members = sorted(edges)
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "Cargo.toml").write_text(
        "[workspace]\nresolver = \"2\"\nmembers = [" + ", ".join(f'"{m}"' for m in members) + "]\n",
        encoding="utf-8",
    )
    sections = {"normal": "dependencies", "build": "build-dependencies", "dev": "dev-dependencies"}
    for name in members:
        package = directory / name
        (package / "src").mkdir(parents=True, exist_ok=True)
        (package / "src" / "lib.rs").write_text("", encoding="utf-8")
        lines = [f'[package]\nname = "{name}"\nversion = "0.1.0"\nedition = "2021"\npublish = false\n']
        for kind, section in sections.items():
            deps = edges[name].get(kind, [])
            if deps:
                lines.append(f"[{section}]")
                lines += [f'{dep} = {{ path = "../{dep}" }}' for dep in deps]
                lines.append("")
        (package / "Cargo.toml").write_text("\n".join(lines) + "\n", encoding="utf-8")


# Cases 2 and owners: module edges.


@dataclass
class SourceFile:
    path: str              # repository-relative, forward slashes
    module: str            # top-level module of the server crate
    text: str


def source_files(repo: Path, root: str):
    base = repo / root
    if not base.is_dir():
        return
    for directory, dirs, files in os.walk(base):
        dirs[:] = sorted(d for d in dirs if d not in SKIP_DIRS)
        for name in sorted(files):
            if not name.endswith(".rs"):
                continue
            full = Path(directory) / name
            rel = full.relative_to(base).as_posix()
            module = rel.split("/", 1)[0].removesuffix(".rs")
            text = full.read_text(encoding="utf-8", errors="replace")
            yield SourceFile(full.relative_to(repo).as_posix(), module, text)


class Graph:
    """Edges between the top-level modules of the server crate, and the layer of each module.

    Only the server crate is scanned. Code already extracted into its own package cannot name a
    higher crate without failing to compile, so its module edges need no ratchet.
    """

    def __init__(self, config: Config, repo: Path = ROOT):
        self.config = config
        self.repo = repo
        self.ranks = config.ranks
        self.files = list(source_files(repo, config.root))
        self._counts = {}

    def bucket(self, module: str) -> str:
        """The ratchet name of a destination: the module if it is mapped, else `*`."""
        return module if module in self.config.modules else ANY

    def references(self, source: SourceFile, method: str):
        """Yield the path segments of every `crate::` reference in one file.

        `literal` is the design's 2.1 method: one per `crate::<module>` token, comments included,
        so `use crate::{a::B}` counts nothing. `expanded` strips comments and expands grouped
        imports, so `use crate::{a::{B, C}}` counts two.
        """
        if method == "literal":
            for match in LITERAL.finditer(source.text):
                yield [match.group(1)]
        elif method == "expanded":
            for path in expanded_paths(strip_comments(source.text)):
                segs = [seg for seg in path.split("::") if seg]
                if segs:
                    yield segs
        else:
            raise GraphError(f"unknown counting method {method!r}")

    def counts(self, method: str = "literal") -> dict[tuple[str, str], Counter]:
        """(src, dst) -> path -> references, with real destination module names."""
        if method not in self._counts:
            edges = defaultdict(Counter)
            for source in self.files:
                for segs in self.references(source, method):
                    dst = segs[0]
                    if dst in ("self", "super", "crate") or dst == source.module:
                        continue
                    edges[(source.module, dst)][source.path] += 1
            self._counts[method] = dict(edges)
        return self._counts[method]

    def edge_files(self, src: str, dst: str, method: str = "literal") -> Counter:
        """Per-file counts of one edge; `dst` may be `*`, every unmapped module."""
        total = Counter()
        for (s, d), files in self.counts(method).items():
            if s == src and (d == dst or (dst == ANY and self.bucket(d) == ANY)):
                total.update(files)
        return total

    def upward_pairs(self) -> dict[str, tuple[str, str]]:
        """Every (src, dst) module pair whose destination sits in a higher layer.

        Sources are the mapped modules. Destinations are the other mapped modules and `*`, every
        unmapped module of the server crate, which stays at the top.
        """
        top = self.ranks[self.config.default_package]
        pairs = {}
        for src, src_package in self.config.modules.items():
            src_rank = self.ranks[src_package]
            for dst, dst_package in self.config.modules.items():
                if dst != src and self.ranks[dst_package] > src_rank:
                    pairs[f"{src}:{dst}"] = (src, dst)
            if top > src_rank:
                pairs[f"{src}:{ANY}"] = (src, ANY)
        return dict(sorted(pairs.items()))

    def ratchet_counts(self) -> dict[str, dict[str, int]]:
        return {
            method: {key: sum(self.edge_files(*pair, method).values())
                     for key, pair in self.upward_pairs().items()}
            for method in METHODS
        }

    def expanded(self, src: str, dst: str):
        """Yield (path, segments) for every expanded reference from `src` to `dst` (`*` allowed)."""
        for source in self.files:
            if source.module != src:
                continue
            for segs in self.references(source, "expanded"):
                if segs[0] in ("self", "super", "crate") or segs[0] == src:
                    continue
                if segs[0] == dst or (dst == ANY and self.bucket(segs[0]) == ANY):
                    yield source.path, segs


def strip_comments(text: str) -> str:
    # Keep the newlines of block comments, so line numbers stay right.
    text = re.sub(r"/\*.*?\*/", lambda m: "\n" * m.group(0).count("\n"), text, flags=re.S)
    return "\n".join(line.split("//", 1)[0] for line in text.split("\n"))


def parse_group(text: str, start: int) -> tuple[list[str], int]:
    """`text[start]` is `{`. Return the top-level items of the group and the closing index."""
    depth, items, current = 0, [], []
    for index in range(start, len(text)):
        char = text[index]
        if char == "{":
            depth += 1
            if depth > 1:
                current.append(char)
        elif char == "}":
            depth -= 1
            if depth == 0:
                items.append("".join(current).strip())
                return [item for item in items if item], index
            current.append(char)
        elif char == "," and depth == 1:
            items.append("".join(current).strip())
            current = []
        else:
            current.append(char)
    return [item for item in items if item], len(text)


def expand(prefix: str, text: str) -> list[str]:
    text = " ".join(text.split())
    if not text:
        return []
    if text.startswith("{"):
        items, _ = parse_group(text, 0)
        return [path for item in items for path in expand(prefix, item)]
    brace = text.find("::{")
    if brace >= 0:
        head = text[:brace]
        items, _ = parse_group(text, brace + 2)
        joined = f"{prefix}::{head}" if prefix else head
        return [path for item in items for path in expand(joined, item)]
    path = f"{prefix}::{text}" if prefix else text
    return [re.split(r"\s+as\s+", path)[0].strip()]


def expanded_paths(text: str) -> list[str]:
    paths = []
    for match in re.finditer(r"\bcrate::", text):
        index = match.end()
        if text[index:index + 1] == "{":
            items, _ = parse_group(text, index)
            paths += [path for item in items for path in expand("", item)]
            continue
        head = re.match(r"[\w:]*(::\{)?", text[index:]).group(0)
        if head.endswith("::{"):
            items, _ = parse_group(text, index + len(head) - 1)
            paths += [path for item in items for path in expand(head[:-3], item)]
        else:
            paths.append(head)
    return paths


def destination_file(repo: Path, root: str, segs: list[str]) -> str | None:
    """The deepest module file on the path, where the item is declared or re-exported."""
    base = repo / root
    found = None
    for depth in range(1, len(segs) + 1):
        stem = base.joinpath(*segs[:depth])
        for candidate in (stem.with_suffix(".rs"), stem / "mod.rs"):
            if candidate.is_file():
                found = candidate.relative_to(repo).as_posix()
                break
        else:
            if depth > 1 and not stem.is_dir():
                break
    return found


def owners(graph: Graph, src: str, dst: str) -> dict:
    literal = graph.edge_files(src, dst, "literal")
    expanded = Counter()
    destinations = Counter()
    items = Counter()
    for path, segs in graph.expanded(src, dst):
        expanded[path] += 1
        target = destination_file(graph.repo, graph.config.root, segs)
        if target:
            destinations[target] += 1
        items[segs[-1] if len(segs) > 1 else "<module>"] += 1
    sources = sorted(set(literal) | set(expanded), key=lambda p: (-expanded[p], -literal[p], p))
    return {
        "edge": f"{src}:{dst}",
        "literal": sum(literal.values()),
        "expanded": sum(expanded.values()),
        "sources": [{"path": p, "literal": literal[p], "expanded": expanded[p]} for p in sources],
        "destinations": [{"path": p, "expanded": n} for p, n in
                         sorted(destinations.items(), key=lambda row: (-row[1], row[0]))],
        "items": items.most_common(),
    }


# The ratchet record.


def ratchet_problems(config: Config, counts: dict[str, dict[str, int]]) -> tuple[list[str], list[str]]:
    """Return (failures, notes). A count above its record, or any unrecorded pair, fails."""
    failures, notes = [], []
    for method in METHODS:
        recorded_counts = config.ratchet.get(method, {})
        for key, count in counts[method].items():
            if key not in recorded_counts:
                failures.append(f"{key} ({method}): {count} references, but the pair is not recorded "
                                f"(run crate_graph.py ratchet --write)")
                continue
            recorded = recorded_counts[key]
            if count > recorded:
                what = "was 0 and must stay 0" if recorded == 0 else f"is above the recorded {recorded}"
                failures.append(f"{key} ({method}): {count} references {what}")
            elif count < recorded:
                notes.append(f"{key} ({method}): {count} references, below the recorded {recorded}; "
                             f"lower the record")
        for key in sorted(set(recorded_counts) - set(counts[method])):
            failures.append(f"{key} ({method}): recorded, but no longer an upward pair in crate-graph.toml")
    return failures, notes


def render_ratchet(counts: dict[str, dict[str, int]], sha: str) -> str:
    lines = [
        RATCHET_BEGIN + " (generated by `python3 scripts/testing/crate_graph.py ratchet --write`)",
        "# References from each mapped module into a module of a higher layer. `*` is every unmapped",
        "# module of the server crate. A count may fall but never rise, and a pair at 0 stays at 0.",
        "# `literal` counts `crate::<module>` tokens with comments (the design's 2.1 method); `expanded`",
        "# strips comments and expands grouped imports, so `use crate::{a::B}` cannot hide an edge.",
        "[ratchet]",
        f'sha = "{sha}"',
    ]
    for method in METHODS:
        lines += ["", f"[ratchet.{method}]"]
        lines += [f'"{key}" = {count}' for key, count in counts[method].items()]
    lines.append(RATCHET_END)
    return "\n".join(lines) + "\n"


def write_ratchet(path: Path, counts: dict[str, int], sha: str) -> None:
    text = path.read_text(encoding="utf-8")
    start, end = text.find(RATCHET_BEGIN), text.find(RATCHET_END)
    block = render_ratchet(counts, sha)
    if start < 0 or end < start:
        text = text.rstrip("\n") + "\n\n" + block
    else:
        end = text.find("\n", end)
        text = text[:start] + block + (text[end + 1:] if end >= 0 else "")
    path.write_text(text, encoding="utf-8")


# Case 6: proof impls.

IMPL = re.compile(r"\bimpl\b")


def proof_impls(text: str, additional=()) -> list[tuple[int, str]]:
    """Return (line, trait) for proof and explicitly named implementations."""
    found = []
    text = strip_comments(text)
    for match in IMPL.finditer(text):
        index = match.end()
        # Skip the impl's own generic parameters, which may nest angle brackets.
        rest = text[index:]
        stripped = rest.lstrip()
        offset = len(rest) - len(stripped)
        if stripped.startswith("<"):
            depth = 0
            for position, char in enumerate(stripped):
                depth += char == "<"
                depth -= char == ">" and stripped[position - 1] != "-"   # not the `>` of `->`
                if depth == 0:
                    offset += position + 1
                    break
        header = re.match(r"([^{;]*?)\s+for\s", text[index + offset:])
        if not header:
            continue
        trait = re.sub(r"<.*", "", header.group(1).strip().lstrip("!")).strip()
        name = trait.split("::")[-1]
        if name in additional or re.fullmatch(r"[A-Za-z_]\w*Proof", name):
            found.append((text.count("\n", 0, match.start()) + 1, trait))
    return found


def proof_report(repo: Path, config: Config, *, runtime: bool = False) -> dict:
    allowed = config.proofs.get("allowed", [])
    runtime_allowed = config.proofs.get("runtime_allowed", {}) if runtime else {}
    scan = config.proofs.get("scan", ["server"])
    impls, outside = [], []
    for top in scan:
        base = repo / top
        if not base.is_dir():
            continue
        for directory, dirs, files in os.walk(base):
            dirs[:] = sorted(d for d in dirs if d not in SKIP_DIRS)
            for name in sorted(files):
                if not name.endswith(".rs"):
                    continue
                full = Path(directory) / name
                rel = full.relative_to(repo).as_posix()
                source = full.read_text(encoding="utf-8", errors="replace")
                for line, trait in proof_impls(source, runtime_allowed):
                    trait_name = trait.split("::")[-1]
                    if runtime and trait_name not in runtime_allowed:
                        continue
                    row = f"{rel}:{line}: impl {trait}"
                    impls.append(row)
                    if runtime:
                        permitted = rel == runtime_allowed[trait_name]
                    else:
                        permitted = any(rel.startswith(prefix) for prefix in allowed)
                    if not permitted:
                        outside.append(row)
    return {"impls": impls, "outside": outside, "allowed": allowed}


# Command line.


def git_sha(repo: Path) -> str:
    try:
        return subprocess.run(["git", "rev-parse", "HEAD"], cwd=repo, text=True,
                              capture_output=True, check=True).stdout.strip()
    except (OSError, subprocess.CalledProcessError):
        return ""


def parse_edge(text: str) -> tuple[str, str]:
    src, sep, dst = text.partition(":")
    if not sep or not src or not dst:
        raise argparse.ArgumentTypeError(f"expected SRC:DST, got {text!r}")
    return src, dst


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--config", type=Path, default=CONFIG)
    parser.add_argument("--repo", type=Path, default=ROOT, help="repository root (default: this checkout)")
    sub = parser.add_subparsers(dest="command", required=True)
    layers = sub.add_parser("layers", help="check workspace dependencies against the layers")
    layers.add_argument("--manifest-dir", type=Path, help="Cargo workspace directory (default: <repo>/server)")
    edges = sub.add_parser("edges", help="print module edge counts")
    edges.add_argument("--expanded", action="store_true", help="also count expanded paths, comments stripped")
    edges.add_argument("--module", action="append", help="only edges from or to this module (repeatable)")
    edges.add_argument("--json", action="store_true")
    ratchet = sub.add_parser("ratchet", help="compare upward module edges with the record")
    ratchet.add_argument("--write", action="store_true", help="rewrite the record with today's counts")
    own = sub.add_parser("owners", help="list the files on both ends of one module edge")
    own.add_argument("--edge", type=parse_edge, required=True, action="append", help="SRC:DST, DST may be *")
    own.add_argument("--json", action="store_true")
    sub.add_parser("proofs", help="check where `impl ...Proof for` blocks live")
    args = parser.parse_args(argv)

    try:
        config = load_config(args.config)
        repo = args.repo.resolve()
        if args.command == "layers":
            metadata = workspace_metadata(args.manifest_dir or repo / "server")
            problems = layer_violations(metadata, config.ranks)
            for problem in problems:
                print(f"FAIL {problem}")
            names = sorted(p["name"] for p in metadata["packages"])
            print(f"{'FAIL' if problems else 'PASS'} layers: {len(names)} packages ({', '.join(names)})")
            return int(bool(problems))
        graph = Graph(config, repo)
        if args.command == "edges":
            rows = []
            literal, expanded = graph.counts("literal"), graph.counts("expanded")
            for src, dst in sorted(set(literal) | (set(expanded) if args.expanded else set())):
                if args.module and src not in args.module and dst not in args.module:
                    continue
                files = literal.get((src, dst), Counter())
                row = {"edge": f"{src}:{dst}", "literal": sum(files.values()), "files": len(files)}
                if args.expanded:
                    row["expanded"] = sum(expanded.get((src, dst), Counter()).values())
                rows.append(row)
            if args.json:
                print(json.dumps(rows, indent=2))
            else:
                for row in rows:
                    extra = f" expanded={row['expanded']}" if args.expanded else ""
                    print(f"{row['edge']}: literal={row['literal']} files={row['files']}{extra}")
            return 0
        if args.command == "ratchet":
            counts = graph.ratchet_counts()
            if args.write:
                sha = git_sha(repo)
                write_ratchet(args.config, counts, sha)
                print(f"wrote {len(counts['literal'])} pairs measured on {sha or 'an unknown revision'} to {args.config}")
                return 0
            failures, notes = ratchet_problems(config, counts)
            for key in graph.upward_pairs():
                cells = [f"{method} {counts[method][key]} (recorded {config.ratchet[method].get(key, 'none')})"
                         for method in METHODS]
                print(f"{key}: " + ", ".join(cells))
            for line in notes:
                print(f"NOTE {line}")
            for line in failures:
                print(f"FAIL {line}")
            print(f"{'FAIL' if failures else 'PASS'} ratchet: {len(graph.upward_pairs())} upward pairs, "
                  f"{sum(counts['literal'].values())} literal and {sum(counts['expanded'].values())} expanded "
                  f"references (record from {config.ratchet_sha or 'unknown'})")
            return int(bool(failures))
        if args.command == "owners":
            reports = [owners(graph, src, dst) for src, dst in args.edge]
            if args.json:
                print(json.dumps(reports, indent=2))
                return 0
            for report in reports:
                print(f"== {report['edge']}: {report['literal']} literal, {report['expanded']} expanded references")
                print("-- source files (literal, expanded, path)")
                for row in report["sources"]:
                    print(f"{row['literal']:5d} {row['expanded']:5d}  {row['path']}")
                print("-- destination module files (expanded refs; the item is declared or re-exported there)")
                for row in report["destinations"]:
                    print(f"{row['expanded']:5d}  {row['path']}")
                print("-- top items")
                print(", ".join(f"{name} {count}" for name, count in report["items"][:20]))
            if len(reports) > 1:
                sets = [{row["path"] for row in r["sources"]} | {row["path"] for row in r["destinations"]}
                        for r in reports]
                shared = sorted(set.intersection(*sets))
                print(f"== files on every listed edge: {len(shared)}")
                for path in shared:
                    print(path)
            return 0
        if args.command == "proofs":
            crate = config.proofs.get("requires", "")
            if crate and not (repo / crate).exists():
                print(f"FAIL missing replication crate source: {crate}")
                return 1
            report = proof_report(repo, config)
            for row in report["outside"]:
                print(f"FAIL {row} is outside {', '.join(report['allowed'])}")
            ok = report["impls"] and not report["outside"]
            if not report["impls"]:
                print("FAIL no `impl ...Proof for` blocks found; the check would pass vacuously")
            print(f"{'PASS' if ok else 'FAIL'} proofs: {len(report['impls'])} impls")
            return int(not ok)
    except GraphError as error:
        print(f"crate_graph: {error}", file=sys.stderr)
        return 2
    return 2


if __name__ == "__main__":
    sys.exit(main())
