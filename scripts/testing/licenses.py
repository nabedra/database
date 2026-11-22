#!/usr/bin/env python3
"""Audit dependency licenses and scan unpacked Cargo registry license files."""

import argparse
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import tomllib


ROOT = Path(__file__).resolve().parents[2]
ALLOWED = {
    "MIT", "Apache-2.0", "Apache-2.0 WITH LLVM-exception", "BSD-2-Clause",
    "BSD-3-Clause", "ISC", "Unicode-3.0", "Unicode-DFS-2016", "Zlib",
    "CC0-1.0", "Unlicense", "BSL-1.0", "MPL-2.0", "0BSD",
    "CDLA-Permissive-2.0",
    # University of Illinois/NCSA, permissive and OSI-approved; libfuzzer-sys bundles LLVM libFuzzer.
    "NCSA",
}
SPDX_TOKEN = re.compile(r"[^\s()]+|[()]")
SPDX_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9.:-]*\+?")
AI_TERMS = re.compile(
    r"\b(?:artificial[- ]intelligence|machine[- ]learning|deep[- ]learning|neural networks?|"
    r"train(?:s|ed|ing)?|generative|A\.I\.|AI(?:[- ]assisted|[- ]generated)?|ML|"
    r"(?:large )?language models?|LLMs?|text and data mining)(?![A-Za-z])", re.I,
)
RESTRICTION_TERMS = re.compile(
    r"\b(?:prohibit(?:s|ed|ion)?|restrict(?:s|ed|ion|ions)?|forbid(?:s|den)?|ban(?:s|ned)?|"
    r"not (?:be )?(?:permitted|allowed|granted|licensed|authori[sz]ed)|"
    r"(?:must|may|shall|will|do|does|can) not|cannot|can't|no (?:right|permission|license|licence)|"
    r"(?:without|requir(?:e|es|ed|ing)) (?:the )?(?:prior |express |explicit )*(?:written )?(?:permission|consent)|"
    r"exclud(?:e|es|ed|ing)|except|opt(?:s|ed)?[- ]out|reserv(?:e|es|ed)|no)\b", re.I,
)
# File names, compared case-insensitively, that carry a crate's license terms.
LICENSE_FILES = ("LICENSE", "LICENCE", "COPYING", "UNLICENSE", "NOTICE", "COPYRIGHT")
RULES = {"license", "ai-restriction"}


@dataclass(frozen=True)
class Dependency:
    ecosystem: str
    name: str
    version: str
    license: str | None
    registry: bool = False


@dataclass(frozen=True)
class Finding:
    ecosystem: str
    name: str
    version: str
    rule: str
    detail: str
    file: str | None = None


def license_allowed(expression):
    """An OR offers a choice. An AND requires compliance with both terms."""
    if not isinstance(expression, str):
        return False
    tokens = SPDX_TOKEN.findall(expression)
    position = 0

    def take(token):
        nonlocal position
        if position < len(tokens) and tokens[position] == token:
            position += 1
            return True
        return False

    def identifier():
        nonlocal position
        if position >= len(tokens):
            raise ValueError("missing SPDX identifier")
        token = tokens[position]
        position += 1
        if token in {"AND", "OR", "WITH"} or not SPDX_ID.fullmatch(token):
            raise ValueError("invalid SPDX identifier")
        return token

    def term():
        if take("("):
            result = alternatives()
            if not take(")"):
                raise ValueError("missing closing parenthesis")
            return result
        license_id = identifier()
        if take("WITH"):
            license_id += " WITH " + identifier()
        return license_id in ALLOWED

    def conjunction():
        result = term()
        while take("AND"):
            result = term() and result
        return result

    def alternatives():
        result = conjunction()
        while take("OR"):
            result = conjunction() or result
        return result

    try:
        return alternatives() and position == len(tokens)
    except ValueError:
        return False


def restriction_matches(text):
    # Spell A.I. as AI so its periods do not read as sentence ends.
    normalized = re.sub(r"\bA\.I\.", "AI", " ".join(text.split()), flags=re.I)
    restrictions = list(RESTRICTION_TERMS.finditer(normalized))
    matches = set()
    for ai in AI_TERMS.finditer(normalized):
        for restriction in restrictions:
            start = min(ai.start(), restriction.start())
            end = max(ai.end(), restriction.end())
            evidence = normalized[start:end]
            if len(evidence) <= 120 and not re.search(r"[.!?] ", evidence):
                matches.add(evidence)
    return sorted(matches)


def cargo_metadata(workspace):
    command = ["cargo", "metadata", "--format-version", "1", "--locked", "--all-features"]
    process = subprocess.run(command, cwd=workspace, text=True, capture_output=True)
    if process.returncode:
        raise ValueError(f"{workspace}: cargo metadata failed: {process.stderr.strip()}")
    return json.loads(process.stdout)


def cargo_dependencies(metadata):
    for package in metadata["packages"]:
        source = package.get("source") or ""
        expression = package.get("license")
        if expression:
            # Cargo still accepts the legacy slash spelling for a license choice.
            expression = expression.replace("/", " OR ")
        yield Dependency(
            "cargo", package["name"], package["version"], expression,
            source.startswith(("registry+", "sparse+")),
        )


def npm_dependencies(lockfile):
    for path, package in lockfile["packages"].items():
        if package.get("link"):
            continue
        name = package.get("name") or path.rsplit("node_modules/", 1)[-1]
        yield Dependency("npm", name, package["version"], package.get("license"))


def python_project(pyproject):
    project = pyproject["project"]
    if project.get("dependencies"):
        raise ValueError("orm/python declares runtime dependencies without an audited lockfile")
    notices = []
    if pyproject.get("build-system", {}).get("requires") or project.get("optional-dependencies"):
        notices.append("orm/python has no lockfile. Build and optional dependencies are unaudited.")
    return Dependency("python", project["name"], project["version"], project.get("license")), notices


def load_exceptions(path):
    with path.open("rb") as source:
        entries = tomllib.load(source).get("exception", [])
    if not isinstance(entries, list):
        raise ValueError(f"{path}: expected [[exception]] rows")
    exceptions = {}
    for row in entries:
        fields = ("ecosystem", "name", "version", "rule", "reason")
        if any(not isinstance(row.get(field), str) or not row[field].strip() for field in fields):
            raise ValueError(f"{path}: each exception needs {', '.join(fields)}")
        if row["rule"] not in RULES or type(row.get("expiry")) is not date:
            raise ValueError(f"{path}: exception for {row['name']} needs a valid rule and TOML expiry date")
        key = tuple(row[field] for field in fields[:-1])
        if key in exceptions:
            raise ValueError(f"{path}: duplicate exception for {key}")
        exceptions[key] = row
    return exceptions


def audit(dependencies, registry_src, exceptions, today):
    report = {"checked": 0, "license_files_scanned": 0, "failures": [], "exceptions": []}
    for dep in sorted(set(dependencies), key=lambda item: (
        item.ecosystem, item.name, item.version, item.license or "", item.registry
    )):
        report["checked"] += 1
        findings = []
        if not license_allowed(dep.license):
            findings.append(Finding(
                dep.ecosystem, dep.name, dep.version, "license",
                dep.license or "missing license expression",
            ))
        if dep.registry and not any(registry_src.glob(f"*/{dep.name}-{dep.version}")):
            # Fail closed: a missing source means its license files were never scanned.
            findings.append(Finding(
                dep.ecosystem, dep.name, dep.version, "ai-restriction",
                f"source not unpacked under {registry_src}; run cargo fetch",
            ))
        elif dep.registry:
            for path in sorted(registry_src.glob(f"*/{dep.name}-{dep.version}/*")):
                if not path.is_file() or not path.name.upper().startswith(LICENSE_FILES):
                    continue
                report["license_files_scanned"] += 1
                text = path.read_text(encoding="utf-8", errors="replace")
                findings.extend(
                    Finding(dep.ecosystem, dep.name, dep.version, "ai-restriction", match, str(path))
                    for match in restriction_matches(text)
                )
        for finding in findings:
            row = exceptions.get((dep.ecosystem, dep.name, dep.version, finding.rule))
            record = asdict(finding)
            if row:
                record["reason"] = row["reason"]
                record["expiry"] = row["expiry"].isoformat()
                if today <= row["expiry"]:
                    report["exceptions"].append(record)
                    continue
            report["failures"].append(record)
    return report


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--json", action="store_true", help="emit a JSON report")
    parser.add_argument("--exceptions", type=Path, default=ROOT / "docs/testing/license-exceptions.toml")
    args = parser.parse_args(argv)
    try:
        exceptions = load_exceptions(args.exceptions)
        dependencies = []
        for workspace in ("server", "server/events", "server/fuzz"):
            if workspace == "server/fuzz" and not (ROOT / workspace / "Cargo.toml").is_file():
                continue
            dependencies.extend(cargo_dependencies(cargo_metadata(ROOT / workspace)))
        lockfile = ROOT / "orm/typescript/package-lock.json"
        dependencies.extend(npm_dependencies(json.loads(lockfile.read_text(encoding="utf-8"))))
        with (ROOT / "orm/python/pyproject.toml").open("rb") as source:
            project, notices = python_project(tomllib.load(source))
        dependencies.append(project)
        cargo_home = Path(os.environ.get("CARGO_HOME", Path.home() / ".cargo"))
        report = audit(dependencies, cargo_home / "registry/src", exceptions, datetime.now(timezone.utc).date())
        report["notices"] = notices
    except (OSError, ValueError, KeyError, TypeError) as error:
        if args.json:
            print(json.dumps({"error": str(error)}))
        else:
            print(f"licenses: {error}", file=sys.stderr)
        return 2

    if args.json:
        print(json.dumps(report, indent=2))
    else:
        for status in ("failures", "exceptions"):
            for item in report[status]:
                location = f" ({item['file']})" if item["file"] else ""
                detail = item["detail"]
                if "expiry" in item:
                    label = "exception expires" if status == "exceptions" else "exception expired"
                    detail += f" [{label} {item['expiry']}: {item['reason']}]"
                print(f"{status}: {item['ecosystem']} {item['name']}@{item['version']} "
                      f"{item['rule']}{location}: {detail}")
        for notice in report["notices"]:
            print(notice)
        print(f"Checked {report['checked']} packages and {report['license_files_scanned']} license files. "
              f"{len(report['failures'])} failures, {len(report['exceptions'])} exceptions.")
    return int(bool(report["failures"]))


if __name__ == "__main__":
    sys.exit(main())
