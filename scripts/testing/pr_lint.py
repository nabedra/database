#!/usr/bin/env python3
"""Check the evidence, design and review lines in a pull request body.

  pr_lint.py [file|-] [--base SHA --head SHA] [--changed-files FILE]

Every pull request body needs the four lines `Guarantees:`, `Evidence:`, `Not run:` and
`Reviewers:`. A fifth line, `Design:`, is required when the pull request changes `specs/**`, changes
a `## Design review` section of a plan in `plans/`, or touches a path of an L2 or L3 module in
docs/testing/modules.toml other than a Markdown file (.agents/skills/design-review/SKILL.md, "Pull
request body"). Markdown whose file name says it is a design, such as DESIGN.md, counts like code. Its
value is
spec paths with T check run URLs and their verdicts, or `none: <reason>`. The changed files come from
`git diff base...head`, or from a newline-separated --changed-files list. With a list, a changed plan
that has a `## Design review` section counts as changing it. Without either, the `Design:` line is
checked only when present.
"""

import argparse
import os
from pathlib import Path
import re
import subprocess
import sys
import tomllib


REQUIRED = ("Guarantees", "Evidence", "Not run", "Reviewers")
FIELD = re.compile(r"^(Guarantees|Evidence|Not run|Reviewers|Design):[ \t]*(.*)$")
FENCE = re.compile(r"^ {0,3}(`{3,}|~{3,})(.*)$")
LOOSE_FENCE = re.compile(r"^[ \t]*(`{3,}|~{3,})(.*)$")
REPO_ROOT = Path(__file__).resolve().parents[2]
MODULES_PATH = REPO_ROOT / "docs" / "testing" / "modules.toml"
RUN_URL = re.compile(r"https://github\.com/[^/\s]+/[^/\s]+/actions/runs/\d+")
SPEC_PATH = re.compile(r"(?<![\w/])specs/[\w./-]+")
VERDICT = re.compile(r"\b(pass|passed|fail|failed|not-run|error)\b", re.I)
HEADING = re.compile(r"^ {0,3}(#{1,6})[ \t]+(.*)$")
# `## Design review`, in any case, at any level below the title, numbered or with more words after it.
DESIGN_HEADING = re.compile(r"(?:\d[\d.]*[ \t]+)?design review\b", re.I)


def fields_of(body):
    fields = {}
    fence = ""
    for line in body.splitlines():
        marker = FENCE.match(line)
        if marker:
            run, tail = marker.groups()
            if fence and run.startswith(fence) and not tail.strip():
                fence = ""
            elif not fence:
                fence = run
            continue
        if fence:
            continue
        match = FIELD.fullmatch(line)
        if match:
            name, value = match.groups()
            fields[name] = value.strip()
    return fields


def design_value_errors(value):
    if not value:
        return ["Design: requires a value."]
    if value.casefold().startswith("none"):
        if not re.fullmatch(r"(?i)none:[ \t]*\S.*", value):
            return ["Design: write `none: <reason>` when no spec applies."]
        return []
    errors = []
    if not SPEC_PATH.search(value):
        errors.append("Design: name the spec paths under specs/.")
    if not RUN_URL.search(value):
        errors.append("Design: give the T check run URLs (https://github.com/<repo>/actions/runs/<id>).")
    if not VERDICT.search(value):
        errors.append("Design: give each run's verdict (pass, fail, not-run or error).")
    return errors


def lint(body, design_reason=None):
    """Returns the problems in `body`. `design_reason` says why a `Design:` line is required, if it is."""
    fields = fields_of(body)
    errors = []
    for name in REQUIRED:
        if name not in fields:
            errors.append(f"Missing {name}: line.")
        elif not fields[name]:
            errors.append(f"{name}: requires a value.")
        elif name == "Reviewers" and fields[name].casefold() == "none":
            errors.append("Reviewers: must name a reviewer or say review is pending. 'none' is invalid.")
    if "Design" in fields:
        errors.extend(design_value_errors(fields["Design"]))
    elif design_reason:
        errors.append(f"Missing Design: line ({design_reason}). Give spec paths with T check run URLs "
                      "and verdicts, or `none: <reason>`.")
    return errors


# --------------------------------------------------------------------------------------------
# When the Design: line is required.

def _glob_regex(pattern):
    out, i = [], 0
    while i < len(pattern):
        if pattern.startswith("**/", i):
            out.append("(?:.*/)?")
            i += 3
        elif pattern.startswith("**", i):
            out.append(".*")
            i += 2
        elif pattern[i] == "*":
            out.append("[^/]*")
            i += 1
        elif pattern[i] == "?":
            out.append("[^/]")
            i += 1
        else:
            out.append(re.escape(pattern[i]))
            i += 1
    return re.compile("^" + "".join(out) + "$")


def high_level_modules(path=MODULES_PATH):
    """[(name, level, path regexes, exclude regexes)] for the L2 and L3 modules."""
    data = tomllib.loads(Path(path).read_text(encoding="utf-8"))
    out = []
    for m in data.get("module", []):
        if m.get("level") in ("L2", "L3"):
            out.append((m["name"], m["level"], [_glob_regex(p) for p in m.get("paths", [])],
                        [_glob_regex(p) for p in m.get("exclude", [])]))
    return out


def design_section(text, fences="strict"):
    """The text of every `## Design review` section of a plan, or None.

    A section runs to the next heading of its level or above. A `#` line in a fenced block is not a heading.
    Renderers differ on fences in lists, so `fences` is the way to read them: "strict" as CommonMark does at
    the top level, "loose" with a closing fence at any indent, or "none" with every line as text."""
    out, level, fence = [], 0, ""
    for line in text.splitlines():
        marker = None if fences == "none" else (LOOSE_FENCE if fences == "loose" and fence else FENCE).match(line)
        if marker:
            run, tail = marker.groups()
            if fence and run.startswith(fence) and not tail.strip():
                fence = ""
            elif not fence and not (run[0] == "`" and "`" in tail):  # such a line is inline code
                fence = run
            else:
                marker = None
        heading = None if marker or fence else HEADING.match(line)
        if heading:
            depth = len(heading.group(1))
            if depth <= level:
                level = 0
            if not level and depth > 1 and DESIGN_HEADING.match(heading.group(2)):
                level = depth
        if level:
            out.append(line)
    return "\n".join(out).strip() or None


def design_sections(text):
    """design_section under every reading of the fences. A change in any of them is a change of the section."""
    return tuple(design_section(text, fences) for fences in ("strict", "loose", "none"))


def design_reason(changed, section_changed=(), modules=None):
    """Why the Design: line is required for these changed files, or None."""
    specs = [f for f in changed if f.startswith("specs/")]
    if specs:
        return f"the pull request changes {specs[0]}"
    if section_changed:
        return f"the pull request changes the Design review section of {section_changed[0]}"
    for f in changed:
        if f.endswith(".md") and "design" not in Path(f).stem.casefold():
            continue  # documentation, such as a module README, is not a design change
        for name, level, paths, exclude in modules or []:
            if any(p.match(f) for p in paths) and not any(e.match(f) for e in exclude):
                return f"the pull request touches {f} in {level} module {name}"
    return None


def _git(*args):
    return subprocess.run(["git", *args], cwd=REPO_ROOT, capture_output=True, text=True, check=True).stdout


def _show(rev, path):
    out = subprocess.run(["git", "show", f"{rev}:{path}"], cwd=REPO_ROOT, capture_output=True, text=True)
    return out.stdout if out.returncode == 0 else ""


def changed_from_git(base, head):
    """(changed files, plans whose Design review section changed) for base...head."""
    merge_base = _git("merge-base", base, head).strip()
    files = [f for f in _git("diff", "--name-only", "--no-renames", "-z", merge_base, head).split("\0") if f]
    sections = [f for f in files if f.startswith("plans/") and f.endswith(".md")
                and design_sections(_show(merge_base, f)) != design_sections(_show(head, f))]
    return files, sections


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("file", nargs="?", help="PR body file, or - for stdin")
    parser.add_argument("--docs-only", action="store_true", help="apply the same requirements to a docs-only PR")
    parser.add_argument("--base", help="base commit of the pull request")
    parser.add_argument("--head", help="head commit of the pull request")
    parser.add_argument("--changed-files", type=Path, help="file listing the changed paths, one per line")
    args = parser.parse_args(argv)
    if bool(args.base) != bool(args.head):
        parser.error("--base and --head go together")
    try:
        if args.file == "-":
            body = sys.stdin.read()
        elif args.file:
            body = Path(args.file).read_text(encoding="utf-8")
        elif "PR_BODY" in os.environ:
            body = os.environ["PR_BODY"]
        else:
            body = sys.stdin.read()
        changed, sections = [], []
        if args.changed_files:
            changed = [l.strip() for l in args.changed_files.read_text(encoding="utf-8").splitlines() if l.strip()]
            sections = [f for f in changed if f.startswith("plans/") and f.endswith(".md")
                        and any(design_sections((REPO_ROOT / f).read_text(encoding="utf-8")
                                                if (REPO_ROOT / f).exists() else ""))]
        if args.base:
            changed, sections = changed_from_git(args.base, args.head)
        reason = design_reason(changed, sections, high_level_modules() if changed else None)
    except (OSError, subprocess.CalledProcessError, tomllib.TOMLDecodeError) as error:
        print(f"pr_lint: {error}", file=sys.stderr)
        return 2
    errors = lint(body, reason)
    for error in errors:
        print(error, file=sys.stderr)
    if not errors:
        extra = f" and the Design: line ({reason})" if reason else ""
        print(f"PR body contains the four required lines{extra}.")
    return int(bool(errors))


if __name__ == "__main__":
    sys.exit(main())
