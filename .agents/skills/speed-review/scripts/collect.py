#!/usr/bin/env python3
"""Print how work flowed through the repository since the last speed review, as Markdown.

Reads local Git history and the GitHub REST API through `gh`. Writes nothing, starts no run and
changes no branch. Run `git fetch origin` first.

  collect.py                         from the last `Head:` in log.md, or the last 7 days
  collect.py --since <sha|date>      from a commit or an ISO date
  collect.py --ci-sample 0           skip the per-job CI sample (one API request per sampled run)

The output says what was sampled. Queue time is a job's `started_at` minus its `created_at`, so
it includes neither the dispatch nor the wait for earlier jobs. Python standard library only.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import re
import statistics
import subprocess
import sys
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

SKILL = Path(__file__).resolve().parents[1]
SKILLS = ".agents/skills"
REPAIR = re.compile(r"\b(fix(es|ed|up)?|revert|flak\w*|follow-?ups?|repair|regression|retry)\b", re.I)
PULL = re.compile(r"#(\d+)")
# First match wins.
CATEGORIES = [
    ("evidence", re.compile(r"^docs/testing/(evidence|coverage-matrix|catalog\.json)")),
    ("harness", re.compile(r"^(\.agents/|\.github/|scripts/(testing|ci_)|docs/testing/|CONTRIBUTING\.md)")),
    ("specs", re.compile(r"^specs/")),
    ("docs", re.compile(r"(\.md$|^docs/|^plans/|/docs/|/notes/|LICENSE$)")),
    ("tests", re.compile(r"(^scripts/|(^|/)(tests?|testdata|fuzz|benches)(/|_|\.)|[/_]tests?\.(rs|py|ts)$"
                         r"|test_[^/]*\.py$|^server/(sim|check))")),
    ("product", re.compile(r"")),
]


def run(*args: str) -> str:
    done = subprocess.run(args, capture_output=True, text=True, encoding="utf-8", errors="replace")
    if done.returncode:
        sys.exit(f"{' '.join(args[:4])} failed: {done.stderr.strip()}")
    return done.stdout


def git(*args: str) -> str:
    return run("git", *args).strip()


def api(path: str) -> dict | list:
    return json.loads(run("gh", "api", path))


def when(text: str) -> dt.datetime:
    moment = dt.datetime.fromisoformat(text.replace("Z", "+00:00"))
    return moment if moment.tzinfo else moment.replace(tzinfo=dt.timezone.utc)


def category(path: str) -> str:
    return next(name for name, pattern in CATEGORIES if pattern.search(path))


def spread(values: list[float], unit: str, scale: float) -> str:
    if not values:
        return "no data"
    ordered = sorted(v / scale for v in values)
    p90 = ordered[min(len(ordered) - 1, int(len(ordered) * 0.9))]
    return f"median {statistics.median(ordered):.1f} {unit}, p90 {p90:.1f} {unit}, n={len(ordered)}"


def table(header: list[str], rows: list[list]) -> None:
    print("\n| " + " | ".join(header) + " |\n|" + " --- |" * len(header))
    for row in rows:
        print("| " + " | ".join(str(cell) for cell in row) + " |")
    print()


def last_head() -> str | None:
    log = SKILL / "log.md"
    heads = re.findall(r"^Head: `?([0-9a-f]{7,40})", log.read_text("utf-8"), re.M) if log.exists() else []
    return heads[0] if heads else None  # newest entry first


def resolve(since: str | None, head: str) -> tuple[str, dt.datetime]:
    """The commit before the range and its time."""
    since = since or last_head() or (dt.date.today() - dt.timedelta(days=7)).isoformat()
    if re.fullmatch(r"\d{4}-\d\d-\d\d.*", since):
        base = git("rev-list", "--first-parent", "-1", f"--before={since}", head)
        if not base:
            base = git("rev-list", "--max-parents=0", head).splitlines()[-1]
        return base, when(since)
    base = git("rev-parse", since + "^{commit}")
    if subprocess.run(["git", "merge-base", "--is-ancestor", base, head]).returncode:
        sys.exit(f"{base} is not an ancestor of {head}")
    return base, when(git("show", "-s", "--format=%cI", base))


def landed(base: str, head: str) -> list[dict]:
    """One entry per first-parent commit: a merged PR, or a direct push."""
    out = []
    for line in git("log", "--first-parent", "--format=%H%x09%P%x09%cI%x09%s", f"{base}..{head}").splitlines():
        sha, parents, date, subject = line.split("\t", 3)
        parents = parents.split()
        commits, syncs = [(date, subject)], 0
        if len(parents) > 1:
            listing = git("log", "--format=%aI%x09%P%x09%s", f"{parents[0]}..{parents[1]}").splitlines()
            rows = [row.split("\t", 2) for row in listing]
            commits = [(r[0], r[2]) for r in rows if len(r[1].split()) == 1] or commits
            syncs = sum(len(r[1].split()) > 1 for r in rows)
        files = {}
        fields = iter(git("diff", "--numstat", "-z", "-M", parents[0], sha).split("\0"))
        for row in filter(None, fields):
            added, deleted, path = row.split("\t", 2)
            if not path:  # a rename: the old and new paths follow
                next(fields)
                path = next(fields)
            files[path] = (int(added) if added.isdigit() else 0, int(deleted) if deleted.isdigit() else 0)
        number = PULL.search(subject)
        out.append({"sha": sha, "date": when(date), "subject": subject, "files": files, "syncs": syncs,
                    "commits": commits, "number": int(number[1]) if number else None})
    return out


def pulls(repo: str, start: dt.datetime) -> dict[int, dict]:
    found, page = {}, 1
    while True:
        batch = api(f"repos/{repo}/pulls?state=all&sort=updated&direction=desc&per_page=100&page={page}")
        found.update((p["number"], p) for p in batch)
        if len(batch) < 100 or when(batch[-1]["updated_at"]) < start:
            return found
        page += 1


def workflow_runs(repo: str, start: dt.datetime) -> tuple[list[dict], list[str]]:
    """Every run created in the window, one query per day: a query returns at most 1,000 runs."""

    def read(day: dt.date) -> tuple[list[dict], bool]:
        found, page = [], 1
        while True:
            batch = api(f"repos/{repo}/actions/runs?created={day}&per_page=100&page={page}")["workflow_runs"]
            found += [r for r in batch if when(r["created_at"]) >= start]
            if len(batch) < 100 or page == 10:
                return found, len(batch) == 100
            page += 1

    today = dt.datetime.now(dt.timezone.utc).date()
    days = [start.date() + dt.timedelta(days=n) for n in range((today - start.date()).days + 1)]
    with ThreadPoolExecutor(8) as pool:
        read_days = list(pool.map(read, days))
    runs = [r for found, _ in read_days for r in found]
    return runs, [str(day) for day, (_, more) in zip(days, read_days) if more]


def report_work(changes: list[dict], prs: dict[int, dict], start: dt.datetime, end: dt.datetime) -> None:
    days = max((end - start).total_seconds() / 86400, 0.1)
    print(f"## Landed\n\n{len(changes)} changes on the main line in {days:.1f} days ({len(changes) / days:.1f} a day).")
    build, wait, after = [], [], []
    for c in changes:
        pr = prs.get(c["number"])
        if not pr or not pr["merged_at"]:
            continue
        opened, merged = when(pr["created_at"]), when(pr["merged_at"])
        first = min(when(d) for d, _ in c["commits"])
        c["after"] = sum(when(d) > opened for d, _ in c["commits"])
        build.append(max((opened - first).total_seconds(), 0))
        wait.append((merged - opened).total_seconds())
        after.append(c["after"])
        c["open_h"] = wait[-1] / 3600
    print(f"\n- First commit to PR opened: {spread(build, 'h', 3600)}")
    print(f"- PR opened to merged: {spread(wait, 'h', 3600)}")
    print(f"- Commits pushed after the PR opened: {spread(after, 'commits', 1)}")
    print(f"- Merges of another branch into the PR branch: {sum(c['syncs'] for c in changes)} in total")
    repair = [c for c in changes if REPAIR.search(c["subject"])]
    commits = [s for c in changes for _, s in c["commits"]]
    print(f"- Commit subjects that read as repair (fix, revert, flake, follow-up): "
          f"{sum(bool(REPAIR.search(s)) for s in commits)} of {len(commits)}; "
          f"main-line subjects: {len(repair)} of {len(changes)}")

    slow = sorted((c for c in changes if "open_h" in c), key=lambda c: -c["open_h"])[:8]
    print("\nLongest from opened to merged:")
    table(["PR", "Hours open", "Commits after opening", "Branch merges", "Title"],
          [[f"#{c['number']}", f"{c['open_h']:.1f}", c["after"], c["syncs"],
            prs[c["number"]]["title"].replace("|", "/")[:80]] for c in slow])

    totals: dict[str, Counter] = defaultdict(Counter)
    touched: Counter = Counter()
    docs_only = []
    for c in changes:
        kinds = set()
        for path, (added, deleted) in c["files"].items():
            kind = category(path)
            kinds.add(kind)
            totals[kind].update(added=added, deleted=deleted, files=1)
            touched[path] += 1
        c["docs_only"] = bool(c["files"]) and all(p.endswith(".md") for p in c["files"])
        if c["docs_only"]:
            docs_only.append(c)
    print("Where the lines went:")
    table(["Kind", "Added", "Deleted", "File changes"],
          [[k, t["added"], t["deleted"], t["files"]] for k, t in sorted(totals.items(), key=lambda kv: -kv[1]["added"])])
    print(f"Docs-only changes: {len(docs_only)}.")
    hot = [(p, n) for p, n in touched.most_common(12) if n >= 3]
    if hot:
        print("\nFiles touched by three or more changes:")
        table(["File", "Changes"], [[f"`{p}`", n] for p, n in hot])


def report_open(prs: dict[int, dict], now: dt.datetime) -> None:
    rows = [p for p in prs.values() if p["state"] == "open"]
    print(f"## Open\n\n{len(rows)} open PRs among those updated in the window.")
    if rows:
        table(["PR", "Age (days)", "Draft", "Title"],
              [[f"#{p['number']}", f"{(now - when(p['created_at'])).total_seconds() / 86400:.1f}",
                "yes" if p["draft"] else "", p["title"].replace("|", "/")[:80]]
               for p in sorted(rows, key=lambda p: p["created_at"])])


def report_ci(repo: str, start: dt.datetime, sample: int, changes: list[dict], prs: dict[int, dict]) -> None:
    runs, truncated = workflow_runs(repo, start)
    print(f"## CI\n\n{len(runs)} runs created in the window.")
    if truncated:
        print(f"\nOnly the first 1,000 runs were read on: {', '.join(truncated)}.")
    if not runs:
        return
    print("\n- By event: " + ", ".join(f"{k} {n}" for k, n in Counter(r["event"] for r in runs).most_common()))
    print("- By result: " + ", ".join(f"{k} {n}" for k, n in Counter(r["conclusion"] or r["status"] for r in runs).most_common()))
    print(f"- Rerun at least once: {sum(r['run_attempt'] > 1 for r in runs)}")
    refs = {prs[c["number"]]["head"]["ref"] for c in changes if c.get("docs_only") and c["number"] in prs}
    print(f"- Runs on branches of docs-only PRs: {sum(r['head_branch'] in refs for r in runs)}")

    finished = [r for r in runs if r["status"] == "completed" and r["conclusion"] != "skipped"]
    if not sample or not finished:
        return
    step = max(len(finished) / sample, 1)
    chosen = [finished[int(i * step)] for i in range(min(sample, len(finished)))]
    queue: dict[str, list[float]] = defaultdict(list)
    work: dict[str, list[float]] = defaultdict(list)
    failed: Counter = Counter()
    by_day: dict[str, list[float]] = defaultdict(list)
    cancelled = 0.0
    with ThreadPoolExecutor(8) as pool:
        listings = list(pool.map(lambda r: api(f"repos/{repo}/actions/runs/{r['id']}/jobs?per_page=100"), chosen))
    for listing in listings:
        for job in listing["jobs"]:
            if job["conclusion"] in (None, "skipped") or not job["started_at"] or not job["completed_at"]:
                continue
            name = re.sub(r"\s*\(.*\)$", "", job["name"])
            waited = max((when(job["started_at"]) - when(job["created_at"])).total_seconds(), 0)
            ran = max((when(job["completed_at"]) - when(job["started_at"])).total_seconds(), 0)
            if job["conclusion"] == "cancelled":
                cancelled += ran
            failed[name] += job["conclusion"] == "failure"
            queue[name].append(waited)
            work[name].append(ran)
            by_day[job["created_at"][:10]].append(waited)
    waits = [v for vs in queue.values() for v in vs]
    runtime = sum(v for vs in work.values() for v in vs)
    print(f"\nJob sample: {len(chosen)} of {len(finished)} finished runs, {len(waits)} jobs.\n")
    print(f"- Queue per job: {spread(waits, 'min', 60)}")
    print(f"- Total queued {sum(waits) / 3600:.1f} h, total running {runtime / 3600:.1f} h, "
          f"running and then cancelled {cancelled / 3600:.1f} h")
    print("\nQueue by day:")
    table(["Day", "Queue per job"], [[d, spread(v, "min", 60)] for d, v in sorted(by_day.items())])
    print("Most runner time:")
    table(["Job", "Runs", "Failed", "Hours", "Median run (min)", "Median queue (min)"],
          [[n.replace("|", "/"), len(v), failed[n], f"{sum(v) / 3600:.1f}", f"{statistics.median(v) / 60:.1f}",
            f"{statistics.median(queue[n]) / 60:.1f}"]
           for n, v in sorted(work.items(), key=lambda kv: -sum(kv[1]))[:15]])


def report_harness(base: str, head: str) -> None:
    def sizes(rev: str) -> dict[str, int]:
        out = {}
        for line in git("ls-tree", "-r", "-l", rev, SKILLS).splitlines():
            meta, path = line.split("\t", 1)
            if path.endswith(".md"):
                out[path] = int(meta.split()[3])
        return out

    before, after = sizes(base), sizes(head)
    print(f"## Harness\n\nSkill text: {sum(before.values())} bytes before, {sum(after.values())} now.")
    rows = [[f"`{p.removeprefix(SKILLS + '/')}`", before.get(p, 0), after.get(p, 0)]
            for p in sorted(before.keys() | after.keys()) if before.get(p) != after.get(p)]
    if rows:
        table(["Changed file", "Before", "Now"], rows)
    today = dt.date.today().isoformat()
    quarantine = [line.split("\t") for line in git("show", f"{head}:.github/quarantine.tsv").splitlines()
                  if line and not line.startswith(("#", "id\t"))]
    print(f"Quarantined checks: {len(quarantine)}, expired: {sum(r[4] < today for r in quarantine)}.")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--since", help="commit or ISO date; default: the last review's head, or 7 days")
    parser.add_argument("--head", default="origin/main")
    parser.add_argument("--ci-sample", type=int, default=120, help="finished runs to read job times from")
    args = parser.parse_args()
    sys.stdout.reconfigure(encoding="utf-8")

    head = git("rev-parse", args.head + "^{commit}")
    base, start = resolve(args.since, head)
    now = dt.datetime.now(dt.timezone.utc)
    repo = api("repos/{owner}/{repo}")["full_name"]
    print(f"# Work since the last speed review\n\nRange: `{base}..{head}`\n")
    changes = landed(base, head)
    prs = pulls(repo, start)
    report_work(changes, prs, start, now)
    report_open(prs, now)
    report_ci(repo, start, args.ci_sample, changes, prs)
    report_harness(base, head)


if __name__ == "__main__":
    main()
