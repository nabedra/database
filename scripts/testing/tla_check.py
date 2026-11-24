#!/usr/bin/env python3
"""Run a design spec's kind T check with the pinned TLC.

  tla_check.py <spec dir> <Name> [--tier T0|T1] [--module M] [--budget-s S] [--workers N] [--out DIR]
  tla_check.py <spec dir> <Name> --tier T2 [--simulate --seed N --num N --depth N] [...]

T0 and T1 run, in order:
  1. every baseline: `<Name>.cfg` and any other `<Name>.<variant>.cfg` except `<Name>.large.cfg`,
     exhaustively with -coverage. A violation (invariant, action property, deadlock, liveness), a TLC
     error, and a stop at the time budget or a state cap are reported separately;
  2. coverage: every action of Next has a nonzero count in at least one completed baseline;
  3. mutant rule: every INVARIANT and PROPERTY of the baselines is named by some mutant's
     `\\* expect:` line, or exempted by a `\\* no-mutant: <Property> -- <reason>` line;
  4. every `mutants/*.cfg`, which must fail on exactly the property its first line
     `\\* expect: <Property>` names (or `Deadlock`). The counterexample length is recorded.
T2 runs `<Name>.large.cfg`, exhaustively or, with --simulate, as a seeded simulation labelled as such.

The model module is MC<Name>.tla if it exists, else <Name>.tla. Each run gets the per-run budget:
--budget-s, else the module's level in docs/testing/modules.toml (L1 and L2 900 s, L3 3600 s). The
module is --module, else the spec folder's name. Logs, counterexamples (-dumpTrace json) and
summary.json go to --out, default target/tla/<spec dir>/<Name>/<tier>.

The last line is `nabedra-tla-summary: {json}`, which run_check.py's `tlc` parser reads. Exit status:
0 pass, 1 fail, 2 not-run, 3 error.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import tla_common as tc  # noqa: E402

EXIT = {"pass": 0, "fail": 1, "not-run": 2, "error": 3}


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _rel(path: Path) -> str:
    try:
        return path.resolve().relative_to(tc.REPO_ROOT).as_posix()
    except ValueError:
        return path.as_posix()


class Report:
    """Collects assertions, not-run reasons and the facts for the record's `tla` object."""

    def __init__(self) -> None:
        self.executed = 0
        self.failed = 0
        self.not_run: list[dict] = []
        self.errors: list[str] = []
        self.incomplete = False

    def check(self, ok: bool, label: str, detail: str = "") -> None:
        self.executed += 1
        if not ok:
            self.failed += 1
        print(f"[tla_check] {'ok  ' if ok else 'FAIL'} {label}" + (f": {detail}" if detail else ""), flush=True)

    def error(self, label: str, detail: str) -> None:
        self.errors.append(f"{label}: {detail}"[:300])
        print(f"[tla_check] ERROR {label}: {detail}", flush=True)

    def skip(self, ident: str, reason: str) -> None:
        self.not_run.append({"id": ident[:200], "reason": reason[:300]})
        print(f"[tla_check] not-run {ident}: {reason}", flush=True)

    def verdict(self) -> str:
        if self.errors:
            return "error"
        if self.failed:
            return "fail"
        if self.incomplete or self.executed == 0:
            return "not-run"
        return "pass"


def describe(res: tc.TlcResult) -> str:
    v = res.violation()
    if v is None:
        return "no violation"
    kind, prop = v
    names = {"invariant": "invariant", "action-property": "action property", "temporal": "liveness property",
             "deadlock": "deadlock", "postcondition": "postcondition", "assumption": "assumption"}
    what = names.get(kind, kind) + (f" {prop}" if prop else "")
    return f"{what} violated, counterexample of {res.cex_states} states" + (" (lasso)" if res.lasso else "")


def run_baseline(rep: Report, spec_dir: Path, model: str, cfg: Path, out: Path, budget: int,
                 workers: str, facts: list[dict], cex_dump: bool = True,
                 extra: list[str] | None = None, label: str | None = None) -> tc.TlcResult:
    stem = cfg.name[: -len(".cfg")]
    label = label or f"baseline {cfg.name}"
    args = ["-workers", workers, "-coverage", "1", *(extra or [])]
    if cex_dump:
        args += ["-dumpTrace", "json", f"/out/{stem}.cex.json"]
    args += ["-config", cfg.relative_to(spec_dir).as_posix(), model]
    res, elapsed = tc.run_tlc(spec_dir, args, out_dir=out, log_path=out / f"{stem}.log", budget_s=budget)
    fact = {"cfg": cfg.name, "distinct_states": res.distinct, "depth": res.depth, "states_left": res.left,
            "duration_s": round(elapsed, 1)}
    if res.errors:
        fact["verdict"] = "error"
        rep.error(label, "; ".join(res.errors)[:300])
    elif res.violations:
        fact["verdict"] = res.violations[0][0]
        rep.check(False, label, describe(res) + f" (log {stem}.log)")
        for line in tc.counterexample_text((out / f"{stem}.log").read_text(encoding="utf-8", errors="replace")):
            print(f"    {line}")
    elif res.complete or res.simulation_done:
        fact["verdict"] = "pass"
        rep.check(True, label, f"{res.distinct if res.distinct is not None else res.generated} "
                               f"{'distinct' if res.distinct is not None else 'generated'} states, depth {res.depth}")
    else:
        fact["verdict"] = "incomplete"
        rep.incomplete = True
        rep.skip(label, tc.incomplete_reason(res, budget))
    fact["detail"] = describe(res) if res.violations else ""
    facts.append(fact)
    return res


def run_mutant(rep: Report, spec_dir: Path, model: str, cfg: Path, out: Path, budget: int,
               workers: str) -> dict:
    parsed = tc.parse_cfg(cfg)
    name = cfg.name[: -len(".cfg")]
    fact = {"name": name, "expect": parsed.expect or "", "observed": "", "ok": False, "cex_length": None,
            "detail": ""}
    label = f"mutant {name}"
    if parsed.expect is None:
        fact["detail"] = "first line is not `\\* expect: <Property>`"
        rep.check(False, label, fact["detail"])
        return fact
    known = set(parsed.invariants) | set(parsed.properties) | {"Deadlock"}
    if parsed.expect not in known:
        fact["detail"] = f"expects {parsed.expect}, which its cfg does not check"
        rep.check(False, label, fact["detail"])
        return fact
    stem = f"mutant-{name}"
    args = ["-workers", workers, "-dumpTrace", "json", f"/out/{stem}.cex.json",
            "-config", cfg.relative_to(spec_dir).as_posix(), model]
    res, _ = tc.run_tlc(spec_dir, args, out_dir=out, log_path=out / f"{stem}.log", budget_s=budget)
    v = res.violation()
    if res.errors:
        fact["observed"] = "error"
        fact["detail"] = ("TLC error: " + "; ".join(res.errors))[:300]
    elif v is None:
        fact["observed"] = "none"
        fact["detail"] = ("no violation within the budget: " + tc.incomplete_reason(res, budget)
                          if not res.complete else "no violation: the mutant is not detected")[:300]
    else:
        kind, prop = v
        fact["observed"] = "Deadlock" if kind == "deadlock" else (prop or kind)
        fact["cex_length"] = res.cex_states
        fact["ok"] = fact["observed"] == parsed.expect
        fact["detail"] = describe(res)
        if not fact["ok"]:
            fact["detail"] = f"expected {parsed.expect}, got: {describe(res)}"[:300]
    rep.check(fact["ok"], label, fact["detail"])
    return fact


def design_check(rep: Report, spec_dir: Path, name: str, model: str, out: Path, budget: int,
                 workers: str) -> dict:
    main_cfg = spec_dir / f"{name}.cfg"
    if not main_cfg.exists():
        raise tc.TlaError(f"{_rel(main_cfg)} does not exist")
    baselines = [main_cfg] + sorted(
        p for p in spec_dir.glob(f"{name}.*.cfg") if p.name != f"{name}.large.cfg"
    )
    runs: list[dict] = []
    coverage: dict[str, int] = {}
    distinct = depth = None
    for cfg in baselines:
        res = run_baseline(rep, spec_dir, model, cfg, out, budget, workers, runs)
        if cfg == main_cfg:
            distinct, depth = res.distinct, res.depth
        if res.complete and not res.violations and not res.errors:
            for action, (_, generated) in res.coverage.items():
                coverage[action] = max(coverage.get(action, 0), generated)

    uncovered = sorted(a for a, n in coverage.items() if n == 0)
    if any(r["verdict"] == "pass" for r in runs):
        if not coverage:
            rep.check(False, "coverage", "no coverage report in any completed baseline")
        else:
            rep.check(not uncovered, "coverage",
                      f"zero coverage: {', '.join(uncovered)}" if uncovered
                      else f"{len(coverage)} actions, minimum {min(coverage.values())}")
    else:
        rep.skip("coverage", "no baseline completed, so coverage was not evaluated")

    cfgs = [tc.parse_cfg(c) for c in baselines]
    props = list(dict.fromkeys(p for c in cfgs for p in (*c.invariants, *c.properties)))
    exempt: dict[str, str] = {}
    for c in cfgs:
        exempt.update(c.no_mutant)
    mutant_cfgs = sorted((spec_dir / "mutants").glob("*.cfg"))
    expects = {tc.parse_cfg(m).expect for m in mutant_cfgs}
    for prop in props:
        if prop in exempt:
            print(f"[tla_check] note: {prop} has no mutant by design: {exempt[prop] or 'no reason given'}")
            continue
        rep.check(prop in expects, f"mutant rule {prop}",
                  "named by a mutant" if prop in expects else "no mutant expects it, and no `no-mutant` line exempts it")

    mutants = [run_mutant(rep, spec_dir, model, m, out, budget, workers) for m in mutant_cfgs]
    covered = [n for n in coverage.values()]
    return {
        "mode": "exhaustive", "distinct_states": distinct, "depth": depth,
        "actions": len(coverage), "min_action_coverage": min(covered) if covered else None,
        "uncovered": uncovered[:50], "unmutated": sorted(exempt)[:50], "runs": runs[:20], "mutants": mutants[:50],
    }


def large_check(rep: Report, spec_dir: Path, name: str, model: str, out: Path, budget: int, workers: str,
                args: argparse.Namespace) -> dict:
    cfg = spec_dir / f"{name}.large.cfg"
    if not cfg.exists():
        raise tc.TlaError(f"{_rel(cfg)} does not exist")
    runs: list[dict] = []
    extra: list[str] = []
    if args.simulate:
        extra = ["-simulate", f"num={args.num}", "-seed", str(args.seed), "-depth", str(args.depth)]
    label = f"{'simulation' if args.simulate else 'exhaustive'} {cfg.name}"
    res = run_baseline(rep, spec_dir, model, cfg, out, budget, workers, runs, extra=extra, label=label)
    facts = {"mode": "simulation" if args.simulate else "exhaustive", "distinct_states": res.distinct,
             "depth": res.depth, "runs": runs}
    if args.simulate:
        facts.update({"seed": args.seed, "behaviors": args.num, "states_generated": res.generated})
    return facts


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("spec_dir", type=Path)
    p.add_argument("name")
    p.add_argument("--tier", choices=("T0", "T1", "T2"), default="T1")
    p.add_argument("--module")
    p.add_argument("--budget-s", type=int)
    p.add_argument("--workers")
    p.add_argument("--out", type=Path)
    p.add_argument("--simulate", action="store_true", help="T2 only: TLC simulation mode")
    p.add_argument("--seed", type=int, default=1)
    p.add_argument("--num", type=int, default=100000, help="simulation: number of behaviors")
    p.add_argument("--depth", type=int, default=100, help="simulation: maximum behavior length")
    args = p.parse_args(argv)
    if args.simulate and args.tier != "T2":
        p.error("--simulate applies only to --tier T2")

    rep = Report()
    spec_dir = args.spec_dir.resolve()
    tla: dict = {"spec": f"{_rel(spec_dir)}/{args.name}", "tier": args.tier, "tools": tc.tool_versions()}
    out = args.out
    try:
        if not spec_dir.is_dir():
            raise tc.TlaError(f"{args.spec_dir} is not a directory")
        module = args.module or spec_dir.name
        budget = tc.budget_for(module, args.budget_s)
        tla["budget_s"] = budget
        model = f"MC{args.name}.tla" if (spec_dir / f"MC{args.name}.tla").exists() else f"{args.name}.tla"
        if not (spec_dir / model).exists():
            raise tc.TlaError(f"neither MC{args.name}.tla nor {args.name}.tla exists in {_rel(spec_dir)}")
        if out is None:
            out = tc.REPO_ROOT / "target" / "tla" / _rel(spec_dir) / args.name / args.tier
            shutil.rmtree(out, ignore_errors=True)
        elif out.exists() and any(out.iterdir()):
            raise tc.TlaError(f"--out {out} is not empty")
        out.mkdir(parents=True, exist_ok=True)
        out = out.resolve()
        workers = args.workers or tc.default_workers()
        print(f"[tla_check] {tla['spec']} tier {args.tier}, module {module}, budget {budget} s per run, "
              f"workers {workers}, {tla['tools']}, output {_rel(out)}", flush=True)
        if args.tier == "T2":
            tla.update(large_check(rep, spec_dir, args.name, model, out, budget, workers, args))
        else:
            tla.update(design_check(rep, spec_dir, args.name, model, out, budget, workers))
    except (tc.TlaError, OSError) as exc:
        rep.error("setup", str(exc))
        tla.setdefault("mode", "simulation" if args.simulate else "exhaustive")

    verdict = rep.verdict()
    artifacts = []
    if out is not None and out.exists():
        (out / "summary.json").write_text(json.dumps({"verdict": verdict, "tla": tla}, indent=2) + "\n",
                                          encoding="utf-8")
        for f in sorted(out.glob("*")):
            if f.is_file() and len(artifacts) < 40:
                artifacts.append({"name": f"tla/{f.name}"[:200], "sha256": _sha256(f)})
    not_run = list(rep.not_run) + [{"id": "tlc-error", "reason": e} for e in rep.errors]
    tc.print_summary({"verdict": verdict, "executed": rep.executed, "failed": rep.failed,
                      "not_run": not_run[:200], "tla": tla, "artifacts": artifacts})
    return EXIT[verdict]


if __name__ == "__main__":
    sys.exit(main())
