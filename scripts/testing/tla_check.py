#!/usr/bin/env python3
"""Run a design spec's kind T check with the pinned TLC.

  tla_check.py <spec dir> <Name> [--tier T0|T1] [--module M] [--budget-s S] [--workers N] [--out DIR]
  tla_check.py <spec dir> <Name> --tier T2 [--simulate --seed N --num N --depth N] [...]

T0 and T1 check, in order:
  1. every baseline: `<Name>.cfg` and any other `<Name>.<variant>.cfg` except `<Name>.large.cfg`,
     exhaustively with -coverage. A violation (invariant, action property, deadlock, liveness), a TLC
     error, and a stop at the time budget or a state cap are reported separately;
  2. coverage: every action of Next has a nonzero count in at least one completed baseline;
  3. mutant rule: the baselines check at least one property, and every INVARIANT and PROPERTY is named by
     some mutant's `\\* expect:` line. Only a type invariant may be exempted, with a
     `\\* no-mutant: <Property> -- <reason>` line;
  4. search rule: nothing limits the search except what the cfg declares (search_problems);
  5. every `mutants/*.cfg`, which must fail on exactly the property its first line
     `\\* expect: <Property>` names (or `Deadlock`). The counterexample length is recorded.
T2 applies the search rule and runs `<Name>.large.cfg`, exhaustively or, with --simulate, as a seeded
simulation labelled as such.

The search rule. TLC reports an exhaustive search whatever a cfg cuts away, so:
  - a module of the model may not read or set TLC's search state, the trace, the clock, the environment or
    random values (TLCGet, TLCSet, TLCExt, RandomElement, ...), so no constraint can be a depth limit or a
    state cap in those terms;
  - a CONSTRAINT or ACTION_CONSTRAINT must be declared in its cfg by `\\* limit: <Name> -- <reason>` and must
    only bound counters: a conjunction, possibly under \\A, of order comparisons (<, <=, >, >=). Equality,
    negation, implication, disjunction and string values select protocol states, which is not a bound;
  - a constraint or a symmetry set may not be active in a run that checks liveness (TLC says which runs do);
  - VIEW is refused, because it merges states that the properties may tell apart;
  - a cfg may not override a name it checks, its constraints or its INIT, NEXT or SPECIFICATION. Giving a
    declared CONSTANT its value is free. Overriding any other definition, such as Nat, needs a `limit` line.
The record lists the declared limits and how many states each constraint kept. What the rule cannot see: a
counter that every action increments, so that its bound is a depth limit; a constraint over an operator that
encodes protocol state as a number; a CONSTANT value, INIT or NEXT that leaves behavior out.

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
import re
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
        self.errors.append(f"{label}: {detail}")
        print(f"[tla_check] ERROR {label}: {detail}", flush=True)

    def skip(self, ident: str, reason: str) -> None:
        self.not_run.append({"id": ident, "reason": reason})
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


# --------------------------------------------------------------------------------------------
# The search rule: what may limit the search.

_LIMIT = re.compile(r"^[ \t]*\\\*[ \t]*limit:[ \t]*(\w+)[ \t]*--[ \t]*(\S.*?)[ \t]*$", re.M)
_SEARCH_STATE = re.compile(r"\b(?:TLC(?:Get|Set)\w*|RandomElement|RandomSubset\w*|RandomSetOfSubsets|JavaTime)\b")
# TLCExt has the trace so far, and Randomization and IOUtils make a run depend on chance or the environment.
_SEARCH_MODULES = {"TLCExt", "Randomization", "IOUtils", "IOUtilsExt"}
_IMPORT = re.compile(r"\bEXTENDS\s+(\w+(?:\s*,\s*\w+)*)|\bINSTANCE\s+(\w+)")
_CONSTANT_DECL = re.compile(r"\bCONSTANTS?\s+(\w+(?:\([\s,_]*\))?(?:\s*,\s*\w+(?:\([\s,_]*\))?)*)")
# A definition starts in column 1. Its body ends where the next unit starts in column 1.
_DEFINITION = re.compile(r"^(?:LOCAL[ \t]+)?(\w+)[ \t]*(?:\([^=]*?\)|\[[^=]*?\])?[ \t]*==(?!=)(.*?)(?=^\w|^-{4}|^={4}|\Z)",
                         re.M | re.S)
_TOKEN = re.compile(r"\\\w+|\w+|<<|>>|<=>|<=|>=|=<|=>|/=|\\/|\|->|->|<-|==|[^\s\w]")
_ORDER = {"<", "<=", "=<", "\\leq", ">", ">=", "\\geq"}
_SELECTS = {"=", "#", "/=", "\\neq", "\\notin", "~", "\\lnot", "\\neg", "=>", "<=>", "\\equiv", "\\/", "\\lor", "\\E", "\\A",
            "\\subseteq", "==", '"', "IF", "CASE", "LET", "TRUE", "FALSE", "ENABLED"}
_QUANTIFIER = re.compile(r"^\\A\s[^:{]*:")


class Model:
    """The modules of a model: <Name> or MC<Name> and what it extends or instantiates under specs/.
    Standard and community modules are not read."""

    def __init__(self, spec_dir: Path, model: str) -> None:
        self.texts: dict[str, str] = {}
        self.definitions: dict[str, list[str]] = {}
        self.constants: set[str] = set()
        self.problems: list[str] = []
        todo = [model.removesuffix(".tla")]
        while todo:
            name = todo.pop()
            path = next((d / f"{name}.tla" for d in (spec_dir, tc.SPECS_DIR / "lib") if (d / f"{name}.tla").exists()),
                        None)
            if name in self.texts or path is None:
                continue
            text = tc.strip_tla_comments(path.read_text(encoding="utf-8"), keep_strings=False)
            self.texts[name] = text
            for m in _IMPORT.finditer(text):
                imported = [i.strip() for i in (m.group(1) or m.group(2)).split(",")]
                todo.extend(imported)
                self.problems += [f"{name}.tla uses the module {i}" for i in imported if i in _SEARCH_MODULES]
            self.problems += [f"{name}.tla uses {u}" for u in sorted(set(_SEARCH_STATE.findall(text)))]
            for m in _DEFINITION.finditer(text):
                self.definitions.setdefault(m.group(1), []).append(m.group(2))
            for m in _CONSTANT_DECL.finditer(text):
                self.constants.update(re.findall(r"\w+", re.sub(r"\([^)]*\)", "", m.group(1))))

    def bound_problem(self, name: str, seen: frozenset = frozenset()) -> str | None:
        """Why the operator is not just a bound on counters, or None."""
        bodies = self.definitions.get(name)
        if not bodies or name in seen:
            return f"{name} has no definition that starts a line in the model's modules"
        for body in bodies:
            for conjunct in re.split(r"/\\|\\land\b", body):
                problem = self._conjunct_problem(name, conjunct, seen | {name})
                if problem:
                    return problem
        return None

    def _conjunct_problem(self, name: str, text: str, seen: frozenset) -> str | None:
        text = text.strip()
        while True:
            inner = _QUANTIFIER.sub("", text.lstrip("( \n\t")).strip()
            if inner == text:
                break
            text = inner
        if not text:
            return None  # the text before a leading /\
        call = re.fullmatch(r"(\w+)(?:\([^<>]*\))?\)*", text)
        if call and call.group(1) in self.definitions:
            return self.bound_problem(call.group(1), seen)
        tokens = _TOKEN.findall(text)
        selects = sorted(_SELECTS.intersection(tokens))
        if selects:
            used = " ".join("a string" if s == '"' else s for s in selects)
            return f"{name} uses {used}, so it selects states instead of bounding a counter"
        if sum(t in _ORDER for t in tokens) != 1:
            return f"{name} has a conjunct that is not one order comparison: {' '.join(text.split())[:80]}"
        return None


def limits_of(cfg: Path) -> dict[str, str]:
    """The names a cfg declares with `\\* limit: <Name> -- <reason>` lines."""
    return dict(_LIMIT.findall(cfg.read_text(encoding="utf-8")))


def checked_names(parsed: tc.Cfg) -> set[str]:
    """The names whose override would change what a run checks or where it searches."""
    return {n for key in ("INVARIANT", "PROPERTY", "CONSTRAINT", "ACTION_CONSTRAINT", "INIT", "NEXT",
                          "SPECIFICATION", "POSTCONDITION") for n in parsed.sections.get(key, [])}


def search_problems(model: Model, cfg: Path) -> tuple[list[str], list[tuple[str, str, str]]]:
    """(problems, declared limits as (kind, name, reason)) of one cfg. See the module docstring."""
    parsed = tc.parse_cfg(cfg)
    declared = limits_of(cfg)
    problems: list[str] = []
    limits: list[tuple[str, str, str]] = []

    def declare(kind: str, name: str) -> None:
        if name in declared:
            limits.append((kind, name, declared[name]))
        else:
            problems.append(f"{kind} {name} has no `\\* limit: {name} -- <reason>` line")

    for kind in ("CONSTRAINT", "ACTION_CONSTRAINT"):
        for name in parsed.sections.get(kind, []):
            declare(kind, name)
            problem = model.bound_problem(name)
            if problem:
                problems.append(f"{kind} {name}: {problem}")
    problems += [f"VIEW {name} merges states" for name in parsed.sections.get("VIEW", [])]
    checked = checked_names(parsed)
    for name in dict.fromkeys(parsed.overrides):
        if name in checked:
            problems.append(f"the cfg overrides {name}, which it checks or runs")
        elif name not in model.constants:
            declare("override", name)
    return problems, limits


def search_check(rep: Report, model: Model, cfgs: list[Path]) -> list[tuple[str, str, str, str]]:
    """One assertion for the model and each cfg. Returns the declared limits with their cfg name."""
    rep.check(not model.problems, "search of the model", "; ".join(model.problems) or "reads no search state")
    out = []
    for cfg in cfgs:
        problems, limits = search_problems(model, cfg)
        rep.check(not problems, f"search of {cfg.name}", "; ".join(problems) or (
            "declared limits: " + ", ".join(n for _, n, _ in limits) if limits else "no limits"))
        out += [(cfg.name, *limit) for limit in limits]
    return out


_CONSTRAINT_COVERAGE = re.compile(r"@!@!@STARTMSG 2778:0 @!@!@\n<(\w+) line [^>]*>: (\d+):(\d+)")


def liveness_problem(log: str, parsed: tc.Cfg) -> str | None:
    """Why this run's liveness result does not count, from TLC's own messages, or None."""
    if "@!@!@STARTMSG 2279:" in log:  # "Declaring symmetry during liveness checking is dangerous"
        return "liveness under SYMMETRY, which can miss violations: use a cfg without it"
    if "@!@!@STARTMSG 2192:" in log:  # "Checking temporal properties"
        names = parsed.sections.get("CONSTRAINT", []) + parsed.sections.get("ACTION_CONSTRAINT", [])
        if names:
            return f"liveness under the constraint {', '.join(names)}, which cuts behaviors short: use a cfg without it"
    return None


def describe_limits(limits: list[tuple[str, str, str, str]], out: Path) -> list[str]:
    """The declared limits for the record, with how many states TLC kept for each constraint."""
    lines = []
    for cfg, kind, name, reason in limits:
        log = out / f"{cfg[: -len('.cfg')]}.log"
        kept = [(a, b) for n, a, b in _CONSTRAINT_COVERAGE.findall(
            log.read_text(encoding="utf-8", errors="replace") if log.exists() else "") if n == name]
        lines.append(f"{cfg}: {kind} {name}" + (f" kept {kept[-1][0]} of {kept[-1][1]} states" if kept else "")
                     + f": {reason}")
    return lines


# --------------------------------------------------------------------------------------------
# Runs.

def run_baseline(rep: Report, spec_dir: Path, model: str, cfg: Path, out: Path, budget: int,
                 workers: str, facts: list[dict], cex_dump: bool = True,
                 extra: list[str] | None = None, label: str | None = None, simulate: bool = False) -> tc.TlcResult:
    stem = cfg.name[: -len(".cfg")]
    label = label or f"baseline {cfg.name}"
    args = ["-workers", workers, "-coverage", "1", *(extra or [])]
    if cex_dump:
        args += ["-dumpTrace", "json", f"/out/{stem}.cex.json"]
    args += ["-config", cfg.relative_to(spec_dir).as_posix(), model]
    res, elapsed = tc.run_tlc(spec_dir, args, out_dir=out, log_path=out / f"{stem}.log", budget_s=budget)
    fact = {"cfg": cfg.name, "distinct_states": res.distinct, "depth": res.depth, "states_left": res.left,
            "duration_s": round(elapsed, 1)}
    log = (out / f"{stem}.log").read_text(encoding="utf-8", errors="replace")
    # A simulation's success is not an exhaustive one, and the other way round.
    done = (res.simulation_done and not res.complete and not res.timed_out) if simulate \
        else (res.complete and not res.simulation_done)
    liveness = liveness_problem(log, tc.parse_cfg(cfg))
    if res.errors:
        fact["verdict"] = "error"
        rep.error(label, "; ".join(res.errors))
    elif res.violations:
        fact["verdict"] = res.violations[0][0]
        rep.check(False, label, describe(res) + f" (log {stem}.log)")
        for line in tc.counterexample_text(log):
            print(f"    {line}")
    elif liveness:
        fact["verdict"] = "liveness-limited"
        rep.check(False, label, liveness)
    elif done and res.exit_code == 0:
        fact["verdict"] = "pass"
        rep.check(True, label, f"{res.distinct if res.distinct is not None else res.generated} "
                               f"{'distinct' if res.distinct is not None else 'generated'} states, depth {res.depth}")
    else:
        fact["verdict"] = "incomplete"
        rep.incomplete = True
        rep.skip(label, tc.incomplete_reason(res, budget))
    fact["detail"] = describe(res) if res.violations else liveness or ""
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
    overridden = sorted(checked_names(parsed).intersection(parsed.overrides))
    if overridden:
        fact["detail"] = f"its cfg overrides {', '.join(overridden)}, which it checks or runs"
        rep.check(False, label, fact["detail"])
        return fact
    stem = f"mutant-{name}"
    args = ["-workers", workers, "-dumpTrace", "json", f"/out/{stem}.cex.json",
            "-config", cfg.relative_to(spec_dir).as_posix(), model]
    res, _ = tc.run_tlc(spec_dir, args, out_dir=out, log_path=out / f"{stem}.log", budget_s=budget)
    v = res.violation()
    if res.errors:
        fact["observed"] = "error"
        fact["detail"] = "TLC error: " + "; ".join(res.errors)
    elif v is None:
        fact["observed"] = "none"
        fact["detail"] = ("no violation within the budget: " + tc.incomplete_reason(res, budget)
                          if not res.complete else "no violation: the mutant is not detected")
    else:
        kind, prop = v
        fact["observed"] = "Deadlock" if kind == "deadlock" else (prop or kind)
        fact["cex_length"] = res.cex_states
        fact["detail"] = describe(res)
        if fact["observed"] != parsed.expect:
            fact["detail"] = f"expected {parsed.expect}, got: {describe(res)}"
        elif res.exit_code == 0:
            fact["detail"] += ", but TLC exited with status 0"
        elif kind == "invariant" and res.cex_states < 2:
            fact["detail"] += ": false in an initial state, so no step of the mutant breaks it"
        else:
            fact["ok"] = True
    rep.check(fact["ok"], label, fact["detail"])
    return fact


def design_check(rep: Report, spec_dir: Path, name: str, model: str, out: Path, budget: int,
                 workers: str) -> dict:
    facts: dict = {"mode": "exhaustive"}
    main_cfg = spec_dir / f"{name}.cfg"
    if not main_cfg.exists():
        raise tc.TlaError(f"{_rel(main_cfg)} does not exist")
    baselines = [main_cfg] + sorted(
        p for p in spec_dir.glob(f"{name}.*.cfg") if p.name != f"{name}.large.cfg"
    )
    limits = search_check(rep, Model(spec_dir, model), baselines)
    runs: list[dict] = []
    coverage: dict[str, int] = {}
    unparsed: list[str] = []
    distinct = depth = None
    for cfg in baselines:
        res = run_baseline(rep, spec_dir, model, cfg, out, budget, workers, runs)
        if cfg == main_cfg:
            distinct, depth = res.distinct, res.depth
        if res.complete and not res.violations and not res.errors:
            for action, (_, generated) in res.coverage.items():
                coverage[action] = max(coverage.get(action, 0), generated)
            unparsed.extend(res.coverage_unparsed)

    uncovered = sorted(a for a, n in coverage.items() if n == 0)
    if any(r["verdict"] == "pass" for r in runs):
        if not coverage:
            rep.check(False, "coverage", "no coverage report in any completed baseline")
        elif unparsed:
            rep.check(False, "coverage", f"unreadable action coverage lines: {'; '.join(unparsed[:3])}")
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
    checked = [p for p in props if p not in exempt]
    rep.check(bool(checked), "properties", f"{len(checked)} properties need a mutant" if checked
              else "the baselines check no property besides exempted ones")
    for prop in props:
        if prop in exempt:
            temporal = any(prop in c.properties for c in cfgs)
            if exempt[prop] and "type" in prop.lower() and not temporal:
                print(f"[tla_check] note: {prop} has no mutant by design: {exempt[prop]}")
            else:
                rep.check(False, f"mutant rule {prop}", "its `no-mutant` line gives no reason" if not exempt[prop]
                          else "only a type invariant, an invariant with Type in its name, may be exempted")
            continue
        rep.check(prop in expects, f"mutant rule {prop}",
                  "named by a mutant" if prop in expects else "no mutant expects it, and no `no-mutant` line exempts it")

    mutants = [run_mutant(rep, spec_dir, model, m, out, budget, workers) for m in mutant_cfgs]
    covered = [n for n in coverage.values()]
    facts.update({
        "distinct_states": distinct, "depth": depth,
        "actions": len(coverage), "min_action_coverage": min(covered) if covered else None,
        "uncovered": uncovered, "unmutated": sorted(exempt), "runs": runs, "mutants": mutants,
    })
    if limits:
        facts["limits"] = describe_limits(limits, out)
    return facts


def large_check(rep: Report, spec_dir: Path, name: str, model: str, out: Path, budget: int, workers: str,
                args: argparse.Namespace) -> dict:
    cfg = spec_dir / f"{name}.large.cfg"
    if not cfg.exists():
        raise tc.TlaError(f"{_rel(cfg)} does not exist")
    limits = search_check(rep, Model(spec_dir, model), [cfg])
    runs: list[dict] = []
    extra: list[str] = []
    if args.simulate:
        extra = ["-simulate", f"num={args.num}", "-seed", str(args.seed), "-depth", str(args.depth)]
    label = f"{'simulation' if args.simulate else 'exhaustive'} {cfg.name}"
    res = run_baseline(rep, spec_dir, model, cfg, out, budget, workers, runs, extra=extra, label=label,
                       simulate=args.simulate)
    facts = {"mode": "simulation" if args.simulate else "exhaustive", "distinct_states": res.distinct,
             "depth": res.depth, "runs": runs}
    if limits:
        facts["limits"] = describe_limits(limits, out)
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
            taken, out = out, None  # nothing of this run may land in it
            raise tc.TlaError(f"--out {taken} is not empty")
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
    # Errors come first: run_check.py trims the record's not-run entries from the end.
    not_run = [{"id": "tlc-error", "reason": e} for e in rep.errors] + rep.not_run
    result = {"verdict": verdict, "executed": rep.executed, "failed": rep.failed, "not_run": not_run, "tla": tla}
    artifacts = []
    if out is not None and out.exists():
        # summary.json keeps every fact. run_check.py trims the record, and keeps this artifact.
        files = [f for f in sorted(out.glob("*")) if f.is_file() and f.name != "summary.json"]
        (out / "summary.json").write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
        artifacts = [{"name": f"tla/{f.name}", "sha256": _sha256(f)} for f in (out / "summary.json", *files)]
    tc.print_summary({**result, "artifacts": artifacts})
    return EXIT[verdict]


if __name__ == "__main__":
    sys.exit(main())
