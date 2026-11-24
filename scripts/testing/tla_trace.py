#!/usr/bin/env python3
"""Lint `nabedra.trace/1` traces and check them against a design spec with TLC.

  tla_trace.py lint <file.ndjson>...
  tla_trace.py check <spec dir> <Name> <trace files or dirs>... [--expect-rejected DIR]...
                     [--module M] [--tier T] [--budget-s S] [--out DIR]

`lint` checks the format (wave-p8/design-review.md, "Trace format", and
.agents/skills/design-review/references/trace-validation.md): line 1 is the header
{"trace":"nabedra.trace/1","spec","seed","sha","workload","params"}, and every later line is a step
{"seq","t","node","action","vars"[,"msg"]} with seq counting from 0 and t never decreasing. Values are
JSON objects, arrays, strings, booleans and integers: no floats and no nulls.

`check` lints each trace, then runs `<Name>Trace.tla` with `<Name>Trace.cfg` once per trace, with
TRACE_PATH set, in the pinned image. A trace is accepted when some behavior of the spec consumes every
line (the POSTCONDITION TraceAccepted from specs/lib/NabedraTrace.tla). For a rejected trace it prints
the 0-based index and the content of the first line no behavior explains. A directory argument means
its `*.ndjson` files. Every trace under --expect-rejected must be rejected, and one named
`*.at-<n>.ndjson` must be rejected at index n. One assertion per trace. The last line is
`nabedra-tla-summary: {json}`. Exit status: 0 pass, 1 fail, 2 not-run, 3 error.
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

FORMAT = "nabedra.trace/1"
HEADER_KEYS = {"trace", "spec", "seed", "sha", "workload", "params"}
STEP_REQUIRED = {"seq", "t", "node", "action", "vars"}
STEP_KEYS = STEP_REQUIRED | {"msg"}
IDENT = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
SHA = re.compile(r"^[0-9a-f]{40}$")
AT = re.compile(r"\.at-(\d+)\.ndjson$")
EXIT = {"pass": 0, "fail": 1, "not-run": 2, "error": 3}
U64_MAX = 2**64 - 1


def _is_int(v) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


def _value_errors(value, where: str) -> list[str]:
    """TLA+-representable JSON: objects, arrays, strings, booleans and integers."""
    if isinstance(value, dict):
        out = []
        for k, v in value.items():
            out.extend(_value_errors(v, f"{where}.{k}"))
        return out
    if isinstance(value, list):
        out = []
        for i, v in enumerate(value):
            out.extend(_value_errors(v, f"{where}[{i}]"))
        return out
    if isinstance(value, (str, bool)) or _is_int(value):
        return []
    if value is None:
        return [f"{where}: null has no TLA+ value"]
    return [f"{where}: {type(value).__name__} is not allowed (use integers)"]


def lint_lines(lines: list[str]) -> list[str]:
    """Every format problem, as `line <n>: <problem>` strings (1-based file lines)."""
    errors: list[str] = []
    if not lines:
        return ["line 1: the trace is empty; the header is missing"]
    prev_t = None
    for number, raw in enumerate(lines, start=1):
        where = f"line {number}"
        if not raw.strip():
            errors.append(f"{where}: blank line")
            continue
        try:
            obj = json.loads(raw)
        except json.JSONDecodeError as exc:
            errors.append(f"{where}: not JSON: {exc.msg}")
            continue
        if not isinstance(obj, dict):
            errors.append(f"{where}: not a JSON object")
            continue
        if number == 1:
            missing = HEADER_KEYS - obj.keys()
            extra = obj.keys() - HEADER_KEYS
            if missing:
                errors.append(f"{where}: header misses {sorted(missing)}")
            if extra:
                errors.append(f"{where}: header has unknown keys {sorted(extra)}")
            if obj.get("trace") != FORMAT:
                errors.append(f"{where}: header trace must be {FORMAT!r}")
            if "spec" in obj and not (isinstance(obj["spec"], str) and IDENT.match(obj["spec"])):
                errors.append(f"{where}: header spec must be a TLA+ module name")
            if "seed" in obj and not (_is_int(obj["seed"]) and 0 <= obj["seed"] <= U64_MAX):
                errors.append(f"{where}: header seed must be an integer from 0 to 2^64-1")
            if "sha" in obj and not (isinstance(obj["sha"], str) and SHA.match(obj["sha"])):
                errors.append(f"{where}: header sha must be a 40-character git SHA")
            if "workload" in obj and not (isinstance(obj["workload"], str) and obj["workload"].strip()):
                errors.append(f"{where}: header workload must be a nonblank string")
            if "params" in obj:
                if not isinstance(obj["params"], dict):
                    errors.append(f"{where}: header params must be an object")
                else:
                    errors.extend(_value_errors(obj["params"], f"{where}: params"))
            continue
        missing = STEP_REQUIRED - obj.keys()
        extra = obj.keys() - STEP_KEYS
        if missing:
            errors.append(f"{where}: step misses {sorted(missing)}")
        if extra:
            errors.append(f"{where}: step has unknown keys {sorted(extra)}")
        if "seq" in obj and obj["seq"] != number - 2:
            errors.append(f"{where}: seq is {obj['seq']!r}, expected {number - 2}")
        if "t" in obj:
            if not (_is_int(obj["t"]) and obj["t"] >= 0):
                errors.append(f"{where}: t must be a non-negative integer (virtual ns)")
            else:
                if prev_t is not None and obj["t"] < prev_t:
                    errors.append(f"{where}: t {obj['t']} is before the previous step's {prev_t}")
                prev_t = obj["t"]
        if "node" in obj and not (isinstance(obj["node"], str) and obj["node"].strip()):
            errors.append(f"{where}: node must be a nonblank string")
        if "action" in obj and not (isinstance(obj["action"], str) and IDENT.match(obj["action"])):
            errors.append(f"{where}: action must be a spec action name")
        for key in ("vars", "msg"):
            if key in obj:
                if not isinstance(obj[key], dict):
                    errors.append(f"{where}: {key} must be an object")
                else:
                    errors.extend(_value_errors(obj[key], f"{where}: {key}"))
    return errors


def read_lines(path: Path) -> list[str]:
    text = Path(path).read_text(encoding="utf-8")
    lines = text.split("\n")
    if lines and lines[-1] == "":
        lines.pop()
    return [l[:-1] if l.endswith("\r") else l for l in lines]


def lint_file(path: Path) -> list[str]:
    try:
        return lint_lines(read_lines(path))
    except UnicodeDecodeError:
        return ["the file is not UTF-8"]
    except OSError as exc:
        return [f"cannot read: {exc}"]


def collect(paths: list[Path]) -> list[Path]:
    out: list[Path] = []
    for p in paths:
        if p.is_dir():
            out.extend(sorted(p.glob("*.ndjson")))
        else:
            out.append(p)
    return list(dict.fromkeys(q.resolve() for q in out))


def check_trace(spec_dir: Path, name: str, trace: Path, out: Path, budget: int, index: int) -> dict:
    """Runs the trace spec on one trace. Returns {outcome: accepted|rejected|error, at, detail}."""
    stem = f"trace-{index:03d}"
    args = ["-workers", "1", "-config", f"{name}Trace.cfg", f"{name}Trace.tla"]
    res, _ = tc.run_tlc(spec_dir, args, out_dir=out, log_path=out / f"{stem}.log", budget_s=budget,
                        traces_dir=trace.parent, trace_path=f"/traces/{trace.name}")
    if res.errors:
        return {"outcome": "error", "at": None, "detail": "; ".join(res.errors)[:300], "log": f"{stem}.log"}
    v = res.violation()
    if v is not None:
        kind, prop = v
        if kind == "postcondition":
            at = res.trace_rejected_at
            return {"outcome": "rejected", "at": at, "detail": "no behavior explains this line",
                    "log": f"{stem}.log"}
        # A design invariant fails in the state the last consumed line produced.
        at = max(res.cex_states - 2, 0) if res.cex_states else None
        return {"outcome": "rejected", "at": at, "detail": f"{kind} {prop} violated after this line".strip(),
                "log": f"{stem}.log"}
    if res.complete:
        return {"outcome": "accepted", "at": None, "detail": "", "log": f"{stem}.log"}
    return {"outcome": "error", "at": None, "detail": tc.incomplete_reason(res, budget), "log": f"{stem}.log"}


def cmd_lint(paths: list[Path]) -> int:
    bad = 0
    files = collect(paths)
    for f in files:
        errors = lint_file(f)
        for e in errors:
            print(f"{f}: {e}")
        bad += bool(errors)
    print(f"{len(files) - bad} valid, {bad} invalid")
    return 1 if bad else 0


def cmd_check(args: argparse.Namespace) -> int:
    executed = failed = 0
    errors: list[str] = []
    failing: list[dict] = []
    spec_dir = args.spec_dir.resolve()
    rel = spec_dir.relative_to(tc.REPO_ROOT).as_posix() if spec_dir.is_relative_to(tc.REPO_ROOT) else str(spec_dir)
    tla = {"spec": f"{rel}/{args.name}", "mode": "trace", "tier": args.tier, "tools": tc.tool_versions()}
    accepted = rejected = expected_rejections = 0
    out = args.out
    try:
        module = args.module or spec_dir.name
        budget = tc.budget_for(module, args.budget_s)
        tla["budget_s"] = budget
        for needed in (f"{args.name}Trace.tla", f"{args.name}Trace.cfg"):
            if not (spec_dir / needed).exists():
                raise tc.TlaError(f"{rel}/{needed} does not exist")
        if out is None:
            out = tc.REPO_ROOT / "target" / "tla" / rel / f"{args.name}Trace"
            shutil.rmtree(out, ignore_errors=True)
        out.mkdir(parents=True, exist_ok=True)
        out = out.resolve()
        plain = [(t, False) for t in collect(args.traces)]
        negative = [(t, True) for t in collect(args.expect_rejected)]
        print(f"[tla_trace] {tla['spec']}: {len(plain)} traces to accept, {len(negative)} to reject, "
              f"budget {budget} s per trace", flush=True)
        for index, (trace, must_reject) in enumerate(plain + negative):
            executed += 1
            label = trace.name
            lint = lint_file(trace)
            if lint:
                failed += 1
                failing.append({"file": label[:200], "index": None, "reason": ("lint: " + lint[0])[:300]})
                print(f"[tla_trace] FAIL {label}: not a valid {FORMAT} trace")
                for e in lint[:20]:
                    print(f"    {e}")
                continue
            header = json.loads(read_lines(trace)[0])
            if header["spec"] != args.name:
                failed += 1
                failing.append({"file": label[:200], "index": None,
                                "reason": f"header spec is {header['spec']}, not {args.name}"})
                print(f"[tla_trace] FAIL {label}: header spec is {header['spec']}, not {args.name}")
                continue
            result = check_trace(spec_dir, args.name, trace, out, budget, index)
            if result["outcome"] == "error":
                errors.append(f"{label}: {result['detail']}"[:300])
                print(f"[tla_trace] ERROR {label}: {result['detail']} (log {result['log']})")
                continue
            if result["outcome"] == "accepted":
                accepted += 1
            else:
                rejected += 1
            at = result["at"]
            lines = read_lines(trace)
            content = lines[at + 1] if at is not None and 0 <= at + 1 < len(lines) else "(end of trace)"
            if must_reject:
                m = AT.search(trace.name)
                want = int(m.group(1)) if m else None
                ok = result["outcome"] == "rejected" and (want is None or want == at)
                if ok:
                    expected_rejections += 1
                    print(f"[tla_trace] ok   {label}: rejected as expected at index {at}: {content}")
                else:
                    failed += 1
                    why = ("accepted, but it must be rejected" if result["outcome"] == "accepted"
                           else f"rejected at index {at}, expected {want}")
                    failing.append({"file": label[:200], "index": at, "reason": why})
                    print(f"[tla_trace] FAIL {label}: {why}")
            elif result["outcome"] == "accepted":
                print(f"[tla_trace] ok   {label}: accepted, {len(lines) - 1} steps")
            else:
                failed += 1
                failing.append({"file": label[:200], "index": at, "reason": f"{result['detail']}: {content}"[:300]})
                print(f"[tla_trace] FAIL {label}: REJECTED at index {at} (seq {at}, file line "
                      f"{at + 2 if at is not None else '?'}), {result['detail']}:\n    {content}")
    except (tc.TlaError, OSError, ValueError) as exc:
        errors.append(f"setup: {exc}"[:300])
        print(f"[tla_trace] ERROR setup: {exc}")

    tla["traces"] = {"checked": executed, "accepted": accepted, "rejected": rejected,
                     "expected_rejections": expected_rejections, "failing": failing[:20]}
    not_run = [{"id": "tlc-error", "reason": e} for e in errors]
    if errors:
        verdict = "error"
    elif failed:
        verdict = "fail"
    elif executed == 0:
        verdict = "not-run"
        not_run.append({"id": "zero-traces", "reason": "no trace files were given"})
    else:
        verdict = "pass"
    artifacts = []
    if out is not None and Path(out).exists():
        (Path(out) / "summary.json").write_text(json.dumps({"verdict": verdict, "tla": tla}, indent=2) + "\n",
                                                encoding="utf-8")
        for f in sorted(Path(out).glob("*")):
            if f.is_file() and len(artifacts) < 40:
                artifacts.append({"name": f"tla/{f.name}"[:200],
                                  "sha256": hashlib.sha256(f.read_bytes()).hexdigest()})
    tc.print_summary({"verdict": verdict, "executed": executed, "failed": failed, "not_run": not_run[:200],
                      "tla": tla, "artifacts": artifacts})
    return EXIT[verdict]


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    li = sub.add_parser("lint", help="check the trace format")
    li.add_argument("paths", nargs="+", type=Path)
    ch = sub.add_parser("check", help="check traces against <Name>Trace.tla")
    ch.add_argument("spec_dir", type=Path)
    ch.add_argument("name")
    ch.add_argument("traces", nargs="*", type=Path)
    ch.add_argument("--expect-rejected", action="append", type=Path, default=[])
    ch.add_argument("--module")
    ch.add_argument("--tier", choices=("T0", "T1", "T2"), default="T1", help="recorded in the evidence")
    ch.add_argument("--budget-s", type=int)
    ch.add_argument("--out", type=Path)
    args = p.parse_args(argv)
    if args.cmd == "lint":
        return cmd_lint(args.paths)
    return cmd_check(args)


if __name__ == "__main__":
    sys.exit(main())
