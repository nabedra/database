#!/usr/bin/env python3
"""Lint `nabedra.trace/1` traces and check them against a design spec with TLC.

  tla_trace.py lint <file.ndjson>...
  tla_trace.py check <spec dir> <Name> <trace files or dirs>... [--expect-rejected DIR]...
                     [--module M] [--tier T] [--budget-s S] [--out DIR]

`lint` checks the format (.agents/skills/design-review/references/trace-validation.md): line 1 is the
header {"trace":"nabedra.trace/1","spec","seed","sha","workload","params"}, and every later line is a
step {"seq","t","node","action","vars"[,"msg"]} with seq counting from 0 and t never decreasing. A
trace has at least one step. Values are JSON objects, arrays, strings, booleans and integers: no floats
and no nulls, and no key twice in one object. TLC integers have 32 bits, and TLC compares `vars` and
`params`, so an integer there must fit.

`check` lints each trace, then runs `<Name>Trace.tla` with `<Name>Trace.cfg` once per trace, with
TRACE_PATH set, in the pinned image. TLC reads a copy of the trace without `t` and `seed`, where an
integer in `msg` that does not fit 32 bits is a decimal string (`tlc_copy`). A trace is accepted when
some behavior of the spec consumes every line through TraceStep (the POSTCONDITION TraceAccepted from
specs/lib/NabedraTrace.tla). `trace_spec_errors` refuses a trace spec that could accept anything, and
`wildcard_error` runs one control trace against it. For a rejected trace it prints the 0-based index
and the content of the first line no behavior explains, and the variables that are missing, unchecked
or different there. A directory argument means its `*.ndjson` files. Every trace under
--expect-rejected must be rejected, and one named `*.at-<n>.ndjson` must be rejected at index n.

If `<spec dir>/trace-mutants/` exists, each `*.cfg` there runs `<Name>TraceMutants.tla` on one trace.
Its first lines name what must come out and the trace, relative to the spec folder:

  \\* expect: rejected at 6     or `accepted` or `error`, optionally `at <n>` and `: <text in the reason>`
  \\* trace: traces/rejected/<file>

One assertion per trace and per trace mutant. The last line is `nabedra-tla-summary: {json}`.
Exit status: 0 pass, 1 fail, 2 not-run, 3 error.
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
I32_MIN, I32_MAX = -(2**31), 2**31 - 1
# specs/lib/NabedraTrace.tla: every step consumes one line, and every line is consumed.
LEVEL_INVARIANT = "TraceIndexIsLevel"
POSTCONDITION = "TraceAccepted"
LIB_MODULE = "NabedraTrace"
REQUIRING = ("RequireVar", "RequireSet", "ChangedVar", "ChangedSet")
# Operators that older trace specs used. They accepted a line that did not log the variable.
REMOVED = {
    "IsEvent": "TraceStep(action name, spec action, checks), which consumes the line and checks its variables",
    "MatchVar": "RequireVar, ChangedVar or OptionalVar in the checks of a TraceStep",
    "MatchSet": "RequireSet, ChangedSet or OptionalSet in the checks of a TraceStep",
    "Logged": "ChangedVar or OptionalVar in the checks of a TraceStep",
    "LoggedValue": "RequireVar, ChangedVar or OptionalVar in the checks of a TraceStep",
}
# Modules that NabedraTrace builds on. A file with one of these names in a spec folder would replace it.
STANDARD_MODULES = {"Naturals", "Integers", "Sequences", "FiniteSets", "Json", "IOUtils", "TLC", "TLCExt"}
# What NabedraTrace takes from them, so a configuration must not override it.
STANDARD_OPERATORS = {"ndJsonDeserialize", "IOEnv", "TLCGet", "TLCSet", "PrintT", "SubSeq", "Len"}
CONTROL_ACTION = "NabedraTraceControl"
_EXPECT_TRACE = re.compile(r"^\\\*\s*expect:\s*(accepted|rejected|error)(?:\s+at\s+(\d+))?(?:\s*:\s*(.+?))?\s*$")
_MUTANT_TRACE = re.compile(r"^\\\*\s*trace:\s*(\S+)\s*$")


def _is_int(v) -> bool:
    return isinstance(v, int) and not isinstance(v, bool)


class DuplicateKey(ValueError):
    pass


def _pairs(pairs):
    out = {}
    for k, v in pairs:
        if k in out:
            raise DuplicateKey(f"the key {k!r} appears twice in one object")
        out[k] = v
    return out


def _load(raw: str):
    """json.loads, with an error for a key that appears twice."""
    try:
        return json.loads(raw, object_pairs_hook=_pairs)
    except RecursionError:
        raise ValueError("nested too deeply") from None


def _fits(v) -> bool:
    return I32_MIN <= v <= I32_MAX


def _value_errors(value, where: str, bounded: bool) -> list[str]:
    """TLA+-representable JSON: objects, arrays, strings, booleans and integers.

    `bounded`: TLC uses the value, so its integers must fit TLC's 32 bits. TLC wraps a larger one
    modulo 2^32 without an error, so it could equal a wrong value."""
    if isinstance(value, dict):
        out = []
        for k, v in value.items():
            out.extend(_value_errors(v, f"{where}.{k}", bounded))
        return out
    if isinstance(value, list):
        out = []
        for i, v in enumerate(value):
            out.extend(_value_errors(v, f"{where}[{i}]", bounded))
        return out
    if isinstance(value, (str, bool)):
        return []
    if _is_int(value):
        if bounded and not _fits(value):
            return [f"{where}: {value} does not fit TLC's 32-bit integers (log it as a string)"]
        return []
    if value is None:
        return [f"{where}: null has no TLA+ value"]
    return [f"{where}: {type(value).__name__} is not allowed (use integers)"]


def lint_lines(lines: list[str]) -> list[str]:
    """Every format problem, as `line <n>: <problem>` strings (1-based file lines)."""
    errors: list[str] = []
    if not lines:
        return ["line 1: the trace is empty; the header is missing"]
    if len(lines) == 1:
        errors.append("line 2: the trace has no steps, so it shows nothing")
    prev_t = None
    for number, raw in enumerate(lines, start=1):
        where = f"line {number}"
        if not raw.strip():
            errors.append(f"{where}: blank line")
            continue
        try:
            obj = _load(raw)
        except DuplicateKey as exc:
            errors.append(f"{where}: {exc}")
            continue
        except ValueError as exc:
            errors.append(f"{where}: not JSON: {getattr(exc, 'msg', exc)}")
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
                    errors.extend(_value_errors(obj["params"], f"{where}: params", True))
            continue
        missing = STEP_REQUIRED - obj.keys()
        extra = obj.keys() - STEP_KEYS
        if missing:
            errors.append(f"{where}: step misses {sorted(missing)}")
        if extra:
            errors.append(f"{where}: step has unknown keys {sorted(extra)}")
        if "seq" in obj and not (_is_int(obj["seq"]) and obj["seq"] == number - 2):
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
                    errors.extend(_value_errors(obj[key], f"{where}: {key}", key == "vars"))
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


def uncovered(paths: list[Path], covered: list[Path]) -> list[str]:
    """Directory arguments that would silently check less than they hold."""
    out = []
    for p in paths:
        if not p.exists():
            out.append(f"{p} does not exist")
        elif p.is_dir():
            nested = [q for q in sorted(p.rglob("*.ndjson")) if q.resolve() not in covered]
            if nested:
                out.append(f"{nested[0]} is in a subfolder of {p}, which is not checked")
    return out


def tlc_copy(lines: list[str]) -> tuple[str, dict[int, list[str]]]:
    """The trace that TLC reads, and the `msg` fields that became strings, by step index.

    TLC wraps integers modulo 2^32, and `t` and `seed` have 64 bits. The lint checked both with exact
    integers, so the copy leaves them out, and a trace spec that reads either gets a TLC error. In `msg`
    an integer that does not fit becomes its decimal string. In `vars` and `params` the lint refused it."""
    wide: dict[int, list[str]] = {}

    def narrow(value, path: str, index: int):
        if isinstance(value, dict):
            return {k: narrow(v, f"{path}.{k}", index) for k, v in value.items()}
        if isinstance(value, list):
            return [narrow(v, f"{path}[{i}]", index) for i, v in enumerate(value)]
        if _is_int(value) and not _fits(value):
            wide.setdefault(index, []).append(path)
            return str(value)
        return value

    out = []
    for i, raw in enumerate(lines):
        obj = _load(raw)
        obj.pop("seed" if i == 0 else "t", None)
        if "msg" in obj:
            obj["msg"] = narrow(obj["msg"], "msg", i - 1)
        out.append(json.dumps(obj, separators=(",", ":")))
    return "\n".join(out) + "\n", wide


def _closure(spec_dir: Path, name: str) -> dict[str, str]:
    """The module and the modules of its folder that it extends or instantiates, comments removed."""
    found: dict[str, str] = {}
    todo = [name]
    while todo:
        mod = todo.pop()
        path = spec_dir / f"{mod}.tla"
        if mod in found or not path.exists():
            continue
        text = tc.strip_tla_comments(path.read_text(encoding="utf-8"), keep_strings=False)
        found[mod] = text
        for m in re.finditer(r"\bEXTENDS\s+(\w+(?:\s*,\s*\w+)*)", text):
            todo += re.findall(r"\w+", m.group(1))
        todo += re.findall(r"\bINSTANCE\s+(\w+)", text)
    return found


def _defines(text: str, op: str) -> bool:
    return re.search(rf"(?<![\w!]){op}\s*(?:\([^)]*\))?\s*==", text) is not None


def lib_operators() -> set[str]:
    text = tc.strip_tla_comments((tc.SPECS_DIR / "lib" / f"{LIB_MODULE}.tla").read_text(encoding="utf-8"))
    return set(re.findall(r"^\s*(?:LOCAL\s+)?(\w+)\s*(?:\([^)]*\))?\s*==", text, re.M)) | {"traceIndex", "traceExplained"}


def trace_spec_errors(spec_dir: Path, name: str) -> list[str]:
    """Why `<Name>Trace.tla` and its cfg could accept a trace they do not explain.

    It finds the mistakes that leave a trace unchecked. It cannot tell that a spec action is too weak, that
    a TraceStep stutters under an action's name, or that OptionalVar or NoVars is used without a reason."""
    errors: list[str] = []
    trace_mod = f"{name}Trace"
    lib = {p.stem for p in (tc.SPECS_DIR / "lib").glob("*.tla")} | STANDARD_MODULES
    for path in sorted(spec_dir.glob("*.tla")):
        if path.stem in lib:
            errors.append(f"{path.name} in the spec folder would replace the shared module of that name")

    cfg = tc.parse_cfg(spec_dir / f"{trace_mod}.cfg")
    if LEVEL_INVARIANT not in cfg.invariants:
        errors.append(f"{trace_mod}.cfg must list the invariant {LEVEL_INVARIANT} (specs/lib/{LIB_MODULE}.tla)")
    if POSTCONDITION not in cfg.postconditions:
        errors.append(f"{trace_mod}.cfg must set POSTCONDITION {POSTCONDITION}: without it every trace is accepted")
    for op in sorted(set(cfg.overrides) & (lib_operators() | STANDARD_OPERATORS)):
        errors.append(f"{trace_mod}.cfg overrides {op}, which the trace check relies on")

    modules = _closure(spec_dir, trace_mod)
    for op, instead in REMOVED.items():
        users = [m for m, text in modules.items() if re.search(rf"\b{op}\b", text)]
        if users and not any(_defines(text, op) for text in modules.values()):
            errors.append(f"{users[0]}.tla uses {op}, which accepted a line that does not log the variable and "
                          f"was removed. Use {instead}")
    used = modules.get(trace_mod, "")
    helpers = "\n".join(modules.values())
    if re.search(r"\btrace(?:Index|Explained)\s*(?:>>\s*)?'|\bUNCHANGED\s*(?:<<[^>]*)?\btrace(?:Index|Explained)\b", used):
        errors.append(f"{trace_mod}.tla sets traceIndex or traceExplained; only TraceStep may consume a line")
    if not re.search(r"\bTraceStep\b", helpers):
        errors.append(f"{trace_mod}.tla takes no TraceStep, so it consumes no line")
    if not re.search(rf"\b(?:{'|'.join(REQUIRING)})\b", helpers):
        errors.append(f"{trace_mod}.tla requires no variable: no step uses {', '.join(REQUIRING[:-1])} or "
                      f"{REQUIRING[-1]}, so a trace of action names alone is accepted")
    return errors


def _printable(text: str) -> str:
    """One ASCII line, so that trace content cannot look like another line of the output."""
    return text.encode("ascii", "backslashreplace").decode()


_NOTE = {"missing": "missing", "unchecked": "logged but not checked by the step", "different": "different value"}


def _notes(res: tc.TlcResult, at: int | None) -> str:
    """The variables TraceStep found wrong at the rejected line, over the behaviors that reach it."""
    by_kind: dict[str, list[str]] = {}
    for index, kind, var in res.trace_notes:
        if index == at:
            by_kind.setdefault(kind, []).append(var)
    return "; ".join(f"{_NOTE.get(k, k)}: {', '.join(v)}" for k, v in by_kind.items())


def check_trace(spec_dir: Path, name: str, trace: Path, out: Path, budget: int, stem: str, *,
                module: str | None = None, cfg: str | None = None) -> dict:
    """Runs the trace spec on one trace. Returns {outcome: accepted|rejected|error, at, detail, log}."""
    module = module or f"{name}Trace"
    text, wide = tlc_copy(read_lines(trace))
    copies = out / "traces" / stem
    copies.mkdir(parents=True, exist_ok=True)
    (copies / trace.name).write_text(text, encoding="utf-8")
    args = ["-workers", "1", "-config", cfg or f"{name}Trace.cfg", f"{module}.tla"]
    res, _ = tc.run_tlc(spec_dir, args, out_dir=out, log_path=out / f"{stem}.log", budget_s=budget,
                        traces_dir=copies, trace_path=f"/traces/{trace.name}")

    def result(outcome: str, at: int | None, detail: str) -> dict:
        return {"outcome": outcome, "at": at, "detail": _printable(detail), "log": f"{stem}.log"}

    if res.errors:
        # TLC prints the behavior up to the state it could not expand, so traceIndex names the line.
        at = res.last_trace_index - 1 if res.last_trace_index is not None else None
        # Nearest first: the line TLC was evaluating, then the ones it may have taken values from.
        fields = [f"index {i} {path}" for i in sorted(wide, reverse=True) if at is None or i <= at for path in wide[i]]
        wider = f"outside 32 bits, so TLC reads a string: {', '.join(fields[:5])}; " if fields else ""
        return result("error", at, wider + "; ".join(res.errors))
    v = res.violation()
    if v is not None:
        kind, prop = v
        if (kind, prop) == ("invariant", LEVEL_INVARIANT):
            return result("error", None, f"{module}.tla takes a step that consumes no line, or one without "
                          f"TraceStep ({LEVEL_INVARIANT} violated), so a line may be left unexplained")
        if kind == "postcondition":
            at = res.trace_rejected_at
            notes = _notes(res, at)
            return result("rejected", at, "no behavior explains this line" + (f" ({notes})" if notes else ""))
        # A design invariant fails in the state the last consumed line produced.
        at = max(res.cex_states - 2, 0) if res.cex_states else None
        return result("rejected", at, f"{kind} {prop} violated after this line".strip())
    if res.complete:
        return result("accepted", None, "")
    return result("error", None, tc.incomplete_reason(res, budget))


def wildcard_error(spec_dir: Path, name: str, accepted: Path, out: Path, budget: int) -> str | None:
    """Checks that the trace spec rejects a line whose action no spec has, after an accepted first line."""
    header, first = read_lines(accepted)[:2]
    step = _load(first)
    bogus = {"seq": 1, "t": step["t"], "node": step["node"], "action": CONTROL_ACTION, "vars": {}}
    control = out / "control.ndjson"
    control.write_text("\n".join([header, first, json.dumps(bogus, separators=(",", ":"))]) + "\n", encoding="utf-8")
    got = check_trace(spec_dir, name, control, out, budget, "control")
    if got["outcome"] == "rejected" and got["at"] == 1:
        return None
    return (f"control: a line with the unknown action {CONTROL_ACTION} at index 1 must be rejected there, but the "
            f"trace was {got['outcome']}" + (f" at index {got['at']}" if got["at"] is not None else "")
            + (f": {got['detail']}" if got["detail"] else "") + f" (log {got['log']})")


def check_mutant(spec_dir: Path, name: str, cfg: Path, out: Path, budget: int) -> str | None:
    """Runs one trace mutant. Returns what is wrong, or None."""
    expect = trace = None
    for line in cfg.read_text(encoding="utf-8").splitlines()[:2]:
        expect = expect or _EXPECT_TRACE.match(line.strip())
        trace = trace or _MUTANT_TRACE.match(line.strip())
    if not expect or not trace:
        return r"its first two lines must be `\* expect: accepted|rejected|error [at <n>][: <text>]` and `\* trace: <path>`"
    path = spec_dir / trace.group(1)
    lint = lint_file(path)
    if lint:
        return f"{trace.group(1)}: {lint[0]}"
    got = check_trace(spec_dir, name, path, out, budget, f"mutant-{cfg.stem}", module=f"{name}TraceMutants",
                      cfg=f"{cfg.parent.name}/{cfg.name}")
    want = (expect.group(1), int(expect.group(2)) if expect.group(2) else got["at"], expect.group(3) or "")
    if (got["outcome"], got["at"]) == want[:2] and want[2] in got["detail"]:
        return None
    at = f" at index {got['at']}" if got["at"] is not None else ""
    return (f"expected `{expect.group(0).split('expect:')[1].strip()}`, but the trace was {got['outcome']}{at}"
            + (f": {got['detail']}" if got["detail"] else ""))


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
    executed = failed = mutants = 0
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
        problems = trace_spec_errors(spec_dir, args.name)
        if problems:
            for problem in problems:
                print(f"[tla_trace] ERROR {rel}: {problem}")
            raise tc.TlaError(problems[0] + (f" (and {len(problems) - 1} more)" if len(problems) > 1 else ""))
        if out is None:
            out = tc.REPO_ROOT / "target" / "tla" / rel / f"{args.name}Trace"
            shutil.rmtree(out, ignore_errors=True)
        out.mkdir(parents=True, exist_ok=True)
        out = out.resolve()
        plain = [(t, False) for t in collect(args.traces)]
        negative = [(t, True) for t in collect(args.expect_rejected)]
        covered = [t for t, _ in plain + negative]
        for problem in uncovered(args.traces + args.expect_rejected, covered):
            raise tc.TlaError(problem)
        for d in args.expect_rejected:
            if not collect([d]):
                raise tc.TlaError(f"{d} (--expect-rejected) has no traces")
        control = None
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
            result = check_trace(spec_dir, args.name, trace, out, budget, f"trace-{index:03d}")
            if result["outcome"] == "error":
                where = f" at index {result['at']}" if result["at"] is not None else ""
                errors.append(f"{label}:{where} {result['detail']}"[:300])
                print(f"[tla_trace] ERROR {label}:{where} {result['detail']} (log {result['log']})")
                continue
            if result["outcome"] == "accepted":
                accepted += 1
                control = control or (None if must_reject else trace)
            else:
                rejected += 1
            at = result["at"]
            lines = read_lines(trace)
            content = _printable(lines[at + 1]) if at is not None and 0 <= at + 1 < len(lines) else "(end of trace)"
            if must_reject:
                m = AT.search(trace.name)
                want = int(m.group(1)) if m else None
                ok = result["outcome"] == "rejected" and (want is None or want == at)
                if ok:
                    expected_rejections += 1
                    print(f"[tla_trace] ok   {label}: rejected as expected at index {at}, {result['detail']}: "
                          f"{content}")
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
        if control is not None:
            problem = wildcard_error(spec_dir, args.name, control, out, budget)
            if problem:
                errors.append(problem[:300])
                print(f"[tla_trace] ERROR {problem}")
            else:
                print(f"[tla_trace] ok   control: a line with the unknown action {CONTROL_ACTION} is rejected")
        for cfg in sorted((spec_dir / "trace-mutants").glob("*.cfg")):
            executed += 1
            mutants += 1
            problem = check_mutant(spec_dir, args.name, cfg, out, budget)
            if problem:
                failed += 1
                failing.append({"file": f"trace-mutants/{cfg.name}"[:200], "index": None, "reason": problem[:300]})
                print(f"[tla_trace] FAIL trace-mutants/{cfg.name}: {problem}")
            else:
                print(f"[tla_trace] ok   trace-mutants/{cfg.name}: {cfg.read_text().splitlines()[0][3:]}")
    except (tc.TlaError, OSError, ValueError) as exc:
        errors.append(f"setup: {exc}"[:300])
        print(f"[tla_trace] ERROR setup: {exc}")

    tla["traces"] = {"checked": executed - mutants, "accepted": accepted, "rejected": rejected,
                     "expected_rejections": expected_rejections, "failing": failing}
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
    if len(failing) > 20:  # the record's limit; summary.json above has them all
        tla["traces"]["failing"] = failing[:20]
        tla["truncated"] = True
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
