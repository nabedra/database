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
  5. every `mutants/*.cfg`. Its control, the same cfg with the baseline's value of Mutant, must pass like a
     baseline, and then it must fail on exactly the property its first line `\\* expect: <Property>` names
     (or `Deadlock`). The counterexample length is recorded.
T2 applies the search rule and runs `<Name>.large.cfg`, exhaustively or, with --simulate, as a seeded
simulation labelled as such.

The search rule. TLC reports an exhaustive search whatever a cfg cuts away, so:
  - a module of the model, and its cfg, may not read or set TLC's search state, the trace, the clock, the
    environment or random values (TLCGet, TLCSet, TLCExt, RandomElement, ...), so no constraint can be a depth
    limit or a state cap in those terms. A cfg may override a name only with an operator that the model's own
    modules define, so it cannot bring one in either;
  - a CONSTRAINT must be declared in its cfg by `\\* limit: <Name> -- <reason>`, and its definition must be, from
    end to end, what the grammar of `_Bound` accepts: a conjunction of upper bounds on counters. Everything
    else is refused, and so is every ACTION_CONSTRAINT, which removes steps;
  - a constraint or a symmetry set may not be active in an exhaustive run that checks liveness. TLC says which
    runs do. In a simulation it says nothing, so there this part does not apply;
  - VIEW is refused, because it merges states that the properties may tell apart;
  - a cfg may not override a name it checks, its constraints or its INIT, NEXT or SPECIFICATION. Giving a
    declared CONSTANT its value is free. Overriding any other definition, such as Nat, needs a `limit` line;
  - an override may not replace a definition that a constraint uses, unless by a constant value;
  - a cfg sets each CONSTANT or other name, and INIT, NEXT and SPECIFICATION, once;
  - a mutant cfg is held to the same rule, with the baselines' `limit` lines only (mutant_problems). It sets the
    constant Mutant to another value than the baselines that check its property, of which there must be one, and
    runs the INIT, NEXT or SPECIFICATION of a baseline. The control shows that the property holds at the
    mutant's own bounds, so other constants may differ. The property may not read Mutant, and the specification's
    definitions name the mutant's value, because a control cannot show that: a property that asks about Mutant
    holds in the control and fails in the mutant although no step changed. Otherwise the mutant does not count
    as caught.

A mutant's control (Controls) is a run of its own, with the per-run budget. It is not run when a baseline, or the
control of another mutant, has the same settings and checks the property.

The record lists the declared limits and how many states each constraint kept. What the rule cannot see: a
variable under an upper bound that is no counter of growth, such as a step counter that every action
increments, or a protocol phase kept as a number; a CONSTANT value, INIT or NEXT that leaves behavior out.
A reviewer has to read those, so summary.json lists the settings of every baseline and mutant (describe_settings),
and the record has their short form and their digest.

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
import tla_trace  # noqa: E402

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
_EXTENDS = re.compile(r"\bEXTENDS\s+(\w+(?:\s*,\s*\w+)*)")
_INSTANCE = re.compile(r"\bINSTANCE\s+(\w+)")
_CONSTANT_DECL = re.compile(r"\bCONSTANTS?\s+(\w+(?:\([\s,_]*\))?(?:\s*,\s*\w+(?:\([\s,_]*\))?)*)")
_VARIABLE_DECL = re.compile(r"\bVARIABLES?\s+(\w+(?:\s*,\s*\w+)*)")
# The parameters of a definition, with one level of brackets inside, as in Op(F(_), x) or f[x \in S[1]].
_PARAMETERS = r"\((?:[^()=]|\([^()=]*\))*\)|\[(?:[^\[\]=]|\[[^\[\]=]*\])*\]"
_UNIT = (r"(?:LOCAL|RECURSIVE|VARIABLES?|CONSTANTS?|ASSUME|ASSUMPTION|AXIOM|THEOREM|LEMMA|PROPOSITION|COROLLARY|"
         r"INSTANCE)\b")
_HEAD = re.compile(rf"(?<!\w)(\w+)\s*({_PARAMETERS})?\s*==(?!=)")
_NEXT_UNIT = re.compile(rf"^(?:{_UNIT}|\w+[ \t]*(?:{_PARAMETERS})?\s*==(?!=)|-{{4}}|={{4}})", re.M)
_ORDER_DEFINITION = re.compile(r"^[ \t]*\w+\s*(?:<=|=<|\\leq|<)\s*\w+\s*==(?!=)", re.M)
_OVERRIDE = re.compile(r"(\w+)\s*<-\s*(?:\[\s*\w+\s*\]\s*)?(\w+)")


class _Refused(Exception):
    """The text does not match the grammar. `at` is the offset in the module's text."""

    def __init__(self, at: int, why: str) -> None:
        super().__init__(why)
        self.at, self.why = at, why


class _Reader:
    """The tokens of a definition's body, from `pos` on."""

    _TOKEN = re.compile(r"\s*(\\[A-Za-z]+|\w+|<=|=<|/\\|[^\s\w])")
    # What may follow a whole body: the end of the module, a line of dashes or equal signs, or a line that starts
    # the next unit of the module.
    _END = re.compile(rf"\s*\Z|\s*\n[ \t]*(?:-{{4}}|={{4}}|{_UNIT}|\w+\s*(?:{_PARAMETERS})?\s*==(?!=)|"
                      r"\w+\s*(?:\\\w+|[^\s\w]+)\s*\w+\s*==(?!=))")

    def __init__(self, model: "Model", text: str, pos: int) -> None:
        self.model, self.text, self.pos = model, text, pos

    def peek(self) -> str:
        m = self._TOKEN.match(self.text, self.pos)
        return m.group(1) if m else ""

    def take(self) -> str:
        m = self._TOKEN.match(self.text, self.pos)
        if not m:
            raise _Refused(self.pos, "the definition ends too early")
        self.pos = m.end()
        return m.group(1)

    def refuse(self, why: str) -> None:
        raise _Refused(self.pos + len(self.text[self.pos:]) - len(self.text[self.pos:].lstrip()), why)

    def expect(self, *tokens: str) -> str:
        if self.peek() not in tokens:
            self.refuse(f"{self.peek()!r} instead of {' or '.join(tokens)}")
        return self.take()

    def constant(self) -> None:
        if not self.model.is_constant(self.peek()):
            self.refuse(f"{self.peek()!r} is not a declared CONSTANT")
        self.take()

    def end(self) -> None:
        if not self._END.match(self.text, self.pos):
            self.refuse(f"{self.peek()!r} after the end")


class _ConstantSet(_Reader):
    r"""A set made of constants:

      set  ::= part { (\cup | \union) part }
      part ::= CONSTANT | "{" CONSTANT { "," CONSTANT } "}"
    """

    def read(self) -> None:
        self.part()
        while self.peek() in ("\\cup", "\\union"):
            self.take()
            self.part()
        self.end()

    def part(self) -> None:
        if self.peek() != "{":
            self.constant()
            return
        self.take()
        self.constant()
        while self.peek() == ",":
            self.take()
            self.constant()
        self.expect("}")


class _Bound(_Reader):
    r"""A constraint: upper bounds on counters, and nothing else.

      body    ::= [and] item { and item }          and   ::= "/\" | \land
      item    ::= "(" body ")" | "\A" NAME { "," NAME } \in SET ":" body | counter less limit | BOUND
      counter ::= ref | Len "(" ref ")" | Cardinality "(" ref ")"
      ref     ::= VARIABLE { "[" index { "," index } "]" }
      index   ::= a NAME of an enclosing "\A" | CONSTANT
      less    ::= "<" | "<=" | "=<" | \leq         limit ::= NUMBER | CONSTANT

    VARIABLE and CONSTANT are declared in the model module or one it extends, and defined nowhere. SET is a
    CONSTANT, or the name of a definition that _ConstantSet reads. BOUND is the name of another definition
    that _Bound reads. The body has to match up to the next unit of the module."""

    AND = ("/\\", "\\land")
    ALLOWED = ("a constraint may only be a conjunction of upper bounds `c < limit` or `c <= limit`, under "
               "`\\A i \\in Set` if needed, where c is v, v[i], Len(v) or Cardinality(v) of a variable v, and "
               "limit a number or a declared CONSTANT")

    def __init__(self, model: "Model", text: str, pos: int, seen: frozenset) -> None:
        super().__init__(model, text, pos)
        self.seen, self.names, self.used = seen, [], set()  # used: the definitions it relies on

    def read(self) -> None:
        self.body()
        self.end()

    def body(self) -> None:
        if self.peek() in self.AND:
            self.take()
        self.item()
        while self.peek() in self.AND:
            self.take()
            self.item()

    def item(self) -> None:
        token = self.peek()
        if token == "(":
            self.take()
            self.body()
            self.expect(")")
        elif token == "\\A":
            self.quantified()
        elif token in ("Len", "Cardinality"):
            self.take()
            self.expect("(")
            self.ref()
            self.expect(")")
            self.limit()
        elif self.model.is_variable(token):
            self.ref()
            self.limit()
        elif re.fullmatch(r"[A-Za-z_]\w*", token) and self.model.places(token):
            problem = self.model.bound_problem(token, self.seen)
            if problem:
                self.refuse(problem)
            self.used |= {token} | self.model.used[token]
            self.take()
        else:
            self.refuse(f"{token!r} starts no upper bound")

    def quantified(self) -> None:
        self.take()
        count = len(self.names)
        while True:
            name = self.peek()
            if not re.fullmatch(r"[A-Za-z_]\w*", name) or name in self.model.variables | self.model.constants:
                self.refuse(f"{name!r} is not a fresh name")
            self.names.append(self.take())
            if self.expect(",", "\\in") == "\\in":
                break
        if not self.model.is_constant(self.peek()) and not self.model.is_constant_set(self.peek()):
            self.refuse(f"{self.peek()!r} is not a declared set of constants")
        self.used.add(self.take())
        self.expect(":")
        self.body()
        del self.names[count:]

    def ref(self) -> None:
        if not self.model.is_variable(self.peek()):
            self.refuse(f"{self.peek()!r} is not a declared VARIABLE")
        self.take()
        while self.peek() == "[":
            self.take()
            while True:
                if self.peek() not in self.names and not self.model.is_constant(self.peek()):
                    self.refuse(f"{self.peek()!r} is not a quantified name or a CONSTANT")
                self.take()
                if self.expect(",", "]") == "]":
                    break

    def limit(self) -> None:
        self.expect("<", "<=", "=<", "\\leq")
        if re.fullmatch(r"[0-9]+", self.peek()):
            self.take()
        else:
            self.constant()


class Model:
    """The modules of a model: <Name> or MC<Name> and what it extends or instantiates under specs/.
    Standard and community modules are not read."""

    def __init__(self, spec_dir: Path, model: str) -> None:
        self.texts: dict[str, str] = {}
        self.lines: dict[str, int] = {}     # the file line where each text starts
        self.constants: set[str] = set()    # declared in the model module or one it extends
        self.variables: set[str] = set()
        self.problems: list[str] = []
        self._index: dict[str, list[tuple[str, re.Match]]] | None = None
        self._regions: dict[str, list[tuple[str, str]]] = {}
        self.used: dict[str, set[str]] = {}  # what each definition that bound_problem accepted relies on
        self.full: dict[str, list[str]] = {}  # the lines of each text, with the strings
        todo = [(model.removesuffix(".tla"), True)]
        self.extended: set[str] = set()
        while todo:
            name, extends = todo.pop(0)
            path = next((d / f"{name}.tla" for d in (spec_dir, tc.SPECS_DIR / "lib") if (d / f"{name}.tla").exists()),
                        None)
            if path is None or name in self.texts and (not extends or name in self.extended):
                continue
            raw = path.read_text(encoding="utf-8")
            text = tla_trace._module_text(raw)
            extended = [m for names in _EXTENDS.findall(text) for m in re.findall(r"\w+", names)]
            todo += [(m, extends) for m in extended] + [(m, False) for m in _INSTANCE.findall(text)]
            if extends:
                self.extended.add(name)
                for m in _CONSTANT_DECL.finditer(text):
                    self.constants.update(re.findall(r"\w+", re.sub(r"\([^)]*\)", "", m.group(1))))
                for m in _VARIABLE_DECL.finditer(text):
                    self.variables.update(re.findall(r"\w+", m.group(1)))
            if name in self.texts:
                continue
            self.texts[name] = text
            header = tla_trace._HEADER.search(raw)
            self.lines[name] = raw.count("\n", 0, header.start() if header else 0) + 1
            first = self.lines[name] - 1
            self.full[name] = tc.strip_tla_comments(raw).split("\n")[first:first + len(text.splitlines())]
            self.problems += [f"{name}.tla uses the module {i}" for i in [*extended, *_INSTANCE.findall(text)]
                              if i in _SEARCH_MODULES]
            self.problems += [f"{name}.tla uses {u}" for u in sorted(set(_SEARCH_STATE.findall(text)))]

    def places(self, name: str) -> list[tuple[str, re.Match]]:
        """Every definition of the name, anywhere in a line: (module, match up to the body)."""
        if self._index is None:
            self._index = {}
            for module, text in self.texts.items():
                for m in _HEAD.finditer(text):
                    self._index.setdefault(m.group(1), []).append((module, m))
        return self._index.get(name, [])

    def defines(self, name: str) -> bool:
        return name in self.constants or bool(self.places(name))

    def is_constant(self, name: str) -> bool:
        return name in self.constants and name not in self.variables and not self.places(name)

    def is_variable(self, name: str) -> bool:
        return name in self.variables and name not in self.constants and not self.places(name)

    def body(self, name: str) -> tuple[str, int] | None:
        """(module, offset) of the body of the name's only definition, which has no parameters and is in the model
        module or one it extends."""
        places = self.places(name)
        if len(places) != 1 or places[0][0] not in self.extended or places[0][1].group(2):
            return None
        return places[0][0], places[0][1].end()

    def is_constant_set(self, name: str) -> bool:
        place = self.body(name) if re.fullmatch(r"[A-Za-z_]\w*", name) else None
        if place is None:
            return False
        try:
            _ConstantSet(self, self.texts[place[0]], place[1]).read()
        except _Refused:
            return False
        return True

    def bound_problem(self, name: str, seen: frozenset = frozenset()) -> str | None:
        """Why the definition is not just upper bounds on counters, or None."""
        if name in seen:
            return f"{name} refers to itself"
        place = self.body(name)
        if place is None:
            return (f"{name} must be defined once, without parameters, in the model module or one it extends; "
                    f"{_Bound.ALLOWED}")
        for trusted in ("Len", "Cardinality"):
            if self.places(trusted):
                return f"the model defines its own {trusted}"
        if any(_ORDER_DEFINITION.search(text) for text in self.texts.values()):
            return "the model defines its own order operator"
        module, pos = place
        reader = _Bound(self, self.texts[module], pos, seen | {name})
        try:
            reader.read()
        except _Refused as refused:
            if "; a constraint may" in refused.why:  # from a definition it names
                return refused.why
            line = self.lines[module] + self.texts[module].count("\n", 0, refused.at)
            return f"{module}.tla line {line}: {refused.why}; {_Bound.ALLOWED}"
        self.used[name] = reader.used - self.constants
        return None

    def regions(self, name: str) -> list[tuple[str, str]]:
        """(text without strings, whole lines with them) of every definition of the name. A region ends at a line
        that starts another definition or unit, but not inside a LET: it may be too long, but not too short."""
        if name in self._regions:
            return self._regions[name]
        out = self._regions[name] = []
        for module, m in self.places(name):
            text, end = self.texts[module], m.end()
            while True:
                after = _NEXT_UNIT.search(text, end)
                end = after.start() if after else len(text)
                words = re.findall(r"\b(?:LET|IN)\b", text[m.start():end])
                if after is None or words.count("LET") <= words.count("IN"):
                    break
                end += 1
            lines = self.full[module][text.count("\n", 0, m.start()):text.count("\n", 0, end) + (after is None)]
            out.append((text[m.start():end], "\n".join(lines)))
        return out

    def closure(self, names: list[str]) -> list[tuple[str, str]]:
        """The regions of the names and of every definition they may use."""
        out, todo, seen = [], list(names), set()
        while todo:
            name = todo.pop()
            if name not in seen:
                seen.add(name)
                regions = self.regions(name)
                out += regions
                todo += {w for text, _ in regions for w in re.findall(r"[A-Za-z_]\w*", text)}
        return out

    def source(self, name: str) -> list[str]:
        """The definitions of the name as written, without comments, for a reader."""
        return ["\n".join(line.rstrip() for line in full.strip().splitlines() if line.strip())
                for _, full in self.regions(name)]


def limits_of(cfg: Path) -> dict[str, str]:
    """The names a cfg declares with `\\* limit: <Name> -- <reason>` lines."""
    return dict(_LIMIT.findall(cfg.read_text(encoding="utf-8")))


def checked_names(parsed: tc.Cfg) -> set[str]:
    """The names whose override would change what a run checks or where it searches."""
    return {n for key in ("INVARIANT", "PROPERTY", "CONSTRAINT", "ACTION_CONSTRAINT", "INIT", "NEXT",
                          "SPECIFICATION", "POSTCONDITION") for n in parsed.sections.get(key, [])}


def search_problems(model: Model, cfg: Path, declared: dict[str, str] | None = None,
                    ) -> tuple[list[str], list[tuple[str, str, str]]]:
    """(problems, declared limits as (kind, name, reason)) of one cfg. See the module docstring. `declared` takes
    the place of the cfg's own `limit` lines."""
    parsed = tc.parse_cfg(cfg)
    declared = limits_of(cfg) if declared is None else declared
    problems: list[str] = []
    limits: list[tuple[str, str, str]] = []

    def declare(kind: str, name: str) -> None:
        if name in declared:
            limits.append((kind, name, declared[name]))
        else:
            problems.append(f"{kind} {name} has no `\\* limit: {name} -- <reason>` line")

    for name in parsed.sections.get("CONSTRAINT", []):
        declare("CONSTRAINT", name)
        problem = model.bound_problem(name)
        if problem:
            problems.append(f"CONSTRAINT {name}: {problem}")
    problems += [f"ACTION_CONSTRAINT {name} removes steps" for name in parsed.sections.get("ACTION_CONSTRAINT", [])]
    problems += [f"VIEW {name} merges states" for name in parsed.sections.get("VIEW", [])]
    text = tc.strip_tla_comments(cfg.read_text(encoding="utf-8"), keep_strings=False)
    # TLC takes the last value of a name, or the `<-` one, so the text would not say which one ran.
    names = parsed.overrides + re.findall(r"\b(?:INIT|NEXT|SPECIFICATION)\b", text)
    problems += [f"the cfg sets {name} more than once" for name in dict.fromkeys(names) if names.count(name) > 1]
    problems += [f"the cfg uses {used}" for used in sorted(set(_SEARCH_STATE.findall(text)))]
    # A standard operator given to a constant or a definition reaches the model without its module naming it.
    problems += [f"the cfg overrides {name} with {target}, which the model's modules do not define"
                 for name, target in _OVERRIDE.findall(text) if not model.defines(target)]
    checked = checked_names(parsed)
    used = {n for name in parsed.sections.get("CONSTRAINT", []) for n in model.used.get(name, ())}
    problems += [f"the cfg overrides {name}, which a constraint uses, with {target}, which is no constant value"
                 for name, target in _OVERRIDE.findall(text) if name in used
                 and not model.is_constant(target) and not model.is_constant_set(target)]
    for name in dict.fromkeys(parsed.overrides):
        if name in checked:
            problems.append(f"the cfg overrides {name}, which it checks or runs")
        elif name in model.variables or name in ("Len", "Cardinality"):
            problems.append(f"the cfg overrides {name}, which a constraint relies on")
        elif name not in model.constants:
            declare("override", name)
    return problems, limits


_SPECIFICATION = ("INIT", "NEXT", "SPECIFICATION")
_CFG_TOKEN = re.compile(r'"[^"]*"|\w+|<-|[^\s\w]')


def settings(cfg: Path) -> frozenset[str]:
    """What makes a cfg's model: each constant's value, and INIT, NEXT or SPECIFICATION."""
    text = tc.strip_tla_comments(cfg.read_text(encoding="utf-8"))
    tokens = _CFG_TOKEN.findall(text)
    out: list[list[str]] = []
    section = None
    for i, token in enumerate(tokens):
        if token in tc._CFG_KEYWORDS:
            section = token.removesuffix("S")
            if section in _SPECIFICATION:
                out.append([section])
            continue
        if section == "CONSTANT" and re.fullmatch(r"\w+", token) and tokens[i + 1:i + 2] in (["="], ["<-"]):
            out.append([])
        if section in ("CONSTANT", *_SPECIFICATION) and out:
            out[-1].append(token)
    return frozenset(" ".join(entry) for entry in out)


def _split(cfg: Path) -> tuple[frozenset[str], str | None]:
    """(every setting but Mutant's value, Mutant's value when it is a plain one)."""
    entries = settings(cfg)
    value = [e for e in entries if e.split()[0] == "Mutant"]
    plain = re.fullmatch(r'Mutant = ("[^"]*"|\w+)', value[0]) if len(value) == 1 else None
    return entries - set(value), plain.group(1) if plain else None


def check_of(parsed: tc.Cfg, name: str) -> str | None:
    """How the cfg checks this property: a state predicate under PROPERTY is not the INVARIANT."""
    if name in parsed.invariants:
        return "INVARIANT"
    if name in parsed.properties:
        return "PROPERTY"
    return "CHECK_DEADLOCK" if name == "Deadlock" and parsed.check_deadlock else None


def mutant_problems(model: Model, cfg: Path, baselines: list[Path]) -> tuple[list[str], str]:
    """Why this mutant cfg cannot count as caught, whatever TLC says, and its control's value of Mutant."""
    declared = {k: v for baseline in baselines for k, v in limits_of(baseline).items()}
    problems = search_problems(model, cfg, declared)[0]
    parsed = tc.parse_cfg(cfg)
    rest, value = _split(cfg)
    if value is None or not model.is_constant("Mutant"):
        return [*problems, "it does not set the CONSTANT Mutant to a plain value"], ""
    # A mutant of the property alone shows nothing about the design.
    if any(re.search(r"\bMutant\b", text) for text, _ in model.regions(parsed.expect or "")):
        problems.append(f"{parsed.expect} reads Mutant, so the mutant may change the property and not the design")
    spec = {entry for entry in rest if entry.split()[0] in _SPECIFICATION}
    start = [w for entry in spec for w in entry.split()[1:]]
    if not any(re.search(rf"(?<![\w\"]){re.escape(value)}(?![\w\"])", full) for _, full in model.closure(start)):
        problems.append(f"no definition of {' or '.join(start)} names {value}, so the mutant changes no step")
    if not any(spec == {e for e in settings(b) if e.split()[0] in _SPECIFICATION} for b in baselines):
        problems.append(f"no baseline runs its {', '.join(sorted(spec)) or 'specification'}")
    # The design that the mutant changes: the first baseline, the main one if it can, that checks the property.
    check = check_of(parsed, parsed.expect or "")
    checkers = [b for b in baselines if check and check_of(tc.parse_cfg(b), parsed.expect or "") == check]
    values = [_split(baseline)[1] for baseline in checkers]
    if not checkers:
        problems.append(f"no baseline checks {parsed.expect} as it does")
    elif None in values:
        problems.append(f"{checkers[values.index(None)].name} does not set Mutant to a plain value")
    elif value in values:
        problems.append(f"Mutant is {value} in {checkers[values.index(value)].name} too")
    return problems, (values or [None])[0] or ""


def control_text(cfg: Path, value: str) -> str:
    """The cfg without its comments and with this value of Mutant."""
    text = tc.strip_tla_comments(cfg.read_text(encoding="utf-8"))
    tokens = list(_CFG_TOKEN.finditer(text))
    for name, equals, old in zip(tokens, tokens[1:], tokens[2:]):
        if (name.group(), equals.group()) == ("Mutant", "="):
            return text[:old.start()] + value + text[old.end():]
    return text


# --------------------------------------------------------------------------------------------
# The settings, for a reader.

def describe_settings(model: Model, cfg: Path, baselines: list[Path], declared: dict[str, str]) -> dict:
    """What a reviewer has to read in one cfg. `changed` is a mutant's difference to the baselines."""
    parsed, entries = tc.parse_cfg(cfg), sorted(settings(cfg))
    spec = [e for e in entries if e.split()[0] in _SPECIFICATION]
    constants = [e for e in entries if e not in spec]
    out: dict = {
        "cfg": cfg.name, "specification": spec, "constants": constants,
        "constraints": [{"name": name, "declared": declared.get(name, ""),
                         "text": [s for n in (name, *sorted(model.used.get(name, ()))) for s in model.source(n)]}
                        for name in parsed.sections.get("CONSTRAINT", [])],
        "action_constraints": parsed.sections.get("ACTION_CONSTRAINT", []), "view": parsed.sections.get("VIEW", []),
        "symmetry": parsed.sections.get("SYMMETRY", []), "check_deadlock": parsed.check_deadlock,
        "overrides": [e for e in constants if e.split()[0] not in model.constants],
        "invariants": parsed.invariants, "properties": parsed.properties,
        # What the names in the settings stand for: MCInit, MCNext, MCNodes.
        "definitions": {n: model.source(n) for n in dict.fromkeys(
            w for e in entries for w in e.split()[1:] if re.fullmatch(r"[A-Za-z_]\w*", w) and model.places(w))},
    }
    if baselines:
        out["expect"] = parsed.expect or ""
        out["changed"] = {b.name: {"mutant": sorted(settings(cfg) - settings(b)), "baseline": sorted(settings(b) - settings(cfg))}
                          for b in baselines}
    return out


def short_settings(full: dict) -> str:
    """One line of the record. A mutant shows what differs from the baseline that is nearest to it."""
    if "changed" in full:
        name, near = min(full["changed"].items(), key=lambda kv: len(kv[1]["mutant"]) + len(kv[1]["baseline"]))
        return (f"mutant {full['cfg'].removesuffix('.cfg')} of {name}: {', '.join(near['mutant']) or 'nothing set'}"
                + (f" instead of {', '.join(near['baseline'])}" if near["baseline"] else ""))
    parts = [*full["specification"], *full["constants"],
             *(f"CONSTRAINT {c['name']} ({'; '.join(' '.join(s.split()) for s in c['text'])})" for c in full["constraints"]),
             *(f"SYMMETRY {s}" for s in full["symmetry"]), *([] if full["check_deadlock"] else ["CHECK_DEADLOCK FALSE"])]
    return f"{full['cfg']}: {', '.join(parts)}"


def settings_facts(model: Model, baselines: list[Path], mutants: list[Path]) -> dict:
    """The record's settings facts. `settings_full` goes to summary.json only."""
    declared = {k: v for baseline in baselines for k, v in limits_of(baseline).items()}
    full = [describe_settings(model, cfg, [], declared) for cfg in baselines]
    full += [describe_settings(model, cfg, baselines, declared) for cfg in mutants]
    digest = hashlib.sha256(json.dumps(full, sort_keys=True).encode("utf-8")).hexdigest()
    return {"settings": [short_settings(f) for f in full], "settings_sha256": digest, "settings_full": full}


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
        names = parsed.sections.get("CONSTRAINT", [])
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

def run_cfg(spec_dir: Path, model: str, cfg: Path, config: str, out: Path, budget: int, args: list[str],
            simulate: bool = False) -> tuple[tc.TlcResult, dict, str]:
    """One run that has to pass: TLC's result, its facts with the verdict, and why it did not pass. `config` is the
    cfg's path as TLC sees it."""
    stem = cfg.name[: -len(".cfg")]
    res, elapsed = tc.run_tlc(spec_dir, [*args, "-config", config, model], out_dir=out, log_path=out / f"{stem}.log",
                              budget_s=budget)
    fact = {"cfg": cfg.name, "distinct_states": res.distinct, "depth": res.depth, "states_left": res.left,
            "duration_s": round(elapsed, 1)}
    log = (out / f"{stem}.log").read_text(encoding="utf-8", errors="replace")
    # A simulation's success is not an exhaustive one, and the other way round.
    done = (res.simulation_done and not res.complete and not res.timed_out) if simulate \
        else (res.complete and not res.simulation_done)
    liveness = liveness_problem(log, tc.parse_cfg(cfg))
    if res.errors:
        fact["verdict"], why = "error", "; ".join(res.errors)
    elif res.violations:
        fact["verdict"], why = res.violations[0][0], describe(res)
    elif liveness:
        fact["verdict"], why = "liveness-limited", liveness
    elif done and res.exit_code == 0:
        fact["verdict"], why = "pass", ""
    else:
        fact["verdict"], why = "incomplete", tc.incomplete_reason(res, budget)
    return res, fact, why


def run_baseline(rep: Report, spec_dir: Path, model: str, cfg: Path, out: Path, budget: int,
                 workers: str, facts: list[dict], cex_dump: bool = True,
                 extra: list[str] | None = None, label: str | None = None, simulate: bool = False) -> tc.TlcResult:
    stem = cfg.name[: -len(".cfg")]
    label = label or f"baseline {cfg.name}"
    args = ["-workers", workers, "-coverage", "1", *(extra or [])]
    if cex_dump:
        args += ["-dumpTrace", "json", f"/out/{stem}.cex.json"]
    res, fact, why = run_cfg(spec_dir, model, cfg, cfg.relative_to(spec_dir).as_posix(), out, budget, args, simulate)
    if fact["verdict"] == "error":
        rep.error(label, why)
    elif fact["verdict"] == "incomplete":
        rep.incomplete = True
        rep.skip(label, why)
    elif fact["verdict"] == "pass":
        rep.check(True, label, f"{res.distinct if res.distinct is not None else res.generated} "
                               f"{'distinct' if res.distinct is not None else 'generated'} states, depth {res.depth}")
    else:
        rep.check(False, label, why + (f" (log {stem}.log)" if res.violations else ""))
        if res.violations:
            for line in tc.counterexample_text((out / f"{stem}.log").read_text(encoding="utf-8", errors="replace")):
                print(f"    {line}")
    fact["detail"] = why if res.violations or fact["verdict"] == "liveness-limited" else ""
    facts.append(fact)
    return res


class Controls:
    """The runs that show that a mutant's property holds without the mutation, at the mutant's own bounds."""

    def __init__(self, spec_dir: Path, model: str, out: Path, budget: int, workers: str, baselines: list[Path],
                 runs: list[dict]) -> None:
        self.spec_dir, self.model, self.out, self.budget, self.workers = spec_dir, model, out, budget, workers
        self.ran = [(cfg, run, run["detail"] or run["verdict"]) for cfg, run in zip(baselines, runs)]
        self.baselines = len(self.ran)
        self.facts: list[dict] = []

    def of(self, cfg: Path, value: str) -> tuple[dict, str, bool]:
        """The control run's facts, why it did not pass, and whether an earlier run is the control, because it had the
        same settings and checked the property."""
        control = self.out / f"control-{cfg.name}"
        control.write_text(control_text(cfg, value), encoding="utf-8")
        parsed, mutant = tc.parse_cfg(control), tc.parse_cfg(cfg)
        if settings(control) != _split(cfg)[0] | {f"Mutant = {value}"} or parsed.sections != mutant.sections \
                or parsed.check_deadlock != mutant.check_deadlock:
            raise tc.TlaError(f"cannot write the control of {cfg.name}")

        def same(other: tc.Cfg) -> bool:
            return settings(other.path) == settings(control) and check_of(other, mutant.expect) == check_of(parsed, mutant.expect) \
                and all(set(other.sections.get(k, [])) == set(parsed.sections.get(k, [])) for k in ("CONSTRAINT", "SYMMETRY"))

        found = [(i, run, why) for i, (ran, run, why) in enumerate(self.ran) if same(tc.parse_cfg(ran))]
        # Another mutant's control that did not pass may have failed on a property that this cfg does not check.
        found = [f for f in found if f[1]["verdict"] == "pass"] or [f for f in found if f[0] < self.baselines]
        if found:
            control.unlink()
            return found[0][1], found[0][2], True
        stem = control.name[: -len(".cfg")]
        args = ["-workers", self.workers, "-dumpTrace", "json", f"/out/{stem}.cex.json"]
        _, fact, why = run_cfg(self.spec_dir, self.model, control, f"/out/{control.name}", self.out, self.budget, args)
        fact["detail"] = why
        print(f"[tla_check] control {control.name}: {fact['verdict']}, {fact['distinct_states']} distinct states, "
              f"{fact['duration_s']} s" + (f": {why}" if why else ""), flush=True)
        self.ran.append((control, fact, why))
        self.facts.append(fact)
        return fact, why, False


def run_mutant(rep: Report, spec_dir: Path, model: str, cfg: Path, out: Path, budget: int,
               workers: str, modules: Model, baselines: list[Path], controls: Controls) -> dict:
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
    problems, value = mutant_problems(modules, cfg, baselines)
    if problems:
        fact["detail"] = "does not count: " + "; ".join(problems)
        rep.check(False, label, fact["detail"])
        return fact
    control, why, earlier = controls.of(cfg, value)
    fact["control"] = control["cfg"]
    if control["verdict"] != "pass":
        fact["detail"] = f"not caught: its control {control['cfg']} " + {
            "error": "ended in a TLC error", "incomplete": "did not finish", "liveness-limited": "does not count",
        }.get(control["verdict"], "fails without the mutation") + f": {why}"
        if earlier:
            rep.skip(label, fact["detail"])  # a baseline, whose own result is the failure, the error or the not-run
        else:
            rep.check(False, label, fact["detail"])
        return fact
    stem = f"mutant-{name}"
    args = ["-workers", workers, "-dumpTrace", "json", f"/out/{stem}.cex.json",
            "-config", cfg.relative_to(spec_dir).as_posix(), model]
    res, _ = tc.run_tlc(spec_dir, args, out_dir=out, log_path=out / f"{stem}.log", budget_s=budget)
    liveness = liveness_problem((out / f"{stem}.log").read_text(encoding="utf-8", errors="replace"), parsed)
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
        elif liveness:
            fact["detail"] += f", but it does not count: {liveness}"
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
    modules = Model(spec_dir, model)
    limits = search_check(rep, modules, baselines)
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

    controls = Controls(spec_dir, model, out, budget, workers, baselines, runs)
    mutants = [run_mutant(rep, spec_dir, model, m, out, budget, workers, modules, baselines, controls)
               for m in mutant_cfgs]
    covered = [n for n in coverage.values()]
    facts.update({
        "distinct_states": distinct, "depth": depth,
        "actions": len(coverage), "min_action_coverage": min(covered) if covered else None,
        "uncovered": uncovered, "unmutated": sorted(exempt), "runs": runs, "mutants": mutants,
    })
    if controls.facts:
        facts["controls"] = controls.facts
    if limits:
        facts["limits"] = describe_limits(limits, out)
    facts.update(settings_facts(modules, baselines, mutant_cfgs))
    return facts


def large_check(rep: Report, spec_dir: Path, name: str, model: str, out: Path, budget: int, workers: str,
                args: argparse.Namespace) -> dict:
    cfg = spec_dir / f"{name}.large.cfg"
    if not cfg.exists():
        raise tc.TlaError(f"{_rel(cfg)} does not exist")
    modules = Model(spec_dir, model)
    limits = search_check(rep, modules, [cfg])
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
    facts.update(settings_facts(modules, [cfg], []))
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
    full = tla.pop("settings_full", [])
    result = {"verdict": verdict, "executed": rep.executed, "failed": rep.failed, "not_run": not_run, "tla": tla}
    artifacts = []
    if out is not None and out.exists():
        # summary.json keeps every fact. run_check.py trims the record, and keeps this artifact.
        files = [f for f in sorted(out.glob("*")) if f.is_file()]  # before summary.json is written
        (out / "summary.json").write_text(json.dumps({**result, "settings": full}, indent=2) + "\n", encoding="utf-8")
        artifacts = [{"name": f"tla/{f.name}", "sha256": _sha256(f)} for f in (out / "summary.json", *files)]
    tc.print_summary({**result, "artifacts": artifacts})
    return EXIT[verdict]


if __name__ == "__main__":
    sys.exit(main())
