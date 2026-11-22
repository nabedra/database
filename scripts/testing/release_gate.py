#!/usr/bin/env python3
"""Release gate (plan phase 18, local only).

  release_gate.py --sha SHA [--notes FILE] [--dry-run] [--allow-unattested]

Runs `matrix.py gate --scope release` on exactly SHA, with attestation verification unless
--allow-unattested is given, and writes the release-notes section that lists every
`cloud-deferred` and `hardware-deferred` cell. Deferred cells are never counted as green.
--dry-run prints the report and the notes and writes nothing. The exit status is 0 only when the
gate passes.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
import matrix  # noqa: E402


def release_notes(res: matrix.GateResult) -> str:
    out = [f"## Testing evidence for `{res.sha}`", "",
           f"Release gate: {'passed' if res.ok else 'FAILED'}. {len(res.passed)} required cells passed on this commit.", ""]
    if res.failing:
        out += ["Cells that are not green:", ""]
        out += [f"- {c.guarantee} {c.kind}: {c.text() or c.state}" for c in res.failing]
        out.append("")
    out += ["### Deferred checks", ""]
    if res.deferred:
        out += ["These cells are required but deferred. They are not counted as green.", ""]
        out += [f"- {c.guarantee} {c.kind}: {c.reason or c.deferred}" for c in res.deferred]
    else:
        out.append("None.")
    return "\n".join(out) + "\n"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    matrix.add_path_args(parser)
    parser.add_argument("--sha", required=True)
    parser.add_argument("--notes", type=Path, help="write the release-notes section here")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--allow-unattested", action="store_true",
                        help="count unverified records (fixtures and local rehearsal only)")
    args = parser.parse_args(argv)
    try:
        res = matrix.gate(matrix.paths_from(args), args.sha, "release",
                          allow_unattested=args.allow_unattested, verify=not args.allow_unattested)
    except (matrix.MatrixError, matrix.run_check.CheckError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 1
    notes = release_notes(res)
    print(res.report())
    if args.dry_run or not args.notes:
        print("\n" + notes, end="")
    else:
        args.notes.write_text(notes, encoding="utf-8")
        print(f"wrote {args.notes}")
    return 0 if res.ok else 1


if __name__ == "__main__":
    sys.exit(main())
