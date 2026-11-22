#!/usr/bin/env python3
"""Read the CI quarantine list (.github/quarantine.tsv) for the workflow and for agents.

Commands:
  check          validate the file; warn about unassigned or expired rows (exit 1 only if malformed)
  skips          libtest arguments that keep quarantined tests out of a blocking suite
  tests          quarantined test paths, one per line
  scenarios      quarantined acceptance scenario IDs, one per line
  scenario-args  `--scenario` arguments for every OLTP acceptance scenario that is not quarantined
  report         a Markdown table for a job summary
"""
import argparse
import datetime
from pathlib import Path
import re
import sys

ROOT = Path(__file__).resolve().parents[1]
DEFAULT = ROOT / '.github' / 'quarantine.tsv'
COLUMNS = ('id', 'kind', 'owner', 'added', 'expires', 'exit_criteria')
SCENARIOS = tuple(f'O{n:02d}' for n in range(1, 30))
def iso(text):
    return datetime.datetime.strptime(text, '%Y-%m-%d').date()


TEST_PATH = re.compile(r'^[a-z_][a-z0-9_]*(::[a-z_][a-z0-9_]*)+$')


def load(path):
    """Return (rows, errors). Lines starting with # are comments; the first other line is the header."""
    rows, errors, header = [], [], None
    for number, line in enumerate(Path(path).read_text().splitlines(), 1):
        if not line.strip() or line.startswith('#'):
            continue
        fields = line.split('\t')
        if header is None:
            header = tuple(fields)
            if header != COLUMNS:
                errors.append(f'line {number}: header must be {"<TAB>".join(COLUMNS)}')
            continue
        if len(fields) != len(COLUMNS) or not all(f.strip() for f in fields):
            errors.append(f'line {number}: expected {len(COLUMNS)} non-empty tab-separated fields')
            continue
        row = dict(zip(COLUMNS, (f.strip() for f in fields)), line=number)
        if row['kind'] == 'test' and not TEST_PATH.match(row['id']):
            errors.append(f'line {number}: a test id must be the full module path (libtest --skip matches substrings)')
        elif row['kind'] == 'scenario' and row['id'] not in SCENARIOS:
            errors.append(f'line {number}: unknown scenario {row["id"]}')
        elif row['kind'] not in ('test', 'scenario'):
            errors.append(f'line {number}: kind must be test or scenario')
        try:
            if iso(row['expires']) < iso(row['added']):
                errors.append(f'line {number}: expires is before added')
        except ValueError:
            errors.append(f'line {number}: added and expires must be ISO dates')
        if any(r['id'] == row['id'] for r in rows):
            errors.append(f'line {number}: duplicate id {row["id"]}')
        rows.append(row)
    if header is None:
        errors.append('missing header')
    return rows, errors


def warnings(rows, today):
    out = []
    for row in rows:
        if row['owner'] == 'unassigned':
            out.append(f'{row["id"]}: no owner; the orchestrator must assign one')
        try:
            if iso(row['expires']) < today:
                out.append(f'{row["id"]}: expired {row["expires"]}; fix it, extend it with a reason, or remove the test')
        except ValueError:
            pass
    return out


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('command', choices=('check', 'skips', 'tests', 'scenarios', 'scenario-args', 'report'))
    parser.add_argument('--file', type=Path, default=DEFAULT)
    parser.add_argument('--today', type=iso, default=datetime.date.today())
    args = parser.parse_args(argv)
    rows, errors = load(args.file)
    if errors:
        for error in errors:
            print(f'{args.file}: {error}', file=sys.stderr)
        return 1
    tests = [r['id'] for r in rows if r['kind'] == 'test']
    scenarios = [r['id'] for r in rows if r['kind'] == 'scenario']
    if args.command == 'check':
        for warning in warnings(rows, args.today):
            print(f'warning: {warning}')
        print(f'{len(tests)} quarantined tests, {len(scenarios)} quarantined scenarios')
    elif args.command == 'skips':
        print(' '.join(f'--skip {t}' for t in tests))
    elif args.command == 'tests':
        print('\n'.join(tests))
    elif args.command == 'scenarios':
        print('\n'.join(scenarios))
    elif args.command == 'scenario-args':
        print(' '.join(f'--scenario {s}' for s in SCENARIOS if s not in scenarios))
    else:
        print('| Quarantined | Kind | Owner | Expires | Exit criteria |\n| --- | --- | --- | --- | --- |')
        for r in rows:
            print(f'| `{r["id"]}` | {r["kind"]} | {r["owner"]} | {r["expires"]} | {r["exit_criteria"]} |')
        for warning in warnings(rows, args.today):
            print(f'\n> Warning: {warning}')
    return 0


if __name__ == '__main__':
    sys.exit(main())
