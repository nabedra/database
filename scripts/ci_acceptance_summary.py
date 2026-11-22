#!/usr/bin/env python3
"""Print an OLTP acceptance results.json (scripts/acceptance_oltp.py) as Markdown for a job summary.

Selected scenarios keep their own status: passed, failed or not-run. Unselected and quarantined
scenarios are counted on one line, so nothing that did not execute reads as a pass.
"""
import json
from pathlib import Path
import sys


def main(path):
    report = json.loads(Path(path).read_text())
    summary = report.get('summary', {})
    commit = report.get('source', {}).get('commit', 'unknown')
    print(f'#### OLTP acceptance `{Path(path).parent.name}` at `{commit}`: '
          f'{summary.get("passed", 0)} passed, {summary.get("failed", 0)} failed, {summary.get("not-run", 0)} not-run\n')
    if report.get('error'):
        print(f'Runner error: {report["error"]}\n')
    print('| Scenario | Status | Reason |\n| --- | --- | --- |')
    skipped = []
    for result in report.get('results', []):
        if result.get('reason') == 'not selected':
            skipped.append(result['scenario'])
            continue
        reason = str(result.get('reason', '')).replace('|', '\\|').replace('\n', ' ')
        print(f'| {result["scenario"]} | {result["status"]} | {reason} |')
    if skipped:
        print(f'\nNot selected (including quarantined): {", ".join(skipped)}')
    print()
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv[1]))
