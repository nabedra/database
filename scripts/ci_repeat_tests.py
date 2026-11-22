#!/usr/bin/env python3
"""Run exact OLTP library tests repeatedly on a built test executable and print a Markdown tally.

A run counts as passed only when libtest reports exactly one passed test. A name that matches no
test is not-run, never passed. Used by the workflow's `tests` and `quarantine` jobs; works locally
with NABEDRA_TEST_EXE set to an already built `nabedra_database_server-*` test executable.
"""
import argparse
import os
from pathlib import Path
import re
import subprocess
import sys

PASSED = re.compile(r'test result: ok\. 1 passed; 0 failed;')


def run_once(executable, name, mode, timeout):
    env = dict(os.environ, NABEDRA_TEST_DIRECT_OLTP=mode)
    try:
        result = subprocess.run([executable, '--exact', name, '--include-ignored', '--test-threads', '1'],
                                env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True, timeout=timeout)
    except subprocess.TimeoutExpired as error:
        return 'failed', f'timed out after {timeout} s\n{error.stdout or ""}'
    output = result.stdout + result.stderr
    if result.returncode:
        return 'failed', output
    return ('passed' if PASSED.search(output) else 'not-run'), output


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('names', nargs='+', help='full test paths, e.g. oltp::pool::tests::c07_m02_...')
    parser.add_argument('--mode', choices=('on', 'off'), default='on')
    parser.add_argument('--repeat', type=int, default=1)
    parser.add_argument('--timeout', type=float, default=1200)
    parser.add_argument('--log', type=Path, required=True)
    parser.add_argument('--report-only', action='store_true', help='always exit 0 (quarantine job)')
    parser.add_argument('--executable', default=os.environ.get('NABEDRA_TEST_EXE'))
    args = parser.parse_args(argv)
    if not args.executable:
        parser.error('set NABEDRA_TEST_EXE or pass --executable')
    tally = {name: {'passed': 0, 'failed': 0, 'not-run': 0} for name in args.names}
    with args.log.open('a') as log:
        for name in args.names:
            for attempt in range(1, args.repeat + 1):
                status, output = run_once(args.executable, name, args.mode, args.timeout)
                tally[name][status] += 1
                log.write(f'===== {name} direct_oltp={args.mode} run {attempt}/{args.repeat}: {status}\n{output}\n')
                print(f'{name} run {attempt}: {status}', file=sys.stderr, flush=True)
    print(f'#### Library tests, direct_oltp {args.mode}, {args.repeat} runs each\n')
    print('| Test | Passed | Failed | Not run |\n| --- | --- | --- | --- |')
    for name, counts in tally.items():
        print(f'| `{name}` | {counts["passed"]} | {counts["failed"]} | {counts["not-run"]} |')
    print()
    bad = any(c['failed'] or c['not-run'] for c in tally.values())
    return 0 if args.report_only or not bad else 1


if __name__ == '__main__':
    sys.exit(main())
