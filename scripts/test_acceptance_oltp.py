"""Acceptance reporting must not convert missing assertions into a pass."""
import hashlib
import json
import os
from pathlib import Path
import re
import shlex
import subprocess
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).parent))
import acceptance_oltp as acceptance


class EssentialRunnerTests(unittest.TestCase):
    def test_native_assertions_resolve_across_server_and_resources(self):
        scenario = next(s for s in acceptance.SCENARIOS if s.id == 'O22')
        passed = 'test result: ok. 1 passed; 0 failed; 0 ignored;'
        with tempfile.TemporaryDirectory(prefix='nabedra-native-fixtures-') as directory:
            root = Path(directory)
            server, resources = root / 'server-tests', root / 'resources-tests'
            runtime = acceptance.ROOT / 'server/tests'
            fixture = (
                'import sys\n'
                f'sys.path.insert(0, {str(runtime)!r})\n'
                'from oltp_runtime_acceptance import assertion, run_native_assertions\n'
                "run_native_assertions(sys.argv[1], ['server_case', 'resource_case'])\n"
                f"for clause in {scenario.clauses!r}:\n"
                "    assertion('O22', clause, {'fixture': 'unit test'})\n"
                f'print({passed!r})\n'
            )
            run_fixture = shlex.join([sys.executable, '-c', fixture])
            server.write_text(
                '#!/bin/sh\n'
                'if [ "$1" = --list ]; then\n'
                '    printf "%s\\n" "acceptance_o22: test" "runtime::server_case: test"\n'
                'elif [ "$2" = acceptance_o22 ]; then\n'
                f'    exec {run_fixture} "$0"\n'
                'else\n'
                '    test "$1:$2" = "--exact:runtime::server_case" || exit 1\n'
                f'    printf "%s\\n" "$2" {shlex.quote(passed)}\n'
                'fi\n'
            )
            server.chmod(0o700)

            for selector, extra_listing, expected_error in [
                ('resource_case', '', None),
                ('resource_case', 'scheduler::server_case: test', 'server_case: expected one native test, found 2'),
                ('other', '', 'resource_case: expected one native test, found 0'),
            ]:
                with self.subTest(error=expected_error):
                    selector = f'scheduler::{selector}'
                    listing = [f'{selector}: test', extra_listing]
                    resources.write_text(
                        '#!/bin/sh\n'
                        f'if [ "$1" = --list ]; then printf "%s\\n" {shlex.join(listing)}; exit; fi\n'
                        f'test "$1:$2" = "--exact:{selector}" || exit 1\n'
                        f'printf "%s\\n" "$2" {shlex.quote(passed)}\n'
                    )
                    resources.chmod(0o700)
                    output = root / 'report'
                    result = subprocess.run(
                        [sys.executable, acceptance.__file__, '--scenario', 'O22',
                         '--test-binary', str(server), '--target-dir', str(root / 'target'),
                         '--output', str(output)],
                        env=os.environ | {'NABEDRA_RESOURCES_TEST_EXE': str(resources)},
                        capture_output=True, text=True, timeout=30,
                    )
                    log = (output / 'O22.log').read_text()
                    self.assertEqual(result.returncode, int(expected_error is not None), log)
                    if expected_error:
                        self.assertIn(expected_error, log)
                    else:
                        self.assertIn('runtime::server_case\n', log)
                        self.assertIn('scheduler::resource_case\n', log)
                        self.assertIn('O22 passed: executed 4/4 clauses', result.stdout)
                    report = json.loads((output / 'results.json').read_text())
                    self.assertEqual(report['resources_test_binary_sha256'],
                                     hashlib.sha256(resources.read_bytes()).hexdigest())

    def test_supporting_component_does_not_pass_a_missing_production_scenario(self):
        with tempfile.TemporaryDirectory(prefix='nabedra-runner-cli-') as directory:
            root = Path(directory)
            binary = root / 'component-tests'
            binary.write_text('#!/bin/sh\necho "supporting_component: test"\n')
            binary.chmod(0o700)
            result = subprocess.run([sys.executable, str(Path(acceptance.__file__)), '--scenario', 'O01',
                                     '--test-binary', str(binary), '--target-dir', str(root / 'target'),
                                     '--output', str(root / 'report')], capture_output=True, text=True, timeout=30)
            self.assertEqual(result.returncode, 2, result.stderr)
            report = json.loads((root / 'report/results.json').read_text())
            self.assertEqual(len(report['results']), 29)
            self.assertIn("O01 not-run: executed 0/6 clauses", result.stdout)
            self.assertEqual(report['summary'], {'passed': 0, 'failed': 0, 'not-run': 1})
            self.assertEqual(next(r for r in report['results'] if r['scenario'] == 'O01')['status'], 'not-run')

    def test_rubric_preserves_all_scenarios_scheduler_ids_and_regressions(self):
        self.assertEqual(sorted(s.number for s in acceptance.SCENARIOS), list(range(1, 30)))
        self.assertEqual(len(acceptance.CATEGORIES), 7)
        for scenario in acceptance.SCENARIOS:
            self.assertTrue(scenario.clauses)
            self.assertEqual(len(scenario.clauses), len(set(scenario.clauses)))
        contract = (acceptance.ROOT / 'docs/testing/scheduler-contract.md').read_text()
        required = set(re.findall(r'^\| ([CMQFRTUAB]\d\d) \|', contract, re.MULTILINE))
        mapped = set(' '.join(acceptance.SCHEDULER.values()).split())
        self.assertLessEqual(required, mapped)
        self.assertTrue(all(ids and set(ids) <= set(range(1, 30)) for ids in acceptance.REGRESSIONS.values()))

    def test_actual_results_ignored_tests_and_missing_prerequisites_remain_distinct(self):
        passed = 'test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out;\n'
        marker = acceptance.MARKER + json.dumps(dict(scenario='O01', status='not-run', reason='provider unavailable')) + '\n'
        scenario = next(s for s in acceptance.SCENARIOS if s.id == 'O01')

        def assertion(clause, status='passed', **fields):
            return acceptance.ASSERTION_MARKER + json.dumps(dict(
                scenario='O01', assertion=clause, status=status,
                detail={'fixture': 'unit test'},
            ) | fields) + '\n'

        first, last = scenario.clauses[0], scenario.clauses[-1]
        complete = ''.join(map(assertion, scenario.clauses))
        incomplete = ''.join(map(assertion, scenario.clauses[:-1]))
        cases = [
            (complete + passed, 0, 'passed', 6, []),
            (passed, 0, 'not-run', 0, scenario.clauses),
            (incomplete + passed, 0, 'not-run', 5, [last]),
            (incomplete + assertion(last, 'not-run') + passed, 0, 'not-run', 5, [last]),
            (incomplete + assertion(last, 'failed') + passed, 0, 'failed', 6, []),
            ('test result: ok. 0 passed; 0 failed; 1 ignored;\n', 0, 'not-run', 0, scenario.clauses),
            (complete + passed + marker, 0, 'not-run', 6, []),
            (marker, 1, 'failed', 0, scenario.clauses),
            (passed + acceptance.MARKER + '{}\n', 0, 'failed', 0, scenario.clauses),
            (passed + assertion(first, scenario='O02'), 0, 'failed', 0, scenario.clauses),
            (passed + assertion('invented'), 0, 'failed', 0, scenario.clauses),
            (passed + complete + assertion(first), 0, 'failed', 6, []),
        ]
        with tempfile.TemporaryDirectory(prefix='nabedra-runner-test-') as directory:
            root = Path(directory)
            binary = root / 'fake-test-executable'
            for output, exit_code, expected, executed, missing in cases:
                with self.subTest(expected=expected, exit_code=exit_code, output=output):
                    binary.write_text(f'#!/bin/sh\nprintf %s {shlex.quote(output)}\nexit {exit_code}\n')
                    binary.chmod(0o700)
                    result = acceptance.run_scenario(binary, 'acceptance_o01', scenario, {}, root / 'case.log', 5)
                    self.assertEqual(result['status'], expected, result['reason'])
                    self.assertEqual(result['executed_clauses'], executed)
                    self.assertEqual(result['missing_clauses'], missing)
                    self.assertTrue(all(clause in result['reason'] for clause in missing))
                    self.assertTrue(all(report['detail'] == {'fixture': 'unit test'}
                                        for report in result['assertions']))


if __name__ == '__main__':
    unittest.main()
