"""Acceptance reporting must not convert missing assertions into a pass."""
import json
from pathlib import Path
import re
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).parent))
import acceptance_oltp as acceptance


class EssentialRunnerTests(unittest.TestCase):
    def test_rubric_preserves_all_scenarios_scheduler_ids_and_regressions(self):
        self.assertEqual(sorted(s.number for s in acceptance.SCENARIOS), list(range(1, 30)))
        self.assertEqual(len(acceptance.CATEGORIES), 7)
        contract = (acceptance.ROOT / 'docs/testing/scheduler-contract.md').read_text()
        required = set(re.findall(r'^\| ([CMQFRTUAB]\d\d) \|', contract, re.MULTILINE))
        mapped = set(' '.join(acceptance.SCHEDULER.values()).split())
        self.assertLessEqual(required, mapped)
        self.assertTrue(all(ids and set(ids) <= set(range(1, 30)) for ids in acceptance.REGRESSIONS.values()))

    def test_actual_results_ignored_tests_and_missing_prerequisites_remain_distinct(self):
        passed = 'test result: ok. 1 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out;\n'
        marker = acceptance.MARKER + json.dumps(dict(scenario='O01', status='not-run', reason='provider unavailable')) + '\n'
        cases = [
            (passed, 0, 'passed'),
            ('test result: ok. 0 passed; 0 failed; 1 ignored;\n', 0, 'not-run'),
            (passed + marker, 0, 'not-run'),
            (marker, 1, 'failed'),
            (passed + acceptance.MARKER + '{}\n', 0, 'failed'),
            ('', 0, 'not-run'),
        ]
        scenario = next(s for s in acceptance.SCENARIOS if s.id == 'O01')
        with tempfile.TemporaryDirectory(prefix='nabedra-runner-test-') as directory:
            root = Path(directory)
            binary = root / 'fake-test-executable'
            for output, exit_code, expected in cases:
                with self.subTest(expected=expected, exit_code=exit_code, output=output):
                    binary.write_text(f'#!{sys.executable}\nimport sys\nprint({output!r}, end="")\nsys.exit({exit_code})\n')
                    binary.chmod(0o700)
                    result = acceptance.run_scenario(binary, 'acceptance_o01', scenario, {}, root / 'case.log', 5)
                    self.assertEqual(result['status'], expected)


if __name__ == '__main__':
    unittest.main()
