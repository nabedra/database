"""The quarantine list must stay machine-readable, owned and visible."""
import datetime
from pathlib import Path
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).parent))
import ci_quarantine as quarantine

HEADER = '\t'.join(quarantine.COLUMNS)


def write(text):
    handle = tempfile.NamedTemporaryFile('w', suffix='.tsv', delete=False)
    handle.write(text)
    handle.close()
    return handle.name


class QuarantineTests(unittest.TestCase):
    def test_repository_list_is_valid(self):
        rows, errors = quarantine.load(quarantine.DEFAULT)
        self.assertEqual(errors, [])
        self.assertTrue(all(r['kind'] in ('test', 'scenario') for r in rows))

    def test_rows_need_full_paths_known_scenarios_and_all_fields(self):
        path = write(f'# comment\n{HEADER}\n'
                     'c07_f03\ttest\tlead\t2026-11-22\t2026-12-06\tfix\n'
                     'O30\tscenario\tlead\t2026-11-22\t2026-12-06\tfix\n'
                     'a::b\ttest\t\t2026-11-22\t2026-12-06\tfix\n'
                     'a::c\ttest\tlead\t2026-11-22\t2026-11-01\tfix\n')
        _, errors = quarantine.load(path)
        self.assertEqual(len(errors), 4, errors)

    def test_skips_scenarios_and_warnings(self):
        path = write(f'{HEADER}\n'
                     'mod::tests::a_test\ttest\tunassigned\t2026-11-01\t2026-11-10\tfix\n'
                     'O28\tscenario\tTOPO\t2026-11-22\t2026-12-06\tfix\n')
        rows, errors = quarantine.load(path)
        self.assertEqual(errors, [])
        warnings = quarantine.warnings(rows, datetime.date(2026, 11, 22))
        self.assertEqual(len(warnings), 2)
        self.assertEqual(quarantine.main(['skips', '--file', path]), 0)
        self.assertEqual(quarantine.main(['check', '--file', path]), 0)
        args = [a for a in quarantine.SCENARIOS if a != 'O28']
        self.assertEqual(len(args), 28)

    def test_malformed_file_fails(self):
        path = write('id\tkind\n')
        self.assertEqual(quarantine.main(['check', '--file', path]), 1)


class RepeatTests(unittest.TestCase):
    def test_only_one_reported_pass_counts_as_passed(self):
        import ci_repeat_tests as repeat
        with tempfile.TemporaryDirectory(prefix='nabedra-repeat-') as directory:
            root = Path(directory)
            exe = root / 'tests'
            exe.write_text(f'#!{sys.executable}\nimport sys\nname = sys.argv[2]\n'
                           'if name == "a::ok": print("test result: ok. 1 passed; 0 failed; 0 ignored;")\n'
                           'elif name == "a::none": print("test result: ok. 0 passed; 0 failed; 0 ignored;")\n'
                           'else: sys.exit(101)\n')
            exe.chmod(0o700)
            log = root / 'log'
            self.assertEqual(repeat.main(['a::ok', '--repeat', '2', '--log', str(log), '--executable', str(exe)]), 0)
            self.assertEqual(repeat.main(['a::none', '--log', str(log), '--executable', str(exe)]), 1)
            self.assertEqual(repeat.main(['a::bad', '--log', str(log), '--executable', str(exe)]), 1)
            self.assertEqual(repeat.main(['a::bad', '--log', str(log), '--executable', str(exe), '--report-only']), 0)
            self.assertIn('a::none direct_oltp=on run 1/1: not-run', log.read_text())

    def test_names_route_to_the_executable_that_lists_them(self):
        import ci_repeat_tests as repeat
        with tempfile.TemporaryDirectory(prefix='nabedra-repeat-') as directory:
            root = Path(directory)
            executables = []
            for crate, test in (('server', 'runtime::ok'), ('storage', 'oltp::ok')):
                exe = root / crate
                exe.write_text(f'#!{sys.executable}\nimport sys\n'
                               f'if sys.argv[1] == "--list": print("{test}: test"); sys.exit(0)\n'
                               f'if sys.argv[2] == "{test}": print("test result: ok. 1 passed; 0 failed; 0 ignored;")\n'
                               'else: print("test result: ok. 0 passed; 0 failed; 0 ignored;")\n')
                exe.chmod(0o700)
                executables += ['--executable', str(exe)]
            log = root / 'log'
            self.assertEqual(repeat.main(['runtime::ok', 'oltp::ok', '--log', str(log), *executables]), 0)
            self.assertEqual(repeat.main(['transport::missing', '--log', str(log), *executables]), 1)
            text = log.read_text()
            self.assertIn('oltp::ok direct_oltp=on run 1/1: passed', text)
            self.assertIn('transport::missing direct_oltp=on run 1/1: not-run', text)
            self.assertIn('no test executable lists transport::missing', text)


if __name__ == '__main__':
    unittest.main()
