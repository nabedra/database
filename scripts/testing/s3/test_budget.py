"""budget.py: monthly S3 request totals and the $5 cap."""
import io
import json
from pathlib import Path
import tempfile
import unittest

import budget


class BudgetTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)

    def write(self, month, name, record):
        directory = self.root / month.replace('-', '/')
        directory.mkdir(parents=True, exist_ok=True)
        (directory / name).write_text(json.dumps(record))

    def run_budget(self, month):
        out = io.StringIO()
        return budget.main(['--month', month, '--records', str(self.root)], out=out), out.getvalue()

    def test_sums_top_level_and_metrics_counts_for_the_month_only(self):
        self.write('2026-11', 'a.json', {'s3_requests': 20000})
        self.write('2026-11', 'b.json', {'metrics': {'s3_requests': 5000}})
        self.write('2026-11', 'c.json', {'check': 'no-s3'})
        self.write('2026-10', 'd.json', {'s3_requests': 10 ** 9})
        self.assertEqual(budget.month_total(self.root, '2026-11'), (25000, 3))
        code, text = self.run_budget('2026-11')
        self.assertEqual(code, 0)
        self.assertIn('25000 in 3 records, $0.1250 of $5.00', text)

    def test_over_budget_exits_nonzero(self):
        # $5 is exactly 1,000,000 requests; one more is over.
        self.write('2026-11', 'a.json', {'s3_requests': 1_000_000})
        self.assertEqual(self.run_budget('2026-11')[0], 0)
        self.write('2026-11', 'b.json', {'s3_requests': 1})
        code, text = self.run_budget('2026-11')
        self.assertEqual(code, 1)
        self.assertIn('over budget', text)

    def test_bad_input(self):
        self.write('2026-11', 'a.json', {'s3_requests': -1})
        self.assertEqual(self.run_budget('2026-11')[0], 2)
        self.assertEqual(self.run_budget('2026-13')[0], 2)
        self.assertEqual(budget.requests_in({'s3_requests': True}), 0)

    def test_missing_month_is_zero(self):
        self.assertEqual(self.run_budget('2030-01')[0], 0)


if __name__ == '__main__':
    unittest.main()
