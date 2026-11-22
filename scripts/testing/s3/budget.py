#!/usr/bin/env python3
"""Sum one month's S3 test requests from evidence records and enforce the budget.

Records live at docs/testing/evidence/records/<yyyy>/<mm>/*.json. A record's S3
request count is its integer `s3_requests` field, either at the top level or
under `metrics`. Records without one count as zero. The cost model is
$0.005 per 1,000 requests, and the monthly cap is $5 (half the approved $10).
Exit 0 within budget, 1 over budget, 2 on unreadable input.
"""
import argparse
import datetime
import json
from pathlib import Path
import sys

PRICE_PER_1000 = 0.005
LIMIT_USD = 5.0
ROOT = Path(__file__).resolve().parents[3] / 'docs' / 'testing' / 'evidence' / 'records'


def requests_in(record):
    for holder in (record, record.get('metrics') if isinstance(record.get('metrics'), dict) else {}):
        value = holder.get('s3_requests')
        if isinstance(value, int) and not isinstance(value, bool):
            if value < 0:
                raise ValueError('negative s3_requests')
            return value
    return 0


def month_total(root, month):
    year, mm = month.split('-')
    total, records = 0, 0
    directory = Path(root) / year / mm
    for path in sorted(directory.glob('*.json')) if directory.is_dir() else []:
        with path.open(encoding='utf-8') as source:
            record = json.load(source)
        if not isinstance(record, dict):
            raise ValueError(f'{path.name}: not an object')
        total += requests_in(record)
        records += 1
    return total, records


def cost(requests):
    return requests * PRICE_PER_1000 / 1000


def main(argv=None, out=sys.stdout):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--month', default=datetime.date.today().strftime('%Y-%m'), help='YYYY-MM')
    parser.add_argument('--records', default=str(ROOT))
    parser.add_argument('--limit-usd', type=float, default=LIMIT_USD)
    args = parser.parse_args(argv)
    try:
        datetime.datetime.strptime(args.month, '%Y-%m')
        total, records = month_total(args.records, args.month)
    except (ValueError, OSError) as error:
        print(f'budget: unreadable input: {error}', file=sys.stderr)
        return 2
    usd = cost(total)
    state = 'over' if usd > args.limit_usd else 'within'
    print(f'S3 test requests {args.month}: {total} in {records} records, '
          f'${usd:.4f} of ${args.limit_usd:.2f} ({state} budget)', file=out)
    return 1 if usd > args.limit_usd else 0


if __name__ == '__main__':
    sys.exit(main())
