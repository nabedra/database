#!/usr/bin/env python3.11
"""D01 program driver for the Python ORM (T9).

Runs the shared point-operation program from `oltp_direct_acceptance.py` through the
public Python ORM and prints canonical results, so the harness can compare them with
the TypeScript driver (`oltp_direct_program.mjs`) and across `direct_oltp` on/off.

Protocol on stdout, one JSON object per line: `{"ready": true}` after opening the
table, `{"checkpoint": <section>}` after each section, then `{"results": [...]}`.
The driver reads one stdin line before each section, so the harness can snapshot
`/v1/runtime/stats` around exactly the operations of a section.
"""
import argparse
import json
import math
import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / 'orm/python/src'))
from nabedra import Database, NabedraError


def decode(value):
    if isinstance(value, dict) and set(value) == {'$i'}:
        return int(value['$i'])
    if isinstance(value, dict) and set(value) == {'$f'}:
        return float(value['$f'])
    if isinstance(value, dict):
        return {key: decode(item) for key, item in value.items()}
    if isinstance(value, list):
        return [decode(item) for item in value]
    return value


def canonical(value):
    if isinstance(value, bool) or value is None or isinstance(value, str):
        return value
    if isinstance(value, int):
        return {'$i': str(value)}
    if isinstance(value, float):
        if value == 0 and math.copysign(1.0, value) < 0:
            return {'$f': '-0'}
        if value.is_integer() and abs(value) < 2 ** 53:
            return {'$f': str(int(value))}
        return {'$f': repr(value)}
    if isinstance(value, dict):
        return {key: canonical(item) for key, item in sorted(value.items())}
    if isinstance(value, list):
        return [canonical(item) for item in value]
    raise TypeError(f'unexpected ORM value {type(value).__name__}')


def step(table, operation, args):
    if operation == 'insert':
        return table.insert(args[0])
    if operation == 'insert_many':
        return table.insert_many(args[0])
    if operation == 'get':
        return table.get(args[0])
    if operation == 'update':
        return table.update(args[0], args[1])
    if operation == 'upsert':
        return table.upsert(args[0])
    if operation == 'delete':
        return table.delete(args[0])
    raise ValueError(f'unknown program operation {operation}')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url', required=True)
    parser.add_argument('--program', type=Path, required=True)
    parser.add_argument('--table', required=True)
    parser.add_argument('--database', default='test')
    parser.add_argument('--ca-file')
    args = parser.parse_args()
    program = json.loads(args.program.read_text())
    db = Database(url=args.url, token=os.environ['NABEDRA_TEST_TOKEN'], database=args.database,
                  ca_file=args.ca_file, timeout=30)
    table = db.table(args.table)
    print(json.dumps({'ready': True}), flush=True)
    results = []
    for section in program['sections']:
        sys.stdin.readline()
        for operation, *raw in section['steps']:
            try:
                value = step(table, operation, decode(raw))
                # A row count is `int` in Python and `number` in TypeScript (each ORM's
                # documented type); compare the count itself.
                outcome = {'ok': {'$count': str(value)} if operation == 'insert_many' else canonical(value)}
            except NabedraError as error:
                outcome = {'error': type(error).__name__, 'code': error.code, 'outcome': error.outcome}
            results.append(dict(section=section['name'], operation=operation, **outcome))
        print(json.dumps({'checkpoint': section['name']}), flush=True)
    print(json.dumps({'results': results}), flush=True)


if __name__ == '__main__':
    main()
