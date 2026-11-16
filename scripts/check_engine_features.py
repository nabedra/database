#!/usr/bin/env python3
"""O02: check exact engine dependency graphs, compile guards, tests and identity."""
import argparse
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / 'server' / 'Cargo.toml'
CASES = (
    ('default', 'olap', True, []),
    ('olap', 'olap', False, ['--no-default-features', '--features', 'olap']),
    ('oltp', 'oltp', False, ['--no-default-features', '--features', 'oltp']),
    ('oltp-s3', 'oltp', True, ['--no-default-features', '--features', 'oltp,s3']),
)


def cargo(command, selection, *extra, check=True):
    result = subprocess.run(['cargo', command, '--locked', '--manifest-path', str(MANIFEST),
                             *selection, *extra], cwd=ROOT, capture_output=True, text=True,
                            timeout=2400)
    if check and result.returncode:
        print(result.stdout, end='')
        print(result.stderr, end='', file=sys.stderr)
        result.check_returncode()
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dependencies-only', action='store_true',
                        help='inspect graphs without claiming builds or tests passed')
    args = parser.parse_args()
    for name, engine, s3, selection in CASES:
        result = cargo('tree', selection, '--edges', 'normal,build', '--prefix', 'none',
                       '--format', '{p}')
        packages = {line.split()[0] for line in result.stdout.splitlines() if line.strip()}
        aws = {p for p in packages if p.startswith('aws-') and not p.startswith('aws-lc-')}
        olap = {p for p in packages if p.startswith('arrow-') or p == 'parquet'}
        native = {'rocksdb', 'librocksdb-sys'} & packages
        assert bool(aws) == s3, (name, 'AWS dependencies', sorted(aws))
        assert bool(olap) == (engine == 'olap'), (name, 'OLAP dependencies', sorted(olap))
        assert native == ({'rocksdb', 'librocksdb-sys'} if engine == 'oltp' else set()), (name, 'native dependencies', sorted(native))
        assert ('openraft' in packages) == (engine == 'oltp'), (name, 'consensus dependency')
        if engine == 'olap':
            assert {'arrow-array', 'arrow-schema', 'parquet'} <= packages, (name, 'required OLAP codecs')
        if s3:
            assert {'aws-config', 'aws-sdk-s3'} <= packages, (name, 'required S3 transport')
        print(f'O02 {name}: dependency graph passed', flush=True)
        if args.dependencies_only:
            continue
        for command, extra in [('test', ('--lib', '--tests')),
                               ('run', ('--bin', 'nabedra-db', '--', '--version'))]:
            result = cargo(command, selection, *extra)
            if command == 'run':
                assert engine in result.stdout.split(), (name, result.stdout)
            print(result.stdout, end='')
            print(f'O02 {name}: {command} passed', flush=True)
    if not args.dependencies_only:
        for name, features in [('neither', []), ('both', ['--features', 'olap,oltp'])]:
            result = cargo('check', ['--no-default-features', *features], '--lib', check=False)
            assert result.returncode != 0, f'O02 {name} unexpectedly compiled'
            assert 'select exactly one engine: olap or oltp' in result.stderr, result.stderr
            print(f'O02 {name}: explicit compile guard passed', flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
