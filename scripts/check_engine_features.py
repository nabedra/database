#!/usr/bin/env python3
"""O02: check exact engine dependency graphs, compile guards, tests and identity."""
import argparse
from pathlib import Path
import subprocess
import sys

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / 'server' / 'Cargo.toml'
EVENTS_MANIFEST = ROOT / 'server' / 'events' / 'Cargo.toml'
# NS26: storage is the in-tree nabedra-kv engine. No build links RocksDB or the C
# toolchain crates its binding pulled in. BLD5: `ring` is the only TLS provider, so
# no build links aws-lc or needs the cmake build tool.
FORBIDDEN_NATIVE = frozenset({'rocksdb', 'librocksdb-sys', 'bindgen', 'clang-sys',
                              'libz-sys', 'bzip2-sys', 'aws-lc-rs', 'aws-lc-sys',
                              'aws-lc-fips-sys', 'cmake'})
CASES = (
    ('default', 'olap', True, []),
    ('olap', 'olap', False, ['--no-default-features', '--features', 'olap']),
    ('oltp', 'oltp', False, ['--no-default-features', '--features', 'oltp']),
    ('oltp-s3', 'oltp', True, ['--no-default-features', '--features', 'oltp-backup-s3']),
    ('cache', 'cache', False, ['--no-default-features', '--features', 'cache']),
)


def cargo(command, selection, *extra, check=True, manifest=MANIFEST):
    result = subprocess.run(['cargo', command, '--locked', '--manifest-path', str(manifest),
                             *selection, *extra], cwd=ROOT, capture_output=True, text=True,
                            timeout=2400)
    if check and result.returncode:
        print(result.stdout, end='')
        print(result.stderr, end='', file=sys.stderr)
        result.check_returncode()
    return result


def package_names(tree):
    return {line.split()[0] for line in tree.splitlines() if line.strip()}


def assert_no_forbidden_native(name, packages):
    native = FORBIDDEN_NATIVE & packages
    assert not native, (name, 'native dependencies', sorted(native))


def assert_dependency_packages(name, engine, s3, tree):
    packages = package_names(tree)
    assert_no_forbidden_native(name, packages)
    aws = {p for p in packages if p.startswith('aws-')}
    olap = {p for p in packages if p.startswith('arrow-') or p == 'parquet'}
    assert bool(aws) == s3, (name, 'AWS dependencies', sorted(aws))
    assert bool(olap) == (engine == 'olap'), (name, 'OLAP dependencies', sorted(olap))
    assert ('openraft' in packages) == (engine in ('oltp', 'cache')), (name, 'consensus dependency')
    if engine == 'oltp':
        assert 'nabedra-kv' in packages, (name, 'native storage engine')
    if engine == 'olap':
        assert {'arrow-array', 'arrow-schema', 'parquet'} <= packages, (name, 'required OLAP codecs')
    if s3:
        assert {'aws-config', 'aws-sdk-s3'} <= packages, (name, 'required S3 transport')
    return sorted(packages)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--dependencies-only', action='store_true',
                        help='inspect graphs without claiming builds or tests passed')
    args = parser.parse_args()
    tree = ('--edges', 'normal,build', '--prefix', 'none', '--format', '{p}')
    events = package_names(cargo('tree', [], *tree, manifest=EVENTS_MANIFEST).stdout)
    assert_no_forbidden_native('events', events)
    assert 'nabedra-kv' in events, ('events', 'native storage engine')
    print('O02 events: dependency graph passed', flush=True)
    for name, engine, s3, selection in CASES:
        result = cargo('tree', selection, *tree)
        assert_dependency_packages(name, engine, s3, result.stdout)
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
        exclusive_engine = 'select exactly one engine: olap, oltp, or cache'
        invalid = (
            ('no-engine', [], exclusive_engine),
            ('olap-oltp', ['--features', 'olap,oltp'], exclusive_engine),
            ('olap-cache', ['--features', 'olap,cache'], exclusive_engine),
            ('oltp-cache', ['--features', 'oltp,cache'], exclusive_engine),
            ('all-engines', ['--features', 'olap,oltp,cache'], exclusive_engine),
            ('cache-s3', ['--features', 'cache,s3'], 'the cache engine does not support S3'),
        )
        for name, features, diagnostic in invalid:
            result = cargo('check', ['--no-default-features', *features], '--lib', check=False)
            assert result.returncode != 0, f'O02 {name} unexpectedly compiled'
            assert diagnostic in result.stderr, result.stderr
            print(f'O02 {name}: explicit compile guard passed', flush=True)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
