#!/usr/bin/env python3
"""O02: check exact engine dependency graphs, compile guards, tests and identity."""
import argparse
import json
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


def assert_no_test_hooks(name, tree):
    # Cargo resolves transitive feature aliases before printing {f}.
    for line in tree.splitlines():
        package, feature_separator, features = line.partition('|')
        if feature_separator:
            assert 'test-hooks' not in features.split(','), (name, package, 'test-hooks enabled')


def package_features(tree):
    """(package, features) for every line of a `{p}|{f}` tree."""
    for line in tree.splitlines():
        package, feature_separator, features = line.partition('|')
        if feature_separator and package.split():
            # A repeated package is printed once in full and then marked `(*)`.
            features = features.strip().removesuffix('(*)').strip()
            yield package.split()[0], {f for f in features.split(',') if f}


def assert_no_sim(name, tree):
    """No product graph enables the deterministic simulation (phase 7, O02, NS26)."""
    runtime = False
    for package, features in package_features(tree):
        assert not package.startswith('nabedra-sim'), (name, package, 'simulation crate in a product graph')
        if package == 'nabedra-rt':
            runtime = True
            assert 'sim' not in features, (name, package, 'sim enabled')
        if package == 'nabedra-kv':
            assert 'sim' not in features, (name, package, 'sim enabled')
        if package == 'tokio':
            assert 'test-util' not in features, (name, package, 'test-util enabled')
    assert runtime, (name, 'nabedra-rt is not in the graph')


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


def check_compile_guards():
    exclusive_engine = 'select exactly one engine: olap, oltp, or cache'
    lib = ('--lib',)
    invalid = (
        ('no-engine', [], lib, exclusive_engine),
        ('olap-oltp', ['--features', 'olap,oltp'], lib, exclusive_engine),
        ('olap-cache', ['--features', 'olap,cache'], lib, exclusive_engine),
        ('oltp-cache', ['--features', 'oltp,cache'], lib, exclusive_engine),
        ('all-engines', ['--features', 'olap,oltp,cache'], lib, exclusive_engine),
        ('cache-s3', ['--features', 'cache,s3'], lib, 'the cache engine does not support S3'),
        # Phase 7: a simulation build of the library is allowed, but never a server binary.
        ('oltp-sim-bin', ['--features', 'oltp,sim'], ('--bin', 'nabedra-db'),
         'cannot be built with the sim feature'),
    )
    for name, features, target, diagnostic in invalid:
        result = cargo('check', ['--no-default-features', *features], *target, check=False)
        assert result.returncode != 0, f'O02 {name} unexpectedly compiled'
        assert diagnostic in result.stderr, result.stderr
        print(f'O02 {name}: explicit compile guard passed', flush=True)
    return [name for name, *_ in invalid]


def installed_version(selection):
    result = cargo('build', selection, '--bin', 'nabedra-db', '--message-format=json')
    artifacts = (json.loads(line) for line in result.stdout.splitlines())
    executable, = [record['executable'] for record in artifacts
                   if record.get('reason') == 'compiler-artifact' and record.get('executable')
                   and record['target']['name'] == 'nabedra-db']
    return subprocess.check_output([executable, '--version'], text=True, timeout=10).strip()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument('--dependencies-only', action='store_true',
                        help='inspect graphs without claiming builds or tests passed')
    mode.add_argument('--acceptance-evidence', type=Path,
                        help='build installed binaries and record O02 checks without running full engine suites')
    args = parser.parse_args()
    tree = ('--edges', 'normal,build', '--prefix', 'none', '--format', '{p}|{f}')
    events_tree = cargo('tree', [], *tree, manifest=EVENTS_MANIFEST).stdout
    assert_no_test_hooks('events', events_tree)
    assert_no_sim('events', events_tree)
    events = package_names(events_tree)
    assert_no_forbidden_native('events', events)
    assert 'nabedra-kv' in events, ('events', 'native storage engine')
    print('O02 events: dependency graph passed', flush=True)
    # The OLAP image selects these features explicitly, independent of Cargo's defaults.
    image_tree = cargo('tree', ['--no-default-features', '--features', 'olap,s3'], *tree).stdout
    assert_no_test_hooks('olap-s3-image', image_tree)
    assert_no_sim('olap-s3-image', image_tree)
    assert_dependency_packages('olap-s3-image', 'olap', True, image_tree)
    print('O02 olap-s3-image: dependency graph passed', flush=True)
    graphs = {'events': sorted(events)}
    versions = {}
    for name, engine, s3, selection in CASES:
        result = cargo('tree', selection, *tree)
        assert_no_test_hooks(name, result.stdout)
        assert_no_sim(name, result.stdout)
        graphs[name] = assert_dependency_packages(name, engine, s3, result.stdout)
        print(f'O02 {name}: dependency graph passed', flush=True)
        if args.acceptance_evidence:
            versions[name] = installed_version(selection)
            assert versions[name].split()[-1] == engine, (name, versions[name])
            print(f'O02 {name}: installed {versions[name]}', flush=True)
        if args.dependencies_only or args.acceptance_evidence:
            continue
        for command, extra in [('test', ('--lib', '--tests')),
                               ('run', ('--bin', 'nabedra-db', '--', '--version'))]:
            result = cargo(command, selection, *extra)
            if command == 'run':
                assert engine in result.stdout.split(), (name, result.stdout)
            print(result.stdout, end='')
            print(f'O02 {name}: {command} passed', flush=True)
    guards = check_compile_guards() if not args.dependencies_only else []
    if args.acceptance_evidence:
        commit = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()
        args.acceptance_evidence.write_text(json.dumps(dict(
            commit=commit, graphs=graphs, versions=versions, compile_guards=guards,
        )) + '\n')
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
