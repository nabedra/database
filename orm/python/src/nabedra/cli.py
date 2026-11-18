"""Local installation, cluster supervision and administration commands."""
from __future__ import annotations

import argparse
import copy
import json
import os
from pathlib import Path
import secrets
import signal
import sys
import tempfile
from uuid import UUID

from ._binary import (BINARY_FAILURE, CLI_VERSION, CONFIG_VERSION, PROTOCOL_VERSION,
                      binary_version, resolve_server_binary)
from ._cli_config import (ConfigError, example, fields, load, protected_write,
                          read_file, read_json, s3_storage, url, validate)
from .errors import ValidationError
from .validation import validate_identifier


class _ArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        super().error('missing or unsupported command arguments; see --help')


def _count(value):
    try:
        number = int(value)
        if 0 <= number <= 1024:
            return number
    except ValueError:
        pass
    raise argparse.ArgumentTypeError('count must be in 0..1024')


def _threads(value):
    count = _count(value)
    if not 1 <= count <= 256:
        raise argparse.ArgumentTypeError('threads must be in 1..256')
    return count


def _cluster_name(value):
    try:
        return validate_identifier(value)
    except ValidationError as error:
        raise argparse.ArgumentTypeError(str(error)) from None


def _parser():
    parser = _ArgumentParser(prog='nabedra', allow_abbrev=False,
                             description='Manage Nabedra Database with nabedra database <command>.')
    parser.add_argument('--version', action='store_true')
    parser.add_argument('--json', action='store_true')
    groups = parser.add_subparsers(dest='group')
    database = groups.add_parser('database', allow_abbrev=False,
                                 description='Install, launch and administer a database.')
    database.add_argument('--version', action='store_true', default=argparse.SUPPRESS)
    database.add_argument('--json', action='store_true', default=argparse.SUPPRESS)
    commands = database.add_subparsers(dest='command')
    worker = commands.add_parser('worker', allow_abbrev=False)
    worker.add_argument('--bootstrap', required=True)
    init = commands.add_parser('init', allow_abbrev=False)
    init.add_argument('--mode', choices=('fixed', 'serverless'))
    init.add_argument('--storage')
    init.add_argument('--control-storage')
    init.add_argument('--region')
    init.add_argument('--data-token-file')
    init.add_argument('--admin-token-file')
    init.add_argument('--enrollment-token-file')
    init.add_argument('--auth-file', help='existing protected admin/data/enrollment JSON')
    init.add_argument('--output', default='nabedra-database.json')
    init.add_argument('--non-interactive', action='store_true')
    init.add_argument('--threads-per-worker', type=_threads, default=4)
    serve = commands.add_parser('serve', allow_abbrev=False)
    serve.add_argument('--cluster-name', type=_cluster_name, default='Nabedra',
                       help='persisted cluster identifier; use the existing name when attaching')
    serve.add_argument('--config')
    serve.add_argument('--mode', choices=('fixed', 'serverless'))
    serve.add_argument('--storage')
    serve.add_argument('--region')
    serve.add_argument('--local-workers', type=_count, default=1)
    serve.add_argument('--threads-per-worker', type=_threads)
    serve.add_argument('--cold-start', action='store_true')
    doctor = commands.add_parser('doctor', allow_abbrev=False)
    doctor.add_argument('--config', default='nabedra-database.json')
    doctor.add_argument('--json', action='store_true')
    cluster = commands.add_parser('cluster', allow_abbrev=False)
    actions = cluster.add_subparsers(dest='action', required=True)
    for name in ('status', 'scale'):
        action = actions.add_parser(name, allow_abbrev=False)
        if name == 'scale':
            action.add_argument('nodes', type=_count)
        action.add_argument('--url', required=True)
        action.add_argument('--admin-token-file', required=True)
        action.add_argument('--ca-file')
        action.add_argument('--json', action='store_true')
    migrate = commands.add_parser('migrate', allow_abbrev=False)
    migrate.add_argument('--storage', required=True)
    migrate.add_argument('--region', required=True)
    migrate.add_argument('--cluster-id', required=True)
    migrate.add_argument('--confirm-writers-stopped', action='store_true', required=True)
    return parser


def _token(path, field=None):
    token = read_file(path, secret=True, limit=16384 if field else 4096).decode('ascii').strip()
    if field and token.startswith('{'):
        record = read_json(path, secret=True)
        fields(record, ('admin_token', 'data_token', 'enrollment_token'))
        token = record[field]
    if not 43 <= len(token) <= 4096 or any(not 33 <= ord(c) <= 126 for c in token):
        raise ConfigError('Invalid protected token file')
    return token


def _auth(config):
    auth = read_json(config['auth_file'], secret=True)
    fields(auth, ('admin_token', 'data_token', 'enrollment_token'))
    for token in auth.values():
        if type(token) is not str or not 43 <= len(token) <= 4096 or any(not 33 <= ord(c) <= 126 for c in token):
            raise ConfigError('Invalid protected authentication file')
    if len(set(auth.values())) != 3:
        raise ConfigError('Admin, data and enrollment tokens must differ')
    return auth


def _init(args):
    def required(value, prompt):
        if value:
            return value
        if args.non_interactive or not sys.stdin.isatty():
            raise ConfigError('Missing settings; supply mode, storage and region flags')
        return input(prompt + ': ').strip()
    mode = required(args.mode, 'Mode (fixed/serverless)')
    if mode not in ('fixed', 'serverless'):
        raise ConfigError('Mode must be fixed or serverless')
    if (mode == 'fixed' and args.control_storage) or (mode == 'serverless' and args.storage):
        raise ConfigError('Use storage for fixed mode and control-storage for serverless mode')
    storage = s3_storage(required(args.storage if mode == 'fixed' else args.control_storage, 'S3 storage URI'), required(args.region, 'AWS region'))
    output = Path(args.output).absolute()
    auth_path = Path(args.auth_file).absolute() if args.auth_file else output.with_suffix('.auth.json')
    if output.exists() or (not args.auth_file and auth_path.exists()):
        raise ConfigError('Refusing to overwrite configuration or credentials')
    config = validate(example(mode, storage, auth_path, args.threads_per_worker))
    if args.auth_file:
        if any((args.data_token_file, args.admin_token_file, args.enrollment_token_file)):
            raise ConfigError('Use auth-file or individual token files')
        _auth(config)
    else:
        auth = {key: _token(path) if path else secrets.token_urlsafe(32) for key, path in (
            ('admin_token', args.admin_token_file), ('data_token', args.data_token_file),
            ('enrollment_token', args.enrollment_token_file))}
        if len(set(auth.values())) != 3:
            raise ConfigError('Admin, data and enrollment tokens must differ')
        protected_write(auth_path, auth)
    try:
        protected_write(output, config)
    except BaseException:
        if not args.auth_file:
            auth_path.unlink()
        raise
    print(json.dumps(dict(config=str(output), auth_file=str(auth_path))))
    return 0


def _absolute_config(config, source):
    result = copy.deepcopy(config)
    base = Path(source).absolute().parent
    def absolute(record, key):
        if record.get(key) is not None:
            record[key] = str(base / record[key])
    absolute(result, 'auth_file')
    if result.get('tls'):
        for key in result['tls']:
            absolute(result['tls'], key)
    if result.get('engine') == 'oltp':
        if result['topology']['mode'] == 'local':
            absolute(result['topology'], 'data_dir')
        else:
            for key in ('cert_file', 'key_file', 'ca_file'):
                absolute(result['topology']['worker_tls'], key)
    elif result['storage']['driver'] == 'http':
        for key in ('token_file', 'ca_file'):
            absolute(result['storage']['options'], key)
    return result


def _serve(args, binary):
    from ._cli_supervise import supervise
    if args.storage:
        if args.config or args.mode not in (None, 'fixed'):
            raise ConfigError('Direct storage requires fixed mode and no config')
        token = os.environ.get('NABEDRA_TOKEN', '')
        if not 43 <= len(token) <= 4096 or any(not 33 <= ord(c) <= 126 for c in token):
            raise ConfigError('Fixed mode requires a valid NABEDRA_TOKEN')
        with tempfile.TemporaryDirectory(prefix='nabedra-fixed-') as temporary:
            auth = Path(temporary) / 'auth.json'
            protected_write(auth, dict(admin_token=secrets.token_urlsafe(32), data_token=token,
                                       enrollment_token=secrets.token_urlsafe(32)))
            config = example('fixed', s3_storage(args.storage, args.region), auth,
                             args.threads_per_worker or 4)
            return supervise(binary, validate(config), args.local_workers,
                             cold_start=args.cold_start, cluster_name=args.cluster_name)
    if args.mode or args.region:
        raise ConfigError('Mode and region require storage; otherwise use config')
    source = args.config or 'nabedra-database.json'
    config = _absolute_config(load(source), source)
    _auth(config)
    if args.threads_per_worker:
        resources = config.setdefault('worker', {}).setdefault('resources', dict(memory_limit_bytes=8 << 30, task_memory_limit_bytes=512 << 20, control_memory_reserve_bytes=256 << 20))
        resources['max_threads'] = args.threads_per_worker
        if config.get('engine') != 'oltp':
            config['worker']['partition_slots'] = args.threads_per_worker
        validate(config)
    return supervise(binary, config, args.local_workers, cold_start=args.cold_start,
                     cluster_name=args.cluster_name)


def main(argv=None):
    parser = _parser()
    args = parser.parse_args(argv)
    try:
        if args.version:
            if getattr(args, 'command', None):
                raise ConfigError('Version does not accept a command')
            binary = resolve_server_binary(check=True)
            versions = dict(cli=CLI_VERSION, binary=binary_version(binary), config=CONFIG_VERSION, protocol=PROTOCOL_VERSION)
            print(json.dumps(versions) if args.json else 'nabedra ' + CLI_VERSION + ' (binary ' + versions['binary'] + ', config 2, protocol 1)')
            return 0
        if getattr(args, 'command', None) is None:
            parser.error('command required')
        if args.command == 'init':
            return _init(args)
        if args.command == 'cluster':
            from ._control_transport import _ControlTransport
            import time
            url(args.url)
            transport = _ControlTransport(args.url, _token(args.admin_token_file, 'admin_token'), ca_file=args.ca_file)
            _, response = transport.request('cluster.' + args.action, {} if args.action == 'status' else dict(nodes=args.nodes), deadline=time.monotonic() + 30)
            print(json.dumps(response, indent=None if args.json else 2))
            return 0
        # Invalid configuration is an argument error, even if no binary exists.
        if args.command == 'doctor':
            config = _absolute_config(load(args.config), args.config)
            _auth(config)
            for path in (config['tls'] or {}).values():
                read_file(path, secret=path == (config['tls'] or {}).get('key_file'))
        if args.command == 'serve' and not args.storage:
            _auth(_absolute_config(load(args.config or 'nabedra-database.json'), args.config or 'nabedra-database.json'))
        if args.command == 'migrate':
            s3_storage(args.storage, args.region)
            if str(UUID(args.cluster_id)) != args.cluster_id.lower():
                raise ValueError('Invalid cluster identity')
        binary = resolve_server_binary(check=True)
        if args.command == 'serve':
            status = _serve(args, binary)
            if status < 0:
                signal.signal(-status, signal.SIG_DFL)
                os.kill(os.getpid(), -status)
            return status
        if args.command == 'worker':
            command = ['worker', '--bootstrap', args.bootstrap]
        elif args.command == 'migrate':
            command = ['database', 'migrate', '--storage', args.storage, '--region', args.region, '--cluster-id', args.cluster_id, '--confirm-writers-stopped']
        else:
            # Backend validates runtime-specific invariants and probes storage GETs.
            # No PUT probe is allowed. Write permissions remain unverified.
            if not args.json:
                print('Config and secret permissions checked. Storage writes remain unverified.', flush=True)
            command = ['doctor', '--config', os.path.abspath(args.config)]
            if args.json:
                command.append('--json')
        os.execv(binary, [binary, *command])
    except (ConfigError, ValueError, TypeError, KeyError, FileNotFoundError):
        print('nabedra: invalid configuration or arguments; see --help', file=sys.stderr)
        return 2
    except (OSError, RuntimeError):
        print(BINARY_FAILURE, file=sys.stderr)
        return 1
    except Exception:
        print('nabedra: operation failed; outcome may be unknown. No automatic retry was attempted.', file=sys.stderr)
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
