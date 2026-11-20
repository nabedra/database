"""Strict, bounded CLI configuration and protected local material."""
from __future__ import annotations

import ipaddress
import json
import os
from pathlib import Path
import re
import stat
from urllib.parse import urlsplit
from uuid import UUID, uuid4


class ConfigError(ValueError):
    pass


def unique(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ConfigError('Duplicate configuration field')
        result[key] = value
    return result


def read_file(path, *, secret=False, limit=131072):
    flags = os.O_RDONLY | getattr(os, 'O_NONBLOCK', 0) | getattr(os, 'O_NOFOLLOW', 0)
    with os.fdopen(os.open(path, flags), 'rb') as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or (secret and info.st_mode & 0o077):
            raise ConfigError('Secret files must be regular files with mode 0600 or stricter')
        data = stream.read(limit + 1)
    if len(data) > limit:
        raise ConfigError('Configuration or secret exceeds size limit')
    return data


def read_json(path, *, secret=False):
    try:
        return json.loads(read_file(path, secret=secret), object_pairs_hook=unique,
                          parse_constant=lambda _: (_ for _ in ()).throw(ConfigError('Invalid JSON number')))
    except (ValueError, UnicodeError, RecursionError):
        raise ConfigError('Invalid configuration JSON') from None


def fields(value, required, optional=()):
    if type(value) is not dict or not set(required) <= value.keys() or value.keys() - set(required) - set(optional):
        raise ConfigError('Missing or unknown configuration fields')


def integer(value, low, high):
    if type(value) is not int or not low <= value <= high:
        raise ConfigError('Configuration integer outside permitted range')


def text(value):
    if type(value) is not str or not value or len(value) > 2048 or any(ord(c) < 32 for c in value):
        raise ConfigError('Invalid configuration text')


def url(value):
    text(value)
    parsed = urlsplit(value)
    try:
        loopback = ipaddress.ip_address(parsed.hostname).is_loopback
    except ValueError:
        loopback = parsed.hostname == 'localhost'
    if (parsed.scheme != 'https' and not (parsed.scheme == 'http' and loopback)) or not parsed.hostname or parsed.username or parsed.password or parsed.query or parsed.fragment or parsed.path not in ('', '/'):
        raise ConfigError('Links require HTTPS except loopback fixtures')
    _ = parsed.port
    return parsed


def listen(value, tls):
    text(value)
    host, port = value.rsplit(':', 1)
    integer(int(port), 0, 65535)
    try:
        local = ipaddress.ip_address(host.strip('[]')).is_loopback
    except ValueError:
        local = host == 'localhost'
    if not tls and not local:
        raise ConfigError('External listeners require TLS')


def s3_storage(uri, region):
    parsed = urlsplit(uri)
    bucket = parsed.netloc
    if (parsed.scheme != 's3' or not re.fullmatch(r'[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]', bucket)
            or '..' in bucket or parsed.query or parsed.fragment or '%' in uri or '\\' in uri):
        raise ConfigError('Expected a general-purpose s3://bucket/prefix/ URI')
    try:
        ipaddress.ip_address(bucket)
    except ValueError:
        pass
    else:
        raise ConfigError('Bucket cannot be an IP address')
    prefix = parsed.path.lstrip('/')
    if any(p in ('.', '..', '') for p in prefix.rstrip('/').split('/')) and prefix:
        raise ConfigError('Invalid storage prefix')
    if not re.fullmatch(r'[a-z]{2}(?:-[a-z0-9]+)+-\d+', region or ''):
        raise ConfigError('A valid AWS region is required')
    return dict(driver='s3', options=dict(bucket=bucket, namespace=prefix, region=region))


def _validate_oltp(config):
    fields(config, ('version', 'engine', 'bind', 'database', 'auth_file', 'topology'),
           ('tls', 'worker', 'cluster_name'))
    integer(config['version'], 1, 1)
    from .validation import validate_identifier
    if type(config['database']) is not str or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]{0,127}', config['database']):
        raise ConfigError('Invalid OLTP database selector')
    if 'cluster_name' in config:
        validate_identifier(config['cluster_name'])
    text(config['auth_file'])
    tls = config.get('tls')
    if tls is not None:
        fields(tls, ('cert_file', 'key_file'), ('ca_file',))
        for path in tls.values():
            if path is not None:
                text(path)
    listen(config['bind'], tls)
    topology = config['topology']
    fields(topology, ('mode',), ('data_dir', 'cluster_id', 'control_group_id', 'enrollment', 'worker_tls',
                                'control_peers', 'scoped_oltp', 'direct_oltp'))
    if topology['mode'] == 'local':
        fields(topology, ('mode', 'data_dir'))
        text(topology['data_dir'])
    elif topology['mode'] == 'remote':
        fields(topology, ('mode', 'cluster_id', 'control_group_id', 'enrollment', 'worker_tls', 'control_peers'),
               ('scoped_oltp', 'direct_oltp'))
        for switch in ('scoped_oltp', 'direct_oltp'):
            if switch in topology and type(topology[switch]) is not bool:
                raise ConfigError(f'{switch} must be a boolean')
        for key in ('cluster_id', 'control_group_id'):
            if str(UUID(topology[key])) != topology[key] or not UUID(topology[key]).int:
                raise ConfigError('Invalid OLTP group identity')
        fields(topology['worker_tls'], ('cert_file', 'key_file', 'ca_file'))
        for path in topology['worker_tls'].values():
            text(path)
        api = topology['enrollment']
        fields(api, ('database', 'enrollment'))
        if api['database'] != config['database']:
            raise ConfigError('API enrollment database differs')
        _validate_enrollment(api['enrollment'])
        peers = topology['control_peers']
        if type(peers) is not list or not 1 <= len(peers) <= 128:
            raise ConfigError('Invalid Control bootstrap peers')
        for peer in peers:
            _validate_enrollment(peer)
        if len({peer['worker_id'] for peer in peers}) != len(peers):
            raise ConfigError('Duplicate Control bootstrap peer')
    else:
        raise ConfigError('Unsupported OLTP topology')
    worker = config.get('worker', {})
    fields(worker, (), ('resources', 'execution', 'queue_size'))
    if 'queue_size' in worker:
        integer(worker['queue_size'], 1, 65536)
    _validate_worker_limits(worker, oltp=True)
    return config


def _validate_enrollment(value):
    fields(value, ('worker_id', 'incarnation', 'certificate_sha256', 'endpoint', 'server_name', 'enrollment_revision'))
    for key in ('worker_id', 'incarnation'):
        if type(value[key]) is not str or str(UUID(value[key])) != value[key] or not UUID(value[key]).int:
            raise ConfigError('Invalid process enrollment identity')
    if type(value['certificate_sha256']) is not str or not re.fullmatch('[0-9a-fA-F]{64}', value['certificate_sha256']):
        raise ConfigError('Invalid certificate fingerprint')
    integer(value['enrollment_revision'], 1, 2**63 - 1)
    text(value['endpoint'])
    text(value['server_name'])
    if any(char.isspace() for char in value['endpoint'] + value['server_name']):
        raise ConfigError('Invalid enrollment endpoint')


def _validate_worker_limits(worker, minimum_threads=1, oltp=False):
    if 'resources' in worker:
        r = worker['resources']
        fields(r, 'max_threads memory_limit_bytes task_memory_limit_bytes control_memory_reserve_bytes'.split())
        integer(r['max_threads'], minimum_threads, 256)
        for key in set(r) - {'max_threads'}:
            integer(r[key], 1, 2**63 - 1)
        if r['task_memory_limit_bytes'] + r['control_memory_reserve_bytes'] > r['memory_limit_bytes']:
            raise ConfigError('Worker memory limits contradict each other')
    if 'execution' in worker:
        e = worker['execution']
        # OLTP only: `direct_oltp` routes eligible point operations on the direct path
        # once `data.direct.v1` is active (default true on the server).
        fields(e, 'max_workers_per_request max_pending_requests max_queue_bytes_per_replica max_retained_result_bytes_per_worker result_retention_ms queue_replication queue_lifetime partition_size'.split(),
               ('direct_oltp',) if oltp else ())
        if 'direct_oltp' in e and type(e['direct_oltp']) is not bool:
            raise ConfigError('direct_oltp must be a boolean')
        for key in ('max_workers_per_request', 'max_pending_requests', 'max_queue_bytes_per_replica', 'max_retained_result_bytes_per_worker', 'result_retention_ms'):
            integer(e[key], 1, 2**63 - 1)
        if e['queue_replication'] != 'all_ready_workers' or e['queue_lifetime'] != 'live_cluster':
            raise ConfigError('Unsupported queue policy')
        text(e['partition_size'])


def validate(config):
    if type(config) is dict and config.get('engine') == 'oltp':
        return _validate_oltp(config)
    fields(config, 'version cluster_id bind advertise_url api_urls storage auth_file tls worker'.split(), ('mode', 'lifecycle'))
    integer(config['version'], 1, 2)
    if config['version'] == 2:
        if config.get('mode') not in ('fixed', 'serverless'):
            raise ConfigError('Version 2 requires mode fixed or serverless')
    elif 'mode' in config:
        raise ConfigError('Version 1 has no mode field')
    if str(UUID(config['cluster_id'])) != config['cluster_id']:
        raise ConfigError('Invalid cluster UUID')
    tls = config['tls']
    if tls is not None:
        fields(tls, ('cert_file', 'key_file', 'ca_file'))
        for path in tls.values():
            text(path)
    listen(config['bind'], tls)
    url(config['advertise_url'])
    if type(config['api_urls']) is not list or not 1 <= len(config['api_urls']) <= 64:
        raise ConfigError('Invalid API URL list')
    for endpoint in config['api_urls']:
        if (url(endpoint).scheme == 'https') != bool(tls):
            raise ConfigError('TLS and API URL schemes must agree')
    if (url(config['advertise_url']).scheme == 'https') != bool(tls):
        raise ConfigError('TLS and advertise scheme must agree')
    text(config['auth_file'])
    storage = config['storage']
    fields(storage, ('driver', 'options'))
    options = storage['options']
    if storage['driver'] == 's3':
        fields(options, ('bucket', 'namespace'), ('region',))
        s3_storage('s3://' + options['bucket'] + '/' + options['namespace'], options.get('region', 'us-east-1'))
    elif storage['driver'] == 'http':
        fields(options, ('base_url', 'namespace', 'token_file', 'ca_file'))
        endpoint = urlsplit(options['base_url'])
        url(endpoint._replace(path='').geturl())
        text(options['token_file'])
    else:
        raise ConfigError('Unsupported storage driver')
    worker = config['worker']
    fields(worker, 'listen operation_slots partition_slots queue_size partition_size'.split(), ('execution', 'resources'))
    listen(worker['listen'], tls)
    for key in ('operation_slots', 'partition_slots', 'queue_size'):
        integer(worker[key], 1, 65536)
    text(worker['partition_size'])
    _validate_worker_limits(worker)
    if 'lifecycle' in config:
        fields(config['lifecycle'], (), 'heartbeat_ms expiry_ms readiness_ms drain_ms lease_ms renew_ms stabilization_ms cooldown_ms retry_initial_ms retry_max_ms max_attempts'.split())
        for number in config['lifecycle'].values():
            integer(number, 1, 2**63 - 1)
    return config


def load(path):
    try:
        return validate(read_json(path))
    except (KeyError, TypeError, ValueError, AttributeError):
        raise ConfigError('Invalid API configuration; see nabedra database init --help') from None


def protected_write(path, value):
    data = (json.dumps(value, indent=2) + '\n').encode() if not isinstance(value, bytes) else value
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, 'wb') as stream:
        stream.write(data)


def example(mode, storage, auth_file, threads=4, bind='127.0.0.1:8000'):
    return dict(version=2, mode=mode, cluster_id=str(uuid4()), bind=bind,
                advertise_url='http://' + bind, api_urls=['http://' + bind],
                storage=storage, auth_file=str(auth_file), tls=None,
                worker=dict(listen='127.0.0.1:0', operation_slots=4, partition_slots=threads,
                            queue_size=64, partition_size='64 MiB',
                            resources=dict(max_threads=threads, memory_limit_bytes=8 << 30,
                                           task_memory_limit_bytes=512 << 20, control_memory_reserve_bytes=256 << 20)))
