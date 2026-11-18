"""Inspected, retained Docker volumes and immutable worker launch identities."""
from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
import re
import stat
import subprocess
import os
import selectors
import time
from uuid import UUID

from ..providers import ProviderError

PREFIX = 'io.nabedra.'


class UnsupportedVolumeExpansion(ProviderError):
    """Docker verified the retained volume, which has no expansion operation."""

    def __init__(self, resource_id):
        self.resource_id = resource_id
        super().__init__('Docker named volumes do not expose filesystem expansion; add a pool or worker')


def identity(value):
    try:
        if str(UUID(value)) == value and UUID(value).int:
            return value
    except (ValueError, TypeError, AttributeError):
        pass
    raise ProviderError('A canonical nonzero identity is required')


def digest(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()


def private_bytes(path, maximum):
    try:
        path = Path(path)
        if not path.is_absolute() or path.resolve(strict=True) != path:
            raise ValueError
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        with os.fdopen(fd, 'rb') as source:
            info = os.fstat(source.fileno())
            if (not stat.S_ISREG(info.st_mode) or info.st_mode & 0o077 or info.st_nlink != 1
                    or not 0 < info.st_size <= maximum):
                raise ValueError
            data = source.read(maximum + 1)
            if len(data) != info.st_size:
                raise ValueError
            return data
    except (OSError, RuntimeError, TypeError, ValueError):
        raise ProviderError('Docker launch requires a bounded private regular file') from None


_INITIALIZE = r'''import fcntl,json,os,sys,pwd,grp,stat
expected=json.loads(sys.argv[1])
user,_,group=(sys.argv[2] or '0').partition(':')
account=pwd.getpwuid(int(user)) if user.isdigit() else pwd.getpwnam(user)
uid=account.pw_uid
gid=(int(group) if group.isdigit() else grp.getgrnam(group).gr_gid) if group else account.pw_gid
directory=os.open('/data',os.O_RDONLY|os.O_DIRECTORY|os.O_NOFOLLOW)
existing_only=len(sys.argv)>3 and sys.argv[3]=='existing'
lock_flags=os.O_RDONLY if existing_only else os.O_RDWR|os.O_CREAT
lock=os.open('.nabedra-provider.lock',lock_flags|os.O_NOFOLLOW,0o600,dir_fd=directory)
fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
def publish(name,data):
    temporary=name+'.pending'
    try:
        os.unlink(temporary,dir_fd=directory)
    except FileNotFoundError:
        pass
    fd=os.open(temporary,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600,dir_fd=directory)
    with os.fdopen(fd,'wb') as target:
        os.fchown(target.fileno(),uid,gid)
        target.write(data);target.flush();os.fsync(target.fileno())
    os.replace(temporary,name,src_dir_fd=directory,dst_dir_fd=directory)
    os.fsync(directory)
name='.nabedra-docker-volume.json'
try:
    fd=os.open(name,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK,dir_fd=directory)
except FileNotFoundError:
    assert not existing_only,'missing retained volume marker'
    # A provider never adopts an existing database merely because its path matches.
    assert set(os.listdir('/data'))<={'.nabedra-provider.lock',name+'.pending'},'nonempty unlabelled volume'
    os.fchown(directory,uid,gid);os.fchmod(directory,0o700)
    publish(name,json.dumps(expected,sort_keys=True).encode())
else:
    assert os.fstat(directory).st_uid==uid and os.fstat(directory).st_gid==gid,'volume owner changed'
    info=os.fstat(fd)
    assert (info.st_uid,info.st_gid)==(uid,gid),'marker owner changed'
    assert stat.S_ISREG(info.st_mode) and info.st_nlink==1 and info.st_mode&0o077==0 and info.st_size<=1024
    with os.fdopen(fd) as source:
        assert json.load(source)==expected,'volume marker mismatch'
if len(sys.argv)>3:
    with open('/seed','rb') as source:
        seed=source.read(33)
    assert len(seed)==32,'invalid mounted key'
    name='.nabedra-data-worker-key'
    try:
        fd=os.open(name,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK,dir_fd=directory)
    except FileNotFoundError:
        assert not existing_only,'missing retained mount key'
        publish(name,seed)
    else:
        with os.fdopen(fd,'rb') as key:
            info=os.fstat(key.fileno())
            assert stat.S_ISREG(info.st_mode) and info.st_nlink==1 and info.st_mode&0o077==0
            assert (info.st_uid,info.st_gid)==(uid,gid) and key.read(33)==seed,'mount key changed'
    space=os.fstatvfs(directory)
    expected['observed_free_bytes']=space.f_bavail*space.f_frsize
print(json.dumps(expected))
'''


@dataclass(frozen=True)
class DockerWorker:
    worker_id: str
    incarnation: str
    bootstrap_file: str
    network_alias: str
    cpu_millicores: int
    memory_bytes: int
    # Explicit immutable pool volume names and absolute paths in this worker.
    volumes: tuple[tuple[str, str], ...]
    binary: str = '/usr/local/bin/nabedra-db'
    zone: str | None = None
    http_ports: tuple[int, int] | None = None

    def validate(self):
        identity(self.worker_id)
        identity(self.incarnation)
        if (not re.fullmatch(r'[a-zA-Z0-9][a-zA-Z0-9_.-]{0,62}', self.network_alias)
                or type(self.cpu_millicores) is not int or not 1 <= self.cpu_millicores <= 64000
                or type(self.memory_bytes) is not int or not 64 << 20 <= self.memory_bytes <= 1 << 40
                or not self.binary.startswith('/') or not 1 <= len(self.volumes) <= 64):
            raise ProviderError('Invalid bounded Docker worker configuration')
        if self.zone is not None and (
                not isinstance(self.zone, str) or not 0 < len(self.zone) <= 128
                or type(self.http_ports) is not tuple or len(self.http_ports) != 2
                or any(type(port) is not int or not 1 <= port <= 65535 for port in self.http_ports)):
            raise ProviderError('Docker data workers require a zone and exact loopback port binding')
        if self.zone is None and self.http_ports is not None:
            raise ProviderError('A Docker launch port is supported only for data workers')
        config = Path(self.bootstrap_file)
        if not config.is_absolute() or not config.is_file() or config.is_symlink():
            raise ProviderError('Docker bootstrap must be an existing absolute regular file')
        if config.stat().st_mode & 0o077:
            raise ProviderError('Docker bootstrap must be private to its owner')
        if config.stat().st_size > 1 << 20:
            raise ProviderError('Docker bootstrap exceeds its size bound')
        try:
            bootstrap=json.loads(config.read_bytes())
            resources=bootstrap['resources']
            if (type(resources['max_threads']) is not int or type(resources['memory_limit_bytes']) is not int
                    or resources['max_threads']*1000>self.cpu_millicores
                    or resources['memory_limit_bytes']>self.memory_bytes):
                raise ValueError
            enrollment=(bootstrap['node']['enrollment'] if self.zone is None
                        else bootstrap['node']['intent']['enrollment'])
            if enrollment['worker_id']!=self.worker_id or enrollment['incarnation']!=self.incarnation:
                raise ValueError
        except (ValueError,KeyError,TypeError):
            raise ProviderError('Docker bootstrap worker identity or advertised resources contradict the enforced limits') from None
        mounts = set()
        for name, path in self.volumes:
            if (not re.fullmatch(r'[a-zA-Z0-9][a-zA-Z0-9_.-]{0,254}', name)
                    or not path.startswith('/') or path == '/' or '..' in Path(path).parts
                    or path == '/config' or path.startswith('/config/') or path in mounts):
                raise ProviderError('Invalid or duplicate Docker volume mount')
            mounts.add(path)
        return config


class DockerProvider:
    """No capacity is inferred from volume creation or a running container.

    A claim has one deterministic resource name and immutable content digest.
    Volumes remain after stop. Docker zones describe placement policy in local
    tests and do not represent independent physical availability zones.
    """

    def __init__(self, *, cluster_id, image, network, timeout=20):
        self.cluster_id = identity(cluster_id)
        if (not isinstance(image, str) or not image or image.startswith('-')
                or not isinstance(network, str) or not re.fullmatch(r'[a-zA-Z0-9][a-zA-Z0-9_.-]{0,254}', network)
                or type(timeout) not in (int, float) or not 0 < timeout <= 30):
            raise ProviderError('Explicit Docker image, network and bounded timeout are required')
        self.image, self.network, self.timeout = image, network, timeout

    def _run(self, *args, json_result=False, deadline=None):
        try:
            with subprocess.Popen(['docker', *args],stdout=subprocess.PIPE,stderr=subprocess.DEVNULL) as process:
                cutoff=time.monotonic()+self.timeout
                if deadline is not None:
                    cutoff=min(cutoff, deadline)
                output=bytearray()
                try:
                    with selectors.DefaultSelector() as selector:
                        selector.register(process.stdout,selectors.EVENT_READ)
                        while True:
                            remaining=cutoff-time.monotonic()
                            if remaining<=0 or not selector.select(remaining):
                                raise subprocess.TimeoutExpired('docker',self.timeout)
                            chunk=os.read(process.stdout.fileno(),min(65536,(8<<20)+1-len(output)))
                            if not chunk:break
                            output.extend(chunk)
                            if len(output)>8<<20:
                                raise ProviderError('Docker inventory exceeds its bound')
                    code=process.wait(timeout=max(.001,cutoff-time.monotonic()))
                except BaseException:
                    process.kill();process.wait()
                    raise
        except (OSError, subprocess.TimeoutExpired):
            raise ProviderError('Docker operation unavailable or timed out; reconcile the same claim') from None
        if code:
            # Never include bootstrap files or arbitrary provider diagnostics.
            raise ProviderError('Docker operation failed; reconcile the same claim')
        try:
            return json.loads(output) if json_result else output.decode().strip()
        except (ValueError, UnicodeError):
            raise ProviderError('Invalid Docker response') from None

    def _labels(self, claim_id, content):
        return {PREFIX + 'cluster': self.cluster_id, PREFIX + 'claim': identity(claim_id),
                PREFIX + 'content': digest(content)}

    def _owned(self, labels, expected):
        if any(labels.get(key) != value for key, value in expected.items()):
            raise ProviderError('Docker resource identity or immutable claim differs')

    def pool_name(self, claim_id):
        return 'nabedra-' + self.cluster_id + '-' + identity(claim_id)

    def ensure_pool(self, *, claim_id, pool_id, worker_id, zone, mount_seed=None,
                    template_digest=None, existing_only=False, deadline=None):
        """Create/inspect one retained volume and durably seed its exact marker."""
        content = dict(pool_id=identity(pool_id), worker_id=identity(worker_id), zone=zone)
        if not isinstance(zone, str) or not zone or len(zone) > 128:
            raise ProviderError('Explicit placement zone is required')
        if mount_seed is not None:
            if (not re.fullmatch(r'[a-f0-9]{64}', template_digest or '') or ',' in str(mount_seed)
                    or len(private_bytes(mount_seed, 32)) != 32):
                raise ProviderError('Docker data worker requires a bound private mount seed')
            content['template'] = template_digest
        labels = self._labels(claim_id, content)
        labels.update({PREFIX+'pool':pool_id,PREFIX+'worker':worker_id,PREFIX+'zone':zone})
        name = self.pool_name(claim_id)
        # Name collisions are inspected BEFORE create, which can mutate labels
        # on some drivers. Listing is an observation, never readiness authority.
        names = self._run('volume', 'ls', '--filter', 'name='+name, '--format', '{{.Name}}',
                          deadline=deadline).splitlines()
        if name not in names:
            if existing_only:
                raise ProviderError('The retained Docker volume is missing')
            args = ['volume', 'create']
            for key, value in labels.items():
                args.extend(['--label', key + '=' + value])
            self._run(*args, name, deadline=deadline)
        observed = self._run('volume', 'inspect', name, json_result=True, deadline=deadline)
        if len(observed) != 1 or observed[0].get('Name') != name:
            raise ProviderError('Docker volume inventory changed')
        self._owned(observed[0].get('Labels') or {}, labels)
        if mount_seed is not None and (
                observed[0].get('Driver') != 'local' or observed[0].get('Scope') != 'local'
                or observed[0].get('Options')):
            raise ProviderError('Docker launch requires a local named volume without driver options')
        marker = dict(format_version=1, volume_name=name, volume_marker=pool_id)
        image_user=self._run('image','inspect','--format','{{json .Config.User}}',self.image,
                             json_result=True,deadline=deadline)
        extra = []
        mode = []
        if mount_seed is not None:
            extra = ['--mount', f'type=bind,source={mount_seed},target=/seed,readonly']
            mode = ['existing' if existing_only else 'initialize']
        volume_mount = 'type=volume,source=' + name + ',target=/data'
        if existing_only and mount_seed is not None:
            volume_mount += ',readonly'
        initialized = self._run('run', '--rm', '--network', 'none', '--cpus', '.25',
            '--memory', '128m', '--pids-limit', '32','--user','0:0', '--mount', volume_mount,
            *extra, '--entrypoint', 'python3', self.image, '-c', _INITIALIZE, json.dumps(marker), image_user,
            *mode, json_result=True, deadline=deadline)
        free = initialized.pop('observed_free_bytes', None) if mount_seed is not None else None
        if initialized != marker:
            raise ProviderError('Docker volume marker was not observed')
        result = dict(resource_id='docker-volume:' + name, volume_name=name,
                      volume_marker=pool_id, state='provisioned')
        if mount_seed is not None:
            if type(free) is not int or free < 0:
                raise ProviderError('Docker backing filesystem free space was not observed')
            result['observed_free_bytes'] = free
        return result

    def ensure_worker(self, *, claim_id, worker: DockerWorker, additional_mounts=(),
                      deadline=None, inspect_only=False):
        """Reconcile an immutable launch. Existing exited launches stay exited."""
        config = worker.validate()
        # Auxiliary mounts are for protected TLS/config artifacts or a cached
        # binary. Data volumes are always explicit named volumes above.
        auxiliary = []
        for source, target in additional_mounts:
            source = Path(source)
            if (not source.is_absolute() or not source.exists() or ',' in str(source)
                    or not target.startswith('/') or ',' in target or target == '/'
                    or '..' in Path(target).parts or target in [p for _, p in worker.volumes]):
                raise ProviderError('Invalid Docker auxiliary mount')
            auxiliary.append((str(source), target))
        data_worker = worker.zone is not None
        worker_details = vars(worker) if data_worker else {
            key: value for key, value in vars(worker).items() if key not in ('zone', 'http_ports')}
        content = dict(worker=worker_details, bootstrap_sha256=hashlib.sha256(config.read_bytes()).hexdigest(),
                       image=self.image, network=self.network, auxiliary=auxiliary)
        if data_worker:
            if not re.fullmatch(r'sha256:[a-f0-9]{64}', self.image):
                raise ProviderError('Automatic Docker launch requires an immutable image ID')
            launch = json.loads(private_bytes(config, 1 << 20))
            enrollment = launch['node']['intent']['enrollment']
            content['auxiliary_sha256'] = [hashlib.sha256(private_bytes(path, 1 << 20)).hexdigest()
                                           for path, _ in auxiliary]
        labels = self._labels(claim_id, content)
        labels[PREFIX + 'worker'] = worker.worker_id
        labels[PREFIX + 'incarnation'] = worker.incarnation
        if data_worker:
            labels.update({PREFIX + 'zone': worker.zone,
                           PREFIX + 'certificate': enrollment['certificate_sha256'],
                           PREFIX + 'enrollment-revision': str(enrollment['enrollment_revision'])})
        name = 'nabedra-worker-' + identity(claim_id)
        ids = self._run('container', 'ls', '-a', '--filter', 'name=^/' + name + '$',
                        '--format', '{{.ID}}', deadline=deadline).splitlines()
        if not ids:
            if inspect_only:
                return None
            for volume, _ in worker.volumes:
                observed = self._run('volume', 'inspect', volume, json_result=True, deadline=deadline)
                if (len(observed) != 1 or (observed[0].get('Labels') or {}).get(PREFIX + 'cluster') != self.cluster_id
                        or (observed[0].get('Labels') or {}).get(PREFIX+'worker')!=worker.worker_id):
                    raise ProviderError('Worker data volume is not owned by this cluster')
            args = ['container', 'create', '--name', name, '--network', self.network,
                    '--network-alias', worker.network_alias, '--cpus', str(worker.cpu_millicores / 1000),
                    '--memory', str(worker.memory_bytes), '--pids-limit', '128']
            if data_worker:
                host, internal = worker.http_ports
                args.extend(['--publish', f'127.0.0.1:{host}:{internal}'])
            for key, value in labels.items():
                args.extend(['--label', key + '=' + value])
            for volume, target in worker.volumes:
                args.extend(['--mount', 'type=volume,source=' + volume + ',target=' + target])
            args.extend(['--mount', 'type=bind,source=' + str(config) + ',target=/bootstrap.json,readonly'])
            for source, target in auxiliary:
                args.extend(['--mount', 'type=bind,source=' + source + ',target=' + target + ',readonly'])
            role = 'data-worker' if data_worker else 'worker'
            args.extend(['--entrypoint', worker.binary, self.image, role, '--bootstrap', '/bootstrap.json'])
            self._run(*args, deadline=deadline)
        observed = self._run('container', 'inspect', name, json_result=True, deadline=deadline)
        if len(observed) != 1:
            raise ProviderError('Docker worker identity is ambiguous')
        record = observed[0]
        self._owned(record.get('Config', {}).get('Labels') or {}, labels)
        if (record.get('HostConfig',{}).get('NanoCpus')!=worker.cpu_millicores*1000000
                or record.get('HostConfig',{}).get('Memory')!=worker.memory_bytes):
            raise ProviderError('Docker worker CPU or memory ceiling changed')
        expected_mounts = {(name, path) for name, path in worker.volumes}
        mounts = record.get('Mounts', [])
        actual_mounts = {(m.get('Name'), m.get('Destination')) for m in mounts
                         if m.get('Type') == 'volume' and (not data_worker or m.get('RW') is True)}
        if expected_mounts != actual_mounts:
            raise ProviderError('Docker worker mounted volume identity changed')
        if data_worker:
            expected_binds = {(str(config), '/bootstrap.json'), *auxiliary}
            actual_binds = {(m.get('Source'), m.get('Destination')) for m in mounts
                            if m.get('Type') == 'bind' and m.get('RW') is False}
            host, internal = worker.http_ports
            ports = {f'{internal}/tcp': [{'HostIp': '127.0.0.1', 'HostPort': str(host)}]}
            network = record.get('NetworkSettings', {}).get('Networks') or {}
            if (record.get('Name') != '/' + name or record.get('Image') != self.image
                    or not re.fullmatch(r'[a-f0-9]{64}', record.get('Id') or '')
                    or record.get('Path') != worker.binary
                    or record.get('Args') != ['data-worker', '--bootstrap', '/bootstrap.json']
                    or set(network) != {self.network}
                    or worker.network_alias not in (network[self.network].get('Aliases') or ())
                    or record['HostConfig'].get('PidsLimit') != 128
                    or record['HostConfig'].get('PortBindings') != ports
                    or len(mounts) != len(expected_binds) + len(expected_mounts)
                    or actual_binds != expected_binds):
                raise ProviderError('Docker data worker launch binding changed')
        started = record.get('State', {}).get('Status') == 'created' and not inspect_only
        if started:
            self._run('container', 'start', record['Id'], deadline=deadline)
            if data_worker:
                record = self.inspect_worker('docker-container:' + record['Id'], deadline=deadline)
            else:
                record = self._run('container', 'inspect', record['Id'], json_result=True, deadline=deadline)[0]
        result = dict(resource_id='docker-container:' + record['Id'], worker_id=worker.worker_id,
                      incarnation=worker.incarnation, state=record['State']['Status'])
        if data_worker:
            result['started'] = started
        return result

    def worker_pool_names(self, worker_id, *, deadline):
        names = self._run('volume', 'ls', '--filter', 'label=' + PREFIX + 'cluster=' + self.cluster_id,
                          '--filter', 'label=' + PREFIX + 'worker=' + identity(worker_id),
                          '--format', '{{.Name}}', deadline=deadline).splitlines()
        if len(names) > 64 or any(not re.fullmatch(r'[a-zA-Z0-9][a-zA-Z0-9_.-]{0,254}', name)
                                  for name in names):
            raise ProviderError('Docker retained volume inventory exceeds its bound')
        return frozenset(names)

    def inspect_launch_environment(self, *, deadline):
        image = self._run('image', 'inspect', self.image, json_result=True, deadline=deadline)
        network = self._run('network', 'inspect', self.network, json_result=True, deadline=deadline)
        if (len(image) != 1 or image[0].get('Id') != self.image
                or len(network) != 1 or network[0].get('Name') != self.network
                or network[0].get('Driver') != 'bridge' or network[0].get('Scope') != 'local'):
            raise ProviderError('Automatic Docker launch requires the installed image and local bridge network')

    def inspect_worker(self, resource_id, *, deadline=None):
        if not isinstance(resource_id, str) or not re.fullmatch(r'docker-container:[a-f0-9]{64}', resource_id):
            raise ProviderError('Exact Docker container identity is required')
        identifier = resource_id.split(':', 1)[1]
        records = self._run('container', 'inspect', identifier, json_result=True, deadline=deadline)
        if len(records) != 1 or records[0].get('Id') != identifier:
            raise ProviderError('Docker worker identity is ambiguous')
        record = records[0]
        if (record.get('Config', {}).get('Labels') or {}).get(PREFIX + 'cluster') != self.cluster_id:
            raise ProviderError('Docker worker belongs to another cluster')
        return record

    def worker_ids(self, *, deadline):
        rows = self._run('container', 'ls', '-a', '--no-trunc', '--filter',
                         'label=' + PREFIX + 'cluster=' + self.cluster_id, '--format', '{{.ID}}',
                         deadline=deadline).splitlines()
        if len(rows) > 4096 or any(not re.fullmatch(r'[a-f0-9]{64}', row) for row in rows):
            raise ProviderError('Docker cluster inventory exceeds its bound')
        return frozenset(rows)

    def expand_pool(self, *, resource_id, target_bytes):
        prefix = 'docker-volume:'
        if (not isinstance(resource_id, str) or not resource_id.startswith(prefix)
                or not re.fullmatch(r'[a-zA-Z0-9][a-zA-Z0-9_.-]{0,254}', resource_id[len(prefix):])
                or type(target_bytes) is not int or target_bytes <= 0):
            raise ProviderError('Exact Docker volume and positive target size are required')
        volume = resource_id[len(prefix):]
        records = self._run('volume', 'inspect', volume, json_result=True)
        if (len(records) != 1 or records[0].get('Name') != volume
                or (records[0].get('Labels') or {}).get(PREFIX + 'cluster') != self.cluster_id):
            raise ProviderError('Docker volume identity or cluster changed')
        raise UnsupportedVolumeExpansion(resource_id)
