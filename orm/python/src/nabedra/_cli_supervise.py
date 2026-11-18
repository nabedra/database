"""CLI ownership of the existing API protocol and enrolled local workers."""
from __future__ import annotations

import copy
import os
from pathlib import Path
import selectors
import signal
import socket
import subprocess
import tempfile
import time
from uuid import uuid4

from ._cli_config import ConfigError, protected_write, read_json
from ._serve_process import _Owner, _Interrupted, _message, _STARTUP_SECONDS
from .validation import validate_identifier


class _Children(_Owner):
    def __init__(self):
        super().__init__()
        self.workers = []
        self.worker_failure = None

    def pump(self, deadline, *, stopping=False):
        # Consume worker diagnostics without retaining or exposing credentials.
        for child in self.workers:
            status = child.poll()
            if status is not None and not stopping:
                self.worker_failure = status if status else 1
                raise RuntimeError('Local worker exited')
        super().pump(deadline, stopping=stopping)

    def start_worker(self, binary, bootstrap):
        child = subprocess.Popen([binary, 'worker', '--bootstrap', str(bootstrap)],
                                 stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
                                 stderr=subprocess.DEVNULL, start_new_session=True)
        self.workers.append(child)

    def drain_workers(self, seconds):
        for child in self.workers:
            if child.poll() is None:
                child.send_signal(self.signals[0] if self.signals else signal.SIGTERM)
        deadline = time.monotonic() + seconds
        while any(child.poll() is None for child in self.workers) and time.monotonic() < deadline:
            self.pump(deadline, stopping=True)
            if len(self.signals) > 1:
                break
        forced = False
        for child in self.workers:
            if child.poll() is None:
                forced = True
                child.kill()
            child.wait()
        return forced


def _port():
    # The runtime must bind and advertise the same address for enrollment.
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


def supervise(binary, config, count, *, cold_start=False, cluster_name='Nabedra'):
    cluster_name = validate_identifier(cluster_name)
    embedded_oltp = config.get('engine') == 'oltp'
    if embedded_oltp and count != 1:
        raise ConfigError('OLTP API configuration connects its declared topology; local-workers must be 1')
    owner = _Children()
    failure = None
    status = 0
    with tempfile.TemporaryDirectory(prefix='nabedra-') as directory:
        path = Path(directory) / 'api.json'
        protected_write(path, config)
        try:
            owner.install()
            owner.spawn(str(path), cold_start=cold_start, binary=binary)
            deadline = time.monotonic() + _STARTUP_SECONDS
            startup = dict(version=1, type='init', name=cluster_name,
                           initial_policy=dict(mode='fixed', desired=count, min_nodes=count, max_nodes=count),
                           provisioning='external', bridge=None)
            owner.send(startup, deadline)
            first = owner.receive(deadline)
            _message(first, 'prepared', False)
            owner.send(dict(version=1, type='activate'), deadline)
            ready = owner.receive(deadline)
            _message(ready, 'ready', False)
            if any(first[k] != ready[k] for k in ('cluster_id', 'bound_url')):
                raise RuntimeError('API readiness mismatch')
            auth = read_json(config['auth_file'], secret=True)
            for _ in range(0 if embedded_oltp else count):
                bootstrap = copy.deepcopy(config['worker'])
                bootstrap.update(version=config['version'], cluster_id=config['cluster_id'],
                                 node_id=str(uuid4()), request_id=None, api_urls=config['api_urls'],
                                 storage=config['storage'], enrollment_token=auth['enrollment_token'], tls=config['tls'])
                if config['version'] == 2:
                    bootstrap['mode'] = config['mode']
                # Local workers use separate loopback listeners and the identical
                # enrollment/template resources as separately launched workers.
                bootstrap['listen'] = '127.0.0.1:0'
                bootstrap['advertise_url'] = ('https' if config['tls'] else 'http') + '://127.0.0.1:' + str(_port())
                bootstrap['listen'] = bootstrap['advertise_url'].split('://')[1]
                worker_path = Path(directory) / (bootstrap['node_id'] + '.json')
                protected_write(worker_path, bootstrap)
                owner.start_worker(binary, worker_path)
            # API is ready, but process launch does not prove worker enrollment.
            owner.announce(ready['bound_url'], deadline, event='nabedra.serve.launched')
            record = owner.receive(None)
            _message(record, 'fatal', False)
            raise RuntimeError('Unexpected API message')
        except _Interrupted:
            pass
        except Exception as error:
            failure = error
        finally:
            try:
                if owner.drain_workers(config.get('lifecycle', {}).get('drain_ms', 300000) / 1000):
                    status = 1
                if owner.shutdown():
                    status = 1
            finally:
                owner.reap()
                owner.close()
        if owner.signals:
            return -owner.signals[0]
        if owner.worker_failure is not None:
            return owner.worker_failure
        if owner.child and owner.child.returncode:
            return owner.child.returncode
        return status or (1 if failure else 0)
