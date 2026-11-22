#!/usr/bin/env python3
"""Export a full OLTP archive from a RocksDB-era cluster before upgrading."""

import argparse
import hashlib
from http.client import HTTPException
import json
import os
from pathlib import Path
import ssl
import sys
from urllib.error import HTTPError, URLError
from urllib.parse import urlsplit
from urllib.request import HTTPRedirectHandler, HTTPSHandler, Request, build_opener
from uuid import UUID, uuid4


def archive_path(repository, cluster_id, backup_id, attempt=1):
    # The old archive.create endpoint requires this exact destination. Match
    # native_archive_id's SHA-256 over the compact serde JSON tuple.
    identity = [list(b'nabedra/native-backup-id/v1'), cluster_id, {'backup_id': backup_id, 'attempt': attempt}]
    digest = hashlib.sha256(json.dumps(identity, separators=(',', ':')).encode()).digest()
    return repository / str(UUID(bytes=digest[:16]))


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def export(workers, database, token, repository, cluster_id, backup_id, *, ca_file=None, timeout=3600,
           insecure=False):
    for worker in workers:
        url = urlsplit(worker)
        if url.scheme == 'http' and not insecure:
            raise RuntimeError(f'HTTP worker URL requires --insecure: {worker}')
        if url.scheme not in ('http', 'https') or not url.hostname:
            raise RuntimeError(f'expected an HTTP or HTTPS worker URL: {worker}')

    # The repository path belongs to the workers and may not be mounted here.
    # Persist only confirmed expiries under the identity of this local export.
    state_root = Path(os.environ.get('XDG_STATE_HOME') or Path.home() / '.local' / 'state')
    identity = json.dumps([cluster_id, database, backup_id, str(repository)], separators=(',', ':'))
    state = state_root / 'nabedra' / 'upgrade-export' / hashlib.sha256(identity.encode()).hexdigest()
    state.mkdir(mode=0o700, parents=True, exist_ok=True)

    context = ssl.create_default_context(cafile=ca_file)
    opener = build_opener(NoRedirect(), HTTPSHandler(context=context))
    for attempt in range(1, 6):
        expired = state / f'expired-{attempt}'
        if expired.is_file():
            continue
        owner = {'backup_id': backup_id, 'attempt': attempt}
        destination = archive_path(repository, cluster_id, backup_id, attempt)
        print(f'Export attempt {attempt} on the workers: {destination}', file=sys.stderr, flush=True)
        manifest = export_attempt(opener, workers, database, token, destination, owner, timeout)
        if manifest is not None:
            return manifest
        expired.touch(mode=0o600)
        print(f'PinExpired for {destination}; ignore this cancelled archive.', file=sys.stderr, flush=True)
    raise RuntimeError('Five export attempts lost their pins. Check source cluster health before retrying.')


def export_attempt(opener, workers, database, token, destination, owner, timeout):
    request_id = str(uuid4())
    payload = {
        'request_id': request_id,
        'operation': 'archive.create',
        'payload': {'output': str(destination), 'owner': owner},
    }
    headers = {'Authorization': f'Bearer {token}', 'X-Nabedra-Database': database,
               'Content-Type': 'application/json'}
    errors = []
    timed_out = False
    for worker in workers:
        request = Request(worker.rstrip('/') + '/v1/cluster/operations',
                          data=json.dumps(payload).encode(), headers=headers)
        try:
            with opener.open(request, timeout=timeout) as response:
                reply = json.load(response)
            manifest = reply['data']
            if (reply.get('request_id') != request_id
                    or manifest['owner'] != owner
                    or manifest['key']['archive_id'] != destination.name):
                raise ValueError('unexpected archive manifest')
            return manifest
        except HTTPError as error:
            try:
                with error:
                    body = error.read(4096).decode(errors='replace')
            except (OSError, HTTPException) as read_error:
                timed_out |= isinstance(read_error, TimeoutError)
                errors.append(f'{worker}: HTTP {error.code}: {read_error}')
                continue
            errors.append(f'{worker}: HTTP {error.code}: {body}')
            if error.code == 409:
                try:
                    rejection = json.loads(body)
                except ValueError:
                    continue
                if rejection == {'request_id': request_id, 'error': {
                    'code': 'conflict', 'message': 'timestamp transition rejected: Rejected(PinExpired)',
                    'outcome': 'not_applied',
                }}:
                    return None
            # The old archive endpoint returns 503 "not accepting requests"
            # on non-owners. Try another worker with the same archive identity.
        except (URLError, OSError, HTTPException, ValueError, KeyError) as error:
            timed_out |= isinstance(error.reason if isinstance(error, URLError) else error, TimeoutError)
            errors.append(f'{worker}: {error}')
    if timed_out:
        errors.append(f'The coordinator may still be exporting. It is safe to rerun with the same '
                      f'--backup-id {owner["backup_id"]}.')
    raise RuntimeError('\n'.join(errors))


def nonzero_uuid(text):
    value = UUID(text)
    if not value.int:
        raise argparse.ArgumentTypeError('expected a nonzero UUID')
    return str(value)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--worker', action='append', required=True, help='worker admin URL, repeat for each worker')
    parser.add_argument('--database', required=True)
    parser.add_argument('--cluster-id', type=nonzero_uuid, required=True)
    parser.add_argument('--backup-id', type=nonzero_uuid, required=True, help='reuse this UUID after an interruption')
    parser.add_argument('--repository', type=Path, required=True, help='absolute archive_repository path on the workers')
    parser.add_argument('--ca-file', help='CA certificate for HTTPS admin URLs')
    parser.add_argument('--insecure', action='store_true', help='allow plain HTTP worker URLs on a protected tunnel')
    args = parser.parse_args()
    token = os.environ.get('NABEDRA_ADMIN_TOKEN')
    if not token or not args.repository.is_absolute():
        parser.error('set NABEDRA_ADMIN_TOKEN and supply the canonical absolute --repository path')
    try:
        manifest = export(args.worker, args.database, token, args.repository, args.cluster_id, args.backup_id,
                          ca_file=args.ca_file, insecure=args.insecure)
    except (RuntimeError, OSError, ValueError) as error:
        parser.exit(1, f'{error}\n')
    print(json.dumps(manifest))


if __name__ == '__main__':
    main()
