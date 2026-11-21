#!/usr/bin/env python3
"""Export a full OLTP archive from a RocksDB-era cluster before upgrading."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import ssl
import sys
from urllib.error import HTTPError, URLError
from urllib.request import HTTPRedirectHandler, HTTPSHandler, Request, build_opener
from uuid import UUID, uuid4


def archive_path(repository, cluster_id, backup_id):
    # The old archive.create endpoint requires this exact destination. Match
    # native_archive_id's SHA-256 over the compact serde JSON tuple.
    identity = [list(b'nabedra/native-backup-id/v1'), cluster_id, {'backup_id': backup_id, 'attempt': 1}]
    digest = hashlib.sha256(json.dumps(identity, separators=(',', ':')).encode()).digest()
    return repository / str(UUID(bytes=digest[:16]))


class NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def export(workers, database, token, destination, backup_id, *, ca_file=None, timeout=3600):
    context = ssl.create_default_context(cafile=ca_file)
    opener = build_opener(NoRedirect(), HTTPSHandler(context=context))
    request_id = str(uuid4())
    payload = {
        'request_id': request_id,
        'operation': 'archive.create',
        'payload': {'output': str(destination), 'owner': {'backup_id': backup_id, 'attempt': 1}},
    }
    headers = {'Authorization': f'Bearer {token}', 'X-Nabedra-Database': database,
               'Content-Type': 'application/json'}
    errors = []
    for worker in workers:
        request = Request(worker.rstrip('/') + '/v1/cluster/operations',
                          data=json.dumps(payload).encode(), headers=headers)
        try:
            with opener.open(request, timeout=timeout) as response:
                reply = json.load(response)
            manifest = reply['data']
            if (reply.get('request_id') != request_id
                    or manifest['owner'] != payload['payload']['owner']
                    or manifest['key']['archive_id'] != destination.name):
                raise ValueError('unexpected archive manifest')
            return manifest
        except HTTPError as error:
            with error:
                errors.append(f'{worker}: HTTP {error.code}: {error.read(4096).decode(errors="replace")}')
        except (URLError, OSError, ValueError, KeyError) as error:
            errors.append(f'{worker}: {error}')
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
    args = parser.parse_args()
    token = os.environ.get('NABEDRA_ADMIN_TOKEN')
    if not token or not args.repository.is_absolute():
        parser.error('set NABEDRA_ADMIN_TOKEN and supply the canonical absolute --repository path')
    destination = archive_path(args.repository, args.cluster_id, args.backup_id)
    print(f'Export destination on the workers: {destination}', file=sys.stderr, flush=True)
    try:
        manifest = export(args.worker, args.database, token, destination, args.backup_id, ca_file=args.ca_file)
    except RuntimeError as error:
        parser.exit(1, f'{error}\n')
    print(json.dumps(manifest))


if __name__ == '__main__':
    main()
