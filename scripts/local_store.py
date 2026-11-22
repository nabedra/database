#!/usr/bin/env python3.11
"""Local-disk conditional object service for development and acceptance runs.

PUT requires Content-Length. Bodyless GET accepts an absent length or zero.
Keys use two or more slash-separated, nonempty ASCII unreserved components.
Run on trusted local networks only: bearer authentication does not provide TLS.

With --test-faults only, the store honours the request header
X-Nabedra-Test-Fault, so adapter tests meet faults over real HTTP:
  lost-response     apply the PUT, then close the connection without a reply
  delay-apply=<ms>  close without a reply now, apply the PUT <ms> later (the
                    condition is checked when it applies)
  status=<code>     reply <code> and apply nothing; status=200 on PUT replies an
                    S3-style embedded error body
Never pass --test-faults outside tests.
"""
import argparse
from contextlib import closing
import hmac
from http.server import BaseHTTPRequestHandler
import json
import os
import queue
import re
import signal
import socket
import socketserver
import sqlite3
import sys
import threading
import time

MAX_BODY = 80 * 1024 * 1024
MAX_KEY = 4096
WORKERS = 4
QUEUE_SIZE = 8
REQUEST_SECONDS = 15
ETAG = re.compile(r'"[\x21\x23-\x7e\x80-\xff]*"')
COMPONENT = re.compile(r'[A-Za-z0-9_.~-]+')
FAULT = re.compile(r'lost-response|delay-apply=[0-9]{1,6}|status=[1-5][0-9][0-9]')
EMBEDDED_ERROR = (b'<?xml version="1.0" encoding="UTF-8"?>\n'
                  b'<Error><Code>InternalError</Code><Message>embedded</Message></Error>')


def connect(database):
    connection = sqlite3.connect(database, timeout=5, isolation_level=None)
    try:
        connection.execute('PRAGMA synchronous=FULL')
        connection.execute('PRAGMA busy_timeout=5000')
        return connection
    except BaseException:
        connection.close()
        raise


def initialize(database):
    with closing(connect(database)) as connection:
        # A rollback journal and FULL sync retain acknowledged transactions on
        # local disks that honor SQLite's fsync calls. No network filesystem.
        connection.execute('PRAGMA journal_mode=DELETE')
        connection.execute('BEGIN IMMEDIATE')
        try:
            connection.execute('CREATE TABLE IF NOT EXISTS generation '
                               '(singleton INTEGER PRIMARY KEY CHECK(singleton=1), value INTEGER NOT NULL)')
            connection.execute('INSERT OR IGNORE INTO generation VALUES (1, 0)')
            connection.execute('CREATE TABLE IF NOT EXISTS objects '
                               '(key TEXT PRIMARY KEY, body BLOB NOT NULL, generation INTEGER NOT NULL)')
            connection.commit()
        except BaseException:
            connection.rollback()
            raise


def tag(generation):
    return f'"g{generation}"'


def close_socket(sock):
    try:
        sock.shutdown(socket.SHUT_RDWR)
    except OSError:
        pass
    sock.close()


class StoreServer(socketserver.TCPServer):
    allow_reuse_address = True
    request_queue_size = QUEUE_SIZE

    def __init__(self, address, database, token, test_faults=False):
        self.database = database
        self.test_faults = test_faults
        self.token = token.encode('ascii')
        self.stopping = threading.Event()
        self.pending = queue.Queue(maxsize=QUEUE_SIZE)
        self.workers = []
        super().__init__(address, Handler)
        self.timeout = 0.2
        self.workers = [threading.Thread(target=self.worker, name=f'store-{i}')
                        for i in range(WORKERS)]
        for worker in self.workers:
            worker.start()

    def process_request(self, request, client_address):
        try:
            self.pending.put_nowait((request, client_address, time.monotonic()))
        except queue.Full:
            close_socket(request)

    def worker(self):
        while True:
            item = self.pending.get()
            if item is None:
                return
            request, address, accepted = item
            remaining = REQUEST_SECONDS - (time.monotonic() - accepted)
            if self.stopping.is_set() or remaining <= 0:
                close_socket(request)
                continue
            request.settimeout(remaining)
            # Socket timeouts alone allow a slow trickle to live indefinitely.
            timer = threading.Timer(remaining, close_socket, args=(request,))
            timer.start()
            try:
                self.finish_request(request, address)
            except Exception:
                # Do not log parser input, authorization, keys or object bytes.
                pass
            finally:
                timer.cancel()
                timer.join()
                close_socket(request)

    def server_close(self):
        self.stopping.set()
        super().server_close()
        for _ in self.workers:
            self.pending.put(None)
        for worker in self.workers:
            worker.join()


class Handler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def log_message(self, format, *args):
        pass

    def send_error(self, code, message=None, explain=None):
        self.reply(code)

    def reply(self, status, body=b'', etag=None):
        self.close_connection = True
        self.send_response(status)
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Connection', 'close')
        if etag is not None:
            self.send_header('ETag', etag)
        self.end_headers()
        if body:
            self.wfile.write(body)

    def handle_expect_100(self):
        # Validation must precede any invitation to transmit the body.
        return True

    def validate(self):
        parts = self.raw_requestline.rstrip(b'\r\n').split(b' ')
        if len(parts) != 3 or any(c < 33 or c > 126 for c in parts[1]):
            self.reply(400)
            return None
        target = parts[1].decode('ascii')
        if any(c in target for c in ('%', '?', '#', '\\')):
            self.reply(400)
            return None
        lengths = self.headers.get_all('Content-Length', [])
        if self.headers.get_all('Transfer-Encoding'):
            self.reply(400)
            return None
        if not lengths and self.command == 'GET':
            lengths = ['0']
        if not lengths:
            self.reply(411)
            return None
        if len(lengths) != 1 or re.fullmatch(r'[0-9]{1,10}', lengths[0]) is None:
            self.reply(400)
            return None
        length = int(lengths[0])
        if length > MAX_BODY:
            self.reply(413)
            return None
        if self.command == 'GET' and length != 0:
            self.reply(400)
            return None
        if target in ('/health/live', '/health/ready') and self.command == 'GET':
            if target.endswith('/ready'):
                try:
                    with closing(connect(self.server.database)) as connection:
                        connection.execute('SELECT value FROM generation WHERE singleton=1').fetchone()
                except sqlite3.Error:
                    self.reply(503)
                    return None
            self.reply(200)
            return None
        auth = self.headers.get_all('Authorization', [])
        expected = b'Bearer ' + self.server.token
        if len(auth) != 1 or not hmac.compare_digest(auth[0].encode('utf-8'), expected):
            self.reply(401)
            return None
        prefix = '/v1/objects/'
        if not target.startswith(prefix):
            self.reply(404)
            return None
        key = target[len(prefix):]
        if len(key) > MAX_KEY:
            self.reply(414)
            return None
        components = key.split('/')
        if len(components) < 2 or any(part in ('.', '..') or COMPONENT.fullmatch(part) is None
                                      for part in components):
            self.reply(400)
            return None
        return key, length

    def fault(self):
        """Return the requested test fault, '' for a malformed one, or None."""
        values = self.headers.get_all('X-Nabedra-Test-Fault', [])
        if not values or not self.server.test_faults:
            return None
        if len(values) != 1 or FAULT.fullmatch(values[0]) is None:
            return ''
        return values[0]

    def do_GET(self):
        validated = self.validate()
        if validated is None:
            return
        key, _ = validated
        fault = self.fault()
        if fault == '' or (fault is not None and fault.startswith('delay-apply=')):
            self.reply(400)
            return
        if fault == 'lost-response':
            self.close_connection = True
            return
        if fault is not None:
            self.reply(int(fault.split('=', 1)[1]))
            return
        with closing(connect(self.server.database)) as connection:
            # One SELECT reads both bytes and generation from one snapshot.
            row = connection.execute('SELECT body, generation FROM objects WHERE key=?', (key,)).fetchone()
        if row is None:
            self.reply(404)
        else:
            self.reply(200, row[0], tag(row[1]))

    def do_PUT(self):
        validated = self.validate()
        if validated is None:
            return
        key, length = validated
        fault = self.fault()
        if fault == '':
            self.reply(400)
            return
        create = self.headers.get_all('If-None-Match', [])
        match = self.headers.get_all('If-Match', [])
        if not ((create == ['*'] and not match) or
                (not create and len(match) == 1 and ETAG.fullmatch(match[0]) is not None)):
            self.reply(400)
            return
        expectations = self.headers.get_all('Expect', [])
        if expectations:
            if len(expectations) != 1 or expectations[0].lower() != '100-continue':
                self.reply(417)
                return
            self.send_response_only(100)
            self.end_headers()
        body = self.rfile.read(length)
        if len(body) != length:
            self.reply(400)
            return
        if fault == 'lost-response':
            apply_put(self.server.database, key, body, create, match)
            self.close_connection = True
            return
        if fault is not None and fault.startswith('delay-apply='):
            delay = int(fault.split('=', 1)[1]) / 1000
            timer = threading.Timer(delay, late_put, args=(self.server.database, key, body, create, match))
            timer.daemon = True
            timer.start()
            self.close_connection = True
            return
        if fault is not None:
            code = int(fault.split('=', 1)[1])
            self.reply(code, EMBEDDED_ERROR if code == 200 else b'')
            return
        generation = apply_put(self.server.database, key, body, create, match)
        if generation is None:
            self.reply(412)
            return
        self.reply(204, etag=tag(generation))


def apply_put(database, key, body, create, match):
    """Apply one conditional PUT. Return the new generation, or None on a failed condition."""
    with closing(connect(database)) as connection:
        connection.execute('BEGIN IMMEDIATE')
        try:
            current = connection.execute('SELECT generation FROM objects WHERE key=?', (key,)).fetchone()
            if (create and current is not None) or (match and (current is None or tag(current[0]) != match[0])):
                connection.rollback()
                return None
            generation = connection.execute('SELECT value FROM generation WHERE singleton=1').fetchone()[0] + 1
            connection.execute('UPDATE generation SET value=? WHERE singleton=1', (generation,))
            # The condition and this write share BEGIN IMMEDIATE. Avoid
            # UPSERT syntax so the helper also runs with SQLite < 3.24.
            if current is None:
                connection.execute('INSERT INTO objects VALUES (?, ?, ?)',
                                   (key, body, generation))
            else:
                connection.execute('UPDATE objects SET body=?, generation=? WHERE key=?',
                                   (body, generation, key))
            connection.commit()
            return generation
        except BaseException:
            # In particular, a COMMIT error may mean the write applied.
            # Close without an HTTP result; the client must reconcile by GET.
            connection.rollback()
            raise


def late_put(database, key, body, create, match):
    try:
        apply_put(database, key, body, create, match)
    except (OSError, sqlite3.Error):
        pass


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--database', required=True, help='SQLite file on local disk')
    parser.add_argument('--host', default='127.0.0.1')
    parser.add_argument('--port', type=int, default=9000)
    parser.add_argument('--test-faults', action='store_true',
                        help='honour X-Nabedra-Test-Fault request headers (tests only)')
    args = parser.parse_args()
    token = os.environ.get('NABEDRA_STORE_TOKEN', '')
    if not token or any(ord(c) < 33 or ord(c) > 126 for c in token):
        print('NABEDRA_STORE_TOKEN must be a nonempty printable ASCII bearer token.', file=sys.stderr)
        return 2
    if not args.database or args.database == ':memory:' or not 0 <= args.port <= 65535:
        print('A local database file and a valid TCP port are required.', file=sys.stderr)
        return 2
    try:
        initialize(args.database)
        with StoreServer((args.host, args.port), args.database, token, args.test_faults) as server:
            def stop(signum, frame):
                server.stopping.set()
            signal.signal(signal.SIGTERM, stop)
            signal.signal(signal.SIGINT, stop)
            host, port = server.server_address[:2]
            print(json.dumps({'host': host, 'port': port}), flush=True)
            while not server.stopping.is_set():
                server.handle_request()
        return 0
    except (OSError, sqlite3.Error, ValueError):
        print('Local store startup or serving failed.', file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
