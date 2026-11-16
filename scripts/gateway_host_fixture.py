"""Finite loopback HTTP fixture; never use this HTTP server for deployment.

The collector bounds allocation while reading each stream, before Gateway.handle.
No web framework is imported. Chunked and declared-length bodies share the bound.
"""
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading

MAX_BODY = 8 * 1024 * 1024


class BodyLimit(ValueError):
    pass


def collect(chunks, limit=MAX_BODY):
    body = bytearray()
    for chunk in chunks:
        if len(chunk) > limit - len(body):
            raise BodyLimit('body limit')
        body.extend(chunk)
    return bytes(body)


def body_chunks(handler, limit=MAX_BODY):
    lengths = handler.headers.get_all('Content-Length', [])
    encodings = handler.headers.get_all('Transfer-Encoding', [])
    if encodings:
        if lengths or encodings != ['chunked']:
            raise ValueError('ambiguous framing')
        total = 0
        while True:
            line = handler.rfile.readline(128)
            if not line.endswith(b'\r\n') or len(line) >= 128:
                raise ValueError('chunk framing')
            size = int(line.strip(), 16)
            if size < 0:
                raise ValueError('chunk size')
            if size > limit - total:
                raise BodyLimit('body limit')
            if size == 0:
                if handler.rfile.read(2) != b'\r\n':
                    raise ValueError('trailers unsupported')
                return
            total += size
            while size:
                chunk = handler.rfile.read(min(size, 65536))
                if not chunk:
                    raise ValueError('truncated body')
                size -= len(chunk)
                yield chunk
            if handler.rfile.read(2) != b'\r\n':
                raise ValueError('chunk framing')
    else:
        if len(lengths) > 1:
            raise ValueError('duplicate length')
        size = int(lengths[0]) if lengths else 0
        if size < 0:
            raise ValueError('negative length')
        if size > limit:
            raise BodyLimit('body limit')
        while size:
            chunk = handler.rfile.read(min(size, 65536))
            if not chunk:
                raise ValueError('truncated body')
            size -= len(chunk)
            yield chunk


@contextmanager
def endpoint(callback, logs=None):
    """callback(method, target, header_pairs, raw_body) -> (status, headers, bytes)."""
    class Handler(BaseHTTPRequestHandler):
        protocol_version = 'HTTP/1.1'

        def setup(self):
            super().setup()
            self.connection.settimeout(3)

        def log_message(self, fmt, *args):
            # Status-only diagnostics exclude targets, headers, bodies and secrets.
            if logs is not None:
                logs.append('http request settled')

        def do_GET(self):
            self.dispatch()

        def do_POST(self):
            self.dispatch()

        def dispatch(self):
            try:
                body = collect(body_chunks(self))
                status, headers, raw = callback(self.command, self.path,
                    tuple(self.headers.raw_items()), body)
            except BodyLimit:
                status, headers, raw = 413, (), b'{"error":{"code":"body_too_large"}}'
            except (ValueError, TimeoutError):
                status, headers, raw = 400, (), b'{"error":{"code":"invalid_request"}}'
            self.send_response(status)
            if not any(k.lower() == 'content-type' for k, _ in headers):
                self.send_header('Content-Type', 'application/json')
            for key, value in headers:
                if key.lower() not in ('content-length', 'connection'):
                    self.send_header(key, value)
            self.send_header('Content-Length', str(len(raw)))
            self.send_header('Connection', 'close')
            self.end_headers()
            self.wfile.write(raw)
            self.close_connection = True

    class Server(ThreadingHTTPServer):
        daemon_threads = False
        block_on_close = True

    server = Server(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.serve_forever, kwargs={'poll_interval': .05})
    thread.start()
    try:
        yield 'http://127.0.0.1:' + str(server.server_port)
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
        assert not thread.is_alive(), 'HTTP fixture failed to settle'
