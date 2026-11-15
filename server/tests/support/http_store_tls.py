"""Owned loopback-only TLS fixture. All output is bounded startup JSON."""
import json
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
import ssl
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[3] / 'scripts'))
from test_tls_material import create_test_tls

material = create_test_tls(Path(sys.argv[1]) / 'tls')


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *args):
        pass

    def do_GET(self):
        self.send_response(200)
        self.send_header('ETag', '"opaque-tls"')
        self.send_header('Content-Length', '2')
        self.end_headers()
        self.wfile.write(b'ok')


with HTTPServer(('127.0.0.1', 0), Handler) as server:
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(material.server_cert, material.server_key)
    server.socket = context.wrap_socket(server.socket, server_side=True)
    print(json.dumps({'port': server.server_port}), flush=True)
    server.serve_forever()
