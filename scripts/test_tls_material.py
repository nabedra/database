"""Disposable local TLS fixtures. Importing this module creates nothing."""

import shutil
import subprocess
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class TLSMaterial:
    server_cert: Path
    server_key: Path
    ca_cert: Path


class TLSMaterialError(RuntimeError):
    """A fixture preparation failure without OpenSSL output or key contents."""


_CONFIG = """\
[req]
prompt = no
distinguished_name = subject
[subject]
CN = localhost
[ca_ext]
basicConstraints = critical,CA:TRUE,pathlen:0
keyUsage = critical,keyCertSign,cRLSign
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid:always
[server_ext]
basicConstraints = critical,CA:FALSE
keyUsage = critical,digitalSignature,keyEncipherment
extendedKeyUsage = serverAuth
subjectAltName = IP:127.0.0.1,DNS:localhost
subjectKeyIdentifier = hash
authorityKeyIdentifier = keyid:always
"""


def _run(openssl: str, *args: str) -> None:
    try:
        subprocess.run(
            [openssl, *args], check=True, timeout=30,
            stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
    except (OSError, subprocess.SubprocessError):
        raise TLSMaterialError("OpenSSL could not prepare test TLS material.") from None


def create_test_tls(directory: Path) -> TLSMaterial:
    """Create a two-day test CA and loopback server certificate in a new directory.

    The parent must exist. Existing destinations raise FileExistsError without
    modification. The caller owns all generated files, including any partial
    fixture left after failure, and must clean up its own temporary directory.
    """
    directory = directory.absolute()
    try:
        directory.mkdir(mode=0o700)
    except FileExistsError:
        raise FileExistsError("Test TLS destination already exists.") from None
    except OSError:
        raise TLSMaterialError("Cannot create test TLS directory.") from None

    openssl = shutil.which("openssl") or shutil.which("/opt/local/bin/openssl")
    if openssl is None:
        raise TLSMaterialError("OpenSSL is required for test TLS material.")

    ca_cert = directory / "ca.crt"
    ca_key = directory / "ca.key"
    server_cert = directory / "server.crt"
    server_key = directory / "server.key"
    csr = directory / "server.csr"
    config = directory / "openssl.cnf"
    try:
        directory.chmod(0o700)
        with config.open("x", encoding="ascii") as handle:
            handle.write(_CONFIG)
        # Precreate keys privately so OpenSSL never writes a world-readable key.
        for key in (ca_key, server_key):
            key.touch(mode=0o600, exist_ok=False)
            key.chmod(0o600)
    except OSError:
        raise TLSMaterialError("Cannot write test TLS fixture files.") from None

    _run(openssl, "req", "-new", "-x509", "-newkey", "rsa:2048", "-nodes",
         "-sha256", "-days", "2", "-subj", "/CN=meshdb disposable test CA",
         "-config", str(config), "-extensions", "ca_ext",
         "-keyout", str(ca_key), "-out", str(ca_cert))
    _run(openssl, "req", "-new", "-newkey", "rsa:2048", "-nodes", "-sha256",
         "-config", str(config), "-keyout", str(server_key), "-out", str(csr))
    _run(openssl, "x509", "-req", "-in", str(csr), "-CA", str(ca_cert),
         "-CAkey", str(ca_key), "-set_serial", "2", "-days", "2", "-sha256",
         "-extfile", str(config), "-extensions", "server_ext",
         "-out", str(server_cert))
    return TLSMaterial(server_cert=server_cert, server_key=server_key, ca_cert=ca_cert)
