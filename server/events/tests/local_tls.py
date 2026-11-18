"""Short-lived CA and client/server certificates for Docker mTLS acceptance."""

import json
from pathlib import Path

from process_fixture import command


class Certificates:
    def __init__(self, root, count):
        self.root = root
        self.root.mkdir(mode=0o755)
        ca_key = root / "ca-key.pem"
        ca = root / "ca.pem"
        ca_config = root / "ca.cnf"
        ca_config.write_text(
            "[req]\nprompt=no\ndistinguished_name=subject\nx509_extensions=ca\n"
            "[subject]\nCN=Events acceptance CA\n"
            "[ca]\nbasicConstraints=critical, CA:TRUE\n"
            "keyUsage=critical,keyCertSign,cRLSign\nsubjectKeyIdentifier=hash\n"
        )
        command(
            "openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-sha256",
            "-days", "1", "-config", str(ca_config), "-keyout", str(ca_key), "-out", str(ca),
        )
        self.nodes = {}
        for broker in range(1, count + 1):
            path = root / f"broker-{broker}"
            path.mkdir(mode=0o755)
            self.nodes[broker] = path
            extensions = path / "extensions.cnf"
            extensions.write_text(
                "basicConstraints=critical,CA:FALSE\n"
                "keyUsage=critical,digitalSignature,keyEncipherment\n"
                "extendedKeyUsage=serverAuth,clientAuth\n"
                f"subjectAltName=DNS:broker-{broker},IP:127.0.0.1\n"
            )
            request = path / "cert.csr"
            private_key, certificate = path / "key.pem", path / "cert.pem"
            command(
                "openssl", "req", "-new", "-newkey", "rsa:2048", "-nodes",
                "-subj", f"/CN=broker-{broker}", "-keyout", str(private_key), "-out", str(request),
            )
            command(
                "openssl", "x509", "-req", "-sha256", "-days", "1", "-in", str(request),
                "-CA", str(ca), "-CAkey", str(ca_key), "-set_serial", str(broker),
                "-extfile", str(extensions), "-out", str(certificate),
            )
            (path / "ca.pem").write_bytes(ca.read_bytes())
            (path / "identity.pem").write_bytes(certificate.read_bytes() + private_key.read_bytes())
            # The fixture's owner-only root is bound to a container running with
            # a different UID. These throwaway certificates expire in one day.
            for filename in ("key.pem", "cert.pem", "ca.pem", "identity.pem"):
                (path / filename).chmod(0o644)
            extensions.unlink()
            request.unlink()

    @staticmethod
    def config(_, settings):
        tls_path = Path("/etc/nabedra-events/tls")
        settings["allow_plaintext"] = False
        settings["http_limits"] = {"lease_ms": 120_000}
        settings["peer_ca"] = str(tls_path / "ca.pem")
        settings["peer_identity"] = str(tls_path / "identity.pem")
        tls = {"cert": str(tls_path / "cert.pem"), "key": str(tls_path / "key.pem")}
        settings["public_tls"] = settings["peer_tls"] = tls
        for node in settings["bootstrap"]["brokers"]:
            node["peer_url"] = node["peer_url"].replace("http:", "https:")

    def mount(self, cluster):
        for broker, path in self.nodes.items():
            service = cluster.compose["services"][cluster.service(broker)]
            service["volumes"].append(f"{path}:/etc/nabedra-events/tls:ro")
            service["ports"].append("127.0.0.1::8081")
        cluster.compose_file.write_text(json.dumps(cluster.compose))
