"""Small, standard-library helpers for acceptance tests against supplied S3 storage.

No fixture cleanup is provided. Keep returned environments private: they contain
credentials and a bearer token despite their redacted repr.
"""

import os
import re
import secrets
import shutil
import subprocess
import sys
from dataclasses import dataclass, field
from pathlib import Path


class ConfigError(ValueError):
    """A configuration error whose message contains no supplied values."""


class _PrivateEnv(dict):
    def __repr__(self):
        return "<private acceptance environment>"

    __str__ = __repr__


def _assignments(text):
    values = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        match = re.fullmatch(r"(?:export\s+)?([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)", line)
        if not match:
            raise ConfigError("Dotenv must contain assignments or comments only.")
        key, raw = match.groups()
        if key in values:
            raise ConfigError("Dotenv contains a duplicate assignment.")
        if raw.startswith(("'", '"')):
            quote = raw[0]
            pattern = r"'([^']*)'(.*)" if quote == "'" else r'"((?:[^"\\]|\\.)*)"(.*)'
            quoted = re.fullmatch(pattern, raw)
            if not quoted:
                raise ConfigError("Dotenv contains an invalid quoted assignment.")
            value, tail = quoted.groups()
            if tail.strip() and not tail.lstrip().startswith("#"):
                raise ConfigError("Dotenv has content after a quoted assignment.")
            if quote == '"':
                value = re.sub(r'\\(["\\$`])', r"\1", value)
        else:
            value = "" if raw.startswith("#") else re.split(r"\s+#", raw, maxsplit=1)[0].strip()
        values[key] = value
    return values


def _segment(value):
    if not isinstance(value, str) or not re.fullmatch(r"[a-z0-9]+(?:-[a-z0-9]+)*", value) or len(value) > 64:
        raise ConfigError("Test name and run ID must be lowercase alphanumeric segments with internal hyphens, at most 64 characters.")
    return value


@dataclass(frozen=True)
class TestNamespace:
    prefix: str
    _owner: object = field(repr=False)

    def key(self, relative_path):
        """Return a key beneath this prefix; never accepts an absolute S3 key."""
        if not isinstance(relative_path, str) or not relative_path:
            raise ConfigError("An object path must be a nonempty relative path.")
        parts = relative_path.split("/")
        if any(part in (".", "..") or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", part) for part in parts):
            raise ConfigError("Object path contains an unsafe segment.")
        self._owner._check_namespace(self)
        return self.prefix + relative_path


@dataclass(frozen=True)
class PreflightResult:
    success: bool
    bucket: str
    region: str
    error: str = ""


@dataclass(frozen=True)
class AcceptanceConfig:
    bucket: str
    region: str
    _aws: dict = field(repr=False)
    _token: str = field(default="", repr=False)
    _prefixes: set = field(default_factory=set, repr=False)

    def allocate_prefix(self, number, test_name, *, run_id=None):
        """Allocate one namespace. Keep it to reuse fixtures after restart."""
        if type(number) is not int or not 1 <= number <= 9999:
            raise ConfigError("Test number must be an integer between 1 and 9999.")
        prefix = f"test-{number}-{_segment(test_name)}-{_segment(run_id if run_id is not None else secrets.token_hex(6))}/"
        if prefix in self._prefixes:
            raise ConfigError("This test prefix has already been allocated.")
        self._prefixes.add(prefix)
        return TestNamespace(prefix, self)

    def _check_namespace(self, namespace):
        if not isinstance(namespace, TestNamespace) or namespace._owner is not self or namespace.prefix not in self._prefixes:
            raise ConfigError("Use a namespace allocated by this configuration.")

    def _aws_env(self):
        # Ignore ambient profiles, endpoint overrides, credential files and debug
        # settings. The supplied region and credentials are the only AWS inputs.
        return _PrivateEnv({
            "PATH": os.defpath,
            "AWS_CONFIG_FILE": os.devnull,
            "AWS_SHARED_CREDENTIALS_FILE": os.devnull,
            "AWS_EC2_METADATA_DISABLED": "true",
            "AWS_MAX_ATTEMPTS": "1",
            "AWS_PAGER": "",
            "AWS_CLI_AUTO_PROMPT": "off",
            **self._aws,
        })

    def server_env(self, namespace):
        """Return a private child env fragment; caller supplies launch settings."""
        self._check_namespace(namespace)
        if not self._token:
            object.__setattr__(self, "_token", secrets.token_urlsafe(32))
        env = self._aws_env()
        env.update(MESHDB_S3_BUCKET=self.bucket, MESHDB_S3_PREFIX=namespace.prefix, MESHDB_TOKEN=self._token)
        return env

    def preflight(self):
        """Make one read-only head-bucket request, with no identity lookup."""
        executable = shutil.which("aws")
        error = ""
        if executable is None:
            error = "Install the AWS CLI and retry the read-only preflight."
        else:
            try:
                result = subprocess.run(
                    [executable, "s3api", "head-bucket", "--bucket", self.bucket,
                     "--region", self.region, "--cli-connect-timeout", "5", "--cli-read-timeout", "10"],
                    env=self._aws_env(), stdout=subprocess.DEVNULL,
                    stderr=subprocess.PIPE, timeout=20, check=False,
                )
                if result.returncode:
                    # Never return CLI stderr: it may contain identifiers or
                    # credentials. Keep only a fixed actionable diagnostic.
                    denied = any(code in result.stderr.lower() for code in (b"403", b"forbidden", b"accessdenied", b"invalidaccesskey", b"signaturedoesnotmatch", b"expiredtoken"))
                    error = ("Check supplied credentials and s3:ListBucket permission on the configured bucket."
                             if denied else "Check supplied bucket, region, credentials and network access; head-bucket failed.")
            except (OSError, subprocess.SubprocessError):
                error = "Check AWS CLI availability and network access; head-bucket did not complete."
        return PreflightResult(not error, self.bucket, self.region, error)


def load_config(dotenv_path=None):
    """Read UTF-8 dotenv assignments literally, without shell evaluation.

    Supports export, whitespace, single/double quotes and trailing comments.
    No expansion, multiline assignments or shell commands. BUCKET_NAME wins;
    ambient AWS settings and dotenv MESHDB_* overrides are ignored.
    """
    path = Path(dotenv_path) if dotenv_path is not None else Path(__file__).resolve().parents[1] / ".env"
    try:
        values = _assignments(path.read_text(encoding="utf-8-sig"))
    except (OSError, UnicodeError):
        raise ConfigError("Cannot read the dotenv file as UTF-8.") from None
    required = ("AWS_REGION", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "BUCKET_NAME")
    if any(not values.get(key) or any(ord(char) < 32 or ord(char) == 127 for char in values[key]) for key in required):
        raise ConfigError("Dotenv requires nonempty AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY and BUCKET_NAME without control characters.")
    bucket, region = values["BUCKET_NAME"], values["AWS_REGION"]
    if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", bucket) or ".." in bucket:
        raise ConfigError("BUCKET_NAME must be an ordinary S3 bucket name.")
    if not re.fullmatch(r"[a-z]{2}(?:-[a-z]+)+-[0-9]+", region):
        raise ConfigError("AWS_REGION must be an AWS region name.")
    aws = {key: values[key] for key in required if key != "BUCKET_NAME"}
    if values.get("AWS_SESSION_TOKEN"):
        if any(ord(char) < 32 or ord(char) == 127 for char in values["AWS_SESSION_TOKEN"]):
            raise ConfigError("AWS_SESSION_TOKEN must not contain control characters.")
        aws["AWS_SESSION_TOKEN"] = values["AWS_SESSION_TOKEN"]
    return AcceptanceConfig(bucket, region, _PrivateEnv(aws))


if __name__ == "__main__":
    try:
        outcome = load_config().preflight()
        print(f"{'success' if outcome.success else 'failure'} region={outcome.region} bucket={outcome.bucket}")
        if outcome.error:
            print(outcome.error)
        sys.exit(0 if outcome.success else 1)
    except ConfigError as failure:
        print(f"failure: {failure}")
        sys.exit(1)
