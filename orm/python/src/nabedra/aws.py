"""Optional lazy AWS credential discovery for trusted server integrations."""
from __future__ import annotations
from datetime import datetime, timezone
from dataclasses import dataclass, field
import threading

@dataclass(frozen=True, repr=False)
class AwsCredentials:
    access_key_id: str = field(repr=False)
    secret_access_key: str = field(repr=False)
    session_token: str | None = field(default=None, kw_only=True, repr=False)
    expires_at: datetime | None = field(default=None, kw_only=True)

    def __post_init__(self):
        for value in (self.access_key_id, self.secret_access_key):
            if not isinstance(value, str) or not value or len(value) > 4096 or any(ord(c) < 32 or ord(c) > 126 for c in value):
                raise ValueError('Invalid AWS credentials')
        if self.session_token is not None and (not isinstance(self.session_token, str) or not self.session_token or len(self.session_token) > 8192 or any(ord(c) < 32 or ord(c) > 126 for c in self.session_token)):
            raise ValueError('Invalid AWS credentials')
        expiry = self.expires_at
        if expiry is not None and (not isinstance(expiry, datetime) or expiry.tzinfo is None or expiry.utcoffset() != timezone.utc.utcoffset(expiry)):
            raise ValueError('Credential expiry must be a UTC datetime')

    def __repr__(self):
        return 'AwsCredentials(<redacted>)'


def default_credentials(profile=None):
    """Return a lazy provider backed by one persistent Boto3 session.

    Construction imports no SDK and resolves no credentials. Each call obtains
    one frozen SDK snapshot, including its session token and known expiration.
    """
    lock = threading.Lock()
    session = None
    def provider():
        nonlocal session
        try:
            with lock:
                if session is None:
                    import boto3
                    session = boto3.Session(profile_name=profile)
                credentials = session.get_credentials()
                if credentials is None:
                    raise ValueError
                # Hold the SDK refresh lock across both snapshot and expiry.
                # get_frozen_credentials performs refresh under that lock itself.
                frozen = credentials.get_frozen_credentials()
                refresh_lock = getattr(credentials, '_refresh_lock', None)
                if refresh_lock is not None:
                    with refresh_lock:
                        frozen = credentials._frozen_credentials
                        expiry = credentials._expiry_time
                else:
                    expiry = None
                if expiry is not None:
                    expiry = expiry.astimezone(timezone.utc)
                return AwsCredentials(frozen.access_key, frozen.secret_key,
                                      session_token=frozen.token, expires_at=expiry)
        except Exception:
            raise ValueError('AWS credential discovery failed') from None
    return provider
