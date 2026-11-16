"""Configured infrastructure adapters for the synchronous serve callbacks.

KubernetesProvider requires an image containing Python 3 and nabedra-database-server,
network access from Pods to the configured API/storage URLs, and explicit RBAC
for Pods and Secrets in one namespace. No infrastructure is created on import.
"""
from __future__ import annotations

import base64
import copy
import hashlib
import functools
import threading
import time
import json
import re
import ssl
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode, urlsplit
from urllib.request import Request, build_opener, HTTPSHandler, HTTPRedirectHandler
from uuid import UUID

from .errors import ValidationError


class ProviderError(RuntimeError):
    """Infrastructure was unavailable or returned inconsistent ownership."""


def _bounded_call(method):
    @functools.wraps(method)
    def call(self, *args, **kwargs):
        previous = getattr(self._calls, 'until', None)
        self._calls.until = min(previous, time.monotonic() + self._timeout) if previous else time.monotonic() + self._timeout
        try:
            return method(self, *args, **kwargs)
        finally:
            self._calls.until = previous
    return call


class _NoRedirect(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


_POD_RUNNER = """import json, os
with open('/nabedra/bootstrap.json') as source:
    bootstrap = json.load(source)
address = os.environ['NABEDRA_POD_IP']
if ':' in address:
    address = '[' + address + ']'
port = int(bootstrap['listen'].rsplit(':', 1)[1])
bootstrap['advertise_url'] = ('https' if bootstrap['tls'] else 'http') + '://' + address + ':' + str(port)
path = '/tmp/nabedra-worker-bootstrap.json'
with os.fdopen(os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600), 'w') as target:
    json.dump(bootstrap, target)
os.execvp('nabedra-database-server', ['nabedra-database-server', 'worker', '--bootstrap', path])
"""


class KubernetesProvider:
    """Provision exact launch identities and delete only their recorded Pod UID.

    Pass provider.start_node, provider.stop_node and provider.list_nodes to serve.
    Each launch retains a Secret containing bootstrap and lifecycle evidence.
    Retained Secrets let a replacement controller reconcile a completed delete.
    The adapter never deletes a Secret automatically.
    New launches use nabedra identities. Existing meshdb Kubernetes identities
    remain readable with their original lifecycle annotations and Pod UID.
    """

    def __init__(self, *, api_url: str, token: str, namespace: str,
                 cluster_id: str, image: str, ca_file: str | None = None,
                 pod_spec: dict | None = None, volume_mounts: list[dict] | None = None, timeout: float = 10):
        parsed = urlsplit(api_url)
        if (parsed.scheme != 'https' or not parsed.hostname or parsed.username or
                parsed.password or parsed.query or parsed.fragment or parsed.path not in ('', '/')):
            raise ValidationError('Kubernetes api_url must be an HTTPS origin')
        if (not isinstance(namespace, str) or len(namespace) > 63 or
                not re.fullmatch(r'[a-z0-9](?:[-a-z0-9]*[a-z0-9])?', namespace)):
            raise ValidationError('Invalid Kubernetes namespace')
        try:
            valid_id = str(UUID(cluster_id)) == cluster_id
        except (ValueError, TypeError, AttributeError):
            valid_id = False
        if not valid_id or not isinstance(token, str) or not token or any(c.isspace() for c in token):
            raise ValidationError('Explicit cluster identity and Kubernetes token are required')
        if not isinstance(image, str) or not image or type(timeout) not in (int, float) or not 0 < timeout <= 30:
            raise ValidationError('Kubernetes image and timeout are required')
        if (pod_spec is not None and type(pod_spec) is not dict) or (volume_mounts is not None and type(volume_mounts) is not list):
            raise ValidationError('Kubernetes pod_spec and volume_mounts must be an object and a list')
        self.cluster_id, self.namespace = cluster_id, namespace
        self._origin, self._token, self._image, self._timeout = api_url.rstrip('/'), token, image, timeout
        self._calls = threading.local()
        self._pod_spec = copy.deepcopy(pod_spec or {})
        self._volume_mounts = copy.deepcopy(volume_mounts or [])
        if any(key in self._pod_spec for key in ('containers', 'initContainers', 'restartPolicy')):
            raise ValidationError('pod_spec cannot override worker containers or restart policy')
        if (not isinstance(self._volume_mounts, list) or
                any(not isinstance(v, dict) or v.get('name') == 'bootstrap' or
                    not isinstance(v.get('mountPath'), str) or v['mountPath'] == '/' or
                    v['mountPath'].rstrip('/') == '/nabedra' or v['mountPath'].startswith('/nabedra/')
                    for v in self._volume_mounts) or
                not isinstance(self._pod_spec.get('volumes', []), list) or
                any(not isinstance(v, dict) or v.get('name') == 'bootstrap' for v in self._pod_spec.get('volumes', []))):
            raise ValidationError('Credential mounts cannot override the bootstrap volume')
        self._http = build_opener(_NoRedirect(), HTTPSHandler(context=ssl.create_default_context(cafile=ca_file)))
        self._base = '/api/v1/namespaces/' + quote(namespace, safe='')

    def _request(self, method, path, body=None, *, missing=False, conflict=False):
        raw = None if body is None else json.dumps(body, separators=(',', ':')).encode()
        if raw is not None and len(raw) > 1 << 20:
            raise ProviderError('Kubernetes request exceeds bootstrap limit')
        req = Request(self._origin + path, data=raw, method=method,
                      headers={'Authorization': 'Bearer ' + self._token,
                               'Content-Type': 'application/json', 'Accept': 'application/json'})
        remaining = (getattr(self._calls, 'until', None) or (time.monotonic() + self._timeout)) - time.monotonic()
        if remaining <= 0:
            raise ProviderError('Kubernetes operation deadline exceeded')
        try:
            with self._http.open(req, timeout=remaining) as response:
                data = response.read((8 << 20) + 1)
                if time.monotonic() >= (getattr(self._calls, 'until', None) or float('inf')):
                    raise ProviderError('Kubernetes operation deadline exceeded')
                if len(data) > 8 << 20:
                    raise ProviderError('Kubernetes inventory exceeds response limit')
                return json.loads(data)
        except HTTPError as exc:
            if missing and exc.code == 404:
                return None
            if conflict and exc.code == 409:
                return None
            raise ProviderError('Kubernetes request failed') from None
        except (OSError, URLError, ValueError):
            raise ProviderError('Kubernetes request failed') from None

    def _labels(self, node, launch, prefix='nabedra.io'):
        return {prefix + '/cluster': self.cluster_id, prefix + '/node': node,
                prefix + '/launch': launch}

    def _identity_prefix(self, meta):
        # Kubernetes identities survive an SDK upgrade. Read the old namespace,
        # but never combine its ownership or lifecycle evidence with the new one.
        labels = meta.get('labels', {})
        prefixes = [prefix for prefix in ('nabedra.io', 'meshdb.io')
                    if any(key.startswith(prefix + '/') for key in labels)]
        if len(prefixes) != 1:
            raise ProviderError('Kubernetes resource ownership mismatch')
        prefix = prefixes[0]
        other = 'meshdb.io' if prefix == 'nabedra.io' else 'nabedra.io'
        if any(key.startswith(other + '/') for key in meta.get('annotations', {})):
            raise ProviderError('Kubernetes resource lifecycle namespace mismatch')
        return prefix

    def _annotation(self, meta, field):
        return meta.get('annotations', {}).get(self._identity_prefix(meta) + '/' + field)

    def _launch_name(self, meta, launch):
        return ('nabedra-' if self._identity_prefix(meta) == 'nabedra.io' else 'meshdb-') + launch

    def _owned(self, record, *, node=None, launch=None):
        meta = record.get('metadata', {})
        labels = meta.get('labels', {})
        prefix = self._identity_prefix(meta)
        try:
            n, l = labels[prefix + '/node'], labels[prefix + '/launch']
            valid = str(UUID(n)) == n and str(UUID(l)) == l
        except (KeyError, ValueError, AttributeError, TypeError):
            valid = False
        if (not valid or meta.get('namespace') != self.namespace or
                labels.get(prefix + '/cluster') != self.cluster_id or
                (node is not None and n != node) or (launch is not None and l != launch)):
            raise ProviderError('Kubernetes resource ownership mismatch')
        if meta.get('name') != self._launch_name(meta, l):
            raise ProviderError('Kubernetes resource identity mismatch')
        return meta, n, l

    def _secret(self, name):
        return self._base + '/secrets/' + quote(name, safe='')

    def _pod(self, name):
        return self._base + '/pods/' + quote(name, safe='')

    def _retain_uid(self, secret, uid, terminated=False):
        meta, _, _ = self._owned(secret)
        # The resourceVersion compares the Secret version; never overwrite another writer.
        annotations = dict(meta.get('annotations', {}))
        prefix = self._identity_prefix(meta)
        previous = annotations.get(prefix + '/pod-uid')
        if previous is not None and previous != uid:
            raise ProviderError('Kubernetes launch UID changed')
        if previous == uid and (not terminated or annotations.get(prefix + '/terminated') == 'true'):
            return secret
        annotations[prefix + '/pod-uid'] = uid
        if terminated:
            annotations[prefix + '/terminated'] = 'true'
        replacement = copy.deepcopy(secret)
        replacement['metadata']['annotations'] = annotations
        return self._request('PUT', self._secret(meta['name']), replacement)

    @_bounded_call
    def start_node(self, payload: dict) -> str:
        if payload.get('cluster_id') != self.cluster_id:
            raise ProviderError('Kubernetes launch cluster mismatch')
        node, launch = payload['node_id'], payload['request_id']
        if any(str(UUID(value)) != value for value in (node, launch)):
            raise ProviderError('Invalid Kubernetes launch identity')
        name = 'nabedra-' + launch
        try:
            port = int(payload['bootstrap']['listen'].rsplit(':', 1)[1])
        except (KeyError, ValueError, IndexError, AttributeError):
            raise ProviderError('Kubernetes workers require an explicit nonzero listen port') from None
        if not 1 <= port <= 65535:
            raise ProviderError('Kubernetes workers require an explicit nonzero listen port')
        bootstrap = json.dumps(payload['bootstrap'], sort_keys=True, separators=(',', ':')).encode()
        digest = hashlib.sha256(bootstrap).hexdigest()
        secret_path = self._secret(name)
        secret = self._request('GET', secret_path, missing=True)
        legacy_name = 'meshdb-' + launch
        legacy = self._request('GET', self._secret(legacy_name), missing=True)
        if legacy is not None:
            if secret is not None:
                raise ProviderError('Kubernetes launch has multiple resource identities')
            name, secret = legacy_name, legacy
        if secret is None:
            self._request('POST', self._base + '/secrets', {
                'apiVersion': 'v1', 'kind': 'Secret', 'type': 'Opaque',
                'metadata': {'name': name, 'namespace': self.namespace,
                             'labels': self._labels(node, launch),
                             'annotations': {'nabedra.io/bootstrap-sha256': digest}},
                'data': {'bootstrap.json': base64.b64encode(bootstrap).decode()}}, conflict=True)
            secret = self._request('GET', secret_path)
        meta, _, _ = self._owned(secret, node=node, launch=launch)
        if (self._annotation(meta, 'bootstrap-sha256') != digest or
                secret.get('data', {}).get('bootstrap.json') != base64.b64encode(bootstrap).decode() or
                self._annotation(meta, 'terminated') == 'true'):
            raise ProviderError('Kubernetes launch identity conflict')
        pod = self._request('GET', self._pod(name), missing=True)
        if pod is None:
            # A recorded UID is never reused after an uncertain or completed removal.
            if self._annotation(meta, 'pod-uid'):
                raise ProviderError('Kubernetes launch was already removed')
            spec = copy.deepcopy(self._pod_spec)
            extra_volumes = spec.pop('volumes', [])
            spec.update(restartPolicy='Never',
                        volumes=[{'name': 'bootstrap', 'secret': {'secretName': name, 'defaultMode': 256}}] + extra_volumes,
                        containers=[{'name': 'worker', 'image': self._image,
                                     'command': ['python3', '-c', _POD_RUNNER],
                                     'env': [{'name': 'NABEDRA_POD_IP', 'valueFrom': {'fieldRef': {'fieldPath': 'status.podIP'}}}],
                                     'volumeMounts': [{'name': 'bootstrap', 'mountPath': '/nabedra', 'readOnly': True}] + copy.deepcopy(self._volume_mounts)}])
            # Match Template::worker_resources for omitted explicit resources.
            resources = payload['bootstrap'].get('resources') or {
                'max_threads': payload['bootstrap']['partition_slots'], 'memory_limit_bytes': 8 << 30}
            if resources:
                spec['containers'][0]['resources'] = {
                    'requests': {'cpu': str(resources['max_threads']), 'memory': str(resources['memory_limit_bytes'])},
                    'limits': {'cpu': str(resources['max_threads']), 'memory': str(resources['memory_limit_bytes'])}}
            self._request('POST', self._base + '/pods', {
                'apiVersion': 'v1', 'kind': 'Pod',
                'metadata': {'name': name, 'namespace': self.namespace,
                             'labels': self._labels(node, launch, self._identity_prefix(meta))}, 'spec': spec}, conflict=True)
            pod = self._request('GET', self._pod(name))
        pod_meta, _, _ = self._owned(pod, node=node, launch=launch)
        uid = pod_meta.get('uid')
        if not isinstance(uid, str) or not uid or '/' in uid:
            raise ProviderError('Kubernetes Pod has no UID')
        self._retain_uid(secret, uid)
        return self.namespace + '/' + name + '/' + uid

    @_bounded_call
    def stop_node(self, resource_id: str) -> None:
        try:
            namespace, name, uid = resource_id.split('/')
        except (ValueError, AttributeError):
            raise ProviderError('Invalid Kubernetes resource identity') from None
        if namespace != self.namespace or not name.startswith(('nabedra-', 'meshdb-')) or not uid:
            raise ProviderError('Kubernetes resource ownership mismatch')
        secret = self._request('GET', self._secret(name))
        meta, node, launch = self._owned(secret)
        if name != self._launch_name(meta, launch) or self._annotation(meta, 'pod-uid') != uid:
            raise ProviderError('Kubernetes resource ownership mismatch')
        pod = self._request('GET', self._pod(name), missing=True)
        if pod is not None:
            pod_meta, _, _ = self._owned(pod, node=node, launch=launch)
            if pod_meta.get('uid') != uid:
                raise ProviderError('Kubernetes Pod UID changed')
            self._request('DELETE', self._pod(name), {
                'apiVersion': 'v1', 'kind': 'DeleteOptions',
                'preconditions': {'uid': uid}}, missing=True)
        # DELETE acknowledgement is not disappearance. Inventory confirms later.
        if self._request('GET', self._pod(name), missing=True) is None:
            self._retain_uid(secret, uid, terminated=True)

    @_bounded_call
    def list_nodes(self) -> list[dict]:
        result = []
        launches = set()
        for prefix in ('nabedra.io', 'meshdb.io'):
            query = {'labelSelector': prefix + '/cluster=' + self.cluster_id, 'limit': '64'}
            continuations = set()
            while True:
                page = self._request('GET', self._base + '/secrets?' + urlencode(query))
                if not isinstance(page.get('items'), list):
                    raise ProviderError('Invalid Kubernetes inventory')
                for secret in page['items']:
                    meta, node, launch = self._owned(secret)
                    if self._identity_prefix(meta) != prefix or launch in launches:
                        raise ProviderError('Kubernetes inventory identity conflict')
                    launches.add(launch)
                    pod = self._request('GET', self._pod(meta['name']), missing=True)
                    uid = self._annotation(meta, 'pod-uid')
                    if pod is not None:
                        pod_meta, _, _ = self._owned(pod, node=node, launch=launch)
                        if self._identity_prefix(pod_meta) != prefix:
                            raise ProviderError('Kubernetes resource ownership mismatch')
                        if uid is not None and pod_meta.get('uid') != uid:
                            raise ProviderError('Kubernetes Pod UID changed')
                        uid = pod_meta['uid']
                        phase = pod.get('status', {}).get('phase')
                        if self._annotation(meta, 'terminated') == 'true' and phase not in ('Failed', 'Succeeded'):
                            raise ProviderError('Kubernetes terminated resource reappeared')
                        state = ('terminated' if phase in ('Failed', 'Succeeded') else
                                 'stopping' if pod_meta.get('deletionTimestamp') else
                                 'running' if phase == 'Running' else 'starting')
                        if self._annotation(meta, 'pod-uid') is None:
                            self._retain_uid(secret, uid)
                    elif uid:
                        self._retain_uid(secret, uid, terminated=True)
                        state = 'terminated'
                    else:
                        continue  # Secret installation alone does not prove Pod creation.
                    result.append(dict(resource_id=self.namespace + '/' + meta['name'] + '/' + uid,
                                       cluster_id=self.cluster_id, node_id=node, request_id=launch, state=state))
                    if len(result) > 4096:
                        raise ProviderError('Kubernetes inventory exceeds callback limit')
                continuation = page.get('metadata', {}).get('continue')
                if not continuation:
                    break
                if continuation in continuations or len(continuations) >= 63:
                    raise ProviderError('Kubernetes inventory pagination exceeded its bound')
                continuations.add(continuation)
                query['continue'] = continuation
        return result
