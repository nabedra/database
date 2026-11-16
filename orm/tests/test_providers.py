"""A02 provider protocol assertions. No Kubernetes credentials or live calls."""
import copy
import unittest
from urllib.parse import urlsplit, unquote, parse_qs

from nabedra.providers import KubernetesProvider, ProviderError
from nabedra.errors import ValidationError

C, N, R = [f'00000000-0000-4000-8000-{n:012d}' for n in range(1, 4)]


def payload():
    return dict(cluster_id=C, node_id=N, request_id=R,
                bootstrap=dict(version=1, cluster_id=C, node_id=N, request_id=R,
                               enrollment_token='credential', api_urls=['https://api.example'], listen='0.0.0.0:8001', partition_slots=2, tls=None))


class KubernetesFixture(KubernetesProvider):
    """Independent API object/version/UID fixture, including lost responses."""
    def __init__(self):
        super().__init__(api_url='https://kubernetes.example', token='private', namespace='workers',
                         cluster_id=C, image='nabedra:test')
        self.objects, self.calls = {}, []
        self.lost_create = False
        self.delay_delete = False
        self.fail_inventory = False

    def _request(self, method, path, body=None, *, missing=False, conflict=False):
        self.calls.append((method, path, copy.deepcopy(body)))
        route = unquote(urlsplit(path).path)
        if method == 'GET' and route.endswith('/secrets'):
            if self.fail_inventory:
                raise ProviderError('fixture unavailable')
            selector = parse_qs(urlsplit(path).query)['labelSelector'][0]
            label, value = selector.split('=', 1)
            return {'items': [copy.deepcopy(v) for k, v in self.objects.items()
                              if '/secrets/' in k and v['metadata']['labels'].get(label) == value], 'metadata': {}}
        if method == 'POST':
            key = route + '/' + body['metadata']['name']
            if key in self.objects:
                if conflict:
                    return None
                raise ProviderError('fixture conflict')
            record = copy.deepcopy(body)
            record['metadata'].update(uid='uid-' + str(len(self.objects)), resourceVersion='1')
            self.objects[key] = record
            if route.endswith('/pods') and self.lost_create:
                self.lost_create = False
                raise ProviderError('fixture lost POST reply')
            return copy.deepcopy(record)
        if method == 'GET':
            if route not in self.objects:
                if missing:
                    return None
                raise ProviderError('fixture missing')
            return copy.deepcopy(self.objects[route])
        if method == 'PUT':
            current = self.objects[route]
            if current['metadata']['resourceVersion'] != body['metadata']['resourceVersion']:
                raise ProviderError('fixture version conflict')
            record = copy.deepcopy(body)
            record['metadata']['resourceVersion'] = str(int(body['metadata']['resourceVersion']) + 1)
            self.objects[route] = record
            return copy.deepcopy(record)
        if method == 'DELETE':
            current = self.objects.get(route)
            if current is not None:
                if body['preconditions']['uid'] != current['metadata']['uid']:
                    raise ProviderError('fixture UID conflict')
                if self.delay_delete:
                    current['metadata']['deletionTimestamp'] = 'pending'
                else:
                    del self.objects[route]
            return {}
        raise AssertionError((method, route))


def retained_legacy_launch(provider, *, lost_create=False):
    provider.lost_create = lost_create
    if lost_create:
        try:
            provider.start_node(payload())
        except ProviderError:
            pass
    else:
        provider.start_node(payload())
    renamed = {}
    for path, record in provider.objects.items():
        meta = record['metadata']
        meta['name'] = meta['name'].replace('nabedra-', 'meshdb-', 1)
        for field in ('labels', 'annotations'):
            if field in meta:
                meta[field] = {k.replace('nabedra.io/', 'meshdb.io/', 1): v
                               for k, v in meta[field].items()}
        renamed[path.replace('/nabedra-', '/meshdb-', 1)] = record
    provider.objects = renamed
    provider.calls.clear()
    pod = provider.objects[provider._pod('meshdb-' + R)]
    return 'workers/meshdb-' + R + '/' + pod['metadata']['uid']


class KubernetesProviderTests(unittest.TestCase):
    def test_a02_legacy_inventory_retry_and_exact_uid_delete_survive_upgrade(self):
        provider = KubernetesFixture()
        resource = retained_legacy_launch(provider)
        self.assertEqual(provider.list_nodes()[0]['resource_id'], resource)
        selectors = [parse_qs(urlsplit(path).query)['labelSelector'][0]
                     for method, path, _ in provider.calls if urlsplit(path).path.endswith('/secrets')]
        self.assertEqual(selectors, ['nabedra.io/cluster=' + C, 'meshdb.io/cluster=' + C])
        self.assertEqual(provider.start_node(payload()), resource)
        self.assertFalse(any(method == 'POST' for method, _, _ in provider.calls))
        name, uid = resource.split('/')[1:]
        provider.objects[provider._pod(name)]['metadata']['uid'] = 'replacement'
        with self.assertRaises(ProviderError):
            provider.stop_node(resource)
        self.assertFalse(any(method == 'DELETE' for method, _, _ in provider.calls))
        provider.objects[provider._pod(name)]['metadata']['uid'] = uid
        provider.stop_node(resource)
        delete = next(body for method, _, body in provider.calls if method == 'DELETE')
        self.assertEqual(delete['preconditions'], {'uid': uid})
        secret = provider.objects[provider._secret(name)]
        self.assertEqual(secret['metadata']['annotations']['meshdb.io/terminated'], 'true')
        self.assertFalse(any(k.startswith('nabedra.io/') for k in secret['metadata']['annotations']))
        restarted = KubernetesFixture()
        restarted.objects = provider.objects
        self.assertEqual(restarted.list_nodes()[0]['state'], 'terminated')
        with self.assertRaises(ProviderError):
            restarted.start_node(payload())
        self.assertFalse(any(method == 'POST' for method, _, _ in restarted.calls))

    def test_a02_legacy_lost_create_retains_uid_in_original_namespace(self):
        provider = KubernetesFixture()
        resource = retained_legacy_launch(provider, lost_create=True)
        self.assertEqual(provider.list_nodes()[0]['resource_id'], resource)
        secret = provider.objects[provider._secret(resource.split('/')[1])]
        self.assertEqual(secret['metadata']['annotations']['meshdb.io/pod-uid'], resource.split('/')[2])
        self.assertEqual(provider.start_node(payload()), resource)
        self.assertFalse(any(method == 'POST' for method, _, _ in provider.calls))

    def test_a02_mixed_identity_namespaces_and_duplicate_launches_fail_closed(self):
        for field, key, value in [('labels', 'nabedra.io/cluster', C),
                                  ('labels', 'nabedra.io/launch', N),
                                  ('annotations', 'nabedra.io/pod-uid', 'forged')]:
            provider = KubernetesFixture()
            resource = retained_legacy_launch(provider)
            secret = provider.objects[provider._secret(resource.split('/')[1])]
            secret['metadata'][field][key] = value
            for call in (provider.list_nodes, lambda: provider.start_node(payload()),
                         lambda: provider.stop_node(resource)):
                with self.assertRaises(ProviderError):
                    call()
            self.assertFalse(any(method in ('POST', 'DELETE') for method, _, _ in provider.calls))
        provider = KubernetesFixture()
        provider.start_node(payload())
        new_objects = copy.deepcopy(provider.objects)
        retained_legacy_launch(provider)
        provider.objects.update(new_objects)
        for call in (provider.list_nodes, lambda: provider.start_node(payload())):
            with self.assertRaises(ProviderError):
                call()
        self.assertFalse(any(method == 'POST' for method, _, _ in provider.calls))

    def test_a02_legacy_selector_failure_never_returns_empty_inventory(self):
        provider = KubernetesFixture()
        retained_legacy_launch(provider)
        request = provider._request
        def unavailable(method, path, *args, **kwargs):
            if 'meshdb.io/cluster' in parse_qs(urlsplit(path).query).get('labelSelector', [''])[0]:
                raise ProviderError('legacy inventory unavailable')
            return request(method, path, *args, **kwargs)
        provider._request = unavailable
        with self.assertRaises(ProviderError):
            provider.list_nodes()

    def test_a02_launch_retry_uses_same_identity_and_bootstrap(self):
        provider = KubernetesFixture()
        first = provider.start_node(payload())
        second = provider.start_node(payload())
        self.assertEqual(first, second)
        creates = [c for c in provider.calls if c[0] == 'POST']
        self.assertEqual(len(creates), 2)  # one Secret and one Pod across both attempts
        pod = next(c[2] for c in creates if c[1].endswith('/pods'))
        self.assertEqual(pod['spec']['containers'][0]['command'][:2], ['python3', '-c'])
        self.assertIn("['nabedra-db', 'worker', '--bootstrap', path]", pod['spec']['containers'][0]['command'][2])
        self.assertEqual(pod['spec']['volumes'][0]['secret']['defaultMode'], 0o400)
        changed = payload()
        changed['bootstrap']['api_urls'] = ['https://different.example']
        with self.assertRaises(ProviderError):
            provider.start_node(changed)
        self.assertEqual(provider.list_nodes()[0]['resource_id'], first)

    def test_a02_lost_create_reply_reconciles_without_second_pod(self):
        provider = KubernetesFixture()
        provider.lost_create = True
        with self.assertRaises(ProviderError):
            provider.start_node(payload())
        inventory = provider.list_nodes()
        self.assertEqual(len(inventory), 1)
        first = inventory[0]['resource_id']
        self.assertEqual(provider.start_node(payload()), first)
        self.assertEqual(len([c for c in provider.calls if c[0] == 'POST' and c[1].endswith('/pods')]), 1)

    def test_a02_delete_requires_ownership_and_exact_uid(self):
        provider = KubernetesFixture()
        resource = provider.start_node(payload())
        namespace, name, uid = resource.split('/')
        key = provider._pod(name)
        provider.objects[key]['metadata']['uid'] = 'replacement-uid'
        with self.assertRaises(ProviderError):
            provider.stop_node(resource)
        self.assertFalse(any(c[0] == 'DELETE' for c in provider.calls))
        provider.objects[key]['metadata']['uid'] = uid
        provider.objects[key]['metadata']['labels']['nabedra.io/cluster'] = R
        with self.assertRaises(ProviderError):
            provider.stop_node(resource)
        self.assertFalse(any(c[0] == 'DELETE' for c in provider.calls))
        provider.objects[key]['metadata']['labels']['nabedra.io/cluster'] = C
        provider.stop_node(resource)
        deletion = next(c for c in provider.calls if c[0] == 'DELETE')
        self.assertEqual(deletion[2]['preconditions'], {'uid': uid})
        self.assertEqual(provider.list_nodes()[0]['state'], 'terminated')
        provider.stop_node(resource)
        with self.assertRaises(ProviderError):
            provider.start_node(payload())

    def test_a02_delete_ack_is_not_disappearance_and_evidence_survives_restart(self):
        provider = KubernetesFixture()
        resource = provider.start_node(payload())
        provider.delay_delete = True
        provider.stop_node(resource)
        self.assertEqual(provider.list_nodes()[0]['state'], 'stopping')
        name = resource.split('/')[1]
        del provider.objects[provider._pod(name)]
        restarted = KubernetesFixture()
        restarted.objects = provider.objects
        self.assertEqual(restarted.list_nodes()[0], dict(resource_id=resource, cluster_id=C,
                                                       node_id=N, request_id=R, state='terminated'))

    def test_a02_inventory_failure_does_not_invent_absence(self):
        provider = KubernetesFixture()
        resource = provider.start_node(payload())
        provider.fail_inventory = True
        with self.assertRaises(ProviderError):
            provider.list_nodes()
        provider.fail_inventory = False
        self.assertEqual(provider.list_nodes()[0]['resource_id'], resource)
        self.assertFalse(any(c[0] == 'DELETE' for c in provider.calls))

    def test_worker_resource_limits_and_explicit_credential_mounts(self):
        provider = KubernetesFixture()
        provider._pod_spec = {'volumes': [{'name': 'credentials', 'secret': {'secretName': 'storage-auth'}}]}
        provider._volume_mounts = [{'name': 'credentials', 'mountPath': '/run/nabedra-auth', 'readOnly': True}]
        value = payload()
        value['bootstrap']['resources'] = dict(max_threads=2, memory_limit_bytes=1073741824,
            task_memory_limit_bytes=1048576, control_memory_reserve_bytes=1048576)
        provider.start_node(value)
        pod = next(body for method, path, body in provider.calls if method == 'POST' and path.endswith('/pods'))
        container = pod['spec']['containers'][0]
        self.assertEqual(container['resources']['limits'], {'cpu': '2', 'memory': '1073741824'})
        self.assertEqual(container['volumeMounts'][1]['mountPath'], '/run/nabedra-auth')
        self.assertEqual(pod['spec']['volumes'][1]['secret']['secretName'], 'storage-auth')

    def test_nonzero_worker_port_is_required_before_creating_resources(self):
        provider = KubernetesFixture()
        value = payload()
        value['bootstrap']['listen'] = '0.0.0.0:0'
        with self.assertRaises(ProviderError):
            provider.start_node(value)
        self.assertEqual(provider.calls, [])

    def test_inventory_pagination_must_advance(self):
        provider = KubernetesFixture()
        provider._request = lambda *args, **kwargs: {'items': [], 'metadata': {'continue': 'same'}}
        with self.assertRaises(ProviderError):
            provider.list_nodes()

    def test_explicit_provider_configuration(self):
        with self.assertRaises(ValidationError):
            KubernetesProvider(api_url='http://example', token='x', namespace='workers', cluster_id=C, image='worker')
        with self.assertRaises(ValidationError):
            KubernetesProvider(api_url='https://example', token='x', namespace='workers', cluster_id=C,
                               image='worker', pod_spec={'containers': []})


if __name__ == '__main__':
    unittest.main()
