"""Focused Cluster contract checks against a real loopback HTTP peer."""
import copy
import json
import socket
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from meshdb import errors

CLUSTER_ID = '00000000-0000-0000-0000-000000000001'
NODE_ID = '00000000-0000-0000-0000-000000000002'
REQUEST_ID = '00000000-0000-0000-0000-000000000003'


def policy(nodes=2):
    return dict(revision=1, mode='fixed', desired=nodes, min_nodes=nodes, max_nodes=nodes)


def status():
    return dict(version=1, cluster_id=CLUSTER_ID, name='test_cluster', **policy(),
                capabilities=dict(provision=True, terminate=True, autoscale=True),
                controller=dict(epoch=0, active=False, degraded=False),
                counts=dict(ready=0, starting=1, draining=0, lost=0),
                nodes=[dict(node_id=NODE_ID, incarnation=None, resource_id=None,
                            ownership='managed', state='starting', operation_slots=4,
                            partition_slots=4, running_operations=0, running_partitions=0,
                            queued_operations=0, queued_partitions=0,
                            heartbeat_age_seconds=None)],
                intents=[dict(request_id=REQUEST_ID, node_id=NODE_ID, resource_id=None,
                              action='start', state='pending', attempts=0, error_code=None)],
                metrics_fresh=False)


class ClusterTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                request = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                cls.seen.append((self.path, self.headers['Authorization'], request))
                if cls.drop:
                    self.connection.shutdown(socket.SHUT_RDWR)
                    self.close_connection = True
                    return
                envelope = {'request_id': request['request_id'],
                            'error' if cls.failure else 'data': cls.reply}
                raw = json.dumps(envelope).encode()
                self.send_response(409 if cls.failure else 200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

            def log_message(self, *args):
                pass
        cls.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        cls.thread = threading.Thread(target=cls.server.serve_forever, daemon=True)
        cls.thread.start()
        cls.url = f'http://127.0.0.1:{cls.server.server_port}/prefix/'

    @classmethod
    def tearDownClass(cls):
        cls.server.shutdown()
        cls.server.server_close()
        cls.thread.join()

    def setUp(self):
        type(self).seen = []
        type(self).reply = policy()
        type(self).failure = False
        type(self).drop = False

    def client(self):
        from meshdb.cluster import Cluster
        return Cluster(self.url, 'private-token')

    def test_operations_use_control_endpoint_and_exact_payloads(self):
        client = self.client()
        self.assertEqual(self.seen, [])
        self.assertEqual(client.scale(2), policy())
        type(self).reply = dict(revision=2, mode='autoscale', desired=2, min_nodes=1, max_nodes=4)
        self.assertEqual(client.autoscale(min_nodes=1, max_nodes=4), self.reply)
        type(self).reply = status()
        result = client.status()
        self.assertEqual(result, status())
        for record, operation, payload in zip(self.seen,
                ['cluster.scale', 'cluster.autoscale', 'cluster.status'],
                [{'nodes': 2}, {'min_nodes': 1, 'max_nodes': 4}, {}]):
            path, auth, request = record
            self.assertEqual(path, '/prefix/v1/cluster/operations')
            self.assertEqual(auth, 'Bearer private-token')
            self.assertEqual(set(request), {'request_id', 'operation', 'payload'})
            self.assertEqual((request['operation'], request['payload']), (operation, payload))
        self.assertEqual(len(self.seen), 3)

    def test_invalid_arguments_never_send_and_boundaries_are_accepted(self):
        client = self.client()
        for value in [True, -1, 1025, 1.0, '2', None]:
            with self.subTest(nodes=value), self.assertRaises(errors.ValidationError):
                client.scale(value)
        for low, high in [(True, 2), (1, False), (0, 1), (2, 1), (1, 1025), (1.0, 2)]:
            with self.subTest(bounds=(low, high)), self.assertRaises(errors.ValidationError):
                client.autoscale(min_nodes=low, max_nodes=high)
        self.assertEqual(self.seen, [])
        for value in (0, 1024):
            type(self).reply = policy(value)
            self.assertEqual(client.scale(value), policy(value))
        type(self).reply = dict(revision=1, mode='autoscale', desired=1024, min_nodes=1, max_nodes=1024)
        self.assertEqual(client.autoscale(min_nodes=1, max_nodes=1024), self.reply)

    def test_malformed_or_mismatched_acceptance_is_unknown_without_retry(self):
        client = self.client()
        bad = [None, dict(policy(), extra=1), dict(policy(), revision=True),
               dict(policy(), desired=1), policy(3), dict(policy(), min_nodes=False)]
        for data in bad:
            type(self).reply = data
            before = len(self.seen)
            with self.subTest(data=data), self.assertRaises(errors.UnknownOutcomeError) as caught:
                client.scale(2)
            self.assertEqual(caught.exception.request_id, self.seen[-1][2]['request_id'])
            self.assertEqual(caught.exception.status_code, 200)
            self.assertEqual(caught.exception.outcome, 'unknown')
            self.assertEqual(len(self.seen), before + 1)
        for data in [policy(), dict(revision=1, mode='autoscale', desired=3, min_nodes=2, max_nodes=4),
                     dict(revision=1, mode='autoscale', desired=5, min_nodes=1, max_nodes=4)]:
            type(self).reply = data
            with self.assertRaises(errors.UnknownOutcomeError):
                client.autoscale(min_nodes=1, max_nodes=4)

    def test_status_validation_rejects_shape_type_identity_and_count_errors(self):
        client = self.client()
        cases = [('unknown', lambda d: d.update(extra=1)),
                 ('version bool', lambda d: d.update(version=True)),
                 ('noncanonical UUID', lambda d: d.update(cluster_id=CLUSTER_ID.replace('-', ''))),
                 ('name', lambda d: d.update(name='bad name')),
                 ('capability', lambda d: d['capabilities'].update(provision=1)),
                 ('epoch', lambda d: d['controller'].update(epoch=True)),
                 ('count', lambda d: d['counts'].update(starting=0)),
                 ('duplicate node', lambda d: d['nodes'].append(copy.deepcopy(d['nodes'][0]))),
                 ('duplicate intent', lambda d: d['intents'].append(copy.deepcopy(d['intents'][0]))),
                 ('slots', lambda d: d['nodes'][0].update(operation_slots=True)),
                 ('running', lambda d: d['nodes'][0].update(running_operations=5)),
                 ('queue', lambda d: d['nodes'][0].update(queued_partitions=4097)),
                 ('heartbeat', lambda d: d['nodes'][0].update(heartbeat_age_seconds=True)),
                 ('resource', lambda d: d['nodes'][0].update(resource_id='x\n')),
                 ('intent UUID', lambda d: d['intents'][0].update(request_id='invalid')),
                 ('provider text', lambda d: d['intents'][0].update(error_code='provider failed: secret'))]
        for label, mutate in cases:
            data = status()
            mutate(data)
            type(self).reply = data
            with self.subTest(label=label), self.assertRaises(errors.ProtocolError) as caught:
                client.status()
            self.assertEqual(caught.exception.request_id, self.seen[-1][2]['request_id'])
            self.assertEqual(caught.exception.status_code, 200)

    def test_status_normalizes_identity_order_and_allows_retained_intents_and_stopped_nodes(self):
        client = self.client()
        data = status()
        second = dict(data['nodes'][0], node_id=REQUEST_ID, state='stopped',
                      resource_id='opaque / resource', heartbeat_age_seconds=1.5)
        data['nodes'].insert(0, second)
        data['intents'].insert(0, dict(data['intents'][0], request_id=CLUSTER_ID,
                                      node_id=REQUEST_ID, action='stop', state='completed'))
        type(self).reply = data
        result = client.status()
        self.assertEqual([n['node_id'] for n in result['nodes']], [NODE_ID, REQUEST_ID])
        self.assertEqual([i['request_id'] for i in result['intents']], [CLUSTER_ID, REQUEST_ID])
        result['nodes'][0]['state'] = 'lost'
        self.assertEqual(client.status()['nodes'][0]['state'], 'starting')

    def test_existing_rejections_and_lost_reply_propagate_without_retry(self):
        client = self.client()
        type(self).failure = True
        type(self).reply = dict(code='conflict', message='rejected', outcome='not_applied')
        with self.assertRaises(errors.ConflictError):
            client.scale(2)
        self.assertEqual(len(self.seen), 1)
        type(self).drop = True
        with self.assertRaises(errors.UnknownOutcomeError):
            client.autoscale(min_nodes=1, max_nodes=3)
        self.assertEqual(len(self.seen), 2)
        with self.assertRaises(errors.TransportError):
            client.status()
        self.assertEqual(len(self.seen), 3)


if __name__ == '__main__':
    unittest.main()


class ResourceCapacityTests(unittest.TestCase):
    def test_c01_capacity_reports_keep_heterogeneous_limits(self):
        from meshdb.cluster import _node
        for threads in (1, 2, 4):
            node = status()['nodes'][0]
            node.update(partition_slots=threads,
                        resources=dict(max_threads=threads, memory_limit_bytes=1000 * threads,
                                       task_memory_limit_bytes=500 * threads,
                                       control_memory_reserve_bytes=100 * threads),
                        available_threads=threads - 1,
                        available_memory_bytes=800 * threads)
            self.assertEqual(_node(node), node)

    def test_c02_capacity_overstatement_and_incomplete_reports_fail(self):
        from meshdb.cluster import _node
        node = status()['nodes'][0]
        node.update(resources=dict(max_threads=4, memory_limit_bytes=4000,
                                   task_memory_limit_bytes=2000, control_memory_reserve_bytes=400),
                    available_threads=3, available_memory_bytes=3000)
        for change in (dict(available_threads=5), dict(available_threads=True),
                       dict(available_memory_bytes=3601), dict(available_memory_bytes=None),
                       dict(resources=None)):
            with self.subTest(change=change):
                with self.assertRaises(errors.ValidationError):
                    _node(dict(node, **change))
        missing = dict(node)
        del missing['available_threads']
        with self.assertRaises(errors.ValidationError):
            _node(missing)
