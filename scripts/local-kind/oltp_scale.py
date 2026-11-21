"""Add and drain an actual Docker data worker alongside the kind StatefulSets."""
from copy import deepcopy
import json
import subprocess

from oltp_direct_cluster import EXECUTION, IDENTITY_FIELDS, WORKERS, operation, resources, uid, write_json
from oltp_recovery_operations import success, wait_for


def scale(topology, growth_claim, run, traffic):
    cluster = topology.cluster
    zone = 'c'
    node = cluster.nodes[zone]
    network = cluster.obj('node', node, namespaced=False)['status']['addresses']
    address = next(item['address'] for item in network if item['type'] == 'InternalIP')
    name = f'{cluster.name}-data'
    pool_id, marker = uid(), uid()
    volume = name + '-pool'
    config_dir = topology.root / 'scale'
    config_dir.mkdir(mode=0o700)
    enrollment = topology.certificate('data4')
    enrollment['endpoint'] = f'{address}:19000'
    worker = {field: enrollment[field] for field in IDENTITY_FIELDS}
    mount = dict(profile={'docker': {'volume_marker': marker}}, worker=worker, generation=1,
                 identity=dict(engine='oltp', pool_format_version=2, cluster_id=topology.cluster_id,
                               worker_id=worker['worker_id'], pool_id=pool_id),
                 mount_path='/pool', provider_volume_id='docker-volume:' + volume, filesystem_uuid='')
    private = config_dir / 'mount.pem'
    topology.openssl('genpkey', '-algorithm', 'Ed25519', '-out', private)
    seed = subprocess.check_output(['openssl', 'pkey', '-in', str(private), '-outform', 'DER'], timeout=15)[-32:]
    public = subprocess.check_output(['openssl', 'pkey', '-in', str(private), '-pubout', '-outform', 'DER'],
                                     timeout=15)[-32:]
    private.unlink()
    hosts = ['127.0.0.1 localhost']
    for peer_name in WORKERS:
        ip = topology.pod(peer_name)['status']['podIP']
        hosts.append(f"{ip} {topology.enrollments[peer_name]['server_name']}")
    (config_dir / 'hosts').write_text('\n'.join(hosts) + '\n')
    certs = ('data4.pem', 'data4.key', 'ca.pem')
    for file in certs:
        path = config_dir / file
        path.write_bytes((topology.root / file).read_bytes())
        path.chmod(0o600)
    auth = {key: topology.auth[key] for key in ('data_token', 'admin_token')}
    write_json(config_dir / 'auth.json', auth, 0o600)

    pool = f'type=volume,source={volume},target=/pool'
    seed_container = name + '-seed'
    mounted_config = f'type=bind,source={config_dir.resolve()},target=/config,readonly'
    hosts_file = f'type=bind,source={(config_dir / "hosts").resolve()},target=/etc/hosts,readonly'
    run('docker', 'volume', 'create', '--name', volume)
    try:
        script = """import json, pathlib, sys
raw = json.load(sys.stdin)
with pathlib.Path('/pool/NABEDRA_RESTART_KEY').open('xb') as f:
    f.write(bytes.fromhex(raw.pop('seed')))
pathlib.Path('/pool/NABEDRA_RESTART_KEY').chmod(0o600)
pathlib.Path('/pool/.nabedra-docker-volume.json').write_text(json.dumps(raw))
"""
        # Seed the freshly created named volume before constructing the worker.
        run('docker', 'run', '--rm', '--name', seed_container, '-i', '--network=none', '--memory=128m',
            '--mount', pool, topology.image, 'python3', '-c', script,
            input=json.dumps(dict(seed=seed.hex(), format_version=1, volume_name=volume, volume_marker=marker)))
        claim_id = growth_claim(topology, 'w3', dict(kind='add_worker', zone=zone, bytes=512 << 20),
                                pool_id, 1, mount['provider_volume_id'], worker=worker)
        intent = dict(claim_id=claim_id, run_id=topology.run_id, enrollment=enrollment,
                      mount=mount, mount_public_key=public.hex())
        operator = 'w3'
        prepared = topology.admin('storage.growth.prepare_worker', intent, worker=operator)
        assert prepared['identity'] == worker, prepared
        peers = topology.admin('cluster.connection', {}, worker=operator)['control_peers']
        control = deepcopy(topology.config[operator]['node']['placements']['control'])
        launch = dict(version=1, bind='127.0.0.1:18080', auth_file='/config/auth.json',
                      resources=resources(1024 << 20), execution=EXECUTION,
                      node=dict(intent=intent, bind='0.0.0.0:19000', scheduler_node_id=4,
                                certificate='/config/data4.pem', private_key='/config/data4.key',
                                authority_ca='/config/ca.pem', control=control, control_enrollments=peers))
        write_json(config_dir / 'data4.json', launch, 0o600)
        container_id = run('docker', 'run', '-d', '--name', name, '--network', f'container:{node}',
                           '--memory', '1152m', '--memory-swap', '1152m', '--mount', pool,
                           '--mount', mounted_config, '--mount', hosts_file, topology.image,
                           'nabedra-db', 'data-worker', '--bootstrap', '/config/data4.json').strip()
        assert len(container_id) == 64

        def ready(number):
            status, reply = topology.worker(operator, '/health/ready')
            return reply if status == 200 and reply.get('ready_workers') == number else None

        joined = wait_for(lambda: ready(4), 'fourth worker joined the existing replicated queue', 90)
        growth = topology.admin('storage.growth.status', {'claim_id': claim_id})
        assert growth['phase'] == 'complete' and joined['replication_factor'] == 3
        assert joined['run_id'] == topology.run_id
        cluster.evidence.record('O17/A02 added ready Docker data worker', container_id=container_id,
                                zone=zone, worker=worker, cluster=joined, growth=growth)
        drain_id = uid()
        drain = dict(worker=worker, run_id=topology.run_id, docker_container_id=container_id)
        initial = topology.admin('worker.drain', drain, worker=operator, request_id=drain_id)
        assert initial['id'] == drain_id and initial['intent'] == drain, initial

        def stop_permit():
            status, reply = topology.worker(operator, '/v1/cluster/operations',
                operation('worker.stop_permit', {}, drain_id), admin=True)
            if status == 200:
                return success(status, reply)
            assert status in (404, 409, 429, 503), (status, reply)
            return None

        permit = wait_for(stop_permit, 'native mount closure and committed queue removal', 100)
        record = permit['record']
        assert record['intent'] == drain and record['id'] == drain_id, permit
        closure = record['phase']['closed']
        assert closure['mount']['expected'] == mount and closure['ready']['run_id'] == topology.run_id, permit
        before_stop = wait_for(lambda: ready(3), 'RF3 ready set before terminating data worker', 40)
        assert before_stop['replication_factor'] == 3 and before_stop['run_id'] == topology.run_id
        assert run('docker', 'inspect', '--format', '{{.Id}}', name).strip() == container_id
        run('docker', 'stop', '-t', '10', container_id, timeout=25)
        cluster.evidence.record('O17/A02 stop permit before Docker termination', permit=permit,
                                ready=before_stop, container_id=container_id,
                                accepted_receipts=sum(status == 202 for _, status in traffic.receipts))
    finally:
        logs = run('docker', 'logs', name, check=False)
        cluster.evidence.file('scale-data-worker.log', logs.stdout + logs.stderr)
        run('docker', 'rm', '-f', name, seed_container, check=False)
        run('docker', 'volume', 'rm', volume)
