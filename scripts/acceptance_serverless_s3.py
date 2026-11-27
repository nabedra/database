#!/usr/bin/env python3.11
"""Live private-fixture S3 acceptance. Never provisions or deletes cloud objects.

Run only after the parent releases native execution. All writes stay below one
fresh allocate_prefix namespace. Reports contain labels and assertions only;
private identifiers, credentials, raw responses and subprocess env are omitted.
"""
import argparse
import concurrent.futures
from contextlib import closing
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import secrets
import shutil
import signal
import socket
import subprocess
import sys
import tempfile
import time
import uuid

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'orm' / 'python' / 'src'))
from acceptance_support import load_config
from acceptance_nodes import announcement
from gateway_host_fixture import endpoint
from nabedra.gateway import Gateway, S3, AwsCredentials
from nabedra._cli_config import example, protected_write
from nabedra._control_transport import _ControlTransport
import httpx


class CheckFailed(RuntimeError):
    pass


def check(condition):
    if not condition:
        raise CheckFailed('acceptance assertion failed')


class S3Probe:
    def __init__(self, config, namespace, root):
        self.config, self.namespace, self.root = config, namespace, root
        self.binary = shutil.which('aws')
        check(self.binary is not None)

    def command(self, args):
        return subprocess.run([self.binary, *args, '--region', self.config.region],
            env=self.config._aws_env(), stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, timeout=35)

    def key(self, relative):
        return self.namespace.key(relative)

    def read(self, relative):
        result = self.command(['s3', 'cp', 's3://' + self.config.bucket + '/' + self.key(relative), '-', '--only-show-errors'])
        check(result.returncode == 0 and len(result.stdout) <= 32 * 1024 * 1024)
        return result.stdout

    def put(self, relative, body, *, etag=None):
        # The older installed AWS CLI lacks If-Match. Use an explicit SDK
        # client for conditional writes, without modifying global dependencies.
        import boto3
        from botocore.config import Config
        from botocore.exceptions import ClientError
        private=self.config._aws_env()
        with closing(boto3.client('s3',region_name=self.config.region,
            aws_access_key_id=private['AWS_ACCESS_KEY_ID'],
            aws_secret_access_key=private['AWS_SECRET_ACCESS_KEY'],
            aws_session_token=private.get('AWS_SESSION_TOKEN'),
            config=Config(connect_timeout=5,read_timeout=10,proxies={},
                          retries={'total_max_attempts':1}))) as client:
            try:
                result=client.put_object(Bucket=self.config.bucket,Key=self.key(relative),
                    Body=body,**({'IfMatch':etag} if etag else {'IfNoneMatch':'*'}))
                return subprocess.CompletedProcess([],0,json.dumps({'ETag':result['ETag']}).encode(),b'')
            except ClientError as error:
                code=error.response.get('Error',{}).get('Code')
                allowed={'PreconditionFailed','AccessDenied','InvalidAccessKeyId','SignatureDoesNotMatch'}
                safe=code if code in allowed else 'S3RequestFailed'
                return subprocess.CompletedProcess([],254,b'',safe.encode())

    def snapshot(self, relative):
        prefix = self.key(relative) + '/'
        result = self.command(['s3api', 'list-objects-v2', '--bucket', self.config.bucket,
            '--prefix', prefix, '--output', 'json'])
        check(result.returncode == 0)
        value = json.loads(result.stdout)
        return {item['Key']: item['ETag'] for item in value.get('Contents', [])}

    def decision_count(self, database, request_id):
        root = json.loads(self.read(database + '/catalog.json'))
        cursor = root['decision_head']; seen = set(); count = 0
        while cursor is not None:
            check(cursor not in seen and len(seen) < 100)
            seen.add(cursor)
            decision = json.loads(self.read(database + '/decisions/' + str(uuid.UUID(cursor)) + '.json'))
            count += decision['request_id'] == request_id
            cursor = decision['parent']
        return count

    def observe_catalog(self, database, request_id):
        """Return labels/counts plus a private run ID for one fresh database."""
        relative=database+'/catalog.json'
        result=self.command(['s3','cp','s3://'+self.config.bucket+'/'+self.key(relative),'-','--only-show-errors'])
        observed={'catalog_created':None,'owner_present':None,'table_count':None,
                  'matching_decision_found':None,'receipt_accepted':None}
        if result.returncode:
            if any(code in result.stderr for code in (b'404',b'NoSuchKey')):
                observed.update(catalog_created=False,owner_present=False,table_count=0,
                                matching_decision_found=False)
            else:
                observed['catalog_read']='failed'
            return observed,None
        try:
            check(len(result.stdout)<=8*1024*1024)
            catalog=json.loads(result.stdout)
            observed.update(catalog_created=True,owner_present=bool(catalog.get('owner_cluster')),
                            table_count=len(catalog['tables']),revision=catalog['revision'])
            observed['matching_decision_found']=self.decision_count(database,request_id)>0
            run_id=(catalog.get('fence') or {}).get('run_id')
            if run_id is not None:
                run_id=str(uuid.UUID(run_id))
            return observed,run_id
        except Exception:
            observed['catalog_read']='invalid_or_incomplete'
            return observed,None


class NativeCluster:
    def __init__(self, args, config, namespace, root):
        self.root, self.process = root, None
        self.cli = [args.cli] if args.cli else [sys.executable, '-m', 'nabedra.cli']
        self.binary = str(Path(args.binary).resolve())
        self.auth = {name: secrets.token_urlsafe(32) for name in ('admin_token','data_token','enrollment_token')}
        auth = root / 'auth.json'; protected_write(auth, self.auth)
        with socket.socket() as reserve:
            reserve.bind(('127.0.0.1',0)); port = reserve.getsockname()[1]
        self.url = 'http://127.0.0.1:' + str(port)
        storage = {'driver':'s3', 'options':dict(bucket=config.bucket,
            namespace=namespace.key('control')+'/', region=config.region)}
        self.value = example('serverless', storage, auth, threads=2, bind='127.0.0.1:'+str(port))
        self.value['worker']['partition_size'] = '4 KiB'
        self.value['worker']['resources']['memory_limit_bytes'] = 2 << 30
        self.value['worker']['resources']['task_memory_limit_bytes'] = 256 << 20
        self.value['lifecycle'] = {'drain_ms':20000}
        self.path = root / 'api.json'; protected_write(self.path, self.value)
        self.marker = root / 'cargo-called'
        trap = root / 'bin'; trap.mkdir()
        cargo = trap / 'cargo'
        cargo.write_text('#!'+sys.executable+'\nfrom pathlib import Path\nPath('+repr(str(self.marker))+').touch()\nraise SystemExit(99)\n')
        cargo.chmod(0o700)
        self.env = config.server_env(namespace)
        self.env.update(PATH=str(trap)+os.pathsep+os.defpath, NABEDRA_SERVER_BINARY=self.binary)
        if not args.cli:
            self.env['PYTHONPATH']=str(Path(__file__).resolve().parents[1] / 'orm' / 'python' / 'src')
        self.pids = []

    def command(self, args, timeout=40):
        return subprocess.run([*self.cli,*args],env=self.env,stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,timeout=timeout)

    def children(self):
        result=subprocess.run(['/bin/ps','-axo','pid=,ppid='],
            stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,timeout=5)
        check(result.returncode==0)
        return [int(parts[0]) for line in result.stdout.splitlines()
                if len(parts:=line.split())==2 and int(parts[1])==self.process.pid]

    def start(self, report):
        self.process = subprocess.Popen([*self.cli,'database','serve','--config',str(self.path),
            '--local-workers','2','--threads-per-worker','2'], env=self.env,
            stdout=subprocess.PIPE,stderr=subprocess.DEVNULL)
        report['enrollment_step']='announcement'
        message=announcement(self.process)
        report['announcement_check']={'event_matches':message.get('event')=='nabedra.serve.launched','url_matches':message.get('bound_url')==self.url}
        check(message.get('event')=='nabedra.serve.launched' and message.get('bound_url')==self.url)
        self.pids=self.children()
        report['enrollment_step']='owned_process_count'
        report['owned_process_count']=len(self.pids)
        check(len(self.pids)==3)
        report['enrollment_step']='ready_workers'
        admin=_ControlTransport(self.url,self.auth['admin_token'],timeout=10)
        deadline=time.monotonic()+60
        while time.monotonic()<deadline:
            check(self.process.poll() is None)
            _, status=admin.request('cluster.status',{},deadline=min(deadline,time.monotonic()+10))
            if status['counts']['ready']==2:
                check(len([n for n in status['nodes'] if n['state']=='ready'])==2)
                return
            time.sleep(.2)
        raise CheckFailed('worker enrollment timeout')

    def stop(self):
        if self.process is None:
            return
        if self.process.poll() is None:
            self.pids=list(set(self.pids + self.children()))
        process,self.process=self.process,None
        if process.poll() is None:
            process.terminate()
        try:
            process.wait(timeout=35)
        except subprocess.TimeoutExpired:
            process.kill(); process.wait(timeout=5)
            for pid in self.pids:
                try: os.kill(pid,signal.SIGKILL)
                except ProcessLookupError: pass
            raise CheckFailed('native drain timeout') from None
        finally:
            process.stdout.close()
        check(process.returncode==-signal.SIGTERM)
        for pid in self.pids:
            try: os.kill(pid,0)
            except ProcessLookupError: continue
            raise CheckFailed('owned native child remains')
        check(not self.marker.exists())


def envelope(name,payload,request_id=None):
    return json.dumps(dict(request_id=request_id or str(uuid.uuid4()),operation=name,payload=payload),
                      separators=(',',':')).encode()


def error_message_kind(value):
    error=value.get('error') if isinstance(value,dict) else None
    message=error.get('message') if isinstance(error,dict) else None
    return {
        'logical request outcome unavailable':'api_observation_unknown',
        'mutation outcome is unknown':'coordinator_unknown',
        'publication outcome is unknown':'publication_unknown',
        'cluster exchange failed':'cluster_exchange',
        'request replication outcome is uncertain; retry the same identity and body':'replication_unknown',
    }.get(message)


class ObservedTransport(httpx.BaseTransport):
    """Observe only fixed status/code labels, preserving the upstream bytes."""
    def __init__(self, report):
        self.report=report
        self.inner=httpx.HTTPTransport(retries=0,trust_env=False)

    def handle_request(self, request):
        try:
            response=self.inner.handle_request(request)
            body=bytearray()
            try:
                for chunk in response.iter_raw(chunk_size=65536):
                    check(len(body)+len(chunk)<=8*1024*1024)
                    body.extend(chunk)
            finally:
                response.close()
            response=httpx.Response(response.status_code,headers=response.headers,content=bytes(body),request=request)
            value=json.loads(response.content)
            code=(value.get('error') or {}).get('code') if isinstance(value,dict) else None
            allowed={'validation_error','storage_unavailable','limit_exceeded','conflict','unauthorized','not_found','outcome_unknown','internal_error','unsupported_operation'}
            self.report['last_compute']={'status':response.status_code,'error_code':code if code in allowed else None,'error_message_kind':error_message_kind(value)}
            return response
        except Exception as error:
            self.report['compute_failure_type']=type(error).__name__
            raise

    def close(self):
        self.inner.close()


def run(args, report):
    phase=lambda name: report.update(phase=name)
    def passed(name):
        report['assertions'].append(name)
        print(json.dumps({'progress':name}),flush=True)
    phase('private-config-preflight')
    config=load_config(args.env_file)
    check(config.preflight().success)
    namespace=config.allocate_prefix(917,'serverless-native')
    # Never serialize or print any values from this mapping.
    private=config._aws_env()
    credentials=AwsCredentials(private['AWS_ACCESS_KEY_ID'],private['AWS_SECRET_ACCESS_KEY'],
        session_token=private.get('AWS_SESSION_TOKEN'))
    forbidden=[v.encode() for k,v in private.items() if k in ('AWS_ACCESS_KEY_ID','AWS_SECRET_ACCESS_KEY','AWS_SESSION_TOKEN')]
    with tempfile.TemporaryDirectory(prefix='nabedra-database-serverless-s3-') as directory:
        root=Path(directory); probe=S3Probe(config,namespace,root)
        cluster=NativeCluster(args,config,namespace,root)
        phase('native-cli-versions')
        version=cluster.command(['--version','--json'])
        report['version_exit']=version.returncode
        check(version.returncode==0 and isinstance(json.loads(version.stdout),dict))
        report['versions']={key:json.loads(version.stdout)[key] for key in ('cli','binary','config','protocol')}
        check(cluster.command(['--help']).returncode==0)
        native_help=subprocess.run([args.binary,'database','migrate','--help'],stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,timeout=10)
        check(native_help.returncode==0 and b'--cluster-id' in native_help.stdout)
        passed('native_cli_version_help_migration_flag')
        if args.cli:
            phase('installed-isolated-core-import')
            code = """
import importlib.util,json,socket,sys
from pathlib import Path
def blocked(*args,**kwargs): raise AssertionError('network forbidden during import')
socket.socket.connect=blocked
socket.getaddrinfo=blocked
import nabedra
from nabedra.aws import default_credentials
assert callable(default_credentials())
assert Path(nabedra.__file__).resolve().is_relative_to(Path(sys.prefix).resolve())
assert all(importlib.util.find_spec(name) is None for name in ('httpx','boto3','flask','django','fastapi','starlette'))
print(json.dumps({'isolated_installed_import':True,'optional_dependencies_absent':True,'network_blocked':True}))
"""
            imported=subprocess.run([str(Path(args.cli).resolve().parent/'python'),'-I','-c',code],
                env=cluster.env,stdout=subprocess.PIPE,stderr=subprocess.DEVNULL,timeout=10)
            check(imported.returncode==0)
            report['installed_import']=json.loads(imported.stdout)
            passed('installed_core_import_isolated_optional_dependencies_absent_network_blocked')
        if args.smoke_only:
            phase('native-smoke-doctor')
            before=probe.snapshot('control')
            doctor=cluster.command(['database','doctor','--config',str(cluster.path),'--json'])
            check(doctor.returncode==0 and json.loads(doctor.stdout)['storage_write']=='unverified')
            check(before==probe.snapshot('control') and not cluster.marker.exists())
            passed('native_s3_doctor_fresh_control_no_observed_changes_no_cargo')
            report.update(phase='complete',result='passed',cleanup='no_native_services_started')
            return
        phase('real-s3-conditional-writes')
        first=probe.put('conditional/probe',b'first')
        check(first.returncode==0)
        etag=json.loads(first.stdout)['ETag']
        duplicate=probe.put('conditional/probe',b'duplicate')
        check(duplicate.returncode!=0 and b'PreconditionFailed' in duplicate.stderr and probe.read('conditional/probe')==b'first')
        changed=probe.put('conditional/probe',b'next',etag=etag)
        check(changed.returncode==0)
        stale=probe.put('conditional/probe',b'stale',etag=etag)
        check(stale.returncode!=0 and b'PreconditionFailed' in stale.stderr and probe.read('conditional/probe')==b'next')
        passed('real_s3_if_none_match_and_stale_if_match_preserve_winner')
        forbidden.extend(token.encode() for token in cluster.auth.values())
        forbidden.append(b'fixture-app')
        forbidden.extend(S3(config.bucket,region=config.region,prefix=namespace.key('database-'+db)+'/',
            credentials=credentials)._header().encode() for db in ('a','b'))
        phase('native-enrollment')
        forwarded=[]; contexts=[]; logs=[]; request_ids=set()
        def attach(ctx):
            selector=ctx.database
            check(selector in ('a','b','denied','expired'))
            target='a' if selector in ('denied','expired') else selector
            grant=AwsCredentials(credentials.access_key_id,credentials.secret_access_key,
                session_token=credentials.session_token)
            if selector=='denied':
                grant=AwsCredentials('AKIA'+'0'*16,'synthetic-invalid-secret')
            elif selector=='expired':
                grant=AwsCredentials(credentials.access_key_id,credentials.secret_access_key,
                    session_token=credentials.session_token,expires_at=datetime(2000,1,1,tzinfo=timezone.utc))
            ctx.principal='fixture-user'
            ctx.storage=S3(config.bucket,region=config.region,prefix=namespace.key('database-'+target)+'/',credentials=grant)
            contexts.append(selector)
        try:
            cluster.start(report); passed('native_shared_api_two_enrolled_worker_processes')
            with Gateway(cluster_url=cluster.url,token=cluster.auth['data_token'],mount_path='/nabedra',timeout=60,_transport=ObservedTransport(report)) as gateway:
                gateway.middleware(attach)
                def host(method,path,headers,body):
                    response=gateway.handle(method=method,path=path,headers=headers,body=body)
                    forwarded.append((method,hashlib.sha256(body).hexdigest(),response.status))
                    return response.status,response.headers,response.body
                with endpoint(host,logs) as public, httpx.Client(trust_env=False,timeout=65) as client:
                    def request(db,body=None,path='/v1/operations'):
                        if body is not None:
                            parsed=json.loads(body)
                            request_ids.add(parsed['request_id'])
                            report['last_operation']=parsed['operation']
                        response=client.request('GET' if body is None else 'POST',public+'/nabedra'+path,
                            headers={'Authorization':'Bearer fixture-app','X-Nabedra-Database':db,'Content-Type':'application/json'},content=body)
                        check(len(response.content)<=8*1024*1024)
                        value=json.loads(response.content)
                        code=(value.get('error') or {}).get('code') if isinstance(value,dict) else None
                        allowed={'validation_error','storage_unavailable','limit_exceeded','conflict','unauthorized','not_found','outcome_unknown','internal_error','unsupported_operation'}
                        report['last_http']={'selector':db,'status':response.status_code,'error_code':code if code in allowed else None,'error_message_kind':error_message_kind(value)}
                        return response.status_code,value
                    def success(db,raw):
                        operation=json.loads(raw)['operation']
                        if operation in ('table.create','table.list'):
                            print(json.dumps({'operation':operation,'selector':db,'state':'started'}),flush=True)
                        status,value=request(db,raw)
                        if status!=200 or 'data' not in value:
                            # This metadata exists only inside this attempt. Never
                            # reconstruct a discarded namespace from object listings.
                            original=dict(report.get('last_http',{}))
                            original_compute=dict(report.get('last_compute',{}))
                            rid=json.loads(raw)['request_id']
                            try:
                                observed,run_id=probe.observe_catalog('database-'+db,rid)
                                if run_id is not None:
                                    receipt_status,receipt=request(db,path='/v1/requests/'+run_id+'/'+rid)
                                    state=receipt.get('state')
                                    allowed={'queued','running','committing','reconciling','succeeded','failed','abandoned'}
                                    observed.update(receipt_status=receipt_status,
                                        receipt_state=state if state in allowed else None,
                                        receipt_error_code=report.get('last_http',{}).get('error_code'))
                                    if receipt_status==200 and state in allowed:
                                        observed['receipt_accepted']=True
                                report['failed_operation_presence']=observed
                            except Exception as error:
                                report['presence_diagnostic_failure_type']=type(error).__name__
                            finally:
                                report['last_http']=original
                                report['last_compute']=original_compute
                        check(status==200 and 'data' in value)
                        if operation in ('table.create','table.list'):
                            print(json.dumps({'operation':operation,'selector':db,'state':'succeeded'}),flush=True)
                        return value['data']
                    phase('cross-database-identities')
                    create_id=str(uuid.uuid4())
                    create=envelope('table.create',dict(name='same',schema={'id':'int64','value':'utf8'},key='id',format='parquet'),create_id)
                    tables={db:success(db,create)['id'] for db in ('a','b')}
                    check(success('a',create)['id']==tables['a'])
                    query_id=str(uuid.uuid4())
                    listing=envelope('table.list',{},query_id)
                    check(success('a',listing)[0]['id']==tables['a'])
                    check(success('b',listing)[0]['id']==tables['b'])
                    passed('same_create_and_list_request_id_bytes_independent_databases')
                    phase('shared-worker-data-and-replay')
                    insert_id=str(uuid.uuid4())
                    writes={db:envelope('table.insert',{'table_id':tables[db],'row':{'id':1,'value':db}},insert_id) for db in ('a','b')}
                    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                        futures=[pool.submit(success,db,writes[db]) for db in ('a','b')]
                        for future in futures: future.result(timeout=65)
                    # Middleware constructs a fresh same-key snapshot on each call.
                    success('a',writes['a'])
                    conflict=envelope('table.insert',{'table_id':tables['a'],'row':{'id':1,'value':'changed'}},insert_id)
                    check(request('a',conflict)[0]==409)
                    for db in ('a','b'):
                        check(success(db,envelope('table.get',{'table_id':tables[db],'key':1}))=={'id':1,'value':db})
                    passed('overlapping_two_database_rows_same_request_id_replay_conflict_readback')
                    phase('receipt-authorization')
                    status,receipt=request('a',envelope('table.list',{}),path='/v1/requests')
                    check(status==202)
                    route='/v1/requests/'+receipt['run_id']+'/'+receipt['request_id']
                    deadline=time.monotonic()+60
                    while time.monotonic()<deadline:
                        status,value=request('a',path=route)
                        check(status==200)
                        if value['state']=='succeeded': break
                        check(value['state'] not in ('failed','abandoned')); time.sleep(.1)
                    check(value['state']=='succeeded' and value['result'][0]['id']==tables['a'])
                    for selector in ('denied','expired'):
                        check(request(selector,envelope('table.get',{'table_id':tables['a'],'key':1}))[0]>=400)
                        check(request(selector,path=route)[0]>=400)
                    passed('warm_data_and_receipt_invalid_aws_grant_no_operator_fallback')
                    passed('known_expired_snapshot_rejected_locally_not_live_expiry')
                    phase('durable-decision-and-control-check')
                    check(probe.decision_count('database-a',insert_id)==1)
                    check(probe.decision_count('database-b',insert_id)==1)
                    check(probe.decision_count('database-a',create_id)==1)
                    check(probe.decision_count('database-b',create_id)==1)
                    # Scan only this fresh control namespace, never arbitrary objects.
                    control=probe.snapshot('control')
                    for key in control:
                        relative=key[len(namespace.prefix):]
                        raw=probe.read(relative)
                        check(all(secret not in raw for secret in forbidden))
                        check(all(request_id.encode() not in raw for request_id in request_ids))
                    passed('durable_decisions_once_per_database_no_customer_secrets_or_pending_ids_in_control')
                    check(all('secret' not in line and 'fixture-app' not in line for line in logs))
                    passed('host_diagnostics_exclude_secret_material')
        finally:
            phase_before=report['phase']
            try: cluster.stop()
            except Exception:
                report['cleanup']='failed'; raise
            else: report['cleanup']='native_children_reaped'
            report['phase']=phase_before
        passed('native_cli_signal_drain_reap_no_cargo_startup')
        phase('doctor-read-only')
        before=probe.snapshot('control')
        doctor=cluster.command(['database','doctor','--config',str(cluster.path),'--json'])
        check(doctor.returncode==0 and json.loads(doctor.stdout)['storage_write']=='unverified')
        check(before==probe.snapshot('control'))
        passed('native_s3_doctor_no_observed_object_changes_write_permission_unverified')
    report.update(phase='complete',result='passed')


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--env-file',required=True,help='explicit private dotenv path; never printed')
    parser.add_argument('--binary',required=True)
    parser.add_argument('--smoke-only',action='store_true',help='version/help/read-only doctor only; no native services or S3 writes')
    parser.add_argument('--cli',help='optional installed nabedra executable')
    parser.add_argument('--output',default='artifacts/cli-serverless/native-s3.json')
    args=parser.parse_args()
    report=dict(result='not_completed',phase='setup',assertions=[],cleanup='not_started',
        target='real_s3',cli_source='installed' if args.cli else 'source',scope='CLI version/help/read-only doctor only' if args.smoke_only else 'fresh private fixture prefixes; native CLI, gateway and two worker processes',
        limits=['No STS rotation or IAM revocation evidence; synthetic invalid credentials test no fallback',
            'Known expired snapshot rejected by gateway; no expiry-during-accepted-work claim',
            'Same bucket with distinct prefixes; separate bucket and equal table UUID coverage belongs to other fixtures',
            'Two client calls overlap; no deterministic proof of simultaneous worker compute slots',
            'Doctor before/after object snapshot is not a server-side S3 audit log',
            'No full scheduler-contract, failover or scale acceptance claim'])
    started=time.monotonic()
    try:
        run(args,report)
    except (Exception,KeyboardInterrupt) as error:
        report['failure_type']=type(error).__name__
        # Never stringify exceptions, commands, AWS stderr, context or response data.
        report['result']='failed'
    finally:
        report['elapsed_s']=round(time.monotonic()-started,3)
        output=Path(args.output); output.parent.mkdir(parents=True,exist_ok=True)
        output.write_text(json.dumps(report,indent=2)+'\n')
    print(json.dumps({'result':report['result'],'phase':report['phase'],'assertions':len(report['assertions'])}))
    return 0 if report['result']=='passed' else 1

if __name__=='__main__':
    raise SystemExit(main())
