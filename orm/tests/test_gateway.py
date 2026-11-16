"""GW01-GW03/SL06 gateway assertions. These are HTTP fixtures, not S3 evidence."""
import asyncio
import base64
from datetime import datetime, timezone, timedelta
import json
import threading
import unittest
from unittest.mock import patch
import httpx
from meshdb.gateway import Gateway, S3, AwsCredentials

RID = '11111111-1111-4111-8111-111111111111'
RUN = '33333333-3333-4333-8333-333333333333'
TID = '22222222-2222-4222-8222-222222222222'
HEADERS = (('Content-Type', 'application/json'), ('Authorization', 'Bearer app-secret'))
BODY = (' { "payload" : {"table_id":"' + TID + '", "row":{"n":1e0}}, "operation":"table.insert", "request_id":"' + RID + '" } ').encode()
CREDS = AwsCredentials('access-sentinel', 'secret-sentinel')

def storage(ctx):
    ctx.principal = 'user'
    ctx.storage = S3('app-a', region='eu-west-1', credentials=CREDS)

def response(request):
    return httpx.Response(200, json={'request_id': RID, 'data': None})

class GatewayTests(unittest.TestCase):
    def gateway(self, handler=response, **kw):
        g = Gateway(cluster_url='https://compute.internal', token='private', mount_path='/meshdb',
                    _transport=httpx.MockTransport(handler), **kw)
        self.addCleanup(g.close)
        return g
    def call(self, g, body=BODY, headers=HEADERS, path='/meshdb/v1/operations', method='POST'):
        return g.handle(method=method, path=path, headers=headers, body=body)
    def test_GW01_raw_order_immutable_and_header_filtering(self):
        seen = []
        def upstream(req):
            seen.append('upstream')
            self.assertEqual(req.content, BODY)
            self.assertEqual(req.headers['authorization'], 'Bearer private')
            self.assertNotIn('cookie', req.headers)
            self.assertNotIn('x-extra', req.headers)
            data = json.loads(base64.urlsafe_b64decode(req.headers['x-meshdb-storage'] + '=='))
            self.assertEqual(data, {'version':1,'bucket':'app-a','region':'eu-west-1','prefix':'meshdb/',
                'credentials':{'access_key_id':'access-sentinel','secret_access_key':'secret-sentinel','session_token':None,'expires_at':None}})
            return response(req)
        g = self.gateway(upstream)
        @g.middleware
        def first(ctx):
            seen.append('first')
            self.assertEqual(ctx.headers.get_all('x-extra'), ('a', 'b'))
            self.assertEqual(ctx.operation.payload['row']['n'], 1.0)
            for name, value in [('raw_body', b'other'), ('operation', None), ('_values', {})]:
                with self.assertRaises(AttributeError):
                    setattr(ctx, name, value)
            with self.assertRaises(TypeError):
                ctx.operation.payload['row']['n'] = 2
            storage(ctx)
        @g.middleware
        def second(ctx):
            seen.append('second')
        with patch('meshdb.client._Transport.request', side_effect=AssertionError('ORM used')):
            result = self.call(g, headers=HEADERS + (('Cookie','secret'),('X-Extra','a'),('X-Extra','b')))
        self.assertEqual(result.status,200)
        self.assertEqual(seen, ['first','second','upstream'])
        self.assertEqual(result.body, response(None).content)
    def test_SL06_rejections_never_forward(self):
        calls = []
        g = self.gateway(lambda r: calls.append(r) or response(r))
        g.middleware(storage)
        bad = [b'{', BODY.replace(b'1e0', b'null'), BODY.replace(b'1e0', b'9223372036854775808'),
               BODY.replace(b'"row"',b'"unknown"'), BODY.replace(b'"n":1e0',b'"n":1,"n":2'),
               BODY.replace(TID.encode(), b'invalid'), b'['*100 + b']'*100]
        for body in bad:
            with self.subTest(body=body[:80]):
                self.assertEqual(self.call(g,body).status,400)
        for path in ('/meshdb/admin/status','/meshdb/v1/operations?x=1','/other/v1/operations','https://evil/v1/operations'):
            self.assertEqual(self.call(g,path=path).status,400)
        for headers in (HEADERS+(('X-MeshDB-Storage','forged'),), HEADERS+(('X-MeshDB-Principal','forged'),), HEADERS+(('Authorization','Bearer duplicate'),), HEADERS+(('X-MeshDB-Database','../bad'),)):
            self.assertIn(self.call(g,headers=headers).status,(400,401))
        self.assertEqual(self.call(g,body=b' '* (8*1024*1024+1)).status,413)
        self.assertEqual(self.call(g,headers=HEADERS+(('X-Extra','x'*32768),)).status,413)
        self.assertEqual(calls, [])
    def test_missing_denied_failing_provider_redacted(self):
        def fail(*args):
            raise RuntimeError('secret-sentinel')
        for cb in (None, fail, lambda c:c.reject('unauthorized','secret-sentinel'),
                   lambda c:(setattr(c,'principal','u'),setattr(c,'storage', S3('app-a',region='eu-west-1',credentials=fail)))):
            g = self.gateway(lambda r: self.fail('Forwarded'))
            if cb:
                g.middleware(cb)
            res = self.call(g)
            self.assertGreaterEqual(res.status,400)
            self.assertNotIn(b'secret-sentinel',res.body)
        for thing in (g, CREDS, S3('app-a',region='eu-west-1',credentials=CREDS)):
            self.assertNotIn('sentinel',repr(thing))
    def test_GW02_uncertain_mutation_once(self):
        calls=[]
        def fail(req):
            calls.append(req)
            raise httpx.ReadError('secret-sentinel')
        g=self.gateway(fail); g.middleware(storage)
        res=self.call(g)
        self.assertEqual(res.status,503)
        self.assertEqual(json.loads(res.body), {'request_id':RID,'error':{'code':'outcome_unknown','message':'Compute response unavailable; do not resubmit','outcome':'unknown'}})
        self.assertEqual(len(calls),1)
        g2=self.gateway(lambda r:httpx.Response(200,content=b'bad',headers={'content-type':'application/json'}));g2.middleware(storage)
        self.assertEqual(json.loads(self.call(g2).body)['error']['outcome'],'unknown')
    def test_GW02_receipts_and_errors_exact(self):
        contexts=[]
        queued = json.dumps({'request_id':RID,'run_id':RUN,'state':'queued'}, indent=2).encode()
        terminal = json.dumps({'request_id':RID,'run_id':RUN,'state':'succeeded','result':None,'error':None}).encode()
        failure = json.dumps({'request_id':RID,'error':{'code':'conflict','message':'conflict','outcome':'not_applied'}}).encode()
        for status, raw, path, method, body in ((202,queued,'/meshdb/v1/requests','POST',BODY),
            (200,terminal,f'/meshdb/v1/requests/{RUN}/{RID}','GET',b''),(409,failure,'/meshdb/v1/operations','POST',BODY)):
            g=self.gateway(lambda req:httpx.Response(status,content=raw,headers={'content-type':'application/json','set-cookie':'bad','location':'https://compute.internal/private'}))
            @g.middleware
            def auth(ctx):
                contexts.append(ctx)
                storage(ctx)
            res=self.call(g, body=body,path=path,method=method)
            self.assertEqual((res.status,res.body),(status,raw))
            self.assertNotIn('set-cookie',dict(res.headers))
            if status==202:
                self.assertEqual(dict(res.headers)['location'],f'/meshdb/v1/requests/{RUN}/{RID}')
        self.assertEqual(contexts[1].receipt.run_id,RUN)
        self.assertIsNone(contexts[1].operation)
    def test_sync_rejects_async_before_callbacks(self):
        g=self.gateway(lambda r:self.fail('Forwarded'))
        @g.middleware
        async def cb(ctx):
            self.fail('Called async')
        self.assertEqual(self.call(g).status,400)
    def test_all_payload_schemas(self):
        from meshdb.gateway_protocol import payload
        q={'select':['x'],'filter':{'op':'and','args':[{'op':'eq','column':'x','value':True},{'op':'not','arg':{'op':'gt','column':'x','value':1.0}}]},'order_by':{'column':'x','desc':False},'limit':10**50}
        samples={'table.create':{'name':'a','schema':{'id':'int64'},'key':'id','format':'parquet'},'table.describe':{'name':'a'},'table.list':{},'table.drop':{'table_id':TID},'table.insert':{'table_id':TID,'row':{'x':True}},'table.insert_many':{'table_id':TID,'rows':[{'x':1}]},'table.get':{'table_id':TID,'key':'k'},'table.update':{'table_id':TID,'key':1,'values':{}},'table.upsert':{'table_id':TID,'row':{'x':2}},'table.delete':{'table_id':TID,'key':1}}
        for name in ('query.collect','query.count','query.explain'):
            samples[name]={'table_id':TID,'query':q}
        for name,p in samples.items():
            payload(name,p)
            with self.assertRaises(ValueError):
                payload(name,{**p,'unknown':0})

class AsyncGatewayTests(unittest.IsolatedAsyncioTestCase):
    async def test_GW03_rejection_never_reparses_outside_capacity(self):
        entered, release = asyncio.Event(), asyncio.Event()
        forwards = []
        async with Gateway(cluster_url='https://compute.internal', token='private', max_concurrency=1,
                           _async_transport=httpx.MockTransport(lambda req: forwards.append(req) or response(req))) as g:
            @g.middleware
            async def authorize(ctx):
                entered.set()
                await release.wait()
                storage(ctx)
            first = asyncio.create_task(g.handle_async(method='POST', path='/v1/operations', headers=HEADERS, body=BODY))
            try:
                await asyncio.wait_for(entered.wait(), 2)
                large = BODY.replace(b'1e0', b'"' + b'x' * (7 * 1024 * 1024) + b'"')
                with patch('meshdb.gateway.wire.loads', side_effect=AssertionError('Parsed outside capacity')) as decode:
                    for handle in (g.handle_async, g.handle):
                        result = handle(method='POST', path='/v1/operations', headers=HEADERS, body=large)
                        if asyncio.iscoroutine(result):
                            result = await result
                        self.assertEqual(result.status, 429)
                        self.assertIsNone(json.loads(result.body)['request_id'])
                    decode.assert_not_called()
                self.assertEqual(forwards, [])
            finally:
                release.set()
                self.assertEqual((await first).status, 200)
            self.assertEqual(len(forwards), 1)
            # An admitted payload failure retains its already validated ID.
            from meshdb.gateway import wire
            with patch('meshdb.gateway.wire.loads', wraps=wire.loads) as decode:
                result = await g.handle_async(method='POST', path='/v1/operations', headers=HEADERS,
                                              body=BODY.replace(b'1e0', b'null'))
                self.assertEqual(result.status, 400)
                self.assertEqual(json.loads(result.body)['request_id'], RID)
                self.assertEqual(decode.call_count, 1)
            self.assertEqual(len(forwards), 1)

    async def test_GW03_context_isolation_sync_bridge_and_cancel(self):
        entered=threading.Event(); release=threading.Event(); seen=[]
        async def upstream(req):
            seen.append(req.content)
            return response(req)
        async with Gateway(cluster_url='http://127.0.0.1:9999',token='private',max_concurrency=2,
                           _async_transport=httpx.MockTransport(upstream)) as g:
            contexts=[]
            @g.middleware
            def sync(ctx):
                contexts.append(ctx)
                entered.set()
                release.wait(2)
                storage(ctx)
            @g.middleware
            async def auth(ctx):
                await asyncio.sleep(0)
                self.assertEqual(ctx.principal,'user')
            task=asyncio.create_task(g.handle_async(method='POST',path='/v1/operations',headers=HEADERS,body=BODY))
            for _ in range(100):
                if entered.is_set():break
                await asyncio.sleep(.001)
            self.assertTrue(entered.is_set())
            # Event loop has progressed while the synchronous callback is blocked.
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):await task
            release.set()
            result=await g.handle_async(method='POST',path='/v1/operations',headers=HEADERS,body=BODY)
            self.assertEqual(result.status,200)
            self.assertEqual(len(contexts),2)
            self.assertIsNot(contexts[0],contexts[1])
            self.assertEqual(len(seen),1)
    async def test_deadline_capacity_and_close(self):
        calls=[]
        async with Gateway(cluster_url='https://compute.internal',token='private',max_concurrency=1,
             middleware_timeout=.02,_async_transport=httpx.MockTransport(lambda r:calls.append(r) or response(r))) as g:
            gate=asyncio.Event()
            @g.middleware
            async def wait(ctx):
                await gate.wait()
                storage(ctx)
            task=asyncio.create_task(g.handle_async(method='POST',path='/v1/operations',headers=HEADERS,body=BODY))
            await asyncio.sleep(0)
            overloaded=await g.handle_async(method='POST',path='/v1/operations',headers=HEADERS,body=BODY)
            self.assertEqual(overloaded.status,429)
            self.assertEqual((await task).status,400)
        self.assertEqual(calls,[])
        self.assertTrue(g._async_client.is_closed)

class GatewayIntegrationTests(unittest.TestCase):
    def test_core_import_with_optional_dependencies_and_frameworks_absent(self):
        import subprocess, sys, os
        script = '''
import sys
class Block:
    def find_spec(self, name, *args):
        if name.split('.')[0] in {'httpx','boto3','django','flask','fastapi','starlette'}:
            raise ImportError('Optional dependency deliberately absent')
sys.meta_path.insert(0,Block())
import meshdb
from meshdb.aws import default_credentials
provider=default_credentials()
assert callable(provider)
assert 'httpx' not in sys.modules and 'boto3' not in sys.modules
assert not any(n in sys.modules for n in ('flask','django','fastapi','starlette'))
'''
        result=subprocess.run([sys.executable,'-c',script],capture_output=True,text=True,env=os.environ.copy(),timeout=10)
        self.assertEqual(result.returncode,0,result.stderr)
    def test_client_selector_on_operations_submit_and_poll(self):
        from meshdb import DB
        from unittest.mock import MagicMock
        calls=[]
        db=DB('http://127.0.0.1:9999/meshdb','token',database='tenant-a')
        class Reply:
            status=200
            headers={'Content-Type':'application/json'}
            def read(self,n):return self.body
            def close(self):pass
        def open(req,**kw):
            calls.append(req)
            r=Reply()
            if req.method=='GET':
                r.body=json.dumps({'request_id':RID,'run_id':RUN,'state':'succeeded','result':[], 'error':None}).encode()
            elif req.full_url.endswith('/v1/requests'):
                r.status=202;r.body=json.dumps({'request_id':RID,'run_id':RUN,'state':'queued'}).encode()
            else:
                rid=json.loads(req.data)['request_id'];r.body=json.dumps({'request_id':rid,'data':[]}).encode()
            return r
        db._transport._opener.open=MagicMock(side_effect=open)
        db.tables()
        receipt=db.submit('table.list',{},request_id=RID)
        receipt.poll()
        self.assertEqual(len(calls),3)
        for req in calls:
            self.assertEqual(dict((k.lower(),v) for k,v in req.header_items())['x-meshdb-database'],'tenant-a')
        for selector in ('../bad','bad\r\nheader',''):
            with self.assertRaises(Exception):DB('https://example.com','token',database=selector)
    def test_running_callback_stays_charged_after_deadline_and_close_returns(self):
        import time
        entered=threading.Event();release=threading.Event();finished=threading.Event()
        g=Gateway(cluster_url='https://compute.internal',token='private',max_concurrency=1,middleware_timeout=.02,_transport=httpx.MockTransport(lambda r:self.fail('Forwarded')))
        @g.middleware
        def cb(ctx):
            entered.set()
            release.wait(2)
            finished.set()
        try:
            result=g.handle(method='POST',path='/v1/operations',headers=HEADERS,body=BODY)
            self.assertTrue(entered.is_set())
            self.assertEqual(result.status,400)
            self.assertFalse(g._slots.acquire(blocking=False))
            self.assertFalse(g._thread_slots.acquire(blocking=False))
            start=time.monotonic();g.close()
            self.assertLess(time.monotonic()-start,.5)
            self.assertFalse(finished.is_set())
        finally:
            release.set()
            self.assertTrue(finished.wait(2))
            g.close()
