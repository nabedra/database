"""GW01/GW02/GW03 host-bound assertions, independent of compute/S3 acceptance."""
import base64
import http.client
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
import httpx
sys.path.insert(0, str(Path(__file__).resolve().parents[3] / 'scripts'))
from gateway_host_fixture import endpoint, collect, BodyLimit, MAX_BODY
from nabedra.gateway import Gateway, S3, AwsCredentials

RID = '11111111-1111-4111-8111-111111111111'
RUN = '33333333-3333-4333-8333-333333333333'
BODY = (' {"request_id":"'+RID+'", "operation":"table.insert", "payload": {"table_id":"22222222-2222-4222-8222-222222222222","row":{"n":1e0}}} ').encode()


class HostTests(unittest.TestCase):
    def test_GW03_stream_is_bounded_before_gateway(self):
        consumed = []
        def chunks():
            for _ in range(3):
                consumed.append(1)
                yield b'x' * (MAX_BODY // 2)
            self.fail('read after limit')
        with self.assertRaises(BodyLimit):
            collect(chunks())
        self.assertEqual(len(consumed), 3)
        calls = []
        with endpoint(lambda *args: calls.append(args)) as url:
            host, port = url.removeprefix('http://').split(':')
            conn = http.client.HTTPConnection(host, int(port), timeout=3)
            conn.putrequest('POST', '/nabedra/v1/operations')
            conn.putheader('Transfer-Encoding', 'chunked')
            conn.endheaders()
            # Reject from chunk size before allocating/reading its promised bytes.
            conn.send(('%x\r\n' % (MAX_BODY+1)).encode())
            response = conn.getresponse()
            self.assertEqual(response.status, 413)
            response.read(); conn.close()
        self.assertEqual(calls, [])

    def test_GW01_GW02_real_http_bytes_receipts_and_secret_logs(self):
        seen, order, logs = [], [], []
        queued = ('{ "request_id":"'+RID+'","run_id":"'+RUN+'","state":"queued" }').encode()
        terminal = json.dumps(dict(request_id=RID, run_id=RUN, state='succeeded', result=[], error=None)).encode()
        failure = json.dumps(dict(request_id=RID,error=dict(code='conflict',message='conflict',outcome='not_applied'))).encode()
        def upstream(method, path, headers, body):
            h = dict((k.lower(),v) for k,v in headers)
            self.assertEqual(h['authorization'], 'Bearer compute-sentinel')
            self.assertNotIn('cookie',h)
            context = json.loads(base64.urlsafe_b64decode(h['x-nabedra-storage']+'=='))
            self.assertEqual(context['credentials']['secret_access_key'],'aws-sentinel')
            seen.append((method,path,body)); order.append('forward')
            if method == 'GET': return 200, (), terminal
            if path.endswith('/requests'): return 202, (), queued
            return 409, (), failure
        with endpoint(upstream, logs) as compute:
            with Gateway(cluster_url=compute, token='compute-sentinel', mount_path='/nabedra') as gateway:
                @gateway.middleware
                def auth(ctx):
                    order.append('auth')
                    if ctx.operation is not None:
                        self.assertEqual(ctx.operation.payload['row']['n'], 1.0)
                    self.assertEqual(ctx.headers.get('authorization'),'Bearer app-sentinel')
                    ctx.principal='user'
                    ctx.storage=S3('app-test',region='eu-west-1',credentials=AwsCredentials('access-sentinel','aws-sentinel'))
                @gateway.middleware
                def authorize(ctx):
                    order.append('authorize')
                def host(method,path,headers,body):
                    res=gateway.handle(method=method,path=path,headers=headers,body=body)
                    return res.status,res.headers,res.body
                with endpoint(host, logs) as public, httpx.Client(trust_env=False, timeout=5) as client:
                    with patch('nabedra.client._Transport.request',side_effect=AssertionError('ORM call')):
                        for path,method,body,status,raw in [('/v1/operations','POST',BODY,409,failure),('/v1/requests','POST',BODY,202,queued),(f'/v1/requests/{RUN}/{RID}','GET',b'',200,terminal)]:
                            response=client.request(method,public+'/nabedra'+path,content=body,headers={'Authorization':'Bearer app-sentinel','Cookie':'cookie-sentinel','Content-Type':'application/json'})
                            self.assertEqual((response.status_code,response.content),(status,raw))
                            self.assertEqual(response.headers['cache-control'],'no-store')
                            if status==202: self.assertEqual(response.headers['location'],f'/nabedra/v1/requests/{RUN}/{RID}')
                    bad=client.post(public+'/nabedra/v1/operations',content=b'{',headers={'Authorization':'Bearer app-sentinel'})
                    self.assertEqual(bad.status_code,400)
        self.assertEqual(order,['auth','authorize','forward']*3)
        self.assertEqual([x[2] for x in seen],[BODY,BODY,b''])
        self.assertNotIn('sentinel',' '.join(logs))
        self.assertFalse(any(n in sys.modules for n in ('flask','django','starlette','fastapi')))
