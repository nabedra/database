"""Harness privacy on failure; these checks never access a private fixture/AWS."""
import contextlib
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import acceptance_serverless_s3 as harness


class PrivacyTests(unittest.TestCase):
    def test_failure_report_never_stringifies_exception_or_private_config(self):
        with tempfile.TemporaryDirectory() as directory:
            output=Path(directory)/'result.json'
            with patch('sys.argv',['acceptance','--env-file','never-read-private-fixture','--binary','never-started','--output',str(output)]), patch.object(harness,'run',side_effect=RuntimeError('credential-sentinel')), contextlib.redirect_stdout(io.StringIO()) as stdout:
                self.assertEqual(harness.main(),1)
            report=json.loads(output.read_text())
            self.assertEqual(report['result'],'failed')
            self.assertEqual(report['assertions'],[])
            self.assertNotIn('credential-sentinel',output.read_text()+stdout.getvalue())
            self.assertNotIn('never-read-private-fixture',output.read_text()+stdout.getvalue())

    def test_prefix_guard_rejects_foreign_or_unsafe_key_before_aws(self):
        from acceptance_support import AcceptanceConfig
        config=AcceptanceConfig('synthetic-bucket','eu-west-1',{})
        namespace=config.allocate_prefix(1,'synthetic',run_id='local-unit')
        with tempfile.TemporaryDirectory() as directory, patch.object(harness.shutil,'which',return_value='/unused-aws'), patch.object(harness.subprocess,'run') as execute:
            probe=harness.S3Probe(config,namespace,Path(directory))
            for path in ('../foreign','/absolute','data/../foreign'):
                with self.assertRaises(ValueError): probe.read(path)
            execute.assert_not_called()

    def test_owned_child_observer_uses_portable_numeric_process_columns(self):
        from types import SimpleNamespace
        import subprocess
        cluster=object.__new__(harness.NativeCluster)
        cluster.process=SimpleNamespace(pid=123)
        result=subprocess.CompletedProcess([],0,b' 124 123\n 125 123\n 126 500\n',b'')
        with patch.object(harness.subprocess,'run',return_value=result) as run:
            self.assertEqual(cluster.children(),[124,125])
            self.assertEqual(run.call_args.args[0],['/bin/ps','-axo','pid=,ppid='])

    def test_catalog_diagnosis_exports_only_presence_and_counts(self):
        from unittest.mock import Mock
        import subprocess
        probe=object.__new__(harness.S3Probe)
        from types import SimpleNamespace
        probe.config=SimpleNamespace(bucket='synthetic-bucket')
        probe.key=Mock(return_value='synthetic-fresh/database-a/catalog.json')
        run='11111111-1111-4111-8111-111111111111'
        owner='22222222-2222-4222-8222-222222222222'
        request='33333333-3333-4333-8333-333333333333'
        catalog={'owner_cluster':owner,'revision':2,'tables':{'same':{}},'fence':{'run_id':run}}
        probe.command=Mock(return_value=subprocess.CompletedProcess([],0,json.dumps(catalog).encode(),b''))
        probe.decision_count=Mock(return_value=1)
        observed,private_run=probe.observe_catalog('database-a',request)
        self.assertEqual(private_run,run)
        self.assertEqual(observed,{'catalog_created':True,'owner_present':True,'table_count':1,'matching_decision_found':True,'receipt_accepted':None,'revision':2})
        for private in (run,owner,request): self.assertNotIn(private,json.dumps(observed))

    def test_receipt_observer_preserves_terminal_error_null(self):
        from unittest.mock import Mock
        import httpx
        raw=b'{"request_id":"private-request","state":"succeeded","result":[],"error":null}'
        transport=object.__new__(harness.ObservedTransport)
        transport.report={}
        transport.inner=Mock()
        transport.inner.handle_request.return_value=httpx.Response(200,headers={'content-type':'application/json'},stream=httpx.ByteStream(raw))
        response=transport.handle_request(httpx.Request('GET','http://127.0.0.1/fixture'))
        self.assertEqual(response.content,raw)
        self.assertEqual(transport.report,{'last_compute':{'status':200,'error_code':None,'error_message_kind':None}})
        self.assertNotIn('private-request',json.dumps(transport.report))

    def test_error_message_classification_is_allowlisted_and_preserves_bytes(self):
        from unittest.mock import Mock
        import httpx
        for message,kind in [('logical request outcome unavailable','api_observation_unknown'),('credential-sentinel',None)]:
            raw=json.dumps({'error':{'code':'outcome_unknown','message':message}}).encode()
            transport=object.__new__(harness.ObservedTransport)
            transport.report={}
            transport.inner=Mock()
            transport.inner.handle_request.return_value=httpx.Response(503,stream=httpx.ByteStream(raw))
            response=transport.handle_request(httpx.Request('POST','http://127.0.0.1/fixture'))
            self.assertEqual(response.content,raw)
            self.assertEqual(transport.report['last_compute']['error_message_kind'],kind)
            self.assertNotIn(message,json.dumps(transport.report))
