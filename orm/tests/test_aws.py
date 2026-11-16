"""Credential-provider fixtures; no real AWS requests are made."""
from datetime import datetime, timedelta, timezone
import sys
import threading
import types
import unittest
from unittest.mock import patch
from meshdb.aws import AwsCredentials, default_credentials
from meshdb.gateway import S3

class AwsTests(unittest.TestCase):
    def test_expiry_and_redaction(self):
        for expiry in (datetime.now(), datetime.now(timezone(timedelta(hours=1))), 'tomorrow'):
            with self.assertRaises(ValueError):
                AwsCredentials('key','secret-sentinel',expires_at=expiry)
        c=AwsCredentials('key','secret-sentinel',session_token='token-sentinel',expires_at=datetime.now(timezone.utc)+timedelta(hours=1))
        self.assertNotIn('sentinel',repr(c))
        s=S3('bucket-name',region='eu-west-1',credentials=c)
        import base64, json
        wire=json.loads(base64.urlsafe_b64decode(s._header()+'=='))
        self.assertEqual(wire['credentials']['session_token'],'token-sentinel')
        self.assertTrue(wire['credentials']['expires_at'].endswith('Z'))
        expired=AwsCredentials('key','secret-sentinel',expires_at=datetime.now(timezone.utc)-timedelta(seconds=1))
        with self.assertRaisesRegex(Exception,'Gateway request rejected'):
            S3('bucket-name',region='eu-west-1',credentials=expired)._header()
    def test_lazy_persistent_session_consistent_refresh_snapshots(self):
        calls=[]
        class Credentials:
            def __init__(self):
                self._refresh_lock=threading.Lock()
                self.n=0
            def get_frozen_credentials(self):
                with self._refresh_lock:
                    self.n+=1
                    self._frozen_credentials=types.SimpleNamespace(access_key=str(self.n),secret_key='secret-'+str(self.n),token='token-'+str(self.n))
                    self._expiry_time=datetime(2030,1,self.n,tzinfo=timezone.utc)
                    return self._frozen_credentials
        credentials=Credentials()
        class Session:
            def __init__(self,profile_name):calls.append(profile_name)
            def get_credentials(self):return credentials
        with patch.dict(sys.modules,{'boto3':types.SimpleNamespace(Session=Session)}):
            provider=default_credentials(profile='selected')
            self.assertEqual(calls,[])
            for n in (1,2):
                value=provider()
                self.assertEqual((value.access_key_id,value.secret_access_key,value.session_token,value.expires_at.day),(str(n),'secret-'+str(n),'token-'+str(n),n))
            self.assertEqual(calls,['selected'])
    def test_provider_error_does_not_expose_exception(self):
        def fail(**kw):raise RuntimeError('secret-sentinel')
        with patch.dict(sys.modules,{'boto3':types.SimpleNamespace(Session=fail)}):
            with self.assertRaisesRegex(ValueError,'AWS credential discovery failed') as caught:
                default_credentials()()
        self.assertNotIn('sentinel',str(caught.exception))
    def test_storage_restrictions(self):
        c=AwsCredentials('key','secret')
        for bucket in ('192.168.1.1','some--x-s3','arn:aws:s3:a','bucket..name'):
            with self.assertRaises(ValueError):S3(bucket,region='eu-west-1',credentials=c)
        for prefix in ('../bad/','/absolute/','a/../b/','a\\b/','a\n/'):
            with self.assertRaises(ValueError):S3('bucket-name',region='eu-west-1',prefix=prefix,credentials=c)

    def test_storage_normalization_and_partition_regions(self):
        c=AwsCredentials('key','secret')
        for prefix in ('db', 'db/', 'db///'):
            self.assertEqual(S3('bucket-name',region='eu-west-1',prefix=prefix,credentials=c).prefix,'db/')
        for prefix in ('', '/', 'a//b/', 'a:b/', 'a%b/', 'a?b/', 'a#b/', 'a\u0085b/', 'x'*1025):
            with self.assertRaises(ValueError):
                S3('bucket-name',region='eu-west-1',prefix=prefix,credentials=c)
        for region in ('us-iso-east-1','us-isob-east-1','us-isof-south-1','us-gov-east-١','us-west-0','eu-bogus-1','eu-west-extra-1','eusc-de-east-1','x'*65):
            with self.assertRaises(ValueError):
                S3('bucket-name',region=region,credentials=c)
        for region in ('us-gov-west-1','cn-north-1','eu-west-1'):
            S3('bucket-name',region=region,credentials=c)

    def test_expiry_wire_has_canonical_seconds(self):
        import base64, json
        from datetime import datetime, timezone
        c=AwsCredentials('key','secret',expires_at=datetime(2099,1,1,0,0,1,999999,tzinfo=timezone.utc))
        header=S3('bucket-name',region='eu-west-1',credentials=c)._header()
        value=json.loads(base64.urlsafe_b64decode(header+'=='))
        self.assertEqual(value['credentials']['expires_at'],'2099-01-01T00:00:01Z')

    def test_wire_field_byte_limits_before_forwarding(self):
        for key, secret, token in [('x'*4097,'s',None),('k','x'*4097,None),('k','s','x'*8193)]:
            with self.assertRaises(ValueError):
                AwsCredentials(key,secret,session_token=token)
        c=AwsCredentials('k','s')
        self.assertEqual(len(S3('bucket-name',region='eu-west-1',prefix='x'*1023,credentials=c).prefix),1024)
        with self.assertRaises(ValueError):
            S3('bucket-name',region='eu-west-1',prefix='x'*1024,credentials=c)
