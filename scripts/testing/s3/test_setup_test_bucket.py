"""setup_test_bucket.py: the plan is exact, and nothing runs without --apply."""
import io
import json
import subprocess
import unittest

import setup_test_bucket as setup


class Recorder:
    def __init__(self, fail=None):
        self.calls = []
        self.fail = fail

    def __call__(self, argv, stdin=None):
        self.calls.append((argv, stdin))
        stdout = ''
        if argv[:3] == ['aws', 'sts', 'get-caller-identity']:
            stdout = '123456789012\n'
        if argv[:3] == ['aws', 'iam', 'list-open-id-connect-providers']:
            stdout = json.dumps({'OpenIDConnectProviderList': []})
        code = 1 if self.fail and argv[:3] == self.fail else 0
        return subprocess.CompletedProcess(argv, code, stdout, '')


class SetupTests(unittest.TestCase):
    def plan(self, *args, runner=None):
        out = io.StringIO()
        runner = runner or Recorder()
        code = setup.main(['--bucket', 'nabedra-test-bucket', *args], runner=runner, out=out)
        return code, out.getvalue(), runner

    def test_plan_only_by_default(self):
        code, text, runner = self.plan()
        self.assertEqual(code, 0)
        self.assertEqual(runner.calls, [])
        self.assertIn('nothing changes without --apply', text)
        self.assertIn('changes the OIDC token subject for every workflow in the repository', text)
        self.assertIn('Update existing OIDC trust policies', text)
        self.assertIn('NABEDRA_TEST_S3_ROLE_ARN = arn:aws:iam::<ACCOUNT_ID>:role/nabedra-test-s3', text)
        self.assertIn('NABEDRA_TEST_S3_REGION   = us-east-1', text)
        self.assertNotIn('LocationConstraint', text)

    def test_trust_is_dispatch_and_schedule_on_main_only(self):
        policy = setup.trust_policy('123456789012')
        statement, = policy['Statement']
        self.assertEqual(statement['Action'], 'sts:AssumeRoleWithWebIdentity')
        self.assertEqual(statement['Principal']['Federated'],
                         'arn:aws:iam::123456789012:oidc-provider/token.actions.githubusercontent.com')
        condition = statement['Condition']
        self.assertEqual(list(condition), ['StringEquals'])  # no wildcard StringLike
        self.assertEqual(condition['StringEquals']['token.actions.githubusercontent.com:aud'], 'sts.amazonaws.com')
        self.assertEqual(condition['StringEquals']['token.actions.githubusercontent.com:sub'], [
            'repo:nabedra@361663474/database@1459052341:ref:refs/heads/main:event_name:workflow_dispatch',
            'repo:nabedra@361663474/database@1459052341:ref:refs/heads/main:event_name:schedule',
        ])
        for subject in condition['StringEquals']['token.actions.githubusercontent.com:sub']:
            self.assertNotIn('*', subject)
            self.assertNotIn('pull_request', subject)
        self.assertEqual(setup.SUBJECT_KEYS, ['repo', 'context', 'event_name'])

    def test_policy_is_limited_to_the_bucket_prefix(self):
        policy = setup.bucket_policy('b-1')
        resources = [s['Resource'] for s in policy['Statement']]
        self.assertEqual(resources, ['arn:aws:s3:::b-1/runs/*', 'arn:aws:s3:::b-1'])
        self.assertEqual(policy['Statement'][1]['Action'], ['s3:ListBucket', 's3:ListBucketMultipartUploads'])
        for statement in policy['Statement']:
            self.assertNotIn('s3:*', statement['Action'])

    def test_lifecycle_expires_and_aborts_after_one_day(self):
        rule, = setup.lifecycle()['Rules']
        self.assertEqual((rule['Status'], rule['Expiration'], rule['AbortIncompleteMultipartUpload']),
                         ('Enabled', {'Days': 1}, {'DaysAfterInitiation': 1}))

    def test_apply_runs_each_step_and_stops_on_failure(self):
        code, text, runner = self.plan('--apply', '--region', 'eu-west-1')
        self.assertEqual(code, 0)
        commands = [argv[:3] for argv, _ in runner.calls]
        self.assertEqual(commands[0], ['aws', 'sts', 'get-caller-identity'])
        self.assertIn(['aws', 's3api', 'put-bucket-lifecycle-configuration'], commands)
        self.assertIn(['gh', 'api', '--method'], commands)
        create = next(argv for argv, _ in runner.calls if argv[:3] == ['aws', 's3api', 'create-bucket'])
        self.assertIn('LocationConstraint=eu-west-1', create)
        gh_stdin = next(stdin for argv, stdin in runner.calls if argv[0] == 'gh')
        self.assertEqual(json.loads(gh_stdin), {
            'include_claim_keys': ['repo', 'context', 'event_name'],
            'use_default': False,
            'use_immutable_subject': True,
        })
        code, text, runner = self.plan('--apply', runner=Recorder(fail=['aws', 's3api', 'create-bucket']))
        self.assertEqual(code, 1)
        self.assertFalse(any(argv[0] == 'gh' for argv, _ in runner.calls))

    def test_rejects_bad_names(self):
        with self.assertRaises(SystemExit):
            setup.main(['--bucket', 'Bad_Bucket'], runner=Recorder(), out=io.StringIO())
        with self.assertRaises(SystemExit):
            setup.main(['--bucket', 'ok-bucket', '--region', 'nowhere'], runner=Recorder(), out=io.StringIO())


if __name__ == '__main__':
    unittest.main()
