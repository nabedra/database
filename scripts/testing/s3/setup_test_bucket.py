#!/usr/bin/env python3
"""Plan or create the S3 test bucket and its GitHub OIDC role.

By default this prints the plan and changes nothing. With --apply it runs the
same steps through the `aws` CLI (and `gh` for the repository's OIDC subject
template) using the caller's usual credentials.

What it sets up:
  - one private bucket with lifecycle rules that expire objects after 1 day and
    abort incomplete multipart uploads after 1 day;
  - the GitHub Actions OIDC identity provider, if the account lacks it;
  - an IAM role that only workflow_dispatch and schedule runs on `main` of the
    repository can assume, with object access limited to `runs/*` in that bucket;
  - the repository's OIDC subject template, so the subject carries the event
    name and the trust policy can tell dispatches from pull requests.
"""
import argparse
import json
import re
import shlex
import subprocess
import sys

REPOSITORY = 'nabedra/database'
REPOSITORY_SUBJECT = 'repo:nabedra@361663474/database@1459052341'
BRANCH = 'main'
EVENTS = ('workflow_dispatch', 'schedule')
OIDC_HOST = 'token.actions.githubusercontent.com'
AUDIENCE = 'sts.amazonaws.com'
# The subject template keys. "context" expands to `ref:refs/heads/<branch>` for
# branch runs and to `pull_request` for pull requests, so PRs never match.
SUBJECT_KEYS = ['repo', 'context', 'event_name']
PREFIX = 'runs/'
BUCKET = re.compile(r'[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]')
ROLE = re.compile(r'[A-Za-z0-9+=,.@_-]{1,64}')
REGION = re.compile(r'[a-z]{2}(-[a-z]+)+-[0-9]')


def subjects():
    """The exact OIDC `sub` values the role trusts."""
    return [f'{REPOSITORY_SUBJECT}:ref:refs/heads/{BRANCH}:event_name:{event}' for event in EVENTS]


def lifecycle():
    return {'Rules': [{
        'ID': 'nabedra-test-expire-1-day',
        'Filter': {'Prefix': ''},
        'Status': 'Enabled',
        'Expiration': {'Days': 1},
        'AbortIncompleteMultipartUpload': {'DaysAfterInitiation': 1},
    }]}


def trust_policy(account):
    provider = f'arn:aws:iam::{account}:oidc-provider/{OIDC_HOST}'
    return {
        'Version': '2012-10-17',
        'Statement': [{
            'Effect': 'Allow',
            'Principal': {'Federated': provider},
            'Action': 'sts:AssumeRoleWithWebIdentity',
            'Condition': {
                'StringEquals': {
                    f'{OIDC_HOST}:aud': AUDIENCE,
                    f'{OIDC_HOST}:sub': subjects(),
                },
            },
        }],
    }


def bucket_policy(bucket):
    """Object access under runs/ and listing of the dedicated bucket."""
    return {
        'Version': '2012-10-17',
        'Statement': [
            {
                'Sid': 'RunObjects',
                'Effect': 'Allow',
                'Action': ['s3:GetObject', 's3:PutObject', 's3:DeleteObject',
                           's3:AbortMultipartUpload', 's3:ListMultipartUploadParts'],
                'Resource': f'arn:aws:s3:::{bucket}/{PREFIX}*',
            },
            {
                # S3 requires bucket-level ListBucket for missing GETs to return 404.
                'Sid': 'ListTestBucket',
                'Effect': 'Allow',
                'Action': ['s3:ListBucket', 's3:ListBucketMultipartUploads'],
                'Resource': f'arn:aws:s3:::{bucket}',
            },
        ],
    }


def steps(bucket, region, role, account):
    """(description, argv) for every change, in order. No step reads secrets."""
    create = ['aws', 's3api', 'create-bucket', '--bucket', bucket, '--region', region]
    if region != 'us-east-1':
        create += ['--create-bucket-configuration', f'LocationConstraint={region}']
    return [
        ('Create the bucket', create),
        ('Block all public access', [
            'aws', 's3api', 'put-public-access-block', '--bucket', bucket,
            '--public-access-block-configuration',
            'BlockPublicAcls=true,IgnorePublicAcls=true,BlockPublicPolicy=true,RestrictPublicBuckets=true']),
        ('Expire objects and abort incomplete multipart uploads after 1 day', [
            'aws', 's3api', 'put-bucket-lifecycle-configuration', '--bucket', bucket,
            '--lifecycle-configuration', json.dumps(lifecycle(), separators=(',', ':'))]),
        ('Register the GitHub OIDC provider (skipped if it exists)', [
            'aws', 'iam', 'create-open-id-connect-provider', '--url', f'https://{OIDC_HOST}',
            '--client-id-list', AUDIENCE]),
        ('Create the role, trusted only by dispatch and schedule runs on main', [
            'aws', 'iam', 'create-role', '--role-name', role,
            '--max-session-duration', '3600',
            '--assume-role-policy-document', json.dumps(trust_policy(account), separators=(',', ':'))]),
        ('Allow listing the test bucket and object access under runs/*', [
            'aws', 'iam', 'put-role-policy', '--role-name', role, '--policy-name', 'nabedra-test-s3',
            '--policy-document', json.dumps(bucket_policy(bucket), separators=(',', ':'))]),
        ('Replace the repository OIDC subject template', [
            'gh', 'api', '--method', 'PUT', f'repos/{REPOSITORY}/actions/oidc/customization/sub',
            '--input', '-']),
    ]


SUBJECT_TEMPLATE = json.dumps({
    'use_default': False,
    'include_claim_keys': SUBJECT_KEYS,
    'use_immutable_subject': True,
})


def run(argv, stdin=None):
    return subprocess.run(argv, input=stdin, capture_output=True, text=True, check=False)


def provider_exists(runner):
    listed = runner(['aws', 'iam', 'list-open-id-connect-providers', '--output', 'json'])
    if listed.returncode != 0:
        raise SystemExit('Listing OIDC providers failed.')
    arns = [p.get('Arn', '') for p in json.loads(listed.stdout or '{}').get('OpenIDConnectProviderList', [])]
    return any(arn.endswith(f'oidc-provider/{OIDC_HOST}') for arn in arns)


def main(argv=None, runner=run, out=sys.stdout):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--bucket', required=True, help='new, globally unique bucket name')
    parser.add_argument('--region', default='us-east-1')
    parser.add_argument('--role', default='nabedra-test-s3')
    parser.add_argument('--account-id', help='AWS account ID for the plan (looked up with --apply)')
    parser.add_argument('--apply', action='store_true', help='make the changes (default: print the plan)')
    args = parser.parse_args(argv)
    if BUCKET.fullmatch(args.bucket) is None or '..' in args.bucket:
        parser.error('invalid bucket name')
    if REGION.fullmatch(args.region) is None:
        parser.error('invalid region')
    if ROLE.fullmatch(args.role) is None:
        parser.error('invalid role name')
    account = args.account_id or '<ACCOUNT_ID>'
    if args.apply:
        identity = runner(['aws', 'sts', 'get-caller-identity', '--query', 'Account', '--output', 'text'])
        if identity.returncode != 0:
            print('aws sts get-caller-identity failed; check your credentials.', file=sys.stderr)
            return 1
        account = identity.stdout.strip()
    if account != '<ACCOUNT_ID>' and re.fullmatch(r'[0-9]{12}', account) is None:
        parser.error('invalid account ID')
    plan = steps(args.bucket, args.region, args.role, account)
    print(f'{"Applying" if args.apply else "Plan (nothing changes without --apply)"}:', file=out)
    for number, (description, command) in enumerate(plan, 1):
        print(f'\n{number}. {description}\n   {shlex.join(command)}', file=out)
        if command[0] == 'gh':
            print('   This changes the OIDC token subject for every workflow in the repository.',
                  file=out)
            print('   Update existing OIDC trust policies to accept the new subjects before applying.',
                  file=out)
            print(f'   stdin: {SUBJECT_TEMPLATE}', file=out)
        if not args.apply:
            continue
        if command[:3] == ['aws', 'iam', 'create-open-id-connect-provider'] and provider_exists(runner):
            print('   exists, skipped', file=out)
            continue
        result = runner(command, SUBJECT_TEMPLATE if command[0] == 'gh' else None)
        if result.returncode != 0:
            print(f'   failed (exit {result.returncode}); stopping. Earlier steps stay applied.', file=out)
            return 1
        print('   done', file=out)
    print('\nRepository variables to set (Settings, Secrets and variables, Actions, Variables):', file=out)
    print(f'  NABEDRA_TEST_S3_ROLE_ARN = arn:aws:iam::{account}:role/{args.role}', file=out)
    print(f'  NABEDRA_TEST_S3_BUCKET   = {args.bucket}', file=out)
    print(f'  NABEDRA_TEST_S3_REGION   = {args.region}', file=out)
    print('Trusted OIDC subjects:', file=out)
    for subject in subjects():
        print(f'  {subject}', file=out)
    return 0


if __name__ == '__main__':
    sys.exit(main())
