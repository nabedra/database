# S3 test bucket

The real-S3 checks (`olap-store-contract-s3` and later S3 checks) run against one
dedicated test bucket. Until it exists the evidence records show
`not-run: s3-unconfigured` with zero executed assertions.

## One-time setup

The repository owner runs this once, with their usual AWS credentials and a `gh`
login that can administer the repository.

1. Read the plan. It prints every command and changes nothing:

   ```
   python3 scripts/testing/s3/setup_test_bucket.py --bucket <new-bucket-name>
   ```

   Options: `--region` (default `us-east-1`), `--role` (default `nabedra-test-s3`) and
   `--account-id` (only to show the real role ARN in the plan).
   The plan includes a repository-wide OIDC subject change. Before applying it,
   update any existing OIDC trust policies that match this repository's token
   subjects. Every workflow that requests an OIDC token will get the new subject
   format, including workflows unrelated to S3.
2. Apply it:

   ```
   python3 scripts/testing/s3/setup_test_bucket.py --bucket <new-bucket-name> --apply
   ```

   It stops at the first failed step. Steps that already ran stay applied, so fix
   the cause and rerun the remaining commands from the plan by hand.
3. Set these repository variables (Settings, Secrets and variables, Actions,
   Variables). They are not secrets: the role grants nothing outside the trusted runs.

   | Variable | Value |
   | --- | --- |
   | `NABEDRA_TEST_S3_ROLE_ARN` | `arn:aws:iam::<account>:role/nabedra-test-s3` (printed at the end) |
   | `NABEDRA_TEST_S3_BUCKET` | the bucket name |
   | `NABEDRA_TEST_S3_REGION` | the region; `us-east-1` when unset |

## After the GitHub transfer

For an existing test bucket, update the role's trust policy in place to use the
two exact subjects below. Restore the repository's custom OIDC subject template
with the final GitHub API command printed by the setup script. Check other
OIDC consumers before changing the repository-wide template.

## What it creates

| Resource | Settings |
| --- | --- |
| Bucket | Public access blocked. One lifecycle rule expires every object after 1 day and aborts incomplete multipart uploads after 1 day. |
| OIDC provider | `token.actions.githubusercontent.com`, audience `sts.amazonaws.com`. Skipped when the account already has it. |
| IAM role | Trusted only for the subjects below. Its inline policy allows get, put, delete and multipart cleanup on `runs/*` in this bucket, plus listing the dedicated test bucket. S3 requires bucket-level list access for reads of missing keys to return 404. Sessions last at most 1 hour. |
| Repository OIDC subject template | Immutable repository subjects with claim keys `repo`, `context`, `event_name`. |

The template retains the immutable owner and repository IDs and adds the event
name. The role trusts these two subjects.

- `repo:nabedra@361663474/database@1459052341:ref:refs/heads/main:event_name:workflow_dispatch`
- `repo:nabedra@361663474/database@1459052341:ref:refs/heads/main:event_name:schedule`

Pull-request runs have the context `pull_request`, so they never match and the
pull-request gate never receives the role. Runs dispatched on another branch do not
match either.

## Running the checks

- On Actions, only dispatched and scheduled jobs whose check `needs` includes `s3`
  assume the role.
- Locally, export `NABEDRA_TEST_S3_BUCKET` (and `NABEDRA_TEST_S3_REGION` when it is
  not `us-east-1`) with your usual credentials.
- Every run writes under `runs/<request_id>/`, from `NABEDRA_REQUEST_ID` or a new
  UUID, so parallel runs never collide.
- Each run prints `nabedra-s3-requests: <n> prefix=runs/<request_id>/`.

## Budget

`budget.py` sums the month's `s3_requests` fields from the evidence records under
`docs/testing/evidence/records/<yyyy>/<mm>/` and exits 1 above $5, at $0.005 per
1,000 requests:

```
python3 scripts/testing/s3/budget.py --month 2026-11
```

A nightly run of about 20,000 requests costs about $0.10. The nightly workflow
omits checks that declare `needs = ["s3"]` when the budget command fails. The
total uses evidence records available in the checked-out revision.

## Tests

```
cd scripts/testing/s3 && python3 -m unittest
```
