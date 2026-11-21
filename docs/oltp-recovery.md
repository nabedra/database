# OLTP worker recovery and direct-mode rollback

## Restart one registered RF3 worker with its retained mount

Keep the two surviving workers running. They provide the Raft majority and hold
accepted queue records for the current run. A cluster-wide cold start abandons
outstanding requests even though committed data survives.

This procedure applies to a registered worker with a Docker-profile primary
mount and an intact retained directory. Keep the original mount, worker ID,
pool ID, bootstrap ID, local system placements, and certificate.

1. Stop or fence the old process and make sure it cannot start again. Wait
   until Control removes its incarnation from the ready set and assigns
   another owner. On a surviving worker, inspect
   `GET /health/ready` and `cluster.connection`. The latter is an operation
   on `POST /v1/cluster/operations` with the admin token.
2. Send the following operation to a surviving worker. Replace the angle
   brackets with the current values. Use the same UUID for `request_id` and
   `payload.id`. If the response is lost, retry with the same UUID and body.
   If Control explicitly refuses preparation while removing the old worker,
   wait for the removal to complete and submit a new request.

   ```json
   {
     "request_id": "<preparation UUID>",
     "operation": "worker.restart.prepare",
     "payload": {
       "id": "<preparation UUID>",
       "run_id": "<current run UUID>",
       "previous": {
         "worker_id": "<worker UUID>",
         "incarnation": "<old incarnation UUID>",
         "enrollment_revision": 1,
         "certificate_sha256": "<certificate digest>"
       },
       "next": {
         "worker_id": "<worker UUID>",
         "incarnation": "<new incarnation UUID>",
         "enrollment_revision": 2,
         "certificate_sha256": "<certificate digest>",
         "endpoint": "<existing peer address>",
         "server_name": "<existing TLS name>"
       },
       "pool_id": "<retained primary pool UUID>",
       "mount_generation": 2
     }
   }
   ```

   The revisions and mount generation shown assume the previous values were
   both 1. Increment each from its actual previous value.
3. Supply the prepared `next` as `node.enrollment` in the bootstrap file.
   Set `node.rolling` to
   `{"preparation_id":"<preparation UUID>","run_id":"<current run UUID>"}`.
   Keep the previous enrollment in `node.control_enrollments` for the
   restarting worker. Use the current peer enrollments for the others.
   Update `node.mount.worker` to the prepared identity and
   `node.mount.generation` to the prepared generation. Start
   `nabedra-db worker --bootstrap /path/to/bootstrap.json`.
4. Check that all three workers report readiness in the original run.
   Check `cluster.connection` for the new incarnation and
   `storage.replica.placement` for the three published voters of each
   affected Data range. Verify acknowledged rows and poll outstanding
   receipts using their original run and request IDs. Retry an uncertain
   mutation with its original ID and body.

Allow for a leader election and queue ownership change after a kill. Clients
can observe retryable failures during that transition. There is no guaranteed
production failover time. The process check uses a 60-second budget between
successful reads of a separate range.

If recovery fenced a manual split, retry the split operation with its
original request ID and body. The current owner settles any committed result
or returns `conflict` with `not_applied` for a split it aborted. After an
abort, issue a new request ID if the source range still needs splitting.

Control requires the previous incarnation to leave the ready set. Enrollment
then requires proof from the retained mount key and exclusive ownership of
the directory. These checks prevent two processes from serving one Raft
identity or an empty directory from discarding its vote and log history.

## Lost mount and storage damage

Keep the surviving quorum running after permanent loss of a registered
system voter's primary volume. Keep the lost worker's bootstrap file and
certificate out of use. Start a retained restart only with the original
intact mount. Reusing the old bootstrap identity on a blank mount would
discard that voter's vote and log history.

Read `cluster.connection` on a surviving worker to check the run and ready
worker count. Use `storage.replica.placement` to see the recorded placement.
The placement can still list the lost voter, so its replica count alone
cannot establish how many voters are available. Keep the survivors running
while reconciling receipts in the original run. If a second failure prevents
quorum, wait for a retained voter to return. A full backup restore creates
a new run and recovers data through the archived cut.

### Check and record a system replacement intent

The registered RF3 admin endpoint can check quorum and record a replacement
intent for a fresh Docker-profile mount. Same-run recovery still requires
learner startup, promotion, and revocation of the lost identity. Keep using
the surviving quorum or a full backup restore for service recovery.

1. Before losing a voter, upgrade every Control voter and send
   `worker.system_replacement.enable` with an empty payload to the Control
   leader. Control writes the activation only after every current voter
   advertises `control.system-replacement.v1` over its authenticated peer
   connection. Keep binaries that decode this capability after activation.
2. After permanent loss, wait for Control to remove the exact lost
   incarnation from the ready set. Then send
   `worker.system_replacement.check` to a surviving worker with the current
   `run_id` and the lost `previous` identity. Use the same fields as the
   retained restart request. The check refuses a worker that still answers
   an authenticated probe or recently acknowledged a Raft heartbeat. It
   requires the current leader and a fresh acknowledgment from the other
   surviving voter in every affected group, including Data. A successful
   result describes an observation of up to 64 affected groups.
3. To record the intent, send `worker.system_replacement.prepare` to the
   current Control leader. Include the same `run_id` and `previous`, plus
   `id` equal to the request UUID, `next` with the fresh worker enrollment,
   and `mount` with the exact Docker-profile mount expectation. Use a fresh
   worker ID, incarnation, certificate, and pool ID. Set the enrollment
   revision and mount generation to 1. The leader rechecks quorum before
   recording the exact old voters and planned learner for each group.
   Retry the same UUID and body to read the original committed result. A
   different intent for the same lost worker returns a conflict.

Send these operations to `POST /v1/cluster/operations` with the admin
token. A successful preparation leaves the original voters recorded in
Control. Continue to keep the old process fenced outside the cluster.

For a quarantined Data replica or refused secondary pool, check
`quarantined_groups` under `storage` and `refused_pools` in
`GET /v1/runtime/stats`. A registered worker that can still run reports the
damage to Control. When a healthy eligible pool and a surviving quorum are
available, Control installs and catches up a fresh learner before promotion.
It commits the change with both the old and new Raft majorities and publishes
the stable voter set. A retry after interruption uses the same planned
removed and replacement replicas.
The scheduler leaves system-group repairs and unreadable primary pools for
operator recovery. A quarantined replica stays offline if a second loss
prevents quorum. Keep damaged storage until the replacement is committed.

## Roll back request routing and recover from a full archive

After `data.direct.v1` activates, keep a binary that understands the
capability and its Raft entries. Binary downgrade is unsupported. Set
`execution.direct_oltp` to `false` on workers and `topology.direct_oltp`
to `false` on remote gateways to route new eligible requests through the
queue. The capability stays active. Same-ID mutation retries still check
their original result before queue admission. For standalone `serve`, put
`direct_oltp: false` in its complete execution config and restart with that
config and `--cold-start`.

Take a full archive while direct commits are active. Incremental export
reads Status decisions and would omit direct writes. An incremental request
returns `UnsupportedOperation` with the instruction to take a full backup,
even with direct routing switched off.

For a stopped standalone database, use the same release for backup,
restore and startup. Set `NABEDRA_TOKEN` before running these commands.

```sh
nabedra-db database backup --database default --data-dir /srv/nabedra/data \
  --output /srv/nabedra/backups/full --cold-start
nabedra-db database restore --database default --input /srv/nabedra/backups/full \
  --data-dir /srv/nabedra/restored --execution-config /etc/nabedra/queued.json
nabedra-db serve --database default --data-dir /srv/nabedra/restored \
  --execution-config /etc/nabedra/queued.json --cold-start
```

Use an empty restore destination and verify the returned cut and restored
rows before sending traffic there. A full archive carries the Data images
and the request-ID records needed to replay direct writes at the cut.
Queue records in RAM are outside the archive.

In a registered deployment, use the queue owner's `archive.create` admin
operation with the configured archive repository and omit `previous`. The
payload includes `owner` with a UUID `backup_id` and `attempt` of 1, and
`output` with the absolute archive directory inside the repository. The
directory name is the UUID formed from the first 16 bytes of SHA-256 over
the compact JSON array `[domain_bytes, cluster_id, owner]`.
`domain_bytes` is the array of UTF-8 byte values for
`nabedra/native-backup-id/v1`. Encode the owner object with `backup_id`
before `attempt`. Preserve the full archive and its encryption keys, if
configured.
