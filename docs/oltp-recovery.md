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

### Replace a permanently lost system voter

1. Before losing a voter, upgrade every Control voter and send
   `worker.system_replacement.enable` with an empty payload to the Control
   leader. The leader checks the authenticated
   `control.system-replacement.hardening.v1` advert from every Control voter
   before it enables the procedure. Reissue this operation after upgrading a
   cluster that already enabled replacement. Use binaries that advertise the
   same capability for future worker enrollment.
2. Fence the lost process outside the cluster. Keep both surviving voters
   and their mounts running. Wait for Control to remove the lost incarnation
   from the ready set. Send `worker.system_replacement.check` to a survivor
   with the current `run_id` and the lost `previous` identity. The same identity
   fields apply as for a retained restart. Control checks for authenticated
   activity from the lost worker and requires fresh evidence from both
   survivors in the planned groups. When the inventory has up to 64 groups,
   the procedure includes all affected groups. With a larger inventory, the
   procedure first restores system groups. Control then scans the inventory in
   bounded pages and queues the revoked worker's Data replicas for the
   existing damage replacement procedure. Check those ranges separately for
   stable RF3.
3. Provision a fresh worker ID, incarnation, TLS certificate, and Docker-profile
   pool. Use revision 1 and mount generation 1. Provision an independent
   Ed25519 signing key on that mount at `NABEDRA_RESTART_KEY` with mode
   `0600`. This file contains the raw 32-byte seed. The `mount_public_key`
   field is the 32-byte public key encoded in hexadecimal.

   These commands provision the signing seed and print its public key. Set
   `replacement_mount` to the provisioned mount path before running them.

   ```sh
   umask 077
   key_file="$(mktemp)"
   openssl genpkey -algorithm Ed25519 -out "$key_file"
   openssl pkey -in "$key_file" -outform DER | tail -c 32 > "$replacement_mount/NABEDRA_RESTART_KEY"
   openssl pkey -in "$key_file" -pubout -outform DER | tail -c 32 | xxd -p -c 32
   rm -f "$key_file"
   ```

4. Send `worker.system_replacement.prepare` to the Control leader. Include
   `run_id`, `previous`, `id` equal to the request UUID, `next` with the new
   enrollment, `mount` with the Docker mount expectation, and
   `mount_public_key`. The response contains the committed intent and exact
   group plan. An identical retry returns the same record. To retire an
   unmounted intent, send `worker.system_replacement.abandon` with
   `{"id":"<intent UUID>"}`. An identical retry confirms abandonment. Prepare
   a fresh candidate with a new worker ID and certificate. When a cold fence
   publishes a new run, Control releases the unmounted execution slot. A
   legacy keyless intent from an earlier run is retired when a new
   preparation replaces it.
5. In the new worker's bootstrap file, put the returned record in
   `node.system_replacement`. Set `node.enrollment`, `node.mount`,
   `node.directory`, `node.pool_id`, `node.scheduler_node_id`, TLS paths, and
   binds for the new worker. Copy the current pre-replacement system placements
   and Control enrollments into `node.placements` and `node.control_enrollments`.
   For format 2, keep the original policy placements in
   `node.bootstrap_placements` when the current placements differ from the
   original placement. Use the current run as `node.initial_run` and omit
   `node.cold`, `node.rolling`, `node.restore`, and secondary pools. Start
   `nabedra-db worker --bootstrap /path/to/bootstrap.json`.
6. Poll `worker.system_replacement.status` on a survivor with the same
   `run_id` and `previous` as in the check. `mounted` means Control checked
   the signed mount proof, revoked the lost worker ID and certificate, and
   claimed the planned groups. `complete` means Control published every stable
   planned voter set. Each group uses Raft's joint transition and requires
   learner catch-up. Group changes wait for the necessary quorum if a
   survivor pauses. A mounted intent can resume with the original mount,
   identity file, and pool contents. Startup checks Control before opening
   the pool, and a wiped mount is refused even if the signing key survived.
   After a complete shutdown during a mounted replacement, start the two
   survivors with one cold-fence profile. Keep their certificates and advance
   their incarnations, enrollment revisions and mount generations. Use the
   intent's target voters for planned system groups and the current voters
   for other groups. Keep the original policy placements. Include the
   candidate's original enrollment with the survivors' new Control
   enrollments and operator pins.

   Wait for `registered cold run <UUID> published` in a survivor's log. Then
   start the candidate on its original mount with its original enrollment and
   replacement record. Add the same cold-fence profile and update the
   survivors' enrollments in the candidate's pre-replacement Control pins.
   The candidate had never joined the old RAM queue and resumes promotion in
   the new run. Use binaries that support this procedure on all retained
   workers. Keep using the original intent's `run_id` for replacement status.
   The cold fence abandons the old RAM queue and preserves committed mutations
   and request IDs.

   Once the procedure completes, a later permanent loss uses a fresh intent
   naming the replacement worker. If the replacement mount is lost while
   promotion is still in progress, preserve the survivors and their quorum.
   Abandonment applies only before mount commits.
7. Check `GET /health/ready` on the replacement. Before it serves requests, it
   verifies its applied Raft voter sets and completes the replicated RAM queue
   join. Poll outstanding receipts in the original run and retry uncertain
   mutations with their original ID and body. Save the new placements and
   enrollments for future retained restarts. Keep the original format 2
   `bootstrap_placements` and remove `system_replacement` from subsequent
   retained-restart bootstrap files. Once queue join begins, a process restart
   requires the normal retained-mount procedure with a new incarnation.

Send the admin operations to `POST /v1/cluster/operations` with the admin
token. A prepared intent authorizes only its exact candidate. Workers check
each peer request against the Control revocations they know, including on an
existing connection. The check is an in-memory lookup and makes no Control
call:

- A Control host checks its applied Control replica.
- A Data-only worker checks a local revocation view: the revoked worker IDs
  and certificate digests. At startup it reads the full list from its
  configured Control peers. A majority must return the same pages, otherwise
  startup fails. It then repeats the revocation read once per second and
  follows changes to Control's published stable voter set. A majority of the
  last known voters must confirm the new placement, and the new set must
  retain a majority of those voters.
- When the view learns a revocation, the worker closes that identity's
  sessions at once and refuses its new ones. A revocation committed while a
  majority of the worker's current Control voter set is reachable takes effect
  within about 1 second, plus up to 2 seconds for a slow read.
- A refresh can fail, for example when the worker cannot reach a Control
  majority. The view then keeps every revocation it already learned, since
  Control never deletes one. The worker keeps serving Data replication and
  direct requests. After 5 seconds without a successful refresh, it logs one
  warning, `Data-only revocation view is stale`, and keeps retrying every
  second. The view stays stale until a majority refresh succeeds, which is
  also logged.

A revocation the view has not learned yet is still not trusted for long:

- Control commits a revocation only through a Control majority, and only
  after the survivors proved the lost worker unreachable.
- The revoked worker is removed from every Raft group it shared, so it cannot
  commit to them.
- It is fenced out of the RAM queue.

What the view guards against is a revoked process that comes back, sending
non-Raft requests to a Data-only worker during the window. If you cannot
restore a Data-only worker's Control connectivity, stop the revoked worker's
host or isolate it from the network.

Update Data workers' Control placements and enrollments before their next
restart. While running, the Data worker counts the new voter after it
confirms the published placement. If a Data-only worker misses enough
replacements to lose contact with a majority of its last known voter set,
restart it with current Control placements and enrollments.

Control accepts up to 1,024 committed worker revocations. It refuses the
next mount before enrolling the candidate or changing any placement. The
revocation index uses a fixed progress-memory reservation and can also
load the extra revocation a prior release could commit past the limit.

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
