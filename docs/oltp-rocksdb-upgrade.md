# Upgrade a RocksDB-era OLTP cluster

Export a full archive from the running old cluster and restore it into a new
cluster on empty volumes. The archive carries committed rows, table UUIDs,
schemas, range boundaries and secondary indexes. Keep the old binary and volumes
available until you finish verification.

1. Before cutover, deploy `node.archive_repository` on every old worker using
   the old release. Use the same canonical absolute path to a shared directory,
   owned by the service user with mode `0700`. Check that the cluster is healthy.
2. Pause application writes and schema changes, and let accepted requests reach
   a terminal outcome. Record the database name, source cluster ID and a newly
   generated backup UUID. With `NABEDRA_ADMIN_TOKEN` set, run the exporter from
   this checkout. Use the worker admin URLs over a protected network or tunnel.

   ```sh
   python3 scripts/oltp_upgrade_export.py \
     --database "$DATABASE" --cluster-id "$SOURCE_CLUSTER_ID" \
     --backup-id "$BACKUP_ID" --repository /srv/nabedra/archives \
     --worker "$WORKER_1" --worker "$WORKER_2" --worker "$WORKER_3"
   ```

   The exporter prints the archive path on the workers and returns the full
   manifest on success. If the connection drops, rerun the same command with
   the same backup UUID. Preserve the whole archive directory, including
   `manifest.json` and its subdirectories. Make it readable by each new worker
   under the same private-directory rules.
3. Prepare three new mounted pools and a registered bootstrap configuration.
   Use a new cluster ID, run ID, bootstrap ID, worker IDs and pool IDs, and
   `pool_format_version: 2`. Keep the same database name. In `node.placements`,
   use `manifest.json`'s `scope.control_group`, `scope.timestamp_group`,
   `scope.initial_status_group` and `scope.catalog_group` as the respective
   group IDs. If the scope lists `status_partitions`, carry their IDs in order
   into the corresponding Status placements. Give every group the same three
   new worker and pool pairs. Set this on all three worker bootstraps:

   ```json
   {
     "restore": { "archives": ["/srv/nabedra/archives/<archive-id>"] }
   }
   ```

   Put `restore` under `node`, then start all three new workers with
   `nabedra-db worker --bootstrap <worker-config.json>`. Each worker verifies
   the archive before it installs the committed data. If restore is interrupted,
   stop that target cluster and retry with the same archive on fresh empty
   volumes. Keep the interrupted volumes out of service. Remove the `restore`
   block from later restart configurations after a successful restore.
4. Wait for all three replicas to become ready. Through the new API gateway,
   compare table UUIDs, schemas, row counts and indexed query results against the
   source. Switch clients to the new endpoint and re-enable writes.

The write outage lasts from the pause in step 2 through export, restore and
verification. Rehearse those steps with representative data to size the outage.
Before re-enabling writes, you can roll back by keeping clients on the old
cluster. Writes made after cutover require a separate rollback plan.

For an S3-backed archive, the same history is stored as checked objects, and
the locator remains in the private worker archive directory. Preserve the
locator and objects together. Registered restore accepts the original `s3`
repository configuration under `node.restore` on an `oltp-backup-s3` build.
Supply the original archive decryption keys when applicable. The upgrade CI job
exercises the shared-directory procedure.
