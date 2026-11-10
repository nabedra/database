
# Storage Module

Pluggable durable object storage (S3, GCS), local caching,
columnar data blocks, snapshots and compaction.

Workers write immutable blocks using the table's format. Coordinators publish
committed block references in the HA catalog before acknowledging a write.
Updates/deletes create new versions; compaction merges them. Parquet files are
never edited in place. Verify uploads before publishing references; retries must
not overwrite committed objects. Use checksums and encrypted provider storage.

Backups preserve a consistent catalog snapshot and every referenced object. Restore
them together and test recovery against defined RPO/RTO. Version/retain objects;
`vacuum` must honor snapshots, active readers and backup retention before deleting.
An upload without a committed reference is an orphan, never an acknowledged write.

File import/export shares the codecs in [data.md](data.md). CSV input can feed a
Parquet table; reading external files does not create a managed table.

```python
class S3:
  bucket: str
  region: str
  prefix: str
  endpoint: str | None
  credentials: dict[str, str] | None

class GCS:
  bucket: str
  prefix: str
  credentials: dict[str, str] | None

class ObjectStore:
  backend: S3 | GCS

  def put(self, key: str, data: bytes) -> None: ...
  def get(self, key: str) -> bytes: ...
  def delete(self, key: str) -> None: ...
  def list(self, prefix: str) -> list[str]: ...
  def exists(self, key: str) -> bool: ...

class Cache:
  capacity: int
  used: int
  policy: str

  def get(self, key: str) -> bytes | None: ...
  def put(self, key: str, data: bytes) -> None: ...
  def evict(self, key: str) -> None: ...
  def prefetch(self, keys: list[str]) -> None: ...
  def clear(self) -> None: ...
  def stats(self) -> dict: ...

class StorageManager:
  backend: S3 | GCS
  cache: Cache

  def read(self, partition: str) -> Iterator[Row]: ...
  def write(self, partition: str, rows: Iterable[Row]) -> None: ...
  def compact(self, table: str) -> Job: ...
  def vacuum(self, table: str) -> Job: ...
  def snapshot(self, name: str) -> Snapshot: ...
  def migrate(self, backend: S3 | GCS) -> Job: ...
  def stats(self) -> dict: ...

class Snapshot:
  id: str
  timestamp: float
  manifest: dict

  def restore(self) -> Job: ...
  def delete(self) -> None: ...
```

## Minimal usage

```python
storage = S3(
  bucket="my-database",
  region="eu-west-1",
  prefix="production/",
  credentials={"access_key": "...", "secret_key": "..."}
)

# Passed into cluster creation.
cluster = cloud.create("production", storage=storage)

cluster.storage.compact("events")
cluster.storage.cache.prefetch(["partition-1"])
```
