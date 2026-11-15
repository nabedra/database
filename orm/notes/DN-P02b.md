# DN-P02b: cluster policy and status client

Status: implemented; independent specification and quality reviews pending.

Added only `orm/src/meshdb/cluster.py`, `orm/tests/test_cluster.py`, and this note. No agents, commits, branches, cloud calls, environment-file access, dependency changes, or edits to other modules were used.

`meshdb.cluster.Cluster(api_url, admin_token, *, timeout=30.0, ca_file=None)` constructs the reviewed `_ControlTransport` without sending a request. `scale(nodes)` submits fixed policy acceptance, `autoscale(*, min_nodes, max_nodes)` submits bounds, and `status()` reads status. All calls use the private transport once. Existing server errors and transport failures propagate unchanged. Invalid local policy arguments raise ValidationError before I/O. Boolean values cannot substitute for integers.

Policy replies must contain exactly the five policy fields, satisfy revision, mode and bound rules, and match the submitted target or bounds. A malformed success raises UnknownOutcomeError with the submission request ID and status_code 200 because persistence may have succeeded. Invalid status success raises ProtocolError with the read request ID and status_code 200.

Status validation checks exact fields and types at every record level, canonical lowercase UUIDs, the existing identifier name rule, bounded resource IDs, capacities, queues, heartbeat ages, policy consistency, and state counts. It returns detached ordinary dictionaries and lists. Node arrays normalize by node_id and intent arrays by request_id; combined records above 4096 fail without truncation. Duplicate node IDs, nonnull incarnations or resource IDs within membership, and duplicate intent request IDs fail. Multiple retained intents may reference the same node or resource; pending intent nodes need not yet appear in membership. Stopped nodes do not enter ready/starting/draining/lost counts. Neither desired capacity nor capability flags impose extra membership constraints.

Intent error codes accept null or machine tokens of 1..256 ASCII characters from letters, digits, underscore, dot, colon and hyphen. This rejects free-form provider sentences and control characters without defining a closed list of server codes. The task did not prescribe an exact error-code bound/alphabet; this interpretation is explicit for review. Resource IDs remain opaque nonempty strings of at most 256 UTF-8 bytes without Unicode Cc control characters.

Initial failure command:

```sh
PYTHONPATH=orm/src:orm/tests python3.11 -B -m unittest test_cluster.ClusterTests.test_operations_use_control_endpoint_and_exact_payloads -v
```

Failed as intended with one assertion failure: `Cluster must validate policy acceptance over the control endpoint`. The module did not exist yet. The test then exercises actual HTTP acceptance and status calls once implementation exists.

Passing focused command:

```sh
PYTHONPATH=orm/src:orm/tests python3.11 -B -m unittest test_cluster -v
```

All six tests passed in 0.704 seconds. They cover exact endpoint, envelope operation and payload for all three methods; construction/local rejection without server requests; fixed and autoscale boundaries; malformed/mismatched policy success diagnostics; representative status shape, boolean, capacity, identity, resource, count and code failures; sorted records with retained completed intents and stopped nodes; and propagation of server rejection and lost replies without retries. The peer is a real loopback HTTP fixture, not a cluster runtime. Transport envelope, JSON, token, TLS and deadline matrices are left to the existing transport tests.

Syntax command:

```sh
python3.11 -m py_compile orm/src/meshdb/cluster.py orm/tests/test_cluster.py
```

Exited 0. Scoped `git diff --check` also exited 0.

The class remains unexported at package level. Full `scale(wait, timeout)` behavior belongs to P02c and is absent here. No server, provisioning callback, data ORM, or real-cluster behavior is implemented or claimed by this slice.
