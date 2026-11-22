# Cache guarantees (fixture)

Module: cache
Level: L2

Prose before the table is ignored.

| ID | Guarantee | Failure model | Scenarios | Checks | n/a |
| --- | --- | --- | --- | --- | --- |
| CACHE-G01 | Failover never invents values | primary crash | K01 K02 | U S R P | R: cloud-deferred (EKS soak); P: hardware-deferred (absolute latency) |
| CACHE-G02 | Expired keys stay expired | clock jumps | K03 | U D | |
| CACHE-G03 | Leases are linearizable within the clock bound | clock error | K04 | U H | M: no protocol state beyond the lease, covered by U |
