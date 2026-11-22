# LIVE2 Cache provider preflight

Source `7e110cb95ee05a5fb742f677bdf17d580733d7d0`, region `us-east-1`.
AWS denied EC2 availability-zone discovery and CloudFormation stack discovery.
The task submitted zero infrastructure mutations and launched zero instances.

| Check | Status | Fixture still required |
| --- | --- | --- |
| K04a single-host parity | Not run | Dedicated EC2 host with sequential pinned Nabedra and Valkey runs, matched resource limits and the paired load sweep. |
| K04b AZ-loss performance and surviving headroom | Not run | Matched three-AZ clusters with client arrival and completion traces, survivor CPU, RAM and network measurements, and replica coverage. |
| K05 repeated growth and drain | Not run | Real provider deployment with 3, 6 and 12 primary groups, live traffic, placement checks, a measured drain and failure recovery. |

The existing Cache provider runner expects a CloudFormation stack with SSM
access. Its default template requires a bounded deployment variant for LIVE2's
instance shutdown timers and cleanup of retained controller EBS volumes.
[Sanitized preflight output](preflight.txt)
