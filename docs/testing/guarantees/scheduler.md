# Scheduler guarantees

Module: scheduler
Level: L1

U03 runs seeded event sequences against the pure scheduling and request
queue transitions. The properties below cover capacity, fairness, bounded
progress, and full worker shutdown. They use explicit events and virtual
time. See the [scheduler contract](../scheduler-contract.md) and the
[scheduling plan](../../../plans/resource-aware-scheduling.md).

| ID | Guarantee | Failure model | Scenarios | Checks | n/a |
| --- | --- | --- | --- | --- | --- |
| SCHED-G01 | Admitted resources never exceed capacity. | Capacity can change during admissions. Stale advertisements, saturation, failed reservations, and cancellations can overlap. | U03, C01, C02, C03, C04, C06, C07, M01, M02, Q02, F03 | U | M: Properties exercise the pure transitions; S: Seeded events drive the pure scheduler directly; H: Counters are checked after each event; F: The property uses structured events; D: Declared capacity gives the limit; R: Process checks are covered by OLAP-G07 and OLTP-G09; P: Admitted capacity is a correctness limit; K: Each event checks the bound |
| SCHED-G02 | Dominant resource shares stay within the fairness bound. | Unequal CPU and memory demand, heterogeneous job times, skew, and delayed workers can affect the eligible set. | U03, C05, C06, M03 | U | M: The seed supplies admission and release sequences; S: The pure scheduler needs no simulated transport; H: The check reads shares and the eligible set directly; F: Events use structured demand and capacity; D: The scheduling policy gives the fairness rule; R: Process checks are covered by OLAP-G07; P: The bound uses grants and shares; K: Finite grant rounds exercise the fairness rule |
| SCHED-G03 | An eligible request never starves beyond the allowed number of grant rounds. | Small jobs can keep arriving while memory-heavy work waits, and child jobs or reductions can encounter full compute pools. | U03, C05, C06, C07, M03, Q01 | U | M: Generated grant sequences exercise bounded progress; S: The property releases resources through direct events; H: The fixture counts rounds to dispatch or aging priority; F: The arrivals and releases are structured; D: The fair-round and 32-round aging rules give the bounds; R: Process checks are covered by OLAP-G07; P: The bound counts grant rounds; K: A bounded sequence reaches the aging threshold |
| SCHED-G04 | Once every worker stops, the next run abandons old pending requests and preserves committed mutations. | Every worker can stop with work outstanding. A partition or missed heartbeat can look like a shutdown to one worker. | U03, R02, R04, R05 | U | M: Seeded worker events exercise the run transition; S: The property drives the queue state directly; H: The old receipt, durable decision, and new queue are inspected; F: Worker loss uses explicit events; D: The surviving-holder and committed-decision rules give expected outcomes; R: Process checks are covered by OLAP-G07 and OLTP-G07; P: This row covers abandonment and durability; K: A finite stop and restart sequence exercises the run boundary |
