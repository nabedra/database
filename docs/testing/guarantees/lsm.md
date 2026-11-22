# LSM guarantees

Module: lsm
Level: L3

OLTP depends on the durability of the `nabedra-kv` storage engine. LSM01
names the seeded storage campaign. Run the same cut assertions with threaded
services and with simulated services, then compare the reopened store with
a serial storage oracle. See the
[native storage contract](../oltp-contract.md#native-storage-engine) and
[design plan](../../../plans/native-storage-engine.md).

| ID | Guarantee | Failure model | Scenarios | Checks | n/a |
| --- | --- | --- | --- | --- | --- |
| LSM-G01 | Every write acknowledged as synced survives a crash. A synced group also makes all earlier unsynced writes durable. | The process can die during journal, flush, or compaction work. Power loss can drop or reorder unsynced sectors while synced sectors survive. | LSM01, NS04, NS05, NS08, NS16 | U S D R | M: The storage campaign uses an executable serial oracle; H: The oracle checks recovered keys and sequences directly; F: Generated storage operations are cut at filesystem boundaries; P: This row covers durability; K: Enumerated cuts and repeated process kills exercise the failure points |
| LSM-G02 | After an NS04 crash cut, recovery yields an exact submitted prefix from the last synced acknowledgment through the last submission. Reopening again preserves that prefix. | A crash can interrupt any write, sync, manifest update, rename, segment change, or relocation boundary. | LSM01, NS04 | U S D | M: The prefix comparison uses the serial storage oracle; H: The recovered image is compared directly; F: The campaign varies filesystem cuts; R: FaultFs enumerates cuts that a real process kill cannot target precisely; P: This row covers recovered state; K: Boundary enumeration exercises the crash model |
| LSM-G03 | After an NS05 power-loss cut, a torn tail recovers to an exact allowed prefix. Loss of a record certified durable by a later record triggers scoped quarantine or pool refusal. | Power loss can discard any subset of unsynced sectors and reorder those it retains. Synced sectors survive in this model. | LSM01, NS05, NS06, NS07 | U S D | M: The storage oracle and explicit recovery classes define the outcomes; H: The recovered image and quarantine or refusal are checked directly; F: The campaign injects sector loss and corruption at known boundaries; R: FaultFs provides control over sector loss and ordering; P: This row covers recovery correctness; K: Enumerated cuts exercise the power-loss model |
