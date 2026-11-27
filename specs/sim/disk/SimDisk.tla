------------------------------ MODULE SimDisk ------------------------------
(***************************************************************************)
(* The simulator's disk: FaultFs and the simulated volume layer, as        *)
(* shipped on main 7dadd41d.                                               *)
(*                                                                         *)
(*   server/rt/src/fs/fault.rs     FaultFs, FaultFile, power_loss_with     *)
(*   server/rt/src/sim/volumes.rs  World::{mount,restart_with,wipe}        *)
(*   server/rt/src/sim.rs          World::crash (the disk cut)             *)
(*                                                                         *)
(* Design: phase-9-disk-seam.md sections 3.2 to 3.4 (P9-RT a and c).       *)
(* Scenarios: NS04, NS05, NS15, O11, O17 precondition. Guarantees that     *)
(* assume this model: LSM-G01..G03, OLTP-G01, OLTP-G08, EVT-G01.           *)
(*                                                                         *)
(* One node, one volume, one directory D that the mount made durable      *)
(* (create_dir_all_durable). A file is two 512-byte sectors, which         *)
(* together form one 4 KiB device page. Byte values are abstract: Zero,   *)
(* the written values Vals, Garbage, and Torn(old, new).                   *)
(*                                                                         *)
(* Faults are budgeted actions: a crash (World::crash), a halt before the  *)
(* crash (FaultFs::crash_after), restart with each cut image (kill, crash, *)
(* 512-byte power loss, 4 KiB destroying power loss), an fsync EIO, and    *)
(* disk loss (World::wipe, then World::restart without the old disk).      *)
(***************************************************************************)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Names,          \* entries of the directory D
    MaxInodes,      \* files a volume generation may create
    Vals,           \* values a write may store in a sector
    Cuts,           \* subset of {"kill", "crash", "power512", "power4k"}
    MaxWrites,      \* writes and truncations
    MaxDirOps,      \* creates, renames and removes
    MaxSyncErrors,  \* injected fsync EIO
    MaxCrashes,     \* World::crash
    MaxWipes,       \* World::wipe
    Bases,          \* inode bases a wipe may draw
    Mutant          \* "none" in the baselines, a mutant name in mutants/*.cfg

NoIno == 0
Inos == 1..MaxInodes
Sectors == 1..2
Zero == <<"Z">>
Garbage == <<"G">>
V(x) == <<"V", x>>        \* the bytes of a write of x
WVals == {V(x) : x \in Vals}
Torn(o, n) == <<"T", o, n>>
RootOffset == 2   \* the mount root's inode number above the volume base

(* ----------------------------------------------------------------------- *)
(* State. The volume (durable and OS layers, pending directory operations)*)
(* and the node (up, halted) are separate; ghosts record what the          *)
(* application was told, for the durability properties.                    *)
(* ----------------------------------------------------------------------- *)
VARIABLES
    up,          \* World: the node incarnation runs
    phase,       \* "up", "aborting" (World::crash between the cut and the abort), "down"
    halted,      \* FaultFs::halt: the image is frozen, every call fails
    osNs,        \* OS namespace: name -> inode
    durNs,       \* durable namespace: name -> inode
    pending,     \* unsynced directory operations, oldest first; each is one atomic op
    osData,      \* inode -> sector values in the page cache
    durData,     \* inode -> sector values on the device
    dirty,       \* inode -> sectors written since the last sync
    minLen,      \* inode -> smallest OS length since the last sync
    nextIno,     \* next inode of this volume generation
    vol,         \* [dev, base, uuid, gen]: the volume identity
    openedGen,   \* volume generation the running incarnation opened
    \* ghosts: what the application observed
    linkAcked,   \* name -> a sync_dir completed after the last op on this name
    fileAcked,   \* inode -> a successful sync covers every write, no EIO since
    sectorAcked, \* inode -> sectors a successful sync covered, not written since
    written,     \* inode -> sector -> values written since the last sync covering it
    dropped,     \* inode -> sectors whose dirty pages an EIO sync dropped (fsyncgate)
    lastSyncOk,  \* inode -> the last sync of the inode succeeded and found no dropped pages
    anchors,     \* stat observations of the mount root: [dev, uuid, ino, gen]
    postCut,     \* writes admitted after the node was marked down (must stay 0)
    zombie,      \* a destructor of the crashed incarnation that tokio has not dropped yet
    lastCut,     \* the cut of the last restart, for properties
    budget       \* fault and workload counters

vars == <<up, phase, halted, osNs, durNs, pending, osData, durData, dirty, minLen,
          nextIno, vol, openedGen, linkAcked, fileAcked, sectorAcked, written,
          dropped, lastSyncOk, anchors, postCut, zombie, lastCut, budget>>

Val0 == {Zero, Garbage} \cup WVals
Val1 == Val0 \cup {Torn(o, n) : o \in Val0, n \in WVals}
Val2 == Val1 \cup {Torn(o, n) : o \in Val1, n \in WVals}
Data == UNION {[1..k -> Val2] : k \in 0..2}
Effect == [name : Names, ino : Inos \cup {NoIno}]

TypeOK ==
    /\ up \in BOOLEAN
    /\ phase \in {"up", "aborting", "down"}
    /\ halted \in BOOLEAN
    /\ osNs \in [Names -> Inos \cup {NoIno}]
    /\ durNs \in [Names -> Inos \cup {NoIno}]
    /\ pending \in Seq(SUBSET Effect)
    /\ osData \in [Inos -> Data]
    /\ durData \in [Inos -> Data]
    /\ dirty \in [Inos -> SUBSET Sectors]
    /\ minLen \in [Inos -> 0..2]
    /\ nextIno \in 1..(MaxInodes + 1)
    /\ vol \in [dev : Nat, base : Bases, uuid : Nat, gen : Nat]
    /\ openedGen \in Nat
    /\ linkAcked \in [Names -> BOOLEAN]
    /\ fileAcked \in [Inos -> BOOLEAN]
    /\ sectorAcked \in [Inos -> SUBSET Sectors]
    /\ written \in [Inos -> [Sectors -> SUBSET WVals]]
    /\ dropped \in [Inos -> SUBSET Sectors]
    /\ lastSyncOk \in [Inos -> BOOLEAN]
    /\ anchors \subseteq [dev : Nat, uuid : Nat, ino : Nat, gen : Nat]
    /\ postCut \in Nat
    /\ zombie \in BOOLEAN
    /\ lastCut \in Cuts \cup {"none", "fresh"}
    /\ budget \in [writes : Nat, dirOps : Nat, syncErrs : Nat, crashes : Nat, wipes : Nat]

(* ----------------------------------------------------------------------- *)
(* Helpers                                                                 *)
(* ----------------------------------------------------------------------- *)
Min(a, b) == IF a < b THEN a ELSE b
Resize(s, l) == [k \in 1..l |-> IF k <= Len(s) THEN s[k] ELSE Zero]
SetAt(s, k, v) == IF k <= Len(s) THEN [s EXCEPT ![k] = v] ELSE Append(s, v)
Touches(op) == {e.name : e \in op}
PendingNames == UNION {Touches(pending[k]) : k \in 1..Len(pending)}

ApplyOp(ns, op) ==
    [n \in Names |-> IF n \in Touches(op) THEN (CHOOSE e \in op : e.name = n).ino ELSE ns[n]]

RECURSIVE ApplyFrom(_, _, _, _)
ApplyFrom(ns, seq, kept, k) ==
    IF k > Len(seq) THEN ns
    ELSE ApplyFrom(IF k \in kept THEN ApplyOp(ns, seq[k]) ELSE ns, seq, kept, k + 1)

\* The operational rule of power_loss_with (fault.rs:1185-1200): ops are visited
\* oldest first; an op survives when its coin says so and it names no entry that a
\* lost op linked or unlinked.
RECURSIVE KeptFrom(_, _, _, _)
KeptFrom(seq, coin, lost, k) ==
    IF k > Len(seq) THEN {}
    ELSE LET depends == Touches(seq[k]) \cap lost # {}
         IN IF coin[k] /\ (~depends \/ Mutant = "no_chain_dependency")
            THEN {k} \cup KeptFrom(seq, coin, lost, k + 1)
            ELSE KeptFrom(seq, coin, lost \cup Touches(seq[k]), k + 1)

\* The declarative rule the properties use: a kept op keeps every earlier op it
\* shares an entry with (rename atomicity plus dependency closure).
Closed(seq, kept) ==
    \A j \in kept : \A i \in 1..(j - 1) :
        (Touches(seq[i]) \cap Touches(seq[j]) # {}) => i \in kept

\* Inode::sync (fault.rs:293-306)
SyncedBytes(i) ==
    LET d0 == Resize([k \in 1..minLen[i] |-> durData[i][k]], Len(osData[i]))
    IN [k \in 1..Len(osData[i]) |->
          IF k \in dirty[i] /\ Mutant # "sync_skips_dirty" THEN osData[i][k] ELSE d0[k]]

Unsynced(i) ==
    \/ dirty[i] # {}
    \/ Len(osData[i]) # Len(durData[i])
    \/ minLen[i] # Len(durData[i])

LengthChanged(i) ==
    Len(osData[i]) # Len(durData[i]) \/ minLen[i] # Len(durData[i])

(* Outcomes of one dirty unit in Inode::after_power_loss (fault.rs:320-387). *)
(* A 512-byte unit is one sector: new, old or torn. A 4 KiB unit is both     *)
(* sectors: new, old, torn at a cut point, zeros, or garbage. Zeros and      *)
(* garbage cover the whole unit, synced sectors included.                    *)
UnitOutcomes(mode) ==
    IF mode = "power512"
    THEN {"new", "old", "torn"} \cup (IF Mutant = "garbage_in_512" THEN {"garbage"} ELSE {})
    ELSE {"new", "old", "torn1", "boundary", "torn2", "zero", "garbage"}

Latest(i, s) == IF s <= Len(osData[i]) THEN osData[i][s] ELSE Zero

\* The new value a power loss may keep for a dirty sector: the latest write (shipped),
\* or under mutant gap_writeback (real writeback) any version written since the last sync.
PickChoices ==
    IF Mutant = "gap_writeback"
    THEN {f \in [Inos -> [Sectors -> WVals \cup {Zero} \cup {Latest(i, s) : i \in Inos, s \in Sectors}]] :
            \A i \in Inos, s \in Sectors :
                f[i][s] \in (IF written[i][s] # {} THEN written[i][s] ELSE {Latest(i, s)})}
    ELSE {[i \in Inos |-> [s \in Sectors |-> Latest(i, s)]]}

\* The bytes of inode i after a power loss, for a given length coin and unit outcomes.
\* `pick` chooses the new value of a sector (the latest write, or under the
\* "writeback" variant any version written since the last sync).
PowerBytes(i, mode, lenCoin, outcome, pick) ==
    LET base0 == IF LengthChanged(i) /\ lenCoin
                 THEN Resize([k \in 1..minLen[i] |-> durData[i][k]], Len(osData[i]))
                 ELSE durData[i]
        limit == Min(Len(base0), Len(osData[i]))
        unitOf(s) == IF mode = "power512" THEN s ELSE 1
        inUnit(u) == {s \in Sectors : unitOf(s) = u}
        live(u) == {s \in inUnit(u) : s <= limit}
        tearAll == Mutant = "tear_clean_sectors"
        isDirty(s) == s \in dirty[i] \/ tearAll
        dirtyUnit(u) == live(u) \cap dirty[i] # {} \/ (tearAll /\ live(u) # {})
        newv(s) == pick[s]
        val(s) ==
            LET u == unitOf(s) IN
            IF s > limit \/ ~dirtyUnit(u) THEN base0[s]
            ELSE CASE outcome[u] = "new"      -> IF s \in dirty[i] THEN newv(s) ELSE base0[s]
                   [] outcome[u] = "old"      -> base0[s]
                   [] outcome[u] = "torn"     -> IF isDirty(s) THEN Torn(base0[s], newv(s)) ELSE base0[s]
                   [] outcome[u] = "torn1"    -> IF s = 1 /\ s \in dirty[i] THEN Torn(base0[s], newv(s)) ELSE base0[s]
                   [] outcome[u] = "boundary" -> IF s = 1 /\ s \in dirty[i] THEN newv(s) ELSE base0[s]
                   [] outcome[u] = "torn2"    -> IF s = 1 /\ s \in dirty[i] THEN newv(s)
                                                 ELSE IF s = 2 /\ s \in dirty[i] THEN Torn(base0[s], newv(s))
                                                 ELSE base0[s]
                   [] outcome[u] = "zero"     -> Zero
                   [] outcome[u] = "garbage"  -> Garbage
    IN IF ~Unsynced(i) /\ ~tearAll THEN durData[i] ELSE [s \in 1..Len(base0) |-> val(s)]

(* ----------------------------------------------------------------------- *)
(* Init: World::mount gives a fresh durable volume (volumes.rs:456).       *)
(* ----------------------------------------------------------------------- *)
FreshNs == [n \in Names |-> NoIno]

Init ==
    /\ up = TRUE
    /\ phase = "up"
    /\ halted = FALSE
    /\ osNs = FreshNs
    /\ durNs = FreshNs
    /\ pending = <<>>
    /\ osData = [i \in Inos |-> <<>>]
    /\ durData = [i \in Inos |-> <<>>]
    /\ dirty = [i \in Inos |-> {}]
    /\ minLen = [i \in Inos |-> 0]
    /\ nextIno = 1
    /\ vol = [dev |-> 1, base |-> CHOOSE b \in Bases : TRUE, uuid |-> 0, gen |-> 0]
    /\ openedGen = 0
    /\ linkAcked = [n \in Names |-> TRUE]
    /\ fileAcked = [i \in Inos |-> TRUE]
    /\ sectorAcked = [i \in Inos |-> {}]
    /\ written = [i \in Inos |-> [s \in Sectors |-> {}]]
    /\ dropped = [i \in Inos |-> {}]
    /\ lastSyncOk = [i \in Inos |-> TRUE]
    \* the pool verifies its mount anchor when it opens (mount.rs VerifiedPoolMount)
    /\ anchors = {[dev |-> 1, uuid |-> 0, ino |-> (CHOOSE b \in Bases : TRUE) + RootOffset, gen |-> 0]}
    /\ postCut = 0
    /\ zombie = FALSE
    /\ lastCut = "none"
    /\ budget = [writes |-> 0, dirOps |-> 0, syncErrs |-> 0, crashes |-> 0, wipes |-> 0]

Live == up /\ ~halted

(* ----------------------------------------------------------------------- *)
(* Application calls (FaultFs, FaultFile). A call on a halted volume fails *)
(* with no effect, which is a stutter here.                                *)
(* ----------------------------------------------------------------------- *)

\* FaultFs::open_file(CreateNew), fault.rs:1427
OpenCreateNew(n) ==
    /\ Live
    /\ osNs[n] = NoIno
    /\ nextIno <= MaxInodes
    /\ budget.dirOps < MaxDirOps
    /\ LET i == nextIno IN
       /\ osNs' = [osNs EXCEPT ![n] = i]
       /\ pending' = Append(pending, {[name |-> n, ino |-> i]})
       /\ osData' = [osData EXCEPT ![i] = <<>>]
       /\ durData' = [durData EXCEPT ![i] = <<>>]
       /\ dirty' = [dirty EXCEPT ![i] = {}]
       /\ minLen' = [minLen EXCEPT ![i] = 0]
       /\ fileAcked' = [fileAcked EXCEPT ![i] = TRUE]
       /\ sectorAcked' = [sectorAcked EXCEPT ![i] = {}]
       /\ written' = [written EXCEPT ![i] = [s \in Sectors |-> {}]]
       /\ dropped' = [dropped EXCEPT ![i] = {}]
       /\ lastSyncOk' = [lastSyncOk EXCEPT ![i] = TRUE]
    /\ nextIno' = nextIno + 1
    /\ linkAcked' = [linkAcked EXCEPT ![n] = FALSE]
    /\ budget' = [budget EXCEPT !.dirOps = @ + 1]
    /\ UNCHANGED <<up, phase, halted, durNs, vol, openedGen, anchors, postCut, zombie, lastCut>>

\* One write of value v to sector s of inode i (FaultFile::write_all_at, fault.rs:1886)
WriteSector(i, s, v) ==
    /\ osData' = [osData EXCEPT ![i] = SetAt(@, s, v)]
    /\ dirty' = [dirty EXCEPT ![i] = @ \cup {s}]
    /\ fileAcked' = [fileAcked EXCEPT ![i] = FALSE]
    /\ sectorAcked' = [sectorAcked EXCEPT ![i] = @ \ {s}]
    /\ written' = [written EXCEPT ![i][s] = @ \cup {v}]
    /\ dropped' = [dropped EXCEPT ![i] = @ \ {s}]

WriteAt(n, s, v) ==
    /\ Live
    /\ osNs[n] # NoIno
    /\ s <= Len(osData[osNs[n]]) + 1
    /\ budget.writes < MaxWrites
    /\ WriteSector(osNs[n], s, v)
    /\ budget' = [budget EXCEPT !.writes = @ + 1]
    /\ UNCHANGED <<up, phase, halted, osNs, durNs, pending, durData, minLen, nextIno,
                   vol, openedGen, linkAcked, lastSyncOk, anchors, postCut, zombie, lastCut>>

\* FaultFile::set_len (fault.rs:1933), truncation only
SetLen(n, l) ==
    /\ Live
    /\ osNs[n] # NoIno
    /\ l < Len(osData[osNs[n]])
    /\ budget.writes < MaxWrites
    /\ LET i == osNs[n] IN
       /\ osData' = [osData EXCEPT ![i] = [k \in 1..l |-> @[k]]]
       /\ minLen' = [minLen EXCEPT ![i] = Min(@, l)]
       /\ fileAcked' = [fileAcked EXCEPT ![i] = FALSE]
       /\ sectorAcked' = [sectorAcked EXCEPT ![i] = {s \in @ : s <= l}]
    /\ budget' = [budget EXCEPT !.writes = @ + 1]
    /\ UNCHANGED <<up, phase, halted, osNs, durNs, pending, durData, dirty, nextIno,
                   vol, openedGen, linkAcked, written, dropped, lastSyncOk, anchors,
                   postCut, zombie, lastCut>>

\* FaultFile::sync_data and sync_all (fault.rs:1859): both make data and length durable.
SyncData(n) ==
    /\ Live
    /\ osNs[n] # NoIno
    /\ LET i == osNs[n] IN
       /\ durData' = [durData EXCEPT ![i] = SyncedBytes(i)]
       /\ dirty' = [dirty EXCEPT ![i] = {}]
       /\ minLen' = [minLen EXCEPT ![i] = Len(osData[i])]
       /\ fileAcked' = [fileAcked EXCEPT ![i] = (dropped[i] = {})]
       /\ sectorAcked' = [sectorAcked EXCEPT ![i] = (1..Len(osData[i])) \ dropped[i]]
       /\ written' = [written EXCEPT ![i] = [s \in Sectors |-> IF s \in dropped[i] THEN @[s] ELSE {}]]
       /\ lastSyncOk' = [lastSyncOk EXCEPT ![i] = (dropped[i] = {})]
    /\ UNCHANGED <<up, phase, halted, osNs, durNs, pending, osData, nextIno, vol,
                   openedGen, linkAcked, dropped, anchors, postCut, zombie, lastCut, budget>>

\* An injected EIO on sync (Fault::on(SyncData), fault.rs:1868): fsyncgate. The dirty
\* sectors are marked clean without reaching the device.
SyncFails(n) ==
    /\ Live
    /\ osNs[n] # NoIno
    /\ dirty[osNs[n]] # {}
    /\ budget.syncErrs < MaxSyncErrors
    /\ LET i == osNs[n] IN
       /\ dirty' = [dirty EXCEPT ![i] = {}]
       /\ dropped' = [dropped EXCEPT ![i] = @ \cup dirty[i]]
       /\ fileAcked' = [fileAcked EXCEPT ![i] = FALSE]
       /\ lastSyncOk' = [lastSyncOk EXCEPT ![i] = FALSE]
    /\ budget' = [budget EXCEPT !.syncErrs = @ + 1]
    /\ UNCHANGED <<up, phase, halted, osNs, durNs, pending, osData, durData, minLen,
                   nextIno, vol, openedGen, linkAcked, sectorAcked, written, anchors,
                   postCut, zombie, lastCut>>

\* Fs::rename (fault.rs:1627): one atomic pending op that unlinks `from` and links `to`.
Rename(from, to) ==
    /\ Live
    /\ from # to
    /\ osNs[from] # NoIno
    /\ budget.dirOps < MaxDirOps
    /\ osNs' = [osNs EXCEPT ![from] = NoIno, ![to] = osNs[from]]
    /\ pending' = IF Mutant = "rename_skips_pending" THEN pending
                  ELSE Append(pending, {[name |-> from, ino |-> NoIno], [name |-> to, ino |-> osNs[from]]})
    /\ linkAcked' = [linkAcked EXCEPT ![from] = FALSE, ![to] = FALSE]
    /\ budget' = [budget EXCEPT !.dirOps = @ + 1]
    /\ UNCHANGED <<up, phase, halted, durNs, osData, durData, dirty, minLen, nextIno,
                   vol, openedGen, fileAcked, sectorAcked, written, dropped, lastSyncOk,
                   anchors, postCut, zombie, lastCut>>

\* Fs::remove (fault.rs:1661)
Remove(n) ==
    /\ Live
    /\ osNs[n] # NoIno
    /\ budget.dirOps < MaxDirOps
    /\ osNs' = [osNs EXCEPT ![n] = NoIno]
    /\ pending' = Append(pending, {[name |-> n, ino |-> NoIno]})
    /\ linkAcked' = [linkAcked EXCEPT ![n] = FALSE]
    /\ budget' = [budget EXCEPT !.dirOps = @ + 1]
    /\ UNCHANGED <<up, phase, halted, durNs, osData, durData, dirty, minLen, nextIno,
                   vol, openedGen, fileAcked, sectorAcked, written, dropped, lastSyncOk,
                   anchors, postCut, zombie, lastCut>>

\* Fs::sync_dir of D (State::sync_dir, fault.rs:1613): every pending op of D becomes durable.
SyncDir ==
    /\ Live
    /\ pending # <<>>
    /\ durNs' = IF Mutant = "syncdir_noop" THEN durNs ELSE ApplyFrom(durNs, pending, 1..Len(pending), 1)
    /\ pending' = <<>>
    /\ linkAcked' = [n \in Names |-> TRUE]
    /\ UNCHANGED <<up, phase, halted, osNs, osData, durData, dirty, minLen, nextIno, vol,
                   openedGen, fileAcked, sectorAcked, written, dropped, lastSyncOk,
                   anchors, postCut, zombie, lastCut, budget>>

(* ----------------------------------------------------------------------- *)
(* Faults                                                                  *)
(* ----------------------------------------------------------------------- *)

\* FaultFs::crash_after(n) and World::crash_volume_after freeze the image after the
\* n-th mutating call; the calls that follow fail without effect. Crash below may
\* happen after any call, so it covers every crash_after boundary (NS04).

\* World::crash (sim.rs, cut_node since #106): cut the network, halt every volume, mark the node down and its services
\* dead, then abort its tasks. Mutant abort_before_halt is the phase 9 planted bug P2.
Crash ==
    /\ up
    /\ budget.crashes < MaxCrashes
    /\ up' = FALSE
    /\ phase' = "aborting"
    /\ halted' = IF Mutant = "abort_before_halt" THEN halted ELSE TRUE
    /\ zombie' = TRUE
    /\ budget' = [budget EXCEPT !.crashes = @ + 1]
    /\ UNCHANGED <<osNs, durNs, pending, osData, durData, dirty, minLen, nextIno, vol,
                   openedGen, linkAcked, fileAcked, sectorAcked, written, dropped,
                   lastSyncOk, anchors, postCut, lastCut>>

\* The abort runs task destructors, which may try to write (sim.rs:521-528). A write is
\* admitted only by a volume that is not halted.
AbortTasks ==
    /\ phase = "aborting"
    /\ phase' = "down"
    /\ halted' = TRUE
    /\ \/ /\ UNCHANGED <<osData, dirty, fileAcked, sectorAcked, written, dropped, postCut, zombie>>
       \/ \E n \in Names, s \in Sectors, v \in WVals :
            /\ osNs[n] # NoIno
            /\ s <= Len(osData[osNs[n]]) + 1
            /\ IF halted
               THEN UNCHANGED <<osData, dirty, fileAcked, sectorAcked, written, dropped, postCut, zombie>>
               ELSE /\ WriteSector(osNs[n], s, v)
                    /\ postCut' = postCut + 1
                    /\ UNCHANGED zombie
    /\ UNCHANGED <<up, osNs, durNs, pending, durData, minLen, nextIno, vol, openedGen,
                   linkAcked, lastSyncOk, anchors, lastCut, budget>>

\* tokio drops an aborted task's future only when it next processes the task, which
\* may be after the restart. The destructor then writes by path from the retired node
\* context. volumes.rs resolves a path only for the current incarnation
\* (World::resolve_node_path), so the write fails. R-sim-11 of the independent read.
ZombieWrite ==
    /\ zombie
    /\ up
    /\ zombie' = FALSE
    /\ \/ /\ UNCHANGED <<osData, dirty, fileAcked, sectorAcked, written, dropped, postCut>>
       \/ /\ Mutant = "no_incarnation_fence"
          /\ \E n \in Names, s \in Sectors, v \in WVals :
               /\ osNs[n] # NoIno
               /\ s <= Len(osData[osNs[n]]) + 1
               /\ WriteSector(osNs[n], s, v)
               /\ postCut' = postCut + 1
    /\ UNCHANGED <<up, phase, halted, osNs, durNs, pending, durData, minLen, nextIno, vol,
                   openedGen, linkAcked, lastSyncOk, anchors, lastCut, budget>>

AllAcked == /\ linkAcked' = [n \in Names |-> TRUE]
            /\ fileAcked' = [i \in Inos |-> TRUE]
            /\ sectorAcked' = [i \in Inos |-> 1..Len(osData'[i])]
            /\ written' = [i \in Inos |-> [s \in Sectors |-> {}]]
            /\ dropped' = [i \in Inos |-> {}]
            /\ lastSyncOk' = [i \in Inos |-> TRUE]

\* Rebuild a fully durable volume from a namespace and per-inode bytes (State::rebuilt).
Rebuild(ns, bytes) ==
    /\ osNs' = ns
    /\ durNs' = ns
    /\ pending' = <<>>
    /\ osData' = bytes
    /\ durData' = bytes
    /\ dirty' = [i \in Inos |-> {}]
    /\ minLen' = [i \in Inos |-> Len(bytes[i])]
    /\ AllAcked

Reopen(cut) ==
    /\ up' = TRUE
    /\ phase' = "up"
    /\ halted' = FALSE
    /\ openedGen' = vol.gen
    /\ lastCut' = cut
    \* the reopened pool verifies its mount anchor (fs::os::metadata of the root)
    /\ anchors' = anchors \cup {[dev |-> vol.dev, uuid |-> vol.uuid, ino |-> vol.base + RootOffset, gen |-> vol.gen]}
    /\ UNCHANGED <<nextIno, vol, postCut, zombie, budget>>

\* World::restart_with(Cut::Kill): every completed write survives, unsynced state stays
\* unsynced, so a later power loss can still drop it (fault.rs:1136).
RestartKill ==
    /\ phase = "down"
    /\ "kill" \in Cuts
    /\ Reopen("kill")
    /\ UNCHANGED <<osNs, durNs, pending, osData, durData, dirty, minLen, linkAcked,
                   fileAcked, sectorAcked, written, dropped, lastSyncOk>>

\* World::restart_with(Cut::Crash): only durable state (fault.rs:1162).
RestartCrash ==
    /\ phase = "down"
    /\ "crash" \in Cuts
    /\ Reopen("crash")
    /\ Rebuild(durNs, durData)

\* World::restart_with(Cut::PowerLoss | PowerLoss4k): fault.rs:1177.
RestartPowerLoss(mode) ==
    /\ phase = "down"
    /\ mode \in Cuts
    /\ Reopen(mode)
    /\ LET L == Len(pending)
           Units == IF mode = "power512" THEN Sectors ELSE {1}
           \* rename_not_atomic decides each half of an op on its own coin
           EffectCoins == IF Mutant = "rename_not_atomic"
                          THEN [1..L -> [Names -> BOOLEAN]]
                          ELSE {[k \in 1..L |-> [n \in Names |-> TRUE]]}
           \* inodes some name links, in the OS layer or on the device
           Linked == ({osNs[n] : n \in Names} \cup {durNs[n] : n \in Names}) \ {NoIno}
           UnitOf(s) == IF mode = "power512" THEN s ELSE 1
           \* the units a power loss decides: those holding a dirty sector (every unit
           \* with data under mutant tear_clean_sectors)
           Decided == {<<i, u>> \in Linked \X Units :
                         \E s \in Sectors : UnitOf(s) = u /\ s <= Len(osData[i])
                                            /\ (s \in dirty[i] \/ Mutant = "tear_clean_sectors")}
       IN \E coin \in [1..L -> BOOLEAN] :
          \E effCoin \in EffectCoins :
          \E newLen \in SUBSET {i \in Linked : LengthChanged(i)} :
          \E outcome \in [Decided -> UnitOutcomes(mode)] :
          \E pick \in PickChoices :
            LET ns == IF Mutant = "rename_not_atomic"
                      THEN ApplyFrom(durNs, [k \in 1..L |-> {e \in pending[k] : effCoin[k][e.name]}],
                                     1..L, 1)
                      ELSE ApplyFrom(durNs, pending, KeptFrom(pending, coin, {}, 1), 1)
                oc(i) == [u \in Units |-> IF <<i, u>> \in Decided THEN outcome[<<i, u>>] ELSE "old"]
            IN Rebuild(ns, [i \in Inos |->
                              IF i \in Linked THEN PowerBytes(i, mode, i \in newLen, oc(i), pick[i])
                              ELSE durData[i]])

\* World::wipe (volumes.rs:550) then World::restart: the node comes back without its
\* disk. Reformat keeps the device number; Replace takes a new one. The shipped code
\* draws a new filesystem uuid, and an inode base different from the old one unless the
\* harness set World::set_mkfs_inodes (88268073, volumes.rs), which numbers every
\* volume from base 0 as a real mkfs does.
Wipe(how, mkfs) ==
    \* World::wipe panics on a live node since e75ec244 (volumes.rs). Mutant
    \* prefix_wipe_while_up is the old API, under which a live incarnation silently
    \* continued on the empty replacement.
    /\ phase = "down" \/ (Mutant = "prefix_wipe_while_up" /\ up)
    /\ budget.wipes < MaxWipes
    /\ \E b \in Bases :
         \* mkfs numbers inodes the same way every time (ext4's root is inode 2), so a
         \* reformat under set_mkfs_inodes repeats the inode numbers.
         \* The default base is a fresh random multiple of 2^20 that differs from the
         \* old one. The model treats it as never used before on this volume: a repeat
         \* of an older base has probability 2^-20 per wipe.
         /\ IF mkfs
            THEN b = vol.base
            ELSE b # vol.base /\ \A a \in anchors : a.ino # b + RootOffset
         /\ vol' = [dev |-> IF how = "reformat" THEN vol.dev ELSE vol.dev + 1,
                    base |-> b,
                    uuid |-> IF Mutant = "wipe_keeps_uuid" THEN vol.uuid ELSE vol.gen + 1,
                    gen |-> vol.gen + 1]
    /\ osNs' = FreshNs
    /\ durNs' = FreshNs
    /\ pending' = <<>>
    /\ osData' = [i \in Inos |-> <<>>]
    /\ durData' = [i \in Inos |-> <<>>]
    /\ dirty' = [i \in Inos |-> {}]
    /\ minLen' = [i \in Inos |-> 0]
    /\ nextIno' = 1
    /\ halted' = ~up   \* the replacement volume is live; the old image is halted
    /\ AllAcked
    /\ budget' = [budget EXCEPT !.wipes = @ + 1]
    /\ UNCHANGED <<up, phase, openedGen, anchors, postCut, zombie, lastCut>>

\* World::restart after a wipe: the node opens the replacement volume.
RestartFresh ==
    /\ phase = "down"
    /\ openedGen # vol.gen
    /\ Reopen("fresh")
    /\ UNCHANGED <<osNs, durNs, pending, osData, durData, dirty, minLen, linkAcked,
                   fileAcked, sectorAcked, written, dropped, lastSyncOk>>

\* All budgets spent and nothing more to do: an explicit terminal stutter.
Done ==
    /\ budget.crashes = MaxCrashes
    /\ phase = "up"
    /\ UNCHANGED vars

Next ==
    \/ \E n \in Names : OpenCreateNew(n)
    \/ \E n \in Names, s \in Sectors, v \in WVals : WriteAt(n, s, v)
    \/ \E n \in Names, l \in 0..1 : SetLen(n, l)
    \/ \E n \in Names : SyncData(n)
    \/ \E n \in Names : SyncFails(n)
    \/ \E a, b \in Names : Rename(a, b)
    \/ \E n \in Names : Remove(n)
    \/ SyncDir
    \/ Crash
    \/ AbortTasks
    \/ ZombieWrite
    \/ RestartKill
    \/ RestartCrash
    \/ \E m \in {"power512", "power4k"} : RestartPowerLoss(m)
    \/ \E how \in {"reformat", "replace"}, mkfs \in BOOLEAN : Wipe(how, mkfs)
    \/ RestartFresh
    \/ Done

Spec == Init /\ [][Next]_vars

(* ----------------------------------------------------------------------- *)
(* Properties                                                              *)
(* ----------------------------------------------------------------------- *)
IsRestart == phase = "down" /\ phase' = "up"
ImageCut == lastCut'

\* What the application was told is durable: the name's link was covered by a
\* sync_dir and the file's data by a successful sync with no EIO since.
AppDurable(n) == osNs[n] # NoIno /\ linkAcked[n] /\ fileAcked[osNs[n]]

\* LSM-G01, OLTP-G01 (NS04, NS05): synced data survives every cut the sim injects.
SyncedSurvives ==
    [][IsRestart /\ ImageCut \in Cuts =>
        \A n \in Names : AppDurable(n) =>
            /\ osNs'[n] # NoIno
            /\ osData'[osNs'[n]] = osData[osNs[n]]]_vars

\* POSIX: a sector a successful sync covered, and that was not written since, survives.
SectorStep(cuts) ==
    IsRestart /\ ImageCut \in cuts =>
        \A n \in Names :
            (osNs[n] # NoIno /\ linkAcked[n]) =>
                \A s \in sectorAcked[osNs[n]] :
                    /\ osNs'[n] # NoIno
                    /\ s <= Len(osData'[osNs'[n]])
                    /\ osData'[osNs'[n]][s] = osData[osNs[n]][s]

NotPower4k == Cuts \ {"power4k"}
AnyCut == Cuts
SyncedSectorsSurvive == [][SectorStep(NotPower4k)]_vars
\* Witness only: fails on the baseline, because a 4 KiB destroying power loss takes
\* synced neighbours of a dirty sector with it (stronger than POSIX).
SyncedSectorsSurviveAnyCut == [][SectorStep(AnyCut)]_vars

\* Every crash image is one the POSIX model allows: the namespace is the durable one
\* plus a dependency-closed set of pending ops, and each sector holds its old value, a
\* value written since its last sync, a torn mix of the two, or (4 KiB only) zeros or
\* garbage over a dirty page.
OldVals(i, s) == {Zero} \cup (IF s <= Len(durData[i]) THEN {durData[i][s]} ELSE {})
WrittenVals(i, s) == written[i][s] \cup (IF s <= Len(osData[i]) THEN {osData[i][s]} ELSE {})
SectorAllowed(i, s, v, cut) ==
    \/ v \in OldVals(i, s)
    \/ v \in WrittenVals(i, s)
    \/ \E o \in OldVals(i, s), w \in WrittenVals(i, s) : v = Torn(o, w)
    \/ cut = "power4k" /\ dirty[i] # {} /\ v \in {Zero, Garbage}
ImageAllowed(cut) ==
    /\ \E kept \in SUBSET (1..Len(pending)) :
         /\ Closed(pending, kept)
         /\ osNs' = ApplyFrom(durNs, pending, kept, 1)
    \* inodes keep their identity across a cut (State::rebuilt keeps reported numbers)
    /\ \A n \in Names : osNs'[n] # NoIno =>
         LET i == osNs'[n] IN
         /\ Len(osData'[i]) \in {Len(durData[i]), Len(osData[i])}
         /\ \A s \in 1..Len(osData'[i]) : SectorAllowed(i, s, osData'[i][s], cut)
CrashImageAllowed ==
    [][IsRestart /\ ImageCut \in Cuts => ImageAllowed(ImageCut)]_vars

\* The OS namespace is always the durable one plus every pending op.
OsIsDurablePlusPending == osNs = ApplyFrom(durNs, pending, 1..Len(pending), 1)

\* Witness: FaultFs reports an inode clean (no dirty sector, no length change) while
\* the device lacks bytes the page cache holds. Only fsyncgate does this, so the
\* baseline fails it: the sim models a sync that drops pages.
CleanMeansDurable ==
    \A i \in Inos : ~Unsynced(i) => osData[i] = durData[i]

\* Rename atomicity seen in any image: no inode under two names.
NoDoubleLink ==
    \A a, b \in Names : (a # b /\ osNs[a] # NoIno) => osNs[a] # osNs[b]

\* Phase 9 P2: nothing written after the cut reaches an image.
NoWriteAfterCut == postCut = 0

\* A running incarnation only ever sees the volume it opened.
IncarnationSeesOneVolume == up => openedGen = vol.gen

\* O17 precondition, RV-REP #3: an anchor recorded before a wipe never verifies against
\* the replacement when the check includes the filesystem uuid.
UuidAnchorRejectsWipe ==
    \A a \in anchors : a.gen < vol.gen => a.uuid # vol.uuid

\* Witness, run by hand from witnesses/ (no baseline checks it): (dev, ino) alone does
\* not tell generations apart once a reformat runs under set_mkfs_inodes, as with a real
\* mkfs. The simulator can now exercise a (dev, ino)-only check against reality.
DevInoAnchorRejectsWipe ==
    \A a \in anchors : a.gen < vol.gen => <<a.dev, a.ino>> # <<vol.dev, vol.base + RootOffset>>

\* Gap witness: no image holds a value older than the latest write but newer than the
\* last sync. Real writeback can persist such an intermediate version; the shipped
\* FaultFs keeps only the durable value and the latest one. Mutant gap_writeback breaks it.
OnlyLatestOrOld ==
    [][IsRestart /\ ImageCut \in {"power512", "power4k"} =>
        \A n \in Names : (osNs[n] # NoIno /\ osNs'[n] = osNs[n]) =>
            \A s \in 1..Len(osData'[osNs[n]]) :
                LET v == osData'[osNs[n]][s] IN
                \/ v \in OldVals(osNs[n], s) \cup {Garbage}
                \/ s <= Len(osData[osNs[n]]) /\ v \in {osData[osNs[n]][s]} \cup {Torn(o, osData[osNs[n]][s]) : o \in OldVals(osNs[n], s)}]_vars

\* Witness: a kill image keeps unsynced data unsynced, so a later power loss can still
\* drop it (R-sim-07, DISK-7). Fails on the baseline.
KillImageDurable == (phase = "up" /\ lastCut = "kill") => \A i \in Inos : ~Unsynced(i)

\* Witness: an unsynced rename can be lost (fails on the baseline).
UnsyncedOpsSurvive ==
    [][IsRestart /\ ImageCut \in {"power512", "power4k"} => osNs' = osNs]_vars

\* Witness: a torn sector is reachable (fails on the baseline).
NoTornSector ==
    \A i \in Inos : \A s \in 1..Len(osData[i]) : ~(\E o \in Val1, w \in WVals : osData[i][s] = Torn(o, w))

=============================================================================
