-------------------------- MODULE JournalIntegrity --------------------------
(* LSM-1: the on-disk upgrade from format 1 (journal records in the CRC     *)
(* frame) to format 2 (authenticated frames under a per-store key).        *)
(* plans/native-storage-engine.md, section 2 "Journal format" and          *)
(* section 9 "Rollout"; code in server/kv/src/store/mod.rs (Store::open),   *)
(* recovery.rs (prepare) and journal/{segment,writer,recovery}.rs.          *)
(*                                                                         *)
(* One store directory. The files that matter are FORMAT, JOURNAL_KEY and  *)
(* the journal segments, each segment in one frame. A file write is a      *)
(* temp file, a sync and a rename (visible to the OS), then a directory    *)
(* sync (durable); a power loss keeps only durable names. A segment is     *)
(* durable when it is activated (activate syncs the journal directory).    *)
(* Two binaries can run: "old" knows format 1 only; "new" reads both       *)
(* formats and, with option "auth", creates format 2 stores and upgrades   *)
(* format 1 stores. Faults: kills and power losses at every step,          *)
(* redeploys in either direction (rollback), a crash-consistent copy of    *)
(* the directory restored with or without JOURNAL_KEY (file-level backup), *)
(* and media damage of the key file. Recovery classes other than "opens"   *)
(* and "refused" belong to Lsm.tla; here a store the format 1 open would   *)
(* refuse is "bad". Forged headers are not modelled byte by byte: a        *)
(* segment in the CRC frame is one a client can forge evidence into, so    *)
(* the properties are about which frame each segment gets, and when.       *)
EXTENDS Naturals, Sequences

CONSTANTS Mutant, MaxCrashes, MaxDeploys, MaxSegs, Faults

ASSUME Mutant \in {"none", "segment_before_format", "format_before_key",
                  "old_ignores_format", "append_v2_into_v1",
                  "upgrade_without_validation", "late_refusal",
                  "reopen_legacy", "no_open_dir_sync"}
ASSUME Faults \in BOOLEAN

VARIABLES
    bin,      \* "old" or "new"
    opt,      \* the new binary's option: "legacy" or "auth"
    phase,    \* "down", "open", "refused", or an upgrade step
    fmtOS, fmtD,   \* FORMAT: 0 (absent), 1 or 2; OS view and durable
    keyOS, keyD,   \* JOURNAL_KEY: "none", "ok" or "bad"
    segs,     \* frame (1 or 2) of each live segment, oldest first
    mode,     \* frame the running open writes: 1 or 2 (0 when down)
    mixed,    \* a record was written in a frame other than its segment's
    bad,      \* the store holds damage a format 1 open refuses
    upgraded, \* FORMAT went from 1 to 2 by an upgrade
    keyLost,  \* a restore left FORMAT 2 without JOURNAL_KEY (a fault)
    snap,     \* the disk (OS view) when the current open started
    crashes, deploys, faults

vars == <<bin, opt, phase, fmtOS, fmtD, keyOS, keyD, segs, mode, mixed, bad,
          upgraded, keyLost, snap, crashes, deploys, faults>>

Disk == <<fmtOS, keyOS, segs>>
Phases == {"down", "open", "refused", "validated", "key_renamed", "key_durable",
           "format_renamed", "formatted"}
HasV2 == \E i \in 1..Len(segs) : segs[i] = 2
Active == IF segs = <<>> THEN 0 ELSE segs[Len(segs)]

\* A new segment. The model keeps the newest MaxSegs; an older one has
\* retired (flush and journal GC retire segments; its frame no longer matters).
Push(s, m) == IF Len(s) < MaxSegs THEN Append(s, m) ELSE Append(Tail(s), m)

\* The segments after an open that writes frame m: a store with a key never
\* appends to a CRC segment; it fences it and continues in a new segment
\* (Writer::finish_open).
Fenced(m) ==
    IF Active # m /\ ~(Mutant = "append_v2_into_v1" /\ m = 2 /\ Active = 1)
    THEN Push(segs, m) ELSE segs

Init ==
    /\ bin \in {"old", "new"}
    /\ opt \in {"legacy", "auth"}
    /\ phase = "down"
    \* An empty directory, or a format 1 store with one segment.
    /\ fmtOS \in {0, 1} /\ fmtD = fmtOS
    /\ keyOS = "none" /\ keyD = "none"
    /\ segs = IF fmtOS = 1 THEN <<1>> ELSE <<>>
    /\ mode = 0 /\ mixed = FALSE
    /\ bad \in IF fmtOS = 1 THEN BOOLEAN ELSE {FALSE}
    /\ upgraded = FALSE /\ keyLost = FALSE
    /\ snap = <<fmtOS, keyOS, segs>>
    /\ crashes = 0 /\ deploys = 0 /\ faults = 0

\* ---------------------------------------------------------------- opens --

\* The old binary (Store::open before LSM-1): FORMAT must say format 1, and
\* its inventory refuses a segment version it does not know.
OpenOld ==
    /\ phase = "down" /\ bin = "old"
    /\ snap' = Disk
    /\ IF fmtOS = 0
       THEN \* creates a format 1 store
            /\ phase' = "open" /\ mode' = 1
            /\ fmtOS' = 1 /\ fmtD' = 1 /\ segs' = <<1>>
       ELSE IF Mutant = "late_refusal" /\ (fmtOS = 2 \/ HasV2)
       THEN \* writes a segment before it looks at FORMAT
            /\ phase' = "refused" /\ mode' = 0 /\ segs' = Push(segs, 1)
            /\ UNCHANGED <<fmtOS, fmtD>>
       ELSE IF (fmtOS = 2 /\ Mutant # "old_ignores_format") \/ HasV2 \/ bad
       THEN /\ phase' = "refused" /\ mode' = 0
            /\ UNCHANGED <<fmtOS, fmtD, segs>>
       ELSE /\ phase' = "open" /\ mode' = 1 /\ segs' = Fenced(1)
            /\ UNCHANGED <<fmtOS, fmtD>>
    /\ UNCHANGED <<bin, opt, keyOS, keyD, mixed, bad, upgraded, keyLost,
                   crashes, deploys, faults>>

\* The new binary, format 2 store: prepare syncs the store directory first,
\* so the FORMAT and JOURNAL_KEY it reads are durable before it writes an
\* authenticated segment (a kill can leave their renames unsynced). Then
\* read_journal_key, and the inventory checks every segment's key check.
OpenNewV2 ==
    /\ phase = "down" /\ bin = "new" /\ fmtOS = 2
    /\ snap' = Disk
    /\ IF Mutant = "no_open_dir_sync"
       THEN UNCHANGED <<fmtD, keyD>>
       ELSE fmtD' = fmtOS /\ keyD' = keyOS
    /\ IF keyOS # "ok" \/ bad
       THEN /\ phase' = "refused" /\ mode' = 0 /\ UNCHANGED segs
       ELSE LET m == IF Mutant = "reopen_legacy" THEN 1 ELSE 2 IN
            /\ phase' = "open" /\ mode' = m /\ segs' = Fenced(m)
    /\ UNCHANGED <<bin, opt, fmtOS, keyOS, mixed, bad, upgraded, keyLost,
                   crashes, deploys, faults>>

\* The new binary, empty directory or format 1 store.
OpenNewV1 ==
    /\ phase = "down" /\ bin = "new" /\ fmtOS \in {0, 1}
    /\ snap' = Disk
    /\ CASE fmtOS = 0 /\ opt = "auth" ->
              \* creates format 2: JOURNAL_KEY durable, then FORMAT
              /\ phase' = "key_durable" /\ mode' = 0
              /\ IF Mutant = "format_before_key"
                 THEN /\ fmtOS' = 2 /\ fmtD' = 2 /\ UNCHANGED <<keyOS, keyD>>
                 ELSE /\ keyOS' = "ok" /\ keyD' = "ok" /\ UNCHANGED <<fmtOS, fmtD>>
              /\ UNCHANGED <<segs, upgraded>>
         [] fmtOS = 0 ->
              /\ phase' = "open" /\ mode' = 1
              /\ fmtOS' = 1 /\ fmtD' = 1 /\ segs' = <<1>>
              /\ UNCHANGED <<keyOS, keyD, upgraded>>
         [] (HasV2 \/ bad) /\ ~(opt = "auth" /\ Mutant = "upgrade_without_validation") ->
              /\ phase' = "refused" /\ mode' = 0
              /\ UNCHANGED <<fmtOS, fmtD, keyOS, keyD, segs, upgraded>>
         [] opt = "auth" /\ Mutant = "upgrade_without_validation" ->
              \* writes the key and FORMAT before any open checked the store
              /\ phase' = "format_renamed" /\ mode' = 0
              /\ keyOS' = "ok" /\ keyD' = "ok" /\ fmtOS' = 2 /\ upgraded' = TRUE
              /\ UNCHANGED <<fmtD, segs>>
         [] opt = "auth" ->
              \* the validating format 1 open, which keeps the lock
              /\ phase' = "validated" /\ mode' = 1 /\ segs' = Fenced(1)
              /\ UNCHANGED <<fmtOS, fmtD, keyOS, keyD, upgraded>>
         [] OTHER ->
              /\ phase' = "open" /\ mode' = 1 /\ segs' = Fenced(1)
              /\ UNCHANGED <<fmtOS, fmtD, keyOS, keyD, upgraded>>
    /\ UNCHANGED <<bin, opt, mixed, bad, keyLost, crashes, deploys, faults>>

OpenNew == OpenNewV1 \/ OpenNewV2

\* -------------------------------------------- upgrade (Store::open) --

UpgradePhases == {"validated", "key_renamed", "key_durable", "format_renamed", "formatted"}

UpgradeStep ==
    /\ phase \in UpgradePhases
    /\ CASE phase = "validated" /\ Mutant = "format_before_key" ->
              /\ phase' = "key_durable" /\ fmtOS' = 2
              /\ UNCHANGED <<fmtD, keyOS, keyD, segs>>
         [] phase = "validated" ->
              /\ phase' = "key_renamed" /\ keyOS' = "ok"
              /\ UNCHANGED <<fmtOS, fmtD, keyD, segs>>
         [] phase = "key_renamed" ->
              /\ phase' = "key_durable" /\ keyD' = keyOS
              /\ UNCHANGED <<fmtOS, fmtD, keyOS, segs>>
         [] phase = "key_durable" /\ Mutant = "segment_before_format" ->
              \* activates an authenticated segment before FORMAT 2 is durable
              /\ phase' = "format_renamed" /\ fmtOS' = 2 /\ segs' = Push(segs, 2)
              /\ UNCHANGED <<fmtD, keyOS, keyD>>
         [] phase = "key_durable" ->
              /\ phase' = "format_renamed" /\ fmtOS' = 2
              /\ UNCHANGED <<fmtD, keyOS, keyD, segs>>
         [] phase = "format_renamed" ->
              /\ phase' = "formatted" /\ fmtD' = fmtOS
              /\ UNCHANGED <<fmtOS, keyOS, keyD, segs>>
         [] phase = "formatted" ->
              \* the validating open closes cleanly; the next open is format 2
              /\ phase' = "down"
              /\ UNCHANGED <<fmtOS, fmtD, keyOS, keyD, segs>>
    /\ upgraded' = (upgraded \/ (fmtD = 1 /\ fmtOS' = 2))
    /\ mode' = IF phase' = "down" THEN 0 ELSE mode
    /\ UNCHANGED <<bin, opt, mixed, bad, keyLost, snap, crashes, deploys, faults>>

\* -------------------------------------------------------- steady state --

\* A record is written into the active segment in the open's frame.
Write ==
    /\ phase \in {"open", "validated"}
    /\ mixed' = (mixed \/ Active # mode)
    /\ UNCHANGED <<bin, opt, phase, fmtOS, fmtD, keyOS, keyD, segs, mode, bad,
                   upgraded, keyLost, snap, crashes, deploys, faults>>

\* A roll activates a new segment in the open's frame.
Roll ==
    /\ phase = "open"
    /\ segs' = Push(segs, mode)
    /\ UNCHANGED <<bin, opt, phase, fmtOS, fmtD, keyOS, keyD, mode, mixed, bad,
                   upgraded, keyLost, snap, crashes, deploys, faults>>

\* ---------------------------------------------------------------- faults --

\* A kill keeps the OS view; a power loss keeps only durable names.
Crash ==
    /\ phase # "down" /\ crashes < MaxCrashes
    /\ \E power \in BOOLEAN :
         /\ fmtOS' = IF power THEN fmtD ELSE fmtOS
         /\ keyOS' = IF power THEN keyD ELSE keyOS
    /\ phase' = "down" /\ mode' = 0 /\ crashes' = crashes + 1
    /\ UNCHANGED <<bin, opt, fmtD, keyD, segs, mixed, bad, upgraded, keyLost,
                   snap, deploys, faults>>

\* Stop the process and install either binary with either option (an
\* upgrade, a rollback, or a configuration change).
Deploy ==
    /\ phase \in {"down", "open", "refused"} /\ deploys < MaxDeploys
    /\ \E b \in {"old", "new"}, o \in {"legacy", "auth"} :
         /\ bin' = b /\ opt' = o
    /\ phase' = "down" /\ mode' = 0 /\ deploys' = deploys + 1
    /\ UNCHANGED <<fmtOS, fmtD, keyOS, keyD, segs, mixed, bad, upgraded,
                   keyLost, snap, crashes, faults>>

\* A crash-consistent copy of the directory (a volume snapshot) restored in
\* its place, with JOURNAL_KEY or without it (a copy of selected files).
Restore ==
    /\ Faults /\ phase \in {"down", "refused"} /\ faults < 1
    /\ \E withKey \in BOOLEAN :
         /\ keyOS' = IF withKey THEN keyD ELSE "none"
         /\ keyD' = IF withKey THEN keyD ELSE "none"
         /\ keyLost' = (~withKey /\ fmtD = 2)
    /\ fmtOS' = fmtD /\ phase' = "down" /\ mode' = 0 /\ faults' = faults + 1
    /\ UNCHANGED <<bin, opt, fmtD, segs, mixed, bad, upgraded, snap,
                   crashes, deploys>>

\* Media damage of the key file.
DamageKey ==
    /\ Faults /\ phase = "down" /\ keyD = "ok" /\ keyOS = "ok" /\ faults < 1
    /\ keyD' = "bad" /\ keyOS' = "bad" /\ faults' = faults + 1
    /\ UNCHANGED <<bin, opt, phase, fmtOS, fmtD, segs, mode, mixed, bad,
                   upgraded, keyLost, snap, crashes, deploys>>

Done ==
    /\ phase \in {"open", "refused"}
    /\ UNCHANGED vars

Next == OpenOld \/ OpenNew \/ UpgradeStep \/ Write \/ Roll \/ Crash \/ Deploy
        \/ Restore \/ DamageKey \/ Done

Spec == Init /\ [][Next]_vars

\* Liveness: the steps that end an upgrade (the opens and Store::open's
\* upgrade steps) eventually happen; faults and redeploys are bounded.
Fairness == WF_vars(OpenOld) /\ WF_vars(OpenNew) /\ WF_vars(UpgradeStep)
LiveSpec == Spec /\ Fairness

\* ------------------------------------------------------------ properties --

TypeOK ==
    /\ bin \in {"old", "new"} /\ opt \in {"legacy", "auth"}
    /\ phase \in Phases
    /\ fmtOS \in 0..2 /\ fmtD \in 0..2
    /\ keyOS \in {"none", "ok", "bad"} /\ keyD \in {"none", "ok", "bad"}
    /\ segs \in Seq({1, 2}) /\ Len(segs) <= MaxSegs
    /\ mode \in 0..2
    /\ {mixed, bad, upgraded, keyLost} \subseteq BOOLEAN
    /\ crashes \in 0..MaxCrashes /\ deploys \in 0..MaxDeploys /\ faults \in 0..1

\* An authenticated segment exists only once FORMAT 2 is durable: no binary
\* ever meets one in a store whose FORMAT says format 1.
KeyCoversFrames == HasV2 => fmtD = 2

\* FORMAT 2 is durable only after JOURNAL_KEY is: no crash leaves a format 2
\* store without its key (only a restore that drops the file can).
FormatHasKey == fmtD = 2 => (keyD # "none" \/ keyLost)

\* A binary that knows only format 1 serves only a store with only CRC
\* segments: a rollback after the upgrade is refused, never served.
OldOpensOnlyV1 ==
    (phase = "open" /\ bin = "old") => (fmtOS = 1 /\ ~HasV2)

\* Every record is in its segment's frame, so recovery can read it.
NoMixedSegment == ~mixed

\* An upgrade happens only to a store a complete format 1 open accepted.
UpgradeValidated == upgraded => ~bad

\* A refused open changed no file.
RefusalLeavesDisk == phase = "refused" => Disk = snap

\* Once the new binary runs with "auth" on a sound store and faults stop,
\* the store ends open and writing authenticated frames.
L_UpgradeCompletes ==
    <>[](bin = "new" /\ opt = "auth" /\ ~bad /\ keyD = "ok" /\ keyOS = "ok")
        => <>(phase = "open" /\ mode = 2)

=============================================================================
