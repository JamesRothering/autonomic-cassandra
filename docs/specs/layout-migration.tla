---------------------------- MODULE layout_migration ----------------------------
(*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *)

(*
 * Model of the layout migration protocol for one partition key:
 * docs/architecture/adaptive-schema.md, sections 4.1 to 4.3 (T6.15).
 *
 * One owner commits phase changes to a log (the cluster metadata). Each
 * coordinator has applied a prefix of that log and only ever applies more
 * of it (assumption A1). Client writes and reads are executed by
 * coordinators against two copies of the partition, base and overflow,
 * according to the class of the phase the coordinator knew when the
 * attempt started (sections 3.3 and 3.4).
 *
 * Rule O1 (owner waits for every node to reach the epoch of the current
 * phase) and rule C1 (a coordinator compares the routing version before and
 * after an attempt and re-executes on a change) can each be switched off,
 * to show that each is necessary.
 *
 * The file is named layout-migration.tla as the task list requires; TLA+
 * module names cannot contain a hyphen, so the module is layout_migration
 * and run.sh copies the file under that name before calling TLC.
 *)
EXTENDS Naturals, Sequences, FiniteSets, TLC

CONSTANTS
    Coords,          \* coordinator nodes (every non-LEFT node of the directory)
    Writes,          \* client write operations
    Reads,           \* client read operations
    MaxAttempts,     \* attempts of rule C1 before a timeout (3 in the design)
    MaxRebackfills,  \* bound on VERIFY -> BACKFILL, to keep the model finite
    EnforceO1,       \* BOOLEAN: the owner obeys rule O1
    EnforceC1,       \* BOOLEAN: coordinators obey rule C1
    EnforceDrain     \* BOOLEAN: the owner leaves ANNOUNCED only after unrouted operations have ended

Ops == Writes \cup Reads

Phases == {"ABSENT", "ANNOUNCED", "DUAL_WRITE", "BACKFILL", "VERIFY", "CUTOVER",
           "NEW_ONLY", "CLEANUP", "STEADY", "ROLLBACK_DUAL", "ABORTING", "REMOVED"}

\* Section 4.1. ABSENT and REMOVED stand for "the key is not in the registry".
Class(p) ==
    CASE p \in {"ABSENT", "ANNOUNCED", "ABORTING", "REMOVED"}             -> 0
      [] p \in {"DUAL_WRITE", "BACKFILL", "VERIFY", "ROLLBACK_DUAL"}      -> 1
      [] p = "CUTOVER"                                                    -> 2
      [] p \in {"NEW_ONLY", "CLEANUP", "STEADY"}                          -> 3

\* I1: the copy that is authoritative for a class, and that readers of the class use.
Authoritative(c) == IF c <= 1 THEN "base" ELSE "overflow"

\* Section 4.2, the (from, to) pairs that change the phase. Generation
\* restarts (X -> X) change no class and are not modelled, except that
\* VERIFY -> BACKFILL starts a new backfill pass.
Allowed ==
    { <<"ABSENT", "ANNOUNCED">>, <<"ANNOUNCED", "DUAL_WRITE">>, <<"DUAL_WRITE", "BACKFILL">>,
      <<"BACKFILL", "VERIFY">>, <<"VERIFY", "CUTOVER">>, <<"CUTOVER", "NEW_ONLY">>,
      <<"NEW_ONLY", "CLEANUP">>, <<"CLEANUP", "STEADY">>,
      <<"VERIFY", "BACKFILL">>,
      <<"ANNOUNCED", "ABORTING">>, <<"DUAL_WRITE", "ABORTING">>, <<"BACKFILL", "ABORTING">>,
      <<"VERIFY", "ABORTING">>, <<"ROLLBACK_DUAL", "ABORTING">>,
      <<"CUTOVER", "ROLLBACK_DUAL">>,
      <<"ABORTING", "REMOVED">> }

VARIABLES
    log,          \* sequence of committed phases; log[e] is the phase of epoch e
    seen,         \* seen[n]: the epoch node n has applied (and reports to the watermark request)
    base,         \* set of writes stored in the base copy
    overflow,     \* set of writes stored in the overflow copy
    backfilled,   \* a backfill pass has completed in the current BACKFILL phase
    reconciled,   \* the reconcile pass of CLEANUP has completed
    rebackfills,  \* number of VERIFY -> BACKFILL transitions taken
    op,           \* per operation: record, see Init
    conc          \* conc[o]: operations whose current attempt overlapped o's current attempt

vars == <<log, seen, base, overflow, backfilled, reconciled, rebackfills, op, conc>>

Epoch == Len(log)
Phase == log[Epoch]

\* The coordinator of an idle operation is a placeholder outside Coords, so that
\* states stay symmetric in the coordinators (needed for SYMMETRY below).
IdleOp == [st |-> "idle", coord |-> "none", cm0 |-> 1, pc |-> 0,
           attempts |-> 0, cls |-> 0, result |-> {}, ackedAtStart |-> {},
           fresh |-> TRUE, ackAfterCommit |-> FALSE]

Init ==
    /\ log = <<"ABSENT">>
    /\ seen = [n \in Coords |-> 1]
    /\ base = {}
    /\ overflow = {}
    /\ backfilled = FALSE
    /\ reconciled = FALSE
    /\ rebackfills = 0
    /\ op = [o \in Ops |-> IdleOp]
    /\ conc = [o \in Ops |-> {}]

Acked(o) == op[o].st = "acked"
Active(o) == op[o].st = "run"
AckedWrites == {w \in Writes : Acked(w)}

-----------------------------------------------------------------------------
\* An attempt is routed, and so subject to rule C1, only if the key was in the
\* registry when the attempt captured its metadata.
Routed(o) == log[op[o].cm0] \notin {"ABSENT", "REMOVED"}

\* Metadata propagation. A node applies the next log entry (A1: never backwards).

Learn(n) ==
    /\ seen[n] < Epoch
    /\ seen' = [seen EXCEPT ![n] = @ + 1]
    /\ UNCHANGED <<log, base, overflow, backfilled, reconciled, rebackfills, op, conc>>

-----------------------------------------------------------------------------
\* Owner.

\* Rule O1: every node has answered with an epoch at or past the epoch of the current phase.
AllAcked == \A n \in Coords : seen[n] >= Epoch

Advance(to) ==
    /\ <<Phase, to>> \in Allowed
    /\ EnforceO1 => AllAcked
    \* An operation that started while the key was not in the registry takes the
    \* stock path and is never re-checked (3.3: "nothingRouted"). The design
    \* bounds such operations by the request timeout; ANNOUNCED must outlast them.
    /\ Phase = "ANNOUNCED" /\ to = "DUAL_WRITE" /\ EnforceDrain =>
           \A o \in Ops : Active(o) => Routed(o)
    /\ Phase = "BACKFILL" /\ to = "VERIFY" => backfilled
    \* 4.6 with ADR-030: the check passes when the overflow copy holds every row of the base copy.
    /\ Phase = "VERIFY" /\ to = "CUTOVER" => base \subseteq overflow
    /\ Phase = "VERIFY" /\ to = "BACKFILL" => ~(base \subseteq overflow) /\ rebackfills < MaxRebackfills
    /\ Phase = "CLEANUP" /\ to = "STEADY" => reconciled
    /\ log' = Append(log, to)
    /\ backfilled' = FALSE
    /\ rebackfills' = IF Phase = "VERIFY" /\ to = "BACKFILL" THEN rebackfills + 1 ELSE rebackfills
    \* CLEANUP -> STEADY: the base partition tombstone (4.6). REMOVED: the buckets were deleted in ABORTING.
    /\ base' = IF to = "STEADY" THEN {} ELSE base
    /\ overflow' = IF to = "REMOVED" THEN {} ELSE overflow
    /\ UNCHANGED <<seen, reconciled, op, conc>>

\* 4.5: every base replica copies what it holds into the overflow table.
\* Replicas are abstracted to one copy, so a pass is one atomic step. A
\* replica starts as soon as it sees BACKFILL; it does not wait for other
\* nodes (O1 already put every node at DUAL_WRITE or later, the same class).
Backfill ==
    /\ Phase = "BACKFILL"
    /\ ~backfilled
    /\ overflow' = overflow \cup base
    /\ backfilled' = TRUE
    /\ UNCHANGED <<log, seen, base, reconciled, rebackfills, op, conc>>

\* 4.6: the reconcile generation of CLEANUP.
Reconcile ==
    /\ Phase = "CLEANUP"
    /\ ~reconciled
    /\ overflow' = overflow \cup base
    /\ reconciled' = TRUE
    /\ UNCHANGED <<log, seen, base, backfilled, rebackfills, op, conc>>

-----------------------------------------------------------------------------
\* Coordinators.

\* Operations active right now, other than o.
Others(o) == {a \in Ops \ {o} : Active(a)}

\* (Re)start of an attempt of o on coordinator n: capture cm0 (rule C1) and
\* reset the overlap bookkeeping to the attempt that starts now.
BeginAttempt(o, n, first) ==
    /\ op' = [op EXCEPT ![o] =
                [@ EXCEPT !.st = "run", !.coord = n, !.cm0 = seen[n], !.pc = 0,
                          !.attempts = @ + 1, !.cls = Class(log[seen[n]]),
                          !.ackedAtStart = IF o \in Writes THEN {}
                                             ELSE IF first THEN AckedWrites ELSE @]]
    /\ conc' = [a \in Ops |->
                  IF a = o THEN Others(o)
                  ELSE IF Active(a) THEN conc[a] \cup {o}
                  ELSE conc[a] \ {o}]

Start(o, n) ==
    /\ op[o].st = "idle"
    /\ BeginAttempt(o, n, TRUE)
    /\ UNCHANGED <<log, seen, base, overflow, backfilled, reconciled, rebackfills>>

\* Number of sends of an attempt: two for a write in C1 or C2, otherwise one.
Steps(o) == IF o \in Writes /\ op[o].cls \in {1, 2} THEN 2 ELSE 1

\* The copy that step pc of a write under class c is sent to (3.3, non-atomic caller):
\* C0 base; C1 overflow then base; C2 base then overflow; C3 overflow.
Target(c, pc) ==
    CASE c = 0 -> "base"
      [] c = 1 -> IF pc = 0 THEN "overflow" ELSE "base"
      [] c = 2 -> IF pc = 0 THEN "base" ELSE "overflow"
      [] c = 3 -> "overflow"

\* One send of a write. It executes "entirely according to cm0", however far
\* the coordinator's metadata has moved on since, and however late.
WriteStep(w) ==
    /\ w \in Writes
    /\ Active(w)
    /\ op[w].pc < Steps(w)
    /\ LET t == Target(op[w].cls, op[w].pc) IN
        /\ base' = IF t = "base" /\ Phase # "STEADY" THEN base \cup {w} ELSE base
        /\ overflow' = IF t = "overflow" /\ Phase # "REMOVED" THEN overflow \cup {w} ELSE overflow
    /\ op' = [op EXCEPT ![w].pc = @ + 1]
    /\ UNCHANGED <<log, seen, backfilled, reconciled, rebackfills, conc>>

\* A read uses the copy of its class (3.4): base in C0 and C1, overflow in C2 and C3.
ReadStep(r) ==
    /\ r \in Reads
    /\ Active(r)
    /\ op[r].pc = 0
    /\ op' = [op EXCEPT ![r].pc = 1,
                        ![r].result = IF Authoritative(op[r].cls) = "base" THEN base ELSE overflow]
    /\ UNCHANGED <<log, seen, base, overflow, backfilled, reconciled, rebackfills, conc>>

\* End of an attempt. Rule C1: the routing version (here: the epoch) the
\* coordinator knows now must equal the one captured before the attempt.
Finish(o) ==
    /\ Active(o)
    /\ op[o].pc = Steps(o)
    /\ LET n == op[o].coord
           changed == EnforceC1 /\ Routed(o) /\ seen[n] # op[o].cm0
       IN
        \/ /\ ~changed
           \* Only what the invariants read is kept; the rest is reset so that
           \* finished operations do not multiply the state space.
           /\ op' = [op EXCEPT ![o] =
                       [IdleOp EXCEPT !.st = "acked", !.cls = op[o].cls,
                                      !.fresh = op[o].ackedAtStart \subseteq op[o].result,
                                      !.ackAfterCommit = /\ o \in Reads
                                                         /\ \E e \in 1..Epoch : log[e] = "NEW_ONLY"]]
           /\ conc' = conc
        \/ /\ changed
           /\ op[o].attempts < MaxAttempts
           /\ BeginAttempt(o, n, FALSE)
        \/ /\ changed
           /\ op[o].attempts >= MaxAttempts
           \* Timed out: never acknowledged, so no invariant reads it again.
           /\ op' = [op EXCEPT ![o] = [IdleOp EXCEPT !.st = "timeout"]]
           /\ conc' = [a \in Ops |-> IF a = o THEN {} ELSE conc[a] \ {o}]
    /\ UNCHANGED <<log, seen, base, overflow, backfilled, reconciled, rebackfills>>

\* A coordinator that crashes or stalls inside an operation needs no action:
\* the operation simply never takes another step and is never acknowledged.

\* The run is over: the key is committed (STEADY) or removed again, and every
\* operation has ended. Stuttering here keeps TLC's deadlock check useful: any
\* other state without a successor is a protocol that is stuck.
Terminated ==
    /\ Phase \in {"STEADY", "REMOVED"}
    /\ \A o \in Ops : op[o].st \in {"acked", "timeout"}
    /\ UNCHANGED vars

-----------------------------------------------------------------------------

Next ==
    \/ \E n \in Coords : Learn(n)
    \/ \E to \in Phases : Advance(to)
    \/ Backfill
    \/ Reconcile
    \/ \E o \in Ops, n \in Coords : Start(o, n)
    \/ \E w \in Writes : WriteStep(w)
    \/ \E r \in Reads : ReadStep(r)
    \/ \E o \in Ops : Finish(o)
    \/ Terminated

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
\* Invariants (section 4.10).

TypeOK ==
    /\ log \in Seq(Phases)
    /\ seen \in [Coords -> 1..Epoch]
    /\ base \subseteq Writes
    /\ overflow \subseteq Writes
    /\ \A o \in Ops : op[o].st \in {"idle", "run", "acked", "timeout"}

\* I1. Exactly one copy is authoritative at every epoch (true by the
\* definition of Authoritative), and that copy holds every acknowledged write.
I1 == \A w \in AckedWrites : w \in (IF Authoritative(Class(Phase)) = "base" THEN base ELSE overflow)

\* I2. No two nodes have been told phases whose classes are more than one apart.
I2 == \A a, b \in Coords :
        LET ca == Class(log[seen[a]]) cb == Class(log[seen[b]])
        IN (ca <= cb + 1) /\ (cb <= ca + 1)

\* I4. Overlapping acknowledged operations executed under equal or adjacent classes.
I4 == \A a, b \in Ops :
        (Acked(a) /\ Acked(b) /\ b \in conc[a]) =>
            LET ca == op[a].cls cb == op[b].cls
            IN (ca <= cb + 1) /\ (cb <= ca + 1)

\* I5. An acknowledged read returns every write acknowledged before the read
\* started. Finish(r) records ackedAtStart \subseteq result as fresh.
I5 == \A r \in Reads : Acked(r) => op[r].fresh

\* I10. After NEW_ONLY is committed no acknowledged read used the base copy,
\* and no later phase is of a class below C3.
I10 ==
    /\ \A r \in Reads : (Acked(r) /\ op[r].ackAfterCommit) => Authoritative(op[r].cls) = "overflow"
    /\ \A i, j \in 1..Epoch : (i < j /\ log[i] = "NEW_ONLY") => Class(log[j]) = 3

Safety == TypeOK /\ I1 /\ I2 /\ I4 /\ I5 /\ I10

\* The invariants the task names. The negative variants check these, not I2:
\* I2 is the direct consequence of O1, so without O1 it fails trivially; the
\* point of the variants is that a client-visible invariant fails.
ClientSafety == TypeOK /\ I1 /\ I4 /\ I5 /\ I10

\* Symmetry reduction for TLC: coordinators are interchangeable, and so are the
\* writes. Sound for the invariants (safety) only, which is all that is checked.
Symmetry == Permutations(Coords) \cup Permutations(Writes)

-----------------------------------------------------------------------------
\* Reachability probes: each is expected to be VIOLATED, which shows that
\* the model can reach the state and the invariants above are not vacuous.

\* The migration can complete with both writes and a read acknowledged.
ProbeSteady == ~(Phase = "STEADY" /\ \A o \in Ops : Acked(o))
\* An abort can complete.
ProbeRemoved == ~(Phase = "REMOVED" /\ \A o \in Ops : Acked(o))
\* Writes can be acknowledged under C1 and under C2 (both dual-write orders).
ProbeClasses == ~(\E a, b \in Writes : Acked(a) /\ Acked(b) /\ op[a].cls = 1 /\ op[b].cls = 2)
\* ADR-030: an unacknowledged C1 write that reached the overflow copy but not
\* the base copy does not stop cutover.
ProbeCutoverOverflowOnly == ~(Phase = "CUTOVER" /\ overflow \ base # {})

=============================================================================
