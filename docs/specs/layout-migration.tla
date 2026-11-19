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
EXTENDS layout_core

\* Coords, Writes, Reads, MaxAttempts, EnforceO1, EnforceC1, the phases and
\* classes, Learn, rules O1 and C1 and the attempt execution are in
\* layout-core.tla (shared with layout-reverse.tla). This model uses the
\* forward phases only.
CONSTANTS
    MaxRebackfills,  \* bound on VERIFY -> BACKFILL, to keep the model finite
    EnforceDrain     \* BOOLEAN: the owner leaves ANNOUNCED only after unrouted operations have ended

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
    base,         \* set of writes stored in the base copy
    overflow,     \* set of writes stored in the overflow copy
    backfilled,   \* a backfill pass has completed in the current BACKFILL phase
    reconciled,   \* the reconcile pass of CLEANUP has completed
    rebackfills   \* number of VERIFY -> BACKFILL transitions taken

vars == <<log, seen, base, overflow, backfilled, reconciled, rebackfills, op, conc>>
modelVars == <<base, overflow, backfilled, reconciled, rebackfills>>

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

Learn(n) == LearnCore(n) /\ UNCHANGED modelVars

-----------------------------------------------------------------------------
\* Owner.

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
\* Coordinators (the execution rules are in layout-core.tla).

Start(o, n) ==
    /\ op[o].st = "idle"
    /\ BeginAttempt(o, n, TRUE, op[o], AckedWrites)
    /\ UNCHANGED <<log, seen, base, overflow, backfilled, reconciled, rebackfills>>

\* The base copy drops sends in STEADY (the base tombstone shadows them); the
\* overflow copy drops sends in REMOVED (the buckets were deleted).
WriteStep(w) ==
    /\ WriteStepCore(w)
    /\ LET t == WriteTarget(w) IN
        /\ base' = IF t = "base" /\ Phase # "STEADY" THEN base \cup {w} ELSE base
        /\ overflow' = IF t = "overflow" /\ Phase # "REMOVED" THEN overflow \cup {w} ELSE overflow
    /\ UNCHANGED <<backfilled, reconciled, rebackfills>>

ReadStep(r) ==
    /\ ReadStepCore(r, IF ReadCopy(r) = "base" THEN base ELSE overflow)
    /\ UNCHANGED modelVars

\* The tag records whether the read was acknowledged after NEW_ONLY (I10).
Finish(o) ==
    /\ FinishCore(o, o \in Reads /\ \E e \in 1..Epoch : log[e] = "NEW_ONLY")
    /\ UNCHANGED modelVars

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

\* I10. After NEW_ONLY is committed no acknowledged read used the base copy,
\* and no later phase is of a class below C3.
I10 ==
    /\ \A r \in Reads : (Acked(r) /\ op[r].tag) => Authoritative(op[r].cls) = "overflow"
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
