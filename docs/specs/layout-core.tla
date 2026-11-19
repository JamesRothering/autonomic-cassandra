------------------------------- MODULE layout_core -------------------------------
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
 * Coordinator part shared by layout-migration.tla (forward, T6.15) and
 * layout-reverse.tla (reverse, T6.17i): phases and classes (sections 4.1
 * and 11.4), metadata propagation (Learn, A1), rule O1 (AllAcked), rule C1
 * and the execution of one operation attempt under the class it captured
 * (sections 3.3 and 3.4).
 *
 * The actions here change only log, seen, op and conc. A module that
 * extends this one adds its own variables and states what its copies do
 * with a send (WriteTarget) and what a read returns (ReadStepCore's argument).
 *)
EXTENDS Integers, Sequences, FiniteSets, TLC

CONSTANTS
    Coords,          \* coordinator nodes (every non-LEFT node of the directory)
    Writes,          \* client write operations
    Reads,           \* client read operations
    MaxAttempts,     \* attempts of rule C1 before a timeout (3 in the design)
    EnforceO1,       \* BOOLEAN: the owner obeys rule O1
    EnforceC1        \* BOOLEAN: coordinators obey rule C1

VARIABLES
    log,          \* sequence of committed phases; log[e] is the phase of epoch e
    seen,         \* seen[n]: the epoch node n has applied (and reports to the watermark request)
    op,           \* per operation: record, see IdleOp
    conc          \* conc[o]: operations whose current attempt overlapped o's current attempt

coreVars == <<log, seen, op, conc>>

Ops == Writes \cup Reads

ForwardPhases == {"ABSENT", "ANNOUNCED", "DUAL_WRITE", "BACKFILL", "VERIFY", "CUTOVER",
                  "NEW_ONLY", "CLEANUP", "STEADY", "ROLLBACK_DUAL", "ABORTING", "REMOVED"}
\* Section 11.4. UNSPLIT stands for "absent again after RemoveKey".
ReversePhases == {"REVERSE_ANNOUNCED", "REVERSE_DUAL_WRITE", "REVERSE_BACKFILL", "REVERSE_VERIFY",
                  "CUTBACK", "BASE_ONLY", "REVERSE_CLEANUP", "REVERSE_ROLLBACK", "UNSPLIT"}
Phases == ForwardPhases \cup ReversePhases

\* Phases in which the key is not in the registry.
Unregistered == {"ABSENT", "REMOVED", "UNSPLIT"}

\* Sections 4.1 and 11.4.
Class(p) ==
    CASE p \in {"ABSENT", "ANNOUNCED", "ABORTING", "REMOVED",
                "BASE_ONLY", "REVERSE_CLEANUP", "UNSPLIT"}                       -> 0
      [] p \in {"DUAL_WRITE", "BACKFILL", "VERIFY", "ROLLBACK_DUAL", "CUTBACK"} -> 1
      [] p \in {"CUTOVER", "REVERSE_DUAL_WRITE", "REVERSE_BACKFILL",
                "REVERSE_VERIFY", "REVERSE_ROLLBACK"}                           -> 2
      [] p \in {"NEW_ONLY", "CLEANUP", "STEADY", "REVERSE_ANNOUNCED"}           -> 3

\* I1: the copy that is authoritative for a class, and that readers of the class use.
Authoritative(c) == IF c <= 1 THEN "base" ELSE "overflow"

Epoch == Len(log)
Phase == log[Epoch]

\* The coordinator of an idle operation is a placeholder outside Coords, so that
\* states stay symmetric in the coordinators (needed for SYMMETRY). The fields
\* row, kind, ts and t0 are used by the reverse model only (the forward model
\* never changes them); tag is what Finish records for the invariants.
IdleOp == [st |-> "idle", coord |-> "none", cm0 |-> 1, pc |-> 0,
           attempts |-> 0, cls |-> 0, result |-> {}, ackedAtStart |-> {},
           fresh |-> TRUE, tag |-> FALSE,
           row |-> "none", kind |-> "none", ts |-> 0, t0 |-> 0]

Acked(o) == op[o].st = "acked"
Active(o) == op[o].st = "run"
AckedWrites == {w \in Writes : Acked(w)}

\* An attempt is routed, and so subject to rule C1, only if the key was in the
\* registry when the attempt captured its metadata.
Routed(o) == log[op[o].cm0] \notin Unregistered

-----------------------------------------------------------------------------
\* Metadata propagation. A node applies the next log entry (A1: never backwards).
LearnCore(n) ==
    /\ seen[n] < Epoch
    /\ seen' = [seen EXCEPT ![n] = @ + 1]
    /\ UNCHANGED <<log, op, conc>>

\* Rule O1: every node has answered with an epoch at or past the epoch of the current phase.
AllAcked == \A n \in Coords : seen[n] >= Epoch

-----------------------------------------------------------------------------
\* Coordinators.

\* Operations active right now, other than o.
Others(o) == {a \in Ops \ {o} : Active(a)}

\* (Re)start of an attempt of o on coordinator n: capture cm0 (rule C1) and
\* reset the overlap bookkeeping to the attempt that starts now. rec is the
\* record the attempt is built from (op[o], or op[o] with the fields a model
\* fixes at the first start); snap is what a read must see (I5).
BeginAttempt(o, n, first, rec, snap) ==
    /\ op' = [op EXCEPT ![o] =
                [rec EXCEPT !.st = "run", !.coord = n, !.cm0 = seen[n], !.pc = 0,
                            !.attempts = rec.attempts + 1, !.cls = Class(log[seen[n]]),
                            !.ackedAtStart = IF o \in Writes THEN {}
                                               ELSE IF first THEN snap ELSE rec.ackedAtStart]]
    /\ conc' = [a \in Ops |->
                  IF a = o THEN Others(o)
                  ELSE IF Active(a) THEN conc[a] \cup {o}
                  ELSE conc[a] \ {o}]

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
\* the coordinator's metadata has moved on since, and however late. The
\* extending module states the effect on its copies of WriteTarget(w).
WriteTarget(w) == Target(op[w].cls, op[w].pc)

WriteStepCore(w) ==
    /\ w \in Writes
    /\ Active(w)
    /\ op[w].pc < Steps(w)
    /\ op' = [op EXCEPT ![w].pc = @ + 1]
    /\ UNCHANGED <<log, seen, conc>>

\* A read uses the copy of its class (3.4): base in C0 and C1, overflow in C2 and C3.
\* result is what the extending module computes from that copy.
ReadCopy(r) == Authoritative(op[r].cls)

ReadStepCore(r, result) ==
    /\ r \in Reads
    /\ Active(r)
    /\ op[r].pc = 0
    /\ op' = [op EXCEPT ![r].pc = 1, ![r].result = result]
    /\ UNCHANGED <<log, seen, conc>>

\* End of an attempt. Rule C1: the routing version (here: the epoch) the
\* coordinator knows now must equal the one captured before the attempt.
\* tag is recorded with an acknowledged operation for the invariants.
FinishCore(o, tag) ==
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
                                      !.tag = tag]]
           /\ conc' = conc
        \/ /\ changed
           /\ op[o].attempts < MaxAttempts
           /\ BeginAttempt(o, n, FALSE, op[o], {})
        \/ /\ changed
           /\ op[o].attempts >= MaxAttempts
           \* Timed out: never acknowledged, so no invariant reads it again.
           /\ op' = [op EXCEPT ![o] = [IdleOp EXCEPT !.st = "timeout"]]
           /\ conc' = [a \in Ops |-> IF a = o THEN {} ELSE conc[a] \ {o}]
    /\ UNCHANGED <<log, seen>>

\* A coordinator that crashes or stalls inside an operation needs no action:
\* the operation simply never takes another step and is never acknowledged.

-----------------------------------------------------------------------------
\* Invariants on the coordinator part (section 4.10).

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
\* started. FinishCore records ackedAtStart \subseteq result as fresh.
I5 == \A r \in Reads : Acked(r) => op[r].fresh

=============================================================================
