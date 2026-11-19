---------------------------- MODULE layout_reverse ----------------------------
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
 * Model of the reverse migration (unsplit) of one split key and of the
 * deferred base tombstone: docs/architecture/adaptive-schema.md, section 11
 * (T6.17i, ADR-036). The coordinator part (classes, Learn, O1, C1, attempts)
 * is layout-core.tla, shared with the forward model.
 *
 * Unlike the forward model, copies hold atoms [row, ts, kind, ldt]: a put or
 * a delete (row tombstone) of a row, with its write timestamp and, for a
 * delete, its local deletion time. Visibility is last-write-wins per row, a
 * delete winning a tie, and a partition deletion (basePD, ovPD) shadows every
 * atom with a timestamp at or below its own. A global clock advances in
 * ticks; a delete can be purged by compaction GcGrace ticks after its local
 * deletion time, together with the atoms it shadows.
 *
 * The run starts in STEADY after a forward history (StartPhase = "STEADY"),
 * or before the split (StartPhase = "ABSENT"), in which case the forward
 * protocol runs first, with the deferred tombstone of ADR-036 and the forward
 * replica deadline of ADR-031.
 *
 * Abstractions: one replica per copy (a pass is one atomic step); a client
 * timestamp is the clock at the operation's first start, or clock + MaxTime
 * for a client clock in the future; an operation's sends, re-executions and
 * ack happen within the tick of its first start, and a Tick times out every
 * operation still running (a mutation or hint is never delivered later; real
 * stalls are bounded by the request timeout and the hint TTL, far below
 * gc_grace, so a tick stands for more than the request timeout). The owner's estimates,
 * straggler_window, reconcile_budget and the write timeout are 0 in the
 * windows of 11.5, so a limit L means "only while clock < L".
 *)
EXTENDS layout_core

CONSTANTS
    Rows,                     \* clustering rows of the key
    MaxTime,                  \* the clock runs from 0 to MaxTime
    GcGrace,                  \* gc_grace_seconds in ticks
    MaxRebackfills,           \* bound on VERIFY -> BACKFILL (both directions)
    MaxUnsplits,              \* bound on BeginUnsplit (each abort lets the owner retry)
    StartPhase,               \* "STEADY" or "ABSENT"
    ForwardTombstone,         \* BOOLEAN: forward cleanup writes the base tombstone at STEADY (as built before ADR-036)
    EnforceDrain,             \* BOOLEAN: the forward ANNOUNCED drain (ADR-028)
    EnforceResidueWindow,     \* BOOLEAN: fast mode and its writes only before frozenSince + G (I16)
    EnforcePurge,             \* BOOLEAN: slow mode waits for the base purge and the probe (I17)
    ProbeChecksResidue,       \* BOOLEAN: the probe requires an empty base partition, not only a purged tombstone
    EnforceReverseDeadline,   \* BOOLEAN: no reverse write after reverseDualSince + G (I18)
    EnforceBucketDeleteOrder  \* BOOLEAN: buckets deleted only after RemoveKey is acknowledged by all (I20)

None == -1
G == GcGrace \div 2
\* Timestamp of a history write made with a client clock in the future (the r4 case).
Future == MaxTime

VARIABLES
    clock,
    base,             \* set of atoms in the base partition K
    overflow,         \* set of atoms in the bucket partitions of incarnation i
    basePD,           \* timestamp (= local deletion time) of the base partition deletion, or None
    ovPD,             \* timestamp of the bucket partition deletion, or None
    sentDels,         \* every delete a client write has sent, acknowledged or not (for "held")
    acked,            \* atoms of acknowledged writes, including the history
    baseState,        \* "RESIDUE" or "TOMBSTONED" (11.5)
    frozenSince,      \* time at which the key last entered C3 from a lower class
    dualSince,        \* forward DUAL_WRITE time, or None
    reverseDualSince, \* REVERSE_DUAL_WRITE time, or None
    mode,             \* "none", "FAST" or "SLOW"
    backfilled,       \* the pass of the current BACKFILL or REVERSE_BACKFILL generation has run
    reconciled,       \* the reconcile pass of CLEANUP or REVERSE_CLEANUP has run
    rebackfills,
    flags             \* history variables of I16 to I20: names of the invariants a step broke

modelVars == <<clock, base, overflow, basePD, ovPD, sentDels, acked, baseState,
               frozenSince, dualSince, reverseDualSince, mode, backfilled, reconciled,
               rebackfills, flags>>
vars == <<coreVars, modelVars>>

Kinds == {"put", "del"}
Atoms == [row : Rows, ts : 0..(2 * MaxTime), kind : Kinds, ldt : 0..MaxTime]
MkAtom(r, t, k, l) == [row |-> r, ts |-> t, kind |-> k, ldt |-> IF k = "del" THEN l ELSE 0]
Max(a, b) == IF a > b THEN a ELSE b

-----------------------------------------------------------------------------
\* Last-write-wins visibility.

\* a overrides b: newer, or a delete at the same timestamp.
Beats(a, b) == a.ts > b.ts \/ (a.ts = b.ts /\ a.kind = "del" /\ b.kind = "put")
RowAtoms(c, r) == {a \in c : a.row = r}
Tops(c, r) == {a \in RowAtoms(c, r) : \A b \in RowAtoms(c, r) : ~Beats(b, a)}
VisiblePuts(c, pd, r) == {a \in Tops(c, r) : a.kind = "put" /\ a.ts > pd}

\* The copy (c, pd) holds the put a: an atom of a's row at least as new as a
\* and not shadowed by the partition deletion is stored, or a client delete
\* that overrides a was sent (if it is unacknowledged its outcome is unknown,
\* R4; once purged it leaves nothing behind).
Held(c, pd, a) ==
    \/ \E b \in RowAtoms(c, a.row) : b.ts >= a.ts /\ b.ts > pd
    \/ \E d \in sentDels : d.row = a.row /\ Beats(d, a)

\* The copy reflects the acknowledged operation a: a put is held; after a
\* delete, only a strictly newer put can be visible.
Reflects(c, pd, a) ==
    IF a.kind = "put" THEN Held(c, pd, a)
    ELSE \A v \in VisiblePuts(c, pd, a.row) : v.ts > a.ts

CopyReflects(which, a) ==
    IF which = "base" THEN Reflects(base, basePD, a)
    ELSE Reflects(overflow, ovPD, a)

AuthNow == Authoritative(Class(Phase))

\* Verification (ADR-030, 11.4, owner answer 8): every atom of the authoritative
\* copy is in the target, or the target has a strictly newer atom of the row
\* (NEWER_IN_TARGET). Rows only in the target are tolerated. Partition
\* deletions are not compared (the model reads the check as auth \subseteq target).
Covered(a, t) == a \in t \/ \E b \in t : b.row = a.row /\ b.ts > a.ts
VerifyOK(auth, target) == \A a \in auth : Covered(a, target)

\* What a pass copies: the merged (top) atoms of the source not shadowed by
\* its partition deletion.
Merged(c, pd) == {a \in c : a \in Tops(c, a.row) /\ a.ts > pd}

-----------------------------------------------------------------------------
\* Initial state.

\* Pre-split content, acknowledged: one row written at timestamp 0 (the r1 and
\* r3 rows of 11.1 and 11.2) and, if there is a second row, one written with a
\* client clock in the future (the r4 row).
RowA == CHOOSE r \in Rows : TRUE
History == {MkAtom(r, IF r = RowA THEN 0 ELSE Future, "put", 0) : r \in Rows}
\* An unacknowledged straggler that reached the base residue after the forward
\* reconcile: a put above any tombstone on the first row (it survives a purge,
\* hazard 2 of 11.9), or a delete of the second row.
Stragglers == {MkAtom(RowA, Future, "put", 0)} \cup
              {MkAtom(r, 0, "del", 0) : r \in Rows \ {RowA}}

InitCommon ==
    /\ seen = [n \in Coords |-> 1]
    /\ op = [o \in Ops |-> IdleOp]
    /\ conc = [o \in Ops |-> {}]
    /\ clock = 0
    /\ ovPD = None
    /\ reverseDualSince = None
    /\ mode = "none"
    /\ backfilled = FALSE
    /\ reconciled = FALSE
    /\ rebackfills = 0
    /\ flags = {}

Init ==
    /\ InitCommon
    /\ LET h == History IN
        /\ acked = h
        /\ IF StartPhase = "STEADY"
           THEN /\ log = <<"STEADY">>
                /\ overflow = h
                /\ \E s \in {{}} \cup {{x} : x \in Stragglers} :
                     /\ base = h \cup s
                     /\ sentDels = {x \in s : x.kind = "del"}
                /\ basePD = IF ForwardTombstone THEN 0 ELSE None
                /\ baseState = IF ForwardTombstone THEN "TOMBSTONED" ELSE "RESIDUE"
                /\ frozenSince = 0
                /\ dualSince = 0
           ELSE /\ log = <<"ABSENT">>
                /\ overflow = {}
                /\ base = h
                /\ sentDels = {}
                /\ basePD = None
                /\ baseState = "RESIDUE"
                /\ frozenSince = 0
                /\ dualSince = None

-----------------------------------------------------------------------------
\* Time and compaction.

\* Compaction purges a delete gc_grace after its local deletion time, and drops
\* the atoms of its row that it shadows with it. The base partition deletion
\* is purged in the same way, with everything at or below it (natural
\* compaction, or forceCompactionForKey in REVERSE_ANNOUNCED); atoms above it
\* (the r4 case) survive. Compactions are placed at tick boundaries: when the
\* clock advances, any subset of the tombstones that are due by then is
\* purged. (A compaction later in a tick is the same as one at the start of
\* the next.) Overflow is no longer compacted after UNSPLIT: nothing reads it.
Due(c, t) == {d \in c : d.kind = "del" /\ t >= d.ldt + GcGrace}
PurgeAll(c, P) == {a \in c : ~\E d \in P : a.row = d.row /\ (a = d \/ Beats(d, a))}

Tick ==
    /\ clock < MaxTime
    /\ clock' = clock + 1
    \* Operations still running can no longer send or be acknowledged.
    /\ op' = [o \in Ops |-> IF Active(o) THEN [IdleOp EXCEPT !.st = "timeout"] ELSE op[o]]
    /\ conc' = [o \in Ops |-> IF Active(o) THEN {} ELSE conc[o] \ {a \in Ops : Active(a)}]
    /\ \E PB \in SUBSET Due(base, clock + 1),
          PO \in SUBSET (IF Phase = "UNSPLIT" THEN {} ELSE Due(overflow, clock + 1)),
          pd \in IF basePD # None /\ clock + 1 >= basePD + GcGrace THEN {FALSE, TRUE} ELSE {FALSE} :
         /\ base' = LET b1 == PurgeAll(base, PB) IN IF pd THEN {a \in b1 : a.ts > basePD} ELSE b1
         /\ basePD' = IF pd THEN None ELSE basePD
         /\ overflow' = PurgeAll(overflow, PO)
    /\ UNCHANGED <<log, seen, ovPD, sentDels, acked, baseState, frozenSince, dualSince,
                   reverseDualSince, mode, backfilled, reconciled, rebackfills, flags>>

-----------------------------------------------------------------------------
\* Owner.

ForwardAllowed ==
    { <<"ABSENT", "ANNOUNCED">>, <<"ANNOUNCED", "DUAL_WRITE">>, <<"DUAL_WRITE", "BACKFILL">>,
      <<"BACKFILL", "VERIFY">>, <<"VERIFY", "CUTOVER">>, <<"CUTOVER", "NEW_ONLY">>,
      <<"NEW_ONLY", "CLEANUP">>, <<"CLEANUP", "STEADY">>,
      <<"VERIFY", "BACKFILL">>,
      <<"ANNOUNCED", "ABORTING">>, <<"DUAL_WRITE", "ABORTING">>, <<"BACKFILL", "ABORTING">>,
      <<"VERIFY", "ABORTING">>, <<"ROLLBACK_DUAL", "ABORTING">>,
      <<"CUTOVER", "ROLLBACK_DUAL">>,
      <<"ABORTING", "REMOVED">> }

\* Section 11.4, apart from BeginUnsplit (its own action).
ReverseAllowed ==
    { <<"REVERSE_ANNOUNCED", "REVERSE_DUAL_WRITE">>, <<"REVERSE_DUAL_WRITE", "REVERSE_BACKFILL">>,
      <<"REVERSE_BACKFILL", "REVERSE_VERIFY">>, <<"REVERSE_VERIFY", "CUTBACK">>,
      <<"CUTBACK", "BASE_ONLY">>, <<"BASE_ONLY", "REVERSE_CLEANUP">>,
      <<"REVERSE_CLEANUP", "UNSPLIT">>,
      <<"REVERSE_VERIFY", "REVERSE_BACKFILL">>,
      <<"REVERSE_ANNOUNCED", "STEADY">>, <<"REVERSE_DUAL_WRITE", "STEADY">>,
      <<"REVERSE_BACKFILL", "STEADY">>, <<"REVERSE_VERIFY", "STEADY">>,
      <<"REVERSE_ROLLBACK", "STEADY">>, <<"CUTBACK", "REVERSE_ROLLBACK">> }

\* 11.5 limits, with the owner's margins at 0.
WindowOpen == clock < frozenSince + G
ReplicaLimitOK ==
    /\ (EnforceResidueWindow /\ baseState = "RESIDUE") => WindowOpen
    /\ EnforceReverseDeadline => clock < reverseDualSince + G

\* Base state after an abort to STEADY (11.4).
AbortBase(from) ==
    IF from = "REVERSE_ANNOUNCED"
    THEN UNCHANGED <<baseState, frozenSince>>
    ELSE /\ baseState' = "RESIDUE"
         /\ frozenSince' =
              IF from \in {"REVERSE_VERIFY", "REVERSE_ROLLBACK"} THEN clock   \* generation complete
              ELSE IF mode = "FAST" THEN frozenSince                         \* old window still covers
              ELSE clock - G                                                 \* slow: window closed

Advance(to) ==
    /\ <<Phase, to>> \in ForwardAllowed \cup ReverseAllowed
    /\ EnforceO1 => AllAcked
    \* Forward guards, as in layout-migration.tla.
    /\ Phase = "ANNOUNCED" /\ to = "DUAL_WRITE" /\ EnforceDrain => \A o \in Ops : Active(o) => Routed(o)
    /\ Phase = "BACKFILL" /\ to = "VERIFY" => backfilled
    /\ Phase = "VERIFY" /\ to = "CUTOVER" => VerifyOK(base, overflow)
    /\ Phase = "VERIFY" /\ to = "BACKFILL" => ~VerifyOK(base, overflow) /\ rebackfills < MaxRebackfills
    /\ Phase = "CLEANUP" /\ to = "STEADY" => reconciled
    \* Reverse guards.
    /\ Phase = "REVERSE_ANNOUNCED" /\ to = "REVERSE_DUAL_WRITE" /\ mode = "SLOW" /\ EnforcePurge =>
           \* every base replica reported a clean purge and probe
           basePD = None /\ (ProbeChecksResidue => base = {})
    /\ Phase = "REVERSE_BACKFILL" /\ to = "REVERSE_VERIFY" => backfilled
    /\ Phase = "REVERSE_VERIFY" /\ to = "CUTBACK" => VerifyOK(overflow, base)
    /\ Phase = "REVERSE_VERIFY" /\ to = "REVERSE_BACKFILL" =>
           ~VerifyOK(overflow, base) /\ rebackfills < MaxRebackfills
    \* Reverse commit window (11.5): the reconcile must still be possible.
    /\ Phase = "CUTBACK" /\ to = "BASE_ONLY" => ReplicaLimitOK
    /\ Phase = "REVERSE_CLEANUP" /\ to = "UNSPLIT" => reconciled
    /\ log' = Append(log, to)
    /\ backfilled' = FALSE
    /\ rebackfills' = IF to \in {"BACKFILL", "REVERSE_BACKFILL"} /\ Phase \in {"VERIFY", "REVERSE_VERIFY"}
                      THEN rebackfills + 1 ELSE rebackfills
    /\ dualSince' = IF to = "DUAL_WRITE" THEN clock ELSE dualSince
    /\ reverseDualSince' = IF to = "REVERSE_DUAL_WRITE" THEN clock
                           ELSE IF to = "STEADY" THEN None ELSE reverseDualSince
    /\ mode' = IF to = "STEADY" THEN "none" ELSE mode
    \* Forward cleanup: with ForwardTombstone, today's base tombstone at STEADY;
    \* otherwise (ADR-036) the residue is kept and the reclaim writes it later.
    /\ IF to = "NEW_ONLY"
       THEN /\ frozenSince' = clock
            /\ UNCHANGED <<baseState, basePD>>
       ELSE IF Phase = "CLEANUP" /\ to = "STEADY"
       THEN /\ basePD' = IF ForwardTombstone THEN clock ELSE basePD
            /\ baseState' = IF ForwardTombstone THEN "TOMBSTONED" ELSE "RESIDUE"
            /\ UNCHANGED frozenSince
       ELSE IF to = "STEADY"
       THEN /\ AbortBase(Phase)
            /\ UNCHANGED basePD
       ELSE UNCHANGED <<baseState, frozenSince, basePD>>
    /\ overflow' = IF to = "REMOVED" THEN {} ELSE overflow
    \* I17: a slow reverse starts writing only into a purged and clean base.
    /\ flags' = IF /\ Phase = "REVERSE_ANNOUNCED" /\ to = "REVERSE_DUAL_WRITE" /\ mode = "SLOW"
                   /\ ~(basePD = None /\ base = {})
                THEN flags \cup {"I17"} ELSE flags
    /\ UNCHANGED <<seen, op, conc, clock, base, ovPD, sentDels, acked, reconciled>>

\* BeginUnsplit (11.4, 11.5): from STEADY, or from NEW_ONLY or CLEANUP (owner
\* answer 2). Fast mode needs the residue and an open window; slow mode a
\* tombstoned base.
BeginUnsplit(m) ==
    /\ Phase \in {"STEADY", "NEW_ONLY", "CLEANUP"}
    /\ Cardinality({e \in 1..Epoch : log[e] = "REVERSE_ANNOUNCED"}) < MaxUnsplits
    /\ EnforceO1 => AllAcked
    /\ m = "FAST" => baseState = "RESIDUE" /\ (EnforceResidueWindow => WindowOpen)
    /\ m = "SLOW" => baseState = "TOMBSTONED"
    /\ log' = Append(log, "REVERSE_ANNOUNCED")
    /\ mode' = m
    /\ backfilled' = FALSE
    /\ reconciled' = FALSE
    /\ rebackfills' = 0
    /\ UNCHANGED <<seen, op, conc, clock, base, overflow, basePD, ovPD, sentDels, acked,
                   baseState, frozenSince, dualSince, reverseDualSince, flags>>

\* Reclaim continuation (11.3 item 2): an unowned STEADY key in RESIDUE once a
\* fast reverse could no longer complete. Writes the base tombstone at ALL.
Reclaim ==
    /\ Phase = "STEADY"
    /\ baseState = "RESIDUE"
    /\ ~WindowOpen
    /\ basePD' = Max(basePD, clock)
    /\ baseState' = "TOMBSTONED"
    /\ UNCHANGED <<coreVars, clock, base, overflow, ovPD, sentDels, acked, frozenSince,
                   dualSince, reverseDualSince, mode, backfilled, reconciled, rebackfills, flags>>

\* Forward passes (4.5, 4.6), base to overflow, under the forward replica
\* deadline of ADR-031 (dualWriteSince + G).
ForwardPass ==
    /\ \/ Phase = "BACKFILL" /\ ~backfilled
       \/ Phase = "CLEANUP" /\ ~reconciled
    /\ clock < dualSince + G
    /\ overflow' = overflow \cup Merged(base, basePD)
    /\ backfilled' = (Phase = "BACKFILL") \/ backfilled
    /\ reconciled' = (Phase = "CLEANUP") \/ reconciled
    /\ UNCHANGED <<coreVars, clock, base, basePD, ovPD, sentDels, acked, baseState,
                   frozenSince, dualSince, reverseDualSince, mode, rebackfills, flags>>

\* Reverse passes (11.4): every bucket replica copies its merged data into base
\* K through RowMapper.toLogical. A bucket partition deletion becomes a base
\* partition deletion. Guarded by the replica limits of 11.5 when enforced;
\* a pass outside them is recorded in flags.
ReversePass ==
    /\ \/ Phase = "REVERSE_BACKFILL" /\ ~backfilled
       \/ Phase = "REVERSE_CLEANUP" /\ ~reconciled
    /\ ReplicaLimitOK
    /\ base' = base \cup Merged(overflow, ovPD)
    /\ basePD' = IF ovPD # None THEN Max(basePD, ovPD) ELSE basePD
    /\ backfilled' = (Phase = "REVERSE_BACKFILL") \/ backfilled
    /\ reconciled' = (Phase = "REVERSE_CLEANUP") \/ reconciled
    /\ flags' = flags \cup
         {f \in {"I16", "I17", "I18", "I19"} :
            \/ f = "I16" /\ baseState = "RESIDUE" /\ ~WindowOpen
            \/ f = "I18" /\ clock >= reverseDualSince + G
            \/ f = "I17" /\ mode = "SLOW" /\ ovPD # None
            \/ f = "I19" /\ ovPD # None}
    /\ UNCHANGED <<coreVars, clock, overflow, ovPD, sentDels, acked, baseState,
                   frozenSince, dualSince, reverseDualSince, mode, rebackfills>>

\* Deletion of the bucket partitions of incarnation i at ALL (11.4, I20).
DeleteBuckets ==
    /\ ovPD = None
    /\ Phase \in {"BASE_ONLY", "REVERSE_CLEANUP", "UNSPLIT"}
    /\ EnforceBucketDeleteOrder => Phase = "UNSPLIT" /\ AllAcked
    /\ ovPD' = clock
    /\ flags' = IF Phase = "UNSPLIT" /\ AllAcked THEN flags ELSE flags \cup {"I20"}
    /\ UNCHANGED <<coreVars, clock, base, overflow, basePD, sentDels, acked, baseState,
                   frozenSince, dualSince, reverseDualSince, mode, backfilled, reconciled,
                   rebackfills>>

-----------------------------------------------------------------------------
\* Coordinators.


\* A write is a put or a delete of one row; its timestamp is the clock, or a
\* client clock MaxTime ahead. A read reads the whole partition.
Start(o, n) ==
    /\ op[o].st = "idle"
    /\ IF o \in Writes
       THEN \E r \in Rows, k \in Kinds, ahead \in {0, MaxTime} :
              \* Stock assumption: no client write carries a timestamp at or below
              \* a delete of its row that compaction may already have purged
              \* (otherwise even an unsplit table shows it once the delete is
              \* gone). Only a delete with a client clock in the future can
              \* leave such a gap.
              /\ ~\E d \in sentDels : d.row = r /\ clock + ahead <= d.ts /\ clock >= d.ldt + GcGrace
              /\ BeginAttempt(o, n, TRUE,
                           [op[o] EXCEPT !.row = r, !.kind = k, !.ts = clock + ahead, !.t0 = clock],
                           acked)
       ELSE BeginAttempt(o, n, TRUE, op[o], acked)
    /\ UNCHANGED <<log, seen, modelVars>>

OpAtom(w) == MkAtom(op[w].row, op[w].ts, op[w].kind, op[w].t0)

WriteStep(w) ==
    /\ WriteStepCore(w)
    /\ LET t == WriteTarget(w) IN
        /\ base' = IF t = "base" THEN base \cup {OpAtom(w)} ELSE base
        /\ overflow' = IF t = "overflow" /\ Phase # "UNSPLIT" THEN overflow \cup {OpAtom(w)} ELSE overflow
    /\ sentDels' = IF op[w].kind = "del" THEN sentDels \cup {OpAtom(w)} ELSE sentDels
    /\ UNCHANGED <<clock, basePD, ovPD, acked, baseState, frozenSince, dualSince,
                   reverseDualSince, mode, backfilled, reconciled, rebackfills, flags>>

\* I5: the copy must reflect every operation acknowledged before the read
\* started; the result is that set if it does, and empty otherwise.
ReadStep(r) ==
    /\ ReadStepCore(r, IF \A a \in op[r].ackedAtStart : CopyReflects(ReadCopy(r), a)
                       THEN op[r].ackedAtStart ELSE {})
    /\ UNCHANGED modelVars

\* Tag of an acknowledged read: "c3" if acknowledged after the forward commit
\* point and before BeginUnsplit (amended I10), "c0" if after BASE_ONLY (I21).
LastIndex(p) == LET S == {e \in 1..Epoch : log[e] = p} IN IF S = {} THEN 0 ELSE CHOOSE e \in S : \A x \in S : x <= e
ForwardCommitted == LastIndex("NEW_ONLY") > 0 \/ log[1] = "STEADY"
ReadTag ==
    IF LastIndex("BASE_ONLY") > 0 THEN "c0"
    ELSE IF ForwardCommitted /\ LastIndex("REVERSE_ANNOUNCED") = 0 THEN "c3"
    ELSE "other"

Finish(o) ==
    /\ FinishCore(o, IF o \in Reads THEN ReadTag ELSE "other")
    /\ acked' = IF o \in Writes /\ op'[o].st = "acked" THEN acked \cup {OpAtom(o)} ELSE acked
    /\ UNCHANGED <<clock, base, overflow, basePD, ovPD, sentDels, baseState, frozenSince,
                   dualSince, reverseDualSince, mode, backfilled, reconciled, rebackfills, flags>>

Learn(n) == LearnCore(n) /\ UNCHANGED modelVars

-----------------------------------------------------------------------------

Next ==
    \/ \E n \in Coords : Learn(n)
    \/ \E to \in Phases : Advance(to)
    \/ \E m \in {"FAST", "SLOW"} : BeginUnsplit(m)
    \/ Reclaim
    \/ ForwardPass
    \/ ReversePass
    \/ DeleteBuckets
    \/ Tick
    \/ \E o \in Ops, n \in Coords : Start(o, n)
    \/ \E w \in Writes : WriteStep(w)
    \/ \E r \in Reads : ReadStep(r)
    \/ \E o \in Ops : Finish(o)

\* The clock is bounded, so every state before MaxTime can tick and a state at
\* MaxTime may legitimately wait for time that the model does not have: the
\* configurations disable TLC's deadlock check. Liveness is shown by the probes.
Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
\* Invariants (sections 4.10 and 11.6).

TypeOK ==
    /\ log \in Seq(Phases)
    /\ seen \in [Coords -> 1..Epoch]
    /\ clock \in 0..MaxTime
    /\ base \subseteq Atoms /\ overflow \subseteq Atoms /\ acked \subseteq Atoms
    /\ sentDels \subseteq Atoms
    /\ basePD \in {None} \cup 0..MaxTime /\ ovPD \in {None} \cup 0..MaxTime
    /\ baseState \in {"RESIDUE", "TOMBSTONED"}
    /\ mode \in {"none", "FAST", "SLOW"}
    /\ \A o \in Ops : op[o].st \in {"idle", "run", "acked", "timeout"}

\* I1, with visibility: the authoritative copy of the current class holds every
\* acknowledged put (deletes are I22's concern).
I1 == \A a \in acked : a.kind = "put" => CopyReflects(AuthNow, a)

\* I10 (amended): after the forward commit point, no transition returns below
\* C3 and no acknowledged read uses base until BeginUnsplit commits REVERSE_ANNOUNCED.
I10 ==
    /\ \A r \in Reads : (Acked(r) /\ op[r].tag = "c3") => Authoritative(op[r].cls) = "overflow"
    /\ \A i, j \in 1..Epoch :
         (i < j /\ (log[i] = "NEW_ONLY" \/ (i = 1 /\ log[1] = "STEADY")) /\ Class(log[j]) < 3) =>
             \E k \in (i + 1)..j : log[k] = "REVERSE_ANNOUNCED"

\* I21: after BASE_ONLY no acknowledged read uses overflow and no later phase is above C0.
I21 ==
    /\ \A r \in Reads : (Acked(r) /\ op[r].tag = "c0") => Authoritative(op[r].cls) = "base"
    /\ \A i, j \in 1..Epoch : (i < j /\ log[i] = "BASE_ONLY") => Class(log[j]) = 0

\* I22 (no resurrection, client level): in the authoritative copy, for every
\* row, the newest acknowledged operation is reflected: after a delete only a
\* newer (unacknowledged) put can be visible; a put is visible or overridden
\* by something newer, never shadowed by a partition deletion.
Newest(r) == {a \in acked : a.row = r /\ \A b \in acked : b.row = r => ~Beats(b, a)}
I22 == \A r \in Rows : \A m \in Newest(r) : CopyReflects(AuthNow, m)

\* I16 to I20 as history: a pass outside a window, a reverse write into an
\* unclean slow-mode target, a base tombstone not written by the reclaim, or an
\* early bucket deletion sets a flag.
I16 == "I16" \notin flags
I17 == "I17" \notin flags
I18 == "I18" \notin flags
I19 == /\ "I19" \notin flags
       /\ (basePD # None /\ Phase = "STEADY" /\ mode = "none") => baseState = "TOMBSTONED"
I20 == "I20" \notin flags

\* Rows are not interchangeable (History tells them apart).
Symmetry == Permutations(Coords) \cup Permutations(Writes)

-----------------------------------------------------------------------------
\* Reachability probes: each is expected to be VIOLATED.

AllAckedOps == \A o \in Ops : Acked(o)
AckedDelete == \E a \in acked : a.kind = "del"
\* A fast unsplit completes with every operation acknowledged, one of them a delete.
ProbeFastUnsplit == ~(Phase = "UNSPLIT" /\ mode = "FAST" /\ AllAckedOps /\ AckedDelete)
\* A slow unsplit completes after the reclaim (or forward) tombstone and its purge.
ProbeSlowUnsplit == ~(Phase = "UNSPLIT" /\ mode = "SLOW" /\ AllAckedOps)
\* A reverse aborts back to STEADY with every operation acknowledged.
ProbeReverseAbort == ~(Phase = "STEADY" /\ Epoch > 2 /\ AllAckedOps /\
                       \E e \in 1..Epoch : log[e] = "REVERSE_BACKFILL")
\* After a fast unsplit, an acknowledged delete copied to base is purged there
\* (with the residue row it shadowed) and I22 still holds.
ProbePurgedAfterFast == ~(Phase = "UNSPLIT" /\ mode = "FAST" /\
                          \E d \in acked : d.kind = "del" /\ d \notin base /\ d \notin overflow)

=============================================================================
