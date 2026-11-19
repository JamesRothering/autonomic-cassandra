<!--
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
-->

# Formal specifications

`layout-migration.tla` models the layout migration protocol of
`docs/architecture/adaptive-schema.md`, sections 4.1 to 4.3 (task T6.15), for one
partition key. It has one owner that commits phases to the cluster metadata log, N
coordinators that each apply a prefix of that log at their own pace (A1), a base copy
and an overflow copy, and client writes and reads that are executed by coordinators
according to the class of the phase they captured (sections 3.2 to 3.4). Operations can
stall or crash between sends (unacknowledged writes), be re-executed under rule C1, or
time out. Backfill, verification (read as `base \subseteq overflow`, ADR-030), the
reconcile pass of `CLEANUP` and the base tombstone are modelled at the level of whole
copies; replicas and consistency levels are not modelled.

The coordinator part (phases and classes, `Learn`, rules O1 and C1, the execution of an
attempt) is in `layout-core.tla`, which this model and `layout-reverse.tla` (below)
extend. The forward model's behaviour did not change when the core was split out (see
the re-run below).

Checked invariants (section 4.10): `I1` (the authoritative copy of the current class
holds every acknowledged write), `I2` (no two nodes know phases more than one class
apart), `I4` (overlapping acknowledged operations ran under equal or adjacent classes),
`I5` (an acknowledged read returns every write acknowledged before it started) and `I10`
(after `NEW_ONLY` no acknowledged read used base, and no later phase is below C3), plus
`TypeOK`. TLC's deadlock check is on: a finished run stutters in `Terminated`, so any
other state without a successor would be a stuck protocol.

Three rules can be switched off: `EnforceO1`, `EnforceC1`, and `EnforceDrain`, the wait
in `ANNOUNCED` until operations that started before the key was registered have ended
(ADR-028; such operations take the stock path and are never validated by C1).

## Configurations and results

All runs use 3 coordinators, 2 writes, 1 read, `MaxAttempts = 2` (the design uses 3; 2
keeps the positive run under an hour and still covers re-execution and timeout) and
`MaxRebackfills = 1`. Negative variants check only `TypeOK, I1, I4, I5, I10`, so that
the counterexample is client-visible rather than the trivial `I2` failure. Observed on
4 cores with `TLC_WORKERS=2`, `TLC_HEAP=2500m`, TLC 2.x, JDK 17.

| Config | Checks | Expected | Observed | Distinct states | Time |
|--------|--------|----------|----------|-----------------|------|
| `layout-migration.cfg` | all rules on, all invariants, symmetry over coordinators and writes | no error, no deadlock | no error (depth 59) | 43,367,375 | 59 min |
| `layout-migration-no-O1.cfg` | owner does not wait for all-node ack | counterexample | `I1` violated: owner reaches `CUTOVER` while c1 still knows nothing of the key; c1 writes base only and acks | 153,105 | 10 s |
| `layout-migration-no-C1.cfg` | coordinators do not re-check the version | counterexample | `I1` violated: a write captured under `ANNOUNCED` (C0) stalls, sends to base after `CUTOVER` and acks | 1,510,519 | 41 s |
| `layout-migration-no-drain.cfg` | no wait in `ANNOUNCED` for unrouted operations | counterexample | `I1` violated: a write started while the key was absent sends to base after `CUTOVER` and acks | 6,918,702 | 2 min 38 s |
| `layout-migration-probe1.cfg` | `ProbeSteady`: migration completes with all operations acked | violated | violated (depth 41) | 18,485,863 | 16 min |
| `layout-migration-probe2.cfg` | `ProbeClasses`: writes acked under C1 and under C2 | violated | violated (depth 28) | 1,581,201 | 74 s |
| `layout-migration-probe3.cfg` | `ProbeCutoverOverflowOnly`: cutover with an unacked overflow-only row (ADR-030) | violated | violated (depth 21) | 228,814 | 13 s |
| `layout-migration-probe4.cfg` | `ProbeRemoved`: an abort completes with all operations acked | violated | violated (depth 19) | 147,027 | 9 s |

The probes are reachability checks: a violation is the expected result and shows that
the positive run's invariants are not vacuously true. The symmetry reduction is sound
because every invariant is symmetric in coordinators and writes; without it the
positive run is about 12 times larger and was not run.

Re-run on 2026-11-19, after the core moved to `layout-core.tla` (T6.17i), with
`TLC_WORKERS=2` and `TLC_HEAP=1500m`. Every configuration gave the outcome in the table,
at the same depth where the table records one. `layout-migration.cfg` again found no error, with 43,367,375 distinct
states at depth 59 (2 h 39 min on the shared machine). The runs that stop at a
violation stop at a point that depends on worker scheduling, so their counts differ
slightly: no-O1 153,269; no-C1 1,424,831 (depth 22); no-drain 6,898,993 (depth 22);
probe1 18,472,280; probe2 1,596,754; probe3 228,825; probe4 120,322.

## Reverse migration: `layout-reverse.tla`

`layout-reverse.tla` models section 11 of `adaptive-schema.md` (T6.17i, ADR-036) for one
split key: the reverse protocol of 11.4 (`BeginUnsplit` from `STEADY`, `NEW_ONLY` or
`CLEANUP`; `REVERSE_ANNOUNCED` to `REVERSE_CLEANUP`; `RemoveKey`; aborts and
`REVERSE_ROLLBACK`), the deferred base tombstone with the `reclaim` continuation, fast
and slow mode with the limits of 11.5, the slow-mode purge and residue probe, and the
deletion of the bucket partitions. With `StartPhase = "ABSENT"` the forward protocol runs
first, with the forward cleanup of ADR-036 (no tombstone at `STEADY`; `ForwardTombstone
= TRUE` models the cleanup as built before ADR-036) and the forward replica deadline of
ADR-031.

Copies hold atoms `[row, ts, kind, ldt]` (a put or a row delete, its timestamp and local
deletion time) instead of write ids. Visibility is last-write-wins per row, with a
delete winning a tie, and a partition deletion (`basePD`, or `ovPD` for the bucket
deletion) shadows every atom at or below its timestamp. A clock advances in ticks.
Compaction may purge a delete `GcGrace` ticks after its local deletion time, together
with the atoms it shadows, and may purge `basePD` together with everything at or below
it. Atoms above `basePD` survive that purge (hazard 2 of 11.9). The initial state is
`STEADY` after a forward history: an acknowledged row at timestamp 0 and, with two rows,
an acknowledged row written with a client clock in the future (the r4 row). The residue
equals the overflow copy, plus at most one unacknowledged straggler (a future put, or a
delete). A client write is a put or a delete of a row at timestamp `clock` or
`clock + MaxTime`.

Invariants (11.6): `I1` (the authoritative copy of the current class holds every
acknowledged put), `I2`, `I4`, `I5` (from the core), amended `I10`, `I21`, `I22` (the
newest acknowledged operation on each row is reflected in the authoritative copy: after
a delete only a newer, unacknowledged put may be visible, and a put is never shadowed by
a partition deletion), and `I16` to `I20` as history variables: a pass outside a window,
a slow reverse into an unclean base, a base partition deletion not written by the
reclaim, or an early bucket deletion sets a flag.

Assumptions and abstractions (also stated in 11.7):
- **Stock timestamp assumption.** No client write carries a timestamp at or below that
  of a delete of its row which compaction may already have purged. Without it, even an
  unsplit table shows the older-stamped write once the delete is gone (a future-stamped
  `DELETE`, a purge, then an `INSERT` stamped below it). TLC found exactly this as an
  `I22` counterexample before the guard was added to `Start`.
- An operation's sends, re-executions and ack happen within the tick of its first start.
  `Tick` times out every operation still running: a mutation or hint is never delivered
  later, because the request timeout and the hint TTL are far below `gc_grace`.
- "Held" is a client-level notion. An acknowledged put may be missing from the
  authoritative copy only if a client delete that overrides it was sent. An
  unacknowledged delete may apply (R4), and once purged it leaves nothing behind.
- One replica per copy, so a pass is one atomic step. Compactions happen at tick
  boundaries. The owner's margins (estimate, `straggler_window`, `reconcile_budget`,
  write timeout) are 0, so a limit `L` means "only while `clock < L`". The probe of 11.5
  is folded into the guard of `REVERSE_ANNOUNCED -> REVERSE_DUAL_WRITE`.
- `GcGrace = 2` (so `G = 1`) and `MaxTime = 4`, where 11.7 proposed 4 and about 8: with
  the larger values even the one-write configuration has more than 4 million states. The
  windows are strict comparisons on integer ticks, so `G = 1` still separates inside a
  window from outside it, and every negative variant below needs at most 4 ticks.
- `MaxUnsplits = 1` (one `BeginUnsplit` per run, which may abort). An unbounded number of
  attempts makes the state space infinite.
- TLC's deadlock check is off. Every state before `MaxTime` can tick, and at `MaxTime`
  a state may wait for time the model does not have. The probes show liveness.

Configurations (all `GcGrace = 2`, `MaxTime = 4`, `MaxAttempts = 2`, `MaxRebackfills = 1`,
`MaxUnsplits = 1`, symmetry over coordinators and writes). The positive configurations
check every invariant above. Negative variants check `TypeOK, I1, I4, I5, I10, I21,
I22` with 1 write and no read (`no-O1`: 1 read). Observed with TLC 2026.11 and JDK 17 on
a shared 4-core machine. Positive runs used `TLC_WORKERS=2` and `TLC_HEAP=1500m`. The
negative variants and probes used one worker and 1200m.

| Config | Constants | Expected | Observed | Distinct states | Depth | Time |
|--------|-----------|----------|----------|-----------------|-------|------|
| `layout-reverse.cfg` | deferred tombstone (design), every rule on, all invariants; 2 coords, 1 write, 1 read, rows r1 (ts 0) and r4 (future) | no error | no error | 15,328,316 | 49 | 29 min 38 s |
| `layout-reverse-legacy.cfg` | `ForwardTombstone = TRUE` (keys split before ADR-036, slow mode only); one row | no error | no error | 3,899,567 | 49 | 7 min 17 s |
| `layout-reverse-2w.cfg` | 1 coordinator, 2 writes, no read, one row | no error | no error | 9,536,067 | 42 | 22 min 45 s |
| `layout-reverse-full.cfg` | `StartPhase = "ABSENT"`: forward split, then the reverse from `NEW_ONLY`, `CLEANUP` or `STEADY`; `MaxAttempts = 1`, 1 write, no read | no error | no error | 2,866,433 | 70 | 7 min 42 s |
| `layout-reverse-no-window.cfg` | `EnforceResidueWindow = FALSE`, 2 coords, 1 write | `I22` | `I22`: a delete of the r1 row acked in C3 at t0 is purged from overflow at t2; then a fast reverse starts, the backfill copies nothing for the row, and at `CUTBACK` the residue row is visible again | 347,118 | 20 | 56 s |
| `layout-reverse-no-purge.cfg` | `ForwardTombstone = TRUE`, `EnforcePurge = FALSE` | `I1`/`I22` | `I1`: a slow reverse at t0 copies r1@0 into base under `basePD = 0`; at `CUTBACK` the acked row is shadowed (11.1) | 156,419 | 15 | 27 s |
| `layout-reverse-no-probe.cfg` | `ForwardTombstone = TRUE`, `ProbeChecksResidue = FALSE` | `I22` | `I22`: a delete stamped in the future (at or above r4's timestamp) is acked in `REVERSE_ANNOUNCED` and purged from overflow at t2, where the base purge removes `basePD` but keeps r4; the reverse copies nothing for r4, and at `CUTBACK` r4 is visible (the r4 example) | 154,130 | 20 | 24 s |
| `layout-reverse-no-O1.cfg` | `EnforceO1 = FALSE`, 1 read | `I1` | `I22`: the owner reaches `CUTBACK` while c1 still knows only `STEADY`; c1 acks a delete written to overflow only, and base readers still see the row. The same check without `I22` fails `I1` (a put in the same position), depth 10 | 229,244 | 10 | 31 s |
| `layout-reverse-no-C1.cfg` | `EnforceC1 = FALSE` | `I1` | `I22`: a delete captured under `REVERSE_ANNOUNCED` (C3) sends to overflow only after `CUTBACK` and acks. Without `I22`: `I1`, depth 18 | 171,642 | 18 | 25 s |
| `layout-reverse-bucket-early.cfg` | `EnforceBucketDeleteOrder = FALSE` | `I1` | `I1`: the buckets are deleted in `REVERSE_CLEANUP`, and the reconcile pass maps the bucket partition deletion to `basePD = 0`, which shadows the acked r1@0 | 341,322 | 23 | 52 s |
| `layout-reverse-no-deadline.cfg` | `ForwardTombstone = TRUE`, `EnforceReverseDeadline = FALSE`, one row | `I22` | `I22`: slow reverse at t2; a delete acked in `REVERSE_CLEANUP` (C0, base only) at t2 is purged at t4, after which the reconcile copies the older overflow row r1@0 | 102,329 | 29 | 33 s |
| `layout-reverse-probe-fast.cfg` | `ProbeFastUnsplit` | violated | violated | 11,533,442 | 31 | 22 min |
| `layout-reverse-probe-slow.cfg` | `ProbeSlowUnsplit` (reclaim, purge, clean probe), one row | violated | violated | 5,482,325 | 35 | 11 min |
| `layout-reverse-probe-abort.cfg` | `ProbeReverseAbort` | violated | violated | 2,042,772 | 17 | 4 min |
| `layout-reverse-probe-purged.cfg` | `ProbePurgedAfterFast`: after a fast unsplit an acked delete is purged from base | violated | violated | 10,744,413 | 30 | 21 min |
| `layout-reverse-probe-full.cfg` | `ProbeFastUnsplit` with the constants of `layout-reverse-full.cfg`: split, then a fast unsplit | violated | violated | 1,996,814 | 47 | 2 min 14 s (2 workers) |

Negative variants and probes stop at the first violation, so their state counts depend
on the number of workers (1 here). In `no-O1` and `no-C1` breadth-first search reaches
the delete variant of the counterexample first. 11.7 names `I1` for these two, and `I1`
fails as well when `I22` is not checked. `no-deadline` needs slow mode, because in fast
mode the residue window (`frozenSince + G`) is never later than the reverse deadline.
It also needs one row, because with the future-stamped r4 row the probe never passes
(correctly), so no slow reverse would start.

## Running

```
TLA_JAR=/path/to/tla2tools.jar TLC_HEAP=2500m TLC_WORKERS=2 \
    sh docs/specs/run.sh layout-migration.cfg -gzip
```

`run.sh` copies `layout-core.tla` and the spec to a temporary directory as
`layout_core.tla`, `layout_migration.tla` and `layout_reverse.tla` (a module name cannot
contain a hyphen) and runs TLC there. A `layout-reverse*.cfg` runs the reverse model.
The positive runs keep several GB of state queue and fingerprints on disk under that
directory; `-gzip` reduces it. Run one large configuration at a time.
