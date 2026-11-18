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

## Running

```
TLA_JAR=/path/to/tla2tools.jar TLC_HEAP=2500m TLC_WORKERS=2 \
    sh docs/specs/run.sh layout-migration.cfg -gzip
```

`run.sh` copies the spec to a temporary directory as `layout_migration.tla` (a module
name cannot contain a hyphen) and runs TLC there. The positive run keeps several GB of
state queue and fingerprints on disk under that directory; `-gzip` reduces it. Run one
large configuration at a time.
