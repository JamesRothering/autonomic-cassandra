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

# D6: Adaptive Schema

## Goal

Change the physical layout of a table in response to a hot spot while clients that use the old logical schema continue to get the same results.

## Scope

- **T6.1** Logical-to-physical layout registry, propagated via cluster metadata.
- **T6.2** Coordinator query rewrite: fan-out reads with merge; routed writes and deletes.
- **T6.3** Migration engine: dual-write, backfill, cutover, and rollback.
- **T6.4** Compatibility tests: old-schema queries return identical results before, during, and after migration.

## Acceptance criteria

- [x] A layout change is recorded in cluster metadata and seen by every node in a three-node in-JVM test.
- [x] Queries written against the logical schema return identical results before, during, and after a migration, in the compatibility test suite.
- [x] A migration that fails during backfill is rolled back to the old layout, and no acknowledged write is lost.

Refined in D1. The authoritative task list, with files, dependencies, acceptance criteria and model tier per task, is section 9 of [adaptive-schema.md](../architecture/adaptive-schema.md) (T6.1 to T6.16; T6.17, reverse migration, is the first follow-up after D6 per ADR-025). D6 depends on T2.8 (cluster ledger) specifically. The scope above is the original outline.

Operator guide for partition split (T6.16): [operations/adaptive-schema.md](../operations/adaptive-schema.md).

## Status

Done. Merged to `main` and tagged `D6` on 2026-11-18. The owner waived the review gate for D6 ("merge to main without review"), so ADR-034 and the D6 ADRs still await owner review.

- Criterion 1: `LayoutRegistryTest` (dtest: the registry through restart replay and CMS snapshots, on nodes with and without the layer), `LayoutTransformationTest`, `LayoutServiceTest`, and the three-node compatibility suite below, where every node routes from the registry it has applied.
- Criterion 2: `LayoutCompatibilitySuite` with `LayoutCompatibilityEarlyTest`, `LateTest`, `OneBehindTest`, `OwnerGateTest` and `RollbackTest` (T6.12: every statement of 3.3 to 3.6 against a never-split twin, with the key held in each phase and a node one epoch behind), and `PartitionSplitEndToEndTest` (T6.9: 3 nodes, a 5000-row partition split into 8 buckets under live load checked against a model).
- Criterion 3: `SplitOwnerCrashTest` (owner killed in `BACKFILL` aborts and rolls back; past the commit point it rolls forward), `LayoutCompatibilityRollbackTest` (everything written while split is read back from the base table after the abort), `BackfillResumeTest`, `SplitCoordinatorCrashTest`, `SplitCmsUnavailableTest`, `SplitHeldCoordinatorTest`, `SplitDeleteRaceTest`, `SplitLateHintTest` and `SplitDecommissionTest` (T6.13), each checking no acknowledged write is lost.
- Protocol: TLA+ model in `docs/specs/` (T6.15). It passes I1, I2, I4, I5 and I10 with 3 coordinators and 2 writes (MaxAttempts 2), and removing O1, C1 or the drain wait (ADR-028) each gives an I1 counterexample.
- Operator guide: `docs/operations/adaptive-schema.md` (T6.16). Not verified there: a node restart while a migration is in `CLEANUP` on a single node (covered on three nodes by `SplitOwnerCrashTest.ownerLostInCleanup`).
- Built differently from FC9: `max_layouts` and `max_keys_per_layout` are constants (8 and 8), and there is no `straggler_reconcile` setting (see `overview.md`).
- Follow-up: T6.17, reverse migration (ADR-025).
