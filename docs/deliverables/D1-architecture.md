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

# D1: Architecture

## Goal

Specify the autonomic architecture and the adaptive-schema design before any code is written, and record the decisions behind them.

## Scope

- **T1.1** Architecture overview spec (`docs/architecture/overview.md`): MAPE-K components and their interfaces; hook points in core; safety model (dry-run, kill switch, rate and blast-radius limits, verify-then-rollback); cluster-wide coordination; in-node versus scale-out elasticity.
- **T1.2** Adaptive-schema design (`docs/architecture/adaptive-schema.md`): logical-to-physical layout registry; query rewrite; dual-write, backfill, cutover, and rollback state machine; compatibility guarantees; failure modes.

## Acceptance criteria

- [x] Both specs (`overview.md` and `adaptive-schema.md`) are merged to `main`.
- [x] ADRs are recorded for each significant decision made in D1, and ADR-001 is re-confirmed or superseded.
- [x] Every code reference in the specs points at a real file in the pinned tree.
- [x] The scopes of D2 to D7 are refined from the specs.

## Review notes (orchestrator)

- Every existing source path cited in both specs was checked against the pinned tree; the only paths that do not exist are files the specs propose to create.
- Hook points H1 to H12 and the knob setters of overview 8.1 were spot-checked by hand.
- No code was compiled or run in D1. Items the specs mark **unverified** are carried into the tasks that depend on them (overview R1, R9; adaptive-schema R2, R9).
- The phase-agreement argument of adaptive-schema 4.3 (rules O1 and C1) was reviewed and accepted, conditional on assumption A1, which T6.6 tests and T6.15 model-checks.

## Questions put to the project owner (answered 2026-11-13; see ADR-024)

Framework (overview section 12):

1. Is it acceptable that enabling cluster-scoped actions makes the cluster incompatible with stock Cassandra nodes?
2. Should automatic scale-in ever be in scope? The design excludes it.
3. Is manual approval the right default for cluster-scoped actions?
4. Is node-local audit history sufficient, or is cluster-wide queryable history required?
5. Should the Kubernetes stub remain a deliverable?

Adaptive schema (adaptive-schema section 10; ADR-020 stays Proposed until answered):

6. Is the compatibility contract acceptable: LWT withdrawn on adapted partitions, weaker isolation for multi-row writes, the behaviour of unknown-outcome writes, and a visible companion table?
7. Is moving only the hot partition an acceptable reading of "changes the schema on the fly", or is whole-table re-partitioning required?
8. Is a commit point with no loss-free rollback acceptable in the MVP?
9. Should the commit point require a second manual approval?
10. Is stalling while any node is unreachable acceptable?
11. Are six more core hooks, five in `StorageProxy`, acceptable?
12. Should DDL mirroring (T6.14) be in the MVP?
13. Is raising `ANY` to `ONE` on adapted partitions acceptable?

## Status

Done. Specs reviewed on 2026-11-12; owner decisions recorded in ADR-024 on 2026-11-13. Tagged `D1`.
