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

- [ ] Both specs (`overview.md` and `adaptive-schema.md`) are merged to `main`.
- [ ] ADRs are recorded for each significant decision made in D1, and ADR-001 is re-confirmed or superseded.
- [ ] Every code reference in the specs points at a real file in the pinned tree.
- [ ] The scopes of D2 to D7 are refined from the specs.

## Status

Not started.
