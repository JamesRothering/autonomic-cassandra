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

# D4: Self-Healing

## Goal

Act on diagnoses with local tuning changes, verify each change, and roll it back when verification fails.

## Scope

- **T4.1** Local tuning actuators: compaction throughput and concurrency, cache sizing, tombstone-driven compaction.
- **T4.2** Apply, verify, and rollback closed loop.
- **T4.3** Automatic repair scheduling.

## Acceptance criteria

- [x] Each actuator has a unit test that applies a change and restores the previous value on rollback.
- [x] A change whose verification fails is rolled back, and the rollback is recorded in the audit trail.
- [x] Automatic repair runs only when the rate and blast-radius limits allow it, verified by a two-node in-JVM test.

Refined in D1. The authoritative task list, with files, dependencies, acceptance criteria and model tier per task, is section 13 of [overview.md](../architecture/overview.md) (T4.1 to T4.5). Automatic repair scheduling already exists upstream (ADR-009), so that task is now auto-repair integration (T4.4). The scope above is the original outline.

## Status

Implemented, awaiting owner review (2026-11-14). T4.1 to T4.5 are committed on `deliverable/D4-self-healing`.

- Criterion 1: the per-actuator contract tests (`KnobActuatorTest` and one test class per actuator); `ActuatorContractCoverageTest` fails when a built actuator has no contract test. `garbage_collect` and `repair_priority` are irreversible by design and have no rollback.
- Criterion 2: `SelfHealingRollbackTest.aRegressingChangeIsRolledBackAndAudited` asserts the `action_events` rows (APPLYING, VERIFYING, ROLLING_BACK, ROLLED_BACK) and the output of `nodetool autonomic events`.
- Criterion 3: `AutoRepairPriorityTest` on two nodes: rejected before `cluster enable`, rejected while another node holds the MAINTENANCE claim, one host given priority once the claim is free, then rejected for the rate limit and the cooldown. With auto-repair disabled only an INFO diagnosis appears.

Points for the review:

- ADR-026 (Accepted): the `repair_overdue` detector reads `system_distributed` in a background task on a core executor, and `analyze` only reads the last snapshot. `Detector` gained default `start()` and `stop()` methods for this.
- `repair_priority` is irreversible: no public API removes a priority entry, so the action commits after reading the entry back and needs a CRITICAL diagnosis under automatic approval. Under the default manual approval that rule is waived.
- A host with no repair history counts as overdue, so a cluster that never enabled auto-repair gets an INFO diagnosis per host.
- The rate window is the governor's fixed one hour; the dtest uses `max_cluster_actions_per_hour: 1`.
- When `compaction_backlog` and `degraded_self` are both open, the correlator marks `degraded_self` as an effect and policies skip it, so the two do not contend for compaction throughput.
- If an operator changes a managed knob by hand, the layer stops adjusting it until `nodetool autonomic revert`.
- `advertised_severity` was tested with a fake handle only.
