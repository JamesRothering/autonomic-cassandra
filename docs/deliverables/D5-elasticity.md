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

# D5: Elasticity

## Goal

Adapt to load spikes within a node first, then provide a provider interface for adding or removing nodes.

## Scope

- **T5.1** In-node elasticity: thread-pool resizing; adaptive throttling and backpressure.
- **T5.2** Scale-out provider SPI, with a simulated provider and a Kubernetes stub.

## Acceptance criteria

- [x] Thread-pool resizing stays within configured minimum and maximum sizes under a load spike in a single-node test.
- [x] Adaptive throttling reduces the rate of accepted requests when latency rises above the SLO and restores it when latency recovers.
- [x] The simulated provider adds a node in a three-node in-JVM test (growing to four). Removing a node is out of scope (ADR-010), and the Kubernetes stub is replaced by a documented example in the operator guide (ADR-024; written in D7, T7.3).

Refined in D1. The authoritative task list, with files, dependencies, acceptance criteria and model tier per task, is section 13 of [overview.md](../architecture/overview.md) (T5.1 to T5.5). Automatic scale-in is out of scope and the Kubernetes stub is reduced to a documented example unless the owner decides otherwise (ADR-010). The scope above is the original outline.

## Status

Done. Approved by the owner on 2026-11-15. Tagged `D5`.

- Criterion 1: `RequestPoolElasticityTest` (1-node dtest with injected pending tasks: sizes move, stay within the envelope and return on recovery), `RequestPoolActuatorTest`.
- Criterion 2: `RequestThrottleTest` (dtest: limiter enabled and lowered on a latency breach, raised and released on recovery, never below the floor), `RequestThrottleActuatorTest` (the AIMD sequence).
- Criterion 3: `ScaleOutTest` (dtest, 3 nodes growing to 4): nothing is requested before `cluster enable --yes` and before `nodetool autonomic approve`; after approval one node is requested and joins; the ledger and `action_events` show REQUESTED, PROVISIONING, JOINING and VERIFYING; the TOPOLOGY claim is released on every node. Also covered: the default `NoopProvider` gives a diagnosis only, and a shutdown during provisioning cancels the request. `ScaleOutActuatorTest` (32 tests) covers every phase, each timeout and owner loss in each phase, including no double provisioning.

Owner decisions at this gate (2026-11-15): the last throttle step stays subject to the improvement check, so the limit can settle just above the floor; ladder ineffectiveness stays per rung, not per knob.

Points for the review:

- `ScaleOutActuator` resumes on `recover` instead of undoing (overview 6.4 reworded). A started join is never undone; it ends FAILED with a CRITICAL diagnosis and the layer pauses.
- Two shared-code changes: a takeover of an expired claim is exempt from the no-sequences rule (only for the same actuator and target, in the recovering phase), and the all-members-alive check now also applies to the TOPOLOGY lane (overview 7.1 check 5).
- A bug found by `ScaleOutTest` and fixed: on shutdown only the first step of a multi-step stop ran, so the claim of a cancelled scale-out stayed in the ledger. `ClusterActionExecution.kill` now runs up to 10 immediate steps.
- The new node is recognised by its address, a node id higher than at request time, and a joining state in the metadata.
- A provider that breaks the same-ticket-for-the-same-request rule can still cause a double node; a cancel that keeps failing ends FAILED and may leave an orphan for the operator.
- New `CpuSensor` (`os.load_per_processor`), and an integral knob step now rounds to the nearest whole number.
- Only node 1 runs the layer in `ScaleOutTest`; takeover of a scale-out by another node is covered by unit tests only.
