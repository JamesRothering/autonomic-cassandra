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

# D3: Self-Diagnosis

## Goal

Detect known failure conditions on each node and explain them in terms an operator can act on.

## Scope

- **T3.1a** Detectors: hot partition, latency SLO breach, tombstone overload.
- **T3.1b** Detectors: compaction backlog, disk pressure, GC pressure, node imbalance.
- **T3.2** Rules-based root-cause correlation and human-readable diagnoses.

## Acceptance criteria

- [x] Each detector has a unit test with a fixed input signal that triggers it and one that does not.
- [x] An injected hot partition in a single-node in-JVM test produces a hot-partition diagnosis that names the partition.
- [x] Each diagnosis is written to `system_autonomic` and visible through `nodetool autonomic history`.

Refined in D1. The authoritative task list, with files, dependencies, acceptance criteria and model tier per task, is section 13 of [overview.md](../architecture/overview.md) (T3.1 to T3.6). The scope above is the original outline.

## Status

Implemented, awaiting owner review (2026-11-14). T3.1 to T3.6 are committed on `deliverable/D3-self-diagnosis`.

- Criterion 1: `DetectorsTest`, `NodeDetectorsTest`, `PeerDetectorsTest` (a triggering and a non-triggering fixture for each of the eleven detectors) and `ThresholdDetectorTest`.
- Criterion 2: `HotPartitionDiagnosisTest` (single-node dtest; the summary names the key as a CQL literal and the evidence carries `key_hex`). The same class shows `tombstone_overload` from tombstone-heavy reads.
- Criterion 3: `HotPartitionDiagnosisTest` reads the diagnosis from `system_autonomic.diagnoses` and from `nodetool autonomic history --diagnoses`; `DiagnosisPersistenceTest`.

Points for the review:

- Persistence counts distinct samples, so a hot partition is diagnosed after about four sampling intervals (about four minutes with defaults), not four control ticks.
- `degraded_self` is an interpretation of the task (see section 7.4 of overview.md).
- `nodetool autonomic history --diagnoses` prints subject and summary; it does not print evidence or the root cause. They are in the table.
- `HotPartitionDiagnosisTest` failed once in the worker's first run and passed in six later runs; the cause of the one failure was not established.
