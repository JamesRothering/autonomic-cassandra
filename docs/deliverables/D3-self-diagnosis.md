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

- [ ] Each detector has a unit test with a fixed input signal that triggers it and one that does not.
- [ ] An injected hot partition in a single-node in-JVM test produces a hot-partition diagnosis that names the partition.
- [ ] Each diagnosis is written to `system_autonomic` and visible through `nodetool autonomic history`.

Scope and acceptance criteria will be refined after D1.

## Status

Not started.
