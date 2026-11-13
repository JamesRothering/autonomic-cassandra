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

- [ ] Each actuator has a unit test that applies a change and restores the previous value on rollback.
- [ ] A change whose verification fails is rolled back, and the rollback is recorded in the audit trail.
- [ ] Automatic repair runs only when the rate and blast-radius limits allow it, verified by a two-node in-JVM test.

Scope and acceptance criteria will be refined after D1.

## Status

Not started.
