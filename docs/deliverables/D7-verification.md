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

# D7: Verification

## Goal

Show the autonomic behaviors end to end under realistic scenarios, and document how operators use the system.

## Scope

- **T7.1** In-JVM multi-node scenario tests: injected hot spot, load spike, and degraded node.
- **T7.2** cassandra-stress workload profiles that drive the scenarios.
- **T7.3** Operator guide.

## Acceptance criteria

- [ ] Each scenario in T7.1 runs in CI or on the development machine within the resource limits in ADR-003.
- [ ] For each scenario, the test asserts the expected diagnosis, action, and verification outcome.
- [ ] The operator guide covers configuration, the kill switch, `nodetool autonomic`, and rollback, and each step is checked against a running node.

Scope and acceptance criteria will be refined after D1.

## Status

Not started.
