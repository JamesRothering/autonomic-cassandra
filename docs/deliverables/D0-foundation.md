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

# D0: Foundation

## Goal

Establish a reproducible build of the pinned Apache Cassandra trunk base, a repository layout for project work, and a decisions log.

## Scope

- **T0.1** Toolchain and pinned clone: install JDK 17 and Ant user-locally (ADR-003); clone the upstream repo and pin commit `2249e37b51af9fd2a72865bbb56c1197e0d0d1ed` (ADR-001).
- **T0.2** Repo layout and decisions log: create `docs/`, the deliverables plan, and `docs/DECISIONS.md`; add the top-level `README.md`; set up the `main` and `deliverable/D0-foundation` branch workflow.
- **T0.3** Baseline build, smoke tests, and `docs/BUILD.md`: run `ant jar` on the pinned commit, run a smoke set of unit tests, and document the steps.

## Acceptance criteria

- [x] `ant jar` succeeds on the pinned commit with JDK 17.
- [x] The smoke unit tests pass.
- [x] The GitHub repository exists with `main` and a tag `D0`.

## Status

Done. Tagged `D0` on 2026-11-12. Build: `ant jar` in 403 s; smoke tests ByteBufferUtilTest (13), FBUtilitiesTest (22), SimpleQueryTest (19) all passed. See [BUILD.md](../BUILD.md).
