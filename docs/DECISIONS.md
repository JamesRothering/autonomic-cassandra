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

# Architectural and Design Decisions Log

This log records architectural and design decisions as numbered Architecture Decision Records (ADRs).

The log is append-only. Never edit a decided entry. If a decision changes, add a new ADR that supersedes the old one, and update the old entry's Status line to point at the new ADR (`Superseded by ADR-XXX`). The only other permitted edit to an existing entry is the Status line itself.

## Template

```
## ADR-NNN: Title

- **Date:** YYYY-MM-DD
- **Status:** Proposed | Accepted | Superseded by ADR-XXX
- **Deliverable:** Dn

**Context.** The forces and facts that make a decision necessary.

**Decision.** What we decided, stated in the active voice.

**Alternatives considered.** Other options and why they were not chosen.

**Consequences.** What follows from the decision, including costs and obligations.
```

## Index

| ADR | Title | Status | Date |
|-----|-------|--------|------|
| [ADR-001](#adr-001-base-the-fork-on-a-pinned-apache-cassandra-trunk-commit) | Base the fork on a pinned Apache Cassandra trunk commit | Accepted (to be re-confirmed in D1) | 2026-11-12 |
| [ADR-002](#adr-002-organize-work-as-deliverables-d0d7-and-keep-changes-separate-from-core) | Organize work as deliverables D0-D7 and keep changes separate from core | Accepted | 2026-11-12 |
| [ADR-003](#adr-003-build-with-jdk-17-and-a-user-local-ant-under-tight-resource-limits) | Build with JDK 17 and a user-local Ant under tight resource limits | Accepted | 2026-11-12 |

## ADR-001: Base the fork on a pinned Apache Cassandra trunk commit

- **Date:** 2026-11-12
- **Status:** Accepted (to be re-confirmed by the D1 architecture review)
- **Deliverable:** D0

**Context.** Safe on-the-fly schema and layout changes need cluster-wide metadata changes that are linearizable. Trunk includes Transactional Cluster Metadata (CEP-21), which provides this. Trunk also includes built-in auto-repair (CEP-37), which the repair scheduling work in D4 depends on. The alternative base, the cassandra-5.0 release branch, is more stable but lacks both features.

**Decision.** Base the fork on Apache Cassandra trunk at commit `2249e37b51af9fd2a72865bbb56c1197e0d0d1ed`. Changes to the base are made only by deliberate rebase onto a newer pinned commit, recorded in a new ADR.

**Alternatives considered.** The cassandra-5.0 branch was rejected because it lacks CEP-21 and CEP-37, and building those features ourselves would cost more than tracking trunk. Following trunk head without a pin was rejected because a moving base makes builds and test results irreproducible.

**Consequences.** Trunk is unstable compared with a release branch, so the fork will see upstream breakage and API churn. The pin keeps our results reproducible in the meantime. Each rebase costs merge effort and needs its own verification run.

## ADR-002: Organize work as deliverables D0-D7 and keep changes separate from core

- **Date:** 2026-11-12
- **Status:** Accepted
- **Deliverable:** D0

**Context.** The project spans monitoring, diagnosis, self-healing, elasticity, and schema migration. Work must be checkpointed so that each stage can be verified before the next one begins. Changes to upstream code must stay small, because each upstream merge or rebase otherwise becomes expensive. Upstream keeps its documentation in `doc/`, so project documentation needs a separate location.

**Decision.** Organize work as deliverables D0 to D7. Each deliverable has its own git branch, `deliverable/Dn-<name>`, and is tagged `Dn` when merged to `main`. Project documentation lives in a new top-level `docs/` directory. New code lives in the package `org.apache.cassandra.autonomic`, and changes to core code are limited to minimal hook points.

**Alternatives considered.** Putting the work on a single long-lived branch was rejected because it gives no verifiable checkpoints. Placing documentation in upstream's `doc/` was rejected to avoid mixing project documents with upstream's own. Spreading changes through core packages was rejected because it would make upstream merges expensive.

**Consequences.** Hook points in core must be designed deliberately in D1 and then kept stable. Each deliverable needs a merge and tag step. Upstream merges touch fewer files.

## ADR-003: Build with JDK 17 and a user-local Ant under tight resource limits

- **Date:** 2026-11-12
- **Status:** Accepted
- **Deliverable:** D0

**Context.** `build.xml` declares `java.supported=11,17,21` with a default of 11. The development machine has 4 cores and 5 GB of RAM and no root access. A full upstream test suite does not fit in that budget.

**Decision.** Build with JDK 17. Install JDK 17 and Ant in user-local directories, without root. Verify changes with targeted unit tests and small (2 to 3 node) in-JVM distributed tests, not the full test suite.

**Alternatives considered.** The default JDK 11 was rejected because it is the oldest supported version and new code would be limited to its language level. JDK 21 was not chosen because upstream CI runs its checks on 11 and 17, so 17 is the newest version with routine upstream coverage. The full test suite was rejected because it exceeds the local memory and time budget.

**Consequences.** Test coverage is narrower than upstream CI, so CI or a larger machine will be needed for full runs before any release. Toolchain setup is part of D0 and is documented in `docs/BUILD.md`.
