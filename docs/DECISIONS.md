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
| [ADR-001](#adr-001-base-the-fork-on-a-pinned-apache-cassandra-trunk-commit) | Base the fork on a pinned Apache Cassandra trunk commit | Accepted (re-confirmed by ADR-004) | 2026-11-12 |
| [ADR-002](#adr-002-organize-work-as-deliverables-d0d7-and-keep-changes-separate-from-core) | Organize work as deliverables D0-D7 and keep changes separate from core | Accepted | 2026-11-12 |
| [ADR-003](#adr-003-build-with-jdk-17-and-a-user-local-ant-under-tight-resource-limits) | Build with JDK 17 and a user-local Ant under tight resource limits | Accepted | 2026-11-12 |
| [ADR-004](#adr-004-re-confirm-the-pinned-trunk-base-after-verifying-cep-21-and-cep-37-in-the-code) | Re-confirm the pinned trunk base after verifying CEP-21 and CEP-37 in the code | Accepted | 2026-11-12 |
| [ADR-005](#adr-005-run-one-in-process-mape-k-loop-per-node-with-three-dedicated-threads) | Run one in-process MAPE-K loop per node with three dedicated threads | Accepted | 2026-11-12 |
| [ADR-006](#adr-006-coordinate-cluster-scoped-actions-through-a-ledger-in-tcm-using-the-custom-transformation-extension-point) | Coordinate cluster-scoped actions through a ledger in TCM using the CUSTOM transformation extension point | Accepted | 2026-11-12 |
| [ADR-007](#adr-007-keep-knowledge-in-three-tiers-memory-a-node-local-keyspace-and-the-tcm-ledger) | Keep knowledge in three tiers: memory, a node-local keyspace, and the TCM ledger | Accepted | 2026-11-12 |
| [ADR-008](#adr-008-commit-an-action-only-when-verification-shows-improvement-and-fail-static) | Commit an action only when verification shows improvement, and fail static | Accepted | 2026-11-12 |
| [ADR-009](#adr-009-reuse-built-in-auto-repair-instead-of-building-a-repair-scheduler) | Reuse built-in auto-repair instead of building a repair scheduler | Accepted | 2026-11-12 |
| [ADR-010](#adr-010-respond-to-load-in-node-first-and-treat-scale-out-as-a-manually-approved-add-only-provider-spi) | Respond to load in-node first and treat scale-out as a manually approved, add-only provider SPI | Accepted | 2026-11-12 |
| [ADR-011](#adr-011-limit-core-changes-to-an-enumerated-set-of-one-statement-hooks-placed-beside-the-auto-repair-hooks) | Limit core changes to an enumerated set of one-statement hooks placed beside the auto-repair hooks | Accepted | 2026-11-12 |
| [ADR-012](#adr-012-place-the-adaptive-schema-indirection-on-the-coordinator-and-keep-layouts-in-cluster-metadata) | Place the adaptive-schema indirection on the coordinator and keep layouts in cluster metadata | Accepted (refined by ADR-014 to ADR-022) | 2026-11-12 |
| [ADR-013](#adr-013-sensors-pull-from-existing-metric-objects-and-add-no-instrumentation-to-request-paths) | Sensors pull from existing metric objects and add no instrumentation to request paths | Accepted | 2026-11-12 |
| [ADR-014](#adr-014-relieve-a-hot-wide-partition-by-hash-splitting-that-partition-alone-into-a-companion-table) | Relieve a hot wide partition by hash-splitting that partition alone into a companion table | Accepted | 2026-11-12 |
| [ADR-015](#adr-015-route-adapted-partitions-at-the-storageproxy-boundary-on-mutations-and-read-commands) | Route adapted partitions at the StorageProxy boundary on mutations and read commands | Accepted | 2026-11-12 |
| [ADR-016](#adr-016-agree-on-migration-phases-by-all-node-epoch-acknowledgement-and-validate-after-execute-on-coordinators) | Agree on migration phases by all-node epoch acknowledgement and validate-after-execute on coordinators | Accepted | 2026-11-12 |
| [ADR-017](#adr-017-order-dual-writes-so-the-authoritative-copy-is-written-last-and-use-the-batchlog-only-where-atomicity-across-mutations-is-needed) | Order dual writes so the authoritative copy is written last, and use the batchlog only where atomicity across mutations is needed | Accepted | 2026-11-12 |
| [ADR-018](#adr-018-backfill-from-every-base-replicas-local-unfiltered-data-and-verify-by-a-coordinator-level-comparison) | Backfill from every base replica's local unfiltered data, and verify by a coordinator-level comparison | Accepted | 2026-11-12 |
| [ADR-019](#adr-019-keep-layouts-in-their-own-bounded-metadata-extension-create-companion-tables-by-ordinary-ddl-and-couple-client-ddl-in-alterschema) | Keep layouts in their own bounded metadata extension, create companion tables by ordinary DDL, and couple client DDL in AlterSchema | Accepted | 2026-11-12 |
| [ADR-020](#adr-020-default-compatibility-contract-for-clients-of-an-adapted-table) | Default compatibility contract for clients of an adapted table | Accepted (by ADR-024) | 2026-11-12 |
| [ADR-021](#adr-021-block-migration-for-tables-using-features-the-indirection-cannot-carry-and-reject-lwt-on-adapted-partitions) | Block migration for tables using features the indirection cannot carry, and reject LWT on adapted partitions | Accepted | 2026-11-12 |
| [ADR-022](#adr-022-add-six-core-hooks-for-adaptive-schema-and-run-its-data-plane-unconditionally) | Add six core hooks for adaptive schema and run its data plane unconditionally | Accepted | 2026-11-12 |
| [ADR-023](#adr-023-host-the-project-as-a-public-github-fork-of-apachecassandra) | Host the project as a public GitHub fork of apache/cassandra | Accepted | 2026-11-12 |
| [ADR-024](#adr-024-owner-decisions-at-the-d1-review) | Owner decisions at the D1 review | Accepted (decision 7 amended by ADR-025) | 2026-11-13 |
| [ADR-025](#adr-025-reverse-migration-is-a-follow-up-not-a-condition-of-d6) | Reverse migration is a follow-up, not a condition of D6 | Accepted | 2026-11-13 |

## ADR-001: Base the fork on a pinned Apache Cassandra trunk commit

- **Date:** 2026-11-12
- **Status:** Accepted (re-confirmed by ADR-004)
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

## ADR-004: Re-confirm the pinned trunk base after verifying CEP-21 and CEP-37 in the code

- **Date:** 2026-11-12
- **Status:** Accepted
- **Deliverable:** D1

**Context.** ADR-001 chose trunk for Transactional Cluster Metadata (CEP-21) and built-in auto-repair (CEP-37) and asked D1 to re-confirm. The D1 review read the pinned tree. TCM is present in `src/java/org/apache/cassandra/tcm/` and is the only metadata path: `Schema.submit` commits an `AlterSchema` transformation through `ClusterMetadataService.commit`. TCM also has an extension point we had not assumed: `Transformation.Kind.CUSTOM`, `CustomTransformation.registerExtension`, and `ExtensionKey`/`ExtensionValue` entries in `ClusterMetadata.extensions`, with `ChangeListener` notification on every node. Auto-repair is present in `src/java/org/apache/cassandra/repair/autorepair/` with a JMX service, cross-node turn-taking, and in-JVM dtests. Two limits were also found: upstream exercises the extension point only in tests, and there is no way to add a replicated system keyspace to an existing TCM cluster.

**Decision.** We keep the base chosen in ADR-001, trunk at commit `2249e37b51af9fd2a72865bbb56c1197e0d0d1ed`. ADR-001 stands; this ADR records the evidence for it.

**Alternatives considered.** Moving to cassandra-5.0 was rejected again: it has neither feature, and the coordination design in ADR-006 and the layout registry in ADR-012 depend on linearizable metadata. Rebasing to a newer trunk commit now was rejected because nothing found requires it and it would invalidate the D0 baseline build.

**Consequences.** The designs in ADR-006, ADR-009 and ADR-012 now depend on TCM and auto-repair internals at this commit, so a future rebase must re-run the ledger and auto-repair tests. The extension point being test-only upstream is carried as risk R1 in the architecture overview.

## ADR-005: Run one in-process MAPE-K loop per node with three dedicated threads

- **Date:** 2026-11-12
- **Status:** Accepted
- **Deliverable:** D2

**Context.** The vision requires continuous monitoring, diagnosis and action on every node. The loop needs cheap access to in-process metric objects and runtime setters, must not disturb request threads, and must be absent when disabled. Many decisions (throttles, pool sizes) are inherently local; a few (topology, schema layout) are cluster-wide.

**Decision.** We run one `AutonomicService` per node inside the Cassandra process. It hosts a Monitor, Analyze, Plan, Execute pipeline over shared knowledge, with typed interfaces (`Sensor`, `Detector`, `Policy`, `Actuator`). It uses exactly three threads created only when enabled: a monitor thread, a control thread for analysis and planning, and a single sequential actuator thread. Node-scoped actions are decided and executed locally without coordination; cluster-scoped actions go through ADR-006.

**Alternatives considered.** An external sidecar or operator process was rejected: it would need JMX or CQL for every read and write, could not register TCM transformations, and adds a deployment unit, though it would isolate failures better. A single central controller (for example on a CMS member) was rejected because local reactions would then depend on network health and on one node, exactly when the cluster is degraded. Running components on shared executors such as `ScheduledExecutors.optionalTasks` was rejected because a slow sensor would delay unrelated core tasks and the layer's cost could not be attributed.

**Consequences.** A bug in the layer runs inside the database process, so per-component failure isolation and the fail-static rules of ADR-008 are mandatory. The single actuator thread serializes node actions for free but means a stuck actuator blocks all others; the layer pauses itself in that case. Several nodes can take the same local action at once, bounded only by per-node envelopes.

## ADR-006: Coordinate cluster-scoped actions through a ledger in TCM using the CUSTOM transformation extension point

- **Date:** 2026-11-12
- **Status:** Accepted
- **Deliverable:** D2

**Context.** Two nodes must not act on the same cluster-wide problem at once, and an action must survive the failure of the node driving it. The code offers several mechanisms. TCM gives a linearizable log; `Kind.CUSTOM` with `CustomTransformation.registerExtension` and `ExtensionKey` lets us add state and transformations without a new `Kind` or serializer changes. Auto-repair coordinates through rows in `system_distributed` and accepts overlap as merely unlikely. `Transformation.execute` is replayed on every node, so it must be deterministic. The commit path cannot always tell the caller whether its transformation was applied. Stock Cassandra cannot deserialize an unknown custom transformation, and `Directory` has no capability flags.

**Decision.** We keep a small `ClusterLedger` as an `ExtensionValue` in `ClusterMetadata`, modified by four idempotent custom transformations (enable, claim, advance, release). A claim is a lease on a lane (`TOPOLOGY`, `SCHEMA_LAYOUT`, `MAINTENANCE`); at most one claim exists per lane. All timestamps and limits are supplied by the proposer as transformation fields. The owner drives the action; other nodes react to phase changes through a `ChangeListener`, so no new messaging verb is added. Serializers are registered unconditionally from the static initializer of `CustomTransformation`. Nothing is written to the log until an operator runs `nodetool autonomic cluster enable`.

**Alternatives considered.** Lightweight transactions on a replicated table were rejected because there is no way to add a replicated system keyspace to an existing TCM cluster and because schema and action state would then not be consistent at one epoch. Auto-repair-style turn-taking over a distributed table was rejected because it is best-effort, which is acceptable for repair but not for topology or layout changes. A dedicated `Transformation.Kind` and a first-class `ClusterMetadata` field were rejected as more invasive (enum, serializer and metadata version changes); they remain the fallback if the extension path proves unreliable. Gossip application state was rejected because it is eventually consistent. A new internode verb for commanding other nodes was rejected because reacting to committed metadata gives the same effect with ordering guarantees.

**Consequences.** Once enabled, the metadata log contains entries stock Cassandra cannot read; enabling is a one-way operator attestation that all nodes run this fork. Lease expiry depends on wall clocks, so a safety margin is required and takeover can be early under large skew. Every cluster actuator must implement `recover` for takeover. The ledger must stay small because it is part of every metadata snapshot. Cluster actions depend on CMS availability; node-scoped actions do not. We rely on an upstream extension point that upstream itself only tests lightly.

## ADR-007: Keep knowledge in three tiers: memory, a node-local keyspace, and the TCM ledger

- **Date:** 2026-11-12
- **Status:** Accepted
- **Deliverable:** D2

**Context.** The working assumption was a `system_autonomic` keyspace holding knowledge and audit. Observations arrive every few seconds per table. Replicated system keyspaces are created once by the TCM `Initialize` transformation, and upstream has an open TODO on how to evolve them, so adding a replicated keyspace to an existing cluster needs new core machinery. Local system keyspaces are a static set built in `Schema.initialize()`; Accord adds one conditionally there.

**Decision.** We store knowledge in three tiers. Observations live only in bounded in-memory ring buffers. Diagnoses, action records, the action audit trail and small durable flags live in a node-local `system_autonomic` keyspace with `LocalStrategy`, always defined so the feature can be enabled at runtime, with a 30-day TTL. Cluster-wide state is limited to the TCM ledger of ADR-006. Virtual tables under `system_views.autonomic_*` expose all three.

**Alternatives considered.** A replicated `system_autonomic` keyspace was rejected for the MVP because of the creation problem above and because audit writes would then depend on cluster availability at the moment the cluster is unhealthy. Adding tables to `system_distributed` was rejected for the same evolution problem. Persisting observations was rejected because the write load would perturb what is being measured and the data is only useful for minutes. Defining the keyspace only when enabled at startup was rejected because it would make enabling require a restart.

**Consequences.** History is per node: `nodetool autonomic history` shows one node, and a lost node loses its audit trail, except for cluster actions, which are also in the metadata log. Two core files change (`Schema.java`, `SchemaConstants.java`). An always-present local keyspace exists on disk even when the feature is off. A cluster-wide history view is deferred and is an open question for the project owner.

## ADR-008: Commit an action only when verification shows improvement, and fail static

- **Date:** 2026-11-12
- **Status:** Accepted
- **Deliverable:** D2

**Context.** An automatic change to a running database can make things worse, and the automation itself can be faulty. The owner requires dry-run, a kill switch, rate and blast-radius limits, and apply, verify, rollback for every action. Runtime setters in this codebase change in-memory values only, so a restart already restores configuration. The existing `db/guardrails` package means something else (client CQL limits).

**Decision.** Every action passes one `SafetyGovernor` and follows one state machine: proposed, approved, applying, verifying, then committed, rolled back, superseded or failed. Intent and undo state are written before side effects. An action commits only if verification shows improvement; neutral or inconclusive results roll back. Each applied action has a dead-man rollback scheduled on the actuator thread, independent of the control thread. If an operator changes a value the layer set, the layer marks the action superseded and does not touch it. The layer fails static: on its own faults it stops approving actions and restores what is in flight, and it pauses itself after repeated rollbacks. Three gates must be open for any change (`enabled`, `dry_run: false`, the actuator's own flag); dry-run is the default. The component is named `SafetyGovernor`, never guardrail.

**Alternatives considered.** Keeping neutral changes was rejected because unproven changes would accumulate and drift the node away from its configured baseline. Per-actuator ad hoc safety logic was rejected because limits must be auditable in one place. Extending `db/guardrails` was rejected because that framework is bound to client state and CQL validation. Persisting committed knob values across restarts was rejected because configuration files must remain the source of truth.

**Consequences.** The layer is conservative: it will undo changes that helped only slightly, and verification windows slow its reaction. Every actuator must implement and test exact undo. Irreversible operations need a special, tightly limited class. Committed knob values are lost at restart, so a `baseline_drift` diagnosis and a `revert` command are required and operators must fold accepted values into configuration.

## ADR-009: Reuse built-in auto-repair instead of building a repair scheduler

- **Date:** 2026-11-12
- **Status:** Accepted
- **Deliverable:** D4

**Context.** The D4 plan included a task to build automatic repair scheduling. The pinned tree already contains CEP-37: a scheduler per repair type, cross-node turn-taking, priority and force-repair controls, per-table parameters, metrics, a JMX service and nodetool commands, wired into daemon startup and bootstrap.

**Decision.** We do not build a repair scheduler. D4 integrates with the existing one: a detector reports overdue repair from auto-repair history, and a cluster-scoped actuator adjusts priority or forces repair for specific hosts through `AutoRepairService` and `AutoRepairUtils`. When auto-repair is disabled in configuration, the layer only reports a diagnosis.

**Alternatives considered.** Building our own scheduler was rejected as duplicate work that would compete with the built-in one for the same repair sessions. Having the layer enable auto-repair by itself was rejected because turning on repair cluster-wide is an operator decision with large resource impact.

**Consequences.** D4 task T4.3 shrinks to an integration task (T4.4 in the refined list). Our behaviour depends on auto-repair's API at this commit, which is recent and may change on rebase. Repair-related healing is unavailable in clusters that do not enable auto-repair.

## ADR-010: Respond to load in-node first and treat scale-out as a manually approved, add-only provider SPI

- **Date:** 2026-11-12
- **Status:** Accepted
- **Deliverable:** D5

**Context.** The vision asks for elastic response to demand spikes. In this codebase, adding a node streams data for the ranges it takes over, which takes tens of minutes to hours and adds load meanwhile; removing a node streams everything out. TCM makes joins linearizable and resumable but not faster. Many resource settings can be changed at runtime through existing setters.

**Decision.** We respond in a fixed order: yield background work, resize request pools, shed load with the native transport rate limiter, and only then propose scale-out. In-node elasticity means moving a defined list of existing runtime knobs inside configured envelopes through the standard verify and rollback loop. Scale-out is a `ScaleOutProvider` SPI whose responsibility ends when a Cassandra process has been started; the node joins by its normal startup. Scale-out adds one node at a time, requires manual approval by default, and is never undone automatically. Automatic scale-in is out of scope.

**Alternatives considered.** Treating scale-out as the first response to a spike was rejected because it makes a short spike worse. Automatic scale-in was rejected for the MVP because a wrong decommission is expensive and slow to reverse. Having the layer drive joins itself (tokens, bootstrap) was rejected because the standard TCM join sequences already do this safely. A built-in Kubernetes provider was rejected because it cannot be tested on the development machine; we recommend reducing it to a documented example.

**Consequences.** Short spikes are handled only by in-node measures and load shedding, which changes client-visible behaviour when the rate limiter engages. Scale-out reacts to sustained trends and needs a longer verification window. Providers must be idempotent on the action id because lease takeover can repeat a request. Capacity can grow automatically but not shrink.

## ADR-011: Limit core changes to an enumerated set of one-statement hooks placed beside the auto-repair hooks

- **Date:** 2026-11-12
- **Status:** Accepted
- **Deliverable:** D2

**Context.** ADR-002 limits core changes to minimal hooks so rebases stay cheap. The review found where hooks are unavoidable: configuration loading, daemon start and drain, the separate startup sequence in the in-JVM dtest `Instance`, the static sets of local keyspaces and virtual tables, the custom transformation registry that must be populated before log replay, the nodetool command list, and two tests that enumerate classes or help output. `SettingsTable` is read-only, so there is no generic hot-reload path.

**Decision.** We allow exactly the hooks listed in section 4.1 of the architecture overview: twelve upstream source files, `cassandra.yaml`, two upstream test files and new help fixtures. Each hook is a single statement or declaration placed next to the equivalent auto-repair or Accord line. Configuration is one nested `autonomic` section; runtime changes go through `AutonomicServiceMBean`; the operator interface is one `nodetool autonomic` command group. A D2 task checks the list of changed core files against the table.

**Alternatives considered.** A zero-hook design using only system properties and reflection was rejected: there is no extension point for local keyspaces, `system_views` tables or early serializer registration. A separate virtual keyspace registered from our own code would save one hook but was rejected to keep the `system_views.autonomic_*` naming operators expect. Using `Kind.CUSTOM` registration at service start instead of in the static initializer was rejected because log replay happens earlier and also on nodes with the feature disabled.

**Consequences.** Any new hook requires a new ADR. Rebase conflicts are confined to known lines. The fork creates a dependency from `tcm` to `autonomic` for serializer registration, so those serializer classes must not depend on configuration or the service.

## ADR-012: Place the adaptive-schema indirection on the coordinator and keep layouts in cluster metadata

- **Date:** 2026-11-12
- **Status:** Accepted (refined by ADR-014 to ADR-022)
- **Deliverable:** D6

**Context.** T1.2 will design adaptive schema in detail, but the framework must fix where it plugs in. Schema changes are TCM transformations, so anything stored in `ClusterMetadata` is consistent with the schema at an epoch. Replicas, streaming, repair and compaction operate on physical tables. The CQL layer offers an existing replaceable `QueryHandler` and statement classes where partition keys are known.

**Decision.** We propose that layouts (logical table to physical tables plus a routing rule) are stored in `ClusterMetadata` under their own extension key; that physical tables are ordinary tables created through the normal schema path; that the logical-to-physical indirection runs on the coordinator at the CQL statement layer, before `StorageProxy`; and that a migration is a cluster-scoped action in the `SCHEMA_LAYOUT` lane of ADR-006, with one metadata commit per phase. T1.2 chooses the exact rewrite site and answers the open questions in section 9.3 of the overview, and then accepts or supersedes this ADR.

**Alternatives considered.** Indirection inside the storage engine on replicas was rejected at this level because it would touch read, write, compaction, streaming and repair paths and make rebases expensive. Using materialized views as the mechanism was rejected because views do not replace the base table's hot partition and have their own consistency limits. Client-side or proxy rewriting was rejected because the vision requires unmodified clients and the database to adapt itself.

**Consequences.** Coordinators carry the rewrite cost and must agree on the layout phase by epoch; coordinators with the feature disabled are a correctness problem T1.2 must solve. Features that bypass the CQL layer need explicit handling. D6 depends on the ledger task in D2.

## ADR-013: Sensors pull from existing metric objects and add no instrumentation to request paths

- **Date:** 2026-11-12
- **Status:** Accepted
- **Deliverable:** D2

**Context.** Monitoring needs latency, queue, compaction, disk, GC and per-partition access signals. The codebase already maintains these as in-process objects (`TableMetrics`, `ThreadPoolMetrics`, `ClientRequestsMetricsHolder`, `CompactionMetrics`) and provides windowed top-K partition samplers fed from the read and write paths. Some existing APIs reset shared state (`GCInspector.getAndResetStats`), and each sampler supports one window at a time, shared with `nodetool profileload`. `TopPartitionTracker` is updated only during repair validation.

**Decision.** Sensors poll existing metric objects on the monitor thread at a low cadence and add no code to request, compaction or messaging paths. Hot-partition detection uses the existing samplers in short periodic windows on sufficiently busy tables and yields when a sampler is already active. Sensors never call APIs that reset shared counters; GC data comes from the JVM management beans.

**Alternatives considered.** Adding counters or callbacks to the read and write paths was rejected because it changes hot code, risks performance regressions and enlarges the rebase surface. A continuously running partition frequency sketch was rejected for the MVP for the same reason, though it would remove the sampler contention. Scraping JMX or virtual tables from inside the process was rejected as indirect when the objects are directly reachable.

**Consequences.** Signals have the granularity and decay behaviour of the existing metrics, so reactions take at least a few polling intervals and verification must use rates over our own samples where histograms decay slowly. Operators can occasionally collide with the layer's sampling window. Hot partitions on tables below the sampling rate threshold are not detected.

## ADR-014: Relieve a hot wide partition by hash-splitting that partition alone into a companion table

- **Date:** 2026-11-12
- **Status:** Accepted
- **Deliverable:** D6

**Context.** The owner requires that a hot spot triggers a schema change that mitigates it while old clients keep working. Hot spots come in several shapes. A layout change can relieve a wide partition accessed row by row, an oversized partition, and a time series whose newest rows take all writes. It cannot relieve a small partition with a very high request rate, token skew, or coordinator load. The migration runs on a system that is already under load, so its cost matters. Materialized views are disabled by default in the base (`Config.materialized_views_enabled`) and do not replace the hot base partition.

**Decision.** The first transformation is `HASH_SPLIT`: one logical partition is moved into a companion table in the same keyspace whose partition key is the logical partition key plus a bucket column, and its rows are spread over N buckets by a fixed hash of the clustering key. All other partitions stay in the original table. The layout records the bucket function and a bounded explicit list of split partition keys. For shapes a layout change cannot relieve, the system diagnoses and proposes no schema action.

**Alternatives considered.** Re-bucketing the whole table was rejected for the first version because the backfill is the whole table, every partition read becomes a fan-out, and range scans lose logical token order. Splitting by clustering ranges was rejected as the first transformation because it needs the data distribution, leaves the newest range hot, and its last bucket grows without bound; it remains the planned answer for slice-heavy partitions. Salting the partition key was rejected because it turns every read into a fan-out without helping a small hot key. A vertical split by column group addresses a different problem. Materialized views were rejected as in ADR-012.

**Consequences.** Point reads and writes stay single-partition operations; slices, full-partition reads and range deletes on a split partition cost N sub-operations permanently. Partitions dominated by such operations must not be split, so a shape profile is required before proposing. The registry carries partition keys and needs hard size bounds. In a cluster with no more nodes than the replication factor the split bounds partition size but moves no load.

## ADR-015: Route adapted partitions at the StorageProxy boundary on mutations and read commands

- **Date:** 2026-11-12
- **Status:** Accepted
- **Deliverable:** D6

**Context.** ADR-012 left the interception site open between a wrapping `QueryHandler` and hooks in the CQL statement classes. Reading the code showed that all unconditional CQL writes reach `StorageProxy.mutateWithTriggers`, conditional writes reach `StorageProxy.cas`, coordinator single-partition reads reach `StorageProxy.read`, and range reads reach `StorageProxy.getRangeSlice`; the partition denylist is checked at exactly these places. Pagers, aggregation, ordering, the read-before-write of list operations, and batch handling all sit above them. `PagingState` contains only the partition key and the last clustering. Mutations and read results carry timestamps, TTLs and tombstones explicitly.

**Decision.** We route below CQL: one statement in each of `mutateWithTriggers`, `read`, `getRangeSlice`, `cas` and `truncateBlocking` calls into `org.apache.cassandra.autonomic.layout`. Writes are rewritten as `PartitionUpdate` objects; reads are fanned out as read commands on the companion table, merged in clustering order, and re-labelled with the logical table's metadata. Statements, result metadata, prepared statements and paging states remain logical and are not invalidated by phase changes. The hooks return after one registry lookup when no layout exists.

**Alternatives considered.** A wrapping `QueryHandler` needs no core edit but sees unbound statements, would have to re-implement paging and aggregation around rewritten CQL, is a startup property, and conflicts with any other custom handler. Hooks in `SelectStatement` and `ModificationStatement` would sit above the pagers and miss the internal read of list operations and the batch path. Rewriting CQL text at any level would have to re-derive timestamps and TTLs that are already resolved in the mutation. Indirection inside the storage engine was rejected in ADR-012.

**Consequences.** Five statements are added to the busiest file in the coordinator path, which is an exception to ADR-013 and extends ADR-011 (see ADR-022). Paths that do not pass through `StorageProxy` see the physical layout: node-local queries, sstable tools and bulk load; bulk load into the original table for a split key is a known gap. The call chain from the pagers to `StorageProxy.read` must be confirmed by the implementing task.

## ADR-016: Agree on migration phases by all-node epoch acknowledgement and validate-after-execute on coordinators

- **Date:** 2026-11-12
- **Status:** Accepted
- **Deliverable:** D6

**Context.** Coordinators switch behaviour when a phase is committed to the metadata log, but each learns of it at its own time, and a request can outlive the phase under which it started. Replicas reject reads whose table epoch is stale, but for writes `StorageProxy` catches up and retries the same mutation objects, which then pass, so the existing schema fence does not stop a stale routing decision. Arming that fence would also need a real table change per phase, which evicts every prepared statement on the table. `ProgressBarrier` waits for a quorum of replicas of affected ranges, not for every coordinator. `TCM_CURRENT_EPOCH_REQ` returns a node's current epoch.

**Decision.** Phases are grouped into four coordinator classes ordered so that neighbouring classes are safe to mix. The owner commits a transition only after every node in the directory that is not `LEFT` has reported an epoch at or past the current phase's epoch, with no quorum fallback and no timeout that lets it proceed. Every routed operation records the layout version before it starts and compares it before returning; on a change it re-executes under the new metadata, and after three attempts fails with a timeout. Routing and backfill run on every node of the fork whether or not the autonomic layer is enabled.

**Alternatives considered.** Replica-side fencing by table epoch was rejected for the reasons above. A new replica-side layout check in the mutation and read verb handlers was rejected because a stale replica cannot fence a stale coordinator at consistency level `ONE` and it adds hooks to the storage path. Coordinator leases with drain waits were rejected because a paused coordinator can act after its lease. Reusing `ProgressBarrier` was rejected because it does not cover non-replica coordinators. Per-node acknowledgement commits in the log were rejected as too many log entries per phase.

**Consequences.** Safety does not depend on clocks. Progress depends on every node being reachable: one unreachable node stalls a migration in a safe phase until it returns or is removed. Operations overlapping a phase change can be retried internally and occasionally fail. The argument assumes a node's metadata epoch never decreases across restart, which must be tested. Conditional writes cannot be re-executed, which is one reason they are rejected on adapted partitions (ADR-021).

## ADR-017: Order dual writes so the authoritative copy is written last, and use the batchlog only where atomicity across mutations is needed

- **Date:** 2026-11-12
- **Status:** Accepted
- **Deliverable:** D6

**Context.** While both copies of a partition exist, every write must reach both, and a coordinator can fail between the two. Options in the code are the batchlog (`StorageProxy.mutateAtomically`), Accord, or two plain writes. Reads use one copy per phase. A write that reaches the read copy but not the other would be lost when reads switch.

**Decision.** For single statements and unlogged batches the coordinator first writes the non-authoritative copy at the client's consistency level and only then the authoritative copy. Logged batches are sent as one logged batch containing both copies, keeping the batchlog's all-or-nothing behaviour. Any write that spans several buckets (range and partition deletes) goes through the batchlog. Consistency level `ANY` is raised to `ONE` for these writes.

**Alternatives considered.** Putting every dual write through the batchlog gives eventual agreement of the copies but allows the read copy to show a write before the other copy has it, and adds batchlog load to the hottest key. Accord transactions were rejected because Accord is disabled by default and the table would have to become an Accord table. Accepting divergence and repairing by a later backfill pass was rejected because no pass can be the last one while writes continue.

**Consequences.** Writes to an adapted partition take two sequential round trips during the dual-write phases. A coordinator failure between the writes leaves an unacknowledged write in the non-authoritative copy; it becomes visible at cutover or is discarded on abort. For logged batches the ordering is not guaranteed, so an unacknowledged batch issued around cutover can be visible, then invisible until the batchlog replays. Multi-row writes within one adapted partition lose single-partition isolation.

## ADR-018: Backfill from every base replica's local unfiltered data, and verify by a coordinator-level comparison

- **Date:** 2026-11-12
- **Status:** Accepted
- **Deliverable:** D6

**Context.** Backfill must preserve write timestamps, TTLs and deletions, must not resurrect deleted data, and must tolerate clients that wrote at any consistency level. A coordinator read returns only live data, so copying it loses tombstones: a late write with a timestamp older than an uncopied tombstone would be live in the new copy and dead in the old. The materialized-view builder reads local partitions unfiltered and emits derived mutations; `ThrottledUnfilteredIterator` bounds batch sizes.

**Decision.** In the backfill phase every replica of the base partition reads its own local data for the key unfiltered, in resumable throttled chunks, re-labels cells without changing values, timestamps, TTLs or deletion times, and writes them to the companion table at consistency level `ALL`. Each replica reports completion through a small metadata commit. If the base replica set changes, the backfill generation restarts. All copying must end within half of `gc_grace_seconds` after dual writes began. Before cutover the owner compares the two copies row by row and cell by cell at `ALL`, re-checking rows that differ, and aborts on a persistent mismatch. Before the base copy is deleted, one more backfill generation picks up late-arriving base-only mutations.

**Alternatives considered.** A coordinator copy of live data at `ALL` is simpler and cheaper but loses tombstones. A single designated replica after a blocking read at `ALL` is cheaper but depends on read repair being enabled and correct; it is kept as an optimisation. Streaming sstables cannot change the partition key. A replica-side forwarder in the write path, as views use, would also catch late mutations but changes the storage engine.

**Consequences.** The partition is rewritten once per base replica, so backfill costs about RF times the partition in writes before compaction. A replica of either table being down stalls backfill until a deadline aborts the migration. Tables with a small `gc_grace_seconds` are ineligible. Verification cannot see tombstones; their equivalence rests on the unfiltered copy and on dual writes.

## ADR-019: Keep layouts in their own bounded metadata extension, create companion tables by ordinary DDL, and couple client DDL in AlterSchema

- **Date:** 2026-11-12
- **Status:** Accepted
- **Deliverable:** D6

**Context.** ADR-012 proposed layouts in `ClusterMetadata` under their own key. A `SchemaTransformation` is serialized as CQL text and re-parsed on every node, so table creation by the layer must be a CQL statement. `TableParams.extensions` exists, but no branch in `TableAttributes` was found that applies it from CQL (whether the keyword is silently ignored is unverified). Client DDL on a table with a layout can make the two physical tables disagree; `AlterSchema.execute` already couples schema changes to another per-table migration state (Accord's consensus migration).

**Decision.** The registry is one extension value with its own format version and hard bounds (8 layouts, 8 keys per layout, 1 KiB per key). Phase changes are custom transformations that update the registry and the ledger claim in one commit. The companion table lives in the same keyspace as the logical table, is created with `CREATE TABLE` through `Schema.submit` before the layout is registered and dropped after it is removed. One statement in `AlterSchema.execute` calls a pure function that drops the companion table with its logical table, rejects DDL on companion tables, rejects changes that would make an adapted table ineligible, and rejects (later mirrors) column changes.

**Alternatives considered.** Storing the layout in the logical table's `extensions` param would make it part of the schema but needs direct construction of schema objects in a custom transformation, changes the logical table's metadata epoch at every phase (evicting prepared statements), and exposes an opaque blob to clients. Creating the companion table inside the custom transformation would make creation atomic with registration but duplicates logic of `AlterSchema`. A separate keyspace for companion tables would hide them from `DESCRIBE KEYSPACE` but separates their replication settings, snapshots and repair scope from the data they belong to.

**Consequences.** The companion table is visible to clients as an ordinary table. Between its creation and the layout's registration, and between removal and drop, an unused table can exist and must be collected. Backups must include both tables and cannot be restored meaningfully without the registry. A sixth core hook is needed, in the TCM package, and it runs on every node during log replay, so it must be deterministic and free of configuration.

## ADR-020: Default compatibility contract for clients of an adapted table

- **Date:** 2026-11-12
- **Status:** Accepted (by ADR-024)
- **Deliverable:** D6

**Context.** The owner requires clients of the old schema to be satisfied "at least eventually" and has not yet quantified this. The design needs a precise default so that tests and eligibility rules have something to hold to.

**Decision.** We propose: always preserved are the logical CQL surface and metadata, read results, durability of acknowledged writes at their consistency level, per-row consistency-level semantics, paging states, timestamps and TTLs, and authorization on the logical table. Allowed to degrade, within bounds stated in the design, are write latency during dual-write phases, latency and availability of reads and deletes that span buckets, isolation of multi-row writes within one partition, the timing of visibility of writes whose outcome the client never learned, and DDL on adapted tables. Not preserved are lightweight transactions and serial reads on an adapted partition, `TRUNCATE` during a migration, and node-local views of the data; a companion table becomes visible in the keyspace.

**Alternatives considered.** A strict contract with no withdrawn capability and no latency degradation would restrict the feature to tables with only point operations and would require LWT support across a partition move. A looser contract allowing temporary rejection of unsupported read shapes was rejected because results must not depend on the migration.

**Consequences.** This ADR stays Proposed until the owner accepts or tightens it. The design document lists, per possible tightening, what must change. Eligibility rules exist to keep clients away from the non-preserved items, but cannot rule out a client starting to use LWT on an adapted partition later.

## ADR-021: Block migration for tables using features the indirection cannot carry, and reject LWT on adapted partitions

- **Date:** 2026-11-12
- **Status:** Accepted
- **Deliverable:** D6

**Context.** Several features act below or beside the coordinator routing: Paxos and Accord per physical partition, counters through a separate write path, secondary indexes and views on the replica, CDC on the commit log of the logical table, triggers, static columns shared by all rows of a partition, and data masking defined on the logical table. Supporting each during and after a move is separate work.

**Decision.** A table is eligible only if it has none of: LWT activity observed, Accord transactional mode, counters, any index, any materialized view, CDC, triggers, static columns, masked columns, compact storage, transient replication, no clustering column, or a `gc_grace_seconds` too small for the backfill bound. The structural part is checked by the policy, by the actuator, and again inside the registering transformation so it is atomic with the schema; client DDL that would break eligibility later is rejected. Conditional writes and serial reads on an adapted partition are rejected from the first phase on.

**Alternatives considered.** Migrating indexes and views along with the data was rejected as far beyond the first version. Supporting LWT by running Paxos on the bucket partition was rejected for now because moving the linearization point needs a quiescing protocol and a conditional write cannot be re-executed after a phase change. Refusing only at execution time instead of up front was rejected because it would surprise clients mid-migration.

**Consequences.** Many real tables are not eligible. LWT use can only be observed, not ruled out for the future, so a client may meet rejections on an adapted partition; this is the main point where the design is not transparent. Each blocked feature is a candidate follow-up.

## ADR-022: Add six core hooks for adaptive schema and run its data plane unconditionally

- **Date:** 2026-11-12
- **Status:** Accepted
- **Deliverable:** D6

**Context.** ADR-011 limits core changes to an enumerated list and requires a new ADR for any addition. ADR-013 keeps the layer out of request paths. The overview states that a node with the layer disabled runs no listeners and no phase work. Adaptive schema needs routing on every coordinator and backfill on every replica once a layout exists, because correctness of client requests then depends on them.

**Decision.** We add hooks H16 to H21: one statement each in `StorageProxy.mutateWithTriggers`, `read`, `getRangeSlice`, `cas` and `truncateBlocking`, placed beside the partition denylist checks, and one in `AlterSchema.execute`. The routing code, the epoch watermark handling and the replica backfill worker run on every node of the fork regardless of `autonomic.enabled`, dry-run, pause or kill; they do nothing while the registry is empty. Only proposing and driving a migration requires an enabled node. A migration has a commit point after which it is not rolled back by timers, the kill switch or restart recovery; before it, every such event aborts loss-free.

**Alternatives considered.** Gating routing on `autonomic.enabled` was rejected because a node with the flag off would serve wrong results for adapted partitions. Tracking per-node capability in the directory was rejected because the directory has no such field and the operator's cluster-enable attestation already asserts that all nodes run the fork. Keeping the verify-then-rollback contract of ADR-008 unchanged was rejected because after the commit point the old copy is stale.

**Consequences.** The framework design must change in the places listed in the adaptive-schema document: service start, ledger helpers for combined commits, claim preconditions, the verify contract for cluster actuators, kill and recovery semantics, and the hook budget check. Turning the layer off does not turn off an existing layout. `StorageProxy` becomes a rebase hot spot for the fork.

## ADR-023: Host the project as a public GitHub fork of apache/cassandra

- **Date:** 2026-11-12
- **Status:** Accepted
- **Deliverable:** D0

**Context.** The owner asked for the project to live in a public GitHub repository. The local checkout is a shallow clone of trunk at the pinned commit, and a standalone repository would need either the full upstream history pushed to it or a new root commit that discards the lineage.

**Decision.** The repository is a GitHub fork of `apache/cassandra` named `autonomic-cassandra`, public, with `main` as its default branch. `main` descends directly from the pinned upstream commit. Commits use the owner's GitHub noreply address, because the account blocks pushes that would publish a private email address.

**Alternatives considered.** A standalone repository with a squashed root commit was rejected because it loses upstream ancestry and makes every later rebase a manual diff. A standalone repository with full history was rejected because it duplicates what the fork network already stores. A private repository was not chosen because the owner asked for a public one.

**Consequences.** The repository appears in the public fork network of Apache Cassandra, so the README states that the project is not affiliated with the ASF. Rebasing onto a newer upstream commit is an ordinary fetch from `upstream`. The fork's `trunk` branch is an untouched mirror of upstream at fork time and is not used for our work.

## ADR-024: Owner decisions at the D1 review

- **Date:** 2026-11-13
- **Status:** Accepted (decision 7 amended by ADR-025)
- **Deliverable:** D1

**Context.** The D1 specs left thirteen questions and three scope calls for the project owner (listed in `docs/deliverables/D1-architecture.md`). ADR-020, the compatibility contract for clients of an adapted table, was Proposed until they were answered.

**Decision.** The owner decided as follows.

| # | Question | Decision |
|---|----------|----------|
| 1 | LWT on adapted partitions | Rejected on adapted partitions in the MVP. Add an operator setting that excludes named tables from ever being proposed |
| 2 | Isolation of multi-row writes on an adapted partition | Weaker isolation accepted |
| 3 | Writes whose outcome the client never learned | Proposed behaviour accepted |
| 4 | Visible companion table | Accepted |
| 5 | `ANY` treated as `ONE` on adapted partitions | Accepted |
| 6 | Scope of "changes the schema on the fly" | Split only the hot partition now. Whole-table re-partitioning is a recorded follow-up for the case where many partitions of one table are hot |
| 7 | No loss-free rollback after `NEW_ONLY` | **Not accepted as proposed.** If a split does not help there must be a way back; it may be slow. Reverse migration is therefore part of D6, not a later follow-up |
| 8 | Approval before the commit point | A second manual approval by default (`commit_approval: manual`), configurable |
| 9 | Stalling while a node is unreachable | Accepted |
| 10 | Six more core hooks (H16 to H21) | Accepted |
| 11 | DDL mirroring (T6.14) | Stays a follow-up; column DDL on an adapted table is rejected in the MVP |
| 12 | Approval for cluster-scoped actions | Manual by default, configurable |
| 13 | One-way `cluster enable` | Accepted |
| - | Automatic scale-in | Out of scope |
| - | Audit history | Node-local for now |
| - | Kubernetes stub | Replaced by a documented example in the operator guide |

ADR-020 is accepted with one change: its statement that `NEW_ONLY` has no undo holds only until the reverse migration of this ADR exists; D6 is not complete without it.

**Alternatives considered.** For decision 7 the proposed alternative was to ship the forward migration alone and build the reverse later; the owner requires the way back to exist, accepting that it takes longer than the pre-commit abort. For decision 6 whole-table re-partitioning was considered as the primary mechanism and deferred because its cost is proportional to the table, on a system that is already hot.

**Consequences.** D6 gains task T6.17 (reverse migration), which must solve open issue O3 of the adaptive-schema design: the base partition tombstone written by cleanup shadows reverse-backfilled cells with older timestamps. The stage-B verdict `split_not_effective` becomes the trigger for proposing a reverse migration, under the same manual approval. D6 also gains an exclusion list in the `layout:` configuration (decision 1). T5.5 drops the Kubernetes stub. The forward path can be built and tested before T6.17, but the D6 tag requires it.

## ADR-025: Reverse migration is a follow-up, not a condition of D6

- **Date:** 2026-11-13
- **Status:** Accepted
- **Deliverable:** D6

**Context.** ADR-024 recorded the owner's answer to question 7 as requiring reverse migration inside D6. The owner clarified the same day: a way back from a completed split should exist eventually; it does not have to ship with the first version.

**Decision.** The forward partition split may ship with a commit point (`NEW_ONLY`) that has no undo, as ADR-020 originally proposed. Reverse migration (T6.17) is the first follow-up after D6 and is not required for the D6 tag. The second manual approval before the commit point (ADR-024, decision 8) stays the default.

**Alternatives considered.** Keeping T6.17 inside D6, as ADR-024 had it, was dropped because the owner does not require it and it contains an unsolved design problem (open issue O3) that would delay the demonstrable forward path.

**Consequences.** Until T6.17 exists, a split that verification finds ineffective stays split and raises `layout.split_not_effective`; the operator guide must say so. Nothing in D6 may make a later reverse migration harder: in particular the choice of how forward cleanup removes the base copy must be made with O3 in mind, and T6.9 must record that choice.

## ADR-026: The `repair_overdue` detector reads auto-repair history in a background task

- **Date:** 2026-11-14
- **Status:** Accepted (owner chose option B on 2026-11-14)
- **Deliverable:** D4

**Context.** `repair_overdue` (T4.4) needs the last successful repair time of each host, which exists only in `system_distributed` through `AutoRepairUtils.getAutoRepairHistory`. That read is a replicated read (`LOCAL_QUORUM`, or `ONE` without `NetworkTopologyStrategy`), so it contacts other replicas and can block for up to `read_request_timeout` (5 s by default). Overview 3.4 says a detector is a pure function of knowledge with no I/O, and 3.5 forbids disk or network I/O on the monitor thread, where sensors run. The first implementation read the table on the control thread, cached for 30 s. The owner rejected that: a slow replica would stall every detector and the kill switch's path, most likely when repairs are overdue and nodes are unhealthy.

**Decision.** The read runs as a background task on an existing core executor, `ScheduledExecutors.optionalTasks`. The detector's `analyze` only reads the latest completed snapshot, an immutable record with the time it was read. `Detector` gained default `start()` and `stop()` methods; `AutonomicService` starts the built-in detectors after building them and stops them when the layer stops or is disabled, so nothing is scheduled while `autonomic.enabled` is false. At most one read is in flight, and the next is scheduled `refresh_ms` (default 30 s) after the previous one completes. A failed read keeps the old snapshot, which counts as no measurement once older than `max_signal_age_ms`. Each snapshot counts once towards `for_evaluations`.

**Alternatives considered.** Keeping the read on the control thread was rejected for the reason above. A sensor on the monitor thread was rejected because it breaks the same rule on a thread whose stall delays every other sensor. A dedicated autonomic thread was rejected because the layer is specified as three threads. Reading the history in the executor was rejected because detection would then depend on an action having been proposed. Dropping the detector was rejected because the repair-priority action would have no trigger.

**Consequences.** No component of the layer waits on a replicated read. A diagnosis from this detector opens after `for_evaluations` distinct reads (at least that many times `refresh_ms`, 2 minutes with defaults), not after that many control ticks. The same pattern is the sanctioned way for any future detector that needs a slow external read; it does not need a new ADR.

## ADR-027: A routed operation is validated against the routing state of its keys, not the layout version

- **Date:** 2026-11-16
- **Status:** Accepted (orchestrator decision during T6.3; confirmed by the owner at the D6 review, 2026-11-18)
- **Deliverable:** D6

**Context.** Rule C1 (adaptive-schema 4.3) has a coordinator compare `TableLayout.version` before and after a routed operation and re-execute on a change, failing with a timeout after three attempts. As built in T6.1, every registry change bumps that version, including `ReportBackfill` and a backfill generation restart. With replication factor 3 a key in `BACKFILL` receives three reports, so a write in flight could be re-executed three times and time out although its phase never changed.

**Decision.** The comparison is per routed key: the layout id, the physical table id, the key's incarnation and its phase (`LayoutWrites.Routed.unchangedIn`; reads share it). `phaseEpoch` is not compared, because a generation restart sets it without changing the phase. A key that is absent from the registry when the operation captures its metadata is not routed and is not validated.

**Alternatives considered.** Not bumping the version in `ReportBackfill` was rejected because the version would then no longer mean "any change", which other readers of the registry rely on. Comparing coordinator classes instead of phases was rejected as weaker than the argument of 4.3 needs to be shown to hold.

**Consequences.** The safety argument of 4.3 is unchanged: it needs only that the coordinator saw the same phase before and after. `VERIFY -> BACKFILL -> VERIFY` within one operation is not detected, and does not need to be, since the class stays C1. A layout id must never be reused with the same physical table; T6.9 must generate fresh ids.

## ADR-028: The owner holds `ANNOUNCED` until operations that started before registration have ended

- **Date:** 2026-11-16
- **Status:** Accepted, amended by ADR-035 (orchestrator decision during T6.3; amended by the owner at the D6 review, 2026-11-18)
- **Deliverable:** D6

**Context.** An operation on a key that is not yet in the registry takes the stock path and is never validated (adaptive-schema 3.3, `nothingRouted`). Rules O1 and C1 therefore say nothing about it. Such an operation that is still in flight when the key passes `DUAL_WRITE` and `BACKFILL` can reach the base table after the backfill pass and still be acknowledged, and readers of the overflow copy then miss an acknowledged write. The design's only wait in `ANNOUNCED` is `cas_drain` (15 s), documented as being for Paxos rounds. Three cases were identified during T6.3: an ordinary write delayed inside its coordinator; a `TRUNCATE`, whose timeout (`truncate_request_timeout`, 60 s) exceeds `cas_drain`; and a write at `ANY` acknowledged on a hint alone, which is on no base replica when backfill runs.

**Decision.** After the all-node acknowledgement of `ANNOUNCED`, the owner waits at least the largest of `cas_drain` and the write, counter-write, CAS-contention, range and truncate request timeouts, plus a margin, before `DUAL_WRITE` (T6.9). The wait starts when every node has acknowledged the `ANNOUNCED` epoch, since from then on every new operation on the key is routed and validated. The TLA+ model (T6.15) carries this as the switchable rule `EnforceDrain`. The hint-only `ANY` case is not closed by the wait: it is covered only if the hint is delivered before the reconcile pass of `CLEANUP`, and is documented as a limitation for tables written at `ANY`.

**Alternatives considered.** Validating every operation on a table that has a layout, whether or not its key is registered, was rejected because it puts the check on the request path of every key of the table for the life of the layout. Making `ANY`-written tables ineligible was rejected because the consistency level is per request and cannot be known from the schema.

**Consequences.** A migration takes at least about a minute longer with default timeouts. The bound relies on request timeouts being enforced from the request's start time; a coordinator thread stalled for longer than the wait between capturing metadata and sending is not covered, and the operator guide must say so. Contract item for T6.16: writes at `ANY` to a key that is about to be split can be lost if their hint outlives the migration.

## ADR-029: The `gc_grace_seconds` eligibility threshold is a constant

- **Date:** 2026-11-16
- **Status:** Accepted (owner, D6 review, 2026-11-18; the manual split path also checks the configured `straggler_window`, as the policy does)
- **Deliverable:** D6

**Context.** A table is ineligible for a split when its `gc_grace_seconds` is too small for invariant I9 (no backfill or reconcile write after `DUAL_WRITE` start plus half of `gc_grace`). The design derives the threshold from the configured straggler window. `Eligibility.structural` runs inside `RegisterKey`, a cluster metadata transformation, which must give the same result on every node and on replay.

**Decision.** The threshold is the constant `Eligibility.MIN_GC_GRACE_SECONDS` = 28800 (twice the default 3 h straggler window plus 1 h), not a function of configuration.

**Alternatives considered.** Reading `layout.straggler_window` inside the transformation was rejected because configuration can differ between nodes and over time, which would make replay non-deterministic. Carrying the window as a field of `RegisterKey` is possible and is the option to take if the owner wants the check to follow configuration.

**Consequences.** An operator who raises `straggler_window` above 3 h is not protected by the eligibility check; the owner-side deadline of I9 (T6.9) must still be enforced from the configured value at run time.

## ADR-030: Verification reports overflow-only rows separately and they do not fail a migration

- **Date:** 2026-11-16
- **Status:** Accepted (orchestrator decision during T6.8; confirmed by the owner at the D6 review, 2026-11-18)
- **Deliverable:** D6

**Context.** In class C1 a routed write goes to the overflow table first and to the base table second (adaptive-schema 3.3). If the coordinator fails or the base write times out in between, the overflow copy holds a row the base copy lacks. The write was never acknowledged, so this is legal (4.11). Section 4.6 verifies by comparing the two copies for equality and treats any difference as a mismatch; a backfill generation cannot repair this difference because backfill copies base to overflow only. As specified, one timed-out client write during `DUAL_WRITE`, `BACKFILL` or `VERIFY` would fail verification twice and abort the migration.

**Decision.** `LayoutVerifier` reports three kinds of mismatch separately: `CONTENT_DIFFERS`, `MISSING_FROM_OVERFLOW` and `MISSING_FROM_BASE`. The verifier decides nothing. `PartitionSplitActuator` (T6.9) treats the first two as failures, as 4.6 says, and tolerates `MISSING_FROM_BASE`: the row is an unacknowledged write that becomes visible at cutover, which 4.11 already allows.

**Alternatives considered.** Writing base first in C1 was rejected because it breaks the adjacency argument of 4.2 (a C2 reader could miss an acknowledged write). Deleting overflow-only rows before cutover was rejected because the delete would race with a retry of the same write.

**Consequences.** A cell in a `MISSING_FROM_BASE` row that differs only in content from its base counterpart is still `CONTENT_DIFFERS` and fails. The tolerated count is recorded in the action's verdict so that an unexpectedly large number is visible. The TLA+ model's verification guard must be read as `base \subseteq overflow`, not equality.

## ADR-031: The registry records when dual write started, and replicas refuse backfill writes past the tombstone deadline

- **Date:** 2026-11-17
- **Status:** Accepted (owner decision, 2026-11-17, on a hazard reported by T6.7)
- **Deliverable:** D6

**Context.** Invariant I9 (adaptive-schema 4.10) forbids any backfill or reconcile write for a key later than the start of `DUAL_WRITE` plus `gc_grace_seconds / 2`: after that, a tombstone that exists only in the overflow table may have been purged, and an older base row copied afterwards would come back to life. As designed only the action's owner enforced this, through `backfill_deadline` (4.5). After the commit point there is no abort, so a reconcile pass that starts or stalls past the deadline while the owner is absent could copy such a row. A replica could not refuse on its own because the registry did not record when dual write started.

**Decision.** `KeyState` records the wall-clock time at which the key entered `DUAL_WRITE` for the current incarnation; later transitions keep it. Before each chunk write, `BackfillTask` on every replica compares its clock with that time plus half of the logical table's `gc_grace_seconds` and, once past it, writes nothing more for the key and commits no `ReportBackfill`. The owner's `backfill_deadline` stays and must fire first, so replicas reach their own limit only when the owner has failed to act.

**Alternatives considered.** Leaving enforcement to the owner's deadline handling in T6.9 and documenting the gap: rejected by the owner, because the guarantee then depends on one node being present at the right time. Stopping tombstone purging on the overflow table for the life of a migration: rejected as a change to compaction on a core path.

**Consequences.** One more field in the registry's wire format. The limit is judged by each replica's own clock, so it is as good as clock synchronisation between nodes, like `gc_grace_seconds` itself. A key that passes the limit in `CLEANUP` cannot complete its reconcile pass: the action ends in a terminal state that needs an operator, the key stays routed to the overflow copy, and the base partition tombstone is not written (T6.9).

## ADR-032: SERIAL reads of an adapted key are rejected from ANNOUNCED on

- **Date:** 2026-11-17
- **Status:** Accepted (orchestrator decision during T6.12; confirmed by the owner at the D6 review, 2026-11-18)
- **Deliverable:** D6

**Context.** The design contradicted itself. Section 3.6 and limitation N1 (the operator-facing contract) reject lightweight transactions and `SERIAL` reads on an adapted key from `ANNOUNCED` on; section 3.4 rejected `SERIAL` only on a command replaced by a fan-out, which happens in classes C2 and C3. The code followed 3.4, and the T6.12 compatibility suite confirmed that `SERIAL` reads were accepted in C0 and C1.

**Decision.** `SERIAL` and `LOCAL_SERIAL` reads of a key in any class other than "absent" are rejected with the same `InvalidRequestException` as lightweight transactions. Section 3.4 is corrected to say so.

**Alternatives considered.** Keeping 3.4's narrower rule and amending N1 instead. Rejected: a `SERIAL` read runs Paxos on the base partition and can complete a proposal left in flight from before registration, which writes the base copy only and bypasses the dual write of C1 (inferred, not demonstrated). The narrower rule buys nothing a client of an LWT-free table needs, since such tables are the only ones proposed.

**Consequences.** A client issuing `SERIAL` reads on a key that has just been announced gets an error where it previously succeeded; this is the documented contract. T6.12's expectations for C0 and C1 change accordingly.

## ADR-033: The base partition tombstone is written after FinishKey, and FinishKey requires a complete reconcile

- **Date:** 2026-11-17
- **Status:** Accepted (orchestrator decision on the T6.9 build and its review; confirmed by the owner at the D6 review, 2026-11-18); the timing of the tombstone is amended by ADR-036 (accepted 2026-11-19)
- **Deliverable:** D6

**Context.** Section 4.1 lists the base partition tombstone before `FinishKey` in `CLEANUP`. With that order, a natural replica that has not reported (a node that became a replica during `CLEANUP`) runs a reconcile pass after the tombstone, reads it from base and copies a partition deletion into every bucket, deleting acknowledged overflow data. The T6.9 review found a second path to the same loss even with the reordering: a replica that joins between the reconcile report and `FinishKey` and whose pass stalls.

**Decision.** The owner writes the base partition tombstone at `ALL` only after `FinishKey` has been acknowledged by every node, when no replica runs a pass for the key any more. Before committing `FinishKey` the owner re-checks that the reconcile generation is complete and that placement has not changed, and starts a new reconcile generation otherwise. `FinishKey` itself is rejected unless every natural replica of the key's token has reported the current generation. A kill between `FinishKey` and the tombstone is recovered by takeover or restart of the claim, which writes the tombstone again.

**Consequences.** Base rows of a split key remain live until the tombstone is written; they are unreachable, because every read of the key in `STEADY` goes to the overflow copy and range scans exclude the key from base. Reverse migration (T6.17) must not assume the base partition is empty, and must wait `gc_grace_seconds` after the tombstone and force a purge before copying back (open issue O3).

## ADR-034: Column mirroring maps cells of dropped columns away and refuses cells of columns the target has not seen

- **Date:** 2026-11-18
- **Status:** Accepted (orchestrator decision on T6.14; the design leaves router tolerance open; confirmed by the owner at the D6 review, 2026-11-18)
- **Deliverable:** D6

**Context.** Section 4.8 says T6.14 mirrors `ADD` and `DROP` of regular columns onto the overflow table in the same epoch, and the T6.14 row asks for "router tolerance for the epoch in which columns change". Both tables change in one `AlterSchema`, so a single `ClusterMetadata` always has the same regular columns on both. Mismatches can only arise between data or metadata taken at different epochs on one node: a row read under the older epoch that is mapped with the newer epoch's table, or the reverse.

**Decision.** `RowMapper` re-labels a cell to the target table's column of the same name. If the target has no such column but records it as dropped, the cell is skipped (and the column omitted from the update's column set): the cells of a dropped column are invisible to every read of the target. If the target has neither the column nor a dropped record (an addition the target's metadata has not seen), the mapper still throws `IllegalArgumentException`, and the caller (write path, backfill pass) treats it as a failure of that attempt and retries under newer metadata; it never drops the cell. A column present in the target and absent in the source needs nothing. Mirroring itself is in `LayoutSchemaCoupling`: the overflow table gains and loses exactly the regular columns of the logical table, and records a dropped column with the logical table's drop time. Renames and type alterations of regular columns, and any other column change, stay rejected.

**Alternatives considered.** Skipping every unknown cell: rejected, a write of a new column could be lost without an error. Failing on dropped columns too: rejected, a backfill chunk read just before a `DROP` would fail until the pass restarted, for data that is gone anyway.

**Consequences.** A write that races an `ADD` on a coordinator with older metadata fails or is retried like any write to an unknown column in stock Cassandra. During a backfill pass, a chunk read before a `DROP` is mapped without the dropped column.

## ADR-035: Before `DUAL_WRITE`, the owner waits until no node holds hints written before the split for the key's replicas

- **Date:** 2026-11-18
- **Status:** Accepted (owner decision at the D6 review, 2026-11-18; amends ADR-028)
- **Deliverable:** D6

**Context.** ADR-028 left one way to lose an acknowledged write. A write at `ANY` to a key that is not yet registered can be acknowledged on a hint alone, when the coordinator reaches no replica. That hint is then the only copy, and hint replay (`HintVerbHandler`) applies it straight to the base table, bypassing the layout router. If it is still undelivered after the reconcile pass of `CLEANUP`, it lands in base only and is never read again: reads of the key go to the overflow copy, and the base partition tombstone (ADR-033) shadows it. The drain wait of ADR-028 covers requests in flight, not hints already stored. Writes at `ANY` to a key that is already routed are not at risk, since they are acknowledged as `ONE` (`LayoutWrites`).

**Decision.** Between the all-node acknowledgement of `ANNOUNCED` and `DUAL_WRITE`, the owner also waits until no node holds a hint, created before that acknowledgement, for any natural replica of the key's token. All nodes are up at that point (O1), so such hints can be delivered. The wait is bounded. If it expires, the migration aborts, which is still before the commit point, with a diagnosis naming the nodes that still hold hints. The check is per host and by hint age, not per key, because reading hint files to find one key costs too much.

**Alternatives considered.** Documenting the gap as a limitation for tables written at `ANY` (the ADR-028 position): rejected by the owner, since the fix is cheap and removes a limitation from the contract. Rejecting `ANY` on adapted keys: rejected, because it does not close the gap: the dangerous hint predates registration.

**Consequences.** A cluster with a stuck hint backlog toward one of the key's replicas cannot split that key until the backlog drains, or until the operator truncates the hints. The limitation on `ANY` writes is removed from the operator guide. A coordinator thread stalled for longer than the ADR-028 wait is still not covered.

## ADR-036: Reverse migration keeps the base residue for a window instead of writing the base tombstone at cleanup (O3)

- **Date:** 2026-11-18
- **Status:** Accepted (owner decision, 2026-11-19: option (d) and every recommendation of adaptive-schema 11.11, see Owner answers below); amends the timing of ADR-033
- **Deliverable:** D6 follow-up (T6.17)

**Context.** T6.17 must return a split key to the base table (adaptive-schema section 11). After a split, forward cleanup writes a base partition tombstone at time `Tt` once `FinishKey` is acknowledged (ADR-033). The overflow copy holds cells with their original timestamps, many older than `Tt`. Copied back into the same base partition, they stay invisible until the tombstone is purged, `gc_grace_seconds` plus a compaction later (open issue O3). Without the tombstone, the stale base rows (the residue) come back after an unsplit if a delete made while split, which exists only in the overflow table, has meanwhile been purged there. Section 11.1 and 11.2 work both hazards with timestamps. The reverse must keep every acknowledged write with its original timestamp, keep deletes made while split deleted, and lose nothing during the reverse.

**Decision.** Option (d) of section 11.2.

- **Forward cleanup no longer writes the base partition tombstone in `STEADY`.** This reverses ADR-033's timing; its ordering rule stays: no tombstone while a reconcile pass can run, and `FinishKey` requires a complete reconcile. The registry records when the key entered class C3 (`frozenSince`) and the base state `RESIDUE`. Range-scan exclusion and C3 routing keep the residue unreachable, as today.
- **Fast mode.** A reverse migration whose copy and reconcile writes are all issued before `frozenSince + gc_grace_seconds / 2` merges into the residue. Every delete that exists only in overflow is younger than `frozenSince` and cannot have been purged yet, so last-write-wins gives the overflow state. The limit is enforced by the owner and by every replica, as ADR-031 does.
- **Reclaim.** When fast mode can no longer finish in time and no reverse owns the key, a `reclaim` continuation of the split action writes the base tombstone at `ALL`, as `deleteBase` does today, and records `Tt` in the registry.
- **Slow mode** (option (a)): once a tombstone exists, a reverse starts only after every base replica has passed the tombstone's local deletion time plus `gc_grace_seconds`, run `forceCompactionForKey` for the key, and found its partition empty in a local unfiltered probe. Residue newer than the tombstone stops the reverse for an operator decision. Keys split under the current forward cleanup use this mode.
- **Reverse protocol** (section 11.4): `BeginUnsplit` (C3), reverse dual write, backfill and verify (C2, overflow authoritative and written last), `CUTBACK` (C1), `BASE_ONLY` (C0, the reverse commit point), reverse cleanup with a reconcile pass, `RemoveKey`, then deletion of the bucket partitions or drop of the overflow table, only after every node acknowledged the removal. The reverse reuses the coordinator classes, rules O1 and C1, the validation of ADR-027, the replica deadline of ADR-031 with new limits, and the verification of ADR-030 with roles swapped. The drain wait (ADR-028) and the hint wait (ADR-035) are not needed, because every operation on the key is routed and validated. Abort before the commit point returns to `STEADY`. New invariants I8-R and I16 to I22, and an amended I10.

**Alternatives considered.**
- (a) alone (wait out `gc_grace_seconds` after the tombstone, then force a purge): correct with a residue probe, but no unsplit for at least 10 days after cleanup by default. The main trigger, `layout.split_not_effective`, fires minutes after the commit. Kept as the slow mode.
- (b) No base tombstone ever, with a purge or overwrite of stale rows at reverse time: resurrects a row whose overflow delete was purged. Purging the stale rows needs a tombstone (back to (a)). Subtracting them atom by atom can destroy acknowledged cells through row deletions and cannot tell a purged delete from an unacknowledged straggler. The residue's space is never reclaimed.
- (c1) A generation in the base partition key: changes the logical table, breaking contract P1 and evicting prepared statements. Rejected.
- (c2) Re-stamping copied cells above the tombstone, or copying deletion info explicitly into a fresh target: changes original timestamps (P6, I7), or is impossible because the logical table id is fixed. Rejected.
- (c3) Re-split to one bucket: keeps LWT withdrawn and DDL restricted, so it is not an unsplit. Rejected.

**Consequences.**
- The base residue of a split key (one partition times RF) stays on disk for up to about half of `gc_grace_seconds` after the commit (5 days by default), where today it goes at the end of `CLEANUP`.
- The registry format goes to version 2, with new `KeyState` fields, legacy decoding and an upgrade gate. The reverse report set is bounded by `MAX_HINT_REPORTS` rather than 32, at no growth of the worst-case size.
- An unacknowledged straggler that reached the residue can become visible after a fast unsplit, so contract D6's bound needs a clause.
- Tables at the minimum `gc_grace_seconds` (28800) get practically no fast window unless a reverse may start from `NEW_ONLY` or `CLEANUP`.
- The TLA+ model must gain timestamps, deletes and purging (section 11.7). The negative variants without the window, without the purge, without the probe, and with an early bucket deletion must each fail.
- Hazards recorded for the forward path as built:
  - `Tt` is not in the registry, and adoption can move it later than `STEADY`.
  - Residue newer than `Tt` survives cleanup and purge.
  - Keys stuck at `RECONCILE_PAST_DEADLINE` have no way out.
  - A single timed-out update to an existing row aborts verification.
- Subtasks T6.17a to T6.17l and nine owner questions are in sections 11.10 and 11.11. T6.17b, the forward change, waits for this ADR to be accepted.

**Owner answers (2026-11-19, the questions of adaptive-schema 11.11).**
1. Option (d) and the change to forward cleanup: accepted.
2. A reverse may start from `NEW_ONLY` or `CLEANUP` when stage B says not effective: accepted.
3. Residue that survives the slow-mode purge (timestamps above `Tt`): held for an operator decision; no automatic second tombstone.
4. Reverse report set bounded by `MAX_HINT_REPORTS` (1024): accepted.
5. A second manual approval before the reverse commit point (`BASE_ONLY`): required, as for `NEW_ONLY`.
6. Keys stuck at `RECONCILE_PAST_DEADLINE`: the unsplit action may write the base tombstone itself once the replicas' limit plus `straggler_window` has passed, which then allows a slow unsplit.
7. `forceCompactionForKey` in slow mode: accepted; the operator may choose to wait for natural compaction with only the probe.
8. `NEWER_IN_TARGET` cell differences tolerated (and counted) in verification, in both directions.
9. Contract D6 amended as in 11.8.

## ADR-037: Unsplit actuator decisions beyond the T6.17 design note

- **Date:** 2026-11-19
- **Status:** Accepted (orchestrator decision on T6.17c and T6.17f; within ADR-036)
- **Deliverable:** D6 follow-up (T6.17)

**Context.** Building the replica copy (T6.17c) and the unsplit actuator (T6.17f) exposed four points that section 11 did not settle.

**Decision.**
1. `REVERSE_BACKFILL` and `REVERSE_CLEANUP` each start their own reconcile generation on entry, and completion counts only reports of a generation started in that phase. Otherwise slow-mode purge reports in the `BeginUnsplit` generation would be counted as backfill.
2. For a key stuck at `RECONCILE_PAST_DEADLINE` (owner answer 6), `BeginUnsplit` carries an optional `Tt` (flags byte; values without it decode as before), accepted only for `CLEANUP` with `RESIDUE` in slow mode. The action records `Tt` in its claim first, writes the tombstone, then begins the slow unsplit.
3. Each replica's `BasePurge` reason is published in gossip application state `X1` (no other user in the codebase), and `SplitOutcomeDetectors.UnsplitResidue` raises `layout.unsplit_residue` from it. The remedy text names a repair of the key's range for the repair-state reasons (T6.17d finding, 11.5).
4. `purge_timeout` counts from the later of the all-node ack and `Tt + gc_grace_seconds`.

**Consequences.** `X1` is reserved for the autonomic layer. Slow-mode unsplits of keys whose sstables are in mixed repair states wait for an operator repair, then time out with a diagnosis that says so.
