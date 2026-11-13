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
| [ADR-020](#adr-020-default-compatibility-contract-for-clients-of-an-adapted-table) | Default compatibility contract for clients of an adapted table | Proposed | 2026-11-12 |
| [ADR-021](#adr-021-block-migration-for-tables-using-features-the-indirection-cannot-carry-and-reject-lwt-on-adapted-partitions) | Block migration for tables using features the indirection cannot carry, and reject LWT on adapted partitions | Accepted | 2026-11-12 |
| [ADR-022](#adr-022-add-six-core-hooks-for-adaptive-schema-and-run-its-data-plane-unconditionally) | Add six core hooks for adaptive schema and run its data plane unconditionally | Accepted | 2026-11-12 |
| [ADR-023](#adr-023-host-the-project-as-a-public-github-fork-of-apachecassandra) | Host the project as a public GitHub fork of apache/cassandra | Accepted | 2026-11-12 |

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
- **Status:** Proposed
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
