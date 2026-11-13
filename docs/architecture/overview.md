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

# Autonomic Cassandra: Architecture Overview

Status: D1 / T1.1. Base: Apache Cassandra trunk at `2249e37b` (ADR-001, re-confirmed by ADR-004).
Decisions referenced as ADR-NNN are in [`docs/DECISIONS.md`](../DECISIONS.md).

**Amended by T1.2.** Section 9.1 of [`adaptive-schema.md`](adaptive-schema.md) lists framework changes FC1 to FC10 that the adaptive-schema design requires (service start, ledger helpers, claim preconditions, the verify contract for cluster actuators, kill and recovery past a commit point, hook budget, configuration, nodetool). Where they conflict with this document, FC1 to FC10 take precedence. The implementers of T2.1, T2.6, T2.7, T2.8, T2.10 and T3.2 must read them.

Conventions used in this document:

- All source paths are relative to the repository root and exist in the pinned checkout. `src/java/org/apache/cassandra/` is abbreviated `o.a.c/` only inside prose, never in the hook tables.
- "Verified" means the file was read at the pinned commit. Anything not read is marked **unverified**.
- "Core" means any file that exists upstream. "Ours" means files under `src/java/org/apache/cassandra/autonomic/`.

## 1. Goals, non-goals, MVP boundaries

### 1.1 Goals

1. **Self-diagnosis.** Each node continuously turns raw metrics into named diagnoses with evidence an operator can read.
2. **Self-healing.** A diagnosis can trigger a bounded corrective action. Every action is applied, verified against metrics over an observation window, and rolled back if it did not help.
3. **Elasticity.** A node adapts its own resource allocation to load within seconds. The cluster can request more capacity through a provider interface when in-node measures are exhausted.
4. **Adaptive schema.** A hot spot can trigger a change of physical layout while clients of the logical schema keep working. This document defines only the framework it plugs into; the mechanism is T1.2.
5. **Safety before capability.** The layer is off by default, can be stopped at any time, cannot act faster or wider than configured limits, and fails static: when the layer itself is unhealthy, it stops acting and the database behaves like stock Cassandra.
6. **Cheap rebases.** Core is touched only at the hook points listed in section 4.

### 1.2 Non-goals

- Replacing operators. The layer automates bounded, reversible responses. Irreversible or expensive operations default to manual approval.
- Learning-based or predictive control. Detectors and policies are rules with thresholds and hysteresis. The interfaces do not preclude other implementations.
- A general workflow engine. Multi-step actions exist only for scale-out and schema layout migration.
- Changing replica placement algorithms, the storage engine, or the wire protocol.
- Mixed clusters with stock Cassandra nodes once cluster-scoped actions are enabled (section 6.5).
- Managing clients, drivers, or anything outside the Cassandra process, except through the scale-out provider SPI.

### 1.3 MVP scope boundaries

| Area | In MVP | Out of MVP |
|------|--------|------------|
| Control loop | One loop per node, three threads, rule-based | Hierarchical or centralized controllers |
| Knowledge | In-memory observations; node-local durable diagnoses and action audit; small cluster ledger in TCM | Replicated history, long-term metric storage |
| Node actions | Bounded numeric knobs listed in section 8.1, targeted garbage-collecting compaction | Changing table schema options, compaction strategy changes |
| Cluster actions | One in flight per lane (section 6.2); scale-out by one node; auto-repair prioritization | Parallel cluster actions, scale-in automation, token moves |
| Approval | Automatic for node-scoped actions, manual by default for cluster-scoped | Policy languages, RBAC on approvals |
| Verification | Metric comparison before/after with fixed windows | Statistical significance testing, canarying across nodes |
| Tests | Unit tests and 1 to 3 node in-JVM dtests (ADR-003) | Performance benchmarks, upgrade tests, simulator coverage |

## 2. What the base already provides (verified)

These findings change the plan and are the basis of several ADRs.

| Capability | Where | What it means for us |
|------------|-------|----------------------|
| Transactional Cluster Metadata (CEP-21) | `src/java/org/apache/cassandra/tcm/` (`ClusterMetadata.java`, `ClusterMetadataService.java`, `Transformation.java`, `log/LocalLog.java`) | Present and is the only metadata path. Schema changes go through it: `Schema.submit(SchemaTransformation)` in `src/java/org/apache/cassandra/schema/Schema.java` commits an `AlterSchema` transformation. |
| TCM extension point | `Transformation.Kind.CUSTOM` (id 30), `src/java/org/apache/cassandra/tcm/transformations/CustomTransformation.java` (`registerExtension(String, AsymmetricMetadataSerializer)`), `src/java/org/apache/cassandra/tcm/extensions/ExtensionKey.java`, `ExtensionValue.java`, and `ClusterMetadata.Transformer.with(ExtensionKey, ExtensionValue)` | We can add linearizable cluster state and new transformations without adding a `Kind` or changing the `ClusterMetadata` serializer. Upstream uses it only in tests (`PokeString`, `PokeInt`). |
| Metadata change notification | `src/java/org/apache/cassandra/tcm/listeners/ChangeListener.java`, registered with `LocalLog.addListener(ChangeListener)` | Every node can react to committed ledger changes. This replaces a new messaging verb. |
| Auto-repair (CEP-37) | `src/java/org/apache/cassandra/repair/autorepair/` (`AutoRepair.java`, `AutoRepairUtils.java`, `AutoRepairConfig.java`), `src/java/org/apache/cassandra/service/AutoRepairService.java`, config field `Config.auto_repair` | A complete scheduler with cross-node turn-taking (`AutoRepairUtils.myTurnToRunRepair`), priorities, force-repair, and a JMX surface. It is off unless configured. We do not build a repair scheduler (ADR-009). |
| Partition access sampling | `src/java/org/apache/cassandra/metrics/Sampler.java`, `FrequencySampler.java`, `TableMetrics.samplers`, `ColumnFamilyStore.beginLocalSampling` / `finishLocalSampling` | Windowed top-K per table for `READS`, `WRITES`, `WRITE_SIZE`, `CAS_CONTENTIONS`, `LOCAL_READ_TIME`, `READ_ROW_COUNT`, `READ_TOMBSTONE_COUNT`, `READ_SSTABLE_COUNT`. Only one sampling window per sampler at a time: `FrequencySampler.beginSampling` throws `RuntimeException("Sampling already in progress")`. |
| Top partitions by size and tombstones | `src/java/org/apache/cassandra/metrics/TopPartitionTracker.java`, `ColumnFamilyStore.topPartitions` | Updated only during repair validation (`src/java/org/apache/cassandra/repair/ValidationManager.java`). Useful, but stale unless repair runs. Not an access-frequency signal. |
| Partition denylist | `src/java/org/apache/cassandra/schema/PartitionDenylist.java`, `StorageProxy.denylistKey`, table `system_distributed.partition_denylist` | An existing blunt hot-partition mitigation (reject access). Not used by the MVP; listed as a possible last-resort actuator. |
| Runtime knobs | `src/java/org/apache/cassandra/service/StorageService.java`, `CacheService.java`, `src/java/org/apache/cassandra/transport/ClientResourceLimits.java` | Section 8.1 lists the setters. All are in-memory only: a restart restores the `cassandra.yaml` value. |
| Virtual tables | `src/java/org/apache/cassandra/db/virtual/SystemViewsKeyspace.java` | Table sets are added with `.addAll(X.getAll(VIRTUAL_VIEWS))` (see `LocalRepairTables`, `AccordVirtualTables`). |
| nodetool | `src/java/org/apache/cassandra/tools/nodetool/NodetoolCommand.java` | picocli. Command groups with subcommands exist (`CMSAdmin.java`). Help output is fixture-tested (`test/unit/org/apache/cassandra/tools/nodetool/NodetoolHelpCommandsOutputTest.java`, fixtures in `test/resources/nodetool/help/`). |
| Accord | `src/java/org/apache/cassandra/service/accord/`, `Config.accord` (`AccordConfig.enabled = false`) | Present, disabled by default. It does not affect D2 to D5. It matters to D6 (section 9). |

Things expected but **not** found:

- No hot-reload path through `system_views.settings`. `src/java/org/apache/cassandra/db/virtual/SettingsTable.java` extends `AbstractVirtualTable` and is read-only at this commit. Runtime changes go through JMX MBeans.
- No mechanism to add a replicated system keyspace to an existing TCM cluster. Replicated system keyspaces are created once by `src/java/org/apache/cassandra/tcm/transformations/cms/Initialize.java`; `SystemDistributedKeyspace.java` carries the comment `// TODO: TCM - how do we evolve these tables?`. This drives ADR-007.
- No feature or capability flags per node in `ClusterMetadata.directory`. Only Cassandra version and metadata serialization version are tracked. This drives section 6.5.
- The existing package `o.a.c/db/guardrails` is about limiting client CQL usage. It is unrelated to our safety limits. To avoid confusion, this design never calls its own limits "guardrails" in code; the component is `SafetyGovernor`.

## 3. Component model

### 3.1 Overview

```
                 +---------------------- AutonomicService (per node) ----------------------+
                 |                                                                         |
 metrics, JMX -->|  Sensors --> ObservationStore --> Detectors --> Correlator --> Policies |
 gossip, TCM     |  (Monitor)    (Knowledge, RAM)    (Analyze)                   (Plan)    |
                 |                     |                  |                        |       |
                 |                     v                  v                        v       |
                 |               KnowledgeStore  <---- diagnoses ------------- Plan/Action |
                 |          (system_autonomic, local)                              |       |
                 |                                                                 v       |
                 |                               SafetyGovernor --> ActionExecutor (Execute)
                 |                                      ^               |   apply/verify/rollback
                 |                                      |               v                  |
                 |                          ClusterCoordinator      Actuators --> core setters
                 +------------------------------|------------------------------------------+
                                                v
                              TCM: ClusterLedger extension in ClusterMetadata
```

One `AutonomicService` singleton exists per node (ADR-005). It owns all components and three executors. Nothing is created until `start()` is called with `autonomic.enabled: true`.

### 3.2 Package layout

```
src/java/org/apache/cassandra/autonomic/
  AutonomicService.java, AutonomicServiceMBean.java, AutonomicConfig.java
  monitor/    Sensor, Observation, SignalId, Subject, ObservationSink, sensors/*
  knowledge/  ObservationStore, KnowledgeView, KnowledgeStore, AutonomicKeyspace,
              AutonomicVirtualTables
  analyze/    Detector, Diagnosis, Severity, Correlator, detectors/*
  plan/       Policy, Plan, Action, ActionScope, VerifySpec, policies/*
  execute/    Actuator, ActionContext, Applied, Verdict, ActionState, ActionRecord,
              ActionOutcome, ActionExecutor, MetricVerifier, actuators/*
  safety/     SafetyGovernor, Decision, Cooldowns, RateWindow
  coord/      ClusterLedger, ClusterLedgerValue, Claim, AutonomicTransformations,
              ClusterCoordinator
  elastic/    ScaleOutProvider, ScaleRequest, ProvisionTicket, ProvisionStatus
  layout/     (D6, defined by T1.2)
```

### 3.3 Key types

All types below are immutable value classes unless noted. Times are epoch milliseconds from `org.apache.cassandra.utils.Clock.Global` so tests can substitute a clock.

```java
public final class SignalId            // e.g. "table.read_latency_p99_micros"
{ public final String name; }

public final class Subject             // what an observation or action is about
{
    public enum Kind { NODE, KEYSPACE, TABLE, PARTITION, THREAD_POOL, PEER, CLUSTER }
    public final Kind kind;
    public final String keyspace;      // nullable
    public final String table;         // nullable
    public final String detail;        // partition key as CQL literal, pool name, peer host id; nullable
    public String asString();          // stable form "TABLE:ks.tbl", "PARTITION:ks.tbl:<key>", used as storage key
}

public final class Observation
{
    public final SignalId signal;
    public final Subject subject;
    public final long atMillis;
    public final double value;
}

public final class Diagnosis
{
    public final UUID id;              // timeuuid, stable while the condition persists
    public final String type;          // e.g. "hot_partition", "compaction_backlog"
    public final Subject subject;
    public final Severity severity;    // INFO, WARN, CRITICAL
    public final double confidence;    // 0..1
    public final long firstSeenMillis, lastSeenMillis;
    public final String summary;       // one sentence for operators
    public final Map<String, String> evidence;  // signal name -> formatted value/threshold
    public final UUID rootCause;       // nullable; set by Correlator
}

public final class Action
{
    public final UUID id;              // timeuuid
    public final String actuator;      // Actuator.name()
    public final ActionScope scope;    // NODE or CLUSTER
    public final Subject target;
    public final Map<String, String> params;
    public final String dedupeKey;     // actuator + ":" + target.asString(); cooldown and conflict key
    public final VerifySpec verify;
}

public final class VerifySpec
{
    public final SignalId goal;        // must improve
    public final Subject goalSubject;
    public final boolean lowerIsBetter;
    public final double minImprovement;        // relative, e.g. 0.10
    public final List<Guard> guards;           // signals that must not regress by more than tolerance
    public final long baselineWindowMillis;    // window before apply used as baseline
    public final long observationWindowMillis; // window after apply used for the verdict
}

public final class Plan
{
    public final UUID id;
    public final UUID diagnosisId;
    public final String policy;
    public final List<Action> actions;  // MVP: exactly one
    public final String rationale;
}

public enum Verdict { IMPROVED, NEUTRAL, REGRESSED, INCONCLUSIVE }

public final class Applied             // returned by apply; persisted before verification starts
{ public final Map<String, String> undo; }  // everything rollback needs, e.g. {"previous":"64"}

public final class ActionOutcome
{
    public final ActionState finalState;
    public final Verdict verdict;      // nullable if never verified
    public final String detail;
    public final Map<String, Double> baseline, result;
    public final long startedMillis, endedMillis;
}
```

### 3.4 Component interfaces

```java
/** Monitor. Pull-based; called on the monitor thread. Must not block on I/O or locks held by request paths. */
public interface Sensor
{
    String name();
    Set<SignalId> signals();
    long minIntervalMillis();                         // the loop never calls collect more often than this
    void collect(ObservationSink sink, long nowMillis) throws Exception;
    default void start() {}                           // e.g. open a sampling window
    default void stop() {}
}
public interface ObservationSink { void accept(Observation o); }

/** Read-only view of knowledge, safe to call from any autonomic thread. */
public interface KnowledgeView
{
    OptionalDouble latest(SignalId signal, Subject subject);
    Series series(SignalId signal, Subject subject, long lookbackMillis);   // Series: long[] times, double[] values
    Collection<Subject> subjects(SignalId signal);
    Collection<Diagnosis> activeDiagnoses();
    Collection<ActionRecord> recentActions(long lookbackMillis);            // local actions
    ClusterLedger ledger();                                                 // from ClusterMetadata.current(); EMPTY if absent
    long nowMillis();
}

/** Analyze. Pure function of knowledge. No side effects, no I/O. */
public interface Detector
{
    String name();
    Set<SignalId> requires();
    List<Diagnosis> analyze(KnowledgeView knowledge);   // returns conditions currently true, after hysteresis
}

/** Analyze, second stage. Links symptoms to causes and suppresses derived diagnoses. */
public interface Correlator
{ List<Diagnosis> correlate(List<Diagnosis> raw, KnowledgeView knowledge); }

/** Plan. Pure function. Returns empty if no action is appropriate. */
public interface Policy
{
    String name();
    Set<String> handles();                               // diagnosis types
    Optional<Plan> plan(Diagnosis diagnosis, KnowledgeView knowledge);
}

/** Execute. The verify/rollback contract is section 7.3. Called on the actuator thread only. */
public interface Actuator
{
    String name();
    ActionScope scope();
    boolean reversible();
    /** Cheap precondition check; no side effects. */
    Optional<String> validate(Action action, KnowledgeView knowledge);
    /** Makes the change. Must return what rollback needs. Must be idempotent for a given action id. */
    Applied apply(Action action, ActionContext ctx) throws Exception;
    /** Default: MetricVerifier.compare(action.verify, baseline, knowledge). */
    Verdict verify(Action action, Applied applied, Map<String, Double> baseline, KnowledgeView knowledge);
    /** Restores the state captured in applied. Must be idempotent. Must not throw for "already restored". */
    void rollback(Action action, Applied applied, ActionContext ctx) throws Exception;
    /** True if the live value is no longer what apply set, i.e. an operator changed it. */
    boolean overridden(Action action, Applied applied);
}

/** Safety. Single decision point; see section 7. */
public interface SafetyGovernor
{
    Decision evaluate(Action action, KnowledgeView knowledge);  // APPROVE, REJECT(reason), HOLD_FOR_APPROVAL
    void recordOutcome(Action action, ActionOutcome outcome);   // feeds cooldowns, rate windows, quarantine
}
```

Registration of sensors, detectors, policies and actuators is static in `AutonomicService` for the MVP (a list built in code, filtered by per-component `enabled` flags in configuration). A plug-in mechanism through `FBUtilities.construct(className, ...)` is only used for `ScaleOutProvider`.

### 3.5 Threading model

| Executor | Created by | Cadence | Runs | Must never |
|----------|-----------|---------|------|-----------|
| `AutonomicMonitor` (1 thread) | `executorFactory().scheduled(false, "AutonomicMonitor")` (`src/java/org/apache/cassandra/concurrent/ExecutorFactory.java`) | `monitor_interval` (default 5s); each sensor at most every `minIntervalMillis()` | `Sensor.collect`, writes to in-memory `ObservationStore` | Do disk or network I/O, take locks used by request paths, call anything that resets shared state (for example `GCInspector.getAndResetStats`) |
| `AutonomicControl` (1 thread) | same factory, `"AutonomicControl"` | `analyze_interval` (default 15s) | detectors, correlator, policies, `SafetyGovernor.evaluate`, verdict evaluation, non-critical persistence (diagnoses) | Call actuators, commit to TCM, wait on futures |
| `AutonomicActuator` (1 thread, sequential) | same factory, `"AutonomicActuator"` | event-driven plus scheduled verify deadlines | `Actuator.apply` / `rollback`, write-ahead of action records, TCM commits with a deadline | Block longer than `action_step_timeout` (default 30s) in one task; long operations are split into steps that reschedule themselves |

Consequences:

- Node-scoped actions are serialized by the single actuator thread. No additional locking is needed.
- The control thread hands actions to the actuator thread through a bounded queue (size 16). If the queue is full the action is rejected with reason `EXECUTOR_BUSY`.
- TCM commits can block for a long time: the default path retries up to `cms_commit_timeout` (`ClusterMetadataService.commit` javadoc). `ClusterCoordinator` always uses the overload `commit(transform, onSuccess, onFailure, deadlineLimitNanos)` with `action_step_timeout` as the deadline.
- Request threads are never touched. Sensors read metric objects that the request path already maintains.
- In the in-JVM dtest framework each instance has its own class loader, so each node gets its own `AutonomicService` and executors.

### 3.6 Lifecycle

```
DISABLED --start()--> STARTING --armed--> RUNNING <--resume()/pause()--> PAUSED
    ^                                        |                              |
    +---------------- shutdownBlocking() ----+------------------------------+
```

- `AutonomicService.instance.start()` is called from daemon setup (section 4). If `autonomic.enabled` is false it registers the MBean and returns: no executors, no listeners, no sensors.
- STARTING: executors are created; recovery runs (section 7.6); the persisted pause flag is read. The loop is **armed** only when `ClusterMetadata.current().myNodeState() == NodeState.JOINED` and `startup_grace` (default 5m) has elapsed since start, so decaying histograms have warmed up. Until armed, sensors run but detectors do not.
- PAUSED: sensors and detectors run, diagnoses are recorded, no new action is approved. In-flight actions finish verification and may roll back.
- `kill()`: PAUSED plus immediate rollback of all in-flight node-scoped actions and release of any cluster claim owned by this node at its next safe step. The pause flag is persisted so a restart does not re-arm the layer.
- `shutdownBlocking()` is called from `StorageService.drain` before `AutoRepair.instance.shutdownBlocking()`, for the same reason given in the comment there: later stages stop accepting writes to system tables.
- `autonomic.enabled` can be flipped at runtime through the MBean; this calls `start()` or `shutdownBlocking()`.

## 4. Hook points into core

Policy (ADR-011): each hook is one statement where possible, placed next to the equivalent auto-repair or Accord hook so that rebase conflicts are obvious and local.

### 4.1 Files modified

| # | File | Location | Modification | Size | Needed by |
|---|------|----------|--------------|------|-----------|
| H1 | `src/java/org/apache/cassandra/config/Config.java` | next to `public volatile AutoRepairConfig auto_repair` | add `public final AutonomicConfig autonomic = new AutonomicConfig();` | 1 line + import | T2.1 |
| H2 | `src/java/org/apache/cassandra/config/DatabaseDescriptor.java` | next to `getAutoRepairConfig()` | add `getAutonomicConfig()` | 4 lines | T2.1 |
| H3 | `src/java/org/apache/cassandra/service/CassandraDaemon.java` | `setup()`, after `StorageService.instance.doAutoRepairSetup();` | add `AutonomicService.instance.start();` | 1 line | T2.1 |
| H4 | `src/java/org/apache/cassandra/service/StorageService.java` | `drain(boolean)`, before `AutoRepair.instance.shutdownBlocking();` | add `AutonomicService.instance.shutdownBlocking();` | 1 line | T2.1 |
| H5 | `test/distributed/org/apache/cassandra/distributed/impl/Instance.java` | after `StorageService.instance.doAutoRepairSetup();` and in shutdown next to `AutoRepair.instance.shutdownBlocking(...)` | same two calls as H3/H4. Required because in-JVM dtests do not run `CassandraDaemon.setup()`; `Instance.java` has its own startup sequence | 2 lines | T2.1 |
| H6 | `src/java/org/apache/cassandra/schema/SchemaConstants.java` | constants and `LOCAL_SYSTEM_KEYSPACE_NAMES` | add `AUTONOMIC_KEYSPACE_NAME = "system_autonomic"` and include it in the set | 2 lines | T2.3 |
| H7 | `src/java/org/apache/cassandra/schema/Schema.java` | `initialize()`, where `AccordKeyspace.metadata()` is conditionally added | add `initialLocal = initialLocal.with(AutonomicKeyspace.metadata());` (unconditional, section 5.2) | 1 line | T2.3 |
| H8 | `src/java/org/apache/cassandra/db/virtual/SystemViewsKeyspace.java` | builder chain | add `.addAll(AutonomicVirtualTables.getAll(VIRTUAL_VIEWS))` | 1 line | T2.4 |
| H9 | `src/java/org/apache/cassandra/tcm/transformations/CustomTransformation.java` | static initializer, after the `PokeString`/`PokeInt`/`ClearInt` registrations | add `AutonomicTransformations.register();` which calls `registerExtension(name, serializer)` for each of our transformations | 1 line | T2.8 |
| H10 | `src/java/org/apache/cassandra/tools/nodetool/NodetoolCommand.java` | `subcommands` list | add `Autonomic.class` | 1 line | T2.7 |
| H11 | `src/java/org/apache/cassandra/tools/NodeProbe.java` | next to `autoRepairProxy` | add `AutonomicServiceMBean autonomicProxy`, its initialization, and an accessor | about 8 lines | T2.7 |
| H12 | `src/java/org/apache/cassandra/metrics/CassandraMetricsRegistry.java` | metric group list, after `.add(AutoRepairMetrics.TYPE_NAME)` | add `.add(AutonomicMetrics.TYPE_NAME)` | 1 line | T2.9 |
| H13 | `conf/cassandra.yaml` | after the commented `auto_repair:` block | add a commented `autonomic:` block (section 10.1) | comments only | T2.1 |
| H14 | `test/unit/org/apache/cassandra/config/DatabaseDescriptorRefTest.java` | allowed class list | add `AutonomicConfig` and its nested classes (the test fails when `DatabaseDescriptor` loads an unlisted class) | few lines | T2.1 |
| H15 | `test/resources/nodetool/help/` | new fixture files | generated with `test/unit/org/apache/cassandra/tools/nodetool/NodetoolHelpGenerator.java`; the top-level `help` fixture changes too | new files + 1 changed | T2.7 |

Why H9 must be a static registration and unconditional: the metadata log is replayed in `Startup.initialize(...)`, which `CassandraDaemon.setup()` calls long before H3. `CustomTransformation.Serializer.deserialize` looks up the child serializer by name; a missing registration fails replay. It must also run on nodes where `autonomic.enabled` is false and in offline tools that read the log, because the log is shared by the whole cluster. `AutonomicTransformations.register()` must therefore depend only on serializer classes, not on `AutonomicService` or `DatabaseDescriptor`.

Total: 12 upstream source files, one config file, two upstream test files, plus fixtures. H12 is **unverified** as strictly required (the list was read, the failure mode when a type is missing was not); follow the `AutoRepairMetrics` precedent.

### 4.2 Core APIs read (no modification)

| Signal source | API | Notes |
|---------------|-----|-------|
| Per-table latency, sstables, tombstones | `ColumnFamilyStore.metric` (`src/java/org/apache/cassandra/metrics/TableMetrics.java`): `readLatency`, `writeLatency`, `rangeLatency`, `coordinatorReadLatency`, `coordinatorWriteLatency`, `sstablesPerReadHistogram`, `tombstoneScannedHistogram`, `liveScannedHistogram`, `pendingCompactions`, `liveSSTableCount`, `pendingFlushes`, `maxPartitionSize`, `meanPartitionSize`, `tombstoneWarnings`, `tombstoneFailures` | Read `getSnapshot()` percentiles and meter rates. Iterate tables with the static `ColumnFamilyStore.all()`. Skip system keyspaces with `SchemaConstants.isSystemKeyspace`. |
| Client request outcome | `src/java/org/apache/cassandra/metrics/ClientRequestsMetricsHolder.java`: `readMetrics`, `writeMetrics`, `casReadMetrics`, `casWriteMetrics` (`timeouts`, `unavailables`, `failures`, latency) | Node-wide coordinator view. |
| Thread pools | `CassandraMetricsRegistry.Metrics.allThreadPoolMetrics()` returning `ThreadPoolMetrics` (`activeTasks`, `pendingTasks`, `totalBlocked`, `maxPoolSize`, `oldestTaskQueueTime`) | Same source as `system_views.thread_pools`. |
| Compaction | `CompactionManager.instance.getMetrics()` (`pendingTasks`, `bytesCompactedThroughput`) | |
| Disk | `DiskUsageMonitor.instance.getDiskUsage()` (`src/java/org/apache/cassandra/service/disk/usage/DiskUsageMonitor.java`) | Whether the monitor is started when its guardrail is unset is **unverified**; fall back to `java.nio.file.FileStore` on data directories. |
| GC | `java.lang.management.GarbageCollectorMXBean` directly | Do not use `GCInspector.getAndResetStats()`: it resets counters shared with `nodetool gcstats`. |
| Hot partitions | `ColumnFamilyStore.beginLocalSampling(String, int, int)` / `finishLocalSampling(String, int)` | Section 5.4 for the contention rule. |
| Large/tombstone-heavy partitions | `ColumnFamilyStore.getTopSizePartitions()`, `getTopTombstonePartitions()` | Only fresh after repair validation. |
| Peers | Gossip `ApplicationState.LOAD` and `SEVERITY` (`src/java/org/apache/cassandra/gms/ApplicationState.java`), `DynamicEndpointSnitch.getScores()` | Used by the imbalance and degraded-peer detectors. No new messages. |
| Cluster state | `ClusterMetadata.current()` (`directory`, `inProgressSequences`, `extensions`, `myNodeState()`), `ClusterMetadataService.instance().log().addListener(ChangeListener)` | |

### 4.3 Core APIs called by actuators (no modification)

Listed in section 8.1 (knobs) and section 8.3 (topology).

## 5. Knowledge store

Decision: ADR-007. Three tiers with different lifetimes.

| Tier | Contents | Location | Scope | Retention |
|------|----------|----------|-------|-----------|
| Observations | Time series per (signal, subject) | In-memory ring buffers (`ObservationStore`) | Node | `observation_retention` (default 30m); at most `max_series` (default 5000) series, least-recently-updated evicted |
| Diagnoses and actions | Diagnoses, action records, action state transitions, small persistent flags | `system_autonomic` keyspace, `LocalStrategy` | Node | TTL 30 days (`history_ttl`) |
| Cluster ledger | Cluster-action claims, recent cluster outcomes, cluster-actions-enabled flag | `ClusterMetadata.extensions` | Cluster, linearizable | Bounded structure: at most one claim per lane, 32 recent entries |

Raw observations are never written to disk: at a 5s cadence over thousands of tables that would be a self-inflicted write load, and their value expires within the verification window.

### 5.1 Local tables

`AutonomicKeyspace` follows `src/java/org/apache/cassandra/service/accord/AccordKeyspace.java`: tables are parsed with `CreateTableStatement.parse(...)`, given ids from `TableId.forSystemTable(keyspace, name)`, and the keyspace is created with `KeyspaceParams.local()`.

```sql
CREATE TABLE system_autonomic.diagnoses (
    day date,                      -- UTC day of first_seen; bounds partition size
    id timeuuid,
    type text,
    subject text,                  -- Subject.asString()
    severity text,
    confidence double,
    state text,                    -- OPEN | CLEARED
    first_seen timestamp,
    last_seen timestamp,
    summary text,
    evidence map<text, text>,
    root_cause timeuuid,
    PRIMARY KEY ((day), id)
) WITH CLUSTERING ORDER BY (id DESC) AND default_time_to_live = 2592000;

CREATE TABLE system_autonomic.actions (
    day date,
    id timeuuid,
    diagnosis_id timeuuid,
    policy text,
    actuator text,
    scope text,                    -- NODE | CLUSTER
    target text,
    params map<text, text>,
    dry_run boolean,
    state text,                    -- ActionState name
    state_reason text,
    undo map<text, text>,          -- Applied.undo; written before verification starts
    baseline map<text, double>,
    result map<text, double>,
    verdict text,
    proposed_at timestamp,
    updated_at timestamp,
    PRIMARY KEY ((day), id)
) WITH CLUSTERING ORDER BY (id DESC) AND default_time_to_live = 2592000;

CREATE TABLE system_autonomic.action_events (   -- append-only audit trail
    action_id timeuuid,
    at timeuuid,
    from_state text,
    to_state text,
    detail text,
    PRIMARY KEY ((action_id), at)
) WITH default_time_to_live = 2592000;

CREATE TABLE system_autonomic.local_state (     -- small durable flags
    key text PRIMARY KEY,          -- 'paused', 'inflight/<action id>', 'cooldown/<dedupe key>', 'quarantine/<policy>'
    value text,
    updated_at timestamp
);
```

Access is through `QueryProcessor.executeInternal(String, Object...)`. Rows in `diagnoses` are written on open, on severity change, and on clear, not on every evaluation.

### 5.2 Why the keyspace is always defined

`Schema.initialize()` builds the set of local keyspaces once, in a static initializer. Following the Accord precedent (conditional on a startup flag) would mean `autonomic.enabled` could not be turned on at runtime. The keyspace is therefore always defined; with the feature off it holds four empty tables and nothing writes to them. This is the "knowledge store schema" exception already present in the D2 acceptance criteria. What stock Cassandra does on downgrade when it finds an unknown `system_autonomic` directory is **unverified**.

### 5.3 Virtual tables

All read-only, all local to the queried node, defined in `AutonomicVirtualTables.getAll(String keyspace)`.

| Table | Source | Columns (key first) |
|-------|--------|---------------------|
| `system_views.autonomic_status` | `AutonomicService` | `name text` PK, `value text`: state, dry_run, paused_reason, armed, loop iteration counts, last tick times, in-flight action count |
| `system_views.autonomic_components` | component registry | `kind text`, `name text` PK; `enabled boolean`, `healthy boolean`, `last_run timestamp`, `last_duration_micros bigint`, `consecutive_failures int`, `disabled_reason text` |
| `system_views.autonomic_signals` | `ObservationStore` | `signal text`, `subject text` PK; `value double`, `at timestamp`, `samples int` |
| `system_views.autonomic_diagnoses` | memory (open) and `system_autonomic.diagnoses` (last 24h) | as the table |
| `system_views.autonomic_actions` | `system_autonomic.actions` (last 7 days) | as the table minus `undo` |
| `system_views.autonomic_cluster_ledger` | `ClusterMetadata.current()` | `lane text` PK; `action_id`, `actuator`, `target`, `owner` (host id), `phase`, `step`, `claimed_at`, `lease_expires_at`, plus one row per recent entry with `lane = 'recent/<n>'` |

Cluster-wide viewing of node-local tables is not in the MVP. `src/java/org/apache/cassandra/db/virtual/RemoteToLocalVirtualKeyspace.java` wraps a virtual keyspace so that it can be queried on other nodes; it is used only for Accord debugging today and its suitability is **unverified**. `nodetool autonomic history` reads one node.

### 5.4 Sampling contention rule

The hot-partition sensor opens a `READS` and a `WRITES` window per user table for `sampling_window` (default 10s) every `sampling_interval` (default 60s), only for tables whose request rate exceeds `sampling_min_ops_per_sec`. Because each sampler supports one window at a time and operators use the same samplers through `nodetool profileload` (`StorageService.samplePartitions`, `SamplingManager`):

- before opening, check `Sampler.isActive()`; if active, skip this table this round;
- if `beginLocalSampling` throws, treat as skipped, not as a sensor failure;
- the operator can still collide with our window and see "Sampling already in progress". This is documented in the operator guide (T7.3) and is a known limitation (risk R6).

## 6. Coordination

Decision: ADR-006.

### 6.1 Scope classification

An action is **node-scoped** if its entire effect and its rollback are confined to in-memory state of the local process. Everything else is **cluster-scoped**.

| Scope | Examples | Decided by | Serialized by |
|-------|----------|-----------|---------------|
| NODE | compaction throughput, pool sizes, cache sizes, local rate limit, local severity | the node itself | the single actuator thread |
| CLUSTER | scale-out, layout migration, auto-repair prioritization (writes `system_distributed`), anything using `Schema.submit` | any enabled node may propose; exactly one wins the claim | the cluster ledger in TCM |

Node-scoped actions deliberately need no coordination. The cost is that several nodes may take the same local action at the same time (for example, all replicas of a hot range lower compaction throughput). Per-node envelopes (section 8.1) bound the combined effect.

### 6.2 Cluster ledger

One extension entry in `ClusterMetadata.extensions`:

```java
public final class ClusterLedger                       // immutable
{
    public final int formatVersion;                    // 1
    public final boolean clusterActionsEnabled;        // default false; set only by an operator command
    public final ImmutableMap<Lane, Claim> claims;     // at most one per lane
    public final ImmutableList<Recent> recent;         // newest first, max 32
}
public enum Lane { TOPOLOGY, SCHEMA_LAYOUT, MAINTENANCE }

public final class Claim
{
    public final UUID actionId;
    public final String actuator, target;
    public final NodeId owner;
    public final String phase;                         // actuator-defined, e.g. "PROVISIONING"
    public final int step;                             // increments on every advance; CAS token
    public final long claimedAtMillis, leaseExpiresAtMillis;
    public final ImmutableMap<String, String> state;   // small actuator state; max 16 entries / 4 KiB serialized
}
public final class Recent { UUID actionId; String actuator, target; Lane lane; String finalState; long endedAtMillis; }
```

`ClusterLedgerValue implements ExtensionValue<ClusterLedger>` and is stored under `ExtensionKey<ClusterLedger, ClusterLedgerValue>` with id `"org.apache.cassandra.autonomic.ledger"`. Implementation constraints found in the code:

- Implement `ExtensionValue` directly. `AbstractExtensionValue` declares `serializeInternal`, `deserializeInternal` and `serializedSizeInternal` package-private, so it cannot be extended from our package.
- The value class needs a public no-argument constructor: `ExtensionKey.newValue()` constructs it reflectively during deserialization, and the key serializes the value class name, so **the class must never be renamed or moved**.
- `ClusterMetadata.Transformer.build()` calls `withLastModified(epoch)` on the value in place ("these are actually mutable"). A transformation must always put a **new** `ClusterLedgerValue` into the transformer and never mutate the one obtained from `prev`.
- Serialization is versioned by our own `formatVersion` written first, independent of the TCM `Version` argument.

Transformations (each a `Transformation` with `kind()` returning `Kind.CUSTOM`, wrapped in `new CustomTransformation(NAME, child)` when committed, returning `Transformation.success(prev.transformer().with(KEY, newValue), LockedRanges.AffectedRanges.EMPTY)` or `new Transformation.Rejected(ExceptionCode.INVALID, reason)`):

| Transformation | Fields | Succeeds when | Effect |
|----------------|--------|---------------|--------|
| `SetClusterActionsEnabled` | `enabled` | always | sets the flag; creates the ledger if absent |
| `ClaimAction` | `lane`, `claim`, `nowMillis`, `cooldownMillis` | flag set; lane free or held by the same `actionId` (idempotent) or held with `leaseExpiresAtMillis < nowMillis`; no `Recent` with the same actuator and target ended within `cooldownMillis`; `prev.inProgressSequences.isEmpty()` when lane is `TOPOLOGY` | writes the claim with `step = 0` |
| `AdvanceAction` | `actionId`, `owner`, `expectedStep`, `phase`, `state`, `nowMillis`, `leaseMillis` | lane holds `actionId` with that `owner` and `step == expectedStep` | `step + 1`, new phase/state, lease renewed |
| `ReleaseAction` | `actionId`, `owner` (null when forced by an operator), `finalState`, `nowMillis` | lane holds `actionId` (and owner matches unless forced) | removes the claim, prepends a `Recent` |

Rules that follow from how TCM works:

- **Determinism.** `Transformation.execute` runs on every node during replay. It must not read clocks, configuration, or local state. All times are fields supplied by the proposer (the same pattern as `ClusterMetadata.Transformer.withAccordDownStatusSince(..., updateTimeMillis, ...)`). Limits such as cooldown are fields too.
- **Ambiguous commits.** The `ClusterMetadataService.commit` javadoc states there is no reliable way to learn whether a particular transformation was executed after a timeout. Every transformation above is idempotent for the same `actionId` and `step`, and the proposer decides success by reading the resulting `ClusterLedger`, not by the commit return path.
- **Lease and clocks.** Lease expiry compares proposer-supplied timestamps. Clock skew larger than the lease margin can cause an early takeover. Default lease is 10 minutes, renewed every `lease/3`; the owner stops acting on its own claim once its local clock is past `leaseExpiresAtMillis - lease_safety_margin` (default 60s).
- **Rate.** Cluster actions are rare (at most a few per hour by default limits). Each produces a handful of log entries. The ledger is not used for heartbeats or metrics.

### 6.3 Who proposes

Any armed node whose policies produce a cluster-scoped plan may propose. To avoid a thundering herd of rejected claims, a node proposes only if it is the **preferred proposer**: the JOINED node with the lowest `NodeId` among those it currently sees alive via the failure detector. This is an optimization, not a safety property; if two nodes disagree, `ClaimAction` still admits one.

The claim owner drives the action. Steps that must happen on a specific other node (for example, D6 phase changes, or a future scale-in where the leaving node must call its own `decommission`) are not sent as messages: the owner advances the claim, and the target node's `ChangeListener` observes the new phase and acts. The ledger is the command channel.

### 6.4 Failure of the owner and partitions

| Situation | Behaviour |
|-----------|-----------|
| Owner crashes or is partitioned from the CMS | It cannot renew. After expiry any preferred proposer commits `ClaimAction` for the same lane with a **new** claim whose `actuator` is the original and `phase` is `RECOVERING`; the actuator's `recover(Claim)` decides to resume or to undo. MVP actuators always undo, then release with `ROLLED_BACK`. |
| Owner is alive but cannot commit | `AdvanceAction` fails at its deadline. The owner retries until `leaseExpiresAtMillis - lease_safety_margin`, then stops acting and treats the claim as lost. |
| Owner returns after a takeover | Its next `AdvanceAction` is rejected (`step` or `owner` mismatch). It marks its local record `FAILED` with reason `CLAIM_LOST` and performs no further side effects. |
| CMS unavailable | No cluster action can start or advance. Node-scoped actions are unaffected. In-flight cluster actions stall; side effects already issued (for example a provisioning request) are covered by the provider's idempotency key. |
| Node cannot see the latest epoch | It acts on a stale ledger only for reads. All decisions that matter are CAS-style commits and are rejected if stale. |

Cluster-scoped actuators therefore extend the base contract:

```java
public interface ClusterActuator extends Actuator
{
    Lane lane();
    /** One bounded step. Returns the next phase, or a terminal result. Called on the actuator thread of the owner. */
    StepResult step(Action action, Claim claim, ActionContext ctx) throws Exception;
    /** Called on a new owner after lease expiry. Must leave the cluster in the pre-action state or complete the action. */
    StepResult recover(Claim expired, ActionContext ctx) throws Exception;
    /** Called on every node when the claim's phase changes; for work that must happen locally. Default no-op. */
    default void onPhase(Claim claim, ActionContext ctx) {}
}
```

### 6.5 Mixed versions and feature-disabled nodes

- **Feature-disabled nodes of this fork** deserialize and apply ledger transformations (H9 is unconditional) but never propose and never run `onPhase` work. A cluster action that needs every node to participate (D6) must check participation explicitly; how is a T1.2 question (section 9.3). Scale-out does not need it.
- **Stock Cassandra nodes** cannot deserialize our transformation names: `CustomTransformation.getSerializer` asserts on an unknown extension, and `ClusterMetadata` deserialization of the extension map loads our value class by name. A stock node receiving such a log entry or snapshot fails. `Kind.CUSTOM` has `introducedIn = V0`, so the built-in `Transformation.eligibleToCommit` check gives no protection, and `Directory` carries no capability flags we could test.
- Consequence and rule: **nothing is written to the TCM log until an operator runs `nodetool autonomic cluster enable`**, which commits `SetClusterActionsEnabled(true)`. That command is the operator's attestation that every node runs this fork. Before it, a cluster may freely mix fork and stock nodes and use all node-scoped features. After it, adding a stock node is unsupported. The command refuses if `directory.clusterMinVersion != directory.clusterMaxVersion` (a best-effort check; it cannot tell the fork from stock at the same version).
- Rolling upgrades between versions of this fork: the ledger carries `formatVersion`; a new format may only be written after all nodes are upgraded. For the MVP there is one format, and this rule is recorded as an obligation rather than implemented.

## 7. Safety model

Decision: ADR-008.

### 7.1 Limits enforced by `SafetyGovernor`

Evaluated in this order; the first failing check rejects the action with its name as `state_reason`.

| # | Check | Default | Notes |
|---|-------|---------|-------|
| 1 | `KILLED` / `PAUSED` / not armed | | |
| 2 | `DRY_RUN` | `dry_run: true` | Action is recorded as `REJECTED` with reason `DRY_RUN` and the full plan, so operators can see what would have happened. Dry-run is the default when the layer is first enabled. |
| 3 | `ACTUATOR_DISABLED` | each actuator off by default | |
| 4 | `VALIDATION` | | `Actuator.validate` |
| 5 | `NODE_STATE` | | node must be `JOINED`, not draining (`StorageService.isDraining()`); for `TOPOLOGY` lane also no in-progress sequences and every directory member alive |
| 6 | `STORE_UNAVAILABLE` | | the write-ahead record for the action must be writable (section 7.6) |
| 7 | `QUARANTINED` | 6h | set when the same policy saw `quarantine_after_rollbacks` (default 3) rollbacks within `quarantine_window` (default 1h) |
| 8 | `COOLDOWN` | 10m after commit, 30m after rollback, doubling per consecutive rollback up to 6h | keyed by `dedupeKey` |
| 9 | `CONFLICT` | | another in-flight action has the same `dedupeKey` or the same knob |
| 10 | `CONCURRENCY` | 1 in-flight node action per node; 1 cluster action per lane | |
| 11 | `RATE` | 6 actions/hour per node, 2/hour per actuator, 2 cluster actions/hour | sliding windows |
| 12 | `BLAST_RADIUS` | | per-actuator: knob step at most `max_step_fraction` (default 0.25) of current value and inside its envelope; `TOPOLOGY` at most one node per action |
| 13 | `APPROVAL` | `auto` for NODE, `manual` for CLUSTER | manual: the action stays `PROPOSED` until `nodetool autonomic approve <id>` or `proposal_ttl` (default 1h) expires |

### 7.2 Action state machine

```
PROPOSED --governor approve / operator approve--> APPROVED --actuator thread--> APPLYING
   |                                                                         |        |
   +--> REJECTED (governor, dry-run, ttl)                     apply threw ---+        v
                                                                   |             VERIFYING
                                                                   v            /    |    \
                                               FAILED <-- rollback threw   IMPROVED  |   REGRESSED, NEUTRAL,
                                                  ^                            |     |   INCONCLUSIVE, kill,
                                                  |                            v     |   deadline
                                                  |                       COMMITTED  |        |
                                                  |                                  v        v
                                                  |                            SUPERSEDED  ROLLING_BACK
                                                  +----------------------------------------/    |
                                                                                                v
                                                                                           ROLLED_BACK
```

| State | Entered when | Persisted before side effects? |
|-------|-------------|-------------------------------|
| `PROPOSED` | a policy returns a plan | yes (control thread) |
| `REJECTED` (terminal) | governor rejects, dry-run, proposal expires | yes |
| `APPROVED` | governor or operator approves | yes |
| `APPLYING` | actuator thread picks it up; baseline captured | **yes, synchronously, with `local_state` key `inflight/<id>`** |
| `VERIFYING` | `apply` returned; `undo` persisted | yes, synchronously |
| `COMMITTED` (terminal) | verdict `IMPROVED` | yes |
| `ROLLING_BACK` | verdict not `IMPROVED`, kill, dead-man deadline, `apply` threw after partial effect | yes |
| `ROLLED_BACK` (terminal) | `rollback` returned | yes |
| `SUPERSEDED` (terminal) | `Actuator.overridden` is true at verification time: an operator changed the value. The layer does not undo an operator's change. | yes |
| `FAILED` (terminal) | `rollback` threw, or claim lost. Raises diagnosis `autonomic.action_failed` (CRITICAL) and pauses the layer. | yes |

Every transition appends a row to `system_autonomic.action_events`, increments a metric, and publishes a diagnostic event.

### 7.3 Verify and rollback contract

Every actuator must satisfy all of the following; T2.6 provides an abstract test class that each actuator's unit test extends.

1. `apply` is idempotent per action id and returns `Applied.undo` sufficient to restore the prior state without consulting anything else.
2. `rollback(apply(x))` restores every value `apply` changed, exactly. Calling `rollback` twice is harmless.
3. `rollback` uses only local, non-blocking operations for node-scoped actuators. It must be callable when the knowledge store and TCM are unavailable.
4. `overridden` compares the live value with the value `apply` set. If they differ, neither verification nor rollback touches the knob.
5. **Baseline first.** Before `apply`, the executor records the mean of the goal signal and each guard signal over `baselineWindowMillis`. If fewer than `min_samples` (default 3) observations exist, the action is rejected with `INSUFFICIENT_BASELINE`.
6. **Verdict.** After `observationWindowMillis`, `MetricVerifier` computes the same means. `REGRESSED` if any guard worsened beyond its tolerance or the goal worsened by more than `minImprovement`; `IMPROVED` if the goal improved by at least `minImprovement` and no guard regressed; `INCONCLUSIVE` if samples are missing; otherwise `NEUTRAL`.
7. **A change must earn its place.** Only `IMPROVED` commits. `NEUTRAL` and `INCONCLUSIVE` roll back. This biases the system toward the operator's configured baseline.
8. **Early abort.** During the window the control thread checks guards every tick; a guard beyond `abort_tolerance` (default twice its tolerance) triggers immediate rollback.
9. **Dead-man deadline.** At `apply` time the actuator thread schedules its own rollback task at `observationWindowMillis + verify_grace`. Verification cancels it. If the control thread stalls, the action still rolls back.
10. **Irreversible actuators** (`reversible() == false`, for example a garbage-collecting compaction) must be bounded in cost by construction, are rate-limited to one per target per day by default, and are verified for reporting only: the terminal state is `COMMITTED` with the verdict recorded. Policies may only select them at severity `CRITICAL`.
11. **Committed is not permanent** for node knobs: a committed value is still an in-memory override of `cassandra.yaml`. The `baseline_drift` detector (T4.1) reports knobs that differ from configuration, and `nodetool autonomic revert` restores all of them.

### 7.4 Oscillation damping

| Mechanism | Where | Rule |
|-----------|-------|------|
| Hysteresis | `ThresholdDetector` base class | separate `raise` and `clear` thresholds; clear threshold defaults to 80% of raise |
| Persistence | same | condition must hold for `for_evaluations` consecutive ticks (default 4, about 1 minute) before a diagnosis opens, and be absent for the same number before it clears |
| Cooldown | governor check 8 | no action on the same `dedupeKey` for the cooldown; longer after rollback, with exponential growth |
| Step limit | governor check 12 | at most 25% change per action; additive-increase, multiplicative-decrease for rate limits |
| Direction lock | `KnobActuator` | after a rollback of an increase, the same knob may not be increased again until the diagnosis has cleared once |
| Quarantine | governor check 7 | repeated rollbacks disable the policy for hours and raise a diagnosis |
| One change at a time | governor check 10 | a second action cannot start while one is verifying, so effects are attributable |

### 7.5 When the autonomic layer is the problem

| Failure | Detection | Response |
|---------|-----------|----------|
| A sensor, detector or policy throws | caught per component invocation | counted; after `component_failure_threshold` (default 5) consecutive failures the component is disabled until restart or `nodetool autonomic resume`, and `autonomic.component_failed` is raised. `Error`s go through `JVMStabilityInspector.inspectThrowable`. |
| A sensor is slow | duration measured per call | over `sensor_budget` (default 200ms) counts as a failure for the rule above |
| Monitor or control thread stalls | each thread records a heartbeat; the other checks it | diagnosis `autonomic.loop_stalled`; in-flight actions are protected by the dead-man deadline on the actuator thread |
| Actuator thread stalls | control thread sees an action in `APPLYING` beyond `action_step_timeout` | layer pauses itself; state is visible in `autonomic_status`; no automatic interruption of the thread |
| Actions keep making things worse | rollback ratio | if at least `breaker_rollbacks` (default 4) of the last `breaker_window` (default 6) actions rolled back or failed, the layer enters `PAUSED` with reason `CIRCUIT_BREAKER` and stays there until an operator resumes it |
| Knowledge store unwritable | write failure | diagnoses continue in memory; no action is approved (check 6). Rollbacks still run. |
| Too much memory | `max_series` cap | eviction; `autonomic.series_evicted` counter |
| Node restarts mid-action | recovery (7.6) | |
| Layer overhead itself | `AutonomicMetrics` | time per tick, per sensor, store writes; visible without enabling any action |

Fail-static principle: in every failure above the layer converges to "do nothing new, restore what is in flight". No failure path applies a new change.

### 7.6 Restart recovery

All node-scoped knobs are in-memory overrides, so a restart has already restored `cassandra.yaml` values. On `start()`:

1. read `local_state` keys `inflight/*`;
2. for each node-scoped action in `APPLYING`, `VERIFYING` or `ROLLING_BACK`: mark `ROLLED_BACK` with reason `RESTART`, delete the key; do not call `rollback`;
3. for each cluster-scoped action: read the ledger; if this node still owns an unexpired claim, call `recover`; otherwise mark `FAILED` with reason `CLAIM_LOST`;
4. apply a post-restart cooldown to each affected `dedupeKey`.

## 8. Elasticity

Decision: ADR-010.

### 8.1 In-node elasticity: the knobs

"In-node elasticity" means moving these existing runtime settings inside a configured envelope `[min, max]` around the `cassandra.yaml` value, through the apply/verify/rollback loop. All setters below were read at the pinned commit.

| Knob | Setter | Getter for undo | Direction under foreground pressure | Default envelope |
|------|--------|-----------------|-------------------------------------|------------------|
| Compaction throughput | `StorageService.instance.setCompactionThroughputMbPerSec(int)` (also calls `CompactionManager.instance.setRateInBytes`) | `getCompactionThroughputMbPerSec()` | down; up when backlog grows and foreground is healthy | 0.25x to 4x configured |
| Concurrent compactors | `StorageService.instance.setConcurrentCompactors(int)` | `getConcurrentCompactors()` | down; up for backlog | 1 to available processors / 2 |
| Stream throughput | `StorageService.instance.setStreamThroughputMebibytesPerSec(int)` | `getStreamThroughputMebibytesPerSec()` | down | 0.25x to 2x |
| Request stage pools | `StorageService.instance.setConcurrency(String stage, int core, int max)` for `ReadStage`, `MutationStage`, `CounterMutationStage`, `ViewMutationStage` (`src/java/org/apache/cassandra/concurrent/Stage.java`) | `getConcurrency(List<String>)` | up when `pendingTasks` is sustained and CPU is not saturated; down otherwise | 0.5x to 2x configured, never above 8x processors |
| Native transport rate limit | `ClientResourceLimits.setNativeTransportMaxRequestsPerSecond(int)` and `StorageService.instance.setNativeTransportRateLimitingEnabled(boolean)` | corresponding getters | enable and lower (AIMD) when coordinator latency breaches the SLO and queues grow; raise and finally disable on recovery | floor `min_requests_per_second` (default 1000) |
| Native transport in-flight bytes | `StorageService.instance.setNativeTransportMaxConcurrentRequestsInBytes(long)` | getter | down under heap pressure | 0.25x to 1x |
| Key / row / counter cache capacity | `CacheService.instance.setKeyCacheCapacityInMB(long)`, `setRowCacheCapacityInMB(long)`, `setCounterCacheCapacityInMB(long)` | getters (**unverified** names) | up on low hit rate with heap headroom; down under GC pressure | 0.5x to 2x |
| Hinted handoff throttle | `StorageService.instance.setHintedHandoffThrottleInKB(int)` | no `getHintedHandoffThrottle*` method found in `StorageService`; read `DatabaseDescriptor` (**unverified** name) | down | 0.25x to 1x |
| Advertised severity | `DynamicEndpointSnitch.setSeverity(double)` | `getSeverity()` | up on a self-diagnosed degraded node, so coordinators using the dynamic snitch prefer other replicas | 0 to 1 |

Notes:

- `native_transport_throw_on_overload` and the `native_transport_queue_max_item_age_threshold` backoff settings are non-volatile fields in `Config.java`; no runtime setter was found. They are configuration, not actuators.
- Enabling the native rate limiter changes client-visible behaviour (requests are delayed or rejected with an overload error depending on `native_transport_throw_on_overload`). The throttle actuator is therefore separately enabled and its policy only fires at `CRITICAL`.
- Memtable, heap, and GC settings are not runtime-tunable and are out of scope.
- Per-table local compaction overrides exist (`ColumnFamilyStore.setCompactionParameters(Map)`); they are reversible and node-local but change on-disk organisation. Out of MVP, noted as a follow-up actuator.

The order of response to a load spike is fixed by policy priority: (1) yield background work (compaction, streaming, hints), (2) resize request pools, (3) shed load with the rate limiter, (4) propose scale-out.

### 8.2 What scale-out costs

- Adding a node streams data for every range it takes over. Time is proportional to data per node and bounded by stream throughput; expect tens of minutes to hours. During that time the cluster does **more** work, not less. Scale-out therefore cannot answer a spike measured in minutes; it answers a sustained trend.
- After a join, existing replicas keep data they no longer own until cleanup runs.
- Removing a node streams all its data out first. Scale-in is slower and riskier than scale-out and is not automated in the MVP.
- What TCM changes: joins are multi-step sequences committed to the metadata log (`PREPARE_JOIN`, `START_JOIN`, `MID_JOIN`, `FINISH_JOIN` in `Transformation.Kind`; `src/java/org/apache/cassandra/tcm/sequences/BootstrapAndJoin.java`), with range locking (`LockedRanges`) and progress barriers. This makes membership changes linearizable and resumable and removes the gossip-era ring-settle waits and the risk of concurrent conflicting topology changes. It does not make streaming faster. Whether non-overlapping joins can run concurrently in practice at this commit is **unverified**; the MVP adds one node at a time.
- `AutoRepairUtils.runRepairOnNewlyBootstrappedNodeIfEnabled()` is called at the end of `BootstrapAndJoin`, so a joined node is repaired by auto-repair when that is enabled.

### 8.3 Scale-out provider SPI

The provider's job ends at "a Cassandra process configured to join this cluster has been started". The join itself is the new node's normal startup.

```java
public interface ScaleOutProvider
{
    String name();
    Capabilities capabilities();     // canAdd, canRemove, minNodes, maxNodes, typicalProvisionMillis

    /** Idempotent on request.actionId. Must return quickly; provisioning is asynchronous. */
    ProvisionTicket requestNode(ScaleRequest request) throws ProviderException;
    ProvisionStatus status(ProvisionTicket ticket) throws ProviderException;  // PENDING | STARTED(address) | FAILED(reason)
    /** Undo of requestNode before the node has joined. Idempotent. */
    void cancel(ProvisionTicket ticket) throws ProviderException;
    /** Release capacity for a node whose state is LEFT. MVP: called only by operator-initiated flows. */
    void releaseNode(String hostId, String actionId) throws ProviderException;
}
public final class ScaleRequest { UUID actionId; String datacenter, rack; String reason; }
public final class ProvisionTicket { String id; Map<String, String> attributes; }  // stored in Claim.state
```

`ScaleOutActuator` (a `ClusterActuator`, lane `TOPOLOGY`, approval `manual` by default):

| Phase | Step | Rollback / recover |
|-------|------|--------------------|
| `REQUESTED` | `provider.requestNode`; store ticket in claim | `provider.cancel` |
| `PROVISIONING` | poll `provider.status`; timeout `provision_timeout` | `provider.cancel` |
| `JOINING` | watch `ClusterMetadata.current().directory` for a new node at the reported address reaching `JOINED`; timeout `join_timeout` | no automatic undo of a started join; release the claim as `FAILED` and raise a CRITICAL diagnosis for the operator |
| `VERIFYING` | goal: the triggering signal (for example, max node disk usage or sustained pool saturation) improves over a long window (default 1h) | none: a joined node is never automatically removed. Terminal state `COMMITTED` with verdict recorded. |

Providers: `NoopProvider` (default; logs and reports `FAILED("no provider configured")`), `SimulatedProvider` for tests (section 11), and a Kubernetes provider reduced to a documented stub (see the recommendation in section 13, task T5.5).

## 9. Adaptive schema at framework level

This section fixes only what T1.2 must build on. Decision: ADR-012 (Proposed; T1.2 accepts or supersedes it).

### 9.1 Shape

- A **layout** maps one logical table (the schema clients see) to one or more physical tables, with a routing rule. Layouts live in `ClusterMetadata.extensions` under their own `ExtensionKey` (not inside `ClusterLedger`, so their size and format evolve independently). Because schema and layouts are both part of `ClusterMetadata`, a coordinator at epoch E sees a consistent pair. This is the property that made TCM the base.
- Physical tables are ordinary tables created through the normal `Schema.submit(...)` / `AlterSchema` path. Replicas, streaming, repair, compaction and backups see nothing new.
- The **indirection sits on the coordinator, at the CQL statement layer**: after a statement is prepared against the logical table and before it reaches `StorageProxy`. Two candidate sites exist and T1.2 chooses:
  - a wrapping `QueryHandler` installed with the existing `cassandra.custom_query_handler_class` property (`CassandraRelevantProperties.CUSTOM_QUERY_HANDLER_CLASS`, read in the static initializer of `src/java/org/apache/cassandra/service/ClientState.java`). Zero core edits, but it is a startup-time system property, it sees whole statements rather than bound partitions, and internal callers of `QueryProcessor.executeInternal` bypass it;
  - a narrow hook inside `src/java/org/apache/cassandra/cql3/statements/SelectStatement.java` and `ModificationStatement.java` where the partition key is known. More precise, more invasive.
- A migration is a cluster-scoped action in lane `SCHEMA_LAYOUT`, implemented as a `ClusterActuator`. Its phases (for example dual-write, backfill, cutover, cleanup) are ledger phases; each phase change is a TCM commit; every coordinator switches behaviour in `onPhase` at a known epoch.
- The framework supplies: the claim and lease, the phase channel, the safety governor (manual approval by default), audit, the kill switch, and the hot-partition diagnosis that triggers a proposal.

### 9.2 What the framework does not supply

Query rewriting, result merging, backfill, and data equivalence checking. These are D6.

### 9.3 Open questions T1.2 must answer

1. **Which layout transformations are in scope?** Candidates: bucketing a wide partition by a clustering-derived component; salting a hot partition key; splitting a table by column group. Each has different read fan-out and ordering consequences. Which hot-spot shapes does each actually relieve?
2. **Rewrite site**: wrapping `QueryHandler` or statement-level hooks (9.1). How are prepared statements invalidated or re-bound on phase change, given the existing `SchemaChangeListener.onAlterTable(..., affectStatements)` path?
3. **Ordering and paging**: can results keep the logical clustering order and stable paging state across a fan-out merge? What happens to a paging state issued before cutover?
4. **Range and token queries**: `SELECT` without a partition key and `token()` restrictions depend on physical placement. Are they supported, degraded, or rejected for adapted tables?
5. **Write path atomicity during dual-write**: batchlog, Accord, or accept divergence and repair by backfill? What is the guarantee if a coordinator dies between the two writes?
6. **Timestamps, TTLs, tombstones**: backfill must preserve write timestamps and TTLs and must not resurrect deleted data. What is the mechanism and how does it interact with `gc_grace_seconds`?
7. **Unsupported features**: LWT/Paxos, Accord-managed tables (`TableMetadata.requiresAccordSupport`), counters, secondary indexes and SAI, materialized views, CDC, triggers, static columns. For each: support, block the migration, or migrate too?
8. **Epoch agreement**: how does the owner know every coordinator has adopted a phase before advancing? Is `src/java/org/apache/cassandra/tcm/sequences/ProgressBarrier.java` reusable, and what about feature-disabled nodes (section 6.5) that will never perform dual-writes?
9. **Stale coordinators**: a coordinator behind on epochs routes by an old layout. Which phases tolerate that, and how are the others fenced (replica-side epoch check, as TCM does for placements)?
10. **Rollback boundary**: until which phase is rollback loss-free? After cutover, new writes exist only in the new layout; is rollback then a reverse migration?
11. **"Eventually satisfied"**: the owner's wording permits temporary degradation for old-schema clients. What exactly may degrade (latency, availability at a consistency level, ordering) and for how long?
12. **Consistency levels**: what does `QUORUM` mean for a logical read that fans out to several physical partitions with different replica sets?
13. **Client-visible metadata**: what do `system_schema` tables and `DESCRIBE` show, and do drivers' token-aware routing decisions still land on a replica?
14. **Authorization**: permissions are granted on the logical table; how are physical tables protected from direct access?
15. **Layout registry size**: is the routing rule a predicate (small) or an explicit key list (unbounded)? It lives in every metadata snapshot.
16. **Verification signal**: what metric proves the hot spot was relieved, and over what window, given that migration itself adds load?

## 10. Configuration and observability

### 10.1 `cassandra.yaml` section

`AutonomicConfig` is a plain class with public fields, loaded as a nested section like `AutoRepairConfig`. Durations use `DurationSpec` types. Fields that can change at runtime are `volatile` and have MBean setters; there is no other hot-reload path at this commit.

```yaml
autonomic:
  enabled: false                      # master switch; runtime-settable
  dry_run: true                       # runtime-settable
  monitor_interval: 5s
  analyze_interval: 15s
  startup_grace: 5m
  observation_retention: 30m
  max_series: 5000
  history_ttl: 30d
  slo:                                # inputs to detectors
    read_latency_p99: 50ms
    write_latency_p99: 20ms
  sampling:
    enabled: true
    window: 10s
    interval: 60s
    min_ops_per_sec: 100
    capacity: 256
  safety:
    max_actions_per_hour: 6
    max_actions_per_actuator_per_hour: 2
    max_cluster_actions_per_hour: 2
    max_step_fraction: 0.25
    cooldown_after_commit: 10m
    cooldown_after_rollback: 30m
    cooldown_max: 6h
    quarantine_after_rollbacks: 3
    quarantine_window: 1h
    quarantine_duration: 6h
    breaker_rollbacks: 4
    breaker_window: 6
    node_approval: auto               # auto | manual
    cluster_approval: manual
    proposal_ttl: 1h
    verify_grace: 30s
    action_step_timeout: 30s
    component_failure_threshold: 5
    sensor_budget: 200ms
  coordination:
    lease: 10m
    lease_safety_margin: 60s
  detectors:                          # name -> options; all enabled when the layer is enabled
    hot_partition: { enabled: true, share_of_table_ops: 0.3, for_evaluations: 4 }
    compaction_backlog: { enabled: true, pending_tasks: 100 }
  actuators:                          # name -> options; each disabled by default
    compaction_throughput: { enabled: false, min_factor: 0.25, max_factor: 4.0 }
    request_pools: { enabled: false, min_factor: 0.5, max_factor: 2.0 }
    request_throttle: { enabled: false, min_requests_per_second: 1000 }
    scale_out: { enabled: false }
  scale_out:
    provider: org.apache.cassandra.autonomic.elastic.NoopProvider
    parameters: {}
    provision_timeout: 15m
    join_timeout: 6h
```

Three independent gates must all be open before anything changes: `enabled`, `dry_run: false`, and the specific actuator's `enabled`. Cluster-scoped actions need a fourth: `nodetool autonomic cluster enable`.

### 10.2 Operator interface

`AutonomicServiceMBean` (`org.apache.cassandra.db:type=AutonomicService`, registered through `MBeanWrapper.instance`) exposes: `getStatus()`, `setEnabled(boolean)`, `setDryRun(boolean)`, `pause(String reason)`, `resume()`, `kill(String reason)`, `revert()`, `approve(String actionId)`, `reject(String actionId)`, `setActuatorEnabled(String, boolean)`, `getHistory(int limit)`, `enableClusterActions(boolean)`, `releaseClaim(String lane)`.

`nodetool autonomic` is one picocli command group modelled on `CMSAdmin.java`:

| Subcommand | Effect |
|------------|--------|
| `status` | state, gates, component health, in-flight actions, ledger summary |
| `history [--limit N] [--diagnoses]` | recent actions or diagnoses from this node |
| `pause [reason]` / `resume` | stop / allow new actions |
| `kill [reason]` | pause and roll back everything in flight |
| `revert` | restore every managed knob to its `cassandra.yaml` value |
| `dryrun on\|off` | toggle dry-run |
| `approve <id>` / `reject <id>` | manual approval |
| `actuator enable\|disable <name>` | per-actuator gate |
| `cluster enable\|disable\|status\|release <lane>` | ledger flag and forced release (commits to TCM) |

### 10.3 Observability of the layer itself

- **Metrics** (`AutonomicMetrics`, type `Autonomic`): tick durations per stage; per-sensor duration and failures; observations per second; series count and evictions; diagnoses opened/cleared by type; actions by actuator and terminal state; rollbacks; governor rejections by reason; store write failures; ledger commit latency and rejections; current state as a gauge.
- **Diagnostic events** through `src/java/org/apache/cassandra/diag/DiagnosticEventService.java`: `AutonomicEvent` with types `DIAGNOSIS_OPENED`, `DIAGNOSIS_CLEARED`, `ACTION_TRANSITION`, `LAYER_STATE`.
- **Logs**: one INFO line per action transition and per diagnosis open/clear, with ids; component failures at WARN through `NoSpamLogger`.
- **Virtual tables**: section 5.3.
- **Self-diagnoses**: `autonomic.component_failed`, `autonomic.loop_stalled`, `autonomic.action_failed`, `autonomic.circuit_breaker`, `autonomic.baseline_drift` appear in the same diagnosis stream as database diagnoses.

## 11. Testing strategy

Hardware limit: 4 cores, 5 GB RAM; the junit fork default heap is 1024m (`build.xml`, `maxmemory`). Run with `.build/run-tests.sh -s -a test -t <Class>` or `-a jvm-dtest`.

| Deliverable | Unit tests (no cluster) | In-JVM dtests | Notes |
|-------------|------------------------|---------------|-------|
| D2 | loop scheduling with a fake clock; `ObservationStore` eviction; governor checks one by one; state machine transitions including dead-man rollback and restart recovery with a fake actuator; ledger transformations against a hand-built `ClusterMetadata` (model on `test/unit/org/apache/cassandra/tcm/ClusterMetadataTransformationTest.java`); serializer round-trip | 1 node: default config creates no `Autonomic*` threads; enable at runtime; `nodetool autonomic` subcommands; kill within one interval. 2 nodes: two concurrent `ClaimAction` commits, exactly one owner; lease expiry takeover; restart replays the log with ledger entries | Detectors and actuators are faked in D2 |
| D3 | each detector with a synthetic `KnowledgeView`: one series that triggers, one that does not, one that flaps and must not trigger | 1 node: write-heavy single partition yields `hot_partition` naming the key; tombstone-heavy reads yield `tombstone_overload` | Use low thresholds and short windows via config rather than real load |
| D4 | abstract actuator contract test (7.3 rules 1 to 4) extended by every actuator; `MetricVerifier` verdict table | 1 node: forced `REGRESSED` verdict rolls back and is audited. 2 nodes: auto-repair prioritization is rate-limited and visible in `system_distributed.auto_repair_priority` | Model auto-repair setup on `test/distributed/org/apache/cassandra/distributed/test/repair/AutoRepairSchedulerTest.java` |
| D5 | AIMD controller; envelope clamping; `ScaleOutActuator` phases with a fake provider and fake metadata | 1 node: pool resize stays in envelope under synthetic `pendingTasks`. 3 nodes: simulated provider adds a fourth node | A 4-instance cluster in a 1024m fork is **unverified**; fallback is 2 nodes growing to 3 |
| D6 | to be defined by T1.2 | 3 nodes | |
| D7 | none | scenario tests (hot spot, load spike, degraded node), fault injection of the layer itself | Each scenario under 5 minutes wall clock |

Test-specific design points:

- **Synthetic signals.** A test-only `InjectedSensor` lets a test push observations for any signal. Scenario tests assert on diagnoses and actions without generating real load, which the hardware could not do reliably.
- **Simulated provider.** In-JVM instances are isolated by class loader, so a provider running inside a node cannot call `ClusterUtils.addInstance(cluster, ...)`. `SimulatedProvider` records requests in a static queue inside the instance; the test thread polls it with `instance.callOnInstance(...)`, adds the node from the test side, and reports the address back the same way.
- **Clock.** All autonomic code takes time from an injected `LongSupplier` defaulting to `Clock.Global::currentTimeMillis`.
- **No sleeps for windows.** Tests set windows to hundreds of milliseconds through configuration.

## 12. Risks and open questions

| ID | Risk | Mitigation / status |
|----|------|---------------------|
| R1 | Upstream uses `Kind.CUSTOM` and `ExtensionKey` only in tests. Snapshot, log catch-up and upgrade paths with a real extension value are exercised by nobody else. | T2.8 includes restart, snapshot (`nodetool cms snapshot`) and new-node catch-up tests. If it fails, fallback is a dedicated `Transformation.Kind` (more invasive, ADR-006). |
| R2 | The `CustomTransformation` extension registry is a plain static `HashMap`. | Register only from the static initializer (H9). Never register at runtime. |
| R3 | Cluster-scoped features make the metadata log unreadable by stock Cassandra. | Explicit operator gate (6.5). Documented as one-way. |
| R4 | Decaying histograms respond slowly; a 1 to 2 minute verification window may see the pre-change tail. | Verifier uses rates and means over the window computed from our own samples, not the histogram's long-decay percentiles, where possible. Default observation window 3 minutes. Needs tuning in D4. |
| R5 | All replicas of a hot range take the same node-scoped action simultaneously. | Envelopes and step limits. Cross-node staggering is not in the MVP. |
| R6 | Sampler contention with `nodetool profileload`. | Yield rule (5.4); documented. |
| R7 | Trunk instability (ADR-001 consequence). | Hooks are next to auto-repair hooks, which upstream keeps working. |
| R8 | Lease expiry depends on wall clocks. | Safety margin; cluster actions are few and slow. A fencing epoch check inside every provider call is not possible for external systems; idempotency keys cover duplicates. |
| R9 | Memory: three or four in-JVM instances in a 1024m fork. | Unverified; fallback sizes in section 11. |
| R10 | The layer's own writes and sampling perturb the metrics it reads. | Low cadence, no raw observation persistence, sampling only above a rate threshold, self-metrics. |
| R11 | A `COMMITTED` knob value silently diverges from `cassandra.yaml` and is lost on restart, changing behaviour after a restart. | `baseline_drift` diagnosis and `revert`; operator guide tells operators to fold accepted values into configuration. |

Open questions for the project owner:

1. Is it acceptable that enabling cluster-scoped actions makes the cluster incompatible with stock Cassandra nodes (R3)?
2. Should automatic scale-**in** ever be in scope? This design excludes it.
3. Is manual approval as the default for cluster-scoped actions (scale-out, layout migration) consistent with the vision, or should automatic be the default once dry-run has been turned off?
4. Is node-local audit history sufficient for the MVP, or is a cluster-wide queryable history required (which needs a replicated keyspace and the problem in ADR-007)?
5. Should the Kubernetes stub remain a deliverable (T5.5)? It cannot be tested here and adds little beyond the SPI.
6. For "eventually satisfied" old-schema clients: what degradation is tolerable (question 11 in section 9.3)? T1.2 cannot be completed without this.

## 13. Refined task list

Tiers: **haiku** = mechanical or boilerplate; **sonnet** = standard implementation from this spec; **fable** = architectural or system-wide risk. "Hn" refers to section 4.1. New files are under `src/java/org/apache/cassandra/autonomic/` (abbreviated `autonomic/`) and tests under `test/unit/org/apache/cassandra/autonomic/` or `test/distributed/org/apache/cassandra/distributed/test/autonomic/`.

### D2 Framework

| ID | Description | Files | Depends | Acceptance | Tier |
|----|-------------|-------|---------|-----------|------|
| T2.1 | Config section and lifecycle skeleton: `AutonomicConfig`, `AutonomicService` (states of 3.6, no components yet), `AutonomicServiceMBean`, daemon hooks | `autonomic/AutonomicConfig.java`, `AutonomicService.java`, `AutonomicServiceMBean.java`; H1 to H5, H13, H14 | none | default config: MBean registered, zero `Autonomic*` threads (asserted in a unit test and a 1-node dtest); `enabled: true` creates exactly three; `DatabaseDescriptorRefTest` passes; yaml with the section in 10.1 parses | sonnet |
| T2.2 | Core types and interfaces of 3.3 and 3.4; `ObservationStore` ring buffers with caps; `KnowledgeView` implementation; control loop scheduling with per-component failure isolation and circuit breaker (7.5 rows 1 to 3) | `autonomic/monitor/*`, `knowledge/ObservationStore.java`, `KnowledgeView.java`, `analyze/*`, `plan/*`, `execute/` types | T2.1 | fake-clock unit tests: sensor cadence respects `minIntervalMillis`; throwing sensor is disabled after threshold and others keep running; `max_series` eviction; detectors do not run before armed | sonnet |
| T2.3 | Local knowledge store: `AutonomicKeyspace` with the tables of 5.1, `KnowledgeStore` DAO | `autonomic/knowledge/AutonomicKeyspace.java`, `KnowledgeStore.java`; H6, H7 | T2.1 | keyspace exists on a fresh node and after restart; round-trip of diagnosis, action, event, local_state rows; rows carry the TTL; write failure is counted and does not throw into the loop | sonnet |
| T2.4 | Virtual tables of 5.3 | `autonomic/knowledge/AutonomicVirtualTables.java`; H8 | T2.2, T2.3 (ledger table stubbed until T2.8) | each table selectable with the layer disabled (empty or status only) and enabled; unit test per table following existing virtual table tests | haiku |
| T2.5 | Sensors over existing metrics: table, client request, thread pool, compaction, disk, GC, peers (gossip load, snitch scores), hot-partition sampling with the yield rule of 5.4; test-only `InjectedSensor` | `autonomic/monitor/sensors/*` | T2.2 | unit test per sensor emits expected signal ids against a `CQLTester` node; sampling sensor skips when a sampler is active and never throws on "already in progress"; no sensor calls a resetting API | sonnet |
| T2.6 | `SafetyGovernor` (7.1), `ActionExecutor` state machine (7.2), `MetricVerifier`, dead-man deadline, restart recovery (7.6), pause/kill/revert, abstract actuator contract test | `autonomic/safety/*`, `execute/ActionExecutor.java`, `MetricVerifier.java`, `ActionRecord.java`; test `AbstractActuatorContractTest` | T2.2, T2.3 | one unit test per governor check; every edge of the state machine reached with a fake actuator; kill rolls back in-flight actions within one `analyze_interval`; control thread blocked in test still yields rollback at the deadline; restart with an `inflight/*` row yields `ROLLED_BACK(RESTART)`; persisted pause survives restart | fable |
| T2.7 | `nodetool autonomic` command group and `NodeProbe` proxy | `src/java/org/apache/cassandra/tools/nodetool/Autonomic.java`; H10, H11, H15 | T2.1, T2.6 | `NodetoolHelpCommandsOutputTest` passes with new fixtures; 1-node dtest runs `status`, `pause`, `resume`, `kill`, `history`, `dryrun` | haiku |
| T2.8 | Cluster ledger: `ClusterLedger`, `ClusterLedgerValue`, the four transformations and serializers, `AutonomicTransformations.register()`, `ClusterCoordinator` (propose, advance, release, takeover, `ChangeListener` dispatch), `ClusterActuator` interface, `nodetool autonomic cluster` | `autonomic/coord/*`, `execute/ClusterActuator.java`; H9 | T2.6 | unit: each transformation's accept and reject cases, idempotent re-apply, serializer round-trip, value object never mutated in place; dtest 2 nodes: concurrent claims produce one owner; expired lease is taken over and `recover` runs; node restart replays a log containing ledger entries; a node with `enabled: false` replays them too; nothing is committed before `cluster enable` | fable |
| T2.9 | `AutonomicMetrics`, `AutonomicEvent` diagnostic events | `autonomic/AutonomicMetrics.java`, `AutonomicEvent.java`; H12 | T2.2, T2.6 | metrics of 10.3 registered and incremented in unit tests; events published on transitions | haiku |
| T2.10 | D2 integration test and hook budget check | dtest `AutonomicFrameworkTest`; script listing files changed outside `autonomic/` against the pinned commit | all above | the three D2 acceptance criteria in `docs/deliverables/D2-framework.md`; the script output equals the table in 4.1 | sonnet |

### D3 Self-diagnosis

| ID | Description | Files | Depends | Acceptance | Tier |
|----|-------------|-------|---------|-----------|------|
| T3.1 | `ThresholdDetector` base with hysteresis and persistence (7.4) | `autonomic/analyze/ThresholdDetector.java` | T2.2 | unit: opens only after `for_evaluations`, clears only below clear threshold, flapping input never opens | sonnet |
| T3.2 | Detectors: `hot_partition` (share of table operations from sampler), `latency_slo_breach` (per table and node), `tombstone_overload` | `autonomic/analyze/detectors/*` | T3.1, T2.5 | triggering and non-triggering fixture per detector; diagnosis names the partition key and carries evidence values | sonnet |
| T3.3 | Detectors: `compaction_backlog`, `disk_pressure`, `gc_pressure`, `pool_saturation` | same | T3.1, T2.5 | as above | sonnet |
| T3.4 | Detectors: `node_imbalance` (gossip load), `degraded_peer` and `degraded_self` (snitch scores against own latency) | same | T3.1, T2.5 | unit with synthetic peer signals; documented limitation that these use gossip-age data | sonnet |
| T3.5 | Rule-based `Correlator`: a fixed table of cause/effect pairs (for example `compaction_backlog` causes `latency_slo_breach` on the same table; `gc_pressure` causes node-wide latency), suppression of derived diagnoses, operator-readable summaries | `autonomic/analyze/RuleCorrelator.java` | T3.2, T3.3 | unit: given both diagnoses, the effect carries `rootCause` and policies receive only the root; summary strings reviewed against a table in the test | sonnet |
| T3.6 | Diagnosis persistence and dtest | dtest `HotPartitionDiagnosisTest` | T3.2, T2.3, T2.7 | D3 acceptance: injected hot partition yields a named diagnosis in `system_autonomic.diagnoses` and `nodetool autonomic history --diagnoses` | sonnet |

### D4 Self-healing

| ID | Description | Files | Depends | Acceptance | Tier |
|----|-------------|-------|---------|-----------|------|
| T4.1 | `KnobActuator` base: bounded numeric knob with envelope, step limit, undo capture, override detection, direction lock; `baseline_drift` detector; `revert` implementation. Concrete: compaction throughput, concurrent compactors | `autonomic/execute/actuators/KnobActuator.java` and subclasses | T2.6 | contract test passes per knob; envelope and step clamp unit-tested; `revert` restores yaml values | sonnet |
| T4.2 | Knobs: cache capacities, stream and hint throttles, advertised severity. Irreversible: targeted garbage-collecting compaction via `StorageService.garbageCollect` or `forceKeyspaceCompactionForPartitionKey`, under rule 10 of 7.3 | same package | T4.1 | contract tests; irreversible actuator rejected below CRITICAL and beyond one per target per day | sonnet |
| T4.3 | Policies wiring diagnoses to actions with `VerifySpec`: `compaction_backlog`, `tombstone_overload`, `gc_pressure`, `degraded_self` | `autonomic/plan/policies/*` | T3.5, T4.1, T4.2 | unit: each policy returns the expected action and verify spec, and nothing when the knob is at its envelope limit; 1-node dtest: injected regression produces `ROLLED_BACK` with audit rows | sonnet |
| T4.4 | Auto-repair integration (replaces "automatic repair scheduling"): detector `repair_overdue` from `AutoRepairUtils.getAutoRepairHistory`; `MAINTENANCE`-lane cluster actuator that calls `AutoRepairService.instance.setRepairPriorityForHosts` or `AutoRepairUtils.setForceRepair`; diagnosis only (no action) when auto-repair is disabled in configuration | `autonomic/execute/actuators/RepairPriorityActuator.java`, detector | T2.8, T3.1 | 2-node dtest: priority is set at most once per rate window and only with a claim; with auto-repair disabled only a diagnosis appears | sonnet |
| T4.5 | D4 acceptance dtests | dtests | T4.3, T4.4 | the three D4 acceptance criteria | sonnet |

### D5 Elasticity

| ID | Description | Files | Depends | Acceptance | Tier |
|----|-------------|-------|---------|-----------|------|
| T5.1 | Request pool elasticity: `request_pools` knob actuator over `StorageService.setConcurrency`; policy on `pool_saturation` | actuators, policies | T4.1, T3.3 | 1-node dtest with injected `pendingTasks`: sizes move, stay in envelope, and return on recovery | sonnet |
| T5.2 | Adaptive throttling: AIMD controller over the native transport rate limiter; policy on node-wide `latency_slo_breach` at CRITICAL with queue growth | `autonomic/execute/actuators/RequestThrottleActuator.java`, policy | T4.1, T3.2 | unit: AIMD sequence; dtest: limit enabled and lowered on breach, raised and disabled on recovery, never below floor | sonnet |
| T5.3 | Background-yield policy: fixed response order of 8.1 implemented as policy priority | policies | T4.3, T5.1, T5.2 | unit: given simultaneous diagnoses, actions are proposed in the documented order, one at a time | sonnet |
| T5.4 | Scale-out: `ScaleOutProvider` SPI, `NoopProvider`, `ScaleOutActuator` phases of 8.3 with recover, `capacity_exhausted` detector (sustained saturation with all in-node knobs at their limits, or disk trend) | `autonomic/elastic/*`, actuator, detector | T2.8, T5.3 | unit with fake provider and metadata: every phase, each timeout, owner loss in each phase; manual approval required by default | fable |
| T5.5 | `SimulatedProvider` and scale-out dtest. Kubernetes: recommend replacing the "stub with unit test" by a documented example class in the operator guide; keep only if the owner confirms (question 5) | test tree | T5.4 | 3-node (or 2-node fallback) dtest: approved action adds one node, ledger shows phases, claim released | sonnet |

### D6 Adaptive schema

The refined D6 task list (T6.1 to T6.16), with files, dependencies, acceptance criteria and tiers, is in section 9 of [`adaptive-schema.md`](adaptive-schema.md). It replaces the outline that stood here. Changes that design requires to this framework (FC1 to FC10) are listed in its section 9.1 and are not yet folded into the sections above.

### D7 Verification

| ID | Description | Files | Depends | Acceptance | Tier |
|----|-------------|-------|---------|-----------|------|
| T7.1 | Scenario dtests: hot spot, load spike, degraded node; each asserts diagnosis, action, verdict | dtests | D3 to D6 | each under 5 minutes within ADR-003 limits | sonnet |
| T7.2 | Fault injection of the layer: throwing sensor, unwritable store, CMS unreachable during claim, owner killed mid-action, control thread stalled, repeated regressions tripping the breaker | dtests | D2 to D5 | each failure converges to the fail-static behaviour in 7.5 | fable |
| T7.3 | cassandra-stress profiles for the three scenarios | `tools/stress` profile yaml only | T7.1 | profiles run against a single node | haiku |
| T7.4 | Operator guide: configuration, gates, kill switch, `nodetool autonomic`, rollback, sampler contention, one-way cluster enable, folding committed values into yaml | `docs/` | all | each step executed against a running node | sonnet |

### Changes to the previous plan

- D2 gains the cluster ledger (T2.8). It was implicit in "cluster-wide coordination" and is a prerequisite of T4.4, T5.4 and D6.
- D4 "automatic repair scheduling" becomes auto-repair integration (T4.4); the scheduler already exists.
- D5 scale-in is excluded; the Kubernetes stub is recommended for removal.
- D6 depends on T2.8 specifically, not only on "D2".
- D7 gains fault injection of the autonomic layer itself (T7.2).
