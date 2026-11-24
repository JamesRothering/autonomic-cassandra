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

# Failure-mode catalogue (postmortem R8)

- **Status:** proposed, 2026-11-24, for the owner to rule on each row. Revision 2: answers the orchestrator's read and the independent review of revision 1.
- **Commit:** branch `deliverable/D7.1` at `5ebbd97`. The only change since `98f21ef` is `ASSESS.md` (`git diff --stat 98f21ef 5ebbd97`: 1 file, 52 insertions); `src/` and `conf/` are identical, so every `file:line` below holds at both commits.
- **Inputs:** postmortem [`docs/postmortems/2026-11-22-cache-and-memory-near-miss.md`](../postmortems/2026-11-22-cache-and-memory-near-miss.md) (R8), audit [`docs/audits/observability-coverage.md`](../audits/observability-coverage.md), plan [`docs/NEXT-STEPS.md`](../NEXT-STEPS.md), standing prompt [`docs/process/orchestration-prompt.md`](../process/orchestration-prompt.md).

## Purpose

This is the source of truth for what the autonomic layer must handle. It starts from **what can go wrong** with a Cassandra node and cluster, not from what Cassandra already exposes. For each failure mode it records:

- what goes wrong and what it does to the node or the cluster;
- the observations that **detect** it;
- the observations that **tell it apart** from look-alikes (a GC pause from a load spike from a slow disk);
- the **physical facts** the detection or the action relies on (what a counter measures, on or off the heap, settable at runtime or not);
- what the layer does **today**, with `file:line`;
- a **status** and a **proposed disposition**. The owner decides the disposition; this document only proposes.

The audit is an input. Rows it did not consider are marked **new** in the Refs column.

## How to read it

Paths: `A/` is `src/java/org/apache/cassandra/autonomic/`, `S/` is `A/monitor/sensors/`, `C/` is `src/java/org/apache/cassandra/`. `file:line` refers to `5ebbd97` (identical to `98f21ef` for `src/` and `conf/`). The audit's line numbers refer to an older commit; where they moved (for example `S/Signals.java`, where the cache signals were inserted), this document gives the current line.

Terms used below:

- **Signal:** a named time series a sensor emits (listed in `S/Signals.java`).
- **Diagnosis:** a named condition a detector opens, such as `gc_pressure`.
- **Correlator:** `A/analyze/RuleCorrelator.java`. It marks a diagnosis as the *effect* of another (its *root cause*). Policies only receive diagnoses without a root cause (`A/AutonomicService.java:646`).
- **Ladder:** the fixed load response in `A/plan/policies/LoadSpikePolicy.java`: yield background work, advertise severity, resize request pools, shed load, propose scale-out.
- **Verify:** every node action is measured over a window; only an `IMPROVED` verdict commits, anything else rolls back (overview 7.3, ADR-008).
- **Audit IDs** (`CPU-1`, `MEM-7`, ...), **RC-n** (design-versus-built gaps, audit 4.1) and **P-n** (physical-fact assumptions, audit 5) refer to the audit.

### Legend

| Status | Meaning |
|---|---|
| **covered** | Detected with a real input, and the proposed disposition is in place. |
| **partial** | Some of it is seen or done, but a needed input, distinction or action is missing. |
| **gap** | Nothing in the layer detects it. |
| **wrong** | The layer acts on it, or would, on a wrong input or a wrong physical assumption. |

| Disposition | Meaning |
|---|---|
| **act: X** | The layer should take action X (an existing or a new actuator). |
| **diagnose only** | The layer should name it, with evidence, and take no action. It may still prevent wrong actions (a correlator rule or a guard). |
| **out of scope** | The layer neither observes nor acts, for the reason given. The owner must confirm. |
| **alias of X** | The same failure mode as row X, seen from one path. Kept so references resolve; not counted. |

Labels: **unverified** means not checked here. **assumption** means reasoned without a check. **measured** means checked on this machine (Linux 6.8, cgroup v2, 4 cores, 5 GB RAM) on 2026-11-24.

### Keeping it current

- Every sensor, detector, policy, guard and actuator cites the rows it serves (in its javadoc and in its task brief).
- A row that nothing serves is a gap. It is listed in the coverage summary, never left silent.
- A new failure mode gets a row before a task is written for it.
- Each deliverable gate updates the Status and Today columns of the rows it claims (standing prompt 6.3).

### Audit facts spot-checked for this document

Checked again against the current code: P2 (`C/service/GCInspector.java:204-231` classifies the collectors), MEM-8 / P11 (`C/cache/ChunkCache.java:311-314`, `setCapacity` throws), MT-2 (`C/utils/memory/MemtablePool.java:52`, `blockedOnAllocating`), P4 (`C/service/StorageService.java:1773-1778`, a negative core size is skipped), P5 (`C/locator/DynamicEndpointSnitch.java:366-369`, `addSeverity` replaces the local state). All hold. Also re-run: the audit's claim that client timeouts, unavailables, failures, `table.sstables_per_read_p99`, `table.pending_compactions`, `table.live_sstable_count`, `table.pending_flushes` and `table.max_partition_size_bytes` are read by nothing outside `S/`: still true (grep of `A/` for each constant, 2026-11-24).

### Physical facts used by several rows (revision 2)

Two defaults matter: the one in `C/config/Config.java` (used when a setting is absent) and the one in the shipped `conf/cassandra.yaml`. Rows state both where they differ.

| Fact | Evidence |
|---|---|
| `disk_failure_policy`: `ignore` in `Config.java:134`, but `stop` in the shipped `conf/cassandra.yaml:502` | code |
| Under `stop`, `die` or `stop_paranoid`, an `FSError` calls `StorageService.stopTransports()` (`C/service/DefaultDiskErrorsHandler.java:85-90`), which stops the native transport and gossip (`C/service/StorageService.java:645-656`). The JVM keeps running | code |
| A `CorruptSSTableException` stops transports only under `die` or `stop_paranoid` (`DefaultDiskErrorsHandler.java:61-75`); under the shipped `stop` nothing beyond the log happens | code |
| `DisallowedDirectories` is filled only under `best_effort` (`DefaultDiskErrorsHandler.java:92-112`, the only callers of `maybeMarkUnwritable` / `maybeMarkUnreadable`) | grep of `C/` |
| `commit_failure_policy: stop` in both (`Config.java:135`, `conf/cassandra.yaml:519`); under `stop` a commit error also calls `stopTransports()` (`DefaultDiskErrorsHandler.java:147-149`) | code |
| No Cassandra counter of disk errors exists: `StorageMetrics.uncaughtExceptions` (`C/metrics/StorageMetrics.java:47`) counts only throwables passed to `JVMStabilityInspector.uncaughtException` (`C/utils/JVMStabilityInspector.java:73`). The disk error handler is pluggable (`C/service/DiskErrorsHandlerService.java:39, 76-82`), so a counting wrapper is possible | code |
| Key cache default: min(5% of heap, 100 MiB) (`C/config/DatabaseDescriptor.java:991-992`). Counter cache: min(2.5% of heap, 50 MiB) (`:1010-1011`). Both on heap | code |
| Row cache default 0 MiB, so off (`Config.java:580`) | code |
| Chunk cache off unless `-Dcassandra.file_cache_enabled=true` (`Config.java:612`, `C/config/CassandraRelevantProperties.java:278`; `ChunkCache.instance` is null when disabled, `C/cache/ChunkCache.java:52-53`) | code |
| Consequence: with defaults, the caches the layer can resize hold at most about 150 MiB of heap. Cache steps have a **small effect** on heap pressure; memtables are on heap by default (`Config.java:637`) and are the larger lever (FM-JVM-05) | derived from the rows above |
| `bytesCompacted` grows only when a compaction **finishes**, by its whole size (`C/db/compaction/ActiveCompactions.java:49-52`). `compaction.bytes_per_sec` (`S/CompactionSensor.java:66`) is therefore lumpy: zero during a long compaction, a spike at its end. Progress while running is `CompactionInfo.getCompleted()` (`C/db/compaction/CompactionInfo.java:124`) over `ActiveCompactions.getCompactions()` (`:38`) | code |
| Heap OOM: `JVMStabilityInspector` removes the shutdown hook and rethrows (`C/utils/JVMStabilityInspector.java:127-135`); the kill comes from `-XX:OnOutOfMemoryError=kill -9 %p` (`conf/cassandra-env.sh:225`). Heap dump on OOM (`conf/jvm-server.options:141`) goes to `CASSANDRA_HEAPDUMP_DIR`, which defaults to the log directory (`conf/cassandra-env.sh:206-208`) | code |
| Other unstable throwables ("Too many open files" `:166-169`; `FSError`/`CorruptSSTableException` under `die` `:162-164`) call `killCurrentJVM` (`:171-175`), which ends in `System.exit(100)` (`:304`) | code |
| Read routing: the dynamic snitch adds the advertised severity, unweighted, to each replica's latency score (`C/locator/DynamicEndpointSnitch.java:303-304`). `StorageProxy.pickReplica` (`C/service/StorageProxy.java:1993-1998`, "severity 0 is healthy") is called only for remote-DC write forwarding (`:1960-1968`) | code, grep |
| Internode latency is receive time minus the **sender's** creation timestamp when `internode_timeout: true` (default, `Config.java:182`; `C/net/Message.java:1088-1099`; `C/net/InboundMessageHandler.java:130-132`). Only positive values are recorded (`C/metrics/MessagingMetrics.java:68-74`), per DC, not per peer (`:115-119`) | code |
| PSI per cgroup: `cpu.pressure`, `io.pressure` and `memory.pressure` exist in this session's cgroup (`/sys/fs/cgroup/user.slice/user-1000.slice/session-1.scope/`). The `io` controller is **not** enabled (`cgroup.subtree_control` of the root: `cpu memory pids`), so `io.max` and `io.stat` are absent here | measured |
| `/proc/self/io` (`read_bytes`, `write_bytes`), `/proc/net/netstat` `TcpExt: ListenOverflows, ListenDrops` exist | measured |

---

## 1. CPU and scheduler (FM-CPU)

| ID | Failure mode | Detects it | Tells it from look-alikes | Physical facts | Today | Status | Proposed disposition | Refs |
|---|---|---|---|---|---|---|---|---|
| FM-CPU-01 | CPU saturation: runnable threads exceed cores; every request type slows, queues grow | `/proc/stat` user+system; the cgroup's own `cpu.pressure` `some` (measured, present), host `/proc/pressure/cpu` only as fallback; process CPU (`com.sun.management.OperatingSystemMXBean.getProcessCpuLoad`) | High utilisation with low iowait, no steal, no throttling (vs CPU-02..04); GC pause fraction low (vs JVM-01). **Background vs client CPU:** CPU taken by compaction, streaming or repair threads vs request threads needs per-pool thread CPU (`ThreadMXBean.getThreadCpuTime`, JDK API, grouped by pool thread names; cost as FM-CPU-05); background-dominated saturation goes to yield, client-dominated to the ladder | Load average counts tasks in state R **or D** (`proc_loadavg(5)`); `getAvailableProcessors` follows the container quota but the load average is host-wide | `S/CpuSensor.java:62` load average only; pool gate `A/plan/policies/RequestPoolPolicy.java:60, 122, 159` | wrong | act: gate pool growth on utilisation and run queue; prefer shed_load when CPU-bound | CPU-1, CPU-4, P3, T7.18 |
| FM-CPU-02 | Steal time: hypervisor gives the CPU to another guest; latency with no local cause | `/proc/stat` steal column (measured, present) | Steal fraction high while process CPU is normal | Steal is only non-zero on virtualised hosts | none | gap | act: name as cause of `degraded_self`; allow only the advertise_severity rung (note 1) | CPU-3, T7.18 |
| FM-CPU-03 | cgroup CPU throttling: the container hits its quota each period; latency spikes that look like load | cgroup v2 `cpu.max`, `cpu.stat` `nr_throttled`, `throttled_usec` (measured, present); cgroup `cpu.pressure` (measured, present) | Throttled time rising while host CPU is idle | Quota is per period (default 100 ms); more threads make throttling worse | none | gap | act: treat as CPU saturation: no pool growth; shed_load if latency breaches | CPU-5, T7.18 |
| FM-CPU-04 | I/O wait taken for CPU demand: disk-bound node reads as CPU-saturated | `/proc/stat` iowait; the cgroup's `io.pressure` (measured, present), host `/proc/pressure/io` as fallback | iowait or PSI io high, user+system low | Same as CPU-01: load average includes D state | `S/CpuSensor.java:62`; pool step skipped, wrong rung tried | wrong | act: exclude iowait from the pool gate; route to the disk rows (FM-DSK-05) | CPU-2, P3, T7.18 |
| FM-CPU-05 | One thread pinned at 100% (a single compaction, one Netty event loop, GC thread) while total CPU looks fine | `ThreadMXBean.getThreadCpuTime` per thread (JDK API) | Total utilisation low, one thread near one core | **unverified** cost of per-thread CPU sampling over hundreds of threads | none | gap | diagnose only | **new** |
| FM-CPU-06 | CPU frequency scaling or thermal throttling | `/sys/devices/system/cpu/cpu*/cpufreq/` (**unverified** presence in containers and VMs) | Utilisation unchanged, throughput lower | **assumption**: rarely visible from inside a VM or container | none | gap | out of scope: not reliably observable from a guest | **new** |

Notes:

1. On a host with steal, pool growth and background yielding cannot help: the CPU is not this node's to use. Advertising severity moves reads to other replicas, which can.

## 2. Process, OS and container memory (FM-MEM)

| ID | Failure mode | Detects it | Tells it from look-alikes | Physical facts | Today | Status | Proposed disposition | Refs |
|---|---|---|---|---|---|---|---|---|
| FM-MEM-01 | Container memory limit reached: the kernel OOM-kills the JVM; the node vanishes with no Java error | cgroup v2 `memory.max`, `memory.events` `oom_kill`, `high`, `memory.stat` `anon`; the cgroup's `memory.pressure` (measured, present; preferred over host `/proc/pressure/memory`); `/proc/self/status` `VmRSS` (all measured, present) | Heap after GC normal while RSS or `anon` grows: native or off-heap, not heap (vs JVM-04) | `memory.current` **includes page cache** (measured: 1.56 GB current = 0.35 GB anon + 1.12 GB file), so headroom must use `anon` and `memory.events`, not `memory.current` alone. Row cache is 0 MiB by default (`Config.java:580`) and the chunk cache off by default (`Config.java:612`), so with defaults no off-heap cache exists to shrink | none | gap | act: guard every memory-growing action (cache UP) on headroom; shrink the row cache near the limit only when it is configured (small or no effect with defaults) | MEM-13, MEM-16, T7.16 |
| FM-MEM-02 | Host memory exhausted (no cgroup limit): host OOM killer, or heavy reclaim | `/proc/meminfo` `MemAvailable`; the cgroup's `memory.pressure` first, host `/proc/pressure/memory` second (both measured, present) | As MEM-01, at host level | `MemAvailable` already counts reclaimable page cache | none | gap | act: same guards as MEM-01 | MEM-14, T7.16 |
| FM-MEM-03 | Swapping: JVM pages swapped out; GC and requests stall on page faults | `/proc/self/status` `VmSwap`; `/proc/vmstat` `pswpin`, `pgmajfault` (measured) | GC pauses long while GC CPU is low; major faults rise (vs JVM-01) | No in-JVM action fixes swapping; swap was 4 GB on this host (measured `SwapTotal`) | none | gap | diagnose only; correlator: cause of `gc_pressure` and `degraded_self`, so cache steps are not tried | MEM-15, T7.16 |
| FM-MEM-04 | Page cache starved: heap, off-heap or caches grew; sstable reads go to disk | `/proc/meminfo` `Cached`; device read IOPS (FM-DSK-05); chunk cache hit rate | Page cache shrinking while workload is steady (vs `cache_miss_pressure` from a workload change) | Reads of uncached sstable data rely on the OS page cache (`disk_access_mode` `mmap_index_only`, `C/config/Config.java:132`) | none | gap | act: guard cache UP on available memory, not only on heap | MEM-14, T7.16 |
| FM-MEM-05 | Off-heap structures grow with data: bloom filters, index summaries, compression metadata | `BloomFilterMetrics`, `IndexSummaryMetrics`, `TableMetrics.compressionMetadataOffHeapMemoryUsed` (audit MEM-10..12) | Growth tracks sstable count and data size (vs a leak, MEM-06) | Off-heap; bounded only by data and schema (`bloom_filter_fp_chance`) | none | gap | diagnose only: the fix is a schema change, out of MVP (overview 1.3) | MEM-10, MEM-11, MEM-12 |
| FM-MEM-06 | Native memory leak or JVM native growth (thread stacks, GC structures, native libraries) | RSS minus heap committed minus known off-heap pools; NMT via `DiagnosticCommand` `vmNativeMemory` only with `-XX:NativeMemoryTracking` | RSS grows with no matching pool | NMT is not enabled in `conf/jvm-server.options`; its cost is **unverified** | none | gap | diagnose only (estimate from RSS minus known pools) | MEM-17, T7.16 |
| FM-MEM-07 | Transparent huge pages or memory compaction stalls | `/sys/kernel/mm/transparent_hugepage/enabled` (measured: `madvise`); `/proc/vmstat` `compact_stall` | Stalls with no GC, no swap | **unverified** whether `compact_stall` is a usable rate signal | none | gap | out of scope: host configuration, checked once at start at most | **new** |

## 3. JVM (FM-JVM)

| ID | Failure mode | Detects it | Tells it from look-alikes | Physical facts | Today | Status | Proposed disposition | Refs |
|---|---|---|---|---|---|---|---|---|
| FM-JVM-01 | GC pause pressure under G1: young, mixed or full pauses stop requests | Stop-the-world time per collector bean; GC notifications with `GcInfo` (as `C/service/GCInspector.java:270-293`) | Pause fraction high while op rate is flat (vs CLI-01); no major faults (vs MEM-03). **Steal (CPU-02) and cgroup throttling (CPU-03) lengthen pauses** because GC threads lose the CPU: correlator rules make CPU-02 and CPU-03 the root cause of `gc_pressure` when steal or throttled time rises in the same window | JDK 17 default is G1 (`conf/jvm17-server.options:33`); "G1 Young Generation" is a pause; "G1 Old Generation" reports full GCs, also pauses (**assumption**, note 1). Key cache ≤ 100 MiB and counter cache ≤ 50 MiB by default (`C/config/DatabaseDescriptor.java:991-992, 1010-1011`); row cache 0 MiB and off heap (`Config.java:578-580`) | `S/GcSensor.java:71-81` sums all beans; `A/analyze/detectors/GcPressureDetector.java:40`; `A/plan/policies/GcPressurePolicy.java:43` | partial | act: name the heap consumer first and, when it is memtables, flush the largest (JVM-05, primary action); shrink on-heap caches (key, counter) second, **small effect** with defaults (at most about 150 MiB) | MEM-3, P2, P10, T7.17 |
| FM-JVM-02 | ZGC concurrent cycles counted as GC time: `gc_pressure` opens with no pause pressure | Per-collector beans: "ZGC Pauses" vs "ZGC Cycles" | Cycle time high, pause time near zero | JDK 21 options select generational ZGC (`conf/jvm21-server.options:31`); measured on JDK 17 ZGC: 3330 ms cycles vs 2 ms pauses (audit P2) | `S/GcSensor.java:75` adds every bean | wrong | act: pause-only signal; cycle time as its own signal | MEM-3, P2, T7.17 |
| FM-JVM-03 | ZGC allocation stall: allocating threads wait for a cycle to free memory; requests stall with no "pause" | JFR `jdk.ZAllocationStall` (**unverified** name on JDK 21); cycle-time fraction near 1 with heap after GC near max | Concurrent cycles back to back plus high occupancy (vs JVM-02, where heap is fine) | **assumption**: stalls are reported by no `GarbageCollectorMXBean` | none | gap | act: treat as heap pressure (JVM-04 actions) | **new**, T7.17 |
| FM-JVM-04 | Live set too large or old-gen leak: heap after GC rises towards max; ends in OOM | `MemoryPoolMXBean.getCollectionUsage()` of the old pool (audit: supported on G1 and ZGC pools, measured) | After-GC occupancy trend, not the sawtooth of used heap | `gc.heap_used_fraction` includes uncollected garbage (`S/GcSensor.java:87-90`) | evidence only, `A/analyze/detectors/GcPressureDetector.java:70` | gap | act: name memtables vs caches vs other first; flush memtables when they are the consumer (JVM-05, primary); shrink on-heap caches second (small effect with defaults) | MEM-1, MEM-2, T7.16 |
| FM-JVM-05 | Memtables are the heap consumer, not caches | `MemtablePool` on-heap `used()`, `limit` (`C/utils/memory/MemtablePool.java:204-226`, audit MEM-7) | Memtable share of the heap is large (vs caches, JVM-01) | Memtables are on heap by default (`memtable_allocation_type: heap_buffers`, `C/config/Config.java:637`) | none | gap | act: **primary heap action**: diagnose the memtable share first, then flush the largest memtable (new actuator, owner decision); cache steps follow only if caches are the consumer | MEM-7, P10, T7.16, T7.30 |
| FM-JVM-06 | Metaspace or code cache exhausted | `MemoryPoolMXBean` "Metaspace", "CodeHeap ..." (audit, measured) | Grows without heap growth | Rare in a steady server | none | gap | diagnose only | MEM-4 |
| FM-JVM-07 | Direct memory exhausted: "Direct buffer memory" OOM fails reads and writes | `BufferPoolMXBean` "direct"; `C/metrics/BufferPoolMetrics.java`; limit from `-XX:MaxDirectMemorySize` input argument | Heap fine, direct pool at its limit | The chunk cache is off-heap and **not resizable** (`C/cache/ChunkCache.java:311-314`), and **off by default** (`Config.java:612`, `C/cache/ChunkCache.java:52-53`), so with defaults the direct pools are network and I/O buffers; a direct OOM is turned into a heap OOM (`C/utils/JVMStabilityInspector.java:201-209`) and the JVM is then killed by `-XX:OnOutOfMemoryError` (`conf/cassandra-env.sh:225`) | none | gap | diagnose only (no runtime knob shrinks the direct pools) | MEM-5, P11, T7.16 |
| FM-JVM-08 | Safepoint stalls: time to safepoint or non-GC safepoints stop all threads | JFR `jdk.SafepointBegin/End` through `RecordingStream` | Stall with no GC pause recorded | JFR streaming cost **unverified** | none | gap | owner decision: diagnose only after a measured JFR cost, else out of scope | JVM-1 |
| FM-JVM-09 | Heap OOM: the JVM is killed | Precursors only: JVM-04 trend; GC time rising | n/a | `JVMStabilityInspector` does not kill on OOM: it removes the shutdown hook and rethrows, "letting the JVM handle the error" (`C/utils/JVMStabilityInspector.java:127-135`). The kill is `-XX:OnOutOfMemoryError=kill -9 %p` (`conf/cassandra-env.sh:225`); a heap dump is written first (`conf/jvm-server.options:141`) into the log directory by default (`conf/cassandra-env.sh:206-208`; see FM-DSK-09) | none | gap | out of scope: the layer dies with the process; its precursor is acted on in JVM-04 | **new** |
| FM-JVM-10 | G1 humongous allocations from large cells, partitions or mutations | GC log or JFR only (**unverified**) | Correlates with large partitions (RD-03) and large mutations (WRT-06) | **assumption**: no MXBean counts humongous allocations | none | gap | out of scope as its own signal; covered through RD-03 and WRT-06 | **new** |
| FM-JVM-11 | Thread deadlock: two or more threads wait on each other's locks; the stages they serve stop, queues grow, the process looks alive | `ThreadMXBean.findDeadlockedThreads()` (JDK API; nothing in `C/` calls it, grep 2026-11-24) | Pool active count at max with zero completed tasks and deadlocked threads reported (vs saturation, TP-01, where tasks still complete) | Detects monitor and `java.util.concurrent` ownable-synchronizer deadlocks only (JDK API documentation), not livelocks or waits on I/O; cost on hundreds of threads **unverified** | none | gap | diagnose only (CRITICAL, with the thread stacks in the evidence); no automatic restart | **new**, T7.35 |

Notes:

1. `GCInspector` marks "G1 Old Generation" as "partially concurrent" (`C/service/GCInspector.java:223`). In that class this only means "the notification's duration is not stopped time, use the bean's time delta instead" (`:288-293`). It does not mean the collector never pauses. A pause-only signal (T7.17) must not drop the bean because of that label. Whether JDK 17 G1 counts anything other than full GCs under that bean, and whether JDK 20+ adds a "G1 Concurrent GC" bean, are **unverified** (audit 7).

## 4. Disk (FM-DSK)

| ID | Failure mode | Detects it | Tells it from look-alikes | Physical facts | Today | Status | Proposed disposition | Refs |
|---|---|---|---|---|---|---|---|---|
| FM-DSK-01 | Data volume full: writes, flushes and compactions fail | `FileStore` usable/total of each data directory | Which directory; snapshots vs live data (DAT-02) | `getUsableSpace` honours reserved blocks; `localDataDiskUsage` guardrail can also reject writes (`C/db/guardrails/Guardrails.java:552`) | `S/DiskSensor.java:71-81`, but `:75-81` **sums** total and usable space over all data directories, so one full JBOD directory is hidden by the others; `A/analyze/detectors/DiskPressureDetector.java:38` (0.85); trend in `CapacityExhaustedDetector` | partial | act: per-directory usage (sensor fix); snapshot TTL first (DAT-02); scale-out on trend (exists); see AUT-07 | DSK-1, DSK-4 |
| FM-DSK-02 | Commit log volume full: commit errors stop transports under the default `commit_failure_policy: stop` (`C/service/DefaultDiskErrorsHandler.java:147-149`); the node is alive but serves nothing (FM-NODE-01) | `FileStore` of `commitlog_directory` (`C/config/Config.java:485`) | Data volume fine | `commit_failure_policy: stop` (`C/config/Config.java:135`); commit log space bounded by `commitlog_total_space` (`:487`) | not read: `S/DiskSensor.java:71` reads data directories only | gap | act: diagnose CRITICAL; flush memtables to recycle segments (new actuator) | DSK-2, T7.19 |
| FM-DSK-03 | Hints volume full: hints are not stored, replicas miss writes | `FileStore` of `hints_directory` (`C/config/Config.java:126`); `HintsServiceMetrics` | Data volume fine | `max_hints_size_per_host` is 0 (unbounded) by default (`C/config/Config.java:554`) | none | gap | diagnose only; feeds CLU-06 | DSK-2, HNT-2, T7.19 |
| FM-DSK-04 | Saved caches volume full: caches are not saved, cold start after restart | `FileStore` of `saved_caches_directory` (`C/config/Config.java:482`) | n/a | Only affects warm-up after a restart | none | gap | diagnose only (low cost once DSK-02 exists) | DSK-2 |
| FM-DSK-05 | Disk saturated: throughput, IOPS or queue at the device limit; reads and writes slow | `/proc/diskstats` per device (in-flight, io ticks, weighted ms), mapped via `/proc/self/mountinfo` (measured, present); the cgroup's `io.pressure` (measured, present), host `/proc/pressure/io` as fallback; `/proc/self/io` `read_bytes`, `write_bytes` for this process's share of the device (measured, present) | Queue and latency high **with throughput at or near the device's own historical ceiling** (vs DSK-06, where throughput is below it); this process's share high (vs a neighbour on a shared device); vs CPU-01 | Device names differ by host (here `sda`, `dm-0`, measured). `%util` (io ticks) means "busy at least once in the interval"; on NVMe and other parallel devices it reaches 100% far below saturation (**assumption**, from the kernel's definition of `io_ticks`), so the gate uses queue depth, latency and throughput, not `%util` | none | gap | act: yield compaction and streams only when the device is the bottleneck; verify by device utilisation | DSK-3, T7.19 |
| FM-DSK-06 | Disk slow without saturation: failing device, cloud volume out of burst credits | `/proc/diskstats` latency per I/O and queue depth; throughput against the device's historical ceiling (learned) | High latency per I/O with throughput **well below** the historical ceiling (vs DSK-05, at the ceiling). Burst-credit exhaustion caps IOPS at a lower baseline, so the queue grows and it looks like saturation at the new cap: only the drop against the historical ceiling separates them | **assumption**: burst-credit exhaustion shows as a lower IOPS cap and higher latency, with no in-guest counter of credits; needs enough history to know the ceiling | none | gap | diagnose only; cause of `degraded_self`; advertise severity | **new**, T7.19 |
| FM-DSK-07 | Disk I/O errors: `FSError` or failed device | Error count: `/sys/block/*/device/ioerr_cnt` (measured, present) and a count of `FSError`s through a wrapping `DiskErrorsHandler` (new; no Cassandra counter exists, see "Physical facts"); **transport state**: `StorageService.isGossipRunning` (`C/service/StorageService.java:591`), `isNativeTransportRunning` (`:624`). `DisallowedDirectories` (`C/db/DisallowedDirectories.java:53-59`) only under `best_effort` | Errors, not latency; transports stopped by the policy vs by an operator (FM-NODE-01) | `disk_failure_policy`: `ignore` in `Config.java:134`, **`stop` in the shipped `conf/cassandra.yaml:502`**. Under `stop` the first `FSError` stops native transport and gossip (`C/service/DefaultDiskErrorsHandler.java:85-90`); peers then see the node down, so advertised severity can no longer be gossiped. Under `ignore` nothing happens; under `best_effort` directories are disallowed (`:92-112`); under `die` the JVM exits (`C/utils/JVMStabilityInspector.java:162-175`) | none | gap | act: diagnose CRITICAL from the error count and transport state; stop all capacity actions (split, scale-out) and node actions on this node (FM-NODE-01) | DF-1, T7.23 |
| FM-DSK-08 | Read-ahead too large: wasted I/O on random reads | `/sys/block/*/queue/read_ahead_kb` | n/a | Checked once at start by Cassandra (`C/service/StartupChecks.java:691-766`) | none | gap | out of scope: configuration, checked at startup by Cassandra | **new** |
| FM-DSK-09 | Log and heap-dump volume full: logging stops or blocks; a heap dump on OOM fills the volume, and if that is the data volume, the next start fails on space | `FileStore` of the log directory and of `CASSANDRA_HEAPDUMP_DIR` (read from the `-XX:HeapDumpPath` input argument); count and size of `*.hprof` files there | Which volume; dump files vs logs | Heap dump on OOM (`conf/jvm-server.options:141`); its directory defaults to the log directory (`conf/cassandra-env.sh:206-208`); a dump is the heap size. Whether logback blocks or drops when its volume is full **unverified** | none | gap | diagnose only (WARN on headroom below one heap size; CRITICAL near full); deleting dumps stays manual | **new**, T7.37 |
| FM-DSK-10 | Inode exhaustion: space left but no inodes; sstable, commit log and hint file creation fails with "No space left on device" | `statvfs(3)` `f_files`, `f_ffree` per volume, through JNA (shipped, `lib/jna-5.13.0.jar`; Cassandra already binds libc in `C/utils/NativeLibraryLinux.java`); measured with `df -i /` here: 15% used | Space free, inodes near zero (vs DSK-01) | **assumption**: the JDK `FileStore` exposes space only, no inode counts; no `statvfs` binding exists in `C/` today (grep 2026-11-24; the "statvfs" in `S/DiskSensor.java:39` is the one `FileStore` makes internally, which returns no inode counts to Java) | none | gap | diagnose only (CRITICAL near the limit); feeds the same capacity guard as DSK-01 | **new**, T7.37 |
| FM-DSK-11 | cgroup I/O throttling: the container hits an `io.max` limit; I/O latency rises with the device idle | cgroup v2 `io.max`, `io.stat`; the cgroup's `io.pressure` | cgroup `io.pressure` high while device queue and latency in `/proc/diskstats` are low (vs DSK-05, DSK-06) | Measured here: the `io` controller is not enabled (root `cgroup.subtree_control` is `cpu memory pids`), so `io.max` is absent and this row cannot be exercised on this machine; `io.pressure` exists regardless | none | gap | diagnose only; correlator: cause of DSK-05 symptoms, so compaction is yielded (it is the same lever) but no device-bound reasoning is applied | **new**, T7.37 |

## 5. OS limits and time (FM-OS)

| ID | Failure mode | Detects it | Tells it from look-alikes | Physical facts | Today | Status | Proposed disposition | Refs |
|---|---|---|---|---|---|---|---|---|
| FM-OS-01 | File descriptors exhausted: sstable opens and connections fail, **and the JVM exits** | `UnixOperatingSystemMXBean.getOpenFileDescriptorCount/getMaxFileDescriptorCount` (audit, measured) | Fraction of limit | A "Too many open files" `FileNotFoundException`, `FileSystemException` or `SocketException` that reaches `JVMStabilityInspector` marks the JVM unstable (`C/utils/JVMStabilityInspector.java:166-169`) and calls `killCurrentJVM` (`:171-175`), which ends in `System.exit(100)` (`:304`). So the outcome is a node loss, not degraded service. A split adds a table and sstables; connection storms add sockets | none | gap | act: diagnose WARN at a fraction of the limit and **CRITICAL** well before it (the next failure kills the node); block capacity-adding actions (split) above the WARN level | FD-1, T7.23, T7.28 |
| FM-OS-02 | Threads or pids exhausted: "unable to create native thread" (an `OutOfMemoryError`) kills the JVM | `ThreadMXBean.getThreadCount`; cgroup `pids.current`, `pids.max` (measured, present); `/proc/self/limits` `Max processes` | Thread count against limit | Growing a request pool adds threads (`A/plan/policies/RequestPoolPolicy.java:159-163`) | none | gap | act: guard pool growth on thread headroom; diagnose near the limit | **new** |
| FM-OS-03 | Memory map areas exhausted (`vm.max_map_count`): mmap fails | `/proc/self/maps` line count vs `/proc/sys/vm/max_map_count` (measured: 1048576 here) | n/a | Checked once at start by Cassandra (`C/service/StartupChecks.java:777-829`); index files are mmapped by default (`C/config/Config.java:132`) | none | gap | diagnose only; reading `/proc/self/maps` cost **unverified** on a large node | **new** |
| FM-OS-04 | Clock skew between nodes: wrong last-write-wins order, early tombstone purge, LWT anomalies | NTP state needs `adjtimex(2)` (not in `/proc`); cross-node estimate from the same internode latency data as FM-NET-04: with `internode_timeout: true` (default, `Config.java:182`) the latency is receive time minus the sender's timestamp (`C/net/Message.java:1088-1099`, `C/net/InboundMessageHandler.java:130-132`), so it is network delay **plus** skew. A persistent offset in one direction that the coordinator's own round-trip times (one clock) do not show is skew (**assumption**: method not tried) | Skew vs a slow peer (NET-04): skew is a constant offset independent of load; delay varies with load | Only positive values are recorded (`C/metrics/MessagingMetrics.java:68-74`): a receiver behind the sender loses samples instead of showing negative latency. Recorded per DC, not per peer (`:115-119`), so a per-peer estimate needs a new hook on the inbound path (**assumption**: one is feasible without a request-path cost, ADR-013). Lease safety margin covers skew for cluster claims (`A/coord/ClusterCoordinator.java:277-278`, ADR-038) | none | gap | owner decision: audit proposed out of scope (not observable in-JVM); this revision recommends diagnose only from the cross-node estimate, which also guards FM-NET-04 against blaming a peer for skew | CLK-1, T7.33 |

## 6. Network (FM-NET)

| ID | Failure mode | Detects it | Tells it from look-alikes | Physical facts | Today | Status | Proposed disposition | Refs |
|---|---|---|---|---|---|---|---|---|
| FM-NET-01 | Packet loss: retransmits slow internode and client traffic | `/proc/net/snmp` `Tcp: RetransSegs`, `OutSegs` (measured, present) | Retransmit ratio up while CPU and disk are fine | System-wide, not per peer | none | gap | diagnose only | NET-2 |
| FM-NET-02 | NIC saturated or erroring | `/proc/net/dev` bytes, errs, drop (measured, present) | Bytes near link speed; errors | Link speed is in `/sys/class/net/*/speed` (**unverified** on VMs) | none | gap | diagnose only | NET-1 |
| FM-NET-03 | Partition: this node cannot reach some peers; they look down to it | `peer.alive` (gossip); outbound connection failures; unavailables | Some peers down to this node but up to others (needs CLU-01 cross-check) | Gossip liveness is this node's view only | `S/PeerSensor.java:90` emits alive; used only to drop dead peers from medians (`A/analyze/detectors/PeerDetector.java:64`) | partial | diagnose only; cluster lanes already wait for all members alive (overview 7.1 check 5) | MSG-4, GSP-1 |
| FM-NET-04 | High internode latency to one peer or DC | `MessagingMetrics` `dcLatency`, `allLatency` (`C/metrics/MessagingMetrics.java:59-60`) | Network vs a slow peer (CLU-02): latency to many peers in one DC vs one peer. **Clock skew mimics a slow peer**: the value is receive time minus the sender's timestamp (`C/net/InboundMessageHandler.java:130-132`, `C/net/Message.java:1088-1099`, with `internode_timeout: true`, `Config.java:182`), so a sender whose clock is behind looks slow; rule it out with the FM-OS-04 estimate | Recorded per DC, not per peer, and only when positive (`C/metrics/MessagingMetrics.java:68-74, 115-119`); with `internode_timeout: false` the creation time is replaced by the local time and the metric reads near zero (`C/net/Message.java:1091-1092`) | none (snitch scores only) | gap | diagnose only; evidence for `degraded_peer` | MSG-2, T7.21 |
| FM-NET-05 | TLS certificate expires: internode or client connections fail at a known time | `MutualTlsMetrics.internodeCertificateExpirationDays`, `clientCertificateExpirationDays` (`C/metrics/MutualTlsMetrics.java:39, 44`) | Predictable from days left | Only populated with mutual TLS (**unverified** otherwise) | none | gap | diagnose only (WARN some days ahead) | **new** |
| FM-NET-06 | TCP listen-queue overflow: the accept backlog of the native or storage port is full; new connections are dropped by the kernel and clients see connect timeouts, not errors | `/proc/net/netstat` `TcpExt: ListenOverflows`, `ListenDrops` (measured, present) | Overflows rising with a connection storm (CLI-02) vs with the accept thread starved (CPU-01, JVM-01) | Counters are host-wide (network namespace), not per port; the backlog is the smaller of the socket's and `net.core.somaxconn` (**assumption**, from `listen(2)`) | none | gap | diagnose only; evidence for CLI-02 | **new**, T7.33 |

## 7. Write path (FM-WRT)

| ID | Failure mode | Detects it | Tells it from look-alikes | Physical facts | Today | Status | Proposed disposition | Refs |
|---|---|---|---|---|---|---|---|---|
| FM-WRT-01 | Commit log sync slow: writes wait for fsync | `CommitLogMetrics.waitingOnCommit` (`C/metrics/CommitLogMetrics.java:46`); commit log device latency (DSK-05) | Write latency up with mutation pool not saturated and CPU free | Default and shipped `commitlog_sync: periodic`, `commitlog_sync_period: 10000ms` (`conf/cassandra.yaml:665-667`). In periodic mode writes are acknowledged without waiting; a write waits only when the last sync is older than `periodic_commitlog_sync_lag_block`, default 1.5 × the period = 15 s (`C/db/commitlog/PeriodicCommitLogService.java:36-45`, `C/config/DatabaseDescriptor.java:4093-4098`). So under defaults `waitingOnCommit` is non-zero only when fsync lags by seconds, and then it is severe. No pool or throttle action speeds a slow fsync | none | gap | diagnose only; correlator: cause of write `latency_slo_breach`, so the ladder does not grow pools | CL-2, T7.21 |
| FM-WRT-02 | Commit log segment allocation waits: commit log space full until memtables flush | `waitingOnSegmentAllocation` (`:44`), `totalCommitLogSize` (`:42`) | Total size at `commitlog_total_space` | Segments are recycled only after the memtables they cover flush | none | gap | act: flush the oldest memtables (new actuator, owner decision) | CL-1, CL-2, T7.21 |
| FM-WRT-03 | Memtable allocation blocked: the memtable pool is full because flushing lags | `MemtablePool.blockedOnAllocating` (`C/utils/memory/MemtablePool.java:52`); `usedRatio()` | Blocked time > 0 (vs WRT-01) | Only more flush capacity (disk) helps | none | gap | act: yield compaction to give flush the disk; diagnose | MT-2, T7.21 |
| FM-WRT-04 | Flush backlog: pending flushes grow | `table.pending_flushes` (`S/TableSensor.java:120`); `MemtableFlushWriter` pool via `pool.*` | Per table vs pool-wide | Counts flushes created and not yet post-flushed (audit P9) | signal emitted, read by nothing; pool seen by `pool_saturation` | partial | diagnose only (feeds WRT-03) | MT-1, MT-3, T7.21 |
| FM-WRT-05 | Write timeouts and failures seen by clients (the write side of FM-CLI-05) | as FM-CLI-05, write columns | as FM-CLI-05 | Timeout is `write_request_timeout` 2 s (`C/config/Config.java:161`) | as FM-CLI-05 | alias | alias of FM-CLI-05 | CLI-2, T7.20 |
| FM-WRT-06 | Oversized mutations or large batches rejected | `CommitLogMetrics.oversizedMutations` (`:50`); `BatchMetrics` | n/a | Client behaviour; the layer cannot fix it | none | gap | diagnose only | CLI-7 |
| FM-WRT-07 | Batchlog backlog: logged batches are not replayed (a replica down, or replay throttled), so the batchlog table grows and atomicity is delayed | `BatchlogManagerMBean.countAllBatches()` (`C/batchlog/BatchlogManagerMBean.java:27`), `getTotalBatchesReplayed()` (`:32`) | Count growing while peers are up (replay stuck) vs while a peer is down (expected, CLU-01) | `countAllBatches` reads the batchlog table; its cost on a large backlog **unverified** | none | gap | diagnose only (feeds CLU-01 evidence) | **new**, T7.29 |
| FM-WRT-08 | Paxos / LWT subsystem degraded: Paxos repair falling behind, uncommitted Paxos state accumulating, linearizability violations detected | `PaxosMetrics.linearizabilityViolations`, `repairPaxosTopologyRetries` (`C/metrics/PaxosMetrics.java:30-31`); CAS metrics (CLI-07) | Subsystem fault vs contention from the workload (CLI-07) | Default and shipped `paxos_variant: v1` (`Config.java:1215`; commented in `conf/cassandra.yaml:1773`): Paxos v2 and its repair are **not on by default**, and `PaxosMetrics` has only those two counters. What else v2 exposes (Paxos repair state, uncommitted counts) **unverified** | none | gap | diagnose only: a violation is CRITICAL; nothing local fixes it | **new**, T7.32 |

## 8. Read path (FM-RD)

| ID | Failure mode | Detects it | Tells it from look-alikes | Physical facts | Today | Status | Proposed disposition | Refs |
|---|---|---|---|---|---|---|---|---|
| FM-RD-01 | Many sstables per read: read latency up on one table | `table.sstables_per_read_p99` (`S/TableSensor.java:114`); `table.pending_compactions` (`:118`) | Per-table backlog (vs node-wide CMP-01) | Histogram decays slowly (ADR-013) | emitted, read by nothing; correlator rule is node-wide (`A/analyze/RuleCorrelator.java:89`) | gap | act: per-table correlation; compaction throughput UP when that table's backlog is the cause | CMP-2, T7.20 |
| FM-RD-02 | Tombstone overload: reads scan many tombstones, slow or abort | `table.tombstones_scanned_p99`, warnings, failures (`S/TableSensor.java:115, 126-129`) | Per table; purgeable vs not | Garbage-collecting compaction purges only purgeable tombstones | `A/analyze/detectors/TombstoneOverloadDetector.java:43`; `A/plan/policies/TombstoneOverloadPolicy.java:61, 93-104` | covered | act: compaction UP; garbage-collecting compaction at CRITICAL (exists) | TS-1 |
| FM-RD-03 | Large partitions: slow reads, heap churn, repair and streaming cost | `table.max_partition_size_bytes` (`S/TableSensor.java:121`); `partition.size_bytes` for sampled keys; `partitionSize` guardrail (`C/db/guardrails/Guardrails.java:374`) | Large but cold (diagnose) vs large and hot (split) | Index-based size is an upper bound until compaction (ADR-043) | table max emitted, read by nothing; split path reads `partition.size_bytes` (`A/plan/policies/PartitionSplitPolicy.java:208`) | partial | diagnose only for cold large partitions; split only when hot (exists) | LP-1, ADR-014, ADR-043 |
| FM-RD-04 | Hot partition: one key takes a large share of a table's operations | Sampler shares (`S/HotPartitionSensor.java:255`) | Share of table ops, not absolute rate | Sampler shared with `nodetool profileload`; tables below `min_ops_per_sec` are not sampled (ADR-013) | `A/analyze/detectors/HotPartitionDetector.java:45`; `PartitionSplitPolicy` | covered | act: partition split (exists) | ADR-014, ADR-040 |
| FM-RD-05 | Cache miss pressure: hit rate drops, reads go to disk | `cache.*_hit_rate` (`S/CacheSensor.java:99-151`) | Drop against the cache's own level (ADR-046) | Key and counter caches on heap, at most 100 MiB and 50 MiB by default (`C/config/DatabaseDescriptor.java:991-992, 1010-1011`); row cache off heap (`OHCProvider`, `C/config/Config.java:578`) and 0 MiB by default (`:580`); chunk cache off heap, fixed size, and off unless `-Dcassandra.file_cache_enabled` (`:612`, `C/cache/ChunkCache.java:52-53`). With defaults only the key cache can usefully grow, and under BTI it is unused (audit P6): **small effect**; most read misses go to the OS page cache (MEM-04) | `A/analyze/detectors/CacheMissPressureDetector.java`; diagnose only (`A/plan/policies/LoadSpikePolicy.java:75`) | partial | act: grow the key cache within measured headroom (RC-1), small effect with defaults; otherwise diagnose (page cache, MEM-04) | CCH-1, CCH-2, RC-1, ADR-046, T7.16 |
| FM-RD-06 | Read repair storm: replicas disagree; blocking read repair adds latency | `ReadRepairMetrics.repairedBlocking` (`C/metrics/ReadRepairMetrics.java`); `TableMetrics.readRepairRequests` (`C/metrics/TableMetrics.java:394`) | Read latency with read repair rate up (vs load) | Usually follows missed writes (CLU-05, CLU-06) | none | gap | diagnose only; possible act: repair priority (existing actuator), owner decision | RR-1, T7.23 |
| FM-RD-07 | Speculative retries mask or add load: a slow replica hidden, extra reads sent | `TableMetrics.speculativeRetries`, `speculativeFailedRetries` (`C/metrics/TableMetrics.java:265-266`) | Retry rate up while own latency is fine (a peer is slow) | n/a | none | gap | diagnose only; evidence for `degraded_peer` | RR-1, T7.23 |
| FM-RD-08 | Bloom filter false positives: extra sstables read | `BloomFilterMetrics` recent false ratio (audit MEM-10) | High false ratio on one table | Fix is `bloom_filter_fp_chance`, a schema change | none | gap | diagnose only | MEM-10 |
| FM-RD-09 | Read timeouts and failures seen by clients (the read side of FM-CLI-05) | as FM-CLI-05, read columns | as FM-CLI-05 | `read_request_timeout` 5 s (`C/config/Config.java:155`) | as FM-CLI-05 | alias | alias of FM-CLI-05 | CLI-2, T7.20 |
| FM-RD-10 | Secondary index or SAI load: index builds after a create or a bootstrap take compaction-like I/O and CPU; index queries fan out to many replicas and scan widely | SAI: `C/index/sai/metrics/` (`TableQueryMetrics`, `IndexMetrics`, `TableStateMetrics`, `ColumnQueryMetrics`); index builds through the compaction manager's active operations (as CMP-05) | Index query latency vs base-table read latency; build activity vs compaction backlog (CMP-01) | Default index type for `CREATE INDEX` is the legacy `CassandraIndex` (`Config.java:1033`), enabled (`:1034`); `concurrent_index_builders` 2 (`Config.java:444`). Index builds are `CompactionInfo.Holder`s (`C/index/SecondaryIndexBuilder.java:25`) registered as active compactions of type `INDEX_BUILD` (`C/db/compaction/CompactionManager.java:2109-2115`, `C/db/compaction/OperationType.java:43`), so the CMP-05 progress input sees them; whether SAI's own builder takes the same path **unverified** | none | gap | diagnose only; correlator: index build as a cause of CMP-02-like starvation, so the yield step can lower its share (owner decision whether yield should cover builds) | **new**, T7.31 |

## 9. Compaction (FM-CMP)

| ID | Failure mode | Detects it | Tells it from look-alikes | Physical facts | Today | Status | Proposed disposition | Refs |
|---|---|---|---|---|---|---|---|---|
| FM-CMP-01 | Compaction backlog: pending tasks grow; more sstables per read | `compaction.pending_tasks` (`S/CompactionSensor.java:65`); active-compaction progress: sum of `CompactionInfo.getCompleted()` deltas over `ActiveCompactions.getCompactions()` (`C/db/compaction/CompactionInfo.java:124`, `C/db/compaction/ActiveCompactions.java:38`), not `bytes_per_sec` (lumpy, see "Physical facts") | Backlog with idle disk (raise throughput) vs saturated disk (do not) | `concurrent_compactors` and `compaction_throughput` are runtime-settable (overview 8.1) | `A/analyze/detectors/CompactionBacklogDetector.java:35`; `A/plan/policies/CompactionBacklogPolicy.java:45` | partial | act: throughput and compactors UP, gated on disk headroom (DSK-05) | CMP-1, T7.19 |
| FM-CMP-02 | Compaction starves foreground I/O or CPU | Device queue and latency (DSK-05), CPU per pool (CPU-01), active-compaction progress per second (as CMP-01) | Foreground latency up while compaction runs at full rate | n/a | Yield step lowers throughput without knowing the bottleneck (`A/plan/policies/YieldBackgroundPolicy.java:62`); compactors DOWN not wired | partial | act: yield when disk or CPU is the bottleneck; compactors DOWN | CMP-1, DSK-3, RC-2, T7.19, T7.24 |
| FM-CMP-03 | No disk headroom for compaction: compactions reduced or aborted | `CompactionMetrics.compactionsReduced` (`C/metrics/CompactionMetrics.java:65`), `compactionsAborted` | Reduced count rising with disk above threshold | `CompactionTask.reduceScopeForLimitedSpace` (`C/db/compaction/CompactionTask.java:125, 527`) | correlator rule disk to backlog (`A/analyze/RuleCorrelator.java:87`); reduced count not read | partial | diagnose only; see AUT-07 for the suppression it causes | CMP-3 |
| FM-CMP-04 | Wrong compaction strategy for the workload (write amplification or read amplification) | Write amplification (bytes compacted per byte flushed); sstables per read | Persistent, not a spike | Changing strategy is out of MVP (overview 1.3 line 60; 8.1 line 732) | none | gap | diagnose only; owner decision on a per-table override actuator later | **new** |
| FM-CMP-05 | Compaction stuck: pending > 0, nothing compacted for a long time | `compaction.pending_tasks` > 0 with no progress in `CompactionInfo.getCompleted()` of any active compaction (`C/db/compaction/CompactionInfo.java:124`), or no active compaction at all | A long compaction that is moving (completed bytes rise) vs one that is not; `compaction.bytes_per_sec` cannot tell them apart: it is 0 for the whole of a long healthy compaction because `bytesCompacted` is added only at the end (`C/db/compaction/ActiveCompactions.java:51`) | n/a | pending emitted; progress not read | gap | diagnose only | **new**, T7.31 |

## 10. Thread pools and messaging (FM-TP)

| ID | Failure mode | Detects it | Tells it from look-alikes | Physical facts | Today | Status | Proposed disposition | Refs |
|---|---|---|---|---|---|---|---|---|
| FM-TP-01 | Request stage pool saturated (Read, Mutation, Native-Transport-Requests) | `pool.pending_tasks`, active, max (`S/ThreadPoolSensor.java:64-78`) | Pool queue vs CPU (CPU-01) vs disk (DSK-05) | `setConcurrency` writes only the max (`C/service/StorageService.java:1773-1778`, P4) | `A/analyze/detectors/PoolSaturationDetector.java:38`; ladder `A/plan/policies/LoadSpikePolicy.java:85` | partial | act: pool UP (exists) with a correct CPU gate; pools below configured (RC-8) | TP-1, CLI-5, P3, RC-8, T7.18 |
| FM-TP-02 | Internal pool saturated (flush, compaction, hints dispatch, repair) | `pool.*` for every pool with JMX metrics | Which pool | `pool.oldest_task_queue_millis` is 0 for non-SEP pools: unknown, not empty (audit P8) | detected by `pool_saturation`; the ladder only resizes request stages | partial | diagnose only (WRT-03 and CMP rows carry the actions) | TP-1, TP-2, P8 |
| FM-TP-03 | Dropped messages: replicas drop MUTATION or READ after the timeout | `MessagingMetrics.getDroppedMessages()` per verb (audit MSG-1) | Drops with latency flat (load shed by Cassandra itself); drops of requests this node coordinates vs of requests from other coordinators (per verb in `C/metrics/DroppedMessageMetrics.java:59-65`, with internal vs cross-node dropped latency, which separates locally created from remote messages but does not name the coordinator) | The canonical overload signal. The throttle actuator limits only **this node's native clients**: it sets `ClientResourceLimits.setNativeTransportMaxRequestsPerSecond` and the native rate-limiting flag (`A/execute/actuators/RequestThrottleActuator.java:107-125`). Replica work sent by other coordinators is not limited by it | none | gap | diagnose only (overloaded replica); throttle only when this node's own client load is the source (CLI-01, the ladder does that); owner option: advertise severity, which moves **reads** only (`C/locator/DynamicEndpointSnitch.java:303-304`) | MSG-1, T7.21 |
| FM-TP-04 | Internode outbound backlog to one peer | `InternodeOutboundMetrics` pending bytes, dropped (audit MSG-3) | One peer's backlog grows (vs NET-04) | Queues are capped by `internode_application_send_queue_capacity` 4 MiB (`C/config/Config.java:335`) | none | gap | diagnose only; early evidence of `degraded_peer` | MSG-3, T7.21 |
| FM-TP-05 | Coordinator overload: in-flight request bytes at the limit, connections paused, requests discarded | `ClientMetrics` "RequestsSize", "PausedConnections", "RequestDiscarded", "TimedOutBeforeProcessing" (`C/metrics/ClientMetrics.java:194-243`) | Coordinator-side (vs replica drops, TP-03) | Limit `ClientResourceLimits.getGlobalLimit()`; setter `StorageService.setNativeTransportMaxConcurrentRequestsInBytes` (`C/service/StorageService.java:5182`) | none; no actuator for in-flight bytes | gap | act: throttle (exists); in-flight bytes knob (RC-5, owner decision) | CLI-4, RC-5, T7.22, T7.24 |
| FM-TP-06 | Executors without JMX metrics saturate unseen (for example `ScheduledExecutors`) | none today | n/a | Which ones matter is **unverified** (audit TP-1) | none | gap | owner decision: list them first (S task), then decide | TP-1 |

## 11. Clients (FM-CLI)

| ID | Failure mode | Detects it | Tells it from look-alikes | Physical facts | Today | Status | Proposed disposition | Refs |
|---|---|---|---|---|---|---|---|---|
| FM-CLI-01 | Traffic spike or sustained overload | `client.*_ops_per_sec`, p99 (`S/ClientRequestSensor.java:91-94`); pool queues | Op rate up (vs GC, disk, cache: op rate flat). **A peer down or slow also raises this node's op rate**: drivers move its coordinator traffic and its replica share here. Check peer liveness (CLU-01) and peer severity before reading the rise as client demand | Throttle changes client-visible behaviour (overview 8.1 notes) | `A/analyze/detectors/LatencySloBreachDetector.java`; ladder; `A/plan/policies/RequestThrottlePolicy.java:163` (CRITICAL only) | covered | act: the ladder (exists) | CLI-1, ADR-042 |
| FM-CLI-02 | Connection storm: many new connections, authentication load | `ClientMetrics` "Connections", "ConnectedNativeClients" (`C/metrics/ClientMetrics.java:194-196`) | Connection rate up with op rate flat | `native_transport_max_concurrent_connections` -1 (unlimited, `C/config/Config.java:371`); setter `StorageProxy.setNativeTransportMaxConcurrentConnections` (`C/service/StorageProxy.java:3448`), whether it affects existing connections **unverified** | none | gap | diagnose only; owner decision on a connection-limit actuator | CLI-4, **new** (action) |
| FM-CLI-03 | Bad queries: ALLOW FILTERING, large IN, large pages | Guardrails `allowFilteringEnabled`, `inSelectCartesianProduct`, `partitionKeysInSelect` (`C/db/guardrails/Guardrails.java:293, 318, 336`); `GuardrailEvent` published only with `diagnostic_events_enabled` (`C/db/guardrails/GuardrailsDiagnostics.java:43-56`; default false, `C/config/Config.java:838`) | Per-query, not per-node | The layer cannot change clients (overview 1.2, line 52) | none | gap | diagnose only | **new** |
| FM-CLI-04 | Large or multi-partition batches | `BatchMetrics` (audit CLI-7); thresholds `batch_size_warn/fail_threshold` (`C/config/Config.java:412-416`) | n/a | Client behaviour | none | gap | diagnose only | CLI-7 |
| FM-CLI-05 | Client errors (timeouts, unavailables, failures) not watched. **Primary row**; FM-WRT-05 (writes) and FM-RD-09 (reads) are its aliases | `client.*_timeouts`, `_unavailables`, `_failures`, read and write (`S/ClientRequestSensor.java:95-97`) | Which: unavailable means replicas down (CLU-04); timeout means slow; timeouts with latency flat means requests dropped (TP-03) | `write_request_timeout` 2 s (`Config.java:161`), `read_request_timeout` 5 s (`:155`) | emitted, read by nothing (grep, 2026-11-24) | gap | act: detector; error guard on every verify spec (AUT-03) | CLI-2, T7.20 |
| FM-CLI-06 | Range scans as hidden load | `RangeCommandIterator.rangeMetrics` (audit CLI-3); `table.range_*` (emitted, unused) | Range op rate up, point rate flat | n/a | emitted, read by nothing | gap | diagnose only | CLI-3, T7.22 |
| FM-CLI-07 | LWT contention: CAS retries, unfinished commits | `CASClientRequestMetrics.contention`, `unfinishedCommit` (audit CLI-6) | CAS p99 up, plain ops fine | LWT is rejected on adapted partitions (ADR-021) | CAS rates and p99 emitted, unused | gap | diagnose only | CLI-6 |
| FM-CLI-08 | Prepared-statement cache thrash: clients prepare more distinct statements than fit (often literals in prepared text); every eviction forces a re-prepare round trip and parse CPU | `CQLMetrics.preparedStatementsEvicted` (`C/metrics/CQLMetrics.java:35`), `preparedStatementsCount`, `preparedStatementsRatio`, cache size and capacity (`:39-42`) | Evictions rising with a steady count at capacity (thrash) vs a one-off schema change | Cache size default max(1/256 of heap, 10 MiB) (`C/config/DatabaseDescriptor.java:971-974`); on heap. Growing it moves heap from memtables and caches; fixing the client is the real cure | none | gap | diagnose only (name the cause); owner decision whether a resize actuator is worth it (recommend no) | **new**, T7.32 |

## 12. Cluster (FM-CLU)

| ID | Failure mode | Detects it | Tells it from look-alikes | Physical facts | Today | Status | Proposed disposition | Refs |
|---|---|---|---|---|---|---|---|---|
| FM-CLU-01 | Peer down | `peer.alive`; `Gossiper.getUnreachableMembers` (`C/gms/Gossiper.java:416`), `getEndpointDowntime` (`:448`) | Down to all vs down to this node (NET-03) | Hints accumulate for up to `max_hint_window` 3 h (`C/config/Config.java:125`) | alive emitted (`S/PeerSensor.java:90`); no diagnosis | partial | diagnose only (feeds CLU-05, CLU-06) | GSP-1, **new** (diagnosis) |
| FM-CLU-02 | Slow (grey) peer: alive but slow | Snitch score (`S/PeerSensor.java:95, 104`); internode latency (NET-04); outbound backlog (TP-04) | One peer vs network (NET-04) | Dynamic snitch already routes reads away (`dynamic_snitch: true`, `C/config/Config.java:523`) | `A/analyze/detectors/DegradedPeerDetector.java:42`; no policy (grep of `A/plan/`) | partial | diagnose only (Cassandra routes around it); but see AUT-07 | MSG-2, MSG-3 |
| FM-CLU-03 | This node slow (degraded self) | Own coordinator p99 above SLO while no peer is an outlier | Cause: steal, disk, GC, compaction | n/a | `A/analyze/detectors/DegradedSelfDetector.java`; ladder incl. advertise (`A/plan/policies/LoadSpikePolicy.java:146-148`); `DegradedSelfPolicy` dead code | partial | act: ladder (exists) with causes named (CPU-02, DSK-06); remove dead policy (RC-7) | RC-7, T7.24 |
| FM-CLU-04 | Quorum loss: not enough replicas alive; unavailables | `client.*_unavailables_per_sec`; peer liveness per replica set | Unavailables vs timeouts | Nothing local restores quorum | emitted, read by nothing | gap | diagnose only (CRITICAL) | CLI-2, **new** (diagnosis) |
| FM-CLU-05 | Hints accumulate (peer down, or slow to drain) | `StorageMetrics.totalHintsInProgress`, `totalHints` (`C/metrics/StorageMetrics.java:49-50`); `HintsServiceMetrics` | Growth vs drain after recovery | Yield step lowers the hint throttle blind (`A/plan/policies/YieldBackgroundPolicy.java:63`) | none | gap | act: yield hints only when hints are in flight; raise the throttle to drain after recovery (RC-3) | HNT-1, RC-3, T7.22, T7.24 |
| FM-CLU-06 | Hints dropped (window exceeded or volume full): replicas silently miss writes | `HintedHandoffMetrics.notStored` (audit HNT-2) | n/a | Only repair restores consistency | none | gap | act: repair priority for the affected host (existing `RepairPriorityActuator`), owner decision | HNT-2, T7.22 |
| FM-CLU-07 | Streaming load (bootstrap, decommission, repair) slows foreground | `StreamingMetrics`, `StreamManager.getCurrentStreams()` (audit STR-1) | Streams active vs not | Stream yield is blind (`A/plan/policies/YieldBackgroundPolicy.java:62`) | none | gap | act: yield streams only when streams are active | STR-1, T7.22 |
| FM-CLU-08 | Bootstrap, decommission or move stuck or failing | Node state in cluster metadata; stream progress | Progress stalled | Topology lanes already wait for no in-progress sequences (overview 7.1 check 5) | arming reads JOINED (`A/AutonomicService.java:271`) | gap | diagnose only | NODE-1, **new** |
| FM-CLU-09 | Repair overdue | Auto-repair history | n/a | Uses built-in auto-repair (ADR-009) | `A/analyze/detectors/RepairOverdueDetector.java`; `A/plan/policies/RepairOverduePolicy.java` | covered | act: repair priority (exists) | REP-1, ADR-009, ADR-026 |
| FM-CLU-10 | Repair failing (token ranges fail repeatedly) | `AutoRepairMetrics.failedTokenRangesCount` (`C/metrics/AutoRepairMetrics.java:45`) | Failing vs merely late | n/a | not read | partial | diagnose only (cause of CLU-09) | REP-1 |
| FM-CLU-11 | Metadata disagreement: a node behind the TCM log; schema or placement stale | `TCMMetrics.currentEpochGauge` (`C/metrics/TCMMetrics.java:43`); peers' epochs via `TCM_CURRENT_EPOCH_REQ` (`A/layout/EpochWatermarks.java:41-44, 114`) | Lag vs CMS down (CLU-12) | Peer epochs **are** obtainable: the layout code already asks for them (resolves audit 7) | used for layout phases only | gap | diagnose only | TCM-1, SCH-1, T7.23 |
| FM-CLU-12 | CMS unavailable: metadata commits fail; schema changes and cluster actions fail | `TCMMetrics.unreachableCMSMembers`, `commitFailureLatency`, `commitRetries` (`C/metrics/TCMMetrics.java:45, 57, 59`) | n/a | Cluster actions commit to the log, so they fail static | none | gap | diagnose only | TCM-1, **new** |
| FM-CLU-13 | Token or load imbalance | Gossiped `LOAD` per peer (`S/PeerSensor.java:87-91`) | Persistent ratio, not a spike | `LOAD` can be stale (`S/Signals.java:149`) | `A/analyze/detectors/NodeImbalanceDetector.java:43`; no policy | covered | diagnose only (token moves out of MVP, overview 1.3) | overview 1.3 |
| FM-CLU-14 | DC loss: a whole datacenter unreachable | Liveness grouped by DC | All peers of one DC down | Remote-DC actions are topology changes | none | gap | diagnose only | **new** |
| FM-CLU-15 | Mixed versions (rolling upgrade) while cluster actions are on | `Directory.clusterMinVersion` vs `clusterMaxVersion` | n/a | Checked only when cluster actions are enabled (`A/coord/ClusterCoordinator.java:558`) | enable-time check only | partial | act: pause cluster actions while versions differ | overview 6.5, **new** |
| FM-CLU-16 | Gossip flapping: peers marked down and up repeatedly | `FailureDetector.getPhiValues()` (audit GSP-1) | Phi near `phi_convict_threshold` 8 (`C/config/Config.java:262`) | n/a | none | gap | diagnose only | GSP-1 |
| FM-CLU-17 | Hint delivery failing or timing out: the target is up but hints do not drain, so the hint backlog stays and the replica stays inconsistent | `HintsServiceMetrics.hintsFailed`, `hintsTimedOut` (`C/metrics/HintsServiceMetrics.java:66-67`) against `hintsSucceeded` (`:65`); `hintsFileSize` (`:70`) | Failing with the target alive (this row) vs no delivery because the target is down (CLU-05); timeouts with the target slow (CLU-02) vs failures (errors on the target) | Hints that cannot be delivered stay until `max_hint_window` passes and then only repair restores consistency (as CLU-06) | none | gap | diagnose only; evidence for CLU-02 and for the repair trigger of CLU-06 | **new**, T7.29 |

## 13. Data (FM-DAT)

| ID | Failure mode | Detects it | Tells it from look-alikes | Physical facts | Today | Status | Proposed disposition | Refs |
|---|---|---|---|---|---|---|---|---|
| FM-DAT-01 | SSTable corruption: reads of some data fail | Error count: a count of `CorruptSSTableException`s through a wrapping `DiskErrorsHandler` (new; `handleCorruptSSTable`, `C/service/DefaultDiskErrorsHandler.java:61`); client read failures; **transport state** (`C/service/StorageService.java:591, 624`) | Failures concentrated on one table | `disk_failure_policy`: `ignore` in `Config.java:134`, `stop` in the shipped `conf/cassandra.yaml:502`. A corrupt sstable stops transports only under `die` or `stop_paranoid` (`DefaultDiskErrorsHandler.java:66-73`); under the shipped `stop` the node keeps serving and only logs, so client read failures are the visible effect; under `die` the JVM exits (`C/utils/JVMStabilityInspector.java:162-175`) | none | gap | diagnose only (CRITICAL); scrub stays manual | DF-1, **new** |
| FM-DAT-02 | Snapshots accumulate and fill the disk | `TableMetrics.trueSnapshotsSize` (`C/metrics/TableMetrics.java:196`) | Snapshot share of used space (vs live data) | `auto_snapshot: true` by default (`C/config/Config.java:395`; shipped `conf/cassandra.yaml:1205`). `auto_snapshot_ttl` exists and is unset by default (`Config.java:402`; commented `conf/cassandra.yaml:1213`); it applies to the snapshots taken on truncate and drop (`C/db/ColumnFamilyStore.java:2703, 3412`), not to operator or repair snapshots. Cost of the gauge (directory walk) **unverified**; clearing via `StorageService.clearSnapshot` (`C/service/StorageService.java:3034`) is irreversible | none | gap | act: **first** recommend the operator set `auto_snapshot_ttl` (configuration, reversible; diagnosis names it); **then**, only if the owner rules for it, clear old snapshots (new, irreversible actuator; manual approval) | DSK-4, T7.23, T7.30 |
| FM-DAT-03 | TTL or tombstone build-up grows disk use | Droppable tombstone ratio per sstable (**unverified** metric); `table.tombstones_scanned_p99` | Disk growth with flat live data | Purge waits for `gc_grace_seconds` | reads covered by RD-02; disk growth not attributed | partial | diagnose only | TS-1 |
| FM-DAT-04 | Repaired data inconsistent across replicas | `TableMetrics.confirmedRepairedInconsistencies` (`C/metrics/TableMetrics.java:281`) | n/a | Needs repaired-data tracking, off by default (`C/config/Config.java:851-852`) | none | gap | diagnose only | **new** |
| FM-DAT-05 | Commit log or system keyspace corruption at start | Startup failure | n/a | Node does not start; the layer is not running | none | gap | out of scope: the layer is not running | **new** |
| FM-DAT-06 | Too many tables: each table carries a memtable, metadata and per-table metrics on heap, so heap and flush pressure grow with schema size, not data | Count of user tables from schema; `tables` guardrail (`C/db/guardrails/Guardrails.java:83`), thresholds `tables_warn_threshold` and `tables_fail_threshold`, both -1 (off) by default (`Config.java:990-991`) | Heap pressure with many small memtables (JVM-05) vs few large | Partition split adds tables (ADR-014), so the layer itself can push the count up. Per-table heap cost **unverified** (no measurement here) | none | gap | act: guard: block splits above a table-count limit (the guardrail's warn level when set, else an owner-set default); diagnose above it | **new**, T7.28 |

## 14. The autonomic layer itself (FM-AUT)

| ID | Failure mode | Detects it | Tells it from look-alikes | Physical facts | Today | Status | Proposed disposition | Refs |
|---|---|---|---|---|---|---|---|---|
| FM-AUT-01 | Actions oscillate (a knob goes up and down) | Action history | n/a | n/a | Hysteresis and persistence (`A/analyze/ThresholdDetector.java`); cooldown, quarantine (`A/safety/DefaultSafetyGovernor.java:270-278`); direction lock (`A/execute/actuators/KnobActuator.java:47, 182-190`); circuit breaker (`A/AutonomicService.java:1474-1479`) | covered | act: as built | overview 7.4 |
| FM-AUT-02 | Two policies push one knob opposite ways (backlog raises compaction, yield lowers it) | Action history per knob | n/a | n/a | `CONFLICT`/`CONCURRENCY` stop simultaneous actions (`A/safety/DefaultSafetyGovernor.java:280-300`); sequential opposite steps are not prevented (**assumption** from the code: cooldown is keyed by `dedupeKey`) | partial | act: a per-knob rule on opposite directions within a window (note 1) | **new** |
| FM-AUT-03 | An action hurts clients without raising latency (latency turned into timeouts or errors) | Client error rates | n/a | n/a | Guards are latency only (`A/plan/policies/KnobPolicy.java:150-155`) | gap | act: error-rate guard on every verify spec | CLI-2, T7.20 |
| FM-AUT-04 | Yielded knobs stay low after recovery (compaction, streams, hints) | `autonomic.baseline_drift` | n/a | Committed values are sticky overrides (operator guide 8) | No recovery for stream and hint knobs; compaction only by later UP | gap | act: recovery on `baseline_drift` (owner decision per knob) | RC-3, T7.24 |
| FM-AUT-05 | Advertised severity stays raised after recovery; coordinators keep avoiding the node | `peer.severity` of self; `baseline_drift` | n/a | Reads: the dynamic snitch adds the severity, unweighted, to the replica's score (`C/locator/DynamicEndpointSnitch.java:303-304`), so any raised value keeps reads away in proportion. Writes: only remote-DC forwarding looks at it, via `StorageProxy.pickReplica`, where only severity 0 counts as healthy (`C/service/StorageProxy.java:1993-1998`, called only at `:1968` inside `sendMessagesToNonlocalDC`, `:1960-1968`). Local-DC writes ignore severity (audit RC-4 corrected) | no DOWN direction | gap | act: DOWN on recovery | RC-4, T7.24 |
| FM-AUT-06 | A wrong diagnosis suppresses the right action through the correlator (spurious `gc_pressure` under ZGC becomes the root of a real latency breach) | Correlated diagnoses | n/a | Policies get only diagnoses without a root cause (`A/AutonomicService.java:646`) | Rules `A/analyze/RuleCorrelator.java:93-98` with the defect of JVM-02 | wrong | act: fix the input (T7.17); add a test that a false cause cannot suppress the ladder | P2, 4.2 of audit, T7.17 |
| FM-AUT-07 | A cause with no policy suppresses every action on its effects: `disk_pressure` is the root of `compaction_backlog` and, through it, of latency breaches; `degraded_peer` is the root of node latency breaches and `degraded_self`. Nothing acts on either cause | Correlated diagnoses | n/a | No policy handles `disk_pressure` or `degraded_peer` (grep of `A/plan/`); the ladder only excuses `cache_miss_pressure` (`A/plan/policies/LoadSpikePolicy.java:75, 233`) | Rules `A/analyze/RuleCorrelator.java:87-92, 109-112`; skip at `A/AutonomicService.java:646` | wrong | owner decision: treat causes without an acting policy as diagnose-only causes, or keep the suppression on purpose (note 2) | **new** |
| FM-AUT-08 | A sensor's signal does not mean what the policy assumes | Unit tests against the real classes (R5) | n/a | P1 row cache is off-heap; P2 GC time is not pause time; P3 load average is not CPU | `S/GcSensor.java:75`; `S/CpuSensor.java:62`; `A/plan/policies/GcPressurePolicy.java:43` | wrong | act: fix each (T7.16, T7.17, T7.18) and assert the fact in a test | P1, P2, P3, R5 |
| FM-AUT-09 | Action on a resource with nothing to change: row cache shrunk for heap; key cache knob on BTI; hints or streams yielded when idle; cache shrunk while under-filled | Resource state before the action | n/a | Key cache unused under BTI (audit P6); hints and streams invisible (HNT-1, STR-1) | `A/plan/policies/GcPressurePolicy.java:43` (row first); `A/plan/policies/YieldBackgroundPolicy.java:62-63` | wrong | act: skip a step whose resource is idle or elsewhere; each rollback costs a cooldown | P1, P6, HNT-1, STR-1, CCH-2, T7.16, T7.22 |
| FM-AUT-10 | A sensor fails, is slow, or goes silent | Per-component failure count; sensor time; signal age | n/a | n/a | `autonomic.component_failed` after 5 failures; `sensor_budget` (`A/AutonomicService.java:531-568`); `max_signal_age_ms` (`A/analyze/ThresholdDetector.java:74`) | covered | act: as built | overview 7.5 |
| FM-AUT-11 | The layer's own CPU and memory use harms the node | Per-sensor and per-tick time (`AutonomicMetrics`); series count | n/a | `max_series` 5000 (`A/AutonomicConfig.java:44`); the partition sampler runs on request paths each window (`S/HotPartitionSensor.java:255`) | time and series bounded; no measure of the layer's heap or total CPU; no self-diagnosis on overhead | partial | act: self-diagnosis when overhead exceeds a budget, then pause sampling | overview 7.5, **new** |
| FM-AUT-12 | Monitor, control or actuator thread stalls | Heartbeats | n/a | n/a | `A/AutonomicService.java:873-881`; dead-man rollback (overview 7.3.9) | covered | act: as built | overview 7.5 |
| FM-AUT-13 | Actions keep failing or rolling back | Rollback ratio | n/a | n/a | Circuit breaker (`A/AutonomicService.java:1474-1479`); quarantine | covered | act: as built | overview 7.5 |
| FM-AUT-14 | Knowledge store unwritable (disk full, keyspace unavailable) | Write failures | n/a | n/a | No action approved (`A/safety/DefaultSafetyGovernor.java:266-268`); rollbacks still run | covered | act: as built (fail static) | overview 7.5 |
| FM-AUT-15 | Cluster ledger or coordination fails: owner dies holding a claim, partition, TCM commit fails | Lease expiry; commit result | n/a | Lease with safety margin (`A/coord/ClusterCoordinator.java:277-278`); takeover order (ADR-038, `:225`) | lease and takeover built; CMS unavailability not diagnosed (CLU-12) | partial | act: as built; add CLU-12 diagnosis | ADR-006, ADR-038 |
| FM-AUT-16 | Herd: every node acts at once on a cluster-wide cause (all throttle; all advertise severity, so routing gains nothing) | Fraction of peers with the same diagnosis | Local vs cluster-wide cause | Node actions are not coordinated (overview 6.1). **Verified from the code**: severity is added as the same constant to every score (`C/locator/DynamicEndpointSnitch.java:303-304`), so equal severity on every node leaves the score order unchanged and routing gains nothing. It also does harm: the switch from the subsnitch order to the score order is a **ratio** test (`:200-204`, `dynamic_badness_threshold`), and adding the same constant to every score shrinks the ratios, so a genuinely slow replica is avoided less | none | gap | act: skip advertise when most peers are degraded; diagnose a cluster-wide cause | **new** |
| FM-AUT-17 | Wall-clock jump distorts the layer's rates and windows | Monotonic vs wall clock difference | n/a | The layer's clock is `currentTimeMillis` (`A/AutonomicService.java:270`); sensors take elapsed time from it (`S/TableSensor.java:77`); a backward jump gives NaN (`S/SensorSupport.java:71`), a forward jump dilutes one interval (**assumption**) | none | gap | act: use a monotonic clock for intervals (design fix) | **new** |
| FM-AUT-18 | Restart in the middle of an action | In-flight records | n/a | Node knobs are in-memory overrides | overview 7.6 | covered | act: as built | overview 7.6 |
| FM-AUT-19 | Operator changes a knob the layer manages | Live value differs from applied | n/a | n/a | `SUPERSEDED` (overview 7.2) | covered | act: as built | overview 7.3 |
| FM-AUT-20 | Layer acts while the node joins, drains or is not ready | Node state | n/a | n/a | `NODE_STATE` (`A/safety/DefaultSafetyGovernor.java:256-263`); arming on JOINED (`A/AutonomicService.java:271-272`) | covered | act: as built | overview 3.6 |
| FM-AUT-21 | Partition split or unsplit fails or does not help | Split outcome diagnoses | n/a | n/a | `SplitOutcomeDetectors` (`A/AutonomicService.java:318-324`) | covered | act: as built (abort, reverse, reclaim) | ADR-024, ADR-040 |
| FM-AUT-22 | Verification verdict is noise: a borderline verdict commits or rolls back by chance | Repeatability runs | n/a | Hot spot scenario fails about one run in five on 2.68 ms vs a 2.61 ms limit (NEXT-STEPS, open items) | none | partial | act: analyse in T7.25; never loosen a guard to pass | ADR-040, T7.25 |

Notes:

1. **assumption**: a backlog UP and a yield DOWN can follow each other on `compaction_throughput` once each cooldown ends, because they come from different diagnoses. Not tested in a run.
2. Whether suppression is right differs by cause. Under `degraded_peer`, growing local pools does not help, so suppression is reasonable, but the diagnosis should say so. Under `disk_pressure`, a latency breach from a compaction backlog gets no action at all, and the disk itself gets none either (FM-DSK-01). The mechanism exists for `cache_miss_pressure` (ADR-046 point 3); extending it is a one-line change, but it changes behaviour, so it is the owner's call.


## 15. Node lifecycle (FM-NODE) (revision 2)

| ID | Failure mode | Detects it | Tells it from look-alikes | Physical facts | Today | Status | Proposed disposition | Refs |
|---|---|---|---|---|---|---|---|---|
| FM-NODE-01 | Node alive but not serving: native transport and gossip stopped by the disk or commit failure policy, or by an operator; the JVM and the layer keep running, peers see the node down | Own transport state: `StorageService.isGossipRunning` (`C/service/StorageService.java:591`), `isNativeTransportRunning` (`:624`); peers' view (CLU-01) | Stopped by `disk_failure_policy` (DSK-07, error count > 0), by `commit_failure_policy` (commit error, DSK-02), or by an operator (`nodetool disablebinary` / `disablegossip`: no error, a JMX call); vs draining (already gated) | `stopTransports()` stops native transport and gossip only (`C/service/StorageService.java:645-656`); called by the disk policy (`C/service/DefaultDiskErrorsHandler.java:85-90, 102`) and the commit policy (`:147-149`). Shipped config uses `stop` for both (`conf/cassandra.yaml:502, 519`). Verify windows read client latency, which no longer exists, so any action taken now verifies on noise | `NODE_STATE` checks only joined and not draining (`A/AutonomicService.java:1430-1432`, `A/safety/DefaultSafetyGovernor.java:256-258`); transport state is read nowhere in `A/` (grep 2026-11-24) | gap | act: diagnose (CRITICAL when caused by a policy, INFO when by an operator) and add transport state to the `NODE_STATE` gate so no node action runs; never restart transports automatically | **new**, T7.36 |
| FM-NODE-02 | Crash loop or unclean previous exit: the node restarts after a kill (heap OOM, kernel OOM, `System.exit` from `JVMStabilityInspector`, host failure), possibly repeatedly | At start: commit log replay count (`C/db/commitlog/CommitLog.java:183, 211`; non-zero means the previous stop was not after a drain, **assumption**); a new `*.hprof` in the dump directory (FM-DSK-09); cgroup `memory.events` `oom_kill` count above the last value stored (FM-MEM-01); the layer's own record of its last clean stop and of recent start times in the knowledge store (new) | One unclean exit vs a loop (several starts in a window); planned restart (drain first, no replay) vs crash | The layer runs only after the node starts and loses its in-memory knob overrides on restart (FM-AUT-18). If a layer action caused the crash, acting again at once repeats it. Whether `memory.events` survives a restart depends on the cgroup surviving (true for a systemd service restart, **assumption**) | none | gap | act: on start after an unclean exit, diagnose with the cause found and run the layer observe-only for a window; in a crash loop, stay observe-only until an operator clears it (owner decision on the window and the loop threshold) | **new**, T7.36 |

## 16. Optional subsystems off by default (FM-OPT) (revision 2)

Each row is one subsystem with its own failure modes. They are proposed **out of scope while disabled by default**; enabling one in a deployment the layer manages reopens the row. Each needs an owner ruling.

| ID | Failure mode | Detects it | Tells it from look-alikes | Physical facts | Today | Status | Proposed disposition | Refs |
|---|---|---|---|---|---|---|---|---|
| FM-OPT-01 | Accord transactions: journal, coordination and migration failures | `AccordClientRequestMetrics` (`C/metrics/AccordClientRequestMetrics.java:28`) and Accord's own metrics (not surveyed) | n/a | Off by default: `AccordConfig.enabled = false` (`C/config/AccordConfig.java:44`); the shipped yaml has the block commented out (`conf/cassandra.yaml:2894-2896`) | none | gap | out of scope while disabled by default (owner ruling) | audit (Accord), **new** row |
| FM-OPT-02 | Materialised views: view builds and view updates add write and read load on the base table's replicas; views drift from the base | View build progress; `ViewWriteMetrics` (**unverified** names) | n/a | Off by default: `materialized_views_enabled = false` (`Config.java:719`; shipped `conf/cassandra.yaml:2315`); `concurrent_materialized_view_builders` 1 (`Config.java:440`) | none | gap | out of scope while disabled by default (owner ruling) | **new** |
| FM-OPT-03 | CDC: the `cdc_raw` directory fills because no consumer deletes segments, and writes to CDC tables are rejected | `FileStore` of `cdc_raw_directory` (`Config.java:516`); CDC space metrics (**unverified** names) | n/a | Off by default: `cdc_enabled = false` (`Config.java:509`; shipped `conf/cassandra.yaml:444`); `cdc_block_writes = true` (`Config.java:512`); `cdc_total_space` default `0MiB`, meaning computed at start (`:518`; how, **unverified**) | none | gap | out of scope while disabled by default (owner ruling) | **new** |

---

## Coverage summary

**Method.** The counts below are produced by a script, not by hand. It reads every table line of this file that starts with `| FM-`, splits it into its nine columns, takes the Status column as is, and classifies the Proposed disposition by its first words: `act`, `diagnose only`, `out of scope`, `owner decision` (a row whose disposition is itself a question), or `alias`. Alias rows are excluded, so each failure mode is counted once. Rows that propose an action but need an owner ruling on a new actuator count as **act** and appear in group C below. The script, to rerun at each gate:

```python
import collections, sys
rows = [l.strip().strip('|').split(' | ') for l in open(sys.argv[1]) if l.startswith('| FM-')]
assert all(len(r) == 9 for r in rows)          # a stray ' | ' inside a cell breaks the count
def disp(d):
    d = d.strip().lower()
    for k in ('alias', 'act', 'diagnose only', 'out of scope', 'owner decision'):
        if d.startswith(k): return k
    raise ValueError(d)
counted = [(r[0].split('-')[1], r[6].strip(), disp(r[7])) for r in rows if disp(r[7]) != 'alias']
for fam in dict.fromkeys(f for f, _, _ in counted):
    print(fam, collections.Counter(s for f, s, _ in counted if f == fam),
               collections.Counter(d for f, _, d in counted if f == fam))
print(len(counted), collections.Counter(s for _, s, _ in counted), collections.Counter(d for _, _, d in counted))
```

132 table rows, 2 aliases (FM-WRT-05 and FM-RD-09, aliases of FM-CLI-05), **130 failure modes** counted (116 in revision 1, plus 16 new rows, minus 2 aliases).

Counts by status:

| Family | Rows | covered | partial | gap | wrong |
|---|---|---|---|---|---|
| CPU | 6 | 0 | 0 | 4 | 2 |
| MEM | 7 | 0 | 0 | 7 | 0 |
| JVM | 11 | 0 | 1 | 9 | 1 |
| DSK | 11 | 0 | 1 | 10 | 0 |
| OS | 4 | 0 | 0 | 4 | 0 |
| NET | 6 | 0 | 1 | 5 | 0 |
| WRT | 7 | 0 | 1 | 6 | 0 |
| RD | 9 | 2 | 2 | 5 | 0 |
| CMP | 5 | 0 | 3 | 2 | 0 |
| TP | 6 | 0 | 2 | 4 | 0 |
| CLI | 8 | 1 | 0 | 7 | 0 |
| CLU | 17 | 2 | 5 | 10 | 0 |
| DAT | 6 | 0 | 1 | 5 | 0 |
| AUT | 22 | 9 | 4 | 5 | 4 |
| NODE | 2 | 0 | 0 | 2 | 0 |
| OPT | 3 | 0 | 0 | 3 | 0 |
| **Total** | **130** | **14** | **21** | **88** | **7** |

Counts by proposed disposition:

| Family | act | diagnose only | out of scope | owner decision open |
|---|---|---|---|---|
| CPU | 4 | 1 | 1 | 0 |
| MEM | 3 | 3 | 1 | 0 |
| JVM | 5 | 3 | 2 | 1 |
| DSK | 4 | 6 | 1 | 0 |
| OS | 2 | 1 | 0 | 1 |
| NET | 0 | 6 | 0 | 0 |
| WRT | 2 | 5 | 0 | 0 |
| RD | 4 | 5 | 0 | 0 |
| CMP | 2 | 3 | 0 | 0 |
| TP | 2 | 3 | 0 | 1 |
| CLI | 2 | 6 | 0 | 0 |
| CLU | 6 | 11 | 0 | 0 |
| DAT | 2 | 3 | 1 | 0 |
| AUT | 21 | 0 | 0 | 1 |
| NODE | 2 | 0 | 0 | 0 |
| OPT | 0 | 0 | 3 | 0 |
| **Total** | **61** | **56** | **9** | **4** |

"owner decision open": FM-JVM-08, FM-OS-04, FM-TP-06, FM-AUT-07. Rows nothing serves today (status **gap**): 88. Each is mapped to a task below or proposed out of scope.

Changes in the counts since revision 1: FM-TP-03 moved from act to diagnose only (the throttle cannot limit other coordinators' load); FM-WRT-05 and FM-RD-09 became aliases (two act rows fewer); 16 new rows: 3 act (FM-DAT-06, FM-NODE-01, FM-NODE-02), 10 diagnose only (FM-JVM-11, FM-DSK-09, -10, -11, FM-NET-06, FM-WRT-07, -08, FM-RD-10, FM-CLI-08, FM-CLU-17), 3 out of scope (FM-OPT-01, -02, -03). Check: act 61 − 1 − 2 + 3 = 61; diagnose only 45 + 1 + 10 = 56; out of scope 6 + 3 = 9; owner decision open 4.

## Owner decisions

The owner decides; these are recommendations, for one session. Each item: rows, recommendation, reason. Numbers run through all four groups.

### Top decisions

The rulings that change the most behaviour or cost. Each is also in its group below.

- **#46** FM-AUT-07: causes without a policy suppress every action on their effects. Changes what the ladder does under disk pressure.
- **#33** Memtable flush actuator (FM-JVM-05, FM-WRT-02, FM-DSK-02): the primary heap action, since cache steps are small with defaults. Largest single build (T7.30).
- **#39** FM-NODE-01 and FM-DSK-07: no node action while transports are stopped. Shipped `stop` policies make this the common outcome of a disk error.
- **#40** FM-NODE-02: observe-only start after an unclean exit; stay observe-only in a crash loop.
- **#34** FM-DAT-02: recommend `auto_snapshot_ttl` first; build the irreversible snapshot-clearing actuator or not.
- **#51** FM-AUT-04, FM-AUT-05: recover yielded knobs and lower advertised severity after recovery.
- **#41** FM-AUT-16: skip advertising severity when most peers are degraded. Now verified to do harm, not only to gain nothing.
- **#49** FM-TP-03: dropped messages are diagnose only, or advertise severity for reads.
- **#1–#3** Accord, materialised views, CDC out of scope while off by default.
- **#47** Per-thread CPU sampling and a JFR spike (FM-CPU-01, -05, FM-JVM-08): measure the cost first. Decides T7.18's distinction of background from client CPU and the size of T7.35.

### A. Proposed out of scope

1. FM-OPT-01 Accord: out of scope while `accord.enabled` is false by default (`C/config/AccordConfig.java:44`). Reopen when a managed deployment enables it.
2. FM-OPT-02 materialised views: out of scope while `materialized_views_enabled` is false by default (`Config.java:719`).
3. FM-OPT-03 CDC: out of scope while `cdc_enabled` is false by default (`Config.java:509`).
4. FM-CPU-06 CPU frequency and thermal throttling: out of scope. Not reliably visible from a guest.
5. FM-MEM-07 Transparent huge pages and memory compaction: out of scope. Host configuration; at most a one-time check at start.
6. FM-JVM-09 the OOM itself: out of scope. The JVM is killed by `-XX:OnOutOfMemoryError` (`conf/cassandra-env.sh:225`) and the layer dies with it; its precursor (FM-JVM-04) is in scope, and the restart is FM-NODE-02.
7. FM-DSK-08 Read-ahead: out of scope. Cassandra checks it at startup (`C/service/StartupChecks.java:691`).
8. FM-DAT-05 Corruption at start: out of scope. The layer is not running.
9. FM-JVM-10 G1 humongous allocations as a signal: out of scope. No MXBean (**assumption**); large partitions and mutations are covered elsewhere.

### B. Proposed diagnose only

10. FM-CPU-05 one pinned thread: diagnose only. No safe automatic fix; cost of per-thread CPU to be measured (#47).
11. FM-MEM-03 swapping: diagnose only, as a correlator cause. No in-JVM fix; prevents wrong cache steps.
12. FM-MEM-05 off-heap growth with data: diagnose only. The fix is a schema change.
13. FM-MEM-06 native memory: diagnose only, estimated from RSS. NMT cost unmeasured.
14. FM-JVM-06 metaspace: diagnose only. Rare.
15. FM-JVM-07 direct memory: diagnose only. No runtime knob shrinks the direct pools; the chunk cache is off by default anyway.
16. FM-JVM-11 thread deadlock (new): diagnose only, CRITICAL, with stacks. Recommend no automatic restart.
17. FM-DSK-03, FM-DSK-04 hints and saved caches volumes: diagnose only. Cheap once the commit log volume is read.
18. FM-DSK-06 slow disk without load: diagnose only plus advertise severity. Nothing local fixes a slow device.
19. FM-DSK-09 log and heap-dump volume (new), FM-DSK-10 inodes (new), FM-DSK-11 cgroup I/O limit (new): diagnose only. Deleting dumps or logs stays manual; `io.max` is host configuration.
20. FM-OS-03 map count: diagnose only. Rare; reading `/proc/self/maps` cost to check.
21. FM-NET-01 to FM-NET-06 network, TLS and listen-queue overflow (FM-NET-06 new): diagnose only. No network action exists.
22. FM-WRT-01 slow fsync, FM-WRT-04 flush backlog, FM-WRT-06 oversized mutations: diagnose only, with correlator rules so the ladder does not grow pools.
23. FM-WRT-07 batchlog backlog (new), FM-CLU-17 hint delivery failing (new): diagnose only. Evidence for peer and consistency rows.
24. FM-WRT-08 Paxos / LWT subsystem (new): diagnose only; a linearizability violation is CRITICAL. Paxos v2 is off by default (`Config.java:1215`).
25. FM-RD-03 cold large partitions, FM-RD-07 speculative retries, FM-RD-08 bloom false positives: diagnose only. Fixes are schema or elsewhere.
26. FM-RD-06 read repair storm: diagnose only. Recommend no repair-priority trigger now; revisit with data from T7.23.
27. FM-RD-10 secondary index and SAI load (new): diagnose only. Sub-question: should the yield step also cover index builds? Recommend yes, since builds already run as active compactions (`C/db/compaction/CompactionManager.java:2109-2115`).
28. FM-CMP-03 compaction headroom, FM-CMP-04 wrong strategy, FM-CMP-05 stuck compaction: diagnose only. Strategy change is out of MVP.
29. FM-TP-02 internal pools, FM-TP-04 outbound backlog: diagnose only. Actions live in other rows.
30. FM-CLI-02 connection storm, FM-CLI-03, -04, -06, -07 client misuse, FM-CLI-08 prepared-statement thrash (new): diagnose only. The layer does not manage clients (overview 1.2). Recommend no connection-limit knob (effect on existing connections **unverified**) and no prepared-statement cache resize (it takes heap from memtables). FM-CLI-03 needs `diagnostic_events_enabled` or a new hook (ADR-013 forbids request-path hooks).
31. FM-CLU-01, -02, -04, -08, -10, -11, -12, -13, -14, -16 cluster state: diagnose only. Cassandra already routes around peers; topology actions are out of MVP.
32. FM-DAT-01 corruption, FM-DAT-03 TTL build-up, FM-DAT-04 repaired inconsistencies: diagnose only. Scrub and repair choices stay with operators.

### C. Act, but needs a new actuator, guard or action path

33. FM-JVM-05, FM-WRT-02, FM-DSK-02: build a memtable flush actuator (`StorageService.forceKeyspaceFlush`, `C/service/StorageService.java:3111`). Recommend yes. Reason: memtables are the default largest heap user; caches the layer can resize hold at most about 150 MiB with defaults; it is the only local fix for a full commit log.
34. FM-DAT-02, FM-DSK-01: snapshots. Recommend the diagnosis names `auto_snapshot_ttl` (unset by default, `Config.java:402`) as the first remedy, and that the irreversible snapshot-clearing actuator is **not** built in D7.1. Reason: a reversible configuration covers truncate and drop snapshots; the actuator would delete data an operator may need.
35. FM-TP-05 native transport in-flight bytes knob (RC-5): build it or drop it from overview 8.1. Reason: planned in D1, never built.
36. FM-CLU-06 repair priority after dropped hints: reuse `RepairPriorityActuator` from a new trigger. Recommend yes. Reason: the actuator exists; only the trigger is new.
37. FM-CLU-15 pause cluster actions during mixed versions: new gate. Reason: today only checked at enable time.
38. FM-OS-02, FM-OS-01, FM-DAT-06 (new): guards on pool growth (thread headroom) and on splits (FD headroom, table count). Recommend yes; owner to set the default table-count limit when the `tables` guardrail is off. Reason: FD exhaustion now kills the JVM (`C/utils/JVMStabilityInspector.java:166-175`).
39. FM-NODE-01 (new), FM-DSK-07: add transport state to the `NODE_STATE` gate; never restart transports automatically. Recommend yes. Reason: with the shipped `stop` policies a disk or commit error leaves the node alive with no clients, and verify windows would read noise.
40. FM-NODE-02 (new): observe-only window after an unclean exit; observe-only until cleared in a crash loop. Owner to set the window and the loop threshold. Reason: a layer action may have caused the crash.
41. FM-AUT-16 skip advertising severity when most peers are degraded. Recommend yes. Reason: equal severity leaves the order unchanged and weakens the snitch's badness test (`C/locator/DynamicEndpointSnitch.java:200-204, 303-304`).
42. FM-AUT-02 per-knob opposite-direction rule: new governor check. Reason: unproven but plausible oscillation across policies.
43. FM-AUT-17 monotonic clock for intervals: design fix. Reason: wall-clock jumps distort rates.
44. FM-JVM-01 correlator rules: steal (FM-CPU-02) and cgroup throttling (FM-CPU-03) as root causes of `gc_pressure`. Recommend yes. Reason: shrinking caches does not shorten pauses caused by lost CPU.
45. FM-CLI-01 check peer liveness and severity before treating an op-rate rise as client demand. Recommend yes. Reason: a peer down moves its load here; the ladder would throttle clients for a peer's failure.

### D. Unsure, needs a ruling

46. FM-AUT-07 suppression by causes without a policy: recommend treating `disk_pressure` as a diagnose-only cause in the ladder and keeping `degraded_peer` suppression, with the reason in the diagnosis summary. Reason: today a latency breach under disk pressure gets no action at all.
47. FM-CPU-01, FM-CPU-05, FM-JVM-08: recommend one cost spike for per-thread CPU sampling and JFR streaming first, then decide. Reason: both costs unmeasured; per-pool CPU decides whether the CPU gate can tell background from client load.
48. FM-OS-04 clock skew: recommend diagnose only from the cross-node estimate (receiver time minus sender timestamp), which needs a per-peer hook. Reason: the data already exists per DC; it also keeps FM-NET-04 from blaming a peer for skew. Revision 1 recommended out of scope.
49. FM-TP-03 dropped messages: recommend diagnose only, with advertise severity as an option. Reason: the throttle limits only this node's native clients (`A/execute/actuators/RequestThrottleActuator.java:107-125`); severity moves reads only.
50. FM-TP-06 executors without metrics: recommend an S task to list them, then decide. Reason: unknown which matter.
51. FM-AUT-04 and FM-AUT-05 recovery of yielded knobs and of severity: decide per knob (audit 4.1). Reason: sticky overrides were accepted in general, never for these knobs.
52. FM-CPU-02 steal: recommend "advertise only" for `degraded_self` when steal is the cause. Reason: other rungs cannot help.

52 decisions in all (rows grouped where the same reasoning applies): 9 in A, 23 in B, 13 in C, 7 in D.

## Mapping to the NEXT-STEPS plan

| Task | Rows it closes (fully or its share) |
|---|---|
| T7.16 memory sensor and cache resizing | FM-MEM-01, -02, -03, -04, -05, -06; FM-JVM-04, -05 (diagnosis), -07; FM-RD-05; FM-AUT-08 (P1), FM-AUT-09 (row cache, under-filled cache) |
| T7.17 pause-only GC signal | FM-JVM-01, -02, -03 (diagnosis); FM-AUT-06; FM-AUT-08 (P2) |
| R12 JDK 21 toolchain | proof for FM-JVM-02, -03 and FM-AUT-06 on ZGC |
| T7.18 CPU sensor | FM-CPU-01 (incl. cgroup `cpu.pressure`; per-pool CPU after #47), -02, -03, -04; FM-TP-01 (gate); FM-AUT-08 (P3) |
| T7.19 disk I/O sensor | FM-DSK-01 (per-directory usage), -02, -03, -04, -05 (incl. `/proc/self/io`, cgroup `io.pressure`), -06 (historical ceiling); FM-CMP-01 (gate, progress input), -02 (bottleneck) |
| T7.20 inputs already collected | FM-CLI-05 (and its aliases FM-WRT-05, FM-RD-09); FM-RD-01; FM-AUT-03; FM-CLU-04 (unavailables) |
| T7.21 write path and messaging | FM-WRT-01, -02 (diagnosis), -03, -04; FM-TP-02, -03, -04; FM-NET-04; FM-CLU-02 (internode latency and backlog as evidence) |
| T7.22 hints, streaming, client connections | FM-CLU-05, -06 (diagnosis), -07; FM-TP-05 (observation); FM-CLI-02, -06; FM-AUT-09 (idle hints and streams) |
| T7.23 small inputs | FM-OS-01 (diagnosis); FM-DSK-07 (error count); FM-CLU-11; FM-DAT-02 (observation); FM-RD-06, -07 |
| T7.24 unbuilt design items | FM-CMP-02 (compactors DOWN, RC-2); FM-TP-01 (RC-8), FM-TP-05 (RC-5); FM-CLU-03 (RC-7); FM-AUT-04 (RC-3), FM-AUT-05 (RC-4) |
| T7.25 real-pressure scenarios | proof for MEM, JVM, CPU, DSK rows that act; FM-AUT-22 |
| T7.26 gate checklist | the process in "Keeping it current"; coverage table at the D7.1 gate |

Scope added to costed tasks by this revision (inside their NEXT-STEPS ranges is **assumption**; the total below adds 4–10 units to be safe): T7.18 cgroup PSI and per-pool CPU; T7.19 per-directory usage, the progress input, `/proc/self/io`; T7.21 FM-TP-02, FM-CLU-02 and the skew rule-out.

New tasks. Tier and cost units follow `docs/NEXT-STEPS.md` (units are model tokens weighted by list price, sized from sprint 1 tasks of the same kind: haiku small tasks 2–4, sonnet standard tasks 6–12, opus design with build 60–90). Each includes the worker's run and the R11 review of its diff, as T7.15 did; the D7.1 gate review stays in T7.26.

| New task | Rows | Tier | Units |
|---|---|---|---|
| **T7.27** correlator and ladder fixes: causes without a policy (#46), herd rule (#41), steal and throttling as GC causes (#44), peer liveness before client demand (#45), slow fsync as cause (FM-WRT-01), test that a false cause cannot suppress the ladder | FM-AUT-07, FM-AUT-16, FM-AUT-06 (test), FM-JVM-01 (rules), FM-CLI-01 (check) | sonnet | 6–12 |
| **T7.28** OS limits and guards: threads and pids, map count; guards on pool growth and on splits (FDs, table count) | FM-OS-01 (guard), FM-OS-02, FM-OS-03, FM-DAT-06 | haiku sensors, sonnet guards | 4–8 |
| **T7.29** cluster state diagnoses: peer down, quorum risk, DC loss, CMS health, gossip flapping, bootstrap stuck, repair failing, hint delivery failing, batchlog backlog, mixed-version gate | FM-CLU-01, -04, -08, -10, -12, -14, -15, -16, -17; FM-NET-03; FM-WRT-07; FM-AUT-15 (CMS diagnosis) | sonnet | 10–18 |
| **T7.30** new actuators after rulings: memtable flush (#33), repair priority from dropped hints (#36); snapshot clearing only if #34 rules for it (+10–15) | FM-JVM-05 (act), FM-WRT-02, FM-DSK-02 (act), FM-CLU-06 (act); FM-DAT-02 (act, conditional) | opus design and review, sonnet build | 25–45 |
| **T7.31** data, index and compaction diagnoses: corruption, repaired inconsistencies, TTL build-up, bloom false positives, large cold partitions, SAI and index load, stuck compaction (progress input), wrong strategy, compaction headroom | FM-DAT-01, -03, -04; FM-RD-03, -08, -10; FM-CMP-03, -04, -05 | sonnet | 10–18 |
| **T7.32** client misuse and LWT diagnoses: guardrail events, batches, oversized mutations, LWT contention, Paxos health, prepared-statement thrash | FM-CLI-03, -04, -07, -08; FM-WRT-06, -08 | sonnet | 8–14 |
| **T7.33** network and host extras: retransmits, NIC, TLS expiry, listen overflows, one pinned thread; per-peer skew estimate if #48 rules for it | FM-NET-01, -02, -05, -06; FM-CPU-05; FM-OS-04 (conditional) | haiku; sonnet for the skew hook | 4–10 |
| **T7.34** layer self-protection: overhead budget and self-diagnosis, monotonic intervals, opposite-direction rule | FM-AUT-11, FM-AUT-17, FM-AUT-02 | sonnet | 6–12 |
| **T7.35** JVM extras after rulings: cost spike for JFR and per-thread CPU (#47), ZGC stalls, safepoints, metaspace, deadlock, executors without metrics | FM-JVM-03 (stall signal), -06, -08, -11; FM-TP-06 | opus spike, then sonnet | 8–15 |
| **T7.36** node lifecycle (new): transport state in `NODE_STATE` (#39), unclean-exit and crash-loop detection at start, observe-only window (#40) | FM-NODE-01, FM-NODE-02; FM-DSK-07 (guard) | sonnet build, opus review (governor change) | 8–15 |
| **T7.37** volumes and cgroup I/O (new): log and dump volume, inodes through JNA `statvfs`, cgroup `io.max` | FM-DSK-09, -10, -11 | sonnet | 4–8 |

Rows left with no task on purpose: the out-of-scope proposals in group A (FM-OPT-01, -02, -03, FM-CPU-06, FM-MEM-07, FM-JVM-09, FM-JVM-10, FM-DSK-08, FM-DAT-05).

### Revised cost of increment 2

| Part | Units |
|---|---|
| NEXT-STEPS steps 1–13 and orchestration, as costed there | 260–460 |
| Scope added to T7.18, T7.19, T7.21 (raw 4–10, plus 20–25% orchestration) | 5–13 |
| New tasks T7.27–T7.37 (raw 93–175, plus 20–25% orchestration as in NEXT-STEPS) | 112–219 |
| **Total** | **about 380–690** |

That is about 1.1 to 2.0 weekly budgets of 350 units, against about 0.75 to 1.3 before. Rulings can lower it: #34 against the snapshot actuator is already assumed; #47 against JFR leaves T7.35 at about 4–8; #48 against the skew estimate takes about 2–4 off T7.33. Machine time for the real-pressure scenarios (T7.25) is unchanged and still the slower constraint. **Assumption**: the new tasks cost like sprint 1 tasks of the same tier; none has been measured.

## Not verified

- Behaviour of the load average and `getAvailableProcessors` inside a container (FM-CPU-01, -03): from documentation, not run in a container (audit 7).
- JDK 21 collector bean names and what JDK 17 G1 reports under "G1 Old Generation"; whether JDK 20+ adds "G1 Concurrent GC" (FM-JVM-01, -02, note 1 of section 3).
- That ZGC allocation stalls appear in no MXBean, and the JFR event name (FM-JVM-03).
- That no MXBean counts G1 humongous allocations (FM-JVM-10).
- Cost of per-thread CPU sampling (FM-CPU-05), JFR streaming (FM-JVM-08), NMT (FM-MEM-06), reading `/proc/self/maps` on a large node (FM-OS-03), and the `trueSnapshotsSize` gauge (FM-DAT-02).
- `cpufreq` presence in guests (FM-CPU-06); NIC speed on VMs (FM-NET-02); `MutualTlsMetrics` without mutual TLS (FM-NET-05).
- Whether `StorageProxy.setNativeTransportMaxConcurrentConnections` affects existing connections (FM-CLI-02).
- A metric for droppable tombstones per sstable (FM-DAT-03).
- That opposite-direction steps by two policies can follow each other (FM-AUT-02, note 1 of section 14), and that a forward clock jump only dilutes one interval (FM-AUT-17).
- That a spurious `gc_pressure` suppresses the ladder in a real run (FM-AUT-06; follows from the code, audit 7).
- That FM-AUT-07's suppression happens in a real run: follows from `A/AutonomicService.java:646`, `A/analyze/RuleCorrelator.java:87-92, 109-112` and `A/plan/policies/LoadSpikePolicy.java:233`, not run.
- Which executors without JMX metrics matter (FM-TP-06).
- Added in revision 2:
  - That `%util` is meaningless on NVMe and parallel devices (FM-DSK-05), and that burst-credit exhaustion shows only as a lower IOPS cap and higher latency (FM-DSK-06): from the kernel's `io_ticks` definition and cloud documentation, not measured.
  - That a non-zero commit log replay count means the previous stop skipped a drain, and that `memory.events` survives a service restart (FM-NODE-02).
  - That the JDK `FileStore` exposes no inode counts (FM-DSK-10); whether logback blocks or drops on a full volume (FM-DSK-09).
  - The listen backlog rule (FM-NET-06); the skew estimation method and a cheap per-peer hook (FM-OS-04).
  - Cost of `BatchlogManagerMBean.countAllBatches` on a large backlog (FM-WRT-07) and of `findDeadlockedThreads` on many threads (FM-JVM-11); per-table heap cost (FM-DAT-06).
  - What Paxos v2 exposes beyond `PaxosMetrics` (FM-WRT-08); whether SAI builds take the `submitIndexBuild` path (FM-RD-10); metric names for MVs and CDC, and how `cdc_total_space` 0 is resolved (FM-OPT-02, -03).
  - The cost units of T7.27–T7.37: sized by analogy with sprint 1, not measured.
- `io.max` (FM-DSK-11) cannot be exercised on this machine: the `io` controller is not enabled (measured).
- No build or test was run for this document.
