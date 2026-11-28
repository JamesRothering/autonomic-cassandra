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

# Observability coverage audit (postmortem R1)

- **Date:** 2026-11-23. **Branch:** `deliverable/D7-verification` at `784c6e3`, plus the R2 change committed with this audit.
- **Asked by:** the owner: the layer "needs full observability of everything down to the metal. It shouldn't guess. It needs to know."
- **Scope:** postmortem `docs/postmortems/2026-11-22-cache-and-memory-near-miss.md`, recommendation R1, with a preview of R3 (section 4) and R5 (section 5). The cache hit-rate work (T7.15, ADR-046) is referenced, not repeated.
- **Automated check:** `test/unit/org/apache/cassandra/autonomic/InputCoverageTest.java` (R2). Its allowlists point at the items marked **RC-n** below.

## How to read this document

Paths: `A/` is `src/java/org/apache/cassandra/autonomic/`, `S/` is `A/monitor/sensors/`, `C/` is `src/java/org/apache/cassandra/`. `file:line` refers to the code at the commit above.

Evidence rules. A claim about what the layer observes cites `S/Signals.java` or a sensor. A claim about what Cassandra, the JVM or the OS exposes cites the Cassandra class, the JMX bean, or the `/proc` or cgroup file. JVM and OS facts marked *(measured)* were checked on this machine: JDK 17.0.21, Linux 6.8, cgroup v2 (script output quoted where it matters). Anything not checked is marked **unverified**.

Classes:

- **missing-needed**: an existing or planned action, diagnosis or guard lacks this input, or uses a wrong one.
- **missing-diagnostic**: needed to name a cause the layer cannot name today.
- **nice-to-have**: useful, but no planned action or diagnosis depends on it.
- **not observable**: not available from inside the JVM without new native code or running a process; the proof is given.
- **covered**: observed, and the observation is used.

## 1. What the layer observes today

### 1.1 Sensors

All production sensors are registered in `A/AutonomicService.java:179-183`. The monitor thread ticks every `monitor_interval` (default 5 s, `A/AutonomicConfig.java:40`) and calls a sensor only when its `minIntervalMillis()` has passed (`A/AutonomicService.java` monitor tick, the `lastCollected` check). So the effective interval is the larger of the two. Observations are kept for `observation_retention` (30 min, `A/AutonomicConfig.java:43`).

| Sensor | Interval | Source (Cassandra / JVM / OS) | Signals (`S/Signals.java`) | Subject |
|---|---|---|---|---|
| `TableSensor` (`S/TableSensor.java`) | 5 s (`:71`) | `TableMetrics` of each user table (`:100-129`): `readLatency`, `writeLatency`, `rangeLatency`, `coordinatorRead/WriteLatency`, `sstablesPerReadHistogram`, `tombstoneScannedHistogram`, `liveScannedHistogram`, `pendingCompactions`, `liveSSTableCount`, `pendingFlushes`, `max/meanPartitionSize`, `tombstoneWarnings/Failures` | `:64-86` (23 signals) | TABLE (user tables, no indexes, `:83`) |
| `ClientRequestSensor` (`S/ClientRequestSensor.java`) | 5 s (`:63`) | `ClientRequestsMetricsHolder.readMetrics`, `writeMetrics`, `casReadMetrics`, `casWriteMetrics` (`:71-82`): count, total latency, p99, timeouts, unavailables, failures | `:89-112` (24) | NODE |
| `ThreadPoolSensor` (`S/ThreadPoolSensor.java`) | 5 s (`:57`) | `CassandraMetricsRegistry.Metrics.allThreadPoolMetrics()` (`:64`): active, pending, currently blocked, total blocked, max pool size, oldest task queue time | `:115-120` (6) | THREAD_POOL, every pool registered with JMX metrics |
| `CompactionSensor` (`S/CompactionSensor.java`) | 5 s (`:55`) | `CompactionManager.instance.getMetrics()`: `pendingTasks`, `bytesCompacted` (`:63-66`) | `:123-124` (2) | NODE |
| `DiskSensor` (`S/DiskSensor.java`) | 30 s (`:62`) | `java.nio.file.FileStore` of each **data** directory (`:71-81`) | `:127-129` (3) | NODE |
| `GcSensor` (`S/GcSensor.java`) | 5 s (`:62`) | every `GarbageCollectorMXBean`, **summed** (`:71-81`); `MemoryMXBean.getHeapMemoryUsage()` (`:87-90`) | `:132-135` (4) | NODE |
| `CpuSensor` (`S/CpuSensor.java`) | 5 s (`:56`) | `OperatingSystemMXBean.getSystemLoadAverage()` / `getAvailableProcessors()` (`:62-65`) | `:138` (1) | NODE |
| `PeerSensor` (`S/PeerSensor.java`) | 10 s (`:70`) | `Gossiper.instance.getEndpointStates()` (`:79`): `LOAD`, `SEVERITY`, alive; `DynamicEndpointSnitch` scores (`:103-104`) | `:141-144` (4) | PEER |
| `HotPartitionSensor` (`S/HotPartitionSensor.java`) | 1 s (`:153`), sampling windows from `autonomic.sampling` | `ColumnFamilyStore.beginLocalSampling` and the samplers `topRead/WritePartitionFrequency` (`:255`, `:279-285`); `PartitionSizeEstimator` (`:323`) | `:147-154` (7) | PARTITION, TABLE |
| `LayoutSensor` (`S/LayoutSensor.java`) | 5 s (`:80`) | the layer's own `LayoutProfiler` counters for watched keys | `:157-166` (9) | PARTITION, TABLE |

83 signals in all, every one emitted by a registered sensor (checked by `InputCoverageTest.sensorsComeFromTheMainTreeAndEmitOnlyWhatTheyDeclare`). One more signal is derived and never stored: `repair.overdue_ratio` (`A/analyze/detectors/RepairOverdueDetector.java:76`), computed by the detector from auto-repair history on a background read (`:280-300`).

### 1.2 Emitted but read by nothing

These are collected every 5 s and read by no detector, policy, guard or actuator (search of `A/` outside `S/` for each constant): table write, range and coordinator op rates; all table and client *mean* latencies; `table.range_latency_p99_micros`; `table.sstables_per_read_p99`; `table.live_cells_scanned_p99`; `table.pending_compactions`; `table.live_sstable_count`; `table.pending_flushes`; `table.max/mean_partition_size_bytes`; **all client timeouts, unavailables and failures** (`S/Signals.java:92-94, 98-100, 104-106, 110-112`); CAS p99s; `pool.blocked_tasks_delta`; `gc.collections_delta`. The layer collects the error rates a client sees but no detector looks at them (item CLI-2).

### 1.3 Consumers and what they read

Declared inputs after R2 (`Detector.requires()`, `Policy.inputs()`), each checked against the bytecode by `InputCoverageTest`:

| Consumer | Reads |
|---|---|
| `hot_partition` detector | partition shares, op rates, size |
| `latency_slo_breach` | table and client read/write p99 |
| `tombstone_overload` | tombstones scanned p99, warning and failure deltas |
| `compaction_backlog` | `compaction.pending_tasks`, `compaction.bytes_per_sec` (evidence) |
| `disk_pressure` | `disk.used_fraction`, free and total bytes (evidence) |
| `gc_pressure` | `gc.time_fraction`; heap used fraction and GC ms (evidence only) |
| `pool_saturation` | pending, active, max size, blocked, oldest task |
| `node_imbalance`, `degraded_peer`, `degraded_self` | peer load, alive, severity, snitch score; client p99 |
| `capacity_exhausted` | `disk.used_fraction`, `pool.pending_tasks` |
| `repair_overdue`, `baseline_drift`, split outcome detectors | no signals (history, knob state, cluster metadata) |
| policies | the goals and guards above; `os.load_per_processor` (pool growth gate); client op rates (throttle, guard traffic) |

Before R2, five detectors read signals they did not declare (evidence reads): `compaction_backlog`, `disk_pressure`, `gc_pressure`, `degraded_peer` (`peer.severity`) and `hot_partition` (op rates and size). Their declarations are now complete. `DegradedSelfPolicy` (`A/plan/policies/DegradedSelfPolicy.java:40`) is not registered anywhere (`A/plan/policies/Policies.java:101-113`; the ladder answers `degraded_self`): dead code, item RC-7.

## 2. Coverage by resource

One row per item. "Layer" is what the layer observes; "Available" is where the data is; "Why" names the detector, policy or guard it would feed, or the cause it would let the layer name.

### 2.1 CPU

| ID | Item | Layer | Available | Gap and why it matters | Class |
|---|---|---|---|---|---|
| CPU-1 | Utilisation (user, system) | Only the 1-minute load average per processor (`S/Signals.java:138`, `S/CpuSensor.java:62-65`) | `/proc/stat` `cpu` line (user, nice, system, idle, iowait, irq, softirq, steal) *(measured, present)*; process CPU: `com.sun.management.OperatingSystemMXBean.getProcessCpuLoad()`, `getCpuLoad()` *(measured, return values)*; JVM threads: `ThreadMXBean.getThreadCpuTime` | `RequestPoolPolicy` grows a pool only while "CPUs are not saturated" (`A/plan/policies/RequestPoolPolicy.java:48, 60, 122-125`), judged from load average, which is not utilisation (see 5, P3). Overview 8.1's pool rule ("up when pendingTasks is sustained and CPU is not saturated") needs real utilisation. | missing-needed |
| CPU-2 | I/O wait | None; load average mixes runnable (R) and uninterruptible (D) tasks, per `proc_loadavg(5)`: "number of jobs in the run queue (state R) or waiting for disk I/O (state D)" | `/proc/stat` iowait column; `/proc/pressure/io` (PSI) *(measured, present)* | An I/O-bound node looks CPU-saturated, so the pool step is skipped and the wrong rung of the ladder is tried. Needed to tell disk from CPU as the cause of `latency_slo_breach`. | missing-diagnostic |
| CPU-3 | Steal time | None | `/proc/stat` steal column *(measured, present)* | On VMs a noisy neighbour shows as latency with no local cause; the layer would yield background work and resize pools for nothing. Names "degraded host" as cause of `degraded_self`. | missing-diagnostic |
| CPU-4 | Run queue / scheduler pressure | Indirectly in load average | `/proc/pressure/cpu` (PSI `some`), `/proc/loadavg` field 4 (runnable count) | Better saturation signal than load average for the pool gate. | missing-needed (with CPU-1) |
| CPU-5 | cgroup CPU limit and throttling | None | cgroup v2 `cpu.max`, `cpu.stat` (`nr_throttled`, `throttled_usec`) of the process's cgroup from `/proc/self/cgroup` *(measured, both present)* | In a container, throttling causes latency spikes that look like load. Also, `getSystemLoadAverage()` is the host's load while `getAvailableProcessors()` honours the container quota, so `os.load_per_processor` is host load over container CPUs: meaningless in a container (the load average is system-wide per `proc_loadavg(5)`; the behaviour inside a container was **not tested** here). | missing-needed |

### 2.2 Memory

| ID | Item | Layer | Available | Gap and why it matters | Class |
|---|---|---|---|---|---|
| MEM-1 | Heap, total | `gc.heap_used_fraction` = used / max (`S/Signals.java:135`, `S/GcSensor.java:87-90`), evidence only for `gc_pressure` | `MemoryMXBean` | Used heap includes garbage not yet collected, so it is a sawtooth, not pressure. Occupancy *after* GC is the pressure signal (MEM-2). | missing-needed |
| MEM-2 | Heap pools (eden, survivor, old) and after-GC occupancy | None | `MemoryPoolMXBean` per pool; `getCollectionUsage()` is supported on heap pools *(measured: G1 Eden, Survivor, Old Gen and ZHeap report `isCollectionUsageThresholdSupported=true`)* | "Heap headroom", the input of the planned cache UP direction (overview 8.1), is old-gen occupancy after GC. Without it the UP direction cannot be guarded (RC-1). | missing-needed |
| MEM-3 | GC per collector, pause vs concurrent | One sum over all collectors (`S/GcSensor.java:71-81`) | Each `GarbageCollectorMXBean`; GC notifications with `GcInfo` (as `C/service/GCInspector.java` uses) | Summing is wrong for concurrent collectors; see 5, P2. `gc_pressure` (`A/analyze/detectors/GcPressureDetector.java`) can open on concurrent cycle time and shrink caches. | missing-needed |
| MEM-4 | Metaspace, code cache | None | `MemoryPoolMXBean` "Metaspace", "Compressed Class Space", "CodeHeap ..." *(measured)* | Rarely a cause; a metaspace leak ends in OOM. | nice-to-have |
| MEM-5 | Direct buffers | None | `BufferPoolMXBean` "direct" *(measured)*; Cassandra's own pools: `C/metrics/BufferPoolMetrics.java:31-50` (hits, misses, capacity, size, usedSize, overflowSize) for the chunk cache and networking pools; `C/metrics/StorageMetrics.java:56-57` (direct write buffers) | Direct memory exhaustion fails reads and writes (`C/utils/memory/BufferPool.java:470` logs "Make sure direct memory size (-XX:MaxDirectMemorySize) is large enough"). The limit itself is only in `jdk.internal.misc.VM.maxDirectMemory()` (internal API, **unverified** whether reachable without `--add-exports`) or the `-XX:MaxDirectMemorySize` input argument (`RuntimeMXBean.getInputArguments()`). | missing-needed |
| MEM-6 | Mapped buffers | None | `BufferPoolMXBean` "mapped" *(measured)*; `disk_access_mode` default `mmap_index_only` (`C/config/Config.java:132`). That Cassandra's `FileChannel.map` regions are counted there is JDK behaviour, **unverified** for Cassandra here. | Mapped index files compete with the page cache and RSS. | missing-diagnostic |
| MEM-7 | Memtables (heap and off-heap) | None (`table.pending_flushes` is emitted but unused) | Per table `TableMetrics.memtableOnHeapDataSize`, `memtableOffHeapDataSize`, `memtableLiveDataSize`, `allMemtables*` (`C/metrics/TableMetrics.java:104-114`); pool-wide `AbstractAllocatorMemtable.MEMORY_POOL` (`C/db/memtable/AbstractAllocatorMemtable.java:63`) with `onHeap`/`offHeap` `used()`, `usedRatio()`, `reclaimingRatio()`, `limit` (`C/utils/memory/MemtablePool.java:108, 204-226`) | Memtables are on heap by default (`memtable_allocation_type: heap_buffers`, `C/config/Config.java:637`), so they are the usual largest heap consumer. Without them `gc_pressure` cannot tell memtables from caches as the cause, and its cache steps may be the wrong remedy. | missing-diagnostic |
| MEM-8 | Chunk cache | None | `ChunkCache.instance.metrics` (`C/cache/ChunkCache.java:53, 58`; `C/metrics/ChunkCacheMetrics.java:36-40`, a `CacheMetrics`: capacity, size, entries, hits, misses, `missLatency`) | Off-heap (buffers from `BufferPools.forChunkCache()`, `C/cache/ChunkCache.java:53`). **Not resizable at runtime**: `setCapacity` throws "Chunk cache size cannot be changed." (`C/cache/ChunkCache.java:311-314`). Observation only. | missing-diagnostic (hit rate in T7.15) |
| MEM-9 | Key, row, counter caches: capacity, size, entries | None | `CacheService.instance.keyCache/rowCache/counterCache` (`C/service/CacheService.java:103-105`), `getMetrics()` (`C/cache/InstrumentingCache.java:121`), `C/metrics/CacheMetrics.java:33-39`, `C/metrics/AbstractCacheMetrics.java:42-67` (hits, misses, hit rates) | The cache actuators move capacity (`A/execute/actuators/CacheCapacityActuator.java`) with no observation of size or occupancy; a step down on an under-filled cache frees nothing. Hit rate is T7.15. | missing-needed |
| MEM-10 | Bloom filters | None | `C/io/sstable/filter/BloomFilterMetrics.java:60-94` (off-heap memory, disk space, false ratio, recent false ratio), per table through `TableMetrics.formatSpecificGauges` (`C/metrics/TableMetrics.java:333`) | Off-heap memory that grows with sstable count; a high false ratio is a cause of read latency (extra sstables read). | missing-diagnostic |
| MEM-11 | Index summaries | None | `C/io/sstable/indexsummary/IndexSummaryMetrics.java:40` (off-heap memory; BIG format only) | Off-heap; bounded by `index_summary_capacity` (`C/config/Config.java:661`). | nice-to-have |
| MEM-12 | Compression metadata | None | `TableMetrics.compressionMetadataOffHeapMemoryUsed` (`C/metrics/TableMetrics.java:176, 849`) | Off-heap, grows with data. Part of the off-heap total. | nice-to-have (part of T7.16 total) |
| MEM-13 | Process RSS | None | `/proc/self/status` `VmRSS`, `RssAnon`, `RssFile`, `VmSwap` *(measured)*; `/proc/self/smaps_rollup` | The only number that covers heap + all native memory. The kernel kills on RSS, not on heap. | missing-needed |
| MEM-14 | OS available memory and page cache | None | `/proc/meminfo` `MemAvailable`, `Cached`, `Dirty`, `Writeback` *(measured)*; `com.sun.management.OperatingSystemMXBean.getFreeMemorySize()`, `getTotalMemorySize()` *(measured)* | Reads of sstables rely on the page cache; shrinking it (larger caches, more off-heap) costs read latency. Input of a cache UP guard ("available memory"; owner's ADR-047 outline). | missing-needed |
| MEM-15 | Swap use and swap-in rate | None | `/proc/meminfo` `SwapTotal/SwapFree`, `/proc/vmstat` `pswpin/pswpout`, `pgmajfault` *(measured)*; `getFreeSwapSpaceSize()` *(measured)* | A swapping JVM has GC pauses that no in-JVM action fixes; names the cause of `gc_pressure` and `degraded_self`. | missing-diagnostic |
| MEM-16 | cgroup memory limit, usage, OOM events | None | cgroup v2 `memory.max`, `memory.current`, `memory.events` (`oom`, `oom_kill`, `high`), `memory.stat`, `memory.pressure` *(measured, all present)*; `/proc/pressure/memory` | In a container the limit, not `MemTotal`, is the ceiling. Growing a cache without it risks an OOM kill. | missing-needed |
| MEM-17 | Native memory of the JVM itself (threads, GC structures) | None | Native Memory Tracking via `DiagnosticCommand` MBean `vmNativeMemory`, only when started with `-XX:NativeMemoryTracking` (not in `conf/jvm-server.options`, **unverified** cost) | RSS minus known pools is a usable estimate without NMT. | nice-to-have |

### 2.3 Disk

| ID | Item | Layer | Available | Gap and why it matters | Class |
|---|---|---|---|---|---|
| DSK-1 | Space of data directories | `disk.used_fraction`, free, total (`S/Signals.java:127-129`, `S/DiskSensor.java:71-81`) | as observed | Feeds `disk_pressure`, `capacity_exhausted`, split policy free-space check. | covered |
| DSK-2 | Space of the commit log, hints and saved caches directories | None: only `getAllDataFileLocations()` (`S/DiskSensor.java:71`) | `commitlog_directory`, `hints_directory`, `saved_caches_directory` (`C/config/Config.java:126, 482, 485`); `FileStore` as for data | A full commit log volume stops the node (`commit_failure_policy: stop`, `C/config/Config.java:135`) while `disk_pressure` sees nothing if it is a separate volume. | missing-needed |
| DSK-3 | Throughput, IOPS, latency, queue depth, utilisation per device | None | `/proc/diskstats` per device (reads, sectors, ms reading, writes, ms writing, in-flight, io ticks, weighted ms) *(measured, present)*; map directory to device with `/proc/self/mountinfo` *(measured, present)*; `/proc/pressure/io` | The ladder yields compaction to protect the foreground without knowing whether the disk is the bottleneck. Names "disk saturated" as the cause of latency and of `compaction_backlog`. Verifies a compaction throughput step (did the device's utilisation drop?). | missing-needed |
| DSK-4 | Per-table disk use | None (TABLE_LIVE_SSTABLE_COUNT emitted, unused) | `TableMetrics.liveDiskSpaceUsed`, `totalDiskSpaceUsed`, `trueSnapshotsSize` (`C/metrics/TableMetrics.java:162-166, 196`) | Snapshots silently filling the disk is a common cause of `disk_pressure` the layer cannot name. | missing-diagnostic |

### 2.4 Network and internode messaging

| ID | Item | Layer | Available | Gap and why it matters | Class |
|---|---|---|---|---|---|
| NET-1 | NIC bytes, errors, drops | None | `/proc/net/dev` *(measured)* | Saturated or erroring NIC as cause of internode latency. | nice-to-have |
| NET-2 | TCP retransmits | None | `/proc/net/snmp` `Tcp: RetransSegs`, `OutSegs` *(measured)* | System-wide; names a lossy network. | nice-to-have |
| MSG-1 | Dropped messages by verb | None | `MessagingService.instance().metrics.getDroppedMessages()` (`C/metrics/MessagingMetrics.java:181`), per verb `DroppedMessageMetrics.dropped`, internal/cross-node dropped latency (`C/metrics/DroppedMessageMetrics.java:59-65`) | Dropped MUTATION/READ is the canonical overload symptom; the layer cannot see it, so load shedding (throttle) never knows replicas are dropping work. | missing-needed |
| MSG-2 | Internode latency | None (only the coordinator's own p99 and snitch scores) | `MessagingMetrics.DCLatencyRecorder` `dcLatency`, `allLatency` (`C/metrics/MessagingMetrics.java:57-60`), `internalLatency` per verb (`:99`) | `degraded_peer` uses snitch scores only (`A/analyze/detectors/DegradedPeerDetector.java:58-61`); internode latency would let it name network vs peer slowness. | missing-diagnostic |
| MSG-3 | Outbound queue backlog per peer | None | `InternodeOutboundMetrics` per peer: pending tasks and bytes, dropped (by timeout, overload, error) for small, large and urgent connections, expired callbacks (`C/metrics/InternodeOutboundMetrics.java:37-108`), created per peer (`C/net/OutboundConnections.java:112`) | A growing backlog to one peer is the early signal of `degraded_peer`. | missing-diagnostic |
| MSG-4 | Connection state | `peer.alive` from gossip (`S/Signals.java:143`) | outbound connection state via `OutboundConnections` (**unverified** accessor) | Gossip alive is coarse. | nice-to-have |

### 2.5 Write path: commit log, memtables, flushing

| ID | Item | Layer | Available | Gap and why it matters | Class |
|---|---|---|---|---|---|
| CL-1 | Commit log pending tasks, total size | None | `CommitLog.instance.metrics` (`C/db/commitlog/CommitLog.java:87`): `pendingTasks`, `completedTasks`, `totalCommitLogSize` (`C/metrics/CommitLogMetrics.java:38-42`) | Names a commit log that falls behind as the cause of write latency. | missing-diagnostic |
| CL-2 | Waiting on segment allocation, on commit (sync), on flush | None | Timers `waitingOnSegmentAllocation`, `waitingOnCommit`, `waitingOnFlush` (`C/metrics/CommitLogMetrics.java:44-48`); `oversizedMutations` (`:50`) | Direct evidence of a write stall; none of the layer's write-path actions (pool resize, throttle) can fix a slow commit log disk, so the ladder should not try them. | missing-needed |
| MT-1 | Pending flushes | `table.pending_flushes` (`S/Signals.java:82`), read by nothing | `TableMetrics.pendingFlushes` (`C/metrics/TableMetrics.java:144`), incremented when a flush is created and decremented when it completes (`C/db/ColumnFamilyStore.java:1242, 1207`); pool-wide `MemtablePool.numPendingTasks` "PendingFlushTasks" (`C/utils/memory/MemtablePool.java:53, 66`) | Emitted but no detector; see MT-2. | missing-diagnostic |
| MT-2 | Memtable allocation blocking | None | `MemtablePool.blockedOnAllocating` timer "BlockedOnAllocation" (`C/utils/memory/MemtablePool.java:52, 65`) | Writes blocked because flushing cannot keep up: a cause of write latency that only more flush/disk capacity fixes. | missing-needed |
| MT-3 | Flush throughput | None | `TableMetrics.bytesFlushed`, `flushSizeOnDisk` (`C/metrics/TableMetrics.java:146-148`); `MemtableFlushWriter` pool (covered by `ThreadPoolSensor`) | The pool is observed (`pool.*`), its throughput is not. | nice-to-have |

### 2.6 Compaction

| ID | Item | Layer | Available | Gap and why it matters | Class |
|---|---|---|---|---|---|
| CMP-1 | Pending tasks, throughput | `compaction.pending_tasks`, `compaction.bytes_per_sec` (`S/Signals.java:123-124`) | as observed | Feeds `compaction_backlog` and the goal of `CompactionBacklogPolicy`. | covered |
| CMP-2 | Per-table pending compactions, sstable count, sstables per read | Emitted (`S/Signals.java:77, 80-81`), read by nothing | as observed | Would let `latency_slo_breach` on a table be correlated to *that* table's backlog (the correlator rule `compaction_backlog` causes table latency is node-scoped today). | missing-diagnostic |
| CMP-3 | Aborted / reduced compactions, completed | None | `CompactionMetrics.compactionsAborted`, `compactionsReduced`, `sstablesDropppedFromCompactions`, `completedTasks` (`C/metrics/CompactionMetrics.java:52-71`) | `compactionsReduced` means disk space ran short during compaction. | nice-to-have |

### 2.7 Caches

| ID | Item | Layer | Available | Gap and why it matters | Class |
|---|---|---|---|---|---|
| CCH-1 | Hit rates | None | `AbstractCacheMetrics.hits/misses/hitRate/oneMinuteHitRate` (`C/metrics/AbstractCacheMetrics.java:42-67`), row cache per table `rowCacheHit/Miss` (`C/metrics/TableMetrics.java:198-202`) | T7.15 (ADR-046). | missing-needed (T7.15) |
| CCH-2 | Capacity, size, entries | None | see MEM-9 | Input of the cache actuator's envelope and of "is the cache full" (UP only helps a full cache). | missing-needed |
| CCH-3 | Where each cache lives | Assumed in a comment (`A/plan/policies/GcPressurePolicy.java:33`), wrong for the row cache | see section 5, P1 | Finding 3 of the postmortem. | missing-needed |

### 2.8 Hints, streaming, repair, read repair

| ID | Item | Layer | Available | Gap and why it matters | Class |
|---|---|---|---|---|---|
| HNT-1 | Hints in progress, created, written to disk | None | `StorageMetrics.totalHintsInProgress`, `totalHints` (`C/metrics/StorageMetrics.java:49-50`); `HintsServiceMetrics.hintsSucceeded/Failed/TimedOut`, `hintsFileSize` (`C/metrics/HintsServiceMetrics.java:65-75`) | The hint throttle is moved DOWN by the yield step (`A/plan/policies/YieldBackgroundPolicy.java:62`) without knowing whether there are hints to throttle; a step with no hints in flight cannot improve anything and is rolled back. Growing hints also name a down or slow replica. | missing-needed |
| HNT-2 | Hints dropped (not stored) | None | `HintedHandoffMetrics.notStored` per endpoint (`C/metrics/HintedHandoffMetrics.java:47`) | Data loss risk (consistency) after a long outage; repair priority could react. | missing-diagnostic |
| STR-1 | Active streams, bytes in/out | None | `StreamingMetrics.activeStreamsOutbound`, `totalIncomingBytes`, `totalOutgoingBytes` and per peer (`C/metrics/StreamingMetrics.java:42-50`); `StreamManager.getCurrentStreams()` (`C/streaming/StreamManager.java:326`) | Same as HNT-1 for the stream throughput step: yielding streams when none run is a wasted, rolled-back action. Scale-out (`ScaleOutActuator`) also streams and cannot see its own progress. | missing-needed |
| REP-1 | Repair overdue | Derived `repair.overdue_ratio` from `AutoRepairUtils` history (`A/analyze/detectors/RepairOverdueDetector.java:60-110, 280-300`) | `AutoRepairMetrics` (`C/metrics/AutoRepairMetrics.java:39-49`: repairs in progress, longest unrepaired, failed token ranges) | Covered for its purpose; failed token ranges would name *why* repair is overdue. | covered (diagnostic gap nice-to-have) |
| REP-2 | Percent repaired, pending repair bytes | None | `TableMetrics.percentRepaired`, `bytesUnrepaired`, `bytesPendingRepair` (`C/metrics/TableMetrics.java:226-230`) | | nice-to-have |
| RR-1 | Read repair, speculative retries | None | `ReadRepairMetrics.repairedBlocking`, `repairedBackground`, `speculatedRead/Write`, `timedOut` (`C/metrics/ReadRepairMetrics.java:32-63`); per table `speculativeRetries`, `speculativeFailedRetries`, `speculativeInsufficientReplicas`, `additionalWrites` (`C/metrics/TableMetrics.java:265-271`); `readRepairRequests` (`:394`) | Blocking read repair is a cause of read latency (replicas disagree), and speculative retries mask a slow replica: both name causes of `latency_slo_breach` the layer attributes to load today. | missing-diagnostic |

### 2.9 Clients and native transport

| ID | Item | Layer | Available | Gap and why it matters | Class |
|---|---|---|---|---|---|
| CLI-1 | Request rates and p99 (read, write, CAS) | `S/Signals.java:89-112` | as observed | Feeds latency detectors, throttle and guards. | covered |
| CLI-2 | Timeouts, unavailables, failures | Emitted (`S/Signals.java:92-94, 98-100, 104-106, 110-112`), read by nothing | as observed; also `aborts`, `tombstoneAborts`, `readSizeAborts`, `retryCoordinatorBehind` (`C/metrics/ClientRequestMetrics.java:36-45`) | The client-visible failure rate has no detector and is no verification guard: an action that turns latency into timeouts passes its latency guard. | missing-needed |
| CLI-3 | Range (scan) coordinator requests | None | `RangeCommandIterator.rangeMetrics` "RangeSlice" (`C/service/reads/range/RangeCommandIterator.java:80`) | Range scans are a classic hidden load source; only the local `table.range_*` signals exist, unused. | missing-diagnostic |
| CLI-4 | Connections, in-flight request bytes, paused and discarded requests | None | `ClientMetrics` "ConnectedNativeClients", "Connections", "RequestsSize" (= `ClientResourceLimits.getCurrentGlobalUsage()`), "PausedConnections", "ConnectionPaused", "RequestDiscarded", "TimedOutBeforeProcessing" (`C/metrics/ClientMetrics.java:194-243`); limits `ClientResourceLimits.getGlobalLimit()` (`C/transport/ClientResourceLimits.java:63-75`) | In-flight bytes against the limit is the input the planned "native transport in-flight bytes" knob needs (RC-5). Discarded and timed-out-before-processing requests are overload the layer's throttle should see. | missing-needed |
| CLI-5 | Native transport request pool | `pool.*` for "Native-Transport-Requests" (`C/transport/Dispatcher.java:65`) via `ThreadPoolSensor` | as observed | Observed; `pool_saturation` covers it. | covered |
| CLI-6 | CAS contention, unfinished commits | None (CAS rates and p99 emitted) | `CASClientRequestMetrics.contention`, `unfinishedCommit`, `unknownResult` (`C/metrics/CASClientRequestMetrics.java:29-31`) | | nice-to-have |
| CLI-7 | Mutation size, batch sizes, CQL statement cache | None | `ClientWriteRequestMetrics.mutationSize` (`:34`), `BatchMetrics` (`C/metrics/BatchMetrics.java:28-30`), `CQLMetrics.preparedStatementsEvicted` (`C/metrics/CQLMetrics.java:35`) | Client misuse diagnostics. | nice-to-have |

### 2.10 Thread pools

| ID | Item | Layer | Available | Gap and why it matters | Class |
|---|---|---|---|---|---|
| TP-1 | All pools with JMX metrics | `pool.*` (`S/Signals.java:115-120`) for every `ThreadPoolMetrics` (`S/ThreadPoolSensor.java:64`), created by `SEPExecutor` (`C/concurrent/SEPExecutor.java:80`) and `ThreadPoolExecutorJMXAdapter` (`C/concurrent/ThreadPoolExecutorJMXAdapter.java:136`): all `Stage`s (`C/concurrent/Stage.java`), MemtableFlushWriter, MemtablePostFlush, MemtableReclaimMemory (`C/db/ColumnFamilyStore.java:201-210`), compaction, hints dispatch, repair, native transport | as observed | Covered. Executors created without `withJmx` (for example `ScheduledExecutors`) are not in the registry and not observed (**unverified** which ones matter). | covered |
| TP-2 | Oldest queued task | `pool.oldest_task_queue_millis` (`S/Signals.java:120`) | Only `SEPExecutor` implements it (`C/concurrent/SEPExecutor.java:90-101`); every other pool returns the default 0 (`C/concurrent/ResizableThreadPool.java:83-86`) | For the internal pools (flush, compaction) "0" means "unknown", not "empty"; `pool_saturation` evidence can mislead. | nice-to-have |

### 2.11 Process, node and cluster state

| ID | Item | Layer | Available | Gap and why it matters | Class |
|---|---|---|---|---|---|
| FD-1 | File descriptors vs limit | None | `com.sun.management.UnixOperatingSystemMXBean.getOpenFileDescriptorCount()`, `getMaxFileDescriptorCount()` *(measured: 5/1048576)* | Running out of descriptors fails sstable opens and connections; a split adds a table and sstables. | missing-diagnostic |
| CLK-1 | Clock offset / NTP state | None | **Not observable** in-JVM: NTP state is in the kernel (`adjtimex(2)`), not in `/proc` or `/sys`, and Java has no binding; reading it needs JNI or running `chronyc`/`timedatectl`. Cross-node offset could be estimated from internode message creation times (`DroppedMessageMetrics.crossNodeDroppedLatency`, `C/metrics/DroppedMessageMetrics.java:65`, uses the sender's clock), **unverified** as an estimator. | Clock skew breaks LWT and tombstone ordering; ADR-031 judges the layout tombstone deadline by each replica's own clock. | not observable (estimate: missing-diagnostic) |
| JVM-1 | Safepoint time | None | JFR events `jdk.SafepointBegin/End` through `jdk.jfr.consumer.RecordingStream` (JDK 14+); `sun.management.HotspotRuntimeMBean` is internal (**unverified** reachable). Cost of a continuous JFR stream on this node **unverified**. | Long time-to-safepoint is a cause of latency that GC time does not show. | nice-to-have |
| TCM-1 | Metadata log lag and commit health | Only `ClusterMetadata.current()` reads for arming and the ledger | `TCMMetrics.instance` (`C/metrics/TCMMetrics.java:41-70`): current epoch, CMS size, unreachable CMS members, commit retries, fetch latency, `coordinatorBehindSchema/Placements` | A node behind the log is a cause of request failures (`retryCoordinatorBehind`, CLI-2); cluster actions commit to the log. Whether other nodes' epochs are visible locally is **unverified**. | missing-diagnostic |
| GSP-1 | Gossip state, failure detector | `peer.alive`, `LOAD`, `SEVERITY` (`S/PeerSensor.java:79-95`) | `FailureDetector.getPhiValues()` (`C/gms/FailureDetector.java:208`) | Phi gives "about to be marked down". | nice-to-have |
| SCH-1 | Schema agreement | None | Under TCM schema is part of cluster metadata, so agreement is epoch agreement (TCM-1) | | nice-to-have (with TCM-1) |
| DF-1 | Disk failure policy events | None | `DisallowedDirectories.getUnwritableDirectories()`, `getUnreadableDirectories()` (`C/db/DisallowedDirectories.java:53-59`); `StorageMetrics.uncaughtExceptions` (`C/metrics/StorageMetrics.java:47`); `disk_failure_policy` (`C/config/Config.java:134`, default `ignore`) | A data directory disabled by the policy changes capacity; the layer would keep acting on a failing disk. | missing-diagnostic |
| NODE-1 | Node operation mode (draining, joining) | Arming reads `myNodeState() == JOINED` (overview 3.6) | `StorageService` operation mode | Covered for arming. | covered |

### 2.12 Data shape

| ID | Item | Layer | Available | Gap and why it matters | Class |
|---|---|---|---|---|---|
| TS-1 | Tombstones | `table.tombstones_scanned_p99`, warnings, failures (`S/Signals.java:78, 85-86`) | also `purgeableTombstoneScannedHistogram` (`C/metrics/TableMetrics.java:180`) | Covered; purgeable tombstones would tell whether a garbage-collecting compaction can help (it purges only purgeable ones). | covered (refinement nice-to-have) |
| LP-1 | Large partitions | `table.max/mean_partition_size_bytes` emitted, unused; `partition.size_bytes` for sampled hot keys (`S/Signals.java:83-84, 151`) | `TableMetrics.topWritePartitionSize` sampler (`:300`), `localReadSize` histograms (`:319-321`), size warnings | Covered for the split trigger (ADR-043). | covered |

### 2.13 Other metric families in `C/metrics/` not listed above

Checked file by file; none feeds a current or planned action: Accord (`Accord*Metrics.java`; Accord is not used by the layer), `CIDRAuthorizerMetrics`, `MutualTlsMetrics`, `DenylistMetrics`, `PaxosMetrics` (repair retries), `RepairMetrics` (repair message retries), `KeyspaceMetrics` (aggregates of `TableMetrics`), `ViewWriteMetrics`, `TrieMemtableMetricsView` (trie memtable contention), `ClientRequestSizeMetrics`, `ClientMessageSizeMetrics`, `TopPartitionTracker`, and the samplers other than read/write frequency (`TableMetrics.java:296-310`). All **nice-to-have** or out of scope.

### 2.14 Counts

| Class | Count | Items |
|---|---|---|
| missing-needed | 23 | CPU-1, CPU-4, CPU-5, MEM-1, MEM-2, MEM-3, MEM-5, MEM-9, MEM-13, MEM-14, MEM-16, DSK-2, DSK-3, MSG-1, CL-2, MT-2, CCH-1 (T7.15), CCH-2, CCH-3, HNT-1, STR-1, CLI-2, CLI-4 |
| missing-diagnostic | 18 | CPU-2, CPU-3, MEM-6, MEM-7, MEM-8, MEM-15, DSK-4, MSG-2, MSG-3, CL-1, MT-1, CMP-2, HNT-2, RR-1, CLI-3, FD-1, TCM-1, DF-1 |
| nice-to-have | 17 | MEM-4, MEM-11, MEM-12, MEM-17, NET-1, NET-2, MSG-4, MT-3, CMP-3, REP-2, CLI-6, CLI-7, TP-2, JVM-1, GSP-1, SCH-1, and the families of 2.13 (one entry) |
| not observable | 1 | CLK-1 (in-JVM; a cross-node estimate may be possible) |
| covered | 9 | DSK-1, CMP-1, REP-1, CLI-1, CLI-5, TP-1, NODE-1, TS-1, LP-1 |

## 3. The ten most important gaps

1. **MEM-3 / P2: `gc.time_fraction` counts concurrent GC time.** On JDK 21 the shipped options select generational ZGC; `gc_pressure` can open on normal concurrent cycles and shrink caches.
2. **MEM-2, MEM-13, MEM-14, MEM-16: no memory headroom.** Heap after GC, RSS, `MemAvailable`, cgroup limit. The planned cache UP direction (RC-1) and any memory guard need them (T7.16).
3. **CPU-1, CPU-2, CPU-5 / P3: CPU is judged from the load average.** It counts I/O-blocked tasks and, in a container, the host's load; the pool step is gated on it.
4. **DSK-3: no disk I/O.** The ladder yields background I/O blind, and "disk is the bottleneck" cannot be named.
5. **CLI-2: client timeouts, unavailables and failures are collected and ignored.** No detector, no guard.
6. **MSG-1: dropped messages are invisible.** The standard overload signal of Cassandra.
7. **CL-2, MT-2: write stalls are invisible.** Commit log waits and memtable allocation blocking.
8. **MEM-7: memtables are invisible**, though on heap by default the biggest heap user; `gc_pressure` cannot name them.
9. **HNT-1, STR-1: the yield step throttles hints and streams it cannot see.** A step with nothing in flight is a guaranteed rollback that counts against the actuator.
10. **DSK-2: the commit log and hints volumes are not in `disk_pressure`.** A full commit log stops the node.

## 4. Design-to-build reconciliation (R3 preview)

### 4.1 Knob table, overview 8.1

| Knob | Planned direction(s) | Built | Item |
|---|---|---|---|
| Compaction throughput | down; up on backlog | DOWN: yield step (`A/plan/policies/YieldBackgroundPolicy.java:62`). UP: `CompactionBacklogPolicy` (`:45`), `TombstoneOverloadPolicy` (`:61`) | ok. A yielded value is not restored automatically; only `revert` or a later backlog UP moves it (RC-3) |
| Concurrent compactors | down; up for backlog | UP only (`CompactionBacklogPolicy.java:45`); **DOWN not wired** | **RC-2** |
| Stream throughput | down | DOWN (yield). No recovery planned or built | RC-3 |
| Request stage pools | up when queues are sustained and CPU is not saturated; down otherwise | UP (`RequestPoolPolicy.java:161`); DOWN only back towards the configured size (`:175`), never below it, so "down otherwise" (0.5x envelope) is not built | RC-8; CPU input wrong (P3) |
| Native transport rate limit | enable and lower; raise and disable | both (`RequestThrottlePolicy`) | ok |
| **Native transport in-flight bytes** | down under heap pressure | **no actuator at all** (no reference to `setNativeTransportMaxConcurrentRequestsInBytes`, `C/service/StorageService.java:5182`, in `A/`) | **RC-5** |
| Key / row / counter cache capacity | **up on low hit rate with heap headroom**; down under GC pressure | DOWN (`GcPressurePolicy.java:43`); **UP not wired** (finding 2); DOWN starts with the off-heap row cache (P1) | **RC-1** |
| Hinted handoff throttle | down | DOWN (yield). No recovery | RC-3 |
| Advertised severity | up on a degraded node | UP (advertise step). **No down direction is planned**: a raised severity stays, and coordinators keep avoiding the node (`C/service/StorageProxy.java:1995` treats only severity 0 as healthy), until `revert` | **RC-4** |

The owner-accepted rule that committed values are sticky overrides (operator guide section 8, `docs/operations/autonomic.md:439-481`) explains RC-3 and RC-4, but neither is recorded as a decision for the yield knobs and the severity in particular, whose effect on routing persists after the node recovers. Proposed: decide per knob (owner).

### 4.2 Sensors, detectors, policies

- **Sensors (T2.5):** all eight planned sensors are built, plus `CpuSensor` (D5) and `LayoutSensor` (D6). The overview 4.2 table names `DiskUsageMonitor` as the disk source; the sensor uses `FileStore` instead (`S/DiskSensor.java:36-39` explains why). Nothing planned is missing; the gap is what was never planned (section 2).
- **Detectors (T3.2-T3.4, T4.4, T5.4, 10.1):** all built and registered (`A/analyze/detectors/Detectors.java:35-61`, `A/AutonomicService.java:309-326`).
- **Policies (T4.3, T5.1-T5.4):** all built. `DegradedSelfPolicy` exists but is not registered; the ladder answers `degraded_self` (`Policies.java:105`). Remove or register (**RC-7**).
- **Derived signal:** `repair.overdue_ratio` is the goal of `RepairOverduePolicy`'s action but is never stored; it is harmless only because `RepairPriorityActuator` is a cluster actuator that verifies itself (`A/execute/ClusterActuator.java:34-37`) (**RC-6**).
- **Correlator rules (T3.5):** `gc_pressure` causes latency, pool saturation and `degraded_self` (`A/analyze/RuleCorrelator.java:93-97`). With P2, a spurious `gc_pressure` would become the root cause of real latency diagnoses and suppress the ladder for them (**unverified** in a run; follows from the rules).

### 4.3 R2 allowlist cross-reference

| Allowlist entry in `InputCoverageTest` | Item |
|---|---|
| `key_cache_capacity/UP`, `row_cache_capacity/UP`, `counter_cache_capacity/UP` | RC-1 |
| `concurrent_compactors/DOWN` | RC-2 |
| `stream_throughput/UP`, `hint_throttle/UP` | RC-3 |
| `advertised_severity/DOWN` | RC-4 |
| derived `repair.overdue_ratio` | RC-6 |
| `partition_unsplit` names `partition_split` without proposing it | not a gap: it checks for a split in flight |

## 5. Physical-fact assumptions in policies and actuators

| ID | Assumption (where) | Fact (evidence) | Verdict |
|---|---|---|---|
| P1 | "shrink an on-heap cache by one step, the row cache first" (`A/plan/policies/GcPressurePolicy.java:33, 43`) | Row cache provider defaults to `OHCProvider` (`C/config/Config.java:578`), built on `org.caffinitas.ohc` (`C/cache/OHCProvider.java:25, 40`), off heap; the alternative `SerializingCacheProvider` "Serializes cache values off-heap" (`C/cache/SerializingCache.java:38`). Key and counter caches are `CaffeineCache` (`C/service/CacheService.java:128, 179`), "an on-heap cache" (`C/cache/CaffeineCache.java:32`). | **Wrong** for the row cache (postmortem finding 3). Counter and key: right. |
| P2 | `gc.time_fraction` is "Fraction of wall time spent in GC" (`S/Signals.java:132`), computed by summing every collector bean (`S/GcSensor.java:71-81`), and `gc_pressure` treats it as pause pressure | Cassandra's own `GCInspector` classifies "ZGC Cycles", "ZGC Minor/Major Cycles" and "G1 Old Generation" as concurrent, and only the "Pauses" beans as stop-the-world (`C/service/GCInspector.java:203-231`). `conf/jvm21-server.options:31-32` selects generational ZGC. *(measured, JDK 17, non-generational ZGC, same allocation loop: "ZGC Cycles" 3330 ms, "ZGC Pauses" 2 ms; G1: only "G1 Young Generation" and "G1 Old Generation" beans.)* JDK 21 bean names taken from `GCInspector`, **unverified** here (no JDK 21 on this machine); whether JDK 20+ G1 adds a "G1 Concurrent GC" bean is **unverified**. | **Wrong** under ZGC (default on JDK 21); about right under G1 on JDK 17. |
| P3 | "Whether the node's CPUs are saturated ... the system load average per processor" (`S/CpuSensor.java:31-33`), used as the pool growth gate (`A/plan/policies/RequestPoolPolicy.java:48, 60`) | Load average counts tasks in state R **or D** (`proc_loadavg(5)`), so disk waits count as CPU demand; it is system-wide while `getAvailableProcessors()` follows the container quota. | **Wrong** as "CPU saturated" (right only on a CPU-bound host without containers). |
| P4 | Request pool actuator writes only the maximum because `setConcurrency` rejects a core size (overview 8.1, "As built in D5"); stage names `ReadStage` etc. (`A/execute/actuators/RequestPoolActuator.java:43-44, 73`) | `setConcurrency` skips the core size when negative (`C/service/StorageService.java:1773-1778`); the names match `Stage` (`C/concurrent/Stage.java`, `jmxName`). | Right. |
| P5 | Advertised severity: `set` calls `DynamicEndpointSnitch.addSeverity(value)` and `get` reads the gossiped value (`A/execute/actuators/AdvertisedSeverityActuator.java:37-50`) | `addSeverity` *replaces* the local `SEVERITY` state, despite its name (`C/locator/DynamicEndpointSnitch.java:366-369`). It affects routing only with `dynamic_snitch: true` (default, `C/config/Config.java:523`) and without `cassandra.ignore_dynamic_snitch_severity` (`DynamicEndpointSnitch.java:60`). | Right, with those two conditions undocumented in the layer. |
| P6 | Cache capacity is runtime-resizable; a cache configured to 0 is not managed (`A/execute/actuators/CacheCapacityActuator.java:25-27`) | Setters exist (`C/service/CacheService.java:327-349`); a 0 MiB row cache uses `NopCacheProvider` (`:146-147`). Under `sstable.selected_format: bti` the key cache is not used at all (`C/io/sstable/format/bti/TrieIndexEntry.java:64-73`); the default is `big` (`C/config/Config.java:470`). | Right; the key cache knob is meaningless on BTI (not checked by the actuator). |
| P7 | Hint throttle getter "**unverified** name" (overview 8.1) | `DatabaseDescriptor.getHintedHandoffThrottleInKiB()` (`A/execute/actuators/HintThrottleActuator.java:44`) | Resolved; update 8.1. |
| P8 | `pool.oldest_task_queue_millis` "0 when unknown or empty" (`S/Signals.java:120`) | Only SEP pools implement it (`C/concurrent/SEPExecutor.java:90-101`, nanoseconds, converted correctly at `S/ThreadPoolSensor.java:78`); all others always return 0 (`C/concurrent/ResizableThreadPool.java:83-86`). | Right as documented; lossy. |
| P9 | `table.pending_flushes` is the number of flushes pending | Incremented when a `Flush` is built, decremented after the post-flush (`C/db/ColumnFamilyStore.java:1242, 1207`). | Right. |
| P10 | `gc_pressure` is relieved by shrinking caches (`GcPressurePolicy`) | Memtables are on heap by default (`C/config/Config.java:637`) and not observed (MEM-7); the cache step may be the wrong remedy. The verify loop catches an ineffective step but each rollback costs a cooldown. | Not wrong, unproven: needs MEM-7. |
| P11 | The planned "chunk cache" could be a cache knob (owner's T7.16 outline lists it as observed memory) | `ChunkCache.setCapacity` throws (`C/cache/ChunkCache.java:311-314`). | Observe only; do not plan an actuator. |

## 6. Proposed scope for D7.1

T7.15 (cache hit rates, ADR-046) is assumed done first and is not repeated. Ordered by priority. Sizes: S (under a day), M (one to two days), L (more).

| Task | Content | Items | Tier | Size |
|---|---|---|---|---|
| **T7.16** | ADR-047. Memory sensor: heap pools and after-GC old-gen occupancy, metaspace, direct and mapped buffers and Cassandra's `BufferPool`s, memtable pool on/off-heap used, limit and blocked-on-allocation, per-cache capacity/size/entries, chunk cache, bloom filter, index summary and compression metadata totals, RSS, `/proc/meminfo` (available, cached, swap), `/proc/vmstat` swap-in, cgroup `memory.max/current/events`. Cache resize in both directions (goal hit rate; guards GC time, heap after GC, available memory, latency). Fix P1 by choosing caches by where they live, with a unit test asserting the provider classes (R5). | MEM-1..16, CCH-2, CCH-3, RC-1, P1, P10, P11 | opus | L |
| **T7.17** | GC signal fix: pause time only, per collector, classified like `GCInspector`; concurrent cycle time as its own signal; unit test on G1 and ZGC bean names (R5). Ships before or with T7.16, since T7.16's guards use it. | MEM-3, P2 | sonnet | S |
| **T7.18** | CPU sensor: `/proc/stat` utilisation (user, system, iowait, steal), process CPU, PSI cpu/io, cgroup `cpu.max` and `cpu.stat` throttling. Pool gate on utilisation and run queue, not load average; keep load average as evidence. | CPU-1..5, P3 | sonnet | M |
| **T7.19** | Disk I/O sensor: `/proc/diskstats` for the devices of the data, commit log and hints directories (via `/proc/self/mountinfo`); space of commit log and hints volumes into `disk_pressure`. | DSK-2, DSK-3 | sonnet | M |
| **T7.20** | Use what is already collected: client timeout/unavailable/failure detector and an error-rate guard for every verify spec; `table.pending_compactions` and sstables-per-read for per-table correlation. | CLI-2, CMP-2 | sonnet | M |
| **T7.21** | Write path and messaging sensor: commit log pending and wait timers, memtable blocked-on-allocation and pending flush tasks, dropped messages by verb, internode latency, outbound pending bytes per peer; a `write_stall` and a `dropped_messages` diagnosis with correlator rules, so the ladder does not try pools or throttle on a disk-bound write path. | CL-1, CL-2, MT-1, MT-2, MSG-1..3 | sonnet | M |
| **T7.22** | Background-work inputs: hints in progress/created/dropped, active streams and bytes; the yield step skips a knob whose work is idle. Native transport: connections, in-flight bytes against the limit, paused, discarded, timed out before processing; range coordinator metrics. | HNT-1, HNT-2, STR-1, CLI-3, CLI-4 | sonnet | M |
| **T7.23** | Node health odds and ends: file descriptors vs limit, disallowed directories, uncaught exceptions, TCM epoch and commit metrics, snapshot size, read repair and speculative retries. | FD-1, DF-1, TCM-1, DSK-4, RR-1 | haiku | S |
| **T7.24** | Owner decisions, then wiring: RC-2 (concurrent compactors down), RC-3/RC-4 (automatic recovery of yielded knobs and of the advertised severity on `baseline_drift`), RC-5 (build the in-flight bytes knob or drop it from 8.1), RC-7 (remove `DegradedSelfPolicy`), RC-8 (pools below configured). Update 8.1 for P7. | RC-2..8 | sonnet | M |
| **T7.25** | R6 scenarios with real pressure: memory (heap and cache), disk I/O, one per resource class an action acts on. | R6 | sonnet | L |
| **T7.26** | R3: reconciliation checklist in the deliverable template; keep `InputCoverageTest`'s allowlists in step with section 4. | R3 | haiku | S |

Recommended for the owner to mark **out of scope** for D7.1, with a reason recorded: CLK-1 clock skew (not observable in-JVM; revisit with a cross-node estimate), JVM-1 safepoints (JFR cost unmeasured), NET-1/NET-2 NIC and TCP counters (system-wide, no action depends on them), MSG-4, GSP-1, SCH-1, TP-2, MEM-4, MEM-11, MEM-17, CMP-3, MT-3, REP-2, CLI-6, CLI-7, and the families of 2.13 (Accord, auth, TLS, denylist, Paxos repair, CQL statement cache).

## 7. Not verified in this audit

- Behaviour of `os.load_per_processor` inside a container (P3, CPU-5): derived from `proc_loadavg(5)` and the JDK's container support, not run in a container.
- JDK 21 GC bean names and times (P2): taken from `C/service/GCInspector.java:203-231`; measured only on JDK 17.
- Whether `jdk.internal.misc.VM.maxDirectMemory()` is reachable without `--add-exports` (MEM-5), and whether Cassandra's mapped files are counted in the "mapped" buffer pool (MEM-6).
- Cost of continuous JFR streaming (JVM-1) and of NMT (MEM-17).
- Whether a node can see other nodes' TCM epochs locally (TCM-1); an accessor for outbound connection state (MSG-4).
- Which executors without JMX metrics matter (TP-1).
- That a spurious `gc_pressure` under ZGC suppresses the ladder through the correlator in a real run (4.2).
