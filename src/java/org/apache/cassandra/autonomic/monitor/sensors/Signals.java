/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.autonomic.monitor.sensors;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.autonomic.monitor.SignalId;

/**
 * Every signal the production sensors emit, with its unit and meaning. Detectors depend on these names; renaming one
 * is a breaking change. All values are doubles. "Delta" signals are the change since the sensor's previous
 * collection, "per_sec" signals are deltas divided by the elapsed time between collections. Rate and delta signals
 * are not emitted for the first collection of a subject (there is nothing to subtract), nor after a counter went
 * backwards (for example a dropped and re-created table).
 *
 * <p>Subjects: {@code table.*} on {@code Subject.table}, {@code client.*}, {@code compaction.*}, {@code disk.*} and
 * {@code gc.*} on {@code Subject.node()}, {@code pool.*} on {@code Subject.threadPool(name)}, {@code peer.*} on
 * {@code Subject.peer(hostId)}, {@code partition.*} on {@code Subject.partition(keyspace, table, keyHex)} where the
 * subject detail is the raw partition key as lowercase hex (see {@link PartitionKeys}).
 */
public final class Signals
{
    private static final class Def
    {
        final String unit;
        final String meaning;

        Def(String unit, String meaning)
        {
            this.unit = unit;
            this.meaning = meaning;
        }
    }

    private static final Map<SignalId, Def> DEFS = new LinkedHashMap<>();

    private static SignalId def(String name, String unit, String meaning)
    {
        SignalId id = SignalId.of(name);
        if (DEFS.put(id, new Def(unit, meaning)) != null)
            throw new IllegalStateException("duplicate signal " + name);
        return id;
    }

    // ---- TableSensor (subject TABLE; user tables only) ----
    public static final SignalId TABLE_READ_OPS_PER_SEC = def("table.read_ops_per_sec", "ops/s", "Local single-partition reads per second");
    public static final SignalId TABLE_WRITE_OPS_PER_SEC = def("table.write_ops_per_sec", "ops/s", "Local writes per second");
    public static final SignalId TABLE_RANGE_OPS_PER_SEC = def("table.range_ops_per_sec", "ops/s", "Local range reads per second");
    public static final SignalId TABLE_READ_LATENCY_MEAN_MICROS = def("table.read_latency_mean_micros", "us", "Mean local read latency over the last collection interval (absent when no reads)");
    public static final SignalId TABLE_WRITE_LATENCY_MEAN_MICROS = def("table.write_latency_mean_micros", "us", "Mean local write latency over the last collection interval (absent when no writes)");
    public static final SignalId TABLE_RANGE_LATENCY_MEAN_MICROS = def("table.range_latency_mean_micros", "us", "Mean local range read latency over the last collection interval (absent when no range reads)");
    public static final SignalId TABLE_READ_LATENCY_P99_MICROS = def("table.read_latency_p99_micros", "us", "p99 of the decaying local read latency histogram (slow to react, see overview R4)");
    public static final SignalId TABLE_WRITE_LATENCY_P99_MICROS = def("table.write_latency_p99_micros", "us", "p99 of the decaying local write latency histogram");
    public static final SignalId TABLE_RANGE_LATENCY_P99_MICROS = def("table.range_latency_p99_micros", "us", "p99 of the decaying local range read latency histogram");
    public static final SignalId TABLE_COORDINATOR_READ_OPS_PER_SEC = def("table.coordinator_read_ops_per_sec", "ops/s", "Reads coordinated by this node for the table per second");
    public static final SignalId TABLE_COORDINATOR_WRITE_OPS_PER_SEC = def("table.coordinator_write_ops_per_sec", "ops/s", "Writes coordinated by this node for the table per second");
    public static final SignalId TABLE_COORDINATOR_READ_LATENCY_P99_MICROS = def("table.coordinator_read_latency_p99_micros", "us", "p99 of coordinator read latency for the table");
    public static final SignalId TABLE_COORDINATOR_WRITE_LATENCY_P99_MICROS = def("table.coordinator_write_latency_p99_micros", "us", "p99 of coordinator write latency for the table");
    public static final SignalId TABLE_SSTABLES_PER_READ_P99 = def("table.sstables_per_read_p99", "sstables", "p99 of sstables touched per read");
    public static final SignalId TABLE_TOMBSTONES_SCANNED_P99 = def("table.tombstones_scanned_p99", "cells", "p99 of tombstones scanned per read");
    public static final SignalId TABLE_LIVE_CELLS_SCANNED_P99 = def("table.live_cells_scanned_p99", "cells", "p99 of live cells scanned per read");
    public static final SignalId TABLE_PENDING_COMPACTIONS = def("table.pending_compactions", "tasks", "Estimated compactions pending for the table");
    public static final SignalId TABLE_LIVE_SSTABLE_COUNT = def("table.live_sstable_count", "sstables", "Live sstables of the table");
    public static final SignalId TABLE_PENDING_FLUSHES = def("table.pending_flushes", "flushes", "Memtable flushes pending for the table");
    public static final SignalId TABLE_MAX_PARTITION_SIZE_BYTES = def("table.max_partition_size_bytes", "bytes", "Largest compacted partition (estimate over live sstables)");
    public static final SignalId TABLE_MEAN_PARTITION_SIZE_BYTES = def("table.mean_partition_size_bytes", "bytes", "Mean compacted partition size (estimate over live sstables)");
    public static final SignalId TABLE_TOMBSTONE_WARNINGS_DELTA = def("table.tombstone_warnings_delta", "queries", "Queries that crossed the tombstone warn threshold since the previous collection");
    public static final SignalId TABLE_TOMBSTONE_FAILURES_DELTA = def("table.tombstone_failures_delta", "queries", "Queries aborted by the tombstone failure threshold since the previous collection");

    // ---- ClientRequestSensor (subject NODE); ops are read, write, cas_read, cas_write ----
    public static final SignalId CLIENT_READ_OPS_PER_SEC = def("client.read_ops_per_sec", "ops/s", "Coordinator read requests per second");
    public static final SignalId CLIENT_READ_LATENCY_MEAN_MICROS = def("client.read_latency_mean_micros", "us", "Mean coordinator read latency over the last interval (absent when idle)");
    public static final SignalId CLIENT_READ_LATENCY_P99_MICROS = def("client.read_latency_p99_micros", "us", "p99 coordinator read latency (decaying histogram)");
    public static final SignalId CLIENT_READ_TIMEOUTS_PER_SEC = def("client.read_timeouts_per_sec", "ops/s", "Coordinator read timeouts per second");
    public static final SignalId CLIENT_READ_UNAVAILABLES_PER_SEC = def("client.read_unavailables_per_sec", "ops/s", "Coordinator read unavailable errors per second");
    public static final SignalId CLIENT_READ_FAILURES_PER_SEC = def("client.read_failures_per_sec", "ops/s", "Coordinator read failures per second");
    public static final SignalId CLIENT_WRITE_OPS_PER_SEC = def("client.write_ops_per_sec", "ops/s", "Coordinator write requests per second");
    public static final SignalId CLIENT_WRITE_LATENCY_MEAN_MICROS = def("client.write_latency_mean_micros", "us", "Mean coordinator write latency over the last interval (absent when idle)");
    public static final SignalId CLIENT_WRITE_LATENCY_P99_MICROS = def("client.write_latency_p99_micros", "us", "p99 coordinator write latency (decaying histogram)");
    public static final SignalId CLIENT_WRITE_TIMEOUTS_PER_SEC = def("client.write_timeouts_per_sec", "ops/s", "Coordinator write timeouts per second");
    public static final SignalId CLIENT_WRITE_UNAVAILABLES_PER_SEC = def("client.write_unavailables_per_sec", "ops/s", "Coordinator write unavailable errors per second");
    public static final SignalId CLIENT_WRITE_FAILURES_PER_SEC = def("client.write_failures_per_sec", "ops/s", "Coordinator write failures per second");
    public static final SignalId CLIENT_CAS_READ_OPS_PER_SEC = def("client.cas_read_ops_per_sec", "ops/s", "Coordinator serial (CAS) read requests per second");
    public static final SignalId CLIENT_CAS_READ_LATENCY_MEAN_MICROS = def("client.cas_read_latency_mean_micros", "us", "Mean CAS read latency over the last interval (absent when idle)");
    public static final SignalId CLIENT_CAS_READ_LATENCY_P99_MICROS = def("client.cas_read_latency_p99_micros", "us", "p99 CAS read latency (decaying histogram)");
    public static final SignalId CLIENT_CAS_READ_TIMEOUTS_PER_SEC = def("client.cas_read_timeouts_per_sec", "ops/s", "CAS read timeouts per second");
    public static final SignalId CLIENT_CAS_READ_UNAVAILABLES_PER_SEC = def("client.cas_read_unavailables_per_sec", "ops/s", "CAS read unavailable errors per second");
    public static final SignalId CLIENT_CAS_READ_FAILURES_PER_SEC = def("client.cas_read_failures_per_sec", "ops/s", "CAS read failures per second");
    public static final SignalId CLIENT_CAS_WRITE_OPS_PER_SEC = def("client.cas_write_ops_per_sec", "ops/s", "Coordinator CAS write requests per second");
    public static final SignalId CLIENT_CAS_WRITE_LATENCY_MEAN_MICROS = def("client.cas_write_latency_mean_micros", "us", "Mean CAS write latency over the last interval (absent when idle)");
    public static final SignalId CLIENT_CAS_WRITE_LATENCY_P99_MICROS = def("client.cas_write_latency_p99_micros", "us", "p99 CAS write latency (decaying histogram)");
    public static final SignalId CLIENT_CAS_WRITE_TIMEOUTS_PER_SEC = def("client.cas_write_timeouts_per_sec", "ops/s", "CAS write timeouts per second");
    public static final SignalId CLIENT_CAS_WRITE_UNAVAILABLES_PER_SEC = def("client.cas_write_unavailables_per_sec", "ops/s", "CAS write unavailable errors per second");
    public static final SignalId CLIENT_CAS_WRITE_FAILURES_PER_SEC = def("client.cas_write_failures_per_sec", "ops/s", "CAS write failures per second");

    // ---- ThreadPoolSensor (subject THREAD_POOL, detail is the pool name) ----
    public static final SignalId POOL_ACTIVE_TASKS = def("pool.active_tasks", "tasks", "Tasks currently executing");
    public static final SignalId POOL_PENDING_TASKS = def("pool.pending_tasks", "tasks", "Tasks queued");
    public static final SignalId POOL_CURRENTLY_BLOCKED_TASKS = def("pool.currently_blocked_tasks", "tasks", "Tasks currently blocked from being queued");
    public static final SignalId POOL_BLOCKED_TASKS_DELTA = def("pool.blocked_tasks_delta", "tasks", "Tasks that were blocked since the previous collection");
    public static final SignalId POOL_MAX_POOL_SIZE = def("pool.max_pool_size", "threads", "Maximum number of threads of the pool");
    public static final SignalId POOL_OLDEST_TASK_QUEUE_MILLIS = def("pool.oldest_task_queue_millis", "ms", "How long the oldest queued task has waited (0 when unknown or empty)");

    // ---- CompactionSensor (subject NODE) ----
    public static final SignalId COMPACTION_PENDING_TASKS = def("compaction.pending_tasks", "tasks", "Estimated pending compaction tasks, node-wide");
    public static final SignalId COMPACTION_BYTES_PER_SEC = def("compaction.bytes_per_sec", "bytes/s", "Uncompressed bytes compacted per second");

    // ---- DiskSensor (subject NODE) ----
    public static final SignalId DISK_USED_FRACTION = def("disk.used_fraction", "fraction", "Used fraction (0 to 1) of the file stores holding data directories");
    public static final SignalId DISK_FREE_BYTES = def("disk.free_bytes", "bytes", "Usable bytes on the file stores holding data directories");
    public static final SignalId DISK_TOTAL_BYTES = def("disk.total_bytes", "bytes", "Total bytes of the file stores holding data directories");

    // ---- GcSensor (subject NODE) ----
    public static final SignalId GC_TIME_FRACTION = def("gc.time_fraction", "fraction", "Fraction of wall time spent in GC since the previous collection");
    public static final SignalId GC_COLLECTIONS_DELTA = def("gc.collections_delta", "collections", "GC collections since the previous collection, all collectors");
    public static final SignalId GC_TIME_MILLIS_DELTA = def("gc.time_millis_delta", "ms", "Accumulated GC time since the previous collection, all collectors");
    public static final SignalId GC_HEAP_USED_FRACTION = def("gc.heap_used_fraction", "fraction", "Used heap divided by max heap (committed when max is undefined)");

    // ---- CpuSensor (subject NODE) ----
    public static final SignalId OS_LOAD_PER_PROCESSOR = def("os.load_per_processor", "ratio", "One-minute system load average divided by the available processors; absent where the platform has no load average");

    // ---- PeerSensor (subject PEER, detail is the host id, or the address when the host id is unknown) ----
    public static final SignalId PEER_LOAD_BYTES = def("peer.load_bytes", "bytes", "LOAD the peer last gossiped (can be stale); also emitted for this node under its own peer subject");
    public static final SignalId PEER_SEVERITY = def("peer.severity", "score", "SEVERITY the peer last gossiped");
    public static final SignalId PEER_ALIVE = def("peer.alive", "bool", "1 if gossip considers the peer alive, 0 if not");
    public static final SignalId PEER_SNITCH_SCORE = def("peer.snitch_score", "score", "Dynamic snitch score of the endpoint (lower is better); absent without a dynamic snitch");

    // ---- HotPartitionSensor (subject PARTITION; detail is the raw key as lowercase hex) ----
    public static final SignalId PARTITION_READ_SHARE = def("partition.read_share", "fraction", "Share (0 to 1) of the table's reads in the last sampling window that hit this partition");
    public static final SignalId PARTITION_WRITE_SHARE = def("partition.write_share", "fraction", "Share (0 to 1) of the table's writes in the last sampling window that hit this partition");
    public static final SignalId PARTITION_READ_OPS_PER_SEC = def("partition.read_ops_per_sec", "ops/s", "Estimated reads per second of this partition in the last sampling window");
    public static final SignalId PARTITION_WRITE_OPS_PER_SEC = def("partition.write_ops_per_sec", "ops/s", "Estimated writes per second of this partition in the last sampling window");
    public static final SignalId PARTITION_SIZE_BYTES = def("partition.size_bytes", "bytes", "Cheap on-disk size of this partition on this replica: its sstable fragments by index position plus its memtable rows (ADR-043); an upper bound of the live data until compaction. A trigger for the split probe, not a measurement of it");
    // ---- HotPartitionSensor (subject TABLE): the denominators of the shares, so that shares can be added up over windows ----
    public static final SignalId SAMPLED_TABLE_READS = def("partition.sampled_table_reads", "ops", "Reads of the table counted for the last sampling window, the denominator of its partitions' read_share (subject TABLE)");
    public static final SignalId SAMPLED_TABLE_WRITES = def("partition.sampled_table_writes", "ops", "Writes of the table counted for the last sampling window, the denominator of its partitions' write_share (subject TABLE)");

    // ---- LayoutSensor (subject PARTITION; detail is the raw key as lowercase hex); only for keys the LayoutProfiler watches ----
    public static final SignalId KEY_READ_LATENCY_MICROS = def("layout.key_read_latency_micros", "us", "Mean latency, over the last collection interval, of reads of the watched key that this node coordinated (absent when none)");
    public static final SignalId KEY_WRITE_LATENCY_MICROS = def("layout.key_write_latency_micros", "us", "Mean latency, over the last collection interval, of writes of the watched key that this node coordinated (absent when none)");
    public static final SignalId KEY_ERROR_RATE = def("layout.key_error_rate", "fraction", "Fraction of the reads and writes of the watched key in the last collection interval that failed (absent when none)");
    public static final SignalId KEY_READ_OPS_DELTA = def("layout.key_read_ops_delta", "ops", "Reads of the watched key coordinated here since the previous collection");
    public static final SignalId KEY_WRITE_OPS_DELTA = def("layout.key_write_ops_delta", "ops", "Writes of the watched key coordinated here since the previous collection");
    // ---- LayoutSensor (subject TABLE); only for tables with a watched key: the requests that touched no watched key (ADR-040) ----
    public static final SignalId REST_READ_LATENCY_MICROS = def("layout.rest_read_latency_micros", "us", "Mean latency, over the last collection interval, of reads of the table that touched no watched key and that this node coordinated (absent when none)");
    public static final SignalId REST_WRITE_LATENCY_MICROS = def("layout.rest_write_latency_micros", "us", "Mean latency, over the last collection interval, of writes of the table that touched no watched key and that this node coordinated (absent when none)");
    public static final SignalId REST_READ_OPS_DELTA = def("layout.rest_read_ops_delta", "ops", "Reads of the table that touched no watched key, coordinated here since the previous collection");
    public static final SignalId REST_WRITE_OPS_DELTA = def("layout.rest_write_ops_delta", "ops", "Writes of the table that touched no watched key, coordinated here since the previous collection");

    private Signals()
    {
    }

    /** All signals, in declaration order. */
    public static Set<SignalId> all()
    {
        return Collections.unmodifiableSet(DEFS.keySet());
    }

    /** Unit of the signal, or null if unknown. */
    public static String unit(SignalId id)
    {
        Def d = DEFS.get(id);
        return d == null ? null : d.unit;
    }

    /** One-line meaning of the signal, or null if unknown. */
    public static String meaning(SignalId id)
    {
        Def d = DEFS.get(id);
        return d == null ? null : d.meaning;
    }
}
