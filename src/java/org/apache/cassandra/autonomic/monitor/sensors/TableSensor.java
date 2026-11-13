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

import java.util.HashSet;
import java.util.Set;

import org.apache.cassandra.autonomic.monitor.ObservationSink;
import org.apache.cassandra.autonomic.monitor.Sensor;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.metrics.LatencyMetrics;
import org.apache.cassandra.metrics.TableMetrics;
import org.apache.cassandra.schema.SchemaConstants;

import static org.apache.cassandra.autonomic.monitor.sensors.SensorSupport.emit;
import static org.apache.cassandra.autonomic.monitor.sensors.SensorSupport.perSec;

/**
 * Per user table metrics, read from {@link TableMetrics} (ADR-013). Rates and mean latencies are computed from counter
 * deltas between collections; p99 values come from the (slowly decaying) histogram snapshots and are exposed
 * alongside. A table dropped between collections is skipped.
 */
public class TableSensor implements Sensor
{
    private static final Set<SignalId> SIGNALS = Set.of(
        Signals.TABLE_READ_OPS_PER_SEC, Signals.TABLE_WRITE_OPS_PER_SEC, Signals.TABLE_RANGE_OPS_PER_SEC,
        Signals.TABLE_READ_LATENCY_MEAN_MICROS, Signals.TABLE_WRITE_LATENCY_MEAN_MICROS, Signals.TABLE_RANGE_LATENCY_MEAN_MICROS,
        Signals.TABLE_READ_LATENCY_P99_MICROS, Signals.TABLE_WRITE_LATENCY_P99_MICROS, Signals.TABLE_RANGE_LATENCY_P99_MICROS,
        Signals.TABLE_COORDINATOR_READ_OPS_PER_SEC, Signals.TABLE_COORDINATOR_WRITE_OPS_PER_SEC,
        Signals.TABLE_COORDINATOR_READ_LATENCY_P99_MICROS, Signals.TABLE_COORDINATOR_WRITE_LATENCY_P99_MICROS,
        Signals.TABLE_SSTABLES_PER_READ_P99, Signals.TABLE_TOMBSTONES_SCANNED_P99, Signals.TABLE_LIVE_CELLS_SCANNED_P99,
        Signals.TABLE_PENDING_COMPACTIONS, Signals.TABLE_LIVE_SSTABLE_COUNT, Signals.TABLE_PENDING_FLUSHES,
        Signals.TABLE_MAX_PARTITION_SIZE_BYTES, Signals.TABLE_MEAN_PARTITION_SIZE_BYTES,
        Signals.TABLE_TOMBSTONE_WARNINGS_DELTA, Signals.TABLE_TOMBSTONE_FAILURES_DELTA);

    private final SensorSupport.Deltas deltas = new SensorSupport.Deltas();
    private long lastCollect = -1;

    @Override
    public String name()
    {
        return "table";
    }

    @Override
    public Set<SignalId> signals()
    {
        return SIGNALS;
    }

    @Override
    public long minIntervalMillis()
    {
        return 5_000;
    }

    @Override
    public void collect(ObservationSink sink, long now)
    {
        long elapsed = lastCollect < 0 ? -1 : now - lastCollect;
        lastCollect = now;
        Set<String> live = new HashSet<>();
        for (ColumnFamilyStore cfs : ColumnFamilyStore.all())
        {
            String keyspace = cfs.getKeyspaceName();
            if (SchemaConstants.isSystemKeyspace(keyspace) || cfs.isIndex())
                continue;
            Subject subject = Subject.table(keyspace, cfs.getTableName());
            live.add(subject.asString() + '|');
            try
            {
                collectTable(sink, now, elapsed, cfs, subject);
            }
            catch (RuntimeException e)
            {
                // table dropped while reading its metrics: skip it, it is gone next time
            }
        }
        // keys are "<subject>|<counter>"; drop baselines of tables that no longer exist
        deltas.retainWhere(key -> live.contains(key.substring(0, key.indexOf('|') + 1)));
    }

    private void collectTable(ObservationSink sink, long now, long elapsed, ColumnFamilyStore cfs, Subject s)
    {
        TableMetrics m = cfs.metric;
        String k = s.asString() + '|';

        latency(sink, now, elapsed, s, k + "read", m.readLatency, Signals.TABLE_READ_OPS_PER_SEC, Signals.TABLE_READ_LATENCY_MEAN_MICROS, Signals.TABLE_READ_LATENCY_P99_MICROS);
        latency(sink, now, elapsed, s, k + "write", m.writeLatency, Signals.TABLE_WRITE_OPS_PER_SEC, Signals.TABLE_WRITE_LATENCY_MEAN_MICROS, Signals.TABLE_WRITE_LATENCY_P99_MICROS);
        latency(sink, now, elapsed, s, k + "range", m.rangeLatency, Signals.TABLE_RANGE_OPS_PER_SEC, Signals.TABLE_RANGE_LATENCY_MEAN_MICROS, Signals.TABLE_RANGE_LATENCY_P99_MICROS);

        emit(sink, Signals.TABLE_COORDINATOR_READ_OPS_PER_SEC, s, now, perSec(deltas.delta(k + "creads", m.coordinatorReadLatency.getCount()), elapsed));
        emit(sink, Signals.TABLE_COORDINATOR_WRITE_OPS_PER_SEC, s, now, perSec(deltas.delta(k + "cwrites", m.coordinatorWriteLatency.getCount()), elapsed));
        emit(sink, Signals.TABLE_COORDINATOR_READ_LATENCY_P99_MICROS, s, now, m.coordinatorReadLatency.getSnapshot().get99thPercentile());
        emit(sink, Signals.TABLE_COORDINATOR_WRITE_LATENCY_P99_MICROS, s, now, m.coordinatorWriteLatency.getSnapshot().get99thPercentile());

        emit(sink, Signals.TABLE_SSTABLES_PER_READ_P99, s, now, m.sstablesPerReadHistogram.cf.getSnapshot().get99thPercentile());
        emit(sink, Signals.TABLE_TOMBSTONES_SCANNED_P99, s, now, m.tombstoneScannedHistogram.cf.getSnapshot().get99thPercentile());
        emit(sink, Signals.TABLE_LIVE_CELLS_SCANNED_P99, s, now, m.liveScannedHistogram.cf.getSnapshot().get99thPercentile());

        emit(sink, Signals.TABLE_PENDING_COMPACTIONS, s, now, number(m.pendingCompactions.getValue()));
        emit(sink, Signals.TABLE_LIVE_SSTABLE_COUNT, s, now, number(m.liveSSTableCount.getValue()));
        emit(sink, Signals.TABLE_PENDING_FLUSHES, s, now, m.pendingFlushes.getCount());
        emit(sink, Signals.TABLE_MAX_PARTITION_SIZE_BYTES, s, now, number(m.maxPartitionSize.getValue()));
        emit(sink, Signals.TABLE_MEAN_PARTITION_SIZE_BYTES, s, now, number(m.meanPartitionSize.getValue()));

        long warn = deltas.delta(k + "tswarn", m.tombstoneWarnings.getCount());
        if (warn >= 0)
            emit(sink, Signals.TABLE_TOMBSTONE_WARNINGS_DELTA, s, now, warn);
        long fail = deltas.delta(k + "tsfail", m.tombstoneFailures.getCount());
        if (fail >= 0)
            emit(sink, Signals.TABLE_TOMBSTONE_FAILURES_DELTA, s, now, fail);
    }

    private void latency(ObservationSink sink, long now, long elapsed, Subject s, String key, LatencyMetrics lm,
                         SignalId ops, SignalId mean, SignalId p99)
    {
        long count = lm.latency.getCount();
        long totalMicros = lm.totalLatency.getCount();
        long dCount = deltas.delta(key + ".n", count);
        long dMicros = deltas.delta(key + ".us", totalMicros);
        emit(sink, ops, s, now, perSec(dCount, elapsed));
        if (dCount > 0 && dMicros >= 0)
            emit(sink, mean, s, now, (double) dMicros / dCount);
        emit(sink, p99, s, now, lm.latency.getSnapshot().get99thPercentile());
    }

    private static double number(Number n)
    {
        return n == null ? Double.NaN : n.doubleValue();
    }
}
