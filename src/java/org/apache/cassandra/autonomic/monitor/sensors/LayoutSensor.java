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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.autonomic.coord.ClusterLedger;
import org.apache.cassandra.autonomic.layout.KeyState;
import org.apache.cassandra.autonomic.layout.LayoutProfiler;
import org.apache.cassandra.autonomic.layout.LayoutRegistry;
import org.apache.cassandra.autonomic.layout.Phase;
import org.apache.cassandra.autonomic.layout.TableLayout;
import org.apache.cassandra.autonomic.monitor.ObservationSink;
import org.apache.cassandra.autonomic.monitor.Sensor;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;

import static org.apache.cassandra.autonomic.monitor.sensors.SensorSupport.emit;

/**
 * The key signals of adaptive-schema design 7.3, from {@link LayoutProfiler}. It does two things on the monitor thread:
 * it keeps watched, on this node, every key of the layout registry whose migration is running (not STEADY, or STEADY
 * with its action still holding the claim), so the profile stays on for the whole migration; and it emits, for every
 * watched key, the deltas of the profiler's counters since the previous collection as means and an error rate. A
 * collection with nothing watched and an empty registry does nothing else.
 *
 * <p>The signals are what this node coordinated: a node that coordinates none of a key's requests has none.
 */
public class LayoutSensor implements Sensor
{
    /** A watch kept by the sensor expires this long after the sensor stops refreshing it. */
    public static final long KEEP_WATCH_MILLIS = 60_000;

    private static final Set<SignalId> SIGNALS = Set.of(
        Signals.KEY_READ_LATENCY_MICROS, Signals.KEY_WRITE_LATENCY_MICROS, Signals.KEY_ERROR_RATE,
        Signals.KEY_READ_OPS_DELTA, Signals.KEY_WRITE_OPS_DELTA);

    private final Map<String, LayoutProfiler.Shape> previous = new HashMap<>();

    @Override
    public String name()
    {
        return "layout";
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
        keepMigrationsWatched(ClusterMetadata.currentNullable());
        LayoutProfiler.sweep(now);
        Set<String> live = new HashSet<>();
        for (LayoutProfiler.Watch watch : LayoutProfiler.all())
        {
            TableMetadata table = Schema.instance.getTableMetadata(watch.table);
            if (table == null)
                continue;
            Subject subject = PartitionKeys.subject(table.keyspace, table.name, watch.key);
            String id = subject.asString();
            live.add(id);
            LayoutProfiler.Shape current = watch.shape();
            LayoutProfiler.Shape before = previous.put(id, current);
            if (before == null || current.timedReads < before.timedReads || current.timedWrites < before.timedWrites)
                continue;
            long reads = current.timedReads - before.timedReads;
            long writes = current.timedWrites - before.timedWrites;
            long errors = (current.readErrors - before.readErrors) + (current.writeErrors - before.writeErrors);
            emit(sink, Signals.KEY_READ_OPS_DELTA, subject, now, reads);
            emit(sink, Signals.KEY_WRITE_OPS_DELTA, subject, now, writes);
            long readErrors = current.readErrors - before.readErrors;
            long writeErrors = current.writeErrors - before.writeErrors;
            if (reads - readErrors > 0)
                emit(sink, Signals.KEY_READ_LATENCY_MICROS, subject, now, (double) (current.readMicros - before.readMicros) / (reads - readErrors));
            if (writes - writeErrors > 0)
                emit(sink, Signals.KEY_WRITE_LATENCY_MICROS, subject, now, (double) (current.writeMicros - before.writeMicros) / (writes - writeErrors));
            if (reads + writes > 0)
                emit(sink, Signals.KEY_ERROR_RATE, subject, now, (double) errors / (reads + writes));
        }
        previous.keySet().retainAll(live);
    }

    /** Package-visible for tests: the hooks of the keys of running migrations on this node. */
    static void keepMigrationsWatched(ClusterMetadata cm)
    {
        if (cm == null)
            return;
        LayoutRegistry registry = LayoutRegistry.of(cm);
        if (registry.byLogical.isEmpty())
            return;
        ClusterLedger ledger = ClusterLedger.of(cm);
        for (TableLayout layout : registry.byLogical.values())
            for (Map.Entry<ByteBuffer, KeyState> e : layout.keys.entrySet())
            {
                KeyState key = e.getValue();
                boolean running = key.phase != Phase.STEADY || (key.actionId != null && ledger.claim(key.actionId) != null);
                if (running)
                    LayoutProfiler.watch(layout.logical, e.getKey(), KEEP_WATCH_MILLIS);
            }
    }
}
