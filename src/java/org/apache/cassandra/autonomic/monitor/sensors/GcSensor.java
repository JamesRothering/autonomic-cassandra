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

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryUsage;
import java.util.List;
import java.util.Set;

import org.apache.cassandra.autonomic.monitor.ObservationSink;
import org.apache.cassandra.autonomic.monitor.Sensor;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;

import static org.apache.cassandra.autonomic.monitor.sensors.SensorSupport.emit;

/**
 * GC activity from {@link GarbageCollectorMXBean} deltas and heap occupancy from the memory bean. It deliberately
 * does not use the stats-resetting call of {@code GCInspector}, which resets counters shared with {@code nodetool gcstats}.
 */
public class GcSensor implements Sensor
{
    private static final Set<SignalId> SIGNALS = Set.of(
        Signals.GC_TIME_FRACTION, Signals.GC_COLLECTIONS_DELTA, Signals.GC_TIME_MILLIS_DELTA, Signals.GC_HEAP_USED_FRACTION);

    private final List<GarbageCollectorMXBean> beans = ManagementFactory.getGarbageCollectorMXBeans();
    private long lastCollect = -1;
    private long lastCount;
    private long lastTime;

    @Override
    public String name()
    {
        return "gc";
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
        Subject node = Subject.node();
        long count = 0;
        long time = 0;
        for (GarbageCollectorMXBean bean : beans)
        {
            // both return -1 when undefined for a collector
            count += Math.max(0, bean.getCollectionCount());
            time += Math.max(0, bean.getCollectionTime());
        }
        if (lastCollect >= 0 && now > lastCollect && count >= lastCount && time >= lastTime)
        {
            emit(sink, Signals.GC_COLLECTIONS_DELTA, node, now, count - lastCount);
            emit(sink, Signals.GC_TIME_MILLIS_DELTA, node, now, time - lastTime);
            emit(sink, Signals.GC_TIME_FRACTION, node, now, Math.min(1.0, (double) (time - lastTime) / (now - lastCollect)));
        }
        lastCollect = now;
        lastCount = count;
        lastTime = time;

        MemoryUsage heap = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
        long denominator = heap.getMax() > 0 ? heap.getMax() : heap.getCommitted();
        if (denominator > 0)
            emit(sink, Signals.GC_HEAP_USED_FRACTION, node, now, (double) heap.getUsed() / denominator);
    }
}
