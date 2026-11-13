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

import java.util.Set;

import org.apache.cassandra.autonomic.monitor.ObservationSink;
import org.apache.cassandra.autonomic.monitor.Sensor;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.metrics.CompactionMetrics;

import static org.apache.cassandra.autonomic.monitor.sensors.SensorSupport.emit;
import static org.apache.cassandra.autonomic.monitor.sensors.SensorSupport.perSec;

/** Node-wide pending compaction tasks and compaction throughput (delta of the bytes compacted counter). */
public class CompactionSensor implements Sensor
{
    private static final Set<SignalId> SIGNALS = Set.of(Signals.COMPACTION_PENDING_TASKS, Signals.COMPACTION_BYTES_PER_SEC);

    private final SensorSupport.Deltas deltas = new SensorSupport.Deltas();
    private long lastCollect = -1;

    @Override
    public String name()
    {
        return "compaction";
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
        CompactionMetrics m = CompactionManager.instance.getMetrics();
        Subject node = Subject.node();
        emit(sink, Signals.COMPACTION_PENDING_TASKS, node, now, m.pendingTasks.getValue());
        emit(sink, Signals.COMPACTION_BYTES_PER_SEC, node, now, perSec(deltas.delta("bytes", m.bytesCompacted.getCount()), elapsed));
    }
}
