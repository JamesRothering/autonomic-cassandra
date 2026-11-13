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
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.autonomic.monitor.ObservationSink;
import org.apache.cassandra.autonomic.monitor.Sensor;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.metrics.CassandraMetricsRegistry;
import org.apache.cassandra.metrics.ThreadPoolMetrics;

import static org.apache.cassandra.autonomic.monitor.sensors.SensorSupport.emit;

/** Per thread pool activity from the same source as {@code system_views.thread_pools}. */
public class ThreadPoolSensor implements Sensor
{
    private static final Set<SignalId> SIGNALS = Set.of(
        Signals.POOL_ACTIVE_TASKS, Signals.POOL_PENDING_TASKS, Signals.POOL_CURRENTLY_BLOCKED_TASKS,
        Signals.POOL_BLOCKED_TASKS_DELTA, Signals.POOL_MAX_POOL_SIZE, Signals.POOL_OLDEST_TASK_QUEUE_MILLIS);

    private final SensorSupport.Deltas deltas = new SensorSupport.Deltas();

    @Override
    public String name()
    {
        return "thread_pool";
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
        Set<String> live = new HashSet<>();
        for (ThreadPoolMetrics m : CassandraMetricsRegistry.Metrics.allThreadPoolMetrics())
        {
            try
            {
                Subject s = Subject.threadPool(m.poolName);
                live.add(m.poolName);
                emit(sink, Signals.POOL_ACTIVE_TASKS, s, now, m.activeTasks.getValue());
                emit(sink, Signals.POOL_PENDING_TASKS, s, now, m.pendingTasks.getValue());
                emit(sink, Signals.POOL_CURRENTLY_BLOCKED_TASKS, s, now, m.currentBlocked.getCount());
                long blocked = deltas.delta(m.poolName, m.totalBlocked.getCount());
                if (blocked >= 0)
                    emit(sink, Signals.POOL_BLOCKED_TASKS_DELTA, s, now, blocked);
                emit(sink, Signals.POOL_MAX_POOL_SIZE, s, now, m.maxPoolSize.getValue());
                Long oldest = m.oldestTaskQueueTime.getValue();
                emit(sink, Signals.POOL_OLDEST_TASK_QUEUE_MILLIS, s, now, oldest == null ? 0 : TimeUnit.NANOSECONDS.toMillis(oldest));
            }
            catch (RuntimeException e)
            {
                // pool released while being read; skip it
            }
        }
        deltas.retainWhere(live::contains);
    }
}
