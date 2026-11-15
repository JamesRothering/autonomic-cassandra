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

import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.Set;

import org.apache.cassandra.autonomic.monitor.ObservationSink;
import org.apache.cassandra.autonomic.monitor.Sensor;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;

import static org.apache.cassandra.autonomic.monitor.sensors.SensorSupport.emit;

/**
 * Whether the node's CPUs are saturated, for the pool elasticity policy: the system load average per processor.
 * It is a one-minute average, so it lags a change of load; nothing is emitted where the platform has none.
 */
public class CpuSensor implements Sensor
{
    private static final Set<SignalId> SIGNALS = Set.of(Signals.OS_LOAD_PER_PROCESSOR);

    private final OperatingSystemMXBean os = ManagementFactory.getOperatingSystemMXBean();

    @Override
    public String name()
    {
        return "cpu";
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
        double load = os.getSystemLoadAverage();
        int processors = os.getAvailableProcessors();
        if (load >= 0 && processors > 0)
            emit(sink, Signals.OS_LOAD_PER_PROCESSOR, Subject.node(), now, load / processors);
    }
}
