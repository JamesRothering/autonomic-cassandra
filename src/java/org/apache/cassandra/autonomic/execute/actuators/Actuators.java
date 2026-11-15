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
package org.apache.cassandra.autonomic.execute.actuators;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.autonomic.AutonomicConfig;
import org.apache.cassandra.autonomic.execute.Actuator;

/** The built-in actuators, built from {@code autonomic.actuators} when the layer starts. Each is off until enabled. */
public final class Actuators
{
    private Actuators()
    {
    }

    public static List<Actuator> build(AutonomicConfig config)
    {
        List<Actuator> out = new ArrayList<>();
        out.add(CompactionThroughputActuator.create(options(config, CompactionThroughputActuator.NAME)));
        out.add(ConcurrentCompactorsActuator.create());
        out.add(StreamThroughputActuator.create(options(config, StreamThroughputActuator.NAME)));
        out.add(HintThrottleActuator.create(options(config, HintThrottleActuator.NAME)));
        out.add(CacheCapacityActuator.key(options(config, CacheCapacityActuator.KEY)));
        out.add(CacheCapacityActuator.row(options(config, CacheCapacityActuator.ROW)));
        out.add(CacheCapacityActuator.counter(options(config, CacheCapacityActuator.COUNTER)));
        out.add(AdvertisedSeverityActuator.create());
        for (String pool : RequestPoolActuator.STAGES.keySet())
            out.add(RequestPoolActuator.create(pool, options(config, pool)));
        out.add(RequestThrottleActuator.create(options(config, RequestThrottleActuator.NAME)));
        out.add(GarbageCollectActuator.create());
        out.add(RepairPriorityActuator.create());
        return out;
    }

    private static Map<String, Object> options(AutonomicConfig config, String name)
    {
        return config.actuators == null ? null : config.actuators.get(name);
    }
}
