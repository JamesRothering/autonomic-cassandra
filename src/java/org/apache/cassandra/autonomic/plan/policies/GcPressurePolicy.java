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
package org.apache.cassandra.autonomic.plan.policies;

import java.util.function.Function;

import org.apache.cassandra.autonomic.analyze.Diagnosis;
import org.apache.cassandra.autonomic.execute.Actuator;
import org.apache.cassandra.autonomic.execute.actuators.CacheCapacityActuator;
import org.apache.cassandra.autonomic.execute.actuators.KnobActuator.Direction;
import org.apache.cassandra.autonomic.knowledge.KnowledgeView;
import org.apache.cassandra.autonomic.monitor.sensors.Signals;
import org.apache.cassandra.autonomic.plan.VerifySpec;

/**
 * {@code gc_pressure}: shrink an on-heap cache by one step, the row cache first, then the counter and key caches.
 * Goal: less time in GC; guard: coordinator read p99.
 */
public class GcPressurePolicy extends KnobPolicy
{
    public static final String NAME = "gc_pressure";

    public GcPressurePolicy(Function<String, Actuator> actuators, Function<String, Windows> windows)
    {
        super(NAME, "gc_pressure",
              steps(CacheCapacityActuator.ROW, Direction.DOWN, CacheCapacityActuator.COUNTER, Direction.DOWN,
                    CacheCapacityActuator.KEY, Direction.DOWN),
              actuators, windows);
    }

    @Override
    VerifySpec verify(Diagnosis diagnosis, KnowledgeView knowledge, Windows w)
    {
        return spec(Signals.GC_TIME_FRACTION, NODE, w, readLatencyGuard());
    }
}
