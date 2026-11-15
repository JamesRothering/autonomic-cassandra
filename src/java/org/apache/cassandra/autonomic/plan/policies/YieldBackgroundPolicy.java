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

import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.cassandra.autonomic.analyze.Diagnosis;
import org.apache.cassandra.autonomic.analyze.detectors.PoolSaturationDetector;
import org.apache.cassandra.autonomic.execute.Actuator;
import org.apache.cassandra.autonomic.execute.actuators.AdvertisedSeverityActuator;
import org.apache.cassandra.autonomic.execute.actuators.CompactionThroughputActuator;
import org.apache.cassandra.autonomic.execute.actuators.HintThrottleActuator;
import org.apache.cassandra.autonomic.execute.actuators.KnobActuator.Direction;
import org.apache.cassandra.autonomic.execute.actuators.StreamThroughputActuator;
import org.apache.cassandra.autonomic.knowledge.KnowledgeView;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.monitor.sensors.Signals;
import org.apache.cassandra.autonomic.plan.VerifySpec;

/**
 * A step of the load spike ladder that moves knobs one step at a time, whatever the load diagnosis: yield
 * background work (compaction throughput, stream throughput, hint throttle, in that order), or advertise a higher
 * severity. Only knobs whose actuator is enabled are proposed. Goal: the queue of a saturated pool, or the worse of
 * the coordinator p99s according to the diagnosis evidence; guard: the other one.
 */
public class YieldBackgroundPolicy extends KnobPolicy
{
    public static final String YIELD = "yield_background";
    public static final String ADVERTISE = "advertise_severity";

    private final Predicate<String> enabled;

    private YieldBackgroundPolicy(String name, List<Step> steps, Function<String, Actuator> actuators, Function<String, Windows> windows,
                                  Predicate<String> enabled)
    {
        super(name, "load_spike", steps, actuators, windows);
        this.enabled = enabled;
    }

    /** Compaction, streaming and hints give way, in that order. */
    public static YieldBackgroundPolicy background(Function<String, Actuator> actuators, Function<String, Windows> windows, Predicate<String> enabled)
    {
        return new YieldBackgroundPolicy(YIELD, steps(CompactionThroughputActuator.NAME, Direction.DOWN, StreamThroughputActuator.NAME, Direction.DOWN,
                                                      HintThrottleActuator.NAME, Direction.DOWN), actuators, windows, enabled);
    }

    /** Coordinators prefer other replicas of a node that is slow. */
    public static YieldBackgroundPolicy advertise(Function<String, Actuator> actuators, Function<String, Windows> windows, Predicate<String> enabled)
    {
        return new YieldBackgroundPolicy(ADVERTISE, steps(AdvertisedSeverityActuator.NAME, Direction.UP), actuators, windows, enabled);
    }

    /** The actuators this policy may use. */
    public static List<String> yieldActuators()
    {
        return List.of(CompactionThroughputActuator.NAME, StreamThroughputActuator.NAME, HintThrottleActuator.NAME);
    }

    @Override
    boolean usable(String actuator)
    {
        return enabled.test(actuator);
    }

    /** True if the diagnosis evidence has the write p99 further above its SLO than the read p99. */
    static boolean writeWorse(Diagnosis diagnosis)
    {
        return ratio(diagnosis, Signals.CLIENT_WRITE_LATENCY_P99_MICROS) > ratio(diagnosis, Signals.CLIENT_READ_LATENCY_P99_MICROS);
    }

    /** Evidence is {@code "<value> us (slo <slo> us)"} or {@code "<value> us (read slo <slo> us)"}; 0 if absent or unparsable. */
    private static double ratio(Diagnosis diagnosis, SignalId signal)
    {
        String e = diagnosis.evidence.get(signal.name);
        try
        {
            String[] parts = e.split(" us \\((?:read |write )?slo ");
            return Double.parseDouble(parts[0]) / Double.parseDouble(parts[1].replace(" us)", ""));
        }
        catch (RuntimeException ex)
        {
            return 0;
        }
    }

    @Override
    VerifySpec verify(Diagnosis diagnosis, KnowledgeView knowledge, Windows w)
    {
        if (diagnosis.type.equals(PoolSaturationDetector.NAME))
            return spec(Signals.POOL_PENDING_TASKS, Subject.threadPool(String.valueOf(diagnosis.subject.detail)), w, readLatencyGuard(), writeLatencyGuard());
        return writeWorse(diagnosis) ? spec(Signals.CLIENT_WRITE_LATENCY_P99_MICROS, NODE, w, readLatencyGuard())
                                     : spec(Signals.CLIENT_READ_LATENCY_P99_MICROS, NODE, w, writeLatencyGuard());
    }
}
