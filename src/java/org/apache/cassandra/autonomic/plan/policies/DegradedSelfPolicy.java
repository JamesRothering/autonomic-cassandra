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

import java.util.Set;
import java.util.function.Function;

import org.apache.cassandra.autonomic.analyze.Diagnosis;
import org.apache.cassandra.autonomic.execute.Actuator;
import org.apache.cassandra.autonomic.execute.actuators.AdvertisedSeverityActuator;
import org.apache.cassandra.autonomic.execute.actuators.CompactionThroughputActuator;
import org.apache.cassandra.autonomic.execute.actuators.HintThrottleActuator;
import org.apache.cassandra.autonomic.execute.actuators.KnobActuator.Direction;
import org.apache.cassandra.autonomic.execute.actuators.StreamThroughputActuator;
import org.apache.cassandra.autonomic.knowledge.KnowledgeView;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.sensors.Signals;
import org.apache.cassandra.autonomic.plan.VerifySpec;

/**
 * {@code degraded_self}: first yield background work (compaction, streaming, hints, in that order), then raise the
 * advertised severity so that coordinators prefer other replicas (overview 8.1). Goal: the coordinator p99 that is
 * further above its SLO according to the diagnosis evidence (read if unknown); guard: the other one.
 */
public class DegradedSelfPolicy extends KnobPolicy
{
    public static final String NAME = "degraded_self";

    public DegradedSelfPolicy(Function<String, Actuator> actuators, Function<String, Windows> windows)
    {
        super(NAME, "degraded_self",
              steps(CompactionThroughputActuator.NAME, Direction.DOWN, StreamThroughputActuator.NAME, Direction.DOWN,
                    HintThrottleActuator.NAME, Direction.DOWN, AdvertisedSeverityActuator.NAME, Direction.UP),
              actuators, windows);
    }

    @Override
    public Set<SignalId> inputs()
    {
        return Set.of(Signals.CLIENT_READ_LATENCY_P99_MICROS, Signals.CLIENT_READ_OPS_PER_SEC,
                      Signals.CLIENT_WRITE_LATENCY_P99_MICROS, Signals.CLIENT_WRITE_OPS_PER_SEC);
    }

    /** Evidence is {@code "<value> us (slo <slo> us)"}; the ratio to the SLO, or 0 if absent or unparsable. */
    private static double ratio(Diagnosis diagnosis, SignalId signal)
    {
        String e = diagnosis.evidence.get(signal.name);
        try
        {
            String[] parts = e.split(" us \\(slo ");
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
        boolean write = ratio(diagnosis, Signals.CLIENT_WRITE_LATENCY_P99_MICROS) > ratio(diagnosis, Signals.CLIENT_READ_LATENCY_P99_MICROS);
        return write ? spec(Signals.CLIENT_WRITE_LATENCY_P99_MICROS, NODE, w, readLatencyGuard())
                     : spec(Signals.CLIENT_READ_LATENCY_P99_MICROS, NODE, w, writeLatencyGuard());
    }
}
