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
import org.apache.cassandra.autonomic.execute.actuators.CompactionThroughputActuator;
import org.apache.cassandra.autonomic.execute.actuators.ConcurrentCompactorsActuator;
import org.apache.cassandra.autonomic.execute.actuators.KnobActuator.Direction;
import org.apache.cassandra.autonomic.knowledge.KnowledgeView;
import org.apache.cassandra.autonomic.monitor.sensors.Signals;
import org.apache.cassandra.autonomic.plan.VerifySpec;

/**
 * {@code compaction_backlog}: let compaction run faster, first more throughput, then more compactors, while the
 * foreground is healthy (no {@code latency_slo_breach} or {@code degraded_self}). Goal: fewer pending tasks;
 * guards: coordinator read and write p99.
 */
public class CompactionBacklogPolicy extends KnobPolicy
{
    public static final String NAME = "compaction_backlog";

    public CompactionBacklogPolicy(Function<String, Actuator> actuators, Function<String, Windows> windows)
    {
        super(NAME, "compaction_backlog",
              steps(CompactionThroughputActuator.NAME, Direction.UP, ConcurrentCompactorsActuator.NAME, Direction.UP),
              actuators, windows);
    }

    @Override
    boolean applicable(Diagnosis diagnosis, KnowledgeView knowledge)
    {
        return !active(knowledge, "latency_slo_breach", "degraded_self");
    }

    @Override
    VerifySpec verify(Diagnosis diagnosis, KnowledgeView knowledge, Windows w)
    {
        return spec(Signals.COMPACTION_PENDING_TASKS, NODE, w, readLatencyGuard(), writeLatencyGuard());
    }
}
