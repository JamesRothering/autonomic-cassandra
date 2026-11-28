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

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import org.apache.cassandra.autonomic.analyze.Diagnosis;
import org.apache.cassandra.autonomic.analyze.Severity;
import org.apache.cassandra.autonomic.execute.ActionRecord;
import org.apache.cassandra.autonomic.execute.ActionState;
import org.apache.cassandra.autonomic.execute.Actuator;
import org.apache.cassandra.autonomic.execute.actuators.CompactionThroughputActuator;
import org.apache.cassandra.autonomic.execute.actuators.GarbageCollectActuator;
import org.apache.cassandra.autonomic.execute.actuators.KnobActuator;
import org.apache.cassandra.autonomic.knowledge.KnowledgeView;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.monitor.sensors.Signals;
import org.apache.cassandra.autonomic.plan.Action;
import org.apache.cassandra.autonomic.plan.ActionScope;
import org.apache.cassandra.autonomic.plan.Plan;
import org.apache.cassandra.autonomic.plan.Policy;
import org.apache.cassandra.autonomic.plan.VerifySpec;
import org.apache.cassandra.utils.TimeUUID;

/**
 * {@code tombstone_overload} on a table. At WARN, more compaction throughput (compaction is what purges tombstones
 * once they are past gc_grace). At CRITICAL, one garbage-collecting compaction of the table, the irreversible
 * actuator of rule 10, unless one ran on it in the last day. Goal: fewer tombstones scanned per read; guard: the
 * table's read p99. Verification of the irreversible action is for reporting only.
 */
public class TombstoneOverloadPolicy extends KnobPolicy
{
    public static final String NAME = "tombstone_overload";

    private final Function<String, Actuator> actuators;
    private final Function<String, Windows> windows;

    public TombstoneOverloadPolicy(Function<String, Actuator> actuators, Function<String, Windows> windows)
    {
        super(NAME, "tombstone_overload", steps(CompactionThroughputActuator.NAME, KnobActuator.Direction.UP), actuators, windows);
        this.actuators = actuators;
        this.windows = windows;
    }

    @Override
    public Set<SignalId> inputs()
    {
        return Set.of(Signals.TABLE_TOMBSTONES_SCANNED_P99, Signals.TABLE_READ_LATENCY_P99_MICROS, Signals.TABLE_READ_OPS_PER_SEC);
    }

    /** The compaction throughput step, and at CRITICAL the garbage-collecting compaction. */
    @Override
    public Set<String> actuations()
    {
        Set<String> out = new LinkedHashSet<>(super.actuations());
        out.add(Policy.actuation(GarbageCollectActuator.NAME, Policy.APPLY));
        return out;
    }

    @Override
    VerifySpec verify(Diagnosis diagnosis, KnowledgeView knowledge, Windows w)
    {
        return spec(Signals.TABLE_TOMBSTONES_SCANNED_P99, diagnosis.subject, w,
                    new VerifySpec.Guard(Signals.TABLE_READ_LATENCY_P99_MICROS, diagnosis.subject, true, LATENCY_TOLERANCE, Signals.TABLE_READ_OPS_PER_SEC));
    }

    @Override
    public Optional<Plan> plan(Diagnosis diagnosis, KnowledgeView knowledge)
    {
        if (diagnosis.subject.kind != Subject.Kind.TABLE)
            return Optional.empty();
        if (diagnosis.severity != Severity.CRITICAL)
            return super.plan(diagnosis, knowledge);
        if (actuators.apply(GarbageCollectActuator.NAME) == null || collectedToday(diagnosis.subject, knowledge))
            return Optional.empty();
        Optional<VerifySpec> verify = Policies.forProblem(verify(diagnosis, knowledge, windows.apply(GarbageCollectActuator.NAME)), diagnosis, knowledge, NAME);
        if (!verify.isPresent())
            return Optional.empty();
        Action action = new Action(TimeUUID.Generator.nextTimeUUID().asUUID(), GarbageCollectActuator.NAME, ActionScope.NODE,
                                   diagnosis.subject, Map.of(GarbageCollectActuator.PARAM_OPTION, "ROW", KnobActuator.PARAM_DIAGNOSIS, diagnosis.id.toString()),
                                   verify.get());
        return Optional.of(new Plan(TimeUUID.Generator.nextTimeUUID().asUUID(), diagnosis.id, NAME, List.of(action),
                                    "tombstone_overload on " + diagnosis.subject.asString() + " is CRITICAL: garbage-collecting compaction"));
    }

    private static boolean collectedToday(Subject table, KnowledgeView knowledge)
    {
        for (ActionRecord r : knowledge.recentActions(GarbageCollectActuator.INTERVAL_MILLIS))
            if (r.action.actuator.equals(GarbageCollectActuator.NAME) && r.action.target.equals(table)
                && r.state != ActionState.REJECTED && r.state != ActionState.PROPOSED)
                return true;
        return false;
    }
}
