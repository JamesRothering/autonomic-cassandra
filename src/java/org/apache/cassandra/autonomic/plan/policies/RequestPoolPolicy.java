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
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.function.Function;

import org.apache.cassandra.autonomic.analyze.Diagnosis;
import org.apache.cassandra.autonomic.analyze.detectors.BaselineDriftDetector;
import org.apache.cassandra.autonomic.analyze.detectors.PoolSaturationDetector;
import org.apache.cassandra.autonomic.execute.Actuator;
import org.apache.cassandra.autonomic.execute.actuators.KnobActuator;
import org.apache.cassandra.autonomic.execute.actuators.RequestPoolActuator;
import org.apache.cassandra.autonomic.knowledge.KnowledgeView;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.monitor.sensors.Signals;
import org.apache.cassandra.autonomic.plan.Action;
import org.apache.cassandra.autonomic.plan.ActionScope;
import org.apache.cassandra.autonomic.plan.Plan;
import org.apache.cassandra.autonomic.plan.Policy;
import org.apache.cassandra.autonomic.plan.VerifySpec;
import org.apache.cassandra.utils.TimeUUID;

/**
 * Request pool elasticity (overview 8.1).
 * <ul>
 * <li>{@code pool_saturation} of a request stage: one step up for that stage's pool while the CPUs are not
 * saturated ({@code os.load_per_processor} below {@link #CPU_SATURATED}, or unknown). While they are, a pool that is
 * above its configured size is stepped back towards it. Goal: fewer queued tasks in that pool; guards: coordinator p99.</li>
 * <li>{@code autonomic.baseline_drift}: recovery. A request pool that differs from its configured size while its
 * stage is not saturated is stepped back towards the configured size, so a recovered node returns to baseline.
 * Goal: queued tasks do not grow; guards: coordinator p99.</li>
 * </ul>
 * Policies only see open diagnoses, so recovery is driven by the drift diagnosis, not by the end of the saturation.
 */
public class RequestPoolPolicy implements Policy
{
    public static final String NAME = "request_pools";
    /** Load average per processor from which the CPUs count as saturated. */
    public static final double CPU_SATURATED = 0.9;
    static final double STEP_FRACTION = 0.2;

    private final Function<String, Actuator> actuators;
    private final Function<String, Windows> windows;

    public RequestPoolPolicy(Function<String, Actuator> actuators, Function<String, Windows> windows)
    {
        this.actuators = actuators;
        this.windows = windows;
    }

    @Override
    public String name()
    {
        return NAME;
    }

    @Override
    public Set<String> handles()
    {
        return Set.of(PoolSaturationDetector.NAME, BaselineDriftDetector.TYPE);
    }

    /** The pool actuator of a pool name, for example {@code ReadStage}. */
    static String actuatorOf(String pool)
    {
        for (java.util.Map.Entry<String, String> e : RequestPoolActuator.STAGES.entrySet())
            if (e.getValue().equals(pool))
                return e.getKey();
        return null;
    }

    static boolean cpuSaturated(KnowledgeView knowledge)
    {
        OptionalDouble load = knowledge.latest(Signals.OS_LOAD_PER_PROCESSOR, Subject.node());
        return load.isPresent() && load.getAsDouble() >= CPU_SATURATED;
    }

    private static boolean saturated(KnowledgeView knowledge, String pool)
    {
        for (Diagnosis d : knowledge.activeDiagnoses())
            if (d.type.equals(PoolSaturationDetector.NAME) && pool.equals(d.subject.detail))
                return true;
        return false;
    }

    @Override
    public Optional<Plan> plan(Diagnosis diagnosis, KnowledgeView knowledge)
    {
        if (diagnosis.type.equals(PoolSaturationDetector.NAME))
            return saturation(diagnosis, knowledge);
        for (String actuator : diagnosis.evidence.keySet())
        {
            String pool = RequestPoolActuator.STAGES.get(actuator);
            if (pool == null || saturated(knowledge, pool))
                continue;
            Optional<Plan> plan = back(diagnosis, knowledge, actuator, pool, 0.0);
            if (plan.isPresent())
                return plan;
        }
        return Optional.empty();
    }

    private Optional<Plan> saturation(Diagnosis diagnosis, KnowledgeView knowledge)
    {
        String actuator = actuatorOf(diagnosis.subject.detail);
        if (actuator == null || !(actuators.apply(actuator) instanceof KnobActuator))
            return Optional.empty();
        KnobActuator knob = (KnobActuator) actuators.apply(actuator);
        if (cpuSaturated(knowledge))
            return back(diagnosis, knowledge, actuator, diagnosis.subject.detail, 0.10);
        Optional<Double> target = knob.nextTarget(KnobActuator.Direction.UP, STEP_FRACTION, diagnosis.id.toString());
        return target.map(t -> plan(diagnosis, knob, t, diagnosis.subject.detail, 0.10,
                                    diagnosis.type + " on " + diagnosis.subject.detail + ": " + actuator + ' '
                                    + KnobActuator.format(knob.current()) + " -> " + KnobActuator.format(t)));
    }

    /** One step towards the configured size, with the goal tolerance given (0: the queue must not grow). */
    private Optional<Plan> back(Diagnosis diagnosis, KnowledgeView knowledge, String actuator, String pool, double minImprovement)
    {
        if (!(actuators.apply(actuator) instanceof KnobActuator))
            return Optional.empty();
        KnobActuator knob = (KnobActuator) actuators.apply(actuator);
        if (knob.current() <= knob.configured() && diagnosis.type.equals(PoolSaturationDetector.NAME))
            return Optional.empty();
        return knob.nextTargetTowardConfigured(STEP_FRACTION).map(t -> plan(diagnosis, knob, t, pool, minImprovement,
                                                                 diagnosis.type + ": " + actuator + ' ' + KnobActuator.format(knob.current())
                                                                 + " back towards " + KnobActuator.format(knob.configured())));
    }

    private Plan plan(Diagnosis diagnosis, KnobActuator knob, double target, String pool, double minImprovement, String rationale)
    {
        Windows w = windows.apply(knob.name());
        VerifySpec spec = new VerifySpec(Signals.POOL_PENDING_TASKS, Subject.threadPool(pool), true, minImprovement,
                                         List.of(KnobPolicy.readLatencyGuard(), KnobPolicy.writeLatencyGuard()),
                                         w.baselineMillis, w.observationMillis);
        Action action = new Action(TimeUUID.Generator.nextTimeUUID().asUUID(), knob.name(), ActionScope.NODE, Subject.node(),
                                   KnobActuator.params(target, diagnosis.id.toString()), spec);
        return new Plan(TimeUUID.Generator.nextTimeUUID().asUUID(), diagnosis.id, NAME, List.of(action), rationale);
    }
}
