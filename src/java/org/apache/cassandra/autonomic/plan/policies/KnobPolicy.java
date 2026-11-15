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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import org.apache.cassandra.autonomic.analyze.Diagnosis;
import org.apache.cassandra.autonomic.execute.Actuator;
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
 * A policy that answers a diagnosis with one step of the first knob in an ordered list that can still move: it is
 * managed, inside its envelope, not overridden by an operator and not direction-locked for this diagnosis (the order
 * is the priority of overview 8.1). Returns nothing when none can. The policy does not look at whether an actuator is
 * enabled: the governor rejects, or in dry-run only records, what is not enabled.
 */
abstract class KnobPolicy implements Policy
{
    /** One step of one knob. */
    static final class Step
    {
        final String actuator;
        final KnobActuator.Direction direction;

        Step(String actuator, KnobActuator.Direction direction)
        {
            this.actuator = actuator;
            this.direction = direction;
        }
    }

    /** Fraction of the current value per step, below the governor's default {@code max_step_fraction} of 0.25. */
    static final double STEP_FRACTION = 0.2;
    /** Foreground latency is not allowed to worsen by more than this (relative) while a knob is being tried. */
    static final double LATENCY_TOLERANCE = 0.25;

    static final Subject NODE = Subject.node();

    private final String name;
    private final Set<String> handles;
    private final List<Step> steps;
    private final Function<String, Actuator> actuators;
    private final Function<String, Windows> windows;

    KnobPolicy(String name, String handles, List<Step> steps, Function<String, Actuator> actuators, Function<String, Windows> windows)
    {
        this.name = name;
        this.handles = Set.of(handles);
        this.steps = steps;
        this.actuators = actuators;
        this.windows = windows;
    }

    @Override
    public String name()
    {
        return name;
    }

    @Override
    public Set<String> handles()
    {
        return handles;
    }

    /** What the action must achieve and must not harm, over the given windows. */
    abstract VerifySpec verify(Diagnosis diagnosis, KnowledgeView knowledge, Windows windows);

    /** False for a knob that must not be proposed, for example one whose actuator is not enabled. */
    boolean usable(String actuator)
    {
        return true;
    }

    /** Veto before any knob is considered, for example an unhealthy foreground. */
    boolean applicable(Diagnosis diagnosis, KnowledgeView knowledge)
    {
        return true;
    }

    @Override
    public Optional<Plan> plan(Diagnosis diagnosis, KnowledgeView knowledge)
    {
        if (!applicable(diagnosis, knowledge))
            return Optional.empty();
        for (Step step : steps)
        {
            Actuator actuator = actuators.apply(step.actuator);
            if (!(actuator instanceof KnobActuator) || !usable(step.actuator))
                continue;
            KnobActuator knob = (KnobActuator) actuator;
            Optional<Double> target = knob.nextTarget(step.direction, STEP_FRACTION, diagnosis.id.toString());
            if (!target.isPresent())
                continue;
            Action action = new Action(TimeUUID.Generator.nextTimeUUID().asUUID(), knob.name(), ActionScope.NODE, NODE,
                                       KnobActuator.params(target.get(), diagnosis.id.toString()),
                                       verify(diagnosis, knowledge, windows.apply(step.actuator)));
            String rationale = diagnosis.type + " on " + diagnosis.subject.asString() + ": " + knob.name() + ' '
                               + KnobActuator.format(knob.current()) + " -> " + KnobActuator.format(target.get());
            return Optional.of(new Plan(TimeUUID.Generator.nextTimeUUID().asUUID(), diagnosis.id, name, List.of(action), rationale));
        }
        return Optional.empty();
    }

    static VerifySpec.Guard readLatencyGuard()
    {
        return new VerifySpec.Guard(Signals.CLIENT_READ_LATENCY_P99_MICROS, NODE, true, LATENCY_TOLERANCE);
    }

    static VerifySpec.Guard writeLatencyGuard()
    {
        return new VerifySpec.Guard(Signals.CLIENT_WRITE_LATENCY_P99_MICROS, NODE, true, LATENCY_TOLERANCE);
    }

    static VerifySpec spec(SignalId goal, Subject subject, Windows w, VerifySpec.Guard... guards)
    {
        return new VerifySpec(goal, subject, true, 0.10, List.of(guards), w.baselineMillis, w.observationMillis);
    }

    static List<Step> steps(Object... actuatorAndDirection)
    {
        List<Step> out = new ArrayList<>();
        for (int i = 0; i < actuatorAndDirection.length; i += 2)
            out.add(new Step((String) actuatorAndDirection[i], (KnobActuator.Direction) actuatorAndDirection[i + 1]));
        return out;
    }

    static boolean active(KnowledgeView knowledge, String... types)
    {
        for (Diagnosis d : knowledge.activeDiagnoses())
            for (String t : types)
                if (d.type.equals(t))
                    return true;
        return false;
    }
}
