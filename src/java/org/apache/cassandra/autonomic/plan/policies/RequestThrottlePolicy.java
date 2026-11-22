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
import java.util.Set;
import java.util.function.Function;

import org.apache.cassandra.autonomic.analyze.Diagnosis;
import org.apache.cassandra.autonomic.analyze.Severity;
import org.apache.cassandra.autonomic.analyze.detectors.BaselineDriftDetector;
import org.apache.cassandra.autonomic.analyze.detectors.LatencySloBreachDetector;
import org.apache.cassandra.autonomic.execute.Actuator;
import org.apache.cassandra.autonomic.execute.actuators.RequestThrottleActuator;
import org.apache.cassandra.autonomic.knowledge.KnowledgeView;
import org.apache.cassandra.autonomic.knowledge.Series;
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
 * Adaptive throttling of the native transport (overview 8.1), AIMD over {@link RequestThrottleActuator}.
 * <ul>
 * <li>Shed load: a node-wide {@code latency_slo_breach} at CRITICAL while a queue is growing. The limiter is engaged at
 * the observed request rate less one step, and lowered by a step per action, never below the floor. Goal: lower
 * coordinator p99 of the worse operation; guard: the other.</li>
 * <li>Recovery ({@code autonomic.baseline_drift} naming this actuator, no latency breach open): additive increase,
 * and once the limit no longer binds the limiter is released. The latency must not get worse.</li>
 * </ul>
 * Queue growth is derived from the pending tasks of the thread pools: no sensor emits a queue growth signal, so a
 * pool whose pending tasks over the last two minutes are higher in the second half than in the first counts.
 * {@link #breach} is public so that a response ladder can apply it to a latency diagnosis that the correlator
 * attributed to a saturated pool.
 */
public class RequestThrottlePolicy implements Policy
{
    public static final String NAME = "request_throttle";
    public static final double RELEASE_FACTOR = 1.2;
    static final long QUEUE_LOOKBACK_MILLIS = 120_000;
    static final double GROWTH = 1.1;

    /** Which diagnoses the instance answers. */
    public enum Mode
    {
        BREACH, RECOVERY
    }

    private final Function<String, Actuator> actuators;
    private final Function<String, Windows> windows;
    private final Mode mode;

    public RequestThrottlePolicy(Function<String, Actuator> actuators, Function<String, Windows> windows, Mode mode)
    {
        this.actuators = actuators;
        this.windows = windows;
        this.mode = mode;
    }

    @Override
    public String name()
    {
        return mode == Mode.BREACH ? NAME : NAME + "_recovery";
    }

    @Override
    public Set<String> handles()
    {
        return Set.of(mode == Mode.BREACH ? LatencySloBreachDetector.NAME : BaselineDriftDetector.TYPE);
    }

    @Override
    public Optional<Plan> plan(Diagnosis diagnosis, KnowledgeView knowledge)
    {
        return mode == Mode.BREACH ? breach(diagnosis, knowledge) : recovery(diagnosis, knowledge);
    }

    private RequestThrottleActuator knob()
    {
        Actuator a = actuators.apply(RequestThrottleActuator.NAME);
        return a instanceof RequestThrottleActuator ? (RequestThrottleActuator) a : null;
    }

    static double observedRate(KnowledgeView knowledge)
    {
        double total = 0;
        boolean any = false;
        for (SignalId id : List.of(Signals.CLIENT_READ_OPS_PER_SEC, Signals.CLIENT_WRITE_OPS_PER_SEC,
                                   Signals.CLIENT_CAS_READ_OPS_PER_SEC, Signals.CLIENT_CAS_WRITE_OPS_PER_SEC))
        {
            java.util.OptionalDouble v = knowledge.latest(id, Subject.node());
            if (v.isPresent())
            {
                total += v.getAsDouble();
                any = true;
            }
        }
        return any ? total : -1;
    }

    /** True if some pool's queue is longer in the second half of the lookback than in the first. */
    static boolean queueGrowing(KnowledgeView knowledge)
    {
        for (Subject pool : knowledge.subjects(Signals.POOL_PENDING_TASKS))
        {
            Series s = knowledge.series(Signals.POOL_PENDING_TASKS, pool, QUEUE_LOOKBACK_MILLIS);
            if (s.size() < 4)
                continue;
            int half = s.size() / 2;
            double first = 0;
            double second = 0;
            for (int i = 0; i < half; i++)
                first += s.values[i];
            for (int i = s.size() - half; i < s.size(); i++)
                second += s.values[i];
            if (second > first * GROWTH && second > 0)
                return true;
        }
        return false;
    }

    /** Shed load in answer to a node-wide latency breach; the diagnosis may be an effect another policy sees as such. */
    public Optional<Plan> breach(Diagnosis diagnosis, KnowledgeView knowledge)
    {
        RequestThrottleActuator knob = knob();
        if (knob == null || diagnosis.subject.kind != Subject.Kind.NODE || diagnosis.severity != Severity.CRITICAL
            || !queueGrowing(knowledge))
            return Optional.empty();
        String id = diagnosis.id.toString();
        Optional<Double> target = knob.engaged() ? knob.decrease(id) : knob.engageAt(observedRate(knowledge));
        Windows w = windows.apply(RequestThrottleActuator.NAME);
        boolean write = YieldBackgroundPolicy.writeWorse(diagnosis);
        VerifySpec spec = new VerifySpec(write ? Signals.CLIENT_WRITE_LATENCY_P99_MICROS : Signals.CLIENT_READ_LATENCY_P99_MICROS, Subject.node(),
                                         true, 0.10, List.of(write ? KnobPolicy.readLatencyGuard() : KnobPolicy.writeLatencyGuard()),
                                         w.baselineMillis, w.observationMillis);
        return target.flatMap(t -> plan(diagnosis, knowledge, knob, t, spec, "shed load: request limit " + RequestThrottleActuator.format(knob.current()) + " -> " + RequestThrottleActuator.format(t)));
    }

    private Optional<Plan> recovery(Diagnosis diagnosis, KnowledgeView knowledge)
    {
        RequestThrottleActuator knob = knob();
        if (knob == null || !diagnosis.evidence.containsKey(RequestThrottleActuator.NAME) || !knob.engaged()
            || KnobPolicy.active(knowledge, LatencySloBreachDetector.NAME))
            return Optional.empty();
        Optional<Double> target = knob.increase(observedRate(knowledge), RELEASE_FACTOR, diagnosis.id.toString());
        Windows w = windows.apply(RequestThrottleActuator.NAME);
        VerifySpec spec = new VerifySpec(Signals.CLIENT_READ_LATENCY_P99_MICROS, Subject.node(), true, 0.0,
                                         List.of(KnobPolicy.writeLatencyGuard()), w.baselineMillis, w.observationMillis);
        return target.flatMap(t -> plan(diagnosis, knowledge, knob, t, spec, "recovery: request limit " + RequestThrottleActuator.format(knob.current())
                                                              + (t >= knob.ceiling() ? " released" : " -> " + RequestThrottleActuator.format(t))));
    }

    private Optional<Plan> plan(Diagnosis diagnosis, KnowledgeView knowledge, RequestThrottleActuator knob, double target, VerifySpec spec, String rationale)
    {
        Optional<VerifySpec> verify = Policies.forProblem(spec, diagnosis, knowledge, name());
        if (!verify.isPresent())
            return Optional.empty();
        Action action = new Action(TimeUUID.Generator.nextTimeUUID().asUUID(), knob.name(), ActionScope.NODE, Subject.node(),
                                   RequestThrottleActuator.params(target, diagnosis.id.toString()), verify.get());
        return Optional.of(new Plan(TimeUUID.Generator.nextTimeUUID().asUUID(), diagnosis.id, name(), List.of(action), rationale));
    }
}
