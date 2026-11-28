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
import java.util.Comparator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;

import org.apache.cassandra.autonomic.analyze.Diagnosis;
import org.apache.cassandra.autonomic.analyze.Severity;
import org.apache.cassandra.autonomic.analyze.detectors.LatencySloBreachDetector;
import org.apache.cassandra.autonomic.analyze.detectors.PoolSaturationDetector;
import org.apache.cassandra.autonomic.execute.ActionRecord;
import org.apache.cassandra.autonomic.execute.ActionState;
import org.apache.cassandra.autonomic.execute.Actuator;
import org.apache.cassandra.autonomic.execute.actuators.AdvertisedSeverityActuator;
import org.apache.cassandra.autonomic.execute.actuators.RequestPoolActuator;
import org.apache.cassandra.autonomic.execute.actuators.RequestThrottleActuator;
import org.apache.cassandra.autonomic.knowledge.KnowledgeView;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.plan.ActionScope;
import org.apache.cassandra.autonomic.plan.Plan;
import org.apache.cassandra.autonomic.plan.Policy;

/**
 * The fixed response to a load spike (overview 8.1), as policy priority: (1) yield background work, (2) resize the
 * request pools, (3) shed load with the rate limiter, (4) propose scale-out. It replaces the separate policies of
 * those steps for the diagnoses {@code pool_saturation}, {@code latency_slo_breach} and {@code degraded_self}, and
 * reuses them: a {@link Step} wraps each.
 *
 * <ul>
 * <li><b>One at a time.</b> Nothing is proposed while an action of any step is awaiting approval or in flight, and of
 * all open load diagnoses only the first plan is handed out: the most severe, then the oldest, is the lead.</li>
 * <li><b>In order.</b> A step is tried only when every earlier step has nothing left to propose: its knobs are at
 * their envelope limit, locked after a rolled back increase or overridden by an operator, or the step is
 * <em>ineffective</em>: an action of the step was rolled back or failed since the lead diagnosis opened. A step whose
 * actuators are not enabled is skipped as unavailable. Steps that do not apply to the lead's type are skipped.</li>
 * <li><b>Correlator.</b> Policies only receive diagnoses without a root cause. A latency breach caused by a saturated
 * pool is therefore never the lead; the {@code pool_saturation} root is, and the throttle step finds the latency
 * breach among the active diagnoses, where effects are kept, instead of waiting for it.</li>
 * <li><b>Scale-out</b> is the last step, passed to the constructor ({@code ScaleOutPolicy.step}) and reached once
 * the others are exhausted; {@link #NO_SCALE_OUT} proposes nothing. Its action is cluster-scoped and runs for hours,
 * so it does not count for "one at a time": the in-node steps keep answering while a node is being added.</li>
 * </ul>
 */
public class LoadSpikePolicy implements Policy
{
    public static final String NAME = "load_spike";
    /**
     * Causes no policy acts on (ADR-046): naming one never removes an action the layer took before it was named, so a
     * lead whose root cause is of one of these types is treated as having no root cause.
     */
    static final Set<String> DIAGNOSE_ONLY_CAUSES = Set.of("cache_miss_pressure");

    private static boolean isDiagnoseOnly(java.util.UUID rootCause, KnowledgeView knowledge)
    {
        for (Diagnosis d : knowledge.activeDiagnoses())
            if (d.id.equals(rootCause))
                return DIAGNOSE_ONLY_CAUSES.contains(d.type);
        return false;
    }

    static final Set<String> LEADS = Set.of(PoolSaturationDetector.NAME, LatencySloBreachDetector.NAME, "degraded_self");
    static final long LOOKBACK_MILLIS = TimeUnit.HOURS.toMillis(6);

    /** One rung of the ladder. */
    public interface Step
    {
        String name();

        /** Actuators the step may use; its actions are in flight while one of them has a live action. */
        Set<String> actuators();

        /** The next action for the lead, or empty if the step does not apply or is exhausted. Must not look at actuator gates. */
        Optional<Plan> plan(Diagnosis lead, KnowledgeView knowledge);

        /** As {@link Policy#inputs}, of the policy the step wraps. */
        default Set<SignalId> inputs()
        {
            return Set.of();
        }

        /** As {@link Policy#actuations}, of the policy the step wraps. */
        default Set<String> actuations()
        {
            return Set.of();
        }
    }

    /** The extension point of scale-out: no step. */
    public static final Step NO_SCALE_OUT = new Step()
    {
        public String name() { return "scale_out"; }
        public Set<String> actuators() { return Set.of(); }
        public Optional<Plan> plan(Diagnosis lead, KnowledgeView knowledge) { return Optional.empty(); }
    };

    private final List<Step> steps;
    private final Predicate<String> enabled;
    private final Step scaleOut;

    /** @param steps steps one to three, in order; @param scaleOut step four */
    public LoadSpikePolicy(List<Step> steps, Predicate<String> enabled, Step scaleOut)
    {
        this.steps = new ArrayList<>(steps);
        this.enabled = enabled;
        this.scaleOut = scaleOut;
    }

    /** The ladder over the existing policies. */
    public static LoadSpikePolicy standard(Function<String, Actuator> actuators, Function<String, Windows> windows, Predicate<String> enabled, Step scaleOut)
    {
        YieldBackgroundPolicy yield = YieldBackgroundPolicy.background(actuators, windows, enabled);
        YieldBackgroundPolicy advertise = YieldBackgroundPolicy.advertise(actuators, windows, enabled);
        RequestPoolPolicy pools = new RequestPoolPolicy(actuators, windows, RequestPoolPolicy.Mode.SATURATION);
        RequestThrottlePolicy throttle = new RequestThrottlePolicy(actuators, windows, RequestThrottlePolicy.Mode.BREACH);
        List<Step> steps = List.of(
            step("yield_background", Set.copyOf(YieldBackgroundPolicy.yieldActuators()), yield, (lead, k) -> yield.plan(lead, k)),
            // only a node that is slow itself advertises it
            step("advertise_severity", Set.of(AdvertisedSeverityActuator.NAME), advertise,
                 (lead, k) -> lead.type.equals("degraded_self") ? advertise.plan(lead, k) : Optional.empty()),
            step("resize_request_pools", Set.copyOf(RequestPoolActuator.STAGES.keySet()), pools, (lead, k) -> {
                String actuator = RequestPoolPolicy.actuatorOf(String.valueOf(lead.subject.detail));
                return lead.type.equals(PoolSaturationDetector.NAME) && actuator != null && enabled.test(actuator)
                       ? pools.plan(lead, k) : Optional.empty();
            }),
            step("shed_load", Set.of(RequestThrottleActuator.NAME), throttle, (lead, k) -> {
                // the breach may be an effect of the lead: effects are not leads but are still active
                Diagnosis latency = lead.type.equals(LatencySloBreachDetector.NAME) ? lead : nodeBreach(k);
                return latency == null ? Optional.empty() : throttle.breach(latency, k);
            }));
        return new LoadSpikePolicy(steps, enabled, scaleOut == null ? NO_SCALE_OUT : scaleOut);
    }

    private static Step step(String name, Set<String> actuators, Policy wrapped, java.util.function.BiFunction<Diagnosis, KnowledgeView, Optional<Plan>> plan)
    {
        return new Step()
        {
            public String name() { return name; }
            public Set<String> actuators() { return actuators; }
            public Optional<Plan> plan(Diagnosis lead, KnowledgeView knowledge) { return plan.apply(lead, knowledge); }
            public Set<SignalId> inputs() { return wrapped.inputs(); }
            public Set<String> actuations() { return wrapped.actuations(); }
        };
    }

    private static Diagnosis nodeBreach(KnowledgeView knowledge)
    {
        for (Diagnosis d : knowledge.activeDiagnoses())
            if (d.type.equals(LatencySloBreachDetector.NAME) && d.subject.kind == Subject.Kind.NODE && d.severity == Severity.CRITICAL)
                return d;
        return null;
    }

    @Override
    public String name()
    {
        return NAME;
    }

    @Override
    public Set<String> handles()
    {
        return LEADS;
    }

    /** Those of its steps. */
    @Override
    public Set<SignalId> inputs()
    {
        Set<SignalId> out = new LinkedHashSet<>();
        for (Step s : all())
            out.addAll(s.inputs());
        return out;
    }

    /** Those of its steps. */
    @Override
    public Set<String> actuations()
    {
        Set<String> out = new LinkedHashSet<>();
        for (Step s : all())
            out.addAll(s.actuations());
        return out;
    }

    private List<Step> all()
    {
        List<Step> all = new ArrayList<>(steps);
        all.add(scaleOut);
        return all;
    }

    /** Most severe first, then the one seen first, then by id. */
    private static final Comparator<Diagnosis> LEAD_ORDER = Comparator
        .<Diagnosis, Severity>comparing(d -> d.severity, Comparator.reverseOrder())
        .thenComparingLong(d -> d.firstSeenMillis)
        .thenComparing(d -> d.id);

    @Override
    public Optional<Plan> plan(Diagnosis diagnosis, KnowledgeView knowledge)
    {
        List<Step> all = all();
        Set<String> actuators = new LinkedHashSet<>();
        for (Step s : all)
            actuators.addAll(s.actuators());
        if (inFlight(actuators, knowledge))
            return Optional.empty();
        List<Diagnosis> leads = new ArrayList<>();
        for (Diagnosis d : knowledge.activeDiagnoses())
            if (LEADS.contains(d.type) && (d.rootCause == null || isDiagnoseOnly(d.rootCause, knowledge)))
                leads.add(d);
        leads.sort(LEAD_ORDER);
        for (Diagnosis lead : leads)
        {
            Optional<Plan> plan = ladder(lead, all, knowledge);
            if (plan.isPresent())
                return lead.id.equals(diagnosis.id) ? plan : Optional.empty();
        }
        return Optional.empty();
    }

    /**
     * One at a time, among the actions in the node. A scale-out takes hours and does not hold back the in-node
     * steps; the scale-out step itself proposes only one at a time.
     */
    private static boolean inFlight(Set<String> actuators, KnowledgeView knowledge)
    {
        for (ActionRecord r : knowledge.recentActions(LOOKBACK_MILLIS))
            if (actuators.contains(r.action.actuator) && !r.state.isTerminal() && r.action.scope == ActionScope.NODE)
                return true;
        return false;
    }

    private boolean unavailable(Step step)
    {
        return !step.actuators().isEmpty() && step.actuators().stream().noneMatch(enabled);
    }

    /**
     * For {@code capacity_exhausted}: the state of every in-node step when none of them has anything left to propose
     * for the lead, for example {@code yield_background: exhausted, shed_load: ineffective}. Empty while a step
     * still has a proposal or an in-node action is in flight, and when no in-node step is enabled at all: nothing
     * was tried then, so nothing is exhausted.
     */
    public Optional<String> inNodeExhausted(Diagnosis lead, KnowledgeView knowledge)
    {
        Set<String> actuators = new LinkedHashSet<>();
        for (Step s : steps)
            actuators.addAll(s.actuators());
        if (inFlight(actuators, knowledge))
            return Optional.empty();
        List<String> states = new ArrayList<>();
        boolean tried = false;
        for (Step step : steps)
        {
            if (unavailable(step))
            {
                states.add(step.name() + ": not enabled");
                continue;
            }
            tried = true;
            if (ineffective(step, lead, knowledge))
                states.add(step.name() + ": ineffective");
            else if (step.plan(lead, knowledge).isPresent())
                return Optional.empty();
            else
                states.add(step.name() + ": exhausted");
        }
        return tried ? Optional.of(String.join(", ", states)) : Optional.empty();
    }

    /** The first step with something to propose for the lead. */
    private Optional<Plan> ladder(Diagnosis lead, List<Step> all, KnowledgeView knowledge)
    {
        for (Step step : all)
        {
            if (unavailable(step))
                continue;
            if (ineffective(step, lead, knowledge))
                continue;
            Optional<Plan> plan = step.plan(lead, knowledge);
            if (plan.isPresent())
                return plan;
        }
        return Optional.empty();
    }

    /** An action of the step was rolled back or failed since the lead opened. */
    private static boolean ineffective(Step step, Diagnosis lead, KnowledgeView knowledge)
    {
        for (ActionRecord r : knowledge.recentActions(LOOKBACK_MILLIS))
            if (step.actuators().contains(r.action.actuator) && (r.state == ActionState.ROLLED_BACK || r.state == ActionState.FAILED)
                && !r.dryRun && r.proposedMillis >= lead.firstSeenMillis)
                return true;
        return false;
    }
}
