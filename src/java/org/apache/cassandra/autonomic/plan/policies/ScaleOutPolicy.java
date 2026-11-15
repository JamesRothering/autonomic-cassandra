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
import java.util.UUID;
import java.util.function.Function;

import org.apache.cassandra.autonomic.analyze.Diagnosis;
import org.apache.cassandra.autonomic.analyze.detectors.CapacityExhaustedDetector;
import org.apache.cassandra.autonomic.coord.Lane;
import org.apache.cassandra.autonomic.execute.ActionRecord;
import org.apache.cassandra.autonomic.execute.Actuator;
import org.apache.cassandra.autonomic.execute.actuators.ScaleOutActuator;
import org.apache.cassandra.autonomic.knowledge.KnowledgeView;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.monitor.sensors.Signals;
import org.apache.cassandra.autonomic.plan.Action;
import org.apache.cassandra.autonomic.plan.ActionScope;
import org.apache.cassandra.autonomic.plan.Plan;
import org.apache.cassandra.autonomic.plan.Policy;
import org.apache.cassandra.autonomic.plan.VerifySpec;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.utils.TimeUUID;

/**
 * {@code capacity_exhausted}: propose one more node in this node's datacenter and rack (overview 8.3). Two ways in:
 * <ul>
 * <li>as a policy, for the diagnosis triggered by the disk trend, which no in-node step answers;</li>
 * <li>as {@link #step}, the last step of the load spike ladder, for a saturated pool: reached only when the earlier
 * steps have nothing left, and proposing only while the detector's {@code saturation} diagnosis of that pool is
 * open.</li>
 * </ul>
 * Nothing is proposed while the provider cannot add a node (the default {@code NoopProvider}, or the cluster at the
 * provider's {@code maxNodes}): the diagnosis stays visible and that is all. Nor while a scale-out is proposed or
 * running here, or the TOPOLOGY lane is claimed. No provider call: the capabilities were read when the layer started.
 * The action is cluster-scoped, so the governor applies the ledger, rate and approval rules (manual by default).
 */
public class ScaleOutPolicy implements Policy
{
    public static final String NAME = "scale_out";
    static final long LOOKBACK_MILLIS = LoadSpikePolicy.LOOKBACK_MILLIS;

    private final Function<String, Actuator> actuators;
    private final Function<String, Windows> windows;

    public ScaleOutPolicy(Function<String, Actuator> actuators, Function<String, Windows> windows)
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
        return Set.of(CapacityExhaustedDetector.NAME);
    }

    @Override
    public Optional<Plan> plan(Diagnosis diagnosis, KnowledgeView knowledge)
    {
        if (!CapacityExhaustedDetector.TRIGGER_DISK.equals(diagnosis.evidence.get(CapacityExhaustedDetector.EVIDENCE_TRIGGER)))
            return Optional.empty();
        return propose(diagnosis, diagnosis.id, Signals.DISK_USED_FRACTION, Subject.node(), knowledge);
    }

    /** The scale-out step of the load spike ladder. */
    public LoadSpikePolicy.Step step()
    {
        return new LoadSpikePolicy.Step()
        {
            public String name()
            {
                return NAME;
            }

            public Set<String> actuators()
            {
                return Set.of(ScaleOutActuator.NAME);
            }

            public Optional<Plan> plan(Diagnosis lead, KnowledgeView knowledge)
            {
                for (Diagnosis d : knowledge.activeDiagnoses())
                    if (d.type.equals(CapacityExhaustedDetector.NAME) && d.subject.equals(lead.subject)
                        && CapacityExhaustedDetector.TRIGGER_SATURATION.equals(d.evidence.get(CapacityExhaustedDetector.EVIDENCE_TRIGGER)))
                        return propose(d, lead.id, Signals.POOL_PENDING_TASKS, lead.subject, knowledge);
                return Optional.empty();
            }
        };
    }

    /** @param diagnosisId the diagnosis the plan answers: the lead of the ladder, or the capacity diagnosis itself */
    private Optional<Plan> propose(Diagnosis capacity, UUID diagnosisId, SignalId goal, Subject goalSubject, KnowledgeView knowledge)
    {
        Actuator actuator = actuators.apply(ScaleOutActuator.NAME);
        if (!(actuator instanceof ScaleOutActuator))
            return Optional.empty();
        ScaleOutActuator scaleOut = (ScaleOutActuator) actuator;
        if (scaleOut.unavailable().isPresent())
            return Optional.empty();
        Location location = scaleOut.location();
        if (location == null)
            return Optional.empty();
        // one at a time: proposed or running here, or claimed by any node
        for (ActionRecord r : knowledge.recentActions(LOOKBACK_MILLIS))
            if (r.action.actuator.equals(ScaleOutActuator.NAME) && !r.state.isTerminal())
                return Optional.empty();
        if (knowledge.ledger().claims.containsKey(Lane.TOPOLOGY))
            return Optional.empty();
        Windows w = windows.apply(ScaleOutActuator.NAME);
        // verified by the actuator after its own window; the spec names the goal and the baseline window
        VerifySpec verify = new VerifySpec(goal, goalSubject, true, ScaleOutActuator.MIN_IMPROVEMENT, List.of(), w.baselineMillis, w.observationMillis);
        String reason = capacity.summary;
        Action action = new Action(TimeUUID.Generator.nextTimeUUID().asUUID(), ScaleOutActuator.NAME, ActionScope.CLUSTER, Subject.cluster(),
                                   ScaleOutActuator.params(location.datacenter, location.rack, reason), verify);
        return Optional.of(new Plan(TimeUUID.Generator.nextTimeUUID().asUUID(), diagnosisId, NAME, List.of(action),
                                    reason + "; add one node in " + location.datacenter + '/' + location.rack
                                    + " through provider " + scaleOut.provider().name()));
    }
}
