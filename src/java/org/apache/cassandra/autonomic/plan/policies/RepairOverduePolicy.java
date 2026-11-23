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
import org.apache.cassandra.autonomic.analyze.detectors.RepairOverdueDetector;
import org.apache.cassandra.autonomic.execute.actuators.RepairPriorityActuator;
import org.apache.cassandra.autonomic.knowledge.KnowledgeView;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.plan.Action;
import org.apache.cassandra.autonomic.plan.ActionScope;
import org.apache.cassandra.autonomic.plan.Plan;
import org.apache.cassandra.autonomic.plan.Policy;
import org.apache.cassandra.autonomic.plan.VerifySpec;
import org.apache.cassandra.repair.autorepair.AutoRepairConfig.RepairType;
import org.apache.cassandra.utils.TimeUUID;

/**
 * {@code repair_overdue}: put the overdue host first in the auto-repair priority. Does nothing when the diagnosis
 * says auto-repair is disabled in configuration. The action is cluster-scoped, so the governor applies the ledger,
 * rate, blast-radius and approval rules. Its verification is done by the actuator; the spec only names the goal.
 */
public class RepairOverduePolicy implements Policy
{
    public static final String NAME = "repair_overdue";

    private final Function<String, Windows> windows;

    public RepairOverduePolicy(Function<String, Windows> windows)
    {
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
        return Set.of(RepairOverdueDetector.NAME);
    }

    /** The goal is nominal: the cluster actuator verifies its own action (see {@code ClusterActuator}). */
    @Override
    public Set<SignalId> inputs()
    {
        return Set.of(RepairOverdueDetector.OVERDUE_RATIO);
    }

    @Override
    public Set<String> actuations()
    {
        return Set.of(Policy.actuation(RepairPriorityActuator.NAME, Policy.APPLY));
    }

    @Override
    public Optional<Plan> plan(Diagnosis diagnosis, KnowledgeView knowledge)
    {
        if (!Boolean.parseBoolean(diagnosis.evidence.get(RepairOverdueDetector.EVIDENCE_ENABLED)))
            return Optional.empty();
        RepairType type = RepairType.parse(diagnosis.evidence.get(RepairOverdueDetector.EVIDENCE_TYPE));
        Windows w = windows.apply(RepairPriorityActuator.NAME);
        VerifySpec verify = new VerifySpec(RepairOverdueDetector.OVERDUE_RATIO, diagnosis.subject, true, 0.10, List.of(),
                                           w.baselineMillis, w.observationMillis);
        Action action = new Action(TimeUUID.Generator.nextTimeUUID().asUUID(), RepairPriorityActuator.NAME, ActionScope.CLUSTER,
                                   diagnosis.subject, RepairPriorityActuator.params(type, diagnosis.subject.detail), verify);
        return Optional.of(new Plan(TimeUUID.Generator.nextTimeUUID().asUUID(), diagnosis.id, NAME, List.of(action),
                                    diagnosis.summary + "; repair priority for host " + diagnosis.subject.detail));
    }
}
