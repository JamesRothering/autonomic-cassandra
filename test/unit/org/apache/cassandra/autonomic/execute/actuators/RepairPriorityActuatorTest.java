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
package org.apache.cassandra.autonomic.execute.actuators;

import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;

import org.junit.Test;

import org.apache.cassandra.autonomic.coord.Claim;
import org.apache.cassandra.autonomic.coord.Lane;
import org.apache.cassandra.autonomic.execute.ClusterActionExecution;
import org.apache.cassandra.autonomic.execute.StepResult;
import org.apache.cassandra.autonomic.execute.Verdict;
import org.apache.cassandra.autonomic.knowledge.InMemoryKnowledgeView;
import org.apache.cassandra.autonomic.knowledge.ObservationStore;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.plan.Action;
import org.apache.cassandra.autonomic.plan.ActionScope;
import org.apache.cassandra.autonomic.plan.VerifySpec;
import org.apache.cassandra.repair.autorepair.AutoRepairConfig.RepairType;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.utils.TimeUUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RepairPriorityActuatorTest
{
    private static final UUID HOST = UUID.fromString("00000000-0000-0000-0000-00000000000a");

    private boolean enabled = true;
    private boolean known = true;
    private final Set<UUID> priority = new HashSet<>();
    private final RepairPriorityActuator actuator = new RepairPriorityActuator(new RepairPriorityActuator.Scheduler()
    {
        public boolean enabled(RepairType type)
        {
            return enabled;
        }

        public Optional<String> address(UUID hostId)
        {
            return known ? Optional.of("127.0.0.1:7012") : Optional.empty();
        }

        public void prioritise(RepairType type, String address)
        {
            priority.add(HOST);
        }

        public boolean prioritised(RepairType type, UUID hostId)
        {
            return priority.contains(hostId);
        }
    });
    private final InMemoryKnowledgeView view = new InMemoryKnowledgeView(new ObservationStore(8, 8), () -> 1L);

    private static Action action(String host)
    {
        VerifySpec spec = new VerifySpec(SignalId.of("repair.overdue_ratio"), Subject.peer(host), true, 0.1, List.of(), 0, 0);
        return new Action(TimeUUID.Generator.nextTimeUUID().asUUID(), RepairPriorityActuator.NAME, ActionScope.CLUSTER, Subject.peer(host),
                          RepairPriorityActuator.params(RepairType.FULL, host), spec);
    }

    private static Claim claim(Action a, String phase)
    {
        return new Claim(a.id, a.actuator, a.target.asString(), new NodeId(1), phase, 0, 0, 1, ImmutableMap.of());
    }

    @Test
    public void isAMaintenanceLaneClusterActuatorThatIsIrreversible()
    {
        assertEquals(Lane.MAINTENANCE, actuator.lane());
        assertEquals(ActionScope.CLUSTER, actuator.scope());
        assertFalse(actuator.reversible());
    }

    @Test
    public void validateRequiresEnabledAutoRepairAndAKnownHost()
    {
        Action a = action(HOST.toString());
        assertFalse(actuator.validate(a, view).isPresent());
        enabled = false;
        assertTrue(actuator.validate(a, view).get().contains("disabled"));
        enabled = true;
        known = false;
        assertTrue(actuator.validate(a, view).get().contains("directory"));
        known = true;
        assertTrue(actuator.validate(action("not-a-uuid"), view).isPresent());
    }

    @Test
    public void setsThePriorityThenVerifiesItAndCommits()
    {
        Action a = action(HOST.toString());
        StepResult first = actuator.step(a, claim(a, ClusterActionExecution.INITIAL_PHASE), null);
        assertEquals(StepResult.Kind.ADVANCE, first.kind);
        assertEquals(RepairPriorityActuator.PHASE_SET, first.phase);
        assertTrue(priority.contains(HOST));
        StepResult second = actuator.step(a, claim(a, RepairPriorityActuator.PHASE_SET), null);
        assertEquals(StepResult.Kind.COMMITTED, second.kind);
        assertEquals(Verdict.IMPROVED, second.verdict);
    }

    @Test
    public void aPriorityThatIsGoneIsCommittedInconclusive()
    {
        Action a = action(HOST.toString());
        StepResult result = actuator.step(a, claim(a, RepairPriorityActuator.PHASE_SET), null);
        assertEquals(StepResult.Kind.COMMITTED, result.kind);
        assertEquals(Verdict.INCONCLUSIVE, result.verdict);
    }

    @Test
    public void nothingIsWrittenWhenAutoRepairWasDisabledAfterTheProposal()
    {
        enabled = false;
        Action a = action(HOST.toString());
        assertEquals(StepResult.Kind.ROLLED_BACK, actuator.step(a, claim(a, ClusterActionExecution.INITIAL_PHASE), null).kind);
        assertTrue(priority.isEmpty());
    }

    @Test
    public void recoveryAndKillRespectTheCommitPoint()
    {
        Action a = action(HOST.toString());
        assertFalse(actuator.pastCommitPoint(claim(a, ClusterActionExecution.INITIAL_PHASE)));
        assertTrue(actuator.pastCommitPoint(claim(a, RepairPriorityActuator.PHASE_SET)));
        assertEquals(StepResult.Kind.ROLLED_BACK, actuator.recover(claim(a, ClusterActionExecution.INITIAL_PHASE), null).kind);
        assertEquals(StepResult.Kind.COMMITTED_CLEANUP_PENDING, actuator.recover(claim(a, RepairPriorityActuator.PHASE_SET), null).kind);
    }
}
