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
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.autonomic.AutonomicConfig;
import org.apache.cassandra.autonomic.analyze.Diagnosis;
import org.apache.cassandra.autonomic.analyze.Severity;
import org.apache.cassandra.autonomic.coord.Claim;
import org.apache.cassandra.autonomic.coord.ClusterLedger;
import org.apache.cassandra.autonomic.coord.Lane;
import org.apache.cassandra.autonomic.elastic.FakeScaleOutProvider;
import org.apache.cassandra.autonomic.elastic.NoopProvider;
import org.apache.cassandra.autonomic.elastic.ProviderCalls;
import org.apache.cassandra.autonomic.elastic.ScaleOutProvider;
import org.apache.cassandra.autonomic.elastic.ScaleOutProvider.Capabilities;
import org.apache.cassandra.autonomic.execute.ActionRecord;
import org.apache.cassandra.autonomic.execute.ActionState;
import org.apache.cassandra.autonomic.execute.Actuator;
import org.apache.cassandra.autonomic.execute.actuators.ScaleOutActuator;
import org.apache.cassandra.autonomic.knowledge.InMemoryKnowledgeView;
import org.apache.cassandra.autonomic.knowledge.ObservationStore;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.plan.Action;
import org.apache.cassandra.autonomic.plan.ActionScope;
import org.apache.cassandra.autonomic.plan.Plan;
import org.apache.cassandra.autonomic.plan.Policy;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.Location;
import org.apache.cassandra.tcm.membership.NodeId;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** When a scale-out is proposed: by the policy for a disk trend, by the last step of the load spike ladder for saturation. */
public class ScaleOutPolicyTest
{
    private static final long NOW = 1_000_000;
    private static final Subject READ = Subject.threadPool("ReadStage");
    private static final Windows WINDOWS = new Windows(60_000, 120_000);

    private final InMemoryKnowledgeView view = new InMemoryKnowledgeView(new ObservationStore(8, 8), () -> NOW);
    private final List<ActionRecord> records = new ArrayList<>();
    private final FakeScaleOutProvider provider = new FakeScaleOutProvider();
    private ClusterMetadata metadata = new org.apache.cassandra.autonomic.coord.FakeCluster(3).log;
    private ClusterLedger ledger = ClusterLedger.EMPTY;
    private Location location = new Location("dc2", "rack7");
    private ScaleOutActuator actuator;
    private ScaleOutPolicy policy;

    @BeforeClass
    public static void init()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    public ScaleOutPolicyTest()
    {
        view.setActionSource(ms -> records);
        view.setLedgerSource(() -> ledger);
        with(provider);
    }

    private void with(ScaleOutProvider p)
    {
        AutonomicConfig config = new AutonomicConfig();
        actuator = new ScaleOutActuator(p, new ScaleOutActuator.Cluster()
        {
            public ClusterMetadata metadata() { return metadata; }
            public Location location() { return location; }
        }, () -> config.scale_out, () -> view, new ProviderCalls((task, delay) -> () -> {}));
        policy = new ScaleOutPolicy(name -> name.equals(ScaleOutActuator.NAME) ? actuator : null, name -> WINDOWS);
    }

    private static Diagnosis capacity(Subject subject, String trigger)
    {
        return new Diagnosis(UUID.randomUUID(), "capacity_exhausted", subject, Severity.WARN, 1, NOW - 1000, NOW, "capacity is exhausted",
                             Map.of("trigger", trigger), null);
    }

    private static Diagnosis saturated(Subject pool)
    {
        return new Diagnosis(UUID.randomUUID(), "pool_saturation", pool, Severity.CRITICAL, 1, NOW - 60_000, NOW, "", Map.of(), null);
    }

    private void record(ActionState state)
    {
        Action a = new Action(UUID.randomUUID(), ScaleOutActuator.NAME, ActionScope.CLUSTER, Subject.cluster(), Map.of(), null);
        records.add(new ActionRecord(a, state, NOW, null, "scale_out", false, null, Map.of(), Map.of(), Map.of(), null, NOW - 500));
    }

    @Test
    public void aDiskTrendProposesOneNodeInThisNodesRack()
    {
        Diagnosis disk = capacity(Subject.node(), "disk_trend");
        assertTrue(policy.handles().contains("capacity_exhausted"));
        Plan plan = policy.plan(disk, view).get();
        assertEquals(disk.id, plan.diagnosisId);
        assertEquals("scale_out", plan.policy);
        assertEquals(1, plan.actions.size());
        Action a = plan.actions.get(0);
        assertEquals("scale_out", a.actuator);
        assertEquals(ActionScope.CLUSTER, a.scope);
        assertEquals(Subject.cluster(), a.target);
        assertEquals("dc2", a.params.get("datacenter"));
        assertEquals("rack7", a.params.get("rack"));
        assertEquals("capacity is exhausted", a.params.get("reason"));
        assertEquals("disk.used_fraction", a.verify.goal.name);
        assertEquals(Subject.node(), a.verify.goalSubject);
        assertTrue(a.verify.lowerIsBetter);
        assertEquals(60_000, a.verify.baselineWindowMillis);
        assertEquals(1, a.id.version());
        assertEquals("the plan passes the actuator's validation", Optional.empty(), actuator.validate(a, view));
        assertTrue("no provider call to decide", provider.calls.isEmpty());
    }

    @Test
    public void withTheDefaultProviderNothingIsProposedAndTheDiagnosisStaysADiagnosis()
    {
        with(new NoopProvider());
        assertFalse(policy.plan(capacity(Subject.node(), "disk_trend"), view).isPresent());
        view.setActiveDiagnoses(List.of(capacity(READ, "saturation")));
        assertFalse(policy.step().plan(saturated(READ), view).isPresent());
    }

    @Test
    public void nothingIsProposedBeyondWhatTheProviderCanAdd()
    {
        Diagnosis disk = capacity(Subject.node(), "disk_trend");
        provider.capabilities = new Capabilities(true, false, 1, 3, 0);
        with(provider);
        assertFalse("three members, three allowed", policy.plan(disk, view).isPresent());
        provider.capabilities = new Capabilities(true, false, 1, 4, 0);
        with(provider);
        assertTrue(policy.plan(disk, view).isPresent());
        provider.capabilities = new Capabilities(false, true, 1, 100, 0);
        with(provider);
        assertFalse(policy.plan(disk, view).isPresent());
    }

    @Test
    public void oneScaleOutAtATime()
    {
        Diagnosis disk = capacity(Subject.node(), "disk_trend");
        for (ActionState live : List.of(ActionState.PROPOSED, ActionState.APPROVED, ActionState.APPLYING))
        {
            records.clear();
            record(live);
            assertFalse("a " + live + " scale-out", policy.plan(disk, view).isPresent());
        }
        records.clear();
        record(ActionState.COMMITTED);
        assertTrue(policy.plan(disk, view).isPresent());
        // another node's scale-out: the lane is claimed
        Claim claim = new Claim(UUID.randomUUID(), "scale_out", "CLUSTER", new NodeId(2), "JOINING", 3, NOW, NOW + 600_000, ImmutableMap.of());
        ledger = ClusterLedger.EMPTY.withEnabled(true).ledger.withClaim(Lane.TOPOLOGY, claim, NOW, 0, false).ledger;
        assertFalse(policy.plan(disk, view).isPresent());
    }

    @Test
    public void saturationIsAnsweredOnlyThroughTheLadder()
    {
        Diagnosis lead = saturated(READ);
        Diagnosis capacity = capacity(READ, "saturation");
        view.setActiveDiagnoses(List.of(lead, capacity));
        assertFalse("not as a stand-alone policy", policy.plan(capacity, view).isPresent());

        LoadSpikePolicy.Step step = policy.step();
        assertEquals(java.util.Set.of("scale_out"), step.actuators());
        Plan plan = step.plan(lead, view).get();
        assertEquals("the plan answers the lead of the ladder", lead.id, plan.diagnosisId);
        assertEquals("pool.pending_tasks", plan.actions.get(0).verify.goal.name);
        assertEquals(READ, plan.actions.get(0).verify.goalSubject);

        // not without the detector's diagnosis for this pool
        view.setActiveDiagnoses(List.of(lead, capacity(Subject.threadPool("MutationStage"), "saturation"), capacity(Subject.node(), "disk_trend")));
        assertFalse(step.plan(lead, view).isPresent());
    }

    @Test
    public void theLadderReachesScaleOutOnlyWhenItsEarlierStepsHaveNothingLeft()
    {
        boolean[] earlierHasAPlan = { true };
        Action knob = new Action(UUID.randomUUID(), "read_pool", ActionScope.NODE, Subject.node(), Map.of(), null);
        LoadSpikePolicy.Step earlier = new LoadSpikePolicy.Step()
        {
            public String name() { return "resize_request_pools"; }
            public java.util.Set<String> actuators() { return java.util.Set.of("read_pool"); }
            public Optional<Plan> plan(Diagnosis lead, org.apache.cassandra.autonomic.knowledge.KnowledgeView k)
            {
                return earlierHasAPlan[0] ? Optional.of(new Plan(UUID.randomUUID(), lead.id, "p", List.of(knob), "")) : Optional.empty();
            }
        };
        LoadSpikePolicy ladder = new LoadSpikePolicy(List.of(earlier), name -> true, policy.step());
        Diagnosis lead = saturated(READ);
        view.setActiveDiagnoses(List.of(lead, capacity(READ, "saturation")));
        assertEquals("read_pool", ladder.plan(lead, view).get().actions.get(0).actuator);
        assertFalse(ladder.inNodeExhausted(lead, view).isPresent());
        earlierHasAPlan[0] = false;
        assertEquals("scale_out", ladder.plan(lead, view).get().actions.get(0).actuator);
        assertEquals("resize_request_pools: exhausted", ladder.inNodeExhausted(lead, view).get());

        // a scale-out that runs for hours does not hold back the in-node steps; a second scale-out is not proposed
        record(ActionState.APPLYING);
        assertFalse(ladder.plan(lead, view).isPresent());
        earlierHasAPlan[0] = true;
        assertEquals("read_pool", ladder.plan(lead, view).get().actions.get(0).actuator);
    }

    @Test
    public void theBuiltPoliciesContainTheStepAndThePolicy()
    {
        Map<String, Actuator> none = Map.of();
        List<Policy> built = Policies.build(new AutonomicConfig(), none::get);
        assertEquals(1, built.stream().filter(p -> p instanceof ScaleOutPolicy).count());
        assertEquals(1, built.stream().filter(p -> p instanceof LoadSpikePolicy).count());
    }
}
