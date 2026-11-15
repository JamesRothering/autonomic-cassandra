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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import org.junit.Test;

import org.apache.cassandra.autonomic.analyze.Diagnosis;
import org.apache.cassandra.autonomic.analyze.Severity;
import org.apache.cassandra.autonomic.execute.Actuator;
import org.apache.cassandra.autonomic.execute.actuators.FakeHandle;
import org.apache.cassandra.autonomic.execute.actuators.RequestPoolActuator;
import org.apache.cassandra.autonomic.knowledge.InMemoryKnowledgeView;
import org.apache.cassandra.autonomic.knowledge.ObservationStore;
import org.apache.cassandra.autonomic.monitor.Observation;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.monitor.sensors.Signals;
import org.apache.cassandra.autonomic.plan.Action;
import org.apache.cassandra.autonomic.plan.Plan;
import org.apache.cassandra.utils.TimeUUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class RequestPoolPolicyTest
{
    private static final Subject READ_POOL = Subject.threadPool("ReadStage");
    private static final Windows WINDOWS = new Windows(60_000, 120_000);

    private final Map<String, FakeHandle> handles = new HashMap<>();
    private final Map<String, Actuator> actuators = new HashMap<>();
    private final ObservationStore store = new ObservationStore(8, 8);
    private final InMemoryKnowledgeView view = new InMemoryKnowledgeView(store, () -> 1_000_000L);
    private final RequestPoolPolicy policy = new RequestPoolPolicy(actuators::get, name -> WINDOWS);
    private long ids;

    public RequestPoolPolicyTest()
    {
        for (String name : RequestPoolActuator.STAGES.keySet())
        {
            FakeHandle h = new FakeHandle(10);
            handles.put(name, h);
            actuators.put(name, new RequestPoolActuator(name, h, Map.of(), 16));
        }
    }

    private Diagnosis diagnosis(String type, Subject subject, Map<String, String> evidence)
    {
        return new Diagnosis(TimeUUID.atUnixMicrosWithLsbAsUUID(1_000_000L + ids++, 3L), type, subject, Severity.WARN, 1, 0, 0, "", evidence, null);
    }

    private Diagnosis saturated(String pool)
    {
        return diagnosis("pool_saturation", Subject.threadPool(pool), Map.of());
    }

    private Diagnosis drift(String... actuatorNames)
    {
        Map<String, String> evidence = new java.util.LinkedHashMap<>();
        for (String a : actuatorNames)
            evidence.put(a, "12 (configured 10)");
        return diagnosis("autonomic.baseline_drift", Subject.node(), evidence);
    }

    private void load(double perProcessor)
    {
        store.accept(new Observation(Signals.OS_LOAD_PER_PROCESSOR, Subject.node(), 999_000, perProcessor));
    }

    @Test
    public void aSaturatedPoolGrowsOneStepWhileTheCpuIsNotSaturated()
    {
        Diagnosis d = saturated("ReadStage");
        Action a = policy.plan(d, view).get().actions.get(0);
        assertEquals("read_pool", a.actuator);
        assertEquals("12", a.params.get("value"));
        assertEquals(Subject.node(), a.target);
        assertEquals("pool.pending_tasks", a.verify.goal.name);
        assertEquals(READ_POOL, a.verify.goalSubject);
        assertEquals(2, a.verify.guards.size());
        load(0.5);
        assertEquals("12", policy.plan(d, view).get().actions.get(0).params.get("value"));
    }

    @Test
    public void everyRequestStageHasItsOwnActuatorAndOtherPoolsAreIgnored()
    {
        assertEquals("mutation_pool", policy.plan(saturated("MutationStage"), view).get().actions.get(0).actuator);
        assertEquals("counter_mutation_pool", policy.plan(saturated("CounterMutationStage"), view).get().actions.get(0).actuator);
        assertEquals("view_mutation_pool", policy.plan(saturated("ViewMutationStage"), view).get().actions.get(0).actuator);
        assertFalse(policy.plan(saturated("GossipStage"), view).isPresent());
    }

    @Test
    public void nothingIsProposedAtTheEnvelopeLimit()
    {
        handles.get("read_pool").value = 20;
        assertFalse(policy.plan(saturated("ReadStage"), view).isPresent());
    }

    @Test
    public void aSaturatedCpuHoldsTheGrowthAndStepsAnEarlierGrowthBack()
    {
        load(0.95);
        Diagnosis d = saturated("ReadStage");
        assertFalse("at the configured size there is nothing to give back", policy.plan(d, view).isPresent());
        handles.get("read_pool").value = 14;
        Plan plan = policy.plan(d, view).get();
        assertEquals("11", plan.actions.get(0).params.get("value"));
    }

    @Test
    public void aRecoveredPoolReturnsToItsConfiguredSizeThroughTheDriftDiagnosis()
    {
        handles.get("read_pool").value = 14;
        Optional<Plan> plan = policy.plan(drift("read_pool"), view);
        Action a = plan.get().actions.get(0);
        assertEquals("read_pool", a.actuator);
        assertEquals("11", a.params.get("value"));
        assertEquals("the queue must not grow while returning", 0.0, a.verify.minImprovement, 0);
        handles.get("read_pool").value = 11;
        assertEquals("10", policy.plan(drift("read_pool"), view).get().actions.get(0).params.get("value"));
    }

    @Test
    public void driftIsNotUndoneWhileItsPoolIsStillSaturatedOrForOtherKnobs()
    {
        handles.get("read_pool").value = 14;
        view.setActiveDiagnoses(List.of(saturated("ReadStage")));
        assertFalse(policy.plan(drift("read_pool"), view).isPresent());
        view.setActiveDiagnoses(List.of());
        assertFalse(policy.plan(drift("compaction_throughput"), view).isPresent());
        // another pool is taken instead of the saturated one
        handles.get("mutation_pool").value = 14;
        view.setActiveDiagnoses(List.of(saturated("ReadStage")));
        assertEquals("mutation_pool", policy.plan(drift("read_pool", "mutation_pool"), view).get().actions.get(0).actuator);
    }

    @Test
    public void anOperatorsPoolSizeIsLeftAlone() throws Exception
    {
        RequestPoolActuator knob = (RequestPoolActuator) actuators.get("read_pool");
        knob.apply(new Action(TimeUUID.Generator.nextTimeUUID().asUUID(), "read_pool", org.apache.cassandra.autonomic.plan.ActionScope.NODE,
                              Subject.node(), Map.of("value", "12"), null), new org.apache.cassandra.autonomic.execute.ActionContext()
        {
            public long stepDeadlineMillis() { return Long.MAX_VALUE; }
            public long nowMillis() { return 0; }
            public boolean killed() { return false; }
            public void recordUndo(Map<String, String> undo) {}
        });
        handles.get("read_pool").value = 17;
        assertFalse(policy.plan(saturated("ReadStage"), view).isPresent());
        assertFalse(policy.plan(drift("read_pool"), view).isPresent());
        assertTrue(knob.operatorOverride());
    }
}
