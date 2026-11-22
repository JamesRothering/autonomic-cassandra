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
import org.apache.cassandra.autonomic.analyze.ThresholdDetector;
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
    private final RequestPoolPolicy saturation = new RequestPoolPolicy(actuators::get, name -> WINDOWS, RequestPoolPolicy.Mode.SATURATION);
    private final RequestPoolPolicy recovery = new RequestPoolPolicy(actuators::get, name -> WINDOWS, RequestPoolPolicy.Mode.RECOVERY);
    private long ids;

    /** Samples of the goal and both guards of a pool action, {@code count} of them ending 1 s before now. */
    private void baselineSamples(String pool, int count, long fromMillis)
    {
        for (int i = 0; i < count; i++)
        {
            long at = fromMillis + i * 1_000;
            store.accept(new Observation(Signals.POOL_PENDING_TASKS, Subject.threadPool(pool), at, 40));
            store.accept(new Observation(Signals.CLIENT_READ_LATENCY_P99_MICROS, Subject.node(), at, 1000));
            store.accept(new Observation(Signals.CLIENT_WRITE_LATENCY_P99_MICROS, Subject.node(), at, 1000));
        }
    }

    public RequestPoolPolicyTest()
    {
        // enough samples in the baseline window (1_000_000 - 60_000) for every pool
        for (String pool : RequestPoolActuator.STAGES.values())
            baselineSamples(pool, 5, 990_000);
        for (String name : RequestPoolActuator.STAGES.keySet())
        {
            FakeHandle h = new FakeHandle(10);
            handles.put(name, h);
            actuators.put(name, new RequestPoolActuator(name, h, Map.of(), 16));
        }
    }

    private Diagnosis diagnosis(String type, Subject subject, Map<String, String> evidence)
    {
        return diagnosis(type, subject, evidence, 0);
    }

    private Diagnosis diagnosis(String type, Subject subject, Map<String, String> evidence, long firstSeenMillis)
    {
        return new Diagnosis(TimeUUID.atUnixMicrosWithLsbAsUUID(1_000_000L + ids++, 3L), type, subject, Severity.WARN, 1, firstSeenMillis, 0, "", evidence, null);
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
        Action a = saturation.plan(d, view).get().actions.get(0);
        assertEquals("read_pool", a.actuator);
        assertEquals("12", a.params.get("value"));
        assertEquals(Subject.node(), a.target);
        assertEquals("pool.pending_tasks", a.verify.goal.name);
        assertEquals(READ_POOL, a.verify.goalSubject);
        assertEquals(2, a.verify.guards.size());
        assertEquals("client.read_ops_per_sec", a.verify.guards.get(0).trafficSignal.name);
        assertEquals("client.write_ops_per_sec", a.verify.guards.get(1).trafficSignal.name);
        load(0.5);
        assertEquals("12", saturation.plan(d, view).get().actions.get(0).params.get("value"));
    }

    @Test
    public void theBaselineStartsAtTheFirstBreachingCheckAndTheProposalWaitsForSamples()
    {
        ObservationStore fresh = new ObservationStore(8, 8);
        InMemoryKnowledgeView freshView = new InMemoryKnowledgeView(fresh, () -> 1_000_000L);
        Diagnosis d = diagnosis("pool_saturation", READ_POOL, Map.of(ThresholdDetector.BREACH_SINCE, "970000"), 985_000);
        assertEquals(970_000, Policies.problemSinceMillis(d));
        assertFalse("no samples of the problem period yet", saturation.plan(d, freshView).isPresent());

        // two samples since the problem began: still too few
        for (int i = 0; i < 2; i++)
        {
            long at = 975_000 + i * 1_000;
            fresh.accept(new Observation(Signals.POOL_PENDING_TASKS, READ_POOL, at, 40));
            fresh.accept(new Observation(Signals.CLIENT_READ_LATENCY_P99_MICROS, Subject.node(), at, 1000));
            fresh.accept(new Observation(Signals.CLIENT_WRITE_LATENCY_P99_MICROS, Subject.node(), at, 1000));
        }
        assertFalse(saturation.plan(d, freshView).isPresent());

        // a third one, and the proposal is made with the problem's start in its spec
        fresh.accept(new Observation(Signals.POOL_PENDING_TASKS, READ_POOL, 977_000, 40));
        fresh.accept(new Observation(Signals.CLIENT_READ_LATENCY_P99_MICROS, Subject.node(), 977_000, 1000));
        fresh.accept(new Observation(Signals.CLIENT_WRITE_LATENCY_P99_MICROS, Subject.node(), 977_000, 1000));
        Action a = saturation.plan(d, freshView).get().actions.get(0);
        assertEquals(970_000, a.verify.baselineSinceMillis);
    }

    @Test
    public void withoutBreachSinceTheDiagnosisOpeningIsTheStart()
    {
        Diagnosis d = diagnosis("pool_saturation", READ_POOL, Map.of(), 987_000);
        assertEquals(987_000, Policies.problemSinceMillis(d));
        assertEquals(987_000, saturation.plan(d, view).get().actions.get(0).verify.baselineSinceMillis);
        assertEquals(5, Policies.problemSinceMillis(diagnosis("x", READ_POOL, Map.of(ThresholdDetector.BREACH_SINCE, "garbage"), 5)));
    }

    @Test
    public void recoveryProposalsAlsoWaitForTheBaseline()
    {
        handles.get("read_pool").value = 14;
        Diagnosis d = drift("read_pool");
        assertEquals(0, recovery.plan(d, view).get().actions.get(0).verify.baselineSinceMillis);
        InMemoryKnowledgeView empty = new InMemoryKnowledgeView(new ObservationStore(8, 8), () -> 1_000_000L);
        assertFalse(recovery.plan(d, empty).isPresent());
    }

    @Test
    public void everyRequestStageHasItsOwnActuatorAndOtherPoolsAreIgnored()
    {
        assertEquals("mutation_pool", saturation.plan(saturated("MutationStage"), view).get().actions.get(0).actuator);
        assertEquals("counter_mutation_pool", saturation.plan(saturated("CounterMutationStage"), view).get().actions.get(0).actuator);
        assertEquals("view_mutation_pool", saturation.plan(saturated("ViewMutationStage"), view).get().actions.get(0).actuator);
        assertFalse(saturation.plan(saturated("GossipStage"), view).isPresent());
    }

    @Test
    public void nothingIsProposedAtTheEnvelopeLimit()
    {
        handles.get("read_pool").value = 20;
        assertFalse(saturation.plan(saturated("ReadStage"), view).isPresent());
    }

    @Test
    public void aSaturatedCpuHoldsTheGrowthAndStepsAnEarlierGrowthBack()
    {
        load(0.95);
        Diagnosis d = saturated("ReadStage");
        assertFalse("at the configured size there is nothing to give back", saturation.plan(d, view).isPresent());
        handles.get("read_pool").value = 14;
        Plan plan = saturation.plan(d, view).get();
        assertEquals("11", plan.actions.get(0).params.get("value"));
    }

    @Test
    public void aRecoveredPoolReturnsToItsConfiguredSizeThroughTheDriftDiagnosis()
    {
        handles.get("read_pool").value = 14;
        Optional<Plan> plan = recovery.plan(drift("read_pool"), view);
        Action a = plan.get().actions.get(0);
        assertEquals("read_pool", a.actuator);
        assertEquals("11", a.params.get("value"));
        assertEquals("the queue must not grow while returning", 0.0, a.verify.minImprovement, 0);
        handles.get("read_pool").value = 11;
        assertEquals("10", recovery.plan(drift("read_pool"), view).get().actions.get(0).params.get("value"));
    }

    @Test
    public void driftIsNotUndoneWhileItsPoolIsStillSaturatedOrForOtherKnobs()
    {
        handles.get("read_pool").value = 14;
        view.setActiveDiagnoses(List.of(saturated("ReadStage")));
        assertFalse(recovery.plan(drift("read_pool"), view).isPresent());
        view.setActiveDiagnoses(List.of());
        assertFalse(recovery.plan(drift("compaction_throughput"), view).isPresent());
        // another pool is taken instead of the saturated one
        handles.get("mutation_pool").value = 14;
        view.setActiveDiagnoses(List.of(saturated("ReadStage")));
        assertEquals("mutation_pool", recovery.plan(drift("read_pool", "mutation_pool"), view).get().actions.get(0).actuator);
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
        assertFalse(saturation.plan(saturated("ReadStage"), view).isPresent());
        assertFalse(recovery.plan(drift("read_pool"), view).isPresent());
        assertTrue(knob.operatorOverride());
    }
}
