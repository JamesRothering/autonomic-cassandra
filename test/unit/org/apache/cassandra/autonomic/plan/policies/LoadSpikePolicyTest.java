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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.autonomic.analyze.Diagnosis;
import org.apache.cassandra.autonomic.analyze.Severity;
import org.apache.cassandra.autonomic.execute.ActionRecord;
import org.apache.cassandra.autonomic.execute.ActionState;
import org.apache.cassandra.autonomic.execute.Actuator;
import org.apache.cassandra.autonomic.execute.actuators.AdvertisedSeverityActuator;
import org.apache.cassandra.autonomic.execute.actuators.CompactionThroughputActuator;
import org.apache.cassandra.autonomic.execute.actuators.FakeHandle;
import org.apache.cassandra.autonomic.execute.actuators.HintThrottleActuator;
import org.apache.cassandra.autonomic.execute.actuators.RequestPoolActuator;
import org.apache.cassandra.autonomic.execute.actuators.RequestThrottleActuator;
import org.apache.cassandra.autonomic.execute.actuators.StreamThroughputActuator;
import org.apache.cassandra.autonomic.knowledge.InMemoryKnowledgeView;
import org.apache.cassandra.autonomic.knowledge.ObservationStore;
import org.apache.cassandra.autonomic.monitor.Observation;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.monitor.sensors.Signals;
import org.apache.cassandra.autonomic.plan.Action;
import org.apache.cassandra.autonomic.plan.ActionScope;
import org.apache.cassandra.autonomic.plan.Plan;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.TimeUUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * The load spike ladder of overview 8.1: yield background work, resize pools, shed load, scale out; one action at a
 * time and the next step only when the previous is exhausted or ineffective.
 */
public class LoadSpikePolicyTest
{
    private static final long NOW = 1_000_000;
    private static final Subject NODE = Subject.node();
    private static final Windows WINDOWS = new Windows(60_000, 120_000);

    private final Map<String, FakeHandle> handles = new HashMap<>();
    private final Map<String, Actuator> actuators = new HashMap<>();
    private final Set<String> disabled = new HashSet<>();
    private final List<ActionRecord> records = new ArrayList<>();
    private final ObservationStore store = new ObservationStore(32, 32);
    private final InMemoryKnowledgeView view = new InMemoryKnowledgeView(store, () -> NOW);
    private boolean limiterOn;
    private int limit = 1_000_000;
    private LoadSpikePolicy ladder;
    private long ids;

    @BeforeClass
    public static void setupClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    public LoadSpikePolicyTest()
    {
        knob(CompactionThroughputActuator.NAME, 64, h -> new CompactionThroughputActuator(h, Map.of()));
        knob(StreamThroughputActuator.NAME, 100, h -> new StreamThroughputActuator(h, Map.of()));
        knob(HintThrottleActuator.NAME, 1024, h -> new HintThrottleActuator(h, Map.of()));
        knob(AdvertisedSeverityActuator.NAME, 0, h -> new AdvertisedSeverityActuator(h));
        for (String pool : RequestPoolActuator.STAGES.keySet())
            knob(pool, 10, h -> new RequestPoolActuator(pool, h, Map.of(), 16));
        actuators.put(RequestThrottleActuator.NAME, new RequestThrottleActuator(new RequestThrottleActuator.Limiter()
        {
            public boolean enabled() { return limiterOn; }
            public int limit() { return limit; }
            public void set(int l, boolean e) { limit = l; limiterOn = e; }
        }, Map.of()));
        ladder = ladder(null);
        view.setActionSource(ms -> records);
        // a growing queue and a known request rate: the throttle step has what it needs
        for (int i = 0; i < 8; i++)
            store.accept(new Observation(Signals.POOL_PENDING_TASKS, Subject.threadPool("ReadStage"), NOW - (8 - i) * 10_000L, 10 + 10 * i));
        store.accept(new Observation(Signals.CLIENT_READ_OPS_PER_SEC, NODE, NOW - 1000, 4000));
    }

    private void knob(String name, double value, java.util.function.Function<FakeHandle, Actuator> build)
    {
        FakeHandle h = new FakeHandle(value);
        handles.put(name, h);
        actuators.put(name, build.apply(h));
    }

    private LoadSpikePolicy ladder(LoadSpikePolicy.Step scaleOut)
    {
        return LoadSpikePolicy.standard(actuators::get, name -> WINDOWS, name -> !disabled.contains(name), scaleOut);
    }

    private Diagnosis diagnosis(String type, Subject subject, Severity severity, long firstSeen, UUID rootCause, Map<String, String> evidence)
    {
        return new Diagnosis(TimeUUID.atUnixMicrosWithLsbAsUUID(1_000_000L + ids++, 3L), type, subject, severity, 1, firstSeen, NOW, "", evidence, rootCause);
    }

    private Diagnosis saturated(String pool, Severity severity, long firstSeen)
    {
        return diagnosis("pool_saturation", Subject.threadPool(pool), severity, firstSeen, null, Map.of());
    }

    private Diagnosis breach(Severity severity, UUID rootCause)
    {
        return diagnosis("latency_slo_breach", NODE, severity, NOW - 60_000, rootCause,
                         Map.of("client.read_latency_p99_micros", "300000 us (read slo 50000 us)", "client.write_latency_p99_micros", "1000 us (write slo 20000 us)"));
    }

    private String next(Diagnosis lead)
    {
        Optional<Plan> plan = ladder.plan(lead, view);
        return plan.isPresent() ? plan.get().actions.get(0).actuator : null;
    }

    private void atLimit(String name, double value)
    {
        handles.get(name).value = value;
    }

    private void record(String actuator, ActionState state, long proposed)
    {
        Action a = new Action(TimeUUID.Generator.nextTimeUUID().asUUID(), actuator, ActionScope.NODE, NODE, Map.of("value", "1"), null);
        records.add(new ActionRecord(a, state, NOW, null, "load_spike", false, null, Map.of(), Map.of(), Map.of(), null, proposed));
    }

    @Test
    public void stepsAreProposedInTheFixedOrderYieldPoolsThrottleThenScaleOut()
    {
        Diagnosis pool = saturated("ReadStage", Severity.CRITICAL, NOW - 60_000);
        Diagnosis latency = breach(Severity.CRITICAL, pool.id);
        view.setActiveDiagnoses(List.of(pool, latency));

        // 1 yield background: compaction, then streaming, then hints
        assertEquals("compaction_throughput", next(pool));
        atLimit("compaction_throughput", 16);
        assertEquals("stream_throughput", next(pool));
        atLimit("stream_throughput", 25);
        assertEquals("hint_throttle", next(pool));
        atLimit("hint_throttle", 256);
        // 2 resize the pool of the saturated stage
        assertEquals("read_pool", next(pool));
        atLimit("read_pool", 20);
        // 3 shed load, on the latency breach the correlator attributed to the pool
        assertEquals("request_throttle", next(pool));
        limiterOn = true;
        limit = 1000;
        // 4 scale-out has no step yet: nothing is proposed
        assertEquals(null, next(pool));
    }

    @Test
    public void scaleOutExtensionPointIsReachedOnlyAfterTheOtherStepsAreExhausted()
    {
        Action scale = new Action(TimeUUID.Generator.nextTimeUUID().asUUID(), "scale_out", ActionScope.CLUSTER, Subject.cluster(), Map.of(), null);
        LoadSpikePolicy.Step step = new LoadSpikePolicy.Step()
        {
            public String name() { return "scale_out"; }
            public Set<String> actuators() { return Set.of("scale_out"); }
            public Optional<Plan> plan(Diagnosis lead, org.apache.cassandra.autonomic.knowledge.KnowledgeView knowledge)
            {
                return Optional.of(new Plan(TimeUUID.Generator.nextTimeUUID().asUUID(), lead.id, "scale_out", List.of(scale), "add a node"));
            }
        };
        ladder = ladder(step);
        Diagnosis pool = saturated("ReadStage", Severity.CRITICAL, NOW - 60_000);
        view.setActiveDiagnoses(List.of(pool, breach(Severity.CRITICAL, pool.id)));
        assertEquals("a step before it still has something", "compaction_throughput", next(pool));
        for (String knob : List.of("compaction_throughput", "stream_throughput", "hint_throttle"))
            atLimit(knob, handles.get(knob).value / 4);
        atLimit("read_pool", 20);
        limiterOn = true;
        limit = 1000;
        assertEquals("scale_out", next(pool));
        // the extension point does not count as enabled or disabled: it is a step with its own actuator
        disabled.add("scale_out");
        assertEquals("a disabled scale-out actuator is skipped like any step", null, next(pool));
    }

    @Test
    public void nothingIsProposedWhileAnActionOfAnyStepIsAwaitingOrInFlight()
    {
        Diagnosis pool = saturated("ReadStage", Severity.CRITICAL, NOW - 60_000);
        view.setActiveDiagnoses(List.of(pool));
        assertEquals("compaction_throughput", next(pool));
        for (ActionState live : List.of(ActionState.PROPOSED, ActionState.APPROVED, ActionState.APPLYING, ActionState.VERIFYING, ActionState.ROLLING_BACK))
        {
            records.clear();
            record("read_pool", live, NOW - 1000);
            assertEquals("a " + live + " action of a later step blocks the ladder", null, next(pool));
        }
        records.clear();
        record("read_pool", ActionState.COMMITTED, NOW - 1000);
        assertEquals("a finished action does not", "compaction_throughput", next(pool));
        records.clear();
        record("compaction_throughput", ActionState.VERIFYING, NOW - 1000);
        record("compaction_throughput", ActionState.REJECTED, NOW - 1000);
        assertEquals(null, next(pool));
    }

    @Test
    public void ofSeveralOpenLoadDiagnosesOnlyTheLeadGetsAPlan()
    {
        Diagnosis read = saturated("ReadStage", Severity.CRITICAL, NOW - 10_000);
        Diagnosis write = saturated("MutationStage", Severity.WARN, NOW - 90_000);
        view.setActiveDiagnoses(List.of(write, read));
        assertEquals("compaction_throughput", next(read));
        assertEquals("a less severe diagnosis waits for the lead", null, next(write));
        // the lead has nothing left on any step: the next diagnosis is served
        for (String knob : List.of("compaction_throughput", "stream_throughput", "hint_throttle"))
            atLimit(knob, handles.get(knob).value / 4);
        atLimit("read_pool", 20);
        atLimit("compaction_throughput", 16);
        atLimit("stream_throughput", 25);
        atLimit("hint_throttle", 256);
        limiterOn = true;
        limit = 1000;
        assertEquals(null, next(read));
        assertEquals("mutation_pool", next(write));
    }

    @Test
    public void aStepWhoseActionWasRolledBackIsIneffectiveAndTheLadderMovesOn()
    {
        Diagnosis pool = saturated("ReadStage", Severity.CRITICAL, NOW - 60_000);
        view.setActiveDiagnoses(List.of(pool));
        assertEquals("compaction_throughput", next(pool));
        record("stream_throughput", ActionState.ROLLED_BACK, NOW - 30_000);
        assertEquals("one rollback in the step is enough: the pools are next", "read_pool", next(pool));
        records.clear();
        record("stream_throughput", ActionState.ROLLED_BACK, NOW - 120_000);
        assertEquals("a rollback from before the diagnosis opened does not count", "compaction_throughput", next(pool));
    }

    @Test
    public void stepsWhoseActuatorsAreNotEnabledAreSkippedAsUnavailable()
    {
        Diagnosis pool = saturated("ReadStage", Severity.CRITICAL, NOW - 60_000);
        view.setActiveDiagnoses(List.of(pool, breach(Severity.CRITICAL, pool.id)));
        disabled.addAll(List.of("compaction_throughput", "stream_throughput", "hint_throttle"));
        assertEquals("read_pool", next(pool));
        disabled.add("read_pool");
        assertEquals("the pool step is skipped too when this pool's actuator is off", "request_throttle", next(pool));
        disabled.remove("compaction_throughput");
        assertEquals("a step with one enabled knob proposes only that knob", "compaction_throughput", next(pool));
    }

    @Test
    public void aLatencyBreachThatIsAnEffectOfPoolSaturationIsNeverTheLeadButFeedsTheThrottleStep()
    {
        Diagnosis pool = saturated("ReadStage", Severity.WARN, NOW - 60_000);
        Diagnosis latency = breach(Severity.CRITICAL, pool.id);
        view.setActiveDiagnoses(List.of(pool, latency));
        assertEquals("the effect has a root cause and the ladder answers it through the root", null, next(latency));
        disabled.addAll(List.of("compaction_throughput", "stream_throughput", "hint_throttle", "read_pool"));
        Plan plan = ladder.plan(pool, view).get();
        assertEquals("request_throttle", plan.actions.get(0).actuator);
        assertEquals("the throttle plan answers the breach, whose latency it verifies", latency.id, plan.diagnosisId);
        assertEquals("client.read_latency_p99_micros", plan.actions.get(0).verify.goal.name);
    }

    @Test
    public void aLatencyBreachWithoutASaturatedPoolIsTheLeadAndSkipsThePoolStep()
    {
        Diagnosis latency = breach(Severity.CRITICAL, null);
        view.setActiveDiagnoses(List.of(latency));
        assertEquals("compaction_throughput", next(latency));
        for (String knob : List.of("compaction_throughput", "stream_throughput", "hint_throttle"))
            atLimit(knob, handles.get(knob).value / 4);
        atLimit("compaction_throughput", 16);
        atLimit("stream_throughput", 25);
        atLimit("hint_throttle", 256);
        assertEquals("request_throttle", next(latency));
    }

    @Test
    public void aDegradedNodeYieldsThenAdvertisesSeverityBeforeAnythingElse()
    {
        Diagnosis degraded = diagnosis("degraded_self", NODE, Severity.WARN, NOW - 60_000, null,
                                       Map.of("client.read_latency_p99_micros", "60000 us (slo 50000 us)", "client.write_latency_p99_micros", "10000 us (slo 20000 us)"));
        view.setActiveDiagnoses(List.of(degraded));
        assertEquals("compaction_throughput", next(degraded));
        atLimit("compaction_throughput", 16);
        atLimit("stream_throughput", 25);
        atLimit("hint_throttle", 256);
        assertEquals("advertised_severity", next(degraded));
    }

    @Test
    public void everyPlanOfTheLadderPassesItsActuatorsValidationAndStepLimit()
    {
        Diagnosis pool = saturated("ReadStage", Severity.CRITICAL, NOW - 60_000);
        view.setActiveDiagnoses(List.of(pool, breach(Severity.CRITICAL, pool.id)));
        List<String> expected = List.of("compaction_throughput", "stream_throughput", "hint_throttle", "read_pool", "request_throttle");
        for (String name : expected)
        {
            Action a = ladder.plan(pool, view).get().actions.get(0);
            assertEquals(name, a.actuator);
            assertEquals(Optional.empty(), actuators.get(name).validate(a, view));
            assertEquals(Optional.empty(), actuators.get(name).blastRadius(a, view, 0.25));
            disabled.add(name);
        }
        assertFalse(ladder.plan(pool, view).isPresent());
        assertTrue(ladder.handles().containsAll(Set.of("pool_saturation", "latency_slo_breach", "degraded_self")));
    }
}
