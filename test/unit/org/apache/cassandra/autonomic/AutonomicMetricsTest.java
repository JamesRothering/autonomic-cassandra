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
package org.apache.cassandra.autonomic;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.Consumer;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.autonomic.analyze.Detector;
import org.apache.cassandra.autonomic.analyze.Diagnosis;
import org.apache.cassandra.autonomic.analyze.Severity;
import org.apache.cassandra.autonomic.execute.ActionRecord;
import org.apache.cassandra.autonomic.execute.ActionState;
import org.apache.cassandra.autonomic.execute.FakeKnobActuator;
import org.apache.cassandra.autonomic.knowledge.InMemoryKnowledgeStore;
import org.apache.cassandra.autonomic.knowledge.KnowledgeView;
import org.apache.cassandra.autonomic.monitor.Observation;
import org.apache.cassandra.autonomic.monitor.ObservationSink;
import org.apache.cassandra.autonomic.monitor.Sensor;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.plan.Action;
import org.apache.cassandra.autonomic.plan.ActionScope;
import org.apache.cassandra.autonomic.plan.Plan;
import org.apache.cassandra.autonomic.plan.VerifySpec;
import org.apache.cassandra.autonomic.safety.Reasons;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DurationSpec;
import org.apache.cassandra.diag.DiagnosticEventService;
import org.apache.cassandra.utils.TimeUUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The metrics of {@link AutonomicService} and the {@link AutonomicEvent}s, driven the same way as
 * {@link AutonomicLoopTest} and {@link AutonomicExecutionTest}: fake clock, explicit ticks, no real loops.
 */
public class AutonomicMetricsTest
{
    private static final SignalId GOAL = SignalId.of("goal");
    private static final Subject NODE = Subject.node();
    private static final Subject TABLE = Subject.table("ks", "tbl");
    private static final VerifySpec SPEC = new VerifySpec(GOAL, NODE, true, 0.10, List.of(), 10_000, 50);

    private final AtomicLong now = new AtomicLong(1_000_000L);
    private final AtomicBoolean diagnosisActive = new AtomicBoolean();
    private final InMemoryKnowledgeStore store = new InMemoryKnowledgeStore();
    private final List<AutonomicEvent> events = new CopyOnWriteArrayList<>();
    private final Consumer<AutonomicEvent> collector = events::add;
    private final FakeKnobActuator knob = new FakeKnobActuator("knob");
    private AutonomicService service;
    private DurationSpec.IntMillisecondsBound savedGrace;
    private DurationSpec.IntMillisecondsBound savedAnalyze;
    private boolean savedDryRun;
    private boolean savedEvents;

    @BeforeClass
    public static void setupClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    @Before
    public void setup()
    {
        AutonomicConfig c = DatabaseDescriptor.getAutonomicConfig();
        savedGrace = c.startup_grace;
        savedAnalyze = c.analyze_interval;
        savedDryRun = c.dry_run;
        savedEvents = DatabaseDescriptor.diagnosticEventsEnabled();
        c.enabled = true;
        // dry run: a proposal is rejected by the governor, which exercises the terminal and rejection paths
        c.dry_run = true;
        c.startup_grace = new DurationSpec.IntMillisecondsBound("0ms");
        c.analyze_interval = new DurationSpec.IntMillisecondsBound("1h");
        c.safety.max_actions_per_actuator_per_hour = 100;
        DatabaseDescriptor.setDiagnosticEventsEnabled(true);
        service = new AutonomicService(now::get, () -> true, store);
        service.registerActuator(knob);
        service.setActuatorEnabled("knob", true);
        DiagnosticEventService.instance().subscribe(AutonomicEvent.class, collector);
    }

    @After
    public void teardown() throws Exception
    {
        DiagnosticEventService.instance().unsubscribe(collector);
        service.shutdownBlocking();
        AutonomicConfig c = DatabaseDescriptor.getAutonomicConfig();
        c.enabled = false;
        c.dry_run = savedDryRun;
        c.startup_grace = savedGrace;
        c.analyze_interval = savedAnalyze;
        DatabaseDescriptor.setDiagnosticEventsEnabled(savedEvents);
    }

    /** Emits two observations per collect, and throws when told to. */
    private static class SensorUnderTest implements Sensor
    {
        final String name;
        final AtomicBoolean fail = new AtomicBoolean();

        SensorUnderTest(String name)
        {
            this.name = name;
        }

        public String name() { return name; }
        public Set<SignalId> signals() { return Set.of(GOAL); }
        public long minIntervalMillis() { return 0; }

        public void collect(ObservationSink sink, long nowMillis)
        {
            if (fail.get())
                throw new IllegalStateException("sensor broke");
            sink.accept(new Observation(GOAL, NODE, nowMillis, 1.0));
            sink.accept(new Observation(GOAL, NODE, nowMillis, 2.0));
        }
    }

    /** Reports one diagnosis of the given type while {@link #diagnosisActive} is set. */
    private class ToggledDetector implements Detector
    {
        final UUID id = UUID.randomUUID();

        public String name() { return "toggled"; }
        public Set<SignalId> requires() { return Set.of(GOAL); }

        public List<Diagnosis> analyze(KnowledgeView k)
        {
            if (!diagnosisActive.get())
                return List.of();
            return List.of(new Diagnosis(id, "test_diagnosis", TABLE, Severity.WARN, 0.9, 1, k.nowMillis(), "toggled on", Map.of(), null));
        }
    }

    private void startAndArm()
    {
        service.start();
        service.controlTick();
        assertTrue(service.isArmed());
    }

    private static void await(BooleanSupplier condition) throws InterruptedException
    {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        while (!condition.getAsBoolean())
        {
            if (System.nanoTime() > deadline)
                fail("timed out");
            Thread.sleep(5);
        }
    }

    private boolean hasEvent(AutonomicEvent.AutonomicEventType type)
    {
        return events.stream().anyMatch(e -> e.getType() == type);
    }

    private Action action(long value)
    {
        return new Action(TimeUUID.atUnixMicrosWithLsbAsUUID(now.get() * 1000 + value, 5L), "knob", ActionScope.NODE, TABLE,
                          Map.of("value", String.valueOf(value)), SPEC);
    }

    private ActionRecord propose(Action a)
    {
        return service.executor().propose(new Plan(UUID.randomUUID(), UUID.randomUUID(), "policy", List.of(a), "test"), a, service.view());
    }

    @Test
    public void nothingIsRegisteredUntilTheLayerStarts() throws Exception
    {
        assertNull(service.metrics());
        startAndArm();
        assertNotNull(service.metrics());
        service.shutdownBlocking();
        assertNull(service.metrics());
    }

    @Test
    public void monitorTickRecordsDurationAndObservations()
    {
        startAndArm();
        AutonomicMetrics m = service.metrics();
        service.registerSensor(new SensorUnderTest("tick_sensor"));
        long observationsBefore = m.observationsAccepted();
        service.monitorTick();
        service.monitorTick();

        assertEquals(2, m.monitorTicks());
        // the production sensors also run and may add observations, so only the lower bound is exact
        assertTrue(m.observationsAccepted() - observationsBefore >= 4);
    }

    @Test
    public void controlTickRecordsDuration()
    {
        startAndArm();
        AutonomicMetrics m = service.metrics();
        long before = m.controlTicks();
        service.controlTick();
        assertEquals(before + 1, m.controlTicks());
    }

    @Test
    public void failingSensorIncrementsItsFailureCounter()
    {
        startAndArm();
        AutonomicMetrics m = service.metrics();
        SensorUnderTest bad = new SensorUnderTest("bad_sensor");
        bad.fail.set(true);
        service.registerSensor(bad);
        service.monitorTick();
        assertEquals(1, m.count("SensorFailures", "bad_sensor"));
        service.monitorTick();
        assertEquals(2, m.count("SensorFailures", "bad_sensor"));
        assertEquals(0, m.count("SensorFailures", "other_sensor"));
    }

    @Test
    public void openingAndClearingADiagnosisIncrementsItsCountersAndPublishesEvents()
    {
        service.registerDetector(new ToggledDetector());
        startAndArm();
        AutonomicMetrics m = service.metrics();

        diagnosisActive.set(true);
        service.controlTick();
        assertEquals(1, m.count("DiagnosesOpened", "test_diagnosis"));
        assertEquals(0, m.count("DiagnosesCleared", "test_diagnosis"));
        // a diagnosis that stays open is not counted again
        service.controlTick();
        assertEquals(1, m.count("DiagnosesOpened", "test_diagnosis"));

        diagnosisActive.set(false);
        service.controlTick();
        assertEquals(1, m.count("DiagnosesCleared", "test_diagnosis"));
        assertEquals(1, m.count("DiagnosesOpened", "test_diagnosis"));

        assertTrue(hasEvent(AutonomicEvent.AutonomicEventType.DIAGNOSIS_OPENED));
        assertTrue(hasEvent(AutonomicEvent.AutonomicEventType.DIAGNOSIS_CLEARED));
    }

    @Test
    public void actionReachingATerminalStateAndGovernorRejectionIncrementCounters() throws Exception
    {
        startAndArm();
        AutonomicMetrics m = service.metrics();
        // a goal baseline, as the execution tests seed it; without one the proposal is held, not rejected
        for (int i = 1; i <= 3; i++)
            service.store().accept(new Observation(GOAL, NODE, now.get() - i * 100, 100));

        // dry run: the governor rejects, which is a terminal state with a governor reason
        ActionRecord r = propose(action(128));
        // the proposal is returned before the governor decides; the terminal transition arrives on another thread
        await(() -> m.terminal(ActionState.REJECTED) == 1);
        assertEquals(Reasons.DRY_RUN, service.executor().record(r.id()).orElseThrow().stateReason);
        assertEquals(0, m.terminal(ActionState.COMMITTED));
        assertEquals(1, m.count("GovernorRejections", Reasons.DRY_RUN));
        assertEquals(1, m.count("ActionsTerminalByActuator", "knob"));
        assertEquals(0, m.rollbacks());

        // the PROPOSED transition is published first; select the terminal one
        AutonomicEvent action = events.stream()
                                      .filter(e -> e.getType() == AutonomicEvent.AutonomicEventType.ACTION_TRANSITION
                                                   && "REJECTED".equals(e.toMap().get("to")))
                                      .findFirst().orElseThrow();
        Map<String, Serializable> details = action.toMap();
        assertEquals(r.id().toString(), details.get("action_id"));
        assertEquals("REJECTED", details.get("to"));
        assertEquals(Reasons.DRY_RUN, details.get("reason"));
    }

    @Test
    public void layerStateEventIsPublishedOnTheFirstControlTick()
    {
        startAndArm();
        assertTrue(events.stream().anyMatch(e -> e.getType() == AutonomicEvent.AutonomicEventType.LAYER_STATE
                                                 && "RUNNING".equals(e.toMap().get("state"))));
    }

    @Test
    public void noEventsWhenDiagnosticEventsAreDisabled()
    {
        DatabaseDescriptor.setDiagnosticEventsEnabled(false);
        service.registerDetector(new ToggledDetector());
        startAndArm();
        diagnosisActive.set(true);
        service.controlTick();
        assertFalse(hasEvent(AutonomicEvent.AutonomicEventType.DIAGNOSIS_OPENED));
        assertEquals(1, service.metrics().count("DiagnosesOpened", "test_diagnosis"));
    }
}
