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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.autonomic.analyze.Correlator;
import org.apache.cassandra.autonomic.analyze.Detector;
import org.apache.cassandra.autonomic.analyze.Diagnosis;
import org.apache.cassandra.autonomic.analyze.RuleCorrelator;
import org.apache.cassandra.autonomic.analyze.Severity;
import org.apache.cassandra.autonomic.knowledge.KnowledgeView;
import org.apache.cassandra.autonomic.monitor.InjectedSensor;
import org.apache.cassandra.autonomic.monitor.ObservationSink;
import org.apache.cassandra.autonomic.monitor.Sensor;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.plan.Action;
import org.apache.cassandra.autonomic.plan.ActionScope;
import org.apache.cassandra.autonomic.plan.Plan;
import org.apache.cassandra.autonomic.plan.Policy;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.DurationSpec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class AutonomicLoopTest
{
    private static final SignalId SIG = SignalId.of("test.signal");
    private static final Subject SUBJECT = Subject.table("ks", "tbl");

    private final AtomicLong now = new AtomicLong(1_000_000L);
    private final AtomicBoolean joined = new AtomicBoolean(true);
    private AutonomicService service;
    private DurationSpec.IntMillisecondsBound savedGrace;
    private DurationSpec.IntMillisecondsBound savedBudget;
    private int savedThreshold;

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
        savedBudget = c.safety.sensor_budget;
        savedThreshold = c.safety.component_failure_threshold;
        c.enabled = true;
        c.startup_grace = new DurationSpec.IntMillisecondsBound("0ms");
        c.safety.sensor_budget = new DurationSpec.IntMillisecondsBound("200ms");
        c.safety.component_failure_threshold = 3;
        service = new AutonomicService(now::get, joined::get);
    }

    @After
    public void teardown() throws Exception
    {
        service.shutdownBlocking();
        AutonomicConfig c = DatabaseDescriptor.getAutonomicConfig();
        c.enabled = false;
        c.startup_grace = savedGrace;
        c.safety.sensor_budget = savedBudget;
        c.safety.component_failure_threshold = savedThreshold;
    }

    private static class CountingSensor implements Sensor
    {
        final String name;
        final long minInterval;
        final AtomicInteger calls = new AtomicInteger();
        RuntimeException failure;
        Throwable error;
        Runnable duringCollect = () -> {};

        CountingSensor(String name, long minInterval)
        {
            this.name = name;
            this.minInterval = minInterval;
        }

        public String name() { return name; }
        public Set<SignalId> signals() { return Set.of(SIG); }
        public long minIntervalMillis() { return minInterval; }

        public void collect(ObservationSink sink, long nowMillis)
        {
            calls.incrementAndGet();
            duringCollect.run();
            if (failure != null)
                throw failure;
            if (error != null)
                sneakyThrow(error);
        }

        @SuppressWarnings("unchecked")
        private static <T extends Throwable> void sneakyThrow(Throwable t) throws T
        {
            throw (T) t;
        }
    }

    private static class FakeDetector implements Detector
    {
        final AtomicInteger calls = new AtomicInteger();
        volatile boolean fail;
        final UUID id = UUID.randomUUID();
        final String type;

        FakeDetector(String type)
        {
            this.type = type;
        }

        public String name() { return "fake_" + type; }
        public Set<SignalId> requires() { return Set.of(SIG); }

        public List<Diagnosis> analyze(KnowledgeView k)
        {
            calls.incrementAndGet();
            if (fail)
                throw new IllegalStateException("boom");
            return List.of(new Diagnosis(id, type, SUBJECT, Severity.WARN, 0.9, 1, k.nowMillis(), "fake", Map.of(), null));
        }
    }

    private static class FakePolicy implements Policy
    {
        final String type;
        final AtomicInteger calls = new AtomicInteger();

        FakePolicy(String type)
        {
            this.type = type;
        }

        public String name() { return "fake_policy_" + type; }
        public Set<String> handles() { return Set.of(type); }

        public Optional<Plan> plan(Diagnosis d, KnowledgeView k)
        {
            calls.incrementAndGet();
            Action a = new Action(UUID.randomUUID(), "fake_actuator", ActionScope.NODE, d.subject, Map.of("x", "1"), null);
            return Optional.of(new Plan(UUID.randomUUID(), d.id, name(), List.of(a), "because"));
        }
    }

    private void startAndArm()
    {
        service.start();
        service.controlTick();
        assertTrue(service.isArmed());
    }

    private boolean hasDiagnosis(String type)
    {
        return service.view().activeDiagnoses().stream().anyMatch(d -> d.type.equals(type));
    }

    @Test
    public void sensorCadenceRespectsMinInterval()
    {
        CountingSensor sensor = new CountingSensor("s", 1000);
        service.registerSensor(sensor);
        service.start();
        service.monitorTick();
        assertEquals(1, sensor.calls.get());
        now.addAndGet(500);
        service.monitorTick();
        assertEquals(1, sensor.calls.get());
        now.addAndGet(500);
        service.monitorTick();
        assertEquals(2, sensor.calls.get());
        now.addAndGet(999);
        service.monitorTick();
        assertEquals(2, sensor.calls.get());
    }

    @Test
    public void throwingSensorIsDisabledOthersKeepRunning()
    {
        CountingSensor bad = new CountingSensor("bad", 0);
        bad.failure = new IllegalStateException("broken");
        CountingSensor good = new CountingSensor("good", 0);
        service.registerSensor(bad);
        service.registerSensor(good);
        service.start();
        for (int i = 0; i < 6; i++)
        {
            now.addAndGet(10);
            service.monitorTick(); // must not throw
        }
        assertEquals(3, bad.calls.get());
        assertEquals(6, good.calls.get());
        assertTrue(service.componentHealth("sensor", "bad").disabled);
        assertFalse(service.componentHealth("sensor", "good").disabled);
        assertTrue(hasDiagnosis(AutonomicService.COMPONENT_FAILED));
        assertTrue(service.getStatus().get("disabled_components").contains("sensor:bad"));
    }

    @Test
    public void errorFromSensorDoesNotEscapeTick()
    {
        CountingSensor bad = new CountingSensor("err", 0);
        bad.error = new AssertionError("an Error");
        service.registerSensor(bad);
        service.start();
        service.monitorTick();
        assertEquals(1, service.componentHealth("sensor", "err").consecutiveFailures);
    }

    @Test
    public void successResetsConsecutiveFailures()
    {
        CountingSensor s = new CountingSensor("flaky", 0);
        service.registerSensor(s);
        service.start();
        s.failure = new RuntimeException("x");
        service.monitorTick();
        service.monitorTick();
        s.failure = null;
        service.monitorTick();
        s.failure = new RuntimeException("x");
        service.monitorTick();
        service.monitorTick();
        assertFalse(service.componentHealth("sensor", "flaky").disabled);
    }

    @Test
    public void slowSensorCountsAsFailure()
    {
        CountingSensor slow = new CountingSensor("slow", 0);
        slow.duringCollect = () -> now.addAndGet(300);
        service.registerSensor(slow);
        service.start();
        service.monitorTick();
        assertEquals(1, service.componentHealth("sensor", "slow").slowCalls);
        assertEquals(1, service.componentHealth("sensor", "slow").consecutiveFailures);
        service.monitorTick();
        service.monitorTick();
        assertTrue(service.componentHealth("sensor", "slow").disabled);
        assertTrue(hasDiagnosis(AutonomicService.COMPONENT_FAILED));
    }

    @Test
    public void injectedSensorFeedsStoreAndView()
    {
        InjectedSensor injected = new InjectedSensor();
        service.registerSensor(injected);
        service.start();
        injected.push(SIG, SUBJECT, 42.0);
        service.monitorTick();
        assertEquals(42.0, service.view().latest(SIG, SUBJECT).getAsDouble(), 0.0);
        assertTrue(service.view().subjects(SIG).contains(SUBJECT));
    }

    @Test
    public void detectorsDoNotRunBeforeArmed()
    {
        FakeDetector detector = new FakeDetector("t");
        service.registerDetector(detector);
        joined.set(false);
        service.start();
        service.controlTick();
        assertEquals(0, detector.calls.get());
        assertFalse(service.isArmed());
        joined.set(true);
        service.controlTick();
        assertEquals(1, detector.calls.get());
        assertTrue(hasDiagnosis("t"));
    }

    @Test
    public void graceDelaysArming()
    {
        DatabaseDescriptor.getAutonomicConfig().startup_grace = new DurationSpec.IntMillisecondsBound("500ms");
        FakeDetector detector = new FakeDetector("t");
        service.registerDetector(detector);
        service.start();
        service.controlTick();
        assertEquals(0, detector.calls.get());
        now.addAndGet(500);
        service.controlTick();
        assertEquals(1, detector.calls.get());
    }

    @Test
    public void pausedProducesDiagnosesButNoPlans()
    {
        FakeDetector detector = new FakeDetector("t");
        FakePolicy policy = new FakePolicy("t");
        List<Plan> plans = new ArrayList<>();
        service.registerDetector(detector);
        service.registerPolicy(policy);
        service.setPlanSink((p, k) -> plans.add(p));
        startAndArm();
        assertEquals(1, plans.size());

        service.pause("test");
        service.controlTick();
        assertEquals(2, detector.calls.get());
        assertEquals(1, policy.calls.get());
        assertEquals(1, plans.size());
        assertTrue(hasDiagnosis("t"));

        service.resume();
        service.controlTick();
        assertEquals(2, plans.size());
    }

    @Test
    public void detectorCorrelatorPolicySinkPipeline()
    {
        FakeDetector symptom = new FakeDetector("symptom");
        FakeDetector cause = new FakeDetector("cause");
        FakePolicy symptomPolicy = new FakePolicy("symptom");
        FakePolicy causePolicy = new FakePolicy("cause");
        List<Plan> plans = new ArrayList<>();
        service.registerDetector(symptom);
        service.registerDetector(cause);
        service.registerPolicy(symptomPolicy);
        service.registerPolicy(causePolicy);
        // the correlator links the symptom to the cause and suppresses the derived diagnosis
        service.setCorrelator((Correlator) (raw, k) -> {
            List<Diagnosis> out = new ArrayList<>();
            for (Diagnosis d : raw)
                if (d.type.equals("cause"))
                    out.add(d);
            return out;
        });
        service.setPlanSink((p, k) -> plans.add(p));
        startAndArm();
        assertEquals(1, plans.size());
        assertEquals(cause.id, plans.get(0).diagnosisId);
        assertEquals(causePolicy.name(), plans.get(0).policy);
        assertEquals(0, symptomPolicy.calls.get());
        assertEquals(Collections.singletonList("cause"),
                     service.view().activeDiagnoses().stream().map(d -> d.type).collect(java.util.stream.Collectors.toList()));
    }

    @Test
    public void ruleCorrelatorMarksEffectsKeepsThemActiveAndPoliciesGetOnlyTheRoot()
    {
        FakeDetector effect = new FakeDetector("latency_slo_breach");
        FakeDetector cause = new FakeDetector("compaction_backlog");
        FakePolicy effectPolicy = new FakePolicy("latency_slo_breach");
        FakePolicy causePolicy = new FakePolicy("compaction_backlog");
        List<Plan> plans = new ArrayList<>();
        service.registerDetector(effect);
        service.registerDetector(cause);
        service.registerPolicy(effectPolicy);
        service.registerPolicy(causePolicy);
        service.setCorrelator(RuleCorrelator.defaults());
        service.setPlanSink((p, k) -> plans.add(p));
        startAndArm();
        assertEquals(1, plans.size());
        assertEquals(cause.id, plans.get(0).diagnosisId);
        assertEquals(0, effectPolicy.calls.get());
        Map<UUID, Diagnosis> active = new java.util.HashMap<>();
        for (Diagnosis d : service.view().activeDiagnoses())
            active.put(d.id, d);
        assertEquals(2, active.size());
        assertEquals(cause.id, active.get(effect.id).rootCause);
        assertEquals(null, active.get(cause.id).rootCause);
    }

    @Test
    public void throwingPolicyAndDetectorAreIsolatedAndDisabled()
    {
        FakeDetector bad = new FakeDetector("bad");
        bad.fail = true;
        FakeDetector good = new FakeDetector("good");
        Policy badPolicy = new Policy()
        {
            public String name() { return "bad_policy"; }
            public Set<String> handles() { return Set.of("good"); }
            public Optional<Plan> plan(Diagnosis d, KnowledgeView k) { throw new IllegalStateException("nope"); }
        };
        FakePolicy goodPolicy = new FakePolicy("good");
        List<Plan> plans = new ArrayList<>();
        service.registerDetector(bad);
        service.registerDetector(good);
        service.registerPolicy(badPolicy);
        service.registerPolicy(goodPolicy);
        service.setPlanSink((p, k) -> plans.add(p));
        startAndArm();
        service.controlTick();
        service.controlTick();
        service.controlTick();
        assertEquals(3, bad.calls.get());
        assertEquals(4, good.calls.get());
        assertEquals(4, plans.size());
        assertTrue(service.componentHealth("detector", "fake_bad").disabled);
        assertTrue(service.componentHealth("policy", "bad_policy").disabled);
        assertTrue(hasDiagnosis(AutonomicService.COMPONENT_FAILED));
    }

    @Test
    public void throwingSinkDoesNotEscape()
    {
        service.registerDetector(new FakeDetector("t"));
        service.registerPolicy(new FakePolicy("t"));
        service.setPlanSink((p, k) -> { throw new IllegalStateException("sink"); });
        startAndArm();
    }

    @Test
    public void resumeReenablesDisabledComponent()
    {
        FakeDetector detector = new FakeDetector("t");
        detector.fail = true;
        service.registerDetector(detector);
        startAndArm(); // 1 failure
        service.controlTick();
        service.controlTick();
        assertTrue(service.componentHealth("detector", "fake_t").disabled);
        assertTrue(hasDiagnosis(AutonomicService.COMPONENT_FAILED));
        int before = detector.calls.get();
        service.controlTick();
        assertEquals(before, detector.calls.get());

        detector.fail = false;
        service.resume();
        assertFalse(hasDiagnosis(AutonomicService.COMPONENT_FAILED));
        service.controlTick();
        assertEquals(before + 1, detector.calls.get());
        assertTrue(hasDiagnosis("t"));
    }

    @Test
    public void stalledLoopIsDiagnosedAndCleared()
    {
        service.start();
        long analyze = DatabaseDescriptor.getAutonomicConfig().analyze_interval.toMilliseconds();
        service.monitorTick();
        assertFalse(hasDiagnosis(AutonomicService.LOOP_STALLED));
        now.addAndGet(3 * analyze + 1);
        service.monitorTick(); // control thread has not ticked
        assertTrue(hasDiagnosis(AutonomicService.LOOP_STALLED));
        Diagnosis d = service.view().activeDiagnoses().iterator().next();
        assertNotNull(d.id);
        service.controlTick();
        service.monitorTick();
        assertFalse(hasDiagnosis(AutonomicService.LOOP_STALLED));
    }

    @Test
    public void millisecondIntervalsAreHonouredByScheduler() throws Exception
    {
        AutonomicConfig c = DatabaseDescriptor.getAutonomicConfig();
        DurationSpec.IntMillisecondsBound savedMonitor = c.monitor_interval;
        c.monitor_interval = new DurationSpec.IntMillisecondsBound("20ms");
        try
        {
            CountingSensor sensor = new CountingSensor("fast", 0);
            service.registerSensor(sensor);
            service.start();
            long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(10);
            while (sensor.calls.get() < 3 && System.nanoTime() < deadline)
                Thread.sleep(10);
            assertTrue(sensor.calls.get() >= 3);
        }
        finally
        {
            c.monitor_interval = savedMonitor;
        }
    }
}
