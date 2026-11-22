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
import java.util.Map;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.autonomic.analyze.Diagnosis;
import org.apache.cassandra.autonomic.analyze.Severity;
import org.apache.cassandra.autonomic.analyze.ThresholdDetector;
import org.apache.cassandra.autonomic.execute.ActionContext;
import org.apache.cassandra.autonomic.execute.actuators.RequestThrottleActuator;
import org.apache.cassandra.autonomic.knowledge.InMemoryKnowledgeView;
import org.apache.cassandra.autonomic.knowledge.ObservationStore;
import org.apache.cassandra.autonomic.monitor.Observation;
import org.apache.cassandra.autonomic.monitor.SignalId;
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

public class RequestThrottlePolicyTest
{
    private static final Subject NODE = Subject.node();
    private static final Subject POOL = Subject.threadPool("ReadStage");
    private static final Windows WINDOWS = new Windows(60_000, 120_000);
    private static final long NOW = 1_000_000;

    private boolean enabled;
    private int limit = 1_000_000;
    private final RequestThrottleActuator knob = new RequestThrottleActuator(new RequestThrottleActuator.Limiter()
    {
        public boolean enabled() { return enabled; }
        public int limit() { return limit; }
        public void set(int l, boolean e) { limit = l; enabled = e; }
    }, Map.of());
    private final ObservationStore store = new ObservationStore(32, 32);
    private final InMemoryKnowledgeView view = new InMemoryKnowledgeView(store, () -> NOW);
    private final RequestThrottlePolicy breach = new RequestThrottlePolicy(name -> knob, name -> WINDOWS, RequestThrottlePolicy.Mode.BREACH);
    private final RequestThrottlePolicy recovery = new RequestThrottlePolicy(name -> knob, name -> WINDOWS, RequestThrottlePolicy.Mode.RECOVERY);
    private long ids;

    @BeforeClass
    public static void setupClass()
    {
        DatabaseDescriptor.daemonInitialization();
    }

    /** Latency samples of the goal and the guard in the baseline window. */
    private static void latencies(ObservationStore into)
    {
        for (int i = 1; i <= 5; i++)
        {
            into.accept(new Observation(Signals.CLIENT_READ_LATENCY_P99_MICROS, NODE, NOW - i * 1000L, 1000));
            into.accept(new Observation(Signals.CLIENT_WRITE_LATENCY_P99_MICROS, NODE, NOW - i * 1000L, 1000));
        }
    }

    @Before
    public void samples()
    {
        latencies(store);
    }

    private void put(SignalId signal, Subject subject, long ago, double value)
    {
        store.accept(new Observation(signal, subject, NOW - ago, value));
    }

    /** A queue that grows, or one that does not. */
    private void queue(boolean growing)
    {
        for (int i = 0; i < 8; i++)
            put(Signals.POOL_PENDING_TASKS, POOL, (8 - i) * 10_000L, growing ? 10 + 10 * i : 40);
    }

    private Diagnosis diagnosis(String type, Subject subject, Severity severity, Map<String, String> evidence)
    {
        return new Diagnosis(TimeUUID.atUnixMicrosWithLsbAsUUID(1_000_000L + ids++, 3L), type, subject, severity, 1, 0, 0, "", evidence, null);
    }

    private Diagnosis breaching(Severity severity)
    {
        return diagnosis("latency_slo_breach", NODE, severity,
                         Map.of("client.read_latency_p99_micros", "300000 us (read slo 50000 us)",
                                "client.write_latency_p99_micros", "20000 us (write slo 20000 us)"));
    }

    private Diagnosis drift()
    {
        return diagnosis("autonomic.baseline_drift", NODE, Severity.INFO, Map.of("request_throttle", "3200 (configured 1000000)"));
    }

    @Test
    public void aCriticalNodeWideBreachWithAGrowingQueueEngagesTheLimiterBelowTheObservedRate()
    {
        queue(true);
        put(Signals.CLIENT_READ_OPS_PER_SEC, NODE, 1000, 3000);
        put(Signals.CLIENT_WRITE_OPS_PER_SEC, NODE, 1000, 1000);
        Diagnosis d = breaching(Severity.CRITICAL);
        Plan plan = breach.plan(d, view).get();
        assertEquals(d.id, plan.diagnosisId);
        Action a = plan.actions.get(0);
        assertEquals("request_throttle", a.actuator);
        assertEquals(ActionScope.NODE, a.scope);
        assertEquals("3200", a.params.get("value"));
        assertEquals("the worse operation is the goal", "client.read_latency_p99_micros", a.verify.goal.name);
        assertEquals("client.write_latency_p99_micros", a.verify.guards.get(0).signal.name);
        assertEquals(0.10, a.verify.minImprovement, 0);
    }

    @Test
    public void theLimiterWaitsForSamplesOfTheProblemPeriod()
    {
        queue(true);
        put(Signals.CLIENT_READ_OPS_PER_SEC, NODE, 1000, 3000);
        Diagnosis d = diagnosis("latency_slo_breach", NODE, Severity.CRITICAL,
                                Map.of(ThresholdDetector.BREACH_SINCE, Long.toString(NOW - 2500), "client.read_latency_p99_micros", "300000 us"));
        assertFalse("two latency samples since the problem began", breach.plan(d, view).isPresent());
        Diagnosis earlier = diagnosis("latency_slo_breach", NODE, Severity.CRITICAL,
                                      Map.of(ThresholdDetector.BREACH_SINCE, Long.toString(NOW - 3500), "client.read_latency_p99_micros", "300000 us"));
        assertEquals(NOW - 3500, breach.plan(earlier, view).get().actions.get(0).verify.baselineSinceMillis);
    }

    @Test
    public void nothingBelowCriticalWithoutAGrowingQueueOrForATableOrWithoutARate()
    {
        put(Signals.CLIENT_READ_OPS_PER_SEC, NODE, 1000, 4000);
        queue(true);
        assertFalse(breach.plan(breaching(Severity.WARN), view).isPresent());
        assertFalse(breach.plan(diagnosis("latency_slo_breach", Subject.table("ks", "t"), Severity.CRITICAL, Map.of()), view).isPresent());
        queue(false);
        assertFalse("a queue that does not grow is not shed", breach.plan(breaching(Severity.CRITICAL), view).isPresent());
        // the pool series is rebuilt: only a flat queue remains
        ObservationStore other = new ObservationStore(32, 32);
        InMemoryKnowledgeView noRate = new InMemoryKnowledgeView(other, () -> NOW);
        for (int i = 0; i < 8; i++)
            other.accept(new Observation(Signals.POOL_PENDING_TASKS, POOL, NOW - (8 - i) * 10_000L, 10 + 10 * i));
        latencies(other);
        assertFalse("an unknown rate cannot engage the limiter", breach.plan(breaching(Severity.CRITICAL), noRate).isPresent());
    }

    @Test
    public void anEngagedLimiterIsLoweredByAStepAndStopsAtTheFloor() throws Exception
    {
        queue(true);
        enabled = true;
        limit = 1200;
        Action a = breach.plan(breaching(Severity.CRITICAL), view).get().actions.get(0);
        assertEquals("1000", a.params.get("value"));
        limit = 1000;
        assertFalse("at the floor the policy has nothing left", breach.plan(breaching(Severity.CRITICAL), view).isPresent());
    }

    @Test
    public void recoveryRaisesAdditivelyThenReleasesTheLimiterOnceItNoLongerBinds()
    {
        enabled = true;
        limit = 3200;
        put(Signals.CLIENT_READ_OPS_PER_SEC, NODE, 1000, 3000);
        Action a = recovery.plan(drift(), view).get().actions.get(0);
        assertEquals("3300", a.params.get("value"));
        assertEquals("the latency must not get worse", 0.0, a.verify.minImprovement, 0);
        put(Signals.CLIENT_READ_OPS_PER_SEC, NODE, 500, 2000);
        assertEquals("the limit exceeds 1.2x the demand: released", "1000000", recovery.plan(drift(), view).get().actions.get(0).params.get("value"));
    }

    @Test
    public void recoveryWaitsWhileALatencyBreachIsOpenAndIgnoresOtherDrift()
    {
        enabled = true;
        limit = 3200;
        view.setActiveDiagnoses(List.of(breaching(Severity.WARN)));
        assertFalse(recovery.plan(drift(), view).isPresent());
        view.setActiveDiagnoses(List.of());
        assertFalse(recovery.plan(diagnosis("autonomic.baseline_drift", NODE, Severity.INFO, Map.of("read_pool", "6 (configured 4)")), view).isPresent());
        enabled = false;
        assertFalse("not engaged: nothing to recover", recovery.plan(drift(), view).isPresent());
    }

    @Test
    public void handlesTheLatencyBreachAndTheDriftDiagnosisSeparately()
    {
        assertEquals(java.util.Set.of("latency_slo_breach"), breach.handles());
        assertEquals(java.util.Set.of("autonomic.baseline_drift"), recovery.handles());
        assertTrue(!breach.name().equals(recovery.name()));
    }

    @Test
    public void everyActionPassesTheActuatorsValidationAndStepLimit() throws Exception
    {
        queue(true);
        put(Signals.CLIENT_READ_OPS_PER_SEC, NODE, 1000, 4000);
        Action engage = breach.plan(breaching(Severity.CRITICAL), view).get().actions.get(0);
        assertFalse(knob.validate(engage, view).isPresent());
        assertFalse(knob.blastRadius(engage, view, 0.25).isPresent());
        knob.apply(engage, new ActionContext()
        {
            public long stepDeadlineMillis() { return Long.MAX_VALUE; }
            public long nowMillis() { return 0; }
            public boolean killed() { return false; }
            public void recordUndo(Map<String, String> undo) {}
        });
        Action lower = breach.plan(breaching(Severity.CRITICAL), view).get().actions.get(0);
        assertFalse(knob.validate(lower, view).isPresent());
        assertFalse(knob.blastRadius(lower, view, 0.25).isPresent());
    }
}
