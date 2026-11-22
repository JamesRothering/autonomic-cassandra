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
package org.apache.cassandra.autonomic.execute;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

import org.apache.cassandra.autonomic.knowledge.InMemoryKnowledgeView;
import org.apache.cassandra.autonomic.knowledge.ObservationStore;
import org.apache.cassandra.autonomic.monitor.Observation;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.plan.VerifySpec;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Verdict table of overview 7.3 rule 6, baseline rule 5 and early abort rule 8. */
public class MetricVerifierTest
{
    private static final SignalId GOAL = SignalId.of("goal");
    private static final SignalId GUARD = SignalId.of("guard");
    private static final Subject NODE = Subject.node();
    private static final String GOAL_KEY = MetricVerifier.key(GOAL, NODE);
    private static final String GUARD_KEY = MetricVerifier.key(GUARD, NODE);

    // goal: lower is better, must improve 10%; guard: higher is better, may worsen 20%
    private final VerifySpec spec = new VerifySpec(GOAL, NODE, true, 0.10, List.of(new VerifySpec.Guard(GUARD, NODE, false, 0.20)), 10_000, 5_000);
    private final Map<String, Double> baseline = Map.of(GOAL_KEY, 100.0, GUARD_KEY, 50.0);

    private Verdict verdict(Double goal, Double guard)
    {
        Map<String, Double> result = new java.util.HashMap<>();
        if (goal != null)
            result.put(GOAL_KEY, goal);
        if (guard != null)
            result.put(GUARD_KEY, guard);
        return MetricVerifier.verdict(spec, baseline, result);
    }

    @Test
    public void verdictTable()
    {
        assertEquals(Verdict.IMPROVED, verdict(90.0, 50.0));       // exactly minImprovement
        assertEquals(Verdict.IMPROVED, verdict(50.0, 41.0));       // guard worse, within tolerance
        assertEquals(Verdict.NEUTRAL, verdict(95.0, 50.0));        // improved, not enough
        assertEquals(Verdict.NEUTRAL, verdict(105.0, 50.0));       // worse, within minImprovement
        assertEquals(Verdict.REGRESSED, verdict(111.0, 50.0));     // goal worse by more than minImprovement
        assertEquals(Verdict.REGRESSED, verdict(50.0, 39.0));      // goal improved but guard beyond tolerance
        assertEquals(Verdict.INCONCLUSIVE, verdict(null, 50.0));   // goal samples missing
        assertEquals(Verdict.INCONCLUSIVE, verdict(50.0, null));   // guard samples missing
        assertEquals(Verdict.REGRESSED, verdict(null, 30.0));      // a regression that is visible wins over missing samples
    }

    @Test
    public void zeroBaseline()
    {
        Map<String, Double> zero = Map.of(GOAL_KEY, 0.0, GUARD_KEY, 50.0);
        assertEquals(Verdict.NEUTRAL, MetricVerifier.verdict(spec, zero, Map.of(GOAL_KEY, 0.0, GUARD_KEY, 50.0)));
        assertEquals(Verdict.REGRESSED, MetricVerifier.verdict(spec, zero, Map.of(GOAL_KEY, 1.0, GUARD_KEY, 50.0)));
    }

    @Test
    public void baselineNeedsMinSamplesOfEverySignal()
    {
        AtomicLong now = new AtomicLong(100_000);
        ObservationStore store = new ObservationStore(100, 100);
        InMemoryKnowledgeView view = new InMemoryKnowledgeView(store, now::get);
        for (int i = 1; i <= 3; i++)
            store.accept(new Observation(GOAL, NODE, now.get() - i * 1000, 100 + i));
        store.accept(new Observation(GUARD, NODE, now.get() - 1000, 50));
        store.accept(new Observation(GUARD, NODE, now.get() - 2000, 50));
        assertFalse(MetricVerifier.baseline(spec, view).isPresent());

        // a sample outside the baseline window does not count
        store.accept(new Observation(GUARD, NODE, now.get() - 20_000, 50));
        assertFalse(MetricVerifier.baseline(spec, view).isPresent());

        store.accept(new Observation(GUARD, NODE, now.get() - 500, 56));
        Map<String, Double> b = MetricVerifier.baseline(spec, view).get();
        assertEquals(102.0, b.get(GOAL_KEY), 1e-9);
        assertEquals(52.0, b.get(GUARD_KEY), 1e-9);
    }

    @Test
    public void baselineSinceShortensTheLookbackButNeverBeyondTheWindow()
    {
        AtomicLong now = new AtomicLong(100_000);
        ObservationStore store = new ObservationStore(100, 100);
        InMemoryKnowledgeView view = new InMemoryKnowledgeView(store, now::get);
        // calm before the problem (t = 91..95 s), then three samples of the problem
        for (int i = 91; i <= 95; i++)
        {
            store.accept(new Observation(GOAL, NODE, i * 1000L, 10));
            store.accept(new Observation(GUARD, NODE, i * 1000L, 10));
        }
        for (int i = 96; i <= 98; i++)
        {
            store.accept(new Observation(GOAL, NODE, i * 1000L, 100));
            store.accept(new Observation(GUARD, NODE, i * 1000L, 50));
        }
        Map<String, Double> trailing = MetricVerifier.baseline(spec, view).get();
        assertEquals(43.75, trailing.get(GOAL_KEY), 1e-9);

        Map<String, Double> since = MetricVerifier.baseline(spec.withBaselineSince(95_500), view).get();
        assertEquals("only the samples of the problem period", 100.0, since.get(GOAL_KEY), 1e-9);
        assertEquals(50.0, since.get(GUARD_KEY), 1e-9);

        // a start older than the window is capped at the window
        assertEquals(trailing, MetricVerifier.baseline(spec.withBaselineSince(0), view).get());
        // a start in the future or too recent leaves too few samples
        assertFalse(MetricVerifier.baseline(spec.withBaselineSince(97_500), view).isPresent());
        assertFalse(MetricVerifier.baseline(spec.withBaselineSince(200_000), view).isPresent());
        // the default is the trailing window
        assertEquals(VerifySpec.NO_BASELINE_SINCE, spec.baselineSinceMillis);
    }

    @Test
    public void baselineSinceIsPartOfTheSpecsIdentity()
    {
        VerifySpec a = spec.withBaselineSince(5);
        assertEquals(a, spec.withBaselineSince(5));
        assertEquals(a.hashCode(), spec.withBaselineSince(5).hashCode());
        assertFalse(a.equals(spec));
        assertFalse(a.equals(spec.withBaselineSince(6)));
        assertTrue(a.toString().contains("baselineSince=5"));
        assertFalse(spec.toString().contains("baselineSince"));
    }

    @Test
    public void compareUsesOnlyTheObservationWindow()
    {
        AtomicLong now = new AtomicLong(100_000);
        ObservationStore store = new ObservationStore(100, 100);
        InMemoryKnowledgeView view = new InMemoryKnowledgeView(store, now::get);
        // old, bad values before the window; good values inside it
        for (int i = 0; i < 3; i++)
        {
            store.accept(new Observation(GOAL, NODE, 90_000 + i, 500));
            store.accept(new Observation(GUARD, NODE, 90_000 + i, 1));
        }
        for (int i = 0; i < 3; i++)
        {
            store.accept(new Observation(GOAL, NODE, 96_000 + i * 1000, 80));
            store.accept(new Observation(GUARD, NODE, 96_000 + i * 1000, 50));
        }
        assertEquals(Verdict.IMPROVED, MetricVerifier.compare(spec, baseline, view));
        assertEquals(Verdict.INCONCLUSIVE, MetricVerifier.compare(null, baseline, view));
    }

    @Test
    public void earlyAbortAtTwiceTheTolerance()
    {
        AtomicLong now = new AtomicLong(100_000);
        ObservationStore store = new ObservationStore(100, 100);
        InMemoryKnowledgeView view = new InMemoryKnowledgeView(store, now::get);
        // regressed by 30%: beyond tolerance (20%), not beyond abort tolerance (40%)
        for (int i = 0; i < 3; i++)
            store.accept(new Observation(GUARD, NODE, 99_000 + i, 35));
        assertFalse(MetricVerifier.guardBreached(spec, baseline, view, 2_000).isPresent());
        // two samples are not enough to judge
        store.accept(new Observation(GUARD, NODE, 99_500, 1));
        store.accept(new Observation(GUARD, NODE, 99_600, 1));
        assertFalse(MetricVerifier.guardBreached(spec, baseline, view, 450).isPresent());
        // mean since apply now (35*3+2)/5 = 21.4: regressed by 57%
        assertTrue(MetricVerifier.guardBreached(spec, baseline, view, 2_000).isPresent());
    }

    // ADR-045: a latency guard (lower is better, 25%) with a traffic signal, floor 10 ops/s
    private static final SignalId TRAFFIC = SignalId.of("traffic");
    private static final String TRAFFIC_KEY = MetricVerifier.trafficKey(TRAFFIC, NODE);
    private final VerifySpec guarded = new VerifySpec(GOAL, NODE, true, 0.10,
                                                      List.of(new VerifySpec.Guard(GUARD, NODE, true, 0.25, TRAFFIC)), 10_000, 5_000);

    private Verdict guardedVerdict(VerifySpec s, double traffic, Double goal, Double guard)
    {
        Map<String, Double> base = Map.of(GOAL_KEY, 100.0, GUARD_KEY, 1000.0, TRAFFIC_KEY, traffic);
        Map<String, Double> result = new java.util.HashMap<>();
        if (goal != null)
            result.put(GOAL_KEY, goal);
        if (guard != null)
            result.put(GUARD_KEY, guard);
        return MetricVerifier.verdict(s, base, result);
    }

    @Test
    public void idleGuardIsNotJudged()
    {
        assertEquals(Verdict.IMPROVED, guardedVerdict(guarded, 0.5, 50.0, 2000.0));   // +100% p99 on an idle class
        assertEquals(Verdict.IMPROVED, guardedVerdict(guarded, 0.5, 50.0, null));     // and missing p99 is not missing
        assertEquals(Verdict.NEUTRAL, guardedVerdict(guarded, 0.5, 95.0, 2000.0));
        assertEquals(Verdict.REGRESSED, guardedVerdict(guarded, 0.5, 111.0, 1000.0)); // the goal is still judged
        assertEquals(Verdict.IMPROVED, guardedVerdict(guarded, 9.99, 50.0, 2000.0));
    }

    @Test
    public void busyGuardIsJudged()
    {
        assertEquals(Verdict.REGRESSED, guardedVerdict(guarded, 10.0, 50.0, 1300.0)); // at the floor: judged
        assertEquals(Verdict.REGRESSED, guardedVerdict(guarded, 500.0, 50.0, 1300.0));
        assertEquals(Verdict.IMPROVED, guardedVerdict(guarded, 500.0, 50.0, 1200.0));
        // busy before, idle after: the p99 is missing, so it cannot be judged
        assertEquals(Verdict.INCONCLUSIVE, guardedVerdict(guarded, 500.0, 50.0, null));
    }

    @Test
    public void guardWithoutTrafficSignalOrSamplesIsJudged()
    {
        VerifySpec plain = new VerifySpec(GOAL, NODE, true, 0.10, List.of(new VerifySpec.Guard(GUARD, NODE, true, 0.25)), 10_000, 5_000);
        assertEquals(Verdict.REGRESSED, guardedVerdict(plain, 0.0, 50.0, 2000.0));
        // a baseline without traffic samples: the guard is judged (fail-safe)
        Map<String, Double> noTraffic = Map.of(GOAL_KEY, 100.0, GUARD_KEY, 1000.0);
        assertEquals(Verdict.REGRESSED, MetricVerifier.verdict(guarded, noTraffic, Map.of(GOAL_KEY, 50.0, GUARD_KEY, 2000.0)));
        assertEquals(Verdict.INCONCLUSIVE, MetricVerifier.verdict(guarded, noTraffic, Map.of(GOAL_KEY, 50.0)));
    }

    @Test
    public void floorIsConfigurable()
    {
        assertEquals(Verdict.IMPROVED, guardedVerdict(guarded.withGuardMinOps(100), 50.0, 50.0, 2000.0));
        assertEquals(Verdict.REGRESSED, guardedVerdict(guarded.withGuardMinOps(1), 50.0, 50.0, 2000.0));
        assertEquals(Verdict.REGRESSED, guardedVerdict(guarded.withGuardMinOps(0), 0.0, 50.0, 2000.0));
        assertEquals(10.0, guarded.guardMinOpsPerSec, 0);
        assertEquals(100.0, guarded.withGuardMinOps(100).withBaselineSince(5).guardMinOpsPerSec, 0);
    }

    @Test
    public void baselineRecordsTheTrafficMeanWhereThereAreSamples()
    {
        AtomicLong now = new AtomicLong(100_000);
        ObservationStore store = new ObservationStore(100, 100);
        InMemoryKnowledgeView view = new InMemoryKnowledgeView(store, now::get);
        for (int i = 1; i <= 3; i++)
        {
            store.accept(new Observation(GOAL, NODE, now.get() - i * 1000, 100));
            store.accept(new Observation(GUARD, NODE, now.get() - i * 1000, 1000));
        }
        // no traffic samples: the baseline is complete and carries no traffic key
        Map<String, Double> b = MetricVerifier.baseline(guarded, view).get();
        assertEquals(2, b.size());
        // two traffic samples are too few
        store.accept(new Observation(TRAFFIC, NODE, now.get() - 1000, 4));
        store.accept(new Observation(TRAFFIC, NODE, now.get() - 2000, 6));
        assertFalse(MetricVerifier.baseline(guarded, view).get().containsKey(TRAFFIC_KEY));
        store.accept(new Observation(TRAFFIC, NODE, now.get() - 3000, 8));
        b = MetricVerifier.baseline(guarded, view).get();
        assertEquals(6.0, b.get(TRAFFIC_KEY), 1e-9);
        assertTrue(MetricVerifier.idle(guarded, guarded.guards.get(0), b));
        // means (the result map) does not carry traffic
        assertFalse(MetricVerifier.means(guarded, view, 10_000).containsKey(TRAFFIC_KEY));
    }

    @Test
    public void earlyAbortSkipsIdleGuards()
    {
        AtomicLong now = new AtomicLong(100_000);
        ObservationStore store = new ObservationStore(100, 100);
        InMemoryKnowledgeView view = new InMemoryKnowledgeView(store, now::get);
        for (int i = 0; i < 3; i++)
            store.accept(new Observation(GUARD, NODE, 99_000 + i, 3000));
        Map<String, Double> idle = Map.of(GOAL_KEY, 100.0, GUARD_KEY, 1000.0, TRAFFIC_KEY, 2.0);
        Map<String, Double> busy = Map.of(GOAL_KEY, 100.0, GUARD_KEY, 1000.0, TRAFFIC_KEY, 200.0);
        assertFalse(MetricVerifier.guardBreached(guarded, idle, view, 2_000).isPresent());
        assertTrue(MetricVerifier.guardBreached(guarded, busy, view, 2_000).isPresent());
    }

    @Test
    public void trafficSignalIsPartOfTheGuardsIdentity()
    {
        VerifySpec.Guard a = new VerifySpec.Guard(GUARD, NODE, true, 0.25, TRAFFIC);
        assertEquals(a, new VerifySpec.Guard(GUARD, NODE, true, 0.25, TRAFFIC));
        assertEquals(a.hashCode(), new VerifySpec.Guard(GUARD, NODE, true, 0.25, TRAFFIC).hashCode());
        assertFalse(a.equals(new VerifySpec.Guard(GUARD, NODE, true, 0.25)));
        assertTrue(a.toString().contains("traffic=" + TRAFFIC));
        assertFalse(new VerifySpec.Guard(GUARD, NODE, true, 0.25).toString().contains("traffic"));
        assertFalse(guarded.equals(guarded.withGuardMinOps(11)));
    }
}
