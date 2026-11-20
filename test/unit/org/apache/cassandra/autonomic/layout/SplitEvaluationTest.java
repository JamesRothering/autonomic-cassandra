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
package org.apache.cassandra.autonomic.layout;

import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.autonomic.AutonomicConfig;
import org.apache.cassandra.autonomic.execute.Verdict;
import org.apache.cassandra.autonomic.knowledge.InMemoryKnowledgeView;
import org.apache.cassandra.autonomic.knowledge.ObservationStore;
import org.apache.cassandra.autonomic.layout.SplitEvaluation.Baseline;
import org.apache.cassandra.autonomic.layout.SplitEvaluation.Window;
import org.apache.cassandra.autonomic.monitor.Observation;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.monitor.sensors.Signals;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** The stage A and stage B verdict tables of design 7.3. */
public class SplitEvaluationTest
{
    private static final double NAN = Double.NaN;
    private final AutonomicConfig.LayoutConfig lc = new AutonomicConfig().layout;

    private static Baseline base(double read, double write, double error, long reads, long writes, double share)
    {
        return new Baseline(read, write, error, reads, writes, share, false);
    }

    private static Window window(double read, double write, double error, long reads, long writes)
    {
        return new Window(read, write, error, reads, writes);
    }

    // ---- stage A: read latency within baseline * (1 + tolerance), error rate within baseline + 1% ----

    @Test
    public void stageATable()
    {
        Baseline b = base(1000, 500, 0.0, 1000, 1000, 0.6);
        // default tolerance 1.0: up to 2x
        assertTrue(SplitEvaluation.stageA(b, window(1900, 500, 0.0, 500, 500), null, 4, lc).ok);
        assertTrue("exactly 2x passes", SplitEvaluation.stageA(b, window(2000, 500, 0.0, 500, 500), null, 4, lc).ok);
        SplitEvaluation.Result slow = SplitEvaluation.stageA(b, window(2100, 500, 0.0, 500, 500), null, 4, lc);
        assertFalse(slow.ok);
        assertTrue(slow.detail, slow.detail.contains("read latency") && slow.detail.contains("above"));
        // errors: baseline + 1%
        assertTrue(SplitEvaluation.stageA(b, window(1000, 500, 0.01, 500, 500), null, 4, lc).ok);
        SplitEvaluation.Result errors = SplitEvaluation.stageA(b, window(1000, 500, 0.02, 500, 500), null, 4, lc);
        assertFalse(errors.ok);
        assertTrue(errors.detail, errors.detail.contains("error rate"));
        assertTrue(SplitEvaluation.stageA(base(1000, 500, 0.05, 1000, 1000, 0.6), window(1000, 500, 0.06, 500, 500), null, 4, lc).ok);
        // the tolerance is configurable
        lc.read_regression_tolerance = 0.2;
        assertFalse(SplitEvaluation.stageA(b, window(1300, 500, 0.0, 500, 500), null, 4, lc).ok);
        assertTrue(SplitEvaluation.stageA(b, window(1200, 500, 0.0, 500, 500), null, 4, lc).ok);
    }

    @Test
    public void stageAIsNotJudgedWithoutDataAndDoesNotFail()
    {
        Baseline b = base(1000, 500, 0.0, 1000, 1000, 0.6);
        SplitEvaluation.Result noBaseline = SplitEvaluation.stageA(null, window(9999, 9999, 1, 500, 500), null, 4, lc);
        assertTrue(noBaseline.ok);
        assertTrue(noBaseline.detail, noBaseline.detail.contains("no baseline"));
        // fewer than min_samples reads: the latency is not judged; the error rate needs min_samples operations
        assertTrue(SplitEvaluation.stageA(b, window(9999, 500, 0.0, 99, 0), null, 4, lc).ok);
        SplitEvaluation.Result few = SplitEvaluation.stageA(b, window(9999, 9999, 1.0, 40, 40), null, 4, lc);
        assertTrue(few.detail, few.ok && few.detail.contains("not judged"));
        // a baseline with too few reads cannot judge the read latency either
        assertTrue(SplitEvaluation.stageA(base(1000, 500, 0.0, 10, 1000, 0.6), window(9999, 500, 0.0, 500, 500), null, 4, lc).ok);
        // no read latency in the window (a write-only key)
        assertTrue(SplitEvaluation.stageA(b, window(NAN, 500, 0.0, 0, 500), null, 4, lc).ok);
    }

    // ---- the ANNOUNCED baseline and the C1 guard (T6.11 review) ----

    @Test
    public void theAnnouncedBaselineReplacesTheProfileButKeepsItsGoal()
    {
        Baseline proposed = new Baseline(1000, 500, 0.0, 1000, 1000, 0.6, true);
        Window announced = window(800, 400, 0.002, 70, 40);
        assertTrue(SplitEvaluation.sufficient(announced, lc));
        assertFalse(SplitEvaluation.sufficient(window(800, 400, 0.0, 60, 39), lc));
        Baseline b = Baseline.announced(announced, window(300, 200, NAN, 500, 400), 3000, proposed);
        assertEquals(800, b.readMicros, 0.0);
        assertEquals(400, b.writeMicros, 0.0);
        assertEquals(0.002, b.errorRate, 0.0);
        assertEquals(70, b.reads);
        assertEquals(0.6, b.opsShare, 0.0);
        assertTrue(b.sizeOnly);
        assertEquals(3000, b.tableWriteMicros, 0.0);
        // one claim state entry, and back
        Baseline decoded = Baseline.fromMap(Map.of(SplitEvaluation.S_BASELINE, b.encode()));
        assertEquals(b.toParams(), decoded.toParams());
        assertEquals("the rest of the table in ANNOUNCED", 300, b.rest.readMicros, 0.0);
        assertEquals(400, decoded.rest.writes);
        assertTrue(Double.isNaN(Baseline.announced(announced, Window.NONE, NAN, null).opsShare));
    }

    @Test
    public void c1GuardTable()
    {
        Baseline b = new Baseline(1000, 500, 0.0, 1000, 1000, 0.6, false, 2000);
        assertTrue(SplitEvaluation.guardC1(b, window(5000, 5000, 0.01, 500, 500), 6000, List.of(), lc).ok);
        SplitEvaluation.Result errors = SplitEvaluation.guardC1(b, window(1000, 500, 0.02, 500, 500), 2000, List.of(), lc);
        assertFalse(errors.ok);
        assertTrue(errors.detail, errors.detail.contains("error rate"));
        SplitEvaluation.Result table = SplitEvaluation.guardC1(b, window(1000, 500, 0.0, 500, 500), 6001, List.of(), lc);
        assertFalse(table.ok);
        assertTrue(table.detail, table.detail.contains("table write latency") && table.detail.contains("above"));
        SplitEvaluation.Result saturated = SplitEvaluation.guardC1(b, window(1000, 500, 0.0, 500, 500), 2000, List.of("pool_saturation"), lc);
        assertFalse(saturated.ok);
        assertTrue(saturated.detail, saturated.detail.contains("pool_saturation"));
        // not judged: no baseline, too few operations, no table latency
        assertTrue(SplitEvaluation.guardC1(null, null, NAN, List.of(), lc).ok);
        assertTrue(SplitEvaluation.guardC1(b, window(1000, 500, 1.0, 10, 10), NAN, List.of(), lc).ok);
        assertTrue(SplitEvaluation.guardC1(new Baseline(1000, 500, 0.0, 1000, 1000, 0.6, false), window(1000, 500, 0.0, 500, 500), 99999, List.of(), lc).ok);
    }

    /** An idle node: a baseline table latency of 0, or too few operations in its baseline, cannot fail the guard on latency (both directions share it). */
    @Test
    public void c1GuardDoesNotJudgeLatencyWithoutData()
    {
        // a zero baseline: any latency is "above 3 x 0"
        Baseline zero = new Baseline(0, 0, 0.0, 1000, 1000, 0.6, false, 0);
        SplitEvaluation.Result r = SplitEvaluation.guardC1(zero, window(1000, 500, 0.0, 500, 500), 800, List.of(), lc);
        assertTrue(r.detail, r.ok);
        assertFalse(r.detail, r.detail.contains("table write latency"));
        // too few writes in the baseline
        Baseline thin = new Baseline(1000, 500, 0.0, 1000, 10, 0.6, false, 2000);
        assertTrue(SplitEvaluation.guardC1(thin, window(1000, 500, 0.0, 500, 500), 99999, List.of(), lc).ok);
        Baseline b = new Baseline(1000, 500, 0.0, 1000, 1000, 0.6, false, 2000);
        // and it still fails where it is judged
        assertFalse(SplitEvaluation.guardC1(b, window(1000, 500, 0.0, 500, 500), 99999, List.of(), lc).ok);
    }

    @Test
    public void theMeanOfASignalCountsOnlyItsWindow()
    {
        long[] now = { 100_000 };
        ObservationStore store = new ObservationStore(16, 16);
        InMemoryKnowledgeView view = new InMemoryKnowledgeView(store, () -> now[0]);
        Subject table = Subject.table("ks", "t");
        store.accept(new Observation(Signals.TABLE_COORDINATOR_WRITE_LATENCY_P99_MICROS, table, 10_000, 9000));
        store.accept(new Observation(Signals.TABLE_COORDINATOR_WRITE_LATENCY_P99_MICROS, table, 50_000, 1000));
        store.accept(new Observation(Signals.TABLE_COORDINATOR_WRITE_LATENCY_P99_MICROS, table, 60_000, 3000));
        assertEquals(2000, SplitEvaluation.mean(view, Signals.TABLE_COORDINATOR_WRITE_LATENCY_P99_MICROS, table, 40_000, 100_000), 0.0);
        assertTrue(Double.isNaN(SplitEvaluation.mean(view, Signals.TABLE_COORDINATOR_WRITE_LATENCY_P99_MICROS, table, 70_000, 100_000)));
    }

    // ---- stage B: guards, then the goal ----

    private Verdict stageB(Baseline b, Window w, double after)
    {
        return SplitEvaluation.stageB(b, w, null, after, 4, lc).verdict;
    }

    @Test
    public void stageBVerdictTable()
    {
        Baseline b = base(1000, 500, 0.0, 1000, 1000, 0.6);
        Window fine = window(1500, 550, 0.0, 500, 500);
        // goal: the busiest share fell by at least 0.5 (0.6 -> 0.3)
        assertEquals(Verdict.IMPROVED, stageB(b, fine, 0.1));
        assertEquals("exactly the improvement required", Verdict.IMPROVED, stageB(b, fine, 0.3));
        assertEquals(Verdict.NEUTRAL, stageB(b, fine, 0.31));
        assertEquals(Verdict.NEUTRAL, stageB(b, fine, 0.6));
        assertEquals("a share that grew is not an improvement", Verdict.NEUTRAL, stageB(b, fine, 0.9));
        // guards: write latency within baseline + 50%, read latency within the stage A tolerance
        assertEquals(Verdict.IMPROVED, stageB(b, window(2000, 750, 0.0, 500, 500), 0.1));
        assertEquals(Verdict.REGRESSED, stageB(b, window(1500, 751, 0.0, 500, 500), 0.1));
        assertEquals(Verdict.REGRESSED, stageB(b, window(2001, 500, 0.0, 500, 500), 0.1));
        assertEquals("a breached guard wins over a missed goal", Verdict.REGRESSED, stageB(b, window(2001, 500, 0.0, 500, 500), 0.6));
        // a goal that cannot be judged
        assertEquals(Verdict.INCONCLUSIVE, stageB(b, fine, NAN));
        assertEquals(Verdict.INCONCLUSIVE, stageB(base(1000, 500, 0.0, 1000, 1000, NAN), fine, 0.1));
        assertEquals(Verdict.INCONCLUSIVE, stageB(null, fine, 0.1));
        assertEquals("a breached guard is known without the goal", Verdict.REGRESSED, stageB(b, window(3000, 500, 0.0, 500, 500), NAN));
        // a guard without data does not block: too few writes in the window
        assertEquals(Verdict.IMPROVED, stageB(b, window(1500, 5000, 0.0, 500, 99), 0.1));
    }

    @Test
    public void stageBForPartitionSizeOnlyDoesNotMeasureTheGoal()
    {
        Baseline sizeOnly = new Baseline(1000, 500, 0.0, 1000, 1000, 0.6, true);
        SplitEvaluation.Result r = SplitEvaluation.stageB(sizeOnly, window(1500, 500, 0.0, 500, 500), null, 0.01, 4, lc);
        assertEquals(Verdict.INCONCLUSIVE, r.verdict);
        assertTrue(r.detail, r.detail.contains("partition_size_only"));
        assertEquals(Verdict.REGRESSED, SplitEvaluation.stageB(sizeOnly, window(5000, 500, 0.0, 500, 500), null, 0.01, 4, lc).verdict);
    }

    @Test
    public void thresholdsFollowTheConfiguration()
    {
        Baseline b = base(1000, 500, 0.0, 1000, 1000, 0.6);
        lc.min_improvement = 0.9;
        assertEquals(Verdict.NEUTRAL, stageB(b, window(1500, 500, 0.0, 500, 500), 0.1));
        lc.min_improvement = 0.5;
        lc.min_samples = 1000;
        assertEquals("the guards are not judged below min_samples", Verdict.IMPROVED, stageB(b, window(9000, 9000, 0.0, 500, 500), 0.1));
    }

    // ---- ADR-040: the key against its expected overhead, the rest of the table against its baseline ----

    /** Before the split: the key reads and writes in 100us, the rest of the table in 400us, 1000 operations of each class. */
    private static Baseline split(double sliceFraction)
    {
        return new Baseline(100, 100, 0.0, 1000, 1000, 0.6, false, NAN, sliceFraction, window(400, 400, NAN, 1000, 1000));
    }

    @Test
    public void aGoodSplitPassesThoughTheKeyIsSlowerThanBefore()
    {
        // the key now costs what an operation of the rest of the table costs, plus routing: 4.5x its own baseline
        Window key = window(450, 450, 0.0, 500, 500);
        Window rest = window(420, 410, NAN, 500, 500);
        SplitEvaluation.Result b = SplitEvaluation.stageB(split(NAN), key, rest, 0.1, 8, lc);
        assertEquals(b.detail, Verdict.IMPROVED, b.verdict);
        for (String guard : new String[]{ "collateral read latency ok", "collateral write latency ok", "key read latency ok", "key write latency ok" })
            assertTrue(guard + ": " + b.detail, b.detail.contains(guard));
        assertFalse(b.detail, b.detail.contains("not judged"));
        assertTrue(b.detail, b.detail.contains("key write latency ok 450 within 615us (baseline 100us, reference 410us, 1 copy)"));
        // stage A, both copies written: the same numbers pass
        SplitEvaluation.Result a = SplitEvaluation.stageA(split(NAN), key, rest, 8, lc);
        assertTrue(a.detail, a.ok);
        assertTrue(a.detail, a.detail.contains("2 copies"));
    }

    @Test
    public void aSplitThatSlowsTheRestOfTheTableFails()
    {
        Window key = window(450, 450, 0.0, 500, 500);
        // collateral tolerance 0.5: the rest of the table may reach 600us
        assertEquals(Verdict.IMPROVED, SplitEvaluation.stageB(split(NAN), key, window(600, 600, NAN, 500, 500), 0.1, 8, lc).verdict);
        SplitEvaluation.Result reads = SplitEvaluation.stageB(split(NAN), key, window(700, 410, NAN, 500, 500), 0.1, 8, lc);
        assertEquals(reads.detail, Verdict.REGRESSED, reads.verdict);
        assertTrue(reads.detail, reads.detail.contains("collateral read latency 700 above 600us"));
        SplitEvaluation.Result writes = SplitEvaluation.stageA(split(NAN), key, window(420, 650, NAN, 500, 500), 8, lc);
        assertFalse(writes.detail, writes.ok);
        assertTrue(writes.detail, writes.detail.contains("collateral write latency 650 above 600us"));
        // the tolerance is configurable
        lc.collateral_tolerance = 0.1;
        assertEquals(Verdict.REGRESSED, SplitEvaluation.stageB(split(NAN), key, window(450, 410, NAN, 500, 500), 0.1, 8, lc).verdict);
    }

    @Test
    public void aSplitWhoseKeyWritesBlowUpFails()
    {
        Window rest = window(420, 410, NAN, 500, 500);
        // after the commit point one copy: up to 410 * 1.5 = 615us
        SplitEvaluation.Result b = SplitEvaluation.stageB(split(NAN), window(450, 620, 0.0, 500, 500), rest, 0.1, 8, lc);
        assertEquals(b.detail, Verdict.REGRESSED, b.verdict);
        assertTrue(b.detail, b.detail.contains("key write latency 620 above 615us"));
        // in CUTOVER two copies: up to 1230us
        assertTrue(SplitEvaluation.stageA(split(NAN), window(450, 1200, 0.0, 500, 500), rest, 8, lc).ok);
        SplitEvaluation.Result a = SplitEvaluation.stageA(split(NAN), window(450, 1300, 0.0, 500, 500), rest, 8, lc);
        assertFalse(a.detail, a.ok);
        assertTrue(a.detail, a.detail.contains("key write latency 1.30e+03 above 1.23e+03us"));
        // with no rest of the table in the window, the reference is the rest of the table in the baseline (400us)
        assertEquals(Verdict.REGRESSED, SplitEvaluation.stageB(split(NAN), window(100, 610, 0.0, 500, 500), null, 0.1, 8, lc).verdict);
        assertEquals(Verdict.IMPROVED, SplitEvaluation.stageB(split(NAN), window(100, 590, 0.0, 500, 500), null, 0.1, 8, lc).verdict);
    }

    @Test
    public void slicesOfTheKeyMayCostEveryBucket()
    {
        // a fifth of the reads are slices over 8 buckets: the reads may cost 1 + 0.2 * 7 = 2.4 times the reference, then 2x
        Window rest = window(420, 410, NAN, 500, 500);
        assertEquals(Verdict.IMPROVED, SplitEvaluation.stageB(split(0.2), window(2000, 450, 0.0, 500, 500), rest, 0.1, 8, lc).verdict);
        SplitEvaluation.Result r = SplitEvaluation.stageB(split(0.2), window(2100, 450, 0.0, 500, 500), rest, 0.1, 8, lc);
        assertEquals(r.detail, Verdict.REGRESSED, r.verdict);
        assertTrue(r.detail, r.detail.contains("fan-out 2.40"));
        assertEquals("point reads only: no fan-out allowance", Verdict.REGRESSED,
                     SplitEvaluation.stageB(split(0.0), window(900, 450, 0.0, 500, 500), rest, 0.1, 8, lc).verdict);
    }

    @Test
    public void eachGuardNeedsMinSamplesOfItsClass()
    {
        // too few operations of the rest of the table now: the collateral guard is not judged, and the key's reference is
        // the rest of the table in the baseline (400us)
        Window thinRest = window(9000, 9000, NAN, 99, 99);
        SplitEvaluation.Result r = SplitEvaluation.stageB(split(NAN), window(150, 580, 0.0, 500, 500), thinRest, 0.1, 8, lc);
        assertEquals(r.detail, Verdict.IMPROVED, r.verdict);
        assertTrue(r.detail, r.detail.contains("collateral read latency not judged (1000/99 ops, min 100)"));
        assertTrue(r.detail, r.detail.contains("key write latency ok 580 within 600us (baseline 100us, reference 400us"));
        // and with too few in the baseline as well, the key's own baseline
        Baseline thin = new Baseline(100, 100, 0.0, 1000, 1000, 0.6, false, NAN, NAN, window(400, 400, NAN, 99, 99));
        SplitEvaluation.Result k = SplitEvaluation.stageB(thin, window(150, 120, 0.0, 500, 500), thinRest, 0.1, 8, lc);
        assertTrue(k.detail, k.detail.contains("key write latency ok 120 within 150us (baseline 100us, reference 100us"));
        assertEquals(Verdict.REGRESSED, SplitEvaluation.stageB(thin, window(150, 160, 0.0, 500, 500), thinRest, 0.1, 8, lc).verdict);
        // a baseline without the rest of the table (taken before ADR-040) judges the key only
        Baseline old = new Baseline(100, 100, 0.0, 1000, 1000, 0.6, false);
        SplitEvaluation.Result o = SplitEvaluation.stageB(old, window(150, 120, 0.0, 500, 500), window(9000, 9000, NAN, 500, 500), 0.1, 8, lc);
        assertTrue(o.detail, o.detail.contains("collateral write latency not judged (0/500 ops"));
        assertEquals(o.detail, Verdict.IMPROVED, o.verdict);
    }

    // ---- the baseline travels as action parameters ----

    @Test
    public void aBaselineRoundTripsThroughParameters()
    {
        Baseline b = new Baseline(1234.5, NAN, 0.002, 400, 0, 0.55, true);
        Map<String, String> params = b.toParams();
        assertFalse("what was not measured is not written", params.containsKey(SplitEvaluation.P_WRITE_MICROS));
        assertTrue(params.keySet().stream().allMatch(k -> k.startsWith(SplitEvaluation.P_PREFIX)));
        Baseline back = Baseline.fromMap(params);
        assertEquals(1234.5, back.readMicros, 0);
        assertTrue(Double.isNaN(back.writeMicros));
        assertEquals(400, back.reads);
        assertEquals(0.55, back.opsShare, 0);
        assertTrue(back.sizeOnly);
        assertNull("no baseline in an operator's proposal", Baseline.fromMap(Map.of("buckets", "8", "mode", "split")));
        assertNotNull(Baseline.fromMap(Map.of(SplitEvaluation.P_OPS_SHARE, "0.4")));
        Baseline full = new Baseline(100, 200, 0.0, 300, 400, 0.5, false, 900, 0.125, window(410, 420, NAN, 430, 440));
        Baseline decoded = Baseline.fromMap(Map.of(SplitEvaluation.S_BASELINE, full.encode()));
        assertEquals(full.toParams(), decoded.toParams());
        assertEquals(0.125, decoded.sliceFraction, 0);
        assertEquals(420, decoded.rest.writeMicros, 0);
        assertEquals(430, decoded.rest.reads);
        // the policy's profile gives the slice fraction of the key's reads
        LayoutProfiler.Shape profile = new LayoutProfiler.Shape(30, 10, 60, 0, 0, 0, 40, 4000, 60, 6000, 0, 0);
        assertEquals(0.25, Baseline.of(profile, 0.6, false).sliceFraction, 0);
    }

    // ---- the windows come from the signals of LayoutSensor ----

    @Test
    public void aWindowWeighsTheMeansByTheirOperationCounts()
    {
        long[] now = { 100_000 };
        ObservationStore store = new ObservationStore(64, 64);
        InMemoryKnowledgeView view = new InMemoryKnowledgeView(store, () -> now[0]);
        Subject key = Subject.partition("ks", "t", "00ff");
        put(store, Signals.KEY_READ_LATENCY_MICROS, key, 10_000, 100);
        put(store, Signals.KEY_READ_OPS_DELTA, key, 10_000, 100);
        put(store, Signals.KEY_READ_LATENCY_MICROS, key, 20_000, 400);
        put(store, Signals.KEY_READ_OPS_DELTA, key, 20_000, 300);
        put(store, Signals.KEY_WRITE_OPS_DELTA, key, 20_000, 50);
        put(store, Signals.KEY_ERROR_RATE, key, 20_000, 0.1);
        put(store, Signals.KEY_READ_OPS_DELTA, key, 90_000, 1000);
        put(store, Signals.KEY_READ_LATENCY_MICROS, key, 90_000, 5);

        Window w = Window.of(view, key, 0, 30_000);
        assertEquals((100 * 100 + 400 * 300) / 400.0, w.readMicros, 1e-9);
        assertEquals(400, w.reads);
        assertEquals(50, w.writes);
        assertTrue(Double.isNaN(w.writeMicros));
        assertEquals(0.1, w.errorRate, 1e-9);
        assertEquals("a later point is outside the window", 1400, Window.of(view, key, 0, 100_000).reads);
        assertEquals(0, Window.of(view, key, 50_000, 60_000).reads);
        assertTrue(Double.isNaN(Window.of(view, key, 50_000, 60_000).readMicros));
    }

    @Test
    public void theBusiestShareIsTheLargestOfTheBaseKeyAndTheBuckets()
    {
        long[] now = { 100_000 };
        ObservationStore store = new ObservationStore(64, 64);
        InMemoryKnowledgeView view = new InMemoryKnowledgeView(store, () -> now[0]);
        Subject base = Subject.partition("ks", "t", "00ff");
        Subject bucketA = Subject.partition("ks", "t_overflow", "aa");
        Subject bucketB = Subject.partition("ks", "t_overflow", "bb");
        Subject other = Subject.partition("ks", "other", "cc");
        assertTrue(Double.isNaN(SplitEvaluation.busiestShare(view, base, "ks", "t_overflow", 60_000)));
        put(store, Signals.PARTITION_READ_SHARE, bucketA, 90_000, 0.2);
        put(store, Signals.PARTITION_WRITE_SHARE, bucketB, 90_000, 0.25);
        put(store, Signals.PARTITION_READ_SHARE, other, 90_000, 0.9);
        assertEquals(0.25, SplitEvaluation.busiestShare(view, base, "ks", "t_overflow", 60_000), 0);
        put(store, Signals.PARTITION_READ_SHARE, base, 95_000, 0.4);
        assertEquals(0.4, SplitEvaluation.busiestShare(view, base, "ks", "t_overflow", 60_000), 0);
        assertEquals("the base key's sample is the newest", 0.4, SplitEvaluation.busiestShare(view, base, "ks", "t_overflow", 10_000), 0);
        assertTrue("samples older than the window are not seen", Double.isNaN(SplitEvaluation.busiestShare(view, base, "ks", "t_overflow", 4_000)));
        assertTrue(Double.isNaN(SplitEvaluation.busiestShare(view, Subject.partition("ks", "t", "dd"), "ks", "nothing", 1000)));
    }

    private static void put(ObservationStore store, SignalId signal, Subject subject, long time, double value)
    {
        store.accept(new Observation(signal, subject, time, value));
    }
}
