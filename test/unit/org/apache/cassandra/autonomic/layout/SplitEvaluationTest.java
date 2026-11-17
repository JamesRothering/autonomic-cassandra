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
        assertTrue(SplitEvaluation.stageA(b, window(1900, 500, 0.0, 500, 500), lc).ok);
        assertTrue("exactly 2x passes", SplitEvaluation.stageA(b, window(2000, 500, 0.0, 500, 500), lc).ok);
        SplitEvaluation.Result slow = SplitEvaluation.stageA(b, window(2100, 500, 0.0, 500, 500), lc);
        assertFalse(slow.ok);
        assertTrue(slow.detail, slow.detail.contains("read latency") && slow.detail.contains("above"));
        // errors: baseline + 1%
        assertTrue(SplitEvaluation.stageA(b, window(1000, 500, 0.01, 500, 500), lc).ok);
        SplitEvaluation.Result errors = SplitEvaluation.stageA(b, window(1000, 500, 0.02, 500, 500), lc);
        assertFalse(errors.ok);
        assertTrue(errors.detail, errors.detail.contains("error rate"));
        assertTrue(SplitEvaluation.stageA(base(1000, 500, 0.05, 1000, 1000, 0.6), window(1000, 500, 0.06, 500, 500), lc).ok);
        // the tolerance is configurable
        lc.read_regression_tolerance = 0.2;
        assertFalse(SplitEvaluation.stageA(b, window(1300, 500, 0.0, 500, 500), lc).ok);
        assertTrue(SplitEvaluation.stageA(b, window(1200, 500, 0.0, 500, 500), lc).ok);
    }

    @Test
    public void stageAIsNotJudgedWithoutDataAndDoesNotFail()
    {
        Baseline b = base(1000, 500, 0.0, 1000, 1000, 0.6);
        SplitEvaluation.Result noBaseline = SplitEvaluation.stageA(null, window(9999, 9999, 1, 500, 500), lc);
        assertTrue(noBaseline.ok);
        assertTrue(noBaseline.detail, noBaseline.detail.contains("no baseline"));
        // fewer than min_samples reads: the latency is not judged; the error rate needs min_samples operations
        assertTrue(SplitEvaluation.stageA(b, window(9999, 500, 0.0, 99, 0), lc).ok);
        SplitEvaluation.Result few = SplitEvaluation.stageA(b, window(9999, 9999, 1.0, 40, 40), lc);
        assertTrue(few.detail, few.ok && few.detail.contains("not judged"));
        // a baseline with too few reads cannot judge the read latency either
        assertTrue(SplitEvaluation.stageA(base(1000, 500, 0.0, 10, 1000, 0.6), window(9999, 500, 0.0, 500, 500), lc).ok);
        // no read latency in the window (a write-only key)
        assertTrue(SplitEvaluation.stageA(b, window(NAN, 500, 0.0, 0, 500), lc).ok);
    }

    // ---- stage B: guards, then the goal ----

    private Verdict stageB(Baseline b, Window w, double after)
    {
        return SplitEvaluation.stageB(b, w, after, lc).verdict;
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
        // guards: write latency within baseline + 25%, read latency within the stage A tolerance
        assertEquals(Verdict.IMPROVED, stageB(b, window(2000, 625, 0.0, 500, 500), 0.1));
        assertEquals(Verdict.REGRESSED, stageB(b, window(1500, 626, 0.0, 500, 500), 0.1));
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
        SplitEvaluation.Result r = SplitEvaluation.stageB(sizeOnly, window(1500, 500, 0.0, 500, 500), 0.01, lc);
        assertEquals(Verdict.INCONCLUSIVE, r.verdict);
        assertTrue(r.detail, r.detail.contains("partition_size_only"));
        assertEquals(Verdict.REGRESSED, SplitEvaluation.stageB(sizeOnly, window(5000, 500, 0.0, 500, 500), 0.01, lc).verdict);
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
