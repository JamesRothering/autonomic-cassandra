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
package org.apache.cassandra.autonomic.analyze;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.autonomic.knowledge.InMemoryKnowledgeView;
import org.apache.cassandra.autonomic.knowledge.KnowledgeView;
import org.apache.cassandra.autonomic.knowledge.ObservationStore;
import org.apache.cassandra.autonomic.monitor.Observation;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

public class ThresholdDetectorTest
{
    private static final SignalId SIG = SignalId.of("test.load");
    private static final Subject T1 = Subject.table("ks", "t1");
    private static final Subject T2 = Subject.table("ks", "t2");
    private static final long TICK = 15_000;

    private ObservationStore store;
    private long now;
    private InMemoryKnowledgeView view;

    @Before
    public void setUp()
    {
        store = new ObservationStore(16, 100);
        now = 1_000_000;
        view = new InMemoryKnowledgeView(store, () -> now);
    }

    /** Raise 100, clear 80, for_evaluations 3, WARN, CRITICAL from 200; adds extra evidence. */
    private static class Fixture extends ThresholdDetector
    {
        Fixture(ThresholdDetector.Settings s)
        {
            super("test_detector", "test_condition", SIG, s);
        }

        @Override
        protected String summary(Subject subject, double value)
        {
            return subject + " at " + value;
        }

        @Override
        protected Severity severity(Subject subject, double value, KnowledgeView knowledge)
        {
            return value >= 200 ? Severity.CRITICAL : Severity.WARN;
        }

        @Override
        protected void addEvidence(Subject subject, double value, Map<String, String> evidence, KnowledgeView knowledge)
        {
            super.addEvidence(subject, value, evidence, knowledge);
            evidence.put("key_hex", "cafe");
        }
    }

    private static ThresholdDetector.Settings settings(int n)
    {
        return new ThresholdDetector.Settings(100, 80, n, true, 60_000, 1000);
    }

    /** Feeds one value per subject then runs one evaluation one tick later. */
    private List<Diagnosis> tick(ThresholdDetector d, Object... subjectValues)
    {
        now += TICK;
        for (int i = 0; i < subjectValues.length; i += 2)
            store.accept(new Observation(SIG, (Subject) subjectValues[i], now, ((Number) subjectValues[i + 1]).doubleValue()));
        return d.analyze(view);
    }

    @Test
    public void opensOnlyAfterForEvaluationsConsecutiveBreaches()
    {
        ThresholdDetector d = new Fixture(settings(3));
        assertTrue(tick(d, T1, 150).isEmpty());
        assertTrue(tick(d, T1, 150).isEmpty());
        List<Diagnosis> out = tick(d, T1, 150);
        assertEquals(1, out.size());
        assertEquals("test_condition", out.get(0).type);
        assertEquals(T1, out.get(0).subject);
        assertEquals(now, out.get(0).firstSeenMillis);
    }

    @Test
    public void oneSampleReReadOnSeveralTicksCountsOnce()
    {
        ThresholdDetector d = new Fixture(settings(4));
        store.accept(new Observation(SIG, T1, now, 150));
        for (int i = 0; i < 3; i++)
        {
            now += TICK;
            assertTrue(d.analyze(view).isEmpty());
        }
        assertEquals(1, d.trackedSubjects()); // still pending, not dropped while the sample is fresh
        // three more distinct samples complete the four
        assertTrue(tick(d, T1, 150).isEmpty());
        assertTrue(tick(d, T1, 150).isEmpty());
        assertEquals(1, tick(d, T1, 150).size());
    }

    @Test
    public void repeatedSampleDoesNotClearButRefreshesLastSeen()
    {
        ThresholdDetector d = new Fixture(settings(2));
        tick(d, T1, 150);
        tick(d, T1, 150);
        tick(d, T1, 10); // one clearing sample
        for (int i = 0; i < 3; i++)
        {
            now += TICK;
            List<Diagnosis> out = d.analyze(view);
            assertEquals(1, out.size());
            assertEquals(now, out.get(0).lastSeenMillis);
        }
        assertTrue(tick(d, T1, 10).isEmpty()); // second distinct clearing sample
    }

    @Test
    public void breachSinceIsTheFirstBreachingCheckNotTheConfirmingOne()
    {
        ThresholdDetector d = new Fixture(settings(3));
        tick(d, T1, 150);
        long first = now;
        tick(d, T1, 150);
        Diagnosis opened = tick(d, T1, 150).get(0);
        assertEquals(Long.toString(first), opened.evidence.get(ThresholdDetector.BREACH_SINCE));
        assertEquals(now, opened.firstSeenMillis);
        assertEquals(first, Long.parseLong(opened.evidence.get(ThresholdDetector.BREACH_SINCE)));
        // it stays for the open diagnosis
        assertEquals(Long.toString(first), tick(d, T1, 150).get(0).evidence.get(ThresholdDetector.BREACH_SINCE));
        assertEquals(Long.toString(first), tick(d, T1, 90).get(0).evidence.get(ThresholdDetector.BREACH_SINCE));
    }

    @Test
    public void aNewRunOfBreachesAfterAClearStartsFresh()
    {
        ThresholdDetector d = new Fixture(settings(2));
        tick(d, T1, 150);
        assertEquals(1, tick(d, T1, 150).size());
        tick(d, T1, 10);
        assertTrue(tick(d, T1, 10).isEmpty());
        tick(d, T1, 150);
        long second = now;
        Diagnosis again = tick(d, T1, 150).get(0);
        assertEquals(Long.toString(second), again.evidence.get(ThresholdDetector.BREACH_SINCE));

        // a run broken by a calm check before it opens also starts again
        ThresholdDetector e = new Fixture(settings(3));
        tick(e, T2, 150);
        tick(e, T2, 150);
        tick(e, T2, 10);
        tick(e, T2, 150);
        long start = now;
        tick(e, T2, 150);
        assertEquals(Long.toString(start), tick(e, T2, 150).get(0).evidence.get(ThresholdDetector.BREACH_SINCE));
    }

    @Test
    public void exactlyAtRaiseThresholdBreaches()
    {
        ThresholdDetector d = new Fixture(settings(1));
        assertEquals(1, tick(d, T1, 100).size());
    }

    @Test
    public void singleNonBreachingEvaluationResetsCount()
    {
        ThresholdDetector d = new Fixture(settings(3));
        tick(d, T1, 150);
        tick(d, T1, 150);
        assertTrue(tick(d, T1, 99).isEmpty());
        assertTrue(tick(d, T1, 150).isEmpty());
        assertTrue(tick(d, T1, 150).isEmpty());
        assertEquals(1, tick(d, T1, 150).size());
    }

    @Test
    public void flappingInputNeverOpens()
    {
        ThresholdDetector d = new Fixture(settings(3));
        for (int i = 0; i < 50; i++)
        {
            assertTrue(tick(d, T1, 150).isEmpty());
            assertTrue(tick(d, T1, 150).isEmpty());
            assertTrue(tick(d, T1, 10).isEmpty());
        }
        assertEquals(0, d.trackedSubjects());
    }

    @Test
    public void staysOpenBetweenClearAndRaiseAndClearsOnlyBelowClear()
    {
        ThresholdDetector d = new Fixture(settings(2));
        tick(d, T1, 150);
        Diagnosis opened = tick(d, T1, 150).get(0);
        // below raise but not below clear: stays open indefinitely
        for (int i = 0; i < 10; i++)
            assertEquals(1, tick(d, T1, 90).size());
        // exactly at clear is not below it
        for (int i = 0; i < 10; i++)
            assertEquals(1, tick(d, T1, 80).size());
        // below clear needs for_evaluations consecutive evaluations
        assertEquals(1, tick(d, T1, 79).size());
        assertTrue(tick(d, T1, 79).isEmpty());
        assertEquals(0, d.trackedSubjects());
        assertEquals(opened.type, "test_condition");
    }

    @Test
    public void clearCountResetsWhenValueRisesAgain()
    {
        ThresholdDetector d = new Fixture(settings(2));
        tick(d, T1, 150);
        tick(d, T1, 150);
        assertEquals(1, tick(d, T1, 50).size());
        assertEquals(1, tick(d, T1, 90).size()); // in band: resets clearing
        assertEquals(1, tick(d, T1, 50).size());
        assertTrue(tick(d, T1, 50).isEmpty());
    }

    @Test
    public void openDiagnosisKeepsIdAndFirstSeenAndUpdatesLastSeenSeverityEvidence()
    {
        ThresholdDetector d = new Fixture(settings(2));
        tick(d, T1, 150);
        Diagnosis a = tick(d, T1, 150).get(0);
        Diagnosis b = tick(d, T1, 250).get(0);
        assertEquals(a.id, b.id);
        assertEquals(a.firstSeenMillis, b.firstSeenMillis);
        assertEquals(now, b.lastSeenMillis);
        assertEquals(Severity.WARN, a.severity);
        assertEquals(Severity.CRITICAL, b.severity);
        assertEquals("cafe", b.evidence.get("key_hex"));
        assertEquals("250 (raise 100, clear 80)", b.evidence.get(SIG.name));
        assertTrue(b.summary.contains("250"));
        assertEquals(1.0, b.confidence, 0.0);
    }

    @Test
    public void reopenedDiagnosisGetsNewId()
    {
        ThresholdDetector d = new Fixture(settings(1));
        Diagnosis a = tick(d, T1, 150).get(0);
        assertTrue(tick(d, T1, 10).isEmpty());
        Diagnosis b = tick(d, T1, 150).get(0);
        assertNotEquals(a.id, b.id);
    }

    @Test
    public void subjectsAreIndependent()
    {
        ThresholdDetector d = new Fixture(settings(2));
        assertTrue(tick(d, T1, 150, T2, 10).isEmpty());
        List<Diagnosis> out = tick(d, T1, 150, T2, 150);
        assertEquals(1, out.size());
        assertEquals(T1, out.get(0).subject);
        out = tick(d, T1, 150, T2, 150);
        assertEquals(2, out.size());
        // T1 clears while T2 stays
        tick(d, T1, 10, T2, 150);
        out = tick(d, T1, 10, T2, 150);
        assertEquals(1, out.size());
        assertEquals(T2, out.get(0).subject);
    }

    @Test
    public void staleSignalResetsPendingCount()
    {
        ThresholdDetector d = new Fixture(settings(2));
        tick(d, T1, 150);
        now += 120_000; // no new observation; the last point is older than max_signal_age
        assertTrue(d.analyze(view).isEmpty());
        assertEquals(0, d.trackedSubjects());
        assertTrue(tick(d, T1, 150).isEmpty());
        assertEquals(1, tick(d, T1, 150).size());
    }

    @Test
    public void missingSignalClearsOpenDiagnosisAfterForEvaluations()
    {
        ThresholdDetector d = new Fixture(settings(2));
        tick(d, T1, 150);
        Diagnosis open = tick(d, T1, 150).get(0);
        now += 120_000; // all points stale
        List<Diagnosis> held = d.analyze(view);
        assertEquals(1, held.size());
        assertEquals(open.id, held.get(0).id);
        now += TICK;
        assertTrue(d.analyze(view).isEmpty());
        assertEquals(0, d.trackedSubjects());
    }

    @Test
    public void vanishedSubjectDoesNotLeakState()
    {
        ThresholdDetector d = new Fixture(settings(3));
        tick(d, T1, 150);
        assertEquals(1, d.trackedSubjects());
        now += 120_000;
        d.analyze(view);
        assertEquals(0, d.trackedSubjects());
    }

    @Test
    public void stateCapDropsPendingSubjectsButNeverOpenOnes()
    {
        ThresholdDetector d = new Fixture(new ThresholdDetector.Settings(100, 80, 2, true, 60_000, 2));
        Subject a = Subject.table("ks", "a");
        Subject b = Subject.table("ks", "b");
        Subject c = Subject.table("ks", "c");
        tick(d, a, 150, b, 150);
        assertEquals(2, tick(d, a, 150, b, 150).size()); // both open, cap reached
        assertTrue(tick(d, a, 150, b, 150, c, 150).stream().noneMatch(x -> x.subject.equals(c)));
        assertEquals(2, d.trackedSubjects());

        // with a pending subject at the cap, the oldest pending one is evicted for a new one
        ThresholdDetector e = new Fixture(new ThresholdDetector.Settings(100, 80, 3, true, 60_000, 2));
        tick(e, a, 150);
        tick(e, a, 150, b, 150);
        tick(e, a, 150, b, 150, c, 150);
        assertEquals(2, e.trackedSubjects());
    }

    @Test
    public void lowerIsWorseMirrorsThresholds()
    {
        ThresholdDetector d = new Fixture(new ThresholdDetector.Settings(20, 25, 2, false, 60_000, 10));
        assertTrue(tick(d, T1, 20).isEmpty());
        assertEquals(1, tick(d, T1, 5).size());
        assertEquals(1, tick(d, T1, 22).size()); // not above clear yet
        assertEquals(1, tick(d, T1, 30).size());
        assertTrue(tick(d, T1, 30).isEmpty());
    }

    @Test
    public void overriddenMeasureAndFailingHookDoNotThrowIntoLoop()
    {
        ThresholdDetector bad = new Fixture(settings(1))
        {
            @Override
            protected OptionalDouble measure(Subject subject, KnowledgeView knowledge)
            {
                if (subject.equals(T1))
                    throw new IllegalStateException("boom");
                return OptionalDouble.of(500);
            }
        };
        tick(bad, T1, 150, T2, 150);
        List<Diagnosis> out = bad.analyze(view);
        assertEquals(1, out.size());
        assertEquals(T2, out.get(0).subject);
    }

    @Test
    public void settingsFromConfigUsesDefaultsAndValidates()
    {
        Map<String, Object> opts = new HashMap<>();
        opts.put("share_of_table_ops", 0.3);
        opts.put("for_evaluations", 4);
        ThresholdDetector.Settings s = ThresholdDetector.Settings.fromConfig(opts, "share_of_table_ops", 0.5);
        assertEquals(0.3, s.raise, 0.0);
        assertEquals(0.24, s.clear, 1e-12);
        assertEquals(4, s.forEvaluations);

        opts.put("clear_threshold", "0.1");
        opts.put("for_evaluations", 7);
        s = ThresholdDetector.Settings.fromConfig(opts, "share_of_table_ops", 0.5);
        assertEquals(0.1, s.clear, 0.0);
        assertEquals(7, s.forEvaluations);

        // clear above raise is invalid: fall back to defaults
        opts.put("clear_threshold", 0.9);
        s = ThresholdDetector.Settings.fromConfig(opts, "share_of_table_ops", 0.5);
        assertEquals(0.5, s.raise, 0.0);
        assertEquals(0.4, s.clear, 1e-12);
        assertEquals(4, s.forEvaluations);

        s = ThresholdDetector.Settings.fromConfig(new HashMap<>(), "x", 10);
        assertEquals(8, s.clear, 1e-12);
    }
}
