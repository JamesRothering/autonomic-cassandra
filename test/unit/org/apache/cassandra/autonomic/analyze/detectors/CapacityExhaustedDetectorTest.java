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
package org.apache.cassandra.autonomic.analyze.detectors;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.autonomic.analyze.Diagnosis;
import org.apache.cassandra.autonomic.analyze.Severity;
import org.apache.cassandra.autonomic.analyze.ThresholdDetector;
import org.apache.cassandra.autonomic.knowledge.InMemoryKnowledgeView;
import org.apache.cassandra.autonomic.knowledge.ObservationStore;
import org.apache.cassandra.autonomic.monitor.Observation;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.monitor.sensors.Signals;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/** capacity_exhausted with hand-fed signals and a hand-held ladder: saturation, disk trend, flapping, missing signal. */
public class CapacityExhaustedDetectorTest
{
    private static final long TICK = 15_000;
    private static final long SAMPLE = 30_000;
    private static final Subject NODE = Subject.node();
    private static final Subject READ = Subject.threadPool("ReadStage");

    private ObservationStore store;
    private long now;
    private InMemoryKnowledgeView view;
    /** What the ladder answers for the saturated pool; null while it still has something to propose. */
    private String exhausted;
    private CapacityExhaustedDetector detector;
    private double used;

    @Before
    public void setUp()
    {
        store = new ObservationStore(200, 1000);
        now = 100_000_000;
        view = new InMemoryKnowledgeView(store, () -> now);
        exhausted = null;
        detector = CapacityExhaustedDetector.fromConfig(Map.of(), (lead, k) -> Optional.ofNullable(exhausted));
        used = 0.60;
    }

    private Diagnosis saturated(Subject pool, Severity severity, UUID rootCause)
    {
        return new Diagnosis(UUID.randomUUID(), PoolSaturationDetector.NAME, pool, severity, 1, now, now, "", Map.of(), rootCause);
    }

    private List<Diagnosis> tick()
    {
        now += TICK;
        return detector.analyze(view);
    }

    /** One disk sample growing by the given fraction per hour, then one evaluation. */
    private List<Diagnosis> sample(double perHour)
    {
        now += SAMPLE;
        used += perHour * SAMPLE / 3_600_000.0;
        store.accept(new Observation(Signals.DISK_USED_FRACTION, NODE, now, used));
        return detector.analyze(view);
    }

    @Test
    public void defaults()
    {
        ThresholdDetector.Settings s = detector.settings();
        assertEquals(1.0, s.raise, 0);
        assertEquals(0.8, s.clear, 1e-9);
        assertEquals("sustained: twice the usual persistence", 8, s.forEvaluations);
        assertEquals(3, CapacityExhaustedDetector.fromConfig(Map.of("for_evaluations", 3), null).settings().forEvaluations);
        assertEquals("capacity_exhausted", detector.name());
    }

    // ---- (a) saturation with nothing left in the node ----

    @Test
    public void saturationWithEveryInNodeStepExhaustedOpensAfterItPersists()
    {
        view.setActiveDiagnoses(List.of(saturated(READ, Severity.CRITICAL, null)));
        store.accept(new Observation(Signals.POOL_PENDING_TASKS, READ, now, 400));
        exhausted = "yield_background: exhausted, resize_request_pools: exhausted, shed_load: ineffective";
        for (int i = 0; i < 7; i++)
            assertTrue("evaluation " + i, tick().isEmpty());
        List<Diagnosis> open = tick();
        assertEquals(1, open.size());
        Diagnosis d = open.get(0);
        assertEquals("capacity_exhausted", d.type);
        assertEquals(READ, d.subject);
        assertEquals("the severity of the saturation", Severity.CRITICAL, d.severity);
        assertEquals("saturation", d.evidence.get("trigger"));
        assertEquals("ReadStage", d.evidence.get("pool"));
        assertEquals(exhausted, d.evidence.get("in_node_steps"));
        assertEquals("400", d.evidence.get("pool.pending_tasks"));
        assertTrue(d.summary, d.summary.contains("ReadStage"));

        // it holds while the condition does and clears only after as many evaluations without it
        UUID id = d.id;
        assertEquals(id, tick().get(0).id);
        view.setActiveDiagnoses(List.of());
        for (int i = 0; i < 7; i++)
            assertEquals(id, tick().get(0).id);
        assertTrue(tick().isEmpty());
    }

    @Test
    public void saturationIsNotCapacityExhaustionWhileTheNodeStillHasAnAnswer()
    {
        view.setActiveDiagnoses(List.of(saturated(READ, Severity.CRITICAL, null)));
        for (int i = 0; i < 20; i++)
            assertTrue(tick().isEmpty());
        // nor is a pool whose saturation is the effect of something else, or no saturation at all
        exhausted = "yield_background: exhausted";
        view.setActiveDiagnoses(List.of(saturated(READ, Severity.CRITICAL, UUID.randomUUID())));
        for (int i = 0; i < 20; i++)
            assertTrue(tick().isEmpty());
        view.setActiveDiagnoses(List.of());
        for (int i = 0; i < 20; i++)
            assertTrue(tick().isEmpty());
    }

    @Test
    public void flappingExhaustionNeverOpens()
    {
        view.setActiveDiagnoses(List.of(saturated(READ, Severity.WARN, null)));
        for (int i = 0; i < 60; i++)
        {
            // seven evaluations exhausted, then one where a knob has room again
            exhausted = i % 8 == 7 ? null : "yield_background: exhausted";
            assertTrue("evaluation " + i, tick().isEmpty());
        }
    }

    @Test
    public void eachSaturatedPoolIsItsOwnSubject()
    {
        Subject write = Subject.threadPool("MutationStage");
        view.setActiveDiagnoses(List.of(saturated(READ, Severity.WARN, null), saturated(write, Severity.WARN, null)));
        exhausted = "resize_request_pools: exhausted";
        List<Diagnosis> open = List.of();
        for (int i = 0; i < 8; i++)
            open = tick();
        assertEquals(2, open.size());
        assertEquals(List.of(READ, write), List.of(open.get(0).subject, open.get(1).subject));
    }

    // ---- (b) disk trend ----

    @Test
    public void diskTrendThatReachesFullWithinTheHorizonOpens()
    {
        // 0.60 growing 5% an hour: 0.90 in six hours, the horizon is twelve
        List<Diagnosis> open = List.of();
        int samples = 0;
        while (open.isEmpty() && samples < 200)
        {
            open = sample(0.05);
            samples++;
        }
        // half the 30 minute window must be covered before a trend exists, then it must persist for 8 samples
        assertEquals(31 + 7, samples);
        Diagnosis d = open.get(0);
        assertEquals(NODE, d.subject);
        assertEquals("disk_trend", d.evidence.get("trigger"));
        assertEquals(Severity.CRITICAL, d.severity);
        assertEquals(0.05, Double.parseDouble(d.evidence.get("used_fraction_per_hour")), 1e-3);
        assertEquals(5.7, Double.parseDouble(d.evidence.get("hours_to_full")), 0.1);
        assertEquals("12", d.evidence.get("horizon_hours"));
        assertEquals("0.9000", d.evidence.get("full_fraction"));
        assertNotNull(d.evidence.get("disk.used_fraction"));
        assertTrue(d.evidence.get("samples"), d.evidence.get("samples").contains("over 1800000ms"));
        assertTrue(d.summary, d.summary.contains("90% full in 5."));

        // a re-read of the same sample is not a new evaluation, and growth that stops clears it after 8 samples
        UUID id = d.id;
        assertEquals(id, tick().get(0).id);
        int cleared = 0;
        while (!sample(0).isEmpty())
            cleared++;
        assertTrue("the window has to flatten first: " + cleared, cleared > 8 && cleared < 80);
    }

    @Test
    public void slowGrowthAnEmptyDiskOrFewSamplesAreNotATrend()
    {
        // 1% an hour from 0.60: thirty hours to 0.90
        for (int i = 0; i < 100; i++)
            assertTrue(sample(0.01).isEmpty());

        // fast growth on a disk that is nearly empty
        setUp();
        used = 0.10;
        for (int i = 0; i < 100; i++)
            assertTrue(sample(0.05).isEmpty());

        // a shrinking disk
        setUp();
        used = 0.85;
        for (int i = 0; i < 100; i++)
            assertTrue(sample(-0.05).isEmpty());

        // too few points for a line, however steep
        setUp();
        detector = CapacityExhaustedDetector.fromConfig(Map.of("min_samples", 500), null);
        for (int i = 0; i < 100; i++)
            assertTrue(sample(0.2).isEmpty());
    }

    @Test
    public void sawtoothDiskUsageDoesNotOpen()
    {
        // compaction: usage climbs steeply for five minutes, then falls back; the mean slope is flat
        for (int i = 0; i < 300; i++)
        {
            boolean climbing = i % 20 < 10;
            assertTrue("sample " + i, sample(climbing ? 0.6 : -0.6).isEmpty());
        }
    }

    @Test
    public void missingOrStaleDiskSignalIsNoMeasurement()
    {
        for (int i = 0; i < 20; i++)
            assertTrue(tick().isEmpty());
        assertEquals(0, detector.trackedSubjects());

        List<Diagnosis> open = List.of();
        for (int i = 0; i < 60 && open.isEmpty(); i++)
            open = sample(0.05);
        assertEquals(1, open.size());
        // the sensor stops: the diagnosis is kept for a while, then cleared for lack of a measurement
        now += 121_000;
        int kept = 0;
        while (!tick().isEmpty())
            kept++;
        assertEquals(7, kept);
    }

    @Test
    public void aFullDiskIsExhaustedWhateverTheSlope()
    {
        used = 0.93;
        List<Diagnosis> open = List.of();
        for (int i = 0; i < 60 && open.isEmpty(); i++)
            open = sample(0);
        assertEquals(1, open.size());
        assertEquals(Severity.CRITICAL, open.get(0).severity);
        assertEquals("0", open.get(0).evidence.get("hours_to_full"));
    }

    @Test
    public void optionsOfTheTrend()
    {
        detector = CapacityExhaustedDetector.fromConfig(Map.of("horizon_hours", 2, "full_fraction", 0.95, "trend_window_ms", 600_000,
                                                               "min_samples", 5, "min_used_fraction", 0.3, "for_evaluations", 2), null);
        // 5% an hour from 0.60 reaches 0.95 in seven hours: outside a two hour horizon
        for (int i = 0; i < 60; i++)
            assertTrue(sample(0.05).isEmpty());
        // 20% an hour reaches it in under two
        List<Diagnosis> open = List.of();
        for (int i = 0; i < 40 && open.isEmpty(); i++)
            open = sample(0.2);
        assertEquals(1, open.size());
        assertEquals("2", open.get(0).evidence.get("horizon_hours"));
        // malformed options fall back to the defaults instead of failing the layer
        assertNotNull(CapacityExhaustedDetector.fromConfig(Map.of("horizon_hours", "soon", "min_samples", 0), null));
    }
}
