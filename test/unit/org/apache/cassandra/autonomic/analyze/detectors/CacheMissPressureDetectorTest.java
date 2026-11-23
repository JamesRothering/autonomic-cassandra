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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.autonomic.analyze.Detector;
import org.apache.cassandra.autonomic.analyze.Diagnosis;
import org.apache.cassandra.autonomic.analyze.ThresholdDetector;
import org.apache.cassandra.autonomic.knowledge.InMemoryKnowledgeView;
import org.apache.cassandra.autonomic.knowledge.ObservationStore;
import org.apache.cassandra.autonomic.monitor.Observation;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.monitor.sensors.Signals;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class CacheMissPressureDetectorTest
{
    private static final long TICK = 15_000;
    private static final long HOUR = 3_600_000;
    private static final Subject NODE = Subject.node();

    private long now;
    private ObservationStore store;
    private InMemoryKnowledgeView view;
    private Detector detector;

    @Before
    public void setUp()
    {
        store = new ObservationStore(64, 1000);
        now = 10_000_000;
        view = new InMemoryKnowledgeView(store, () -> now);
        Map<String, Object> options = new HashMap<>();
        options.put("for_evaluations", 3);
        detector = CacheMissPressureDetector.fromConfig(options);
    }

    private void put(SignalId signal, double value)
    {
        store.accept(new Observation(signal, NODE, now, value));
    }

    /** One tick per evaluation, each with a new key cache hit rate. */
    private List<Diagnosis> feed(int ticks, double keyHitRate)
    {
        List<Diagnosis> out = null;
        for (int i = 0; i < ticks; i++)
        {
            now += TICK;
            put(Signals.CACHE_KEY_HIT_RATE, keyHitRate);
            put(Signals.CACHE_KEY_REQUESTS_PER_SEC, 250);
            out = detector.analyze(view);
        }
        return out;
    }

    private int ticksPerHour()
    {
        return (int) (HOUR / TICK);
    }

    @Test
    public void raisesOnASustainedDropAgainstTheLongRunLevel()
    {
        assertTrue(feed(ticksPerHour() + 5, 0.8).isEmpty());
        assertTrue(feed(2, 0.55).isEmpty());
        long breachStart = now - TICK;
        List<Diagnosis> out = feed(1, 0.55);
        assertEquals(1, out.size());
        Diagnosis d = out.get(0);
        assertEquals("cache_miss_pressure", d.type);
        assertEquals(NODE, d.subject);
        assertEquals("key", d.evidence.get("cache"));
        assertEquals("0.5500", d.evidence.get("cache.key_hit_rate"));
        assertEquals("0.8000", d.evidence.get("reference_hit_rate"));
        assertEquals("250", d.evidence.get("cache.key_requests_per_sec"));
        assertEquals(Long.toString(breachStart), d.evidence.get(ThresholdDetector.BREACH_SINCE));
        assertTrue(d.summary, d.summary.contains("key cache hit rate is 55%"));
    }

    @Test
    public void aShortDipDoesNotRaise()
    {
        feed(ticksPerHour() + 5, 0.8);
        assertTrue(feed(2, 0.4).isEmpty());
        assertTrue(feed(5, 0.8).isEmpty());
        assertTrue(feed(2, 0.4).isEmpty());
    }

    @Test
    public void aDropSmallerThanTheThresholdDoesNotRaise()
    {
        feed(ticksPerHour() + 5, 0.8);
        assertTrue(feed(10, 0.65).isEmpty());
    }

    @Test
    public void needsAWindowsWorthOfHistory()
    {
        // 50 minutes at 0.8, then a deep drop: no reference yet
        assertTrue(feed(ticksPerHour() * 5 / 6, 0.8).isEmpty());
        assertTrue(feed(10, 0.2).isEmpty());
    }

    @Test
    public void aSustainedDropIsNotItsOwnReferenceAndClearsWhenTheRateRecovers()
    {
        feed(ticksPerHour() + 5, 0.8);
        assertEquals(1, feed(40, 0.5).size());   // 10 minutes later still reported
        // recovers: clears after for_evaluations (3) evaluations inside the clear threshold
        assertEquals(1, feed(2, 0.78).size());
        assertTrue(feed(1, 0.78).isEmpty());
    }

    @Test
    public void namesTheCacheWithTheLargestDrop()
    {
        for (int i = 0; i < ticksPerHour() + 5; i++)
        {
            now += TICK;
            put(Signals.CACHE_KEY_HIT_RATE, 0.9);
            put(Signals.CACHE_CHUNK_HIT_RATE, 0.9);
            detector.analyze(view);
        }
        List<Diagnosis> out = null;
        for (int i = 0; i < 3; i++)
        {
            now += TICK;
            put(Signals.CACHE_KEY_HIT_RATE, 0.6);
            put(Signals.CACHE_CHUNK_HIT_RATE, 0.3);
            put(Signals.CACHE_CHUNK_REQUESTS_PER_SEC, 40);
            out = detector.analyze(view);
        }
        assertEquals(1, out.size());
        assertEquals("chunk", out.get(0).evidence.get("cache"));
        assertEquals("40", out.get(0).evidence.get("cache.chunk_requests_per_sec"));
    }

    @Test
    public void clearsWhenTheHitRateIsNoLongerReported()
    {
        feed(ticksPerHour() + 5, 0.8);
        assertEquals(1, feed(3, 0.4).size());
        // below the request floor the sensor emits no hit rate: the measurement goes stale and the diagnosis clears
        List<Diagnosis> out = null;
        for (int i = 0; i < 12; i++)
        {
            now += TICK;
            out = detector.analyze(view);
        }
        assertTrue(out.isEmpty());
    }

    @Test
    public void aTwoHourDropStaysDiagnosedAgainstThePreDropLevel()
    {
        feed(ticksPerHour() + 5, 0.8);
        assertEquals(1, feed(3, 0.4).size());
        List<Diagnosis> out = feed(2 * ticksPerHour(), 0.4);
        assertEquals(1, out.size());
        assertEquals("0.8000", out.get(0).evidence.get("reference_hit_rate"));
        assertEquals("0.4000", out.get(0).evidence.get("cache.key_hit_rate"));
    }

    @Test
    public void aDropLongerThanRelearnAfterIsTakenAsTheNewLevel()
    {
        feed(ticksPerHour() + 5, 0.8);
        assertEquals(1, feed(3, 0.4).size());
        assertEquals(1, feed(23 * ticksPerHour(), 0.4).size());
        // 25 hours after the drop began, the new level has become the reference
        assertTrue(feed(2 * ticksPerHour(), 0.4).isEmpty());
        // and a further drop from the new level is diagnosed against it
        List<Diagnosis> out = feed(5, 0.1);
        assertEquals(1, out.size());
        assertEquals("0.4000", out.get(0).evidence.get("reference_hit_rate"));
    }

    @Test
    public void relearnAfterIsConfigurable()
    {
        Map<String, Object> options = new HashMap<>();
        options.put("for_evaluations", 3);
        options.put("relearn_after_ms", 2 * HOUR);
        detector = CacheMissPressureDetector.fromConfig(options);
        feed(ticksPerHour() + 5, 0.8);
        assertEquals(1, feed(ticksPerHour(), 0.4).size());
        assertTrue(feed(2 * ticksPerHour(), 0.4).isEmpty());
    }

    @Test
    public void referenceWindowIsConfigurable()
    {
        Map<String, Object> options = new HashMap<>();
        options.put("for_evaluations", 2);
        options.put("reference_window_ms", 300_000);
        detector = CacheMissPressureDetector.fromConfig(options);
        feed(30, 0.8);
        assertEquals(1, feed(2, 0.5).size());
    }
}
