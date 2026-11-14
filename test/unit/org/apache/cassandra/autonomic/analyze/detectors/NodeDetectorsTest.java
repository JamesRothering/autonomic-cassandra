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
import org.apache.cassandra.autonomic.analyze.Severity;
import org.apache.cassandra.autonomic.knowledge.InMemoryKnowledgeView;
import org.apache.cassandra.autonomic.knowledge.ObservationStore;
import org.apache.cassandra.autonomic.monitor.Observation;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.monitor.sensors.Signals;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** compaction_backlog, disk_pressure, gc_pressure and pool_saturation, with hand-fed signals. */
public class NodeDetectorsTest
{
    private static final long TICK = 15_000;
    private static final Subject NODE = Subject.node();

    private ObservationStore store;
    private long now;
    private InMemoryKnowledgeView view;

    @Before
    public void setUp()
    {
        store = new ObservationStore(32, 1000);
        now = 1_000_000;
        view = new InMemoryKnowledgeView(store, () -> now);
    }

    private void put(SignalId signal, Subject subject, double value)
    {
        store.accept(new Observation(signal, subject, now, value));
    }

    private static Map<String, Object> opts(Object... kv)
    {
        Map<String, Object> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2)
            m.put((String) kv[i], kv[i + 1]);
        return m;
    }

    /** A new sample of {@code signal} on each of {@code n} ticks; the diagnoses of the last evaluation. */
    private List<Diagnosis> feed(Detector d, int n, SignalId signal, Subject subject, double value)
    {
        List<Diagnosis> out = null;
        for (int i = 0; i < n; i++)
        {
            now += TICK;
            put(signal, subject, value);
            out = d.analyze(view);
        }
        return out;
    }

    @Test
    public void compactionBacklog()
    {
        Detector d = CompactionBacklogDetector.fromConfig(opts());
        assertTrue(feed(d, 6, Signals.COMPACTION_PENDING_TASKS, NODE, 99).isEmpty());
        put(Signals.COMPACTION_BYTES_PER_SEC, NODE, 1234);
        assertTrue(feed(d, 3, Signals.COMPACTION_PENDING_TASKS, NODE, 150).isEmpty());
        put(Signals.COMPACTION_BYTES_PER_SEC, NODE, 1234);
        Diagnosis x = feed(d, 1, Signals.COMPACTION_PENDING_TASKS, NODE, 150).get(0);
        assertEquals("compaction_backlog", x.type);
        assertEquals(Severity.WARN, x.severity);
        assertEquals("150 (raise 100, clear 80)", x.evidence.get("compaction.pending_tasks"));
        assertEquals("1234", x.evidence.get("compaction.bytes_per_sec"));
        assertEquals(Severity.CRITICAL, feed(d, 1, Signals.COMPACTION_PENDING_TASKS, NODE, 600).get(0).severity);
    }

    @Test
    public void diskPressureTriggersAndClearsNearTheLimit()
    {
        Detector d = DiskPressureDetector.fromConfig(opts("for_evaluations", 2));
        assertTrue(feed(d, 3, Signals.DISK_USED_FRACTION, NODE, 0.84).isEmpty());
        put(Signals.DISK_FREE_BYTES, NODE, 1000);
        put(Signals.DISK_TOTAL_BYTES, NODE, 10000);
        List<Diagnosis> out = feed(d, 2, Signals.DISK_USED_FRACTION, NODE, 0.9);
        assertEquals(1, out.size());
        assertEquals("1000", out.get(0).evidence.get("disk.free_bytes"));
        assertEquals("10000", out.get(0).evidence.get("disk.total_bytes"));
        assertTrue(out.get(0).summary.contains("90%"));
        // 0.82 is below raise but not below clear (0.8075): stays open
        assertEquals(1, feed(d, 4, Signals.DISK_USED_FRACTION, NODE, 0.82).size());
        assertEquals(1, feed(d, 1, Signals.DISK_USED_FRACTION, NODE, 0.80).size());
        assertTrue(feed(d, 1, Signals.DISK_USED_FRACTION, NODE, 0.80).isEmpty());
    }

    @Test
    public void gcPressure()
    {
        Detector d = GcPressureDetector.fromConfig(opts("for_evaluations", 2));
        assertTrue(feed(d, 5, Signals.GC_TIME_FRACTION, NODE, 0.05).isEmpty());
        put(Signals.GC_HEAP_USED_FRACTION, NODE, 0.9);
        List<Diagnosis> out = feed(d, 2, Signals.GC_TIME_FRACTION, NODE, 0.4);
        assertEquals(1, out.size());
        assertEquals(Severity.CRITICAL, out.get(0).severity);
        assertEquals("0.9000", out.get(0).evidence.get("gc.heap_used_fraction"));
    }

    @Test
    public void poolSaturationPerPool()
    {
        Detector d = PoolSaturationDetector.fromConfig(opts("for_evaluations", 2));
        Subject mutation = Subject.threadPool("MutationStage");
        Subject read = Subject.threadPool("ReadStage");
        for (int i = 0; i < 2; i++)
        {
            now += TICK;
            put(Signals.POOL_PENDING_TASKS, mutation, 80);
            put(Signals.POOL_PENDING_TASKS, read, 3);
            put(Signals.POOL_ACTIVE_TASKS, mutation, 32);
            put(Signals.POOL_MAX_POOL_SIZE, mutation, 32);
            put(Signals.POOL_CURRENTLY_BLOCKED_TASKS, mutation, 0);
            if (i == 0)
                assertTrue(d.analyze(view).isEmpty());
        }
        List<Diagnosis> out = d.analyze(view);
        assertEquals(1, out.size());
        Diagnosis x = out.get(0);
        assertEquals(mutation, x.subject);
        assertEquals("MutationStage", x.evidence.get("pool"));
        assertEquals("32", x.evidence.get("pool.max_pool_size"));
        assertEquals(Severity.WARN, x.severity);
        now += TICK;
        put(Signals.POOL_PENDING_TASKS, mutation, 80);
        put(Signals.POOL_CURRENTLY_BLOCKED_TASKS, mutation, 4);
        assertEquals(Severity.CRITICAL, d.analyze(view).get(0).severity);
    }
}
