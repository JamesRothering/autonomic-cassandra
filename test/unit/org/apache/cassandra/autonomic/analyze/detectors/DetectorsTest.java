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

import org.apache.cassandra.autonomic.AutonomicConfig;
import org.apache.cassandra.autonomic.analyze.Detector;
import org.apache.cassandra.autonomic.analyze.Diagnosis;
import org.apache.cassandra.autonomic.analyze.Severity;
import org.apache.cassandra.autonomic.knowledge.InMemoryKnowledgeView;
import org.apache.cassandra.autonomic.knowledge.ObservationStore;
import org.apache.cassandra.autonomic.monitor.Observation;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.monitor.sensors.PartitionKeys;
import org.apache.cassandra.autonomic.monitor.sensors.Signals;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DetectorsTest
{
    private static final long TICK = 15_000;
    private static final Subject T = Subject.table("ks", "t");

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

    private Map<String, Object> opts(Object... kv)
    {
        Map<String, Object> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2)
            m.put((String) kv[i], kv[i + 1]);
        return m;
    }

    // ---- hot_partition ----

    private static Subject hot(String key)
    {
        return PartitionKeys.subject("ks", "t", ByteBufferUtil.bytes(key));
    }

    @Test
    public void hotPartitionOpensWithKeyHexAndEvidence()
    {
        Detector d = HotPartitionDetector.fromConfig(opts("for_evaluations", 2));
        Subject p = hot("k1");
        now += TICK;
        put(Signals.PARTITION_READ_SHARE, p, 0.5);
        put(Signals.PARTITION_READ_OPS_PER_SEC, p, 900);
        assertTrue(d.analyze(view).isEmpty());
        now += TICK;
        List<Diagnosis> out = d.analyze(view);
        assertEquals(1, out.size());
        Diagnosis x = out.get(0);
        assertEquals("hot_partition", x.type);
        assertEquals(p, x.subject);
        assertEquals(Severity.WARN, x.severity);
        assertEquals(ByteBufferUtil.bytesToHex(ByteBufferUtil.bytes("k1")), x.evidence.get("key_hex"));
        assertEquals("0.5000", x.evidence.get("partition.read_share"));
        assertEquals("900", x.evidence.get("partition.read_ops_per_sec"));
        assertTrue(x.summary.contains("50%"));
    }

    @Test
    public void hotPartitionBelowShareOrBelowClearBehaves()
    {
        Detector d = HotPartitionDetector.fromConfig(opts("for_evaluations", 1));
        Subject p = hot("k1");
        now += TICK;
        put(Signals.PARTITION_READ_SHARE, p, 0.29);
        assertTrue(d.analyze(view).isEmpty());
        now += TICK;
        put(Signals.PARTITION_WRITE_SHARE, p, 0.85); // writes count too
        List<Diagnosis> out = d.analyze(view);
        assertEquals(1, out.size());
        assertEquals(Severity.CRITICAL, out.get(0).severity);
        now += TICK;
        put(Signals.PARTITION_READ_SHARE, p, 0.1);
        put(Signals.PARTITION_WRITE_SHARE, p, 0.26); // between clear (0.24) and raise: stays
        assertEquals(1, d.analyze(view).size());
    }

    @Test
    public void hotPartitionGoneWhenSamplesGoStale()
    {
        Detector d = HotPartitionDetector.fromConfig(opts("for_evaluations", 1));
        now += TICK;
        put(Signals.PARTITION_READ_SHARE, hot("k1"), 0.9);
        assertEquals(1, d.analyze(view).size());
        now += 200_000;
        assertTrue(d.analyze(view).isEmpty());
    }

    // ---- latency_slo_breach ----

    private LatencySloBreachDetector latency(Object... kv)
    {
        AutonomicConfig c = new AutonomicConfig();
        return LatencySloBreachDetector.fromConfig(opts(kv), c.slo); // 50ms read, 20ms write
    }

    @Test
    public void latencyBreachPerTableAndNode()
    {
        Detector d = latency("for_evaluations", 2);
        for (int i = 0; i < 2; i++)
        {
            now += TICK;
            put(Signals.TABLE_READ_LATENCY_P99_MICROS, T, 80_000); // 1.6x
            put(Signals.TABLE_WRITE_LATENCY_P99_MICROS, T, 5_000);
            put(Signals.CLIENT_WRITE_LATENCY_P99_MICROS, Subject.node(), 50_000); // 2.5x of 20ms
            if (i == 0)
                assertTrue(d.analyze(view).isEmpty());
        }
        List<Diagnosis> out = d.analyze(view);
        assertEquals(2, out.size());
        for (Diagnosis x : out)
        {
            assertEquals("latency_slo_breach", x.type);
            if (x.subject.equals(T))
            {
                assertEquals(Severity.WARN, x.severity);
                assertTrue(x.evidence.get("table.read_latency_p99_micros").startsWith("80000 us"));
                assertTrue(x.evidence.get("table.read_latency_p99_micros").contains("slo 50000"));
            }
            else
            {
                assertEquals(Subject.node(), x.subject);
                assertEquals(Severity.CRITICAL, x.severity);
            }
        }
    }

    @Test
    public void latencyWithinSloDoesNotTrigger()
    {
        Detector d = latency("for_evaluations", 1);
        now += TICK;
        put(Signals.TABLE_READ_LATENCY_P99_MICROS, T, 49_000);
        put(Signals.TABLE_WRITE_LATENCY_P99_MICROS, T, 19_000);
        put(Signals.CLIENT_READ_LATENCY_P99_MICROS, Subject.node(), 10_000);
        assertTrue(d.analyze(view).isEmpty());
    }

    @Test
    public void latencyClearsOnlyBelowEightyPercentOfSlo()
    {
        Detector d = latency("for_evaluations", 1);
        now += TICK;
        put(Signals.TABLE_READ_LATENCY_P99_MICROS, T, 60_000);
        assertEquals(1, d.analyze(view).size());
        now += TICK;
        put(Signals.TABLE_READ_LATENCY_P99_MICROS, T, 45_000); // under SLO, above 40ms
        assertEquals(1, d.analyze(view).size());
        now += TICK;
        put(Signals.TABLE_READ_LATENCY_P99_MICROS, T, 39_000);
        assertTrue(d.analyze(view).isEmpty());
    }

    // ---- tombstone_overload ----

    @Test
    public void tombstoneOverloadTriggersAndEscalatesOnFailures()
    {
        Detector d = TombstoneOverloadDetector.fromConfig(opts("for_evaluations", 2));
        now += TICK;
        put(Signals.TABLE_TOMBSTONES_SCANNED_P99, T, 5_000);
        put(Signals.TABLE_TOMBSTONE_WARNINGS_DELTA, T, 3);
        assertTrue(d.analyze(view).isEmpty());
        now += TICK;
        put(Signals.TABLE_TOMBSTONES_SCANNED_P99, T, 5_000);
        put(Signals.TABLE_TOMBSTONE_WARNINGS_DELTA, T, 2);
        Diagnosis x = d.analyze(view).get(0);
        assertEquals("tombstone_overload", x.type);
        assertEquals(T, x.subject);
        assertEquals(Severity.WARN, x.severity);
        assertEquals("5", x.evidence.get("table.tombstone_warnings_delta"));
        assertEquals("0", x.evidence.get("table.tombstone_failures_delta"));
        now += TICK;
        put(Signals.TABLE_TOMBSTONES_SCANNED_P99, T, 5_000);
        put(Signals.TABLE_TOMBSTONE_FAILURES_DELTA, T, 1);
        x = d.analyze(view).get(0);
        assertEquals(Severity.CRITICAL, x.severity);
        assertEquals("1", x.evidence.get("table.tombstone_failures_delta"));
    }

    @Test
    public void tombstonesBelowThresholdDoNotTrigger()
    {
        Detector d = TombstoneOverloadDetector.fromConfig(opts("for_evaluations", 1));
        now += TICK;
        put(Signals.TABLE_TOMBSTONES_SCANNED_P99, T, 999);
        assertTrue(d.analyze(view).isEmpty());
    }

    // ---- registration ----

    @Test
    public void buildHonoursEnabledFlags()
    {
        AutonomicConfig c = new AutonomicConfig();
        assertEquals(3, Detectors.build(c).size());
        c.detectors.get("hot_partition").put("enabled", false);
        c.detectors.put("tombstone_overload", opts("enabled", "false"));
        List<Detector> built = Detectors.build(c);
        assertEquals(1, built.size());
        assertEquals("latency_slo_breach", built.get(0).name());
    }
}
