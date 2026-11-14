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
import java.util.UUID;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.autonomic.analyze.Diagnosis;
import org.apache.cassandra.autonomic.analyze.Severity;
import org.apache.cassandra.autonomic.knowledge.InMemoryKnowledgeView;
import org.apache.cassandra.autonomic.knowledge.ObservationStore;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.repair.autorepair.AutoRepairConfig.RepairType;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RepairOverdueDetectorTest
{
    private static final long HOUR = 3_600_000;
    private static final UUID A = UUID.fromString("00000000-0000-0000-0000-00000000000a");
    private static final UUID B = UUID.fromString("00000000-0000-0000-0000-00000000000b");

    private final Map<UUID, Long> finished = new HashMap<>();
    private boolean enabled = true;
    private boolean unreadable;
    private int reads;
    private long now = 100 * HOUR;
    private final InMemoryKnowledgeView view = new InMemoryKnowledgeView(new ObservationStore(8, 8), () -> now);

    private final RepairOverdueDetector.History history = new RepairOverdueDetector.History()
    {
        public boolean enabled(RepairType type)
        {
            return enabled;
        }

        public long intervalMillis(RepairType type)
        {
            return HOUR;
        }

        public Map<UUID, Long> lastFinishMillis(RepairType type, long freshAfterMillis) throws Exception
        {
            reads++;
            if (unreadable)
                throw new Exception("unavailable");
            return new HashMap<>(finished);
        }
    };

    private List<Diagnosis> tick(RepairOverdueDetector d)
    {
        now += 15_000;
        return d.analyze(view);
    }

    @Before
    public void setUp()
    {
        finished.put(A, now - HOUR);
        finished.put(B, now - HOUR);
    }

    private RepairOverdueDetector detector(long refreshMillis)
    {
        return new RepairOverdueDetector(new RepairOverdueDetector.Settings(2.0, 1.6, 2, true, 120_000, 100), history, RepairType.FULL, 4.0, refreshMillis);
    }

    @Test
    public void opensForTheOverdueHostOnlyAfterTheConfiguredEvaluations()
    {
        RepairOverdueDetector d = detector(0);
        assertTrue(tick(d).isEmpty());
        finished.put(B, now - 3 * HOUR);
        assertTrue("first evaluation above the threshold", tick(d).isEmpty());
        List<Diagnosis> out = tick(d);
        assertEquals(1, out.size());
        Diagnosis x = out.get(0);
        assertEquals("repair_overdue", x.type);
        assertEquals(Subject.peer(B.toString()), x.subject);
        assertEquals(Severity.WARN, x.severity);
        assertEquals("FULL", x.evidence.get("repair_type"));
        assertEquals("true", x.evidence.get("auto_repair_enabled"));
        assertTrue(x.evidence.get("repair.overdue_ratio").startsWith("3.0"));
    }

    @Test
    public void criticalFromTheCriticalMultipleAndNeverRepairedCounts()
    {
        RepairOverdueDetector d = detector(0);
        finished.put(A, 0L);
        tick(d);
        Diagnosis x = tick(d).get(0);
        assertEquals(Severity.CRITICAL, x.severity);
        assertEquals("never", x.evidence.get("last_repair_finish_ms"));
    }

    @Test
    public void clearsAfterARepair()
    {
        RepairOverdueDetector d = detector(0);
        finished.put(B, now - 3 * HOUR);
        tick(d);
        assertEquals(1, tick(d).size());
        finished.put(B, now + 15_000);
        assertEquals("one evaluation below clear does not close it", 1, tick(d).size());
        assertTrue(tick(d).isEmpty());
    }

    @Test
    public void disabledAutoRepairIsReportedAtInfoWithTheFlagThePolicyReads()
    {
        enabled = false;
        finished.put(B, 0L);
        RepairOverdueDetector d = detector(0);
        tick(d);
        Diagnosis x = tick(d).get(0);
        assertEquals(Severity.INFO, x.severity);
        assertEquals("false", x.evidence.get("auto_repair_enabled"));
        assertTrue(x.summary, x.summary.contains("disabled"));
    }

    @Test
    public void historyIsReadOncePerRefreshAndAFailedReadLeavesNoMeasurement()
    {
        RepairOverdueDetector d = detector(30_000);
        finished.put(B, now - 3 * HOUR);
        tick(d);
        tick(d);
        assertEquals("two ticks inside the refresh period", 1, reads);
        assertEquals(1, tick(d).size());
        assertEquals(2, reads);
        unreadable = true;
        tick(d);
        assertEquals(2, reads);
        // no measurement: the open diagnosis clears after the configured evaluations without one
        assertEquals(1, tick(d).size());
        assertEquals(3, reads);
        assertTrue(tick(d).isEmpty());
    }

    @Test
    public void builtFromOptionsWithDefaultsAndRegisteredByName()
    {
        RepairOverdueDetector d = RepairOverdueDetector.fromConfig(Map.of("interval_multiple", 3, "refresh_ms", 5), history);
        assertEquals(3.0, d.settings().raise, 0);
        assertEquals(2.4, d.settings().clear, 1e-9);
        assertTrue(d.requires().isEmpty());
    }
}
