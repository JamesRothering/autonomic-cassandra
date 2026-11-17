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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.autonomic.analyze.Diagnosis;
import org.apache.cassandra.autonomic.analyze.Severity;
import org.apache.cassandra.autonomic.execute.ActionRecord;
import org.apache.cassandra.autonomic.execute.ActionState;
import org.apache.cassandra.autonomic.execute.Verdict;
import org.apache.cassandra.autonomic.execute.actuators.PartitionSplitActuator;
import org.apache.cassandra.autonomic.knowledge.InMemoryKnowledgeView;
import org.apache.cassandra.autonomic.knowledge.ObservationStore;
import org.apache.cassandra.autonomic.layout.LayoutEvents;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.plan.Action;
import org.apache.cassandra.autonomic.plan.ActionScope;
import org.apache.cassandra.autonomic.plan.policies.PartitionSplitPolicy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** {@code layout.verify_failed} and {@code layout.split_not_effective}; and the note of the policy on {@code hot_partition}. */
public class SplitOutcomeDetectorsTest
{
    private long now = 100_000_000;
    private final List<ActionRecord> records = new ArrayList<>();
    private final InMemoryKnowledgeView view = new InMemoryKnowledgeView(new ObservationStore(8, 8), () -> now);

    @Before
    public void setUp()
    {
        LayoutEvents.clear();
        view.setActionSource(ms -> records);
    }

    @After
    public void tearDown()
    {
        LayoutEvents.clear();
    }

    private ActionRecord split(ActionState state, Verdict verdict, String keyHex)
    {
        Action a = new Action(UUID.randomUUID(), PartitionSplitActuator.NAME, ActionScope.CLUSTER, PartitionSplitActuator.target("ks", "t", keyHex), Map.of(), null);
        return new ActionRecord(a, state, now - 1000, null, "partition_split", false, null, Map.of(), Map.of(), Map.of(), verdict, now - 5000);
    }

    @Test
    public void aSplitThatEndedNeutralOrRegressedIsNotEffective()
    {
        SplitOutcomeDetectors.SplitNotEffective d = new SplitOutcomeDetectors.SplitNotEffective();
        assertEquals("layout.split_not_effective", d.name());
        records.add(split(ActionState.COMMITTED, Verdict.IMPROVED, "0a"));
        records.add(split(ActionState.COMMITTED, Verdict.INCONCLUSIVE, "0b"));
        records.add(split(ActionState.ROLLED_BACK, Verdict.REGRESSED, "0c"));
        assertEquals(List.of(), d.analyze(view));
        ActionRecord neutral = split(ActionState.COMMITTED, Verdict.NEUTRAL, "0d");
        ActionRecord regressed = split(ActionState.COMMITTED, Verdict.REGRESSED, "0e");
        records.add(neutral);
        records.add(regressed);
        List<Diagnosis> out = d.analyze(view);
        assertEquals(2, out.size());
        Diagnosis n = out.get(0);
        assertEquals("layout.split_not_effective", n.type);
        assertEquals(Severity.WARN, n.severity);
        assertEquals(neutral.action.target, n.subject);
        assertEquals("0d", n.evidence.get("key_hex"));
        assertEquals("NEUTRAL", n.evidence.get("verdict"));
        assertEquals("REGRESSED", out.get(1).evidence.get("verdict"));
        assertEquals("the id is stable", n.id, d.analyze(view).get(0).id);
        assertTrue(n.summary, n.summary.contains("stays split"));
    }

    @Test
    public void aMigrationAbortedByItsVerificationIsReportedForADay()
    {
        SplitOutcomeDetectors.VerifyFailed d = new SplitOutcomeDetectors.VerifyFailed();
        assertEquals("layout.verify_failed", d.name());
        assertEquals(List.of(), d.analyze(view));
        UUID action = UUID.randomUUID();
        LayoutEvents.verifyFailed(action, "ks", "t", "0a", "3 rows differ", now - 1000);
        LayoutEvents.verifyFailed(action, "ks", "t", "0a", "recorded again by a repeated step", now - 500);
        List<Diagnosis> out = d.analyze(view);
        assertEquals(1, out.size());
        Diagnosis x = out.get(0);
        assertEquals("layout.verify_failed", x.type);
        assertEquals(Severity.WARN, x.severity);
        assertEquals(Subject.partition("ks", "t", "0a"), x.subject);
        assertEquals("0a", x.evidence.get("key_hex"));
        assertEquals("3 rows differ", x.evidence.get("verification"));
        now += SplitOutcomeDetectors.OPEN_MILLIS;
        assertEquals("cleared after a day", List.of(), d.analyze(view));
    }

    @Test
    public void thePoliciesNoteIsShownOnTheHotPartitionDiagnosis()
    {
        org.apache.cassandra.autonomic.layout.LayoutAnnotations.clearAll();
        long[] t = { 1_000_000 };
        ObservationStore store = new ObservationStore(32, 1000);
        InMemoryKnowledgeView v = new InMemoryKnowledgeView(store, () -> t[0]);
        Subject p = org.apache.cassandra.autonomic.monitor.sensors.PartitionKeys.subject("ks", "t", org.apache.cassandra.utils.ByteBufferUtil.bytes("k1"));
        HotPartitionDetector d = HotPartitionDetector.fromConfig(Map.of("for_evaluations", 1));
        t[0] += 15_000;
        store.accept(new org.apache.cassandra.autonomic.monitor.Observation(org.apache.cassandra.autonomic.monitor.sensors.Signals.PARTITION_READ_SHARE, p, t[0], 0.5));
        assertEquals(null, d.analyze(v).get(0).evidence.get("layout"));
        org.apache.cassandra.autonomic.layout.LayoutAnnotations.set(p, org.apache.cassandra.autonomic.layout.LayoutAnnotations.notApplicable("small"));
        t[0] += 15_000;
        store.accept(new org.apache.cassandra.autonomic.monitor.Observation(org.apache.cassandra.autonomic.monitor.sensors.Signals.PARTITION_READ_SHARE, p, t[0], 0.5));
        Diagnosis x = d.analyze(v).get(0);
        assertEquals("not_applicable(small)", x.evidence.get("layout"));
        assertEquals(org.apache.cassandra.utils.ByteBufferUtil.bytesToHex(org.apache.cassandra.utils.ByteBufferUtil.bytes("k1")), x.evidence.get("key_hex"));
        org.apache.cassandra.autonomic.layout.LayoutAnnotations.clearAll();
        assertTrue(PartitionSplitPolicy.NAME.equals("partition_split"));
    }
}
