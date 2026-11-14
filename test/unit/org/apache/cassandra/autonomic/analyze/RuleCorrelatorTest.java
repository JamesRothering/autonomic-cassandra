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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.junit.Test;

import org.apache.cassandra.autonomic.analyze.RuleCorrelator.Rule;
import org.apache.cassandra.autonomic.analyze.RuleCorrelator.Scope;
import org.apache.cassandra.autonomic.monitor.Subject;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RuleCorrelatorTest
{
    private static final Subject NODE = Subject.node();
    private static final Subject T1 = Subject.table("ks", "t1");
    private static final Subject T2 = Subject.table("ks", "t2");
    private final RuleCorrelator correlator = RuleCorrelator.defaults();

    private static Diagnosis diag(String type, Subject subject, Severity severity, long first)
    {
        return new Diagnosis(UUID.randomUUID(), type, subject, severity, 1.0, first, first + 1000, type + " summary", java.util.Map.of("k", "v"), null);
    }

    private static Diagnosis diag(String type, Subject subject)
    {
        return diag(type, subject, Severity.WARN, 1);
    }

    private List<Diagnosis> run(Diagnosis... raw)
    {
        return correlator.correlate(Arrays.asList(raw), null);
    }

    private static Diagnosis find(List<Diagnosis> out, UUID id)
    {
        return out.stream().filter(d -> d.id.equals(id)).findFirst().get();
    }

    /** cause type, cause subject, effect type, effect subject, expected effect summary. */
    private static final Object[][] TABLE = {
        { "disk_pressure", NODE, "compaction_backlog", NODE,
          "compaction_backlog summary [likely caused by disk_pressure on NODE: a nearly full disk leaves compaction without room to merge sstables]" },
        { "compaction_backlog", NODE, "latency_slo_breach", T1,
          "latency_slo_breach summary [likely caused by compaction_backlog on NODE: pending compactions leave more sstables per read and compete for disk and CPU]" },
        { "compaction_backlog", NODE, "degraded_self", NODE,
          "degraded_self summary [likely caused by compaction_backlog on NODE: pending compactions slow this node's own reads]" },
        { "gc_pressure", NODE, "latency_slo_breach", NODE,
          "latency_slo_breach summary [likely caused by gc_pressure on NODE: garbage collection pauses stall requests node-wide]" },
        { "gc_pressure", NODE, "pool_saturation", Subject.threadPool("MutationStage"),
          "pool_saturation summary [likely caused by gc_pressure on NODE: garbage collection pauses stop stage threads and let queues grow]" },
        { "gc_pressure", NODE, "degraded_self", NODE,
          "degraded_self summary [likely caused by gc_pressure on NODE: garbage collection pauses slow this node's own requests]" },
        { "pool_saturation", Subject.threadPool("ReadStage"), "latency_slo_breach", T1,
          "latency_slo_breach summary [likely caused by pool_saturation on THREAD_POOL:ReadStage: requests wait in a saturated thread pool queue]" },
        { "pool_saturation", Subject.threadPool("ReadStage"), "degraded_self", NODE,
          "degraded_self summary [likely caused by pool_saturation on THREAD_POOL:ReadStage: requests wait in a saturated thread pool queue]" },
        { "hot_partition", Subject.partition("ks", "t1", "cafe"), "latency_slo_breach", T1,
          "latency_slo_breach summary [likely caused by hot_partition on PARTITION:ks.t1:cafe: one partition takes a large share of the table's operations]" },
        { "tombstone_overload", T1, "latency_slo_breach", T1,
          "latency_slo_breach summary [likely caused by tombstone_overload on TABLE:ks.t1: reads scan many tombstones]" },
        { "degraded_peer", Subject.peer("p1"), "latency_slo_breach", NODE,
          "latency_slo_breach summary [likely caused by degraded_peer on PEER:p1: coordinator requests wait on a slow replica]" },
        { "degraded_peer", Subject.peer("p1"), "degraded_self", NODE,
          "degraded_self summary [likely caused by degraded_peer on PEER:p1: coordinator requests wait on a slow replica, not on this node]" },
    };

    @Test
    public void everyDefaultRuleMarksTheEffectAndWordsTheSummary()
    {
        assertEquals(RuleCorrelator.defaultRules().size(), TABLE.length);
        for (Object[] row : TABLE)
        {
            Diagnosis cause = diag((String) row[0], (Subject) row[1]);
            Diagnosis effect = diag((String) row[2], (Subject) row[3]);
            List<Diagnosis> out = run(effect, cause);
            assertEquals(row[0] + " -> " + row[2], 2, out.size());
            Diagnosis e = find(out, effect.id);
            assertEquals(cause.id, e.rootCause);
            assertEquals(row[4], e.summary);
            assertEquals(effect.evidence, e.evidence);
            assertEquals(effect.severity, e.severity);
            assertEquals(effect.firstSeenMillis, e.firstSeenMillis);
            assertNull(find(out, cause.id).rootCause);
        }
    }

    @Test
    public void scopeAndKindRestrictTheMatch()
    {
        // hot partition on t2 does not explain latency on t1
        Diagnosis hot = diag("hot_partition", Subject.partition("ks", "t2", "cafe"));
        Diagnosis lat = diag("latency_slo_breach", T1);
        assertNull(find(run(hot, lat), lat.id).rootCause);
        Diagnosis lat2 = diag("latency_slo_breach", T2);
        assertEquals(hot.id, find(run(hot, lat2), lat2.id).rootCause);
        // a slow peer explains coordinator (node) latency, not table latency
        Diagnosis peer = diag("degraded_peer", Subject.peer("p1"));
        assertNull(find(run(peer, lat), lat.id).rootCause);
        // unrelated diagnoses pass through unchanged
        Diagnosis other = diag("disk_pressure", NODE);
        assertSame(other, run(other, lat).get(0));
    }

    @Test
    public void chainsResolveToTheUltimateRoot()
    {
        Diagnosis disk = diag("disk_pressure", NODE);
        Diagnosis backlog = diag("compaction_backlog", NODE);
        Diagnosis latency = diag("latency_slo_breach", T1);
        List<Diagnosis> out = run(latency, backlog, disk);
        assertEquals(3, out.size());
        assertEquals(disk.id, find(out, backlog.id).rootCause);
        assertEquals(disk.id, find(out, latency.id).rootCause);
        assertNull(find(out, disk.id).rootCause);
        // the direct cause is named in the effect's summary
        assertTrue(find(out, latency.id).summary.contains("caused by compaction_backlog"));
    }

    @Test
    public void severestCauseWinsRegardlessOfOrder()
    {
        Diagnosis gc = diag("gc_pressure", NODE, Severity.CRITICAL, 5);
        Diagnosis pool = diag("pool_saturation", Subject.threadPool("ReadStage"), Severity.WARN, 1);
        Diagnosis lat = diag("latency_slo_breach", T1);
        assertEquals(gc.id, find(run(lat, pool, gc), lat.id).rootCause);
        assertEquals(gc.id, find(run(gc, pool, lat), lat.id).rootCause);
    }

    @Test
    public void releasedOnTheEvaluationAfterTheCauseClears()
    {
        Diagnosis cause = diag("gc_pressure", NODE);
        Diagnosis effect = diag("latency_slo_breach", NODE);
        assertEquals(cause.id, find(run(effect, cause), effect.id).rootCause);
        List<Diagnosis> later = run(effect);
        assertEquals(1, later.size());
        assertNull(later.get(0).rootCause);
        assertEquals(effect.summary, later.get(0).summary);
    }

    @Test
    public void nothingIsEverDropped()
    {
        List<Diagnosis> raw = new ArrayList<>();
        for (Object[] row : TABLE)
        {
            raw.add(diag((String) row[0], (Subject) row[1]));
            raw.add(diag((String) row[2], (Subject) row[3]));
        }
        List<Diagnosis> out = correlator.correlate(raw, null);
        assertEquals(raw.size(), out.size());
        for (int i = 0; i < raw.size(); i++)
            assertEquals(raw.get(i).id, out.get(i).id);
    }

    @Test
    public void defaultRuleTableHasNoCycles()
    {
        new RuleCorrelator(RuleCorrelator.defaultRules()); // constructor checks
        try
        {
            new RuleCorrelator(List.of(new Rule("a", "b", Scope.LOCAL, null, "x"),
                                       new Rule("b", "c", Scope.LOCAL, null, "x"),
                                       new Rule("c", "a", Scope.LOCAL, null, "x")));
            fail("cycle accepted");
        }
        catch (IllegalArgumentException expected)
        {
            assertTrue(expected.getMessage().contains("cycle"));
        }
        try
        {
            new RuleCorrelator(List.of(new Rule("a", "a", Scope.LOCAL, null, "x")));
            fail("self loop accepted");
        }
        catch (IllegalArgumentException expected)
        {
            // expected
        }
    }
}
