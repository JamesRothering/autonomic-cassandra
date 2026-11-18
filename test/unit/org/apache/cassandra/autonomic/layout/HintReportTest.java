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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.NodeId;

import static org.apache.cassandra.autonomic.layout.LayoutTransformationTest.K1;
import static org.apache.cassandra.autonomic.layout.LayoutTransformationTest.LOGICAL;
import static org.apache.cassandra.autonomic.layout.LayoutTransformationTest.N1;
import static org.apache.cassandra.autonomic.layout.LayoutTransformationTest.N2;
import static org.apache.cassandra.autonomic.layout.LayoutTransformationTest.key;
import static org.apache.cassandra.autonomic.layout.LayoutTransformationTest.keyIn;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** ADR-035: the age and host filter of the hint report, and the node's report while a key is in ANNOUNCED. */
public class HintReportTest
{
    private static final UUID H1 = new UUID(0x1111, 1);
    private static final UUID H2 = new UUID(0x2222, 2);
    private static final long DRAIN = 3000;

    @BeforeClass
    public static void init()
    {
        DatabaseDescriptor.toolInitialization();
    }

    private static String name(UUID host, long timestamp)
    {
        return host + "-" + timestamp + "-2.hints";
    }

    private static HintReport.HintFile file(UUID host, long timestamp)
    {
        return new HintReport.HintFile(host, timestamp);
    }

    // ---- the filter ----

    @Test
    public void namesAreParsedAndOtherFilesIgnored()
    {
        HintReport.HintFile f = HintReport.HintFile.parse(name(H1, 1234));
        assertEquals(H1, f.host);
        assertEquals(1234, f.timestamp);
        assertNull(HintReport.HintFile.parse(H1 + "-1234-2.crc32"));
        assertNull(HintReport.HintFile.parse("garbage.hints"));
        assertNull(HintReport.HintFile.parse(H1 + "-x-2.hints"));
        assertEquals(1, HintReport.parse(List.of(name(H1, 1), H1 + "-1-2.crc32", "other")).size());
    }

    @Test
    public void theCutOfAHostIsTheLaterOfTheFlushAndItsNewestFile()
    {
        List<HintReport.HintFile> files = List.of(file(H1, 100), file(H1, 900), file(H2, 50));
        Map<UUID, Long> cuts = HintReport.cuts(files, Set.of(H1, H2), 500);
        assertEquals(Long.valueOf(900), cuts.get(H1));
        assertEquals(Long.valueOf(500), cuts.get(H2));
        assertEquals("only the hosts asked for", 2, cuts.size());
        assertEquals(Long.valueOf(500), HintReport.cuts(files, Set.of(new UUID(9, 9)), 500).values().iterator().next());
    }

    @Test
    public void onlyFilesOfTheReplicasAtOrBeforeTheirCutBlock()
    {
        Map<UUID, Long> cuts = ImmutableMap.of(H1, 500L);
        // another host's old file does not block
        assertTrue(HintReport.blocking(List.of(file(H2, 10)), cuts).isEmpty());
        // a replica's files created after the cut, hints of other keys under load, do not block
        assertTrue(HintReport.blocking(List.of(file(H1, 501), file(H1, 9000)), cuts).isEmpty());
        // at or before the cut they do; the oldest is named
        assertEquals(ImmutableMap.of(H1, 100L), HintReport.blocking(List.of(file(H1, 500), file(H1, 100), file(H1, 700), file(H2, 1)), cuts));
        assertEquals(ImmutableMap.of(H1, 500L), HintReport.blocking(List.of(file(H1, 500)), cuts));
    }

    @Test
    public void hostsAreTheNaturalReplicas()
    {
        ClusterMetadata md = keyIn(Phase.ANNOUNCED);
        Set<UUID> hosts = HintReport.hosts(md, md.schema.getTableMetadata(LOGICAL), K1);
        assertTrue(hosts.contains(N1.toUUID()));
        assertFalse("node 2 is not a replica of the key", hosts.contains(N2.toUUID()));
    }

    // ---- the node ----

    final class FakeEnv implements HintReport.Env
    {
        ClusterMetadata metadata;
        NodeId self = N2;
        long now = 10_000;
        final List<String> names = new ArrayList<>();
        final List<Collection<UUID>> flushes = new ArrayList<>();
        int scheduled;
        int reports;

        @Override
        public ClusterMetadata metadata()
        {
            return metadata;
        }

        @Override
        public NodeId self(ClusterMetadata cm)
        {
            return self;
        }

        @Override
        public long nowMillis()
        {
            return now;
        }

        @Override
        public long drainMillis()
        {
            return DRAIN;
        }

        @Override
        public long pollMillis()
        {
            return 100;
        }

        @Override
        public void flush(Collection<UUID> hosts)
        {
            flushes.add(hosts);
        }

        @Override
        public Collection<String> hintFileNames()
        {
            return names;
        }

        @Override
        public boolean report(ReportHints report)
        {
            reports++;
            Transformation.Result result = report.wrap().execute(metadata);
            if (!result.isSuccess())
                return false;
            metadata = result.success().metadata;
            return true;
        }

        @Override
        public void schedule(Runnable task, long delayMillis)
        {
            scheduled++;
        }
    }

    @Test
    public void aNodeReportsAfterTheDrainOnceItsOldHintsForTheReplicasAreDelivered()
    {
        FakeEnv env = new FakeEnv();
        env.metadata = keyIn(Phase.ANNOUNCED);
        HintReport report = new HintReport(env);
        UUID replica = N1.toUUID();
        env.names.add(name(replica, 9_000));            // a hint for the replica from before the split
        env.names.add(name(new UUID(7, 7), 1_000));     // an old hint for a node that is not a replica

        report.onRegistryChange(env.metadata);
        assertEquals("a registry change only schedules", 1, env.scheduled);
        assertEquals(0, report.poll());
        assertTrue("no flush before the drain", env.flushes.isEmpty());
        assertEquals(1, report.pendingCount());

        env.now += DRAIN;
        assertEquals("the old file of the replica holds the report", 0, report.poll());
        assertEquals(1, env.flushes.size());
        assertTrue(env.flushes.get(0).contains(replica));
        env.names.add(name(replica, env.now + 1));      // later hints, of another key under load: they do not hold it
        env.now += 1000;
        assertEquals(0, report.poll());
        assertEquals("the cut is taken once", 1, env.flushes.size());

        env.names.remove(name(replica, 9_000));         // delivered
        assertEquals(1, report.poll());
        assertEquals(ImmutableSet.of(N2), key(env.metadata, K1).hintsClear);
        assertEquals(0, report.poll());
        assertEquals("reported once", 1, env.reports);
        assertEquals(0, report.pendingCount());
    }

    @Test
    public void nothingIsDoneOutsideAnnouncedOrForAKeyAlreadyReported() throws Exception
    {
        FakeEnv env = new FakeEnv();
        env.metadata = keyIn(Phase.DUAL_WRITE);
        HintReport report = new HintReport(env);
        report.onRegistryChange(env.metadata);
        assertEquals(0, env.scheduled);
        env.now += DRAIN;
        assertEquals(0, report.poll());
        assertTrue(env.flushes.isEmpty());

        env.metadata = keyIn(Phase.ANNOUNCED);
        env.metadata = new ReportHints(LOGICAL, K1, key(env.metadata, K1).phaseEpoch, N2).wrap().execute(env.metadata).success().metadata;
        assertEquals(0, report.poll());
        assertEquals(0, report.pendingCount());
        assertTrue(env.flushes.isEmpty());
    }
}
