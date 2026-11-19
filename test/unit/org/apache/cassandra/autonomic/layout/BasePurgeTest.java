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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.compaction.CompactionManager;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * T6.17d: the replica's purge and residue probe of a slow reverse in REVERSE_ANNOUNCED, on a real table with a fake
 * node around it: the clock, the report and the registry are the test's; the probe, the flush and the forced
 * compaction are the real ones.
 */
public class BasePurgeTest extends CQLTester
{
    private static final UUID LAYOUT_ID = UUID.fromString("1a2b3c4d-0000-4000-8000-000000000017");
    private static final UUID ACTION = new UUID(17, 17);
    private static final int K = 1;
    private static final NodeId SELF = new NodeId(1);

    private TableMetadata logical;
    private TableMetadata physical;
    private LayoutHeader header;
    private FakeEnv env;

    private void setUp(String options)
    {
        String t = createTable("CREATE TABLE %s (p int, c int, v text, PRIMARY KEY (p, c)) WITH gc_grace_seconds = 1" + options);
        logical = Schema.instance.getTableMetadata(keyspace(), t);
        schemaChange(PhysicalTables.derive(logical, LAYOUT_ID, TableId.generate()));
        physical = Schema.instance.getTableMetadata(keyspace(), PhysicalTables.physicalName(t, LAYOUT_ID));
        header = new LayoutHeader(LAYOUT_ID, logical.id, physical.id, keyspace(), t, physical.name, TableLayout.Transform.HASH_SPLIT, LayoutRegistry.HASH_ALGO, 4, 1);
        env = new FakeEnv();
    }

    private final class FakeEnv implements BasePurge.Env
    {
        ClusterMetadata metadata;
        boolean replica = true;
        boolean force = true;
        boolean accept = true;
        int flushes;
        int compactions;
        final List<ReportBackfill> reports = new ArrayList<>();
        final List<Long> scheduled = new ArrayList<>();
        long offset;

        @Override
        public ClusterMetadata metadata()
        {
            return metadata;
        }

        @Override
        public NodeId self(ClusterMetadata cm)
        {
            return SELF;
        }

        @Override
        public boolean isNaturalReplica(ClusterMetadata cm, TableMetadata table, DecoratedKey key)
        {
            return replica;
        }

        @Override
        public long nowMillis()
        {
            return System.currentTimeMillis() + offset;
        }

        @Override
        public BasePurge.Probe probe(TableMetadata base, DecoratedKey key)
        {
            return BasePurge.probe(base, key);
        }

        @Override
        public void flush(TableMetadata base)
        {
            flushes++;
            cfs().forceBlockingFlush(ColumnFamilyStore.FlushReason.UNIT_TESTS);
        }

        @Override
        public void compact(TableMetadata base, DecoratedKey key)
        {
            compactions++;
            CompactionManager.instance.forceCompactionForKey(cfs(), key);
        }

        @Override
        public BasePurge.RepairStates repairStates(TableMetadata base, DecoratedKey key)
        {
            int repaired = 0;
            int unrepaired = 0;
            int pending = 0;
            for (SSTableReader sstable : cfs().getLiveSSTables())
            {
                if (sstable.isPendingRepair())
                    pending++;
                else if (sstable.isRepaired())
                    repaired++;
                else
                    unrepaired++;
            }
            return new BasePurge.RepairStates(repaired, unrepaired, pending);
        }

        @Override
        public boolean onlyPurgeRepairedTombstones(TableMetadata base)
        {
            return cfs().getCompactionStrategyManager().onlyPurgeRepairedTombstones();
        }

        @Override
        public boolean forceCompaction()
        {
            return force;
        }

        @Override
        public boolean report(ReportBackfill report)
        {
            reports.add(report);
            return accept;
        }

        @Override
        public void schedule(Runnable task, long delayMillis)
        {
            scheduled.add(delayMillis);
        }

        @Override
        public void shutdown()
        {
        }

        private ColumnFamilyStore cfs()
        {
            return Keyspace.open(keyspace()).getColumnFamilyStore(logical.id);
        }
    }

    private static ByteBuffer key()
    {
        return Int32Type.instance.decompose(K);
    }

    private BasePurge purge()
    {
        return new BasePurge(env, 10, 80);
    }

    private void announce(KeyState.ReverseMode mode, Phase phase, long tt)
    {
        KeyState state = new KeyState(phase, Epoch.create(40), 1000, 1, 3, Epoch.create(40), ImmutableSet.of(), ACTION, 400, ImmutableSet.of(),
                                      KeyState.BaseState.TOMBSTONED, 500, tt, mode, phase == Phase.REVERSE_ANNOUNCED ? 0 : 900);
        LayoutRegistry registry = new LayoutRegistry(LayoutRegistry.FORMAT_VERSION_2,
                                                     ImmutableMap.of(logical.id, new TableLayout(header, 2, ImmutableMap.of(key(), state), 5)));
        env.metadata = ClusterMetadata.current().transformer().with(LayoutRegistryValue.KEY, new LayoutRegistryValue(registry)).build().metadata;
    }

    private void fill()
    {
        for (int c = 0; c < 5; c++)
            execute("INSERT INTO %s (p, c, v) VALUES (?, ?, 'x') USING TIMESTAMP 1000", K, c);
        flush();
        execute("DELETE FROM %s USING TIMESTAMP 2000 WHERE p = ?", K);
        flush();
    }

    private DecoratedKey decorated()
    {
        return logical.partitioner.decorateKey(key());
    }

    private static void sleepPastGcGrace() throws InterruptedException
    {
        Thread.sleep(2200);
    }

    @Test
    public void theProbeSeesRowsMarkersAndDeletionsAndNothingElse()
    {
        setUp("");
        assertTrue(BasePurge.probe(logical, decorated()).clean());
        execute("INSERT INTO %s (p, c, v) VALUES (?, 1, 'x') USING TIMESTAMP 1000", K);
        execute("DELETE FROM %s USING TIMESTAMP 1500 WHERE p = ? AND c > 3", K);
        BasePurge.Probe probe = BasePurge.probe(logical, decorated());
        assertFalse(probe.clean());
        assertEquals(1, probe.rows);
        assertEquals(2, probe.markers);
        assertFalse(probe.partitionDeletion);
        assertEquals(1500, probe.maxTimestamp);
        execute("DELETE FROM %s USING TIMESTAMP 2000 WHERE p = ?", K);
        probe = BasePurge.probe(logical, decorated());
        assertTrue(probe.partitionDeletion);
        assertEquals(2000, probe.partitionDeletionTimestamp);
        assertEquals(2000, probe.maxTimestamp);
    }

    @Test
    public void aNodeWaitsForGcGraceThenFlushesCompactsProbesAndReports() throws Exception
    {
        setUp("");
        fill();
        announce(KeyState.ReverseMode.SLOW, Phase.REVERSE_ANNOUNCED, 2000);
        BasePurge purge = purge();
        assertEquals(0, purge.poll());
        assertEquals("not yet: the deletion was made less than gc_grace ago", 0, env.compactions);
        assertEquals(1, purge.reasons().size());
        assertTrue(purge.reasons().get(0), purge.reasons().get(0).contains("WAITING_GC_GRACE"));
        assertFalse(BasePurge.probe(logical, decorated()).clean());

        sleepPastGcGrace();
        env.offset = 5000;
        assertEquals(1, purge.poll());
        assertEquals(1, env.compactions);
        assertTrue(env.flushes >= 1);
        assertTrue(BasePurge.probe(logical, decorated()).clean());
        assertEquals(1, env.reports.size());
        ReportBackfill report = env.reports.get(0);
        assertEquals(logical.id, report.logical);
        assertEquals(3, report.generation);
        assertEquals(Epoch.create(40), report.generationEpoch);
        assertTrue(purge.reasons().isEmpty());
    }

    @Test
    public void aCellAboveTheTombstoneIsNeverPurgedAndTheReasonNamesIt() throws Exception
    {
        setUp("");
        fill();
        long future = 4_102_444_800_000_000L;
        execute("INSERT INTO %s (p, c, v) VALUES (?, 9, 'late') USING TIMESTAMP " + future, K);
        announce(KeyState.ReverseMode.SLOW, Phase.REVERSE_ANNOUNCED, 2000);
        sleepPastGcGrace();
        BasePurge purge = purge();
        assertEquals(0, purge.poll());
        assertTrue(env.reports.isEmpty());
        String reason = purge.reasons().get(0);
        assertTrue(reason, reason.contains("RESIDUE_ABOVE_TT"));
        assertTrue(reason, reason.contains(Long.toString(future)));
        // retried with backoff, probing only: the residue is not compacted again
        int compactions = env.compactions;
        env.offset = 100_000;
        assertEquals(0, purge.poll());
        assertEquals(compactions, env.compactions);
        assertTrue(purge.reasons().get(0).contains("RESIDUE_ABOVE_TT"));
    }

    @Test
    public void theReasonIsPublishedForTheOwner() throws Exception
    {
        // T6.17f: what the other nodes read from gossip on purge_timeout
        setUp("");
        fill();
        long future = 4_102_444_800_000_000L;
        execute("INSERT INTO %s (p, c, v) VALUES (?, 9, 'late') USING TIMESTAMP " + future, K);
        announce(KeyState.ReverseMode.SLOW, Phase.REVERSE_ANNOUNCED, 2000);
        BasePurge purge = purge();
        assertEquals(0, purge.poll());
        // a node waiting for gc_grace publishes when the purge can start (the owner learns an unknown Tt from it)
        List<BasePurge.Published> waiting = BasePurge.parsePublished(purge.summary());
        assertEquals(1, waiting.size());
        assertEquals(BasePurge.Reason.WAITING_GC_GRACE, waiting.get(0).reason);
        assertTrue(waiting.get(0).detail, waiting.get(0).detail.contains("may be purged from " + waiting.get(0).purgeFromMillis));
        sleepPastGcGrace();
        assertEquals(0, purge.poll());
        List<BasePurge.Published> published = BasePurge.parsePublished(purge.summary());
        assertEquals(1, published.size());
        BasePurge.Published p = published.get(0);
        assertEquals(BasePurge.Reason.RESIDUE_ABOVE_TT, p.reason);
        assertEquals(future, p.maxTimestamp);
        assertEquals("the purge time stays published after gc_grace", waiting.get(0).purgeFromMillis, p.purgeFromMillis);
        assertEquals(ByteBufferUtil.bytesToHex(Int32Type.instance.decompose(K)), p.keyHex);
        assertTrue(p.detail, p.detail.contains("needs the operator") || p.detail.contains("residue above"));
        assertTrue(BasePurge.parsePublished("garbage\nx y Z 1").isEmpty());
    }

    @Test
    public void onlyPurgeRepairedTombstonesWithoutRepairLeavesTheDeletionAndSaysSo() throws Exception
    {
        setUp(" AND compaction = {'class': 'SizeTieredCompactionStrategy', 'only_purge_repaired_tombstones': 'true'}");
        fill();
        announce(KeyState.ReverseMode.SLOW, Phase.REVERSE_ANNOUNCED, 2000);
        sleepPastGcGrace();
        BasePurge purge = purge();
        assertEquals(0, purge.poll());
        assertEquals(1, env.compactions);
        assertTrue(env.reports.isEmpty());
        String reason = purge.reasons().get(0);
        assertTrue(reason, reason.contains("ONLY_PURGE_REPAIRED_TOMBSTONES"));
        assertFalse(BasePurge.probe(logical, decorated()).clean());
    }

    @Test
    public void theNaturalCompactionOptionOnlyProbesAndReportsOnceTheNodeHasPurged() throws Exception
    {
        setUp("");
        fill();
        env.force = false;
        announce(KeyState.ReverseMode.SLOW, Phase.REVERSE_ANNOUNCED, 2000);
        sleepPastGcGrace();
        BasePurge purge = purge();
        assertEquals(0, purge.poll());
        assertEquals(0, env.compactions);
        assertEquals(0, env.flushes);
        assertTrue(purge.reasons().get(0), purge.reasons().get(0).contains("STILL_SHADOWED"));
        assertTrue(purge.reasons().get(0), purge.reasons().get(0).contains("natural compaction"));
        // compaction happens by itself
        CompactionManager.instance.forceCompactionForKey(env.cfs(), decorated());
        env.offset = 100_000;
        assertEquals(1, purge.poll());
        assertEquals(1, env.reports.size());
    }

    @Test
    public void anEmptyPartitionIsReportedAtOnceAndOnlySlowReverseAnnouncedKeysAreOwed() throws Exception
    {
        setUp("");
        announce(KeyState.ReverseMode.SLOW, Phase.REVERSE_ANNOUNCED, 2000);
        assertTrue(purge().poll() == 1);
        assertEquals(0, env.compactions);

        for (Phase phase : Phase.values())
        {
            env.reports.clear();
            announce(KeyState.ReverseMode.SLOW, phase, 2000);
            int expected = phase == Phase.REVERSE_ANNOUNCED ? 1 : 0;
            assertEquals(phase.name(), expected, purge().poll());
        }
        // fast mode purges nothing
        env.reports.clear();
        announce(KeyState.ReverseMode.FAST, Phase.REVERSE_ANNOUNCED, 0);
        assertEquals(0, purge().poll());
        // not a replica, or already reported
        announce(KeyState.ReverseMode.SLOW, Phase.REVERSE_ANNOUNCED, 2000);
        env.replica = false;
        assertEquals(0, purge().poll());
        assertTrue(env.reports.isEmpty());
        assertTrue(BasePurge.owes(new KeyState(Phase.REVERSE_ANNOUNCED, Epoch.create(1), 1, 1, 1, Epoch.create(1), ImmutableSet.of(), ACTION, 0,
                                               ImmutableSet.of(), KeyState.BaseState.TOMBSTONED, 1, 1, KeyState.ReverseMode.SLOW, 0)));
    }

    @Test
    public void aRejectedReportIsRetriedAndAStaleKeyIsNotReported() throws Exception
    {
        setUp("");
        announce(KeyState.ReverseMode.SLOW, Phase.REVERSE_ANNOUNCED, 2000);
        env.accept = false;
        BasePurge purge = purge();
        assertEquals(0, purge.poll());
        assertEquals(1, env.reports.size());
        env.accept = true;
        assertEquals(1, purge.poll());
        assertEquals(2, env.reports.size());
    }

    /**
     * Design finding (11.5): the forced compaction does not mix repaired and unrepaired sstables, and a tombstone is
     * dropped only if no other sstable of the key has older data. The data in a repaired sstable and the deletion in
     * an unrepaired one are left as they are, until both are in the same repair state.
     */
    @Test
    public void aDeletionAndTheDataItShadowsInDifferentRepairStatesAreNotPurged() throws Exception
    {
        setUp("");
        for (int c = 0; c < 5; c++)
            execute("INSERT INTO %s (p, c, v) VALUES (?, ?, 'x') USING TIMESTAMP 1000", K, c);
        flush();
        ColumnFamilyStore cfs = env.cfs();
        List<SSTableReader> data = new ArrayList<>(cfs.getLiveSSTables());
        cfs.getCompactionStrategyManager().mutateRepaired(data, System.currentTimeMillis(), null, false);
        execute("DELETE FROM %s USING TIMESTAMP 2000 WHERE p = ?", K);
        flush();
        announce(KeyState.ReverseMode.SLOW, Phase.REVERSE_ANNOUNCED, 2000);
        sleepPastGcGrace();
        BasePurge purge = purge();
        assertEquals(0, purge.poll());
        assertEquals(1, env.compactions);
        String reason = purge.reasons().get(0);
        assertTrue(reason, reason.contains("STILL_SHADOWED"));
        assertTrue(reason, reason.contains("different repair states"));
        assertFalse(BasePurge.probe(logical, decorated()).clean());

        // once the deletion is repaired too, the next forced compaction purges both
        List<SSTableReader> unrepaired = new ArrayList<>();
        for (SSTableReader sstable : cfs.getLiveSSTables())
            if (!sstable.isRepaired())
                unrepaired.add(sstable);
        cfs.getCompactionStrategyManager().mutateRepaired(unrepaired, System.currentTimeMillis(), null, false);
        env.offset = 100_000;
        assertEquals(1, purge.poll());
        assertTrue(BasePurge.probe(logical, decorated()).clean());
    }
}
