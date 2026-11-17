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
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.BufferClusteringBound;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.ClusteringPrefix;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.PreserveTimestamp;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * T6.7: the replica backfill on one node. The base table and the overflow table are real and the writes take the
 * stock path; the layout registry, the replica test, the resume records and the reports are the test's.
 *
 * <p>"Copied" always means: what the node holds for the partition in the overflow table, read unpurged and mapped
 * back, equals what it holds in the base table, row for row and cell for cell (timestamps, TTLs, deletion times), with
 * the same partition deletion and the same range tombstone over every clustering of a grid that is wider than the data.
 */
public class BackfillTaskTest extends CQLTester
{
    private static final UUID LAYOUT_ID = UUID.fromString("1a2b3c4d-0000-4000-8000-000000000007");
    private static final UUID ACTION = new UUID(7, 7);
    private static final int BUCKETS = 4;
    private static final int K = 1;
    private static final NodeId SELF = new NodeId(1);
    private static final long DUAL_WRITE_SINCE = 1_000_000_000L;
    private static final long WRITE_TIMEOUT = 2000;

    private TableMetadata logical;
    private TableMetadata physical;
    private LayoutHeader header;
    private FakeEnv env;

    private void setUp(String options)
    {
        String t = createTable("CREATE TABLE %s (p int, c1 int, c2 int, v text, w text, m map<int, text>, PRIMARY KEY (p, c1, c2))" + options);
        disableCompaction();
        logical = Schema.instance.getTableMetadata(keyspace(), t);
        schemaChange(PhysicalTables.derive(logical, LAYOUT_ID, TableId.generate()));
        physical = Schema.instance.getTableMetadata(keyspace(), PhysicalTables.physicalName(t, LAYOUT_ID));
        disableCompaction(keyspace(), physical.name);
        header = new LayoutHeader(LAYOUT_ID, logical.id, physical.id, keyspace(), t, physical.name,
                                  TableLayout.Transform.HASH_SPLIT, LayoutRegistry.HASH_ALGO, BUCKETS, 2);
        env = new FakeEnv();
        LayoutWrites.setForTesting(null, () -> env.metadata);
    }

    @After
    public void restore()
    {
        LayoutWrites.setForTesting(null, null);
    }

    // ---- the node, as the task sees it ----

    private final class FakeEnv implements BackfillTask.Env
    {
        ClusterMetadata metadata;
        boolean replica = true;
        int chunkRows = 1000;
        long throughput = 0;
        final Map<String, String> state = new TreeMap<>();
        final List<ReportBackfill> reports = new ArrayList<>();
        final List<Long> sleeps = new ArrayList<>();
        final List<List<Mutation>> writes = new ArrayList<>();
        /** Runs before a write is applied; may throw to fail it or change the metadata. */
        Runnable beforeWrite = () -> {};
        Runnable onSleep = () -> {};
        boolean acceptReports = true;
        int metadataLookups;
        long now = DUAL_WRITE_SINCE + 1;

        @Override
        public ClusterMetadata metadata()
        {
            metadataLookups++;
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
        public void write(List<Mutation> mutations)
        {
            beforeWrite.run();
            writes.add(mutations);
            LayoutWrites.writeUnrouted(mutations, ConsistencyLevel.ALL, Dispatcher.RequestTime.forImmediateExecution());
        }

        @Override
        public boolean report(ReportBackfill report)
        {
            reports.add(report);
            return acceptReports;
        }

        @Override
        public Optional<String> getState(String key)
        {
            return Optional.ofNullable(state.get(key));
        }

        @Override
        public boolean putState(String key, String value)
        {
            state.put(key, value);
            return true;
        }

        @Override
        public void deleteState(String key)
        {
            state.remove(key);
        }

        @Override
        public Map<String, String> scanState(String prefix)
        {
            Map<String, String> found = new TreeMap<>();
            state.forEach((k, v) -> {
                if (k.startsWith(prefix))
                    found.put(k, v);
            });
            return found;
        }

        @Override
        public long throughputBytesPerSecond()
        {
            return throughput;
        }

        @Override
        public int chunkRows()
        {
            return chunkRows;
        }

        @Override
        public long nowMillis()
        {
            return now;
        }

        @Override
        public long writeTimeoutMillis()
        {
            return WRITE_TIMEOUT;
        }

        @Override
        public boolean sleep(long millis)
        {
            sleeps.add(millis);
            if (sleeps.size() > 500)
                throw new AssertionError("the task does not end");
            onSleep.run();
            return true;
        }
    }

    private BackfillTask task()
    {
        return new BackfillTask(env, 10, 80);
    }

    private static ByteBuffer key()
    {
        return Int32Type.instance.decompose(K);
    }

    private ClusterMetadata keyIn(Phase phase, int incarnation, int generation, long generationEpoch, NodeId... done)
    {
        return with(new KeyState(phase, Epoch.create(generationEpoch), 1000, incarnation, generation, Epoch.create(generationEpoch),
                                 ImmutableSet.copyOf(done), phase == Phase.STEADY ? null : ACTION, DUAL_WRITE_SINCE));
    }

    private ClusterMetadata with(KeyState state)
    {
        int incarnation = state.incarnation;
        LayoutRegistry registry = new LayoutRegistry(LayoutRegistry.FORMAT_VERSION,
                                                     ImmutableMap.of(logical.id, new TableLayout(header, incarnation + 1, ImmutableMap.of(key(), state), 5)));
        return ClusterMetadata.current().transformer().with(LayoutRegistryValue.KEY, new LayoutRegistryValue(registry)).build().metadata;
    }

    private ClusterMetadata backfill(int incarnation)
    {
        return keyIn(Phase.BACKFILL, incarnation, 1, 30);
    }

    // ---- what the node holds ----

    private static PartitionUpdate held(TableMetadata table, DecoratedKey key)
    {
        SinglePartitionReadCommand command = SinglePartitionReadCommand.fullPartitionRead(table, BackfillTask.NO_PURGE, key);
        try (ReadExecutionController controller = command.executionController();
             UnfilteredPartitionIterator partitions = command.executeLocally(controller))
        {
            if (!partitions.hasNext())
                return PartitionUpdate.emptyUpdate(table, key);
            try (UnfilteredRowIterator partition = partitions.next())
            {
                return PartitionUpdate.fromIterator(partition, ColumnFilter.all(table));
            }
        }
    }

    private PartitionUpdate base()
    {
        return held(logical, logical.partitioner.decorateKey(key()));
    }

    /** The buckets of the incarnation, mapped back to the logical table and merged. */
    private PartitionUpdate overflow(int incarnation)
    {
        List<PartitionUpdate> buckets = new ArrayList<>();
        for (int bucket = 0; bucket < BUCKETS; bucket++)
        {
            PartitionUpdate held = held(physical, RowMapper.physicalKey(logical, physical, key(), incarnation, bucket));
            for (Row row : held)
                assertEquals("bucket of " + row.clustering().toString(physical), bucket,
                             BucketFunction.bucket(row.clustering(), 2, BUCKETS));
            buckets.add(RowMapper.toLogical(held, logical));
        }
        return PartitionUpdate.merge(buckets);
    }

    private DeletionTime covering(PartitionUpdate update, Clustering<?> clustering)
    {
        DeletionTime covering = DeletionTime.LIVE;
        for (Iterator<RangeTombstone> it = update.deletionInfo().rangeIterator(false); it.hasNext(); )
        {
            RangeTombstone range = it.next();
            if (range.deletedSlice().includes(logical.comparator, clustering) && range.deletionTime().supersedes(covering))
                covering = range.deletionTime();
        }
        return covering;
    }

    private static List<String> rows(PartitionUpdate update, TableMetadata table)
    {
        List<String> rows = new ArrayList<>();
        for (Row row : update)
            rows.add(row.toString(table, true));
        return rows;
    }

    private void assertCopied(int incarnation)
    {
        PartitionUpdate base = base();
        PartitionUpdate copy = overflow(incarnation);
        assertEquals(base.partitionLevelDeletion(), copy.partitionLevelDeletion());
        assertEquals(rows(base, logical), rows(copy, logical));
        // Row.equals: clustering, liveness, row deletion, and every cell with its timestamp, ttl and deletion time
        assertEquals(Lists.newArrayList(base.iterator()), Lists.newArrayList(copy.iterator()));
        for (int c1 = -2; c1 <= 14; c1++)
            for (int c2 = -2; c2 <= 8; c2++)
            {
                Clustering<?> clustering = Clustering.make(Int32Type.instance.decompose(c1), Int32Type.instance.decompose(c2));
                assertEquals("range tombstone over " + clustering.toString(logical), covering(base, clustering), covering(copy, clustering));
            }
    }

    private int rangeTombstones(PartitionUpdate update)
    {
        int n = 0;
        for (Iterator<RangeTombstone> it = update.deletionInfo().rangeIterator(false); it.hasNext(); it.next())
            n++;
        return n;
    }

    // ---- data ----

    /**
     * Rows 0..11 x 0..3 with every kind of content the acceptance names, written in three parts so that the test can
     * flush in between: live rows, rows and cells with a TTL, row tombstones, cell tombstones, range tombstones over
     * prefixes and inside one prefix with rows written after them, a map overwritten (a complex deletion) and a map
     * element deleted.
     */
    private void load(int part) throws Throwable
    {
        if (part == 0)
        {
            for (int c1 = 0; c1 < 12; c1++)
                for (int c2 = 0; c2 < 4; c2++)
                    execute("INSERT INTO %s (p, c1, c2, v, w, m) VALUES (?, ?, ?, ?, ?, ?) USING TIMESTAMP ?",
                            K, c1, c2, "v" + c1 + '.' + c2, "w", ImmutableMap.of(1, "a", 2, "b"), 1000L + c1 * 10 + c2);
            // another partition is never touched
            execute("INSERT INTO %s (p, c1, c2, v) VALUES (2, 1, 1, 'other') USING TIMESTAMP 1000");
        }
        else if (part == 1)
        {
            execute("INSERT INTO %s (p, c1, c2, v) VALUES (?, 0, 1, 'ttl') USING TIMESTAMP 2000 AND TTL 100000", K);
            execute("UPDATE %s USING TIMESTAMP 2001 AND TTL 50000 SET w = 'ttlcell' WHERE p = ? AND c1 = 0 AND c2 = 2", K);
            execute("DELETE FROM %s USING TIMESTAMP 2002 WHERE p = ? AND c1 = 1 AND c2 = 0", K);
            execute("DELETE v FROM %s USING TIMESTAMP 2003 WHERE p = ? AND c1 = 1 AND c2 = 1", K);
            // range tombstones: two whole prefixes, a range of prefixes, and a range inside a prefix
            execute("DELETE FROM %s USING TIMESTAMP 2004 WHERE p = ? AND c1 = 3", K);
            execute("DELETE FROM %s USING TIMESTAMP 2005 WHERE p = ? AND c1 >= 5 AND c1 < 9", K);
            execute("DELETE FROM %s USING TIMESTAMP 2006 WHERE p = ? AND c1 = 10 AND c2 > 0 AND c2 <= 2", K);
            // a collection overwrite and an element delete
            execute("UPDATE %s USING TIMESTAMP 2007 SET m = ? WHERE p = ? AND c1 = 2 AND c2 = 0", ImmutableMap.of(7, "seven"), K);
            execute("DELETE m[1] FROM %s USING TIMESTAMP 2008 WHERE p = ? AND c1 = 2 AND c2 = 1", K);
        }
        else
        {
            // rows inside the range tombstones, newer than them, so that chunks end inside an open tombstone
            for (int c1 = 5; c1 < 9; c1++)
                for (int c2 = 0; c2 < 4; c2 += c1 % 2 + 1)
                    execute("INSERT INTO %s (p, c1, c2, v) VALUES (?, ?, ?, 'after') USING TIMESTAMP 3000", K, c1, c2);
            execute("INSERT INTO %s (p, c1, c2, v) VALUES (?, 10, 1, 'after') USING TIMESTAMP 3000", K);
            // a tombstone nested in another, older and newer
            execute("DELETE FROM %s USING TIMESTAMP 1500 WHERE p = ? AND c1 = 6", K);
            execute("DELETE FROM %s USING TIMESTAMP 3001 WHERE p = ? AND c1 = 7 AND c2 >= 1", K);
        }
    }

    private void loadAll(boolean flushBetween, boolean flushLast) throws Throwable
    {
        for (int part = 0; part < 3; part++)
        {
            load(part);
            if (part < 2 ? flushBetween : flushLast)
                flush();
        }
    }

    // ---------------------------------------------------------------------------------------------------------
    // what is copied
    // ---------------------------------------------------------------------------------------------------------

    private void copiesInChunksOfEverySize(boolean flushBetween, boolean flushLast) throws Throwable
    {
        loadAll(flushBetween, flushLast);
        PartitionUpdate base = base();
        assertTrue("the data has range tombstones", rangeTombstones(base) >= 3);
        int incarnation = 1;
        for (int chunkRows : new int[]{ 1, 2, 3, 5, 7, 16, 1000 })
        {
            env.metadata = backfill(incarnation);
            env.chunkRows = chunkRows;
            env.reports.clear();
            env.writes.clear();
            task().runPending();
            assertCopied(incarnation);
            assertEquals("chunks of " + chunkRows, 1, env.reports.size());
            if (chunkRows == 1000)
                assertEquals(1, env.writes.size());
            else
                assertTrue(env.writes.size() > 48 / (chunkRows + 2));
            assertTrue("the record is removed with the report", env.state.isEmpty());
            incarnation++;
        }
    }

    @Test
    public void copiesFromMemtable() throws Throwable
    {
        setUp("");
        copiesInChunksOfEverySize(false, false);
    }

    @Test
    public void copiesFromSSTables() throws Throwable
    {
        setUp("");
        copiesInChunksOfEverySize(true, true);
    }

    @Test
    public void copiesFromSSTablesAndMemtable() throws Throwable
    {
        setUp("");
        copiesInChunksOfEverySize(true, false);
    }

    @Test
    public void copiesWithReversedClustering() throws Throwable
    {
        setUp(" WITH CLUSTERING ORDER BY (c1 DESC, c2 ASC)");
        copiesInChunksOfEverySize(true, false);
    }

    @Test
    public void reportIsForTheGenerationThatWasCopied() throws Throwable
    {
        setUp("");
        load(0);
        env.metadata = keyIn(Phase.CLEANUP, 3, 4, 77);
        task().runPending();
        assertEquals(1, env.reports.size());
        ReportBackfill report = env.reports.get(0);
        assertEquals(logical.id, report.logical);
        assertEquals(key(), report.keyBytes);
        assertEquals(4, report.generation);
        assertEquals(Epoch.create(77), report.generationEpoch);
        assertEquals(SELF, report.nodeId);
        assertCopied(3);
    }

    @Test
    public void partitionDeletionGoesToEveryBucketOnce() throws Throwable
    {
        setUp("");
        load(0);
        execute("DELETE FROM %s USING TIMESTAMP 1055 WHERE p = ?", K);
        flush();
        env.metadata = backfill(1);
        env.chunkRows = 4;
        task().runPending();
        assertCopied(1);
        assertFalse(overflow(1).partitionLevelDeletion().isLive());
        for (int bucket = 0; bucket < BUCKETS; bucket++)
            assertEquals(1055, held(physical, RowMapper.physicalKey(logical, physical, key(), 1, bucket)).partitionLevelDeletion().markedForDeleteAt());
        // only the first chunk carries it: the others write the buckets their rows are in, not all of them
        assertEquals(BUCKETS, env.writes.get(0).size());
        boolean narrower = false;
        for (List<Mutation> write : env.writes.subList(1, env.writes.size()))
        {
            narrower |= write.size() < BUCKETS;
            for (Mutation mutation : write)
                assertTrue(mutation.getPartitionUpdates().iterator().next().partitionLevelDeletion().isLive());
        }
        assertTrue(narrower);

        // a partition that is nothing but its deletion
        execute("DELETE FROM %s USING TIMESTAMP 5000 WHERE p = ?", K);
        env.metadata = backfill(2);
        env.writes.clear();
        task().runPending();
        assertEquals(1, env.writes.size());
        assertTrue(Lists.newArrayList(overflow(2).iterator()).isEmpty());
        assertEquals(5000, overflow(2).partitionLevelDeletion().markedForDeleteAt());
        assertEquals(2, env.reports.size());
    }

    @Test
    public void emptyPartitionIsReportedWithoutAWrite() throws Throwable
    {
        setUp("");
        execute("INSERT INTO %s (p, c1, c2, v) VALUES (2, 1, 1, 'other')");
        env.metadata = backfill(1);
        task().runPending();
        assertTrue(env.writes.isEmpty());
        assertEquals(1, env.reports.size());
    }

    /**
     * The I9 point. A stock local read purges tombstones older than gc_grace although the node still holds them; the
     * backfill read does not, and does not turn expired cells into tombstones either.
     */
    @Test
    public void gcAbleTombstonesAndExpiredCellsTheNodeStillHoldsAreCopied() throws Throwable
    {
        setUp(" WITH gc_grace_seconds = 0");
        // gc_grace is 0 to show the purge; the deadline that follows from it is not the subject here
        env.now = 0;
        for (int c2 = 0; c2 < 4; c2++)
            execute("INSERT INTO %s (p, c1, c2, v) VALUES (?, 1, ?, 'x') USING TIMESTAMP 1000", K, c2);
        execute("DELETE FROM %s USING TIMESTAMP 2000 WHERE p = ? AND c1 = 1 AND c2 = 0", K);
        execute("DELETE FROM %s USING TIMESTAMP 2001 WHERE p = ? AND c1 = 4", K);
        execute("INSERT INTO %s (p, c1, c2, v) VALUES (?, 1, 3, 'gone') USING TIMESTAMP 2002 AND TTL 1", K);
        execute("UPDATE %s USING TIMESTAMP 2003 SET m = ? WHERE p = ? AND c1 = 1 AND c2 = 2", ImmutableMap.of(1, "one"), K);
        flush();
        Thread.sleep(2500);

        // what a stock replica read returns for the partition now
        DecoratedKey key = logical.partitioner.decorateKey(key());
        SinglePartitionReadCommand stock = SinglePartitionReadCommand.fullPartitionRead(logical, FBUtilities.nowInSeconds(), key);
        PartitionUpdate purged;
        try (ReadExecutionController controller = stock.executionController();
             UnfilteredPartitionIterator partitions = stock.executeLocally(controller);
             UnfilteredRowIterator partition = partitions.next())
        {
            purged = PartitionUpdate.fromIterator(partition, ColumnFilter.all(logical));
        }
        assertEquals("the stock read drops the range tombstone", 0, rangeTombstones(purged));
        List<Row> purgedRows = Lists.newArrayList(purged.iterator());
        assertEquals("and the deleted row and the expired row", 2, purgedRows.size());
        assertTrue(purgedRows.get(1).deletion().isLive());
        assertFalse("and the complex deletion of the map", purgedRows.get(1).hasComplexDeletion());

        PartitionUpdate base = base();
        assertEquals(1, rangeTombstones(base));
        List<Row> held = Lists.newArrayList(base.iterator());
        assertEquals(4, held.size());
        assertFalse("row tombstone", held.get(0).deletion().isLive());
        assertTrue("complex deletion", held.get(2).hasComplexDeletion());
        assertTrue("the expired row is still an expiring row", held.get(3).primaryKeyLivenessInfo().isExpiring());
        assertEquals(1, held.get(3).primaryKeyLivenessInfo().ttl());

        env.metadata = backfill(1);
        env.chunkRows = 2;
        task().runPending();
        assertCopied(1);
        assertEquals(held, Lists.newArrayList(overflow(1).iterator()));
        assertEquals(1, rangeTombstones(overflow(1)));
    }

    // ---------------------------------------------------------------------------------------------------------
    // chunk boundaries inside a range tombstone, and resuming there
    // ---------------------------------------------------------------------------------------------------------

    @Test
    public void chunkThatEndsInsideARangeTombstoneCarriesItToItsLastRowOnly() throws Throwable
    {
        setUp("");
        // one tombstone over c1 in [2, 6], with a row in each prefix written after it
        execute("DELETE FROM %s USING TIMESTAMP 100 WHERE p = ? AND c1 >= 2 AND c1 <= 6", K);
        for (int c1 = 0; c1 < 9; c1++)
            execute("INSERT INTO %s (p, c1, c2, v) VALUES (?, ?, 0, 'x') USING TIMESTAMP 200", K, c1);
        flush();
        DecoratedKey key = logical.partitioner.decorateKey(key());

        // rows 0, 1, the opening marker, row 2: the fourth unfiltered ends the chunk inside the tombstone
        BackfillTask.Chunk first = BackfillTask.readChunk(logical, key, null, true, 4, Long.MAX_VALUE);
        assertEquals(3, first.update.rowCount());
        List<RangeTombstone> ranges = Lists.newArrayList(first.update.deletionInfo().rangeIterator(false));
        assertEquals(1, ranges.size());
        assertEquals(ClusteringBound.create(logical.comparator, true, true, 2), ranges.get(0).deletedSlice().start());
        assertEquals("closed at the last row of the chunk, inclusive", ClusteringBound.create(logical.comparator, false, true, 2, 0),
                     ranges.get(0).deletedSlice().end());
        assertEquals(100, ranges.get(0).deletionTime().markedForDeleteAt());
        assertEquals("the next chunk starts strictly after that row", ClusteringBound.create(logical.comparator, true, false, 2, 0), first.next);

        // a chunk never ends on an opening marker: a limit reached there is exceeded by one
        BackfillTask.Chunk three = BackfillTask.readChunk(logical, key, null, true, 3, Long.MAX_VALUE);
        assertEquals(3, three.update.rowCount());
        assertEquals(first.next, three.next);

        // the rest, from the recorded bound as a restarted node would read it back
        String owner = "1.30";
        BackfillTask.Resume resume = BackfillTask.Resume.parse(new BackfillTask.Resume(owner, first.next).format(), owner, logical);
        assertEquals(first.next, resume.from);
        BackfillTask.Chunk second = BackfillTask.readChunk(logical, key, resume.from, false, 1000, Long.MAX_VALUE);
        assertNull(second.next);
        assertEquals(6, second.update.rowCount());
        ranges = Lists.newArrayList(second.update.deletionInfo().rangeIterator(false));
        assertEquals(1, ranges.size());
        assertEquals("reopened where the first chunk stopped", first.next, ranges.get(0).deletedSlice().start());
        assertEquals("and closed where the base table closes it", ClusteringBound.create(logical.comparator, false, true, 6),
                     ranges.get(0).deletedSlice().end());
        assertEquals(100, ranges.get(0).deletionTime().markedForDeleteAt());

        // a chunk that ends on the closing marker resumes behind it
        BackfillTask.Chunk toClose = BackfillTask.readChunk(logical, key, resume.from, false, 6, Long.MAX_VALUE);
        assertEquals(4, toClose.update.rowCount());
        assertEquals(ClusteringBound.create(logical.comparator, true, false, 6), toClose.next);
        BackfillTask.Chunk tail = BackfillTask.readChunk(logical, key, toClose.next, false, 1000, Long.MAX_VALUE);
        assertEquals(2, tail.update.rowCount());
        assertEquals(0, rangeTombstones(tail.update));
        assertNull(tail.next);
    }

    @Test
    public void restartInsideARangeTombstoneNeitherLosesNorWidensIt() throws Throwable
    {
        setUp("");
        loadAll(true, false);
        for (int chunkRows : new int[]{ 1, 2, 3, 5 })
        {
            int incarnation = chunkRows;
            // a write fails every third time; each time the "node restarts": a new task, the records are all it has
            int[] calls = { 0 };
            List<String> resumedFrom = new ArrayList<>();
            env.metadata = backfill(incarnation);
            env.chunkRows = chunkRows;
            env.reports.clear();
            env.beforeWrite = () -> {
                if (++calls[0] % 3 == 0)
                    throw new RuntimeException("an overflow replica did not answer");
            };
            long resumed = BackfillTask.stats.passesResumed.get();
            for (int restart = 0; restart < 200 && env.reports.isEmpty(); restart++)
            {
                BackfillTask task = new BackfillTask(env, 10, 80);
                env.onSleep = () -> {
                    resumedFrom.addAll(env.state.values());
                    // the node goes down during the backoff: the thread is interrupted and the task ends
                    Thread.currentThread().interrupt();
                };
                task.runPending();
                assertTrue(Thread.interrupted() || !env.reports.isEmpty());
            }
            assertEquals(1, env.reports.size());
            assertTrue(BackfillTask.stats.passesResumed.get() - resumed >= 3);
            assertCopied(incarnation);
            // some of the restarts were inside the tombstone over c1 in [5, 9): after a row of prefix 5 to 8
            boolean inside = false;
            for (String record : resumedFrom)
            {
                BackfillTask.Resume resume = BackfillTask.Resume.parse(record, incarnation + ".30", logical);
                assertNotNull(record, resume);
                if (resume.from.size() == 2)
                {
                    int c1 = Int32Type.instance.compose(resume.from.bufferAt(0));
                    inside |= c1 >= 5 && c1 < 9;
                }
            }
            assertTrue("chunks of " + chunkRows + ": " + resumedFrom, inside);
        }
    }

    @Test
    public void resumeRecordIsOfOneGenerationOfOneAttempt() throws Throwable
    {
        setUp("");
        ClusteringBound<?> bound = ClusteringBound.create(logical.comparator, true, false, 5, 1);
        String record = new BackfillTask.Resume("2.30", bound).format();
        assertEquals(bound, BackfillTask.Resume.parse(record, "2.30", logical).from);
        assertNull("another incarnation", BackfillTask.Resume.parse(record, "3.30", logical));
        assertNull("the same generation number of an attempt that started at another epoch", BackfillTask.Resume.parse(record, "2.31", logical));
        assertNull(BackfillTask.Resume.parse("garbage", "2.30", logical));
        assertNull(BackfillTask.Resume.parse(new BackfillTask.Resume("2.30", null).format(), "2.30", logical).from);
        assertNull("all copied, but for another attempt", BackfillTask.Resume.parse(new BackfillTask.Resume("2.30", null).format(), "2.31", logical));
        assertNull(BackfillTask.Resume.parse("2.30 EXCL_START_BOUND zz", "2.30", logical));
        assertNull("not a start", BackfillTask.Resume.parse("2.30 INCL_END_BOUND x00000001", "2.30", logical));
        assertNull("more values than clustering columns", BackfillTask.Resume.parse("2.30 EXCL_START_BOUND x00000001 x00000001 x00000001", "2.30", logical));
        for (ClusteringBound<?> b : new ClusteringBound<?>[]{ ClusteringBound.create(logical.comparator, true, true, 5),
                                                             ClusteringBound.create(logical.comparator, true, false, 5),
                                                             BufferClusteringBound.create(ClusteringPrefix.Kind.INCL_START_BOUND, new ByteBuffer[0]) })
            assertEquals(b, BackfillTask.Resume.parse(new BackfillTask.Resume("1.1", b).format(), "1.1", logical).from);

        // a record of the same layout, key and generation number left by an abandoned attempt is not used
        load(0);
        env.metadata = keyIn(Phase.BACKFILL, 2, 1, 30);
        String stateKey = BackfillTask.STATE_PREFIX + LAYOUT_ID + '/' + stateKeyHash() + "/1";
        env.state.put(stateKey, new BackfillTask.Resume("1.20", ClusteringBound.create(logical.comparator, true, false, 9, 9)).format());
        long resumed = BackfillTask.stats.passesResumed.get();
        task().runPending();
        assertEquals(resumed, BackfillTask.stats.passesResumed.get());
        assertCopied(2);

        // the record of this attempt is: the pass copies what is behind it and nothing before
        env.metadata = keyIn(Phase.BACKFILL, 3, 1, 40);
        env.state.put(stateKey, new BackfillTask.Resume("3.40", ClusteringBound.create(logical.comparator, true, false, 9, 3)).format());
        task().runPending();
        assertEquals(resumed + 1, BackfillTask.stats.passesResumed.get());
        assertEquals(8, overflow(3).rowCount());
    }

    private String stateKeyHash()
    {
        String prefix = new BackfillTask.Pass(LayoutRegistry.of(env.metadata).layout(logical.id), key(),
                                              LayoutRegistry.of(env.metadata).layout(logical.id).keyState(key())).statePrefix();
        String[] parts = prefix.split("/");
        assertEquals(16, parts[2].length());
        return parts[2];
    }

    @Test
    public void recordsOfGenerationsThatAreNotCurrentArePruned() throws Throwable
    {
        setUp("");
        load(0);
        env.metadata = keyIn(Phase.BACKFILL, 1, 2, 30);
        String prefix = BackfillTask.STATE_PREFIX + LAYOUT_ID + '/' + stateKeyHash() + '/';
        env.state.put(prefix + 1, "1.20 INCL_START_BOUND");
        env.state.put(BackfillTask.STATE_PREFIX + UUID.randomUUID() + "/0123456789abcdef/1", "1.20 INCL_START_BOUND");
        env.state.put("cooldown/x", "kept");
        task().runPending();
        assertEquals(Collections.singleton("cooldown/x"), env.state.keySet());
    }

    // ---------------------------------------------------------------------------------------------------------
    // who runs, and for which generation
    // ---------------------------------------------------------------------------------------------------------

    @Test
    public void nodeThatIsNotAReplicaDoesNothing() throws Throwable
    {
        setUp("");
        load(0);
        env.metadata = backfill(1);
        env.replica = false;
        task().runPending();
        assertTrue(env.writes.isEmpty());
        assertTrue(env.reports.isEmpty());
        assertTrue(env.sleeps.isEmpty());
        assertEquals(0, overflow(1).rowCount());
    }

    @Test
    public void onlyBackfillAndCleanupAreRunAndOnlyOnce() throws Throwable
    {
        setUp("");
        load(0);
        for (Phase phase : Phase.values())
        {
            env.metadata = keyIn(phase, 1, 1, 30);
            task().runPending();
            assertEquals(phase.name(), phase == Phase.BACKFILL || phase == Phase.CLEANUP ? 1 : 0, env.reports.size());
            env.reports.clear();
        }
        // a node that is in backfillDone has nothing to do
        env.writes.clear();
        env.metadata = keyIn(Phase.BACKFILL, 1, 1, 30, SELF);
        task().runPending();
        assertTrue(env.writes.isEmpty());
        assertTrue(env.reports.isEmpty());
        env.metadata = keyIn(Phase.BACKFILL, 1, 1, 30, new NodeId(2));
        task().runPending();
        assertEquals(1, env.reports.size());
    }

    @Test
    public void writeIsRepeatedWithBackoffUntilItSucceeds() throws Throwable
    {
        setUp("");
        load(0);
        env.metadata = backfill(1);
        env.chunkRows = 10;
        int[] failures = { 6 };
        int[] calls = { 0 };
        env.beforeWrite = () -> {
            // the third chunk fails six times
            if (++calls[0] >= 3 && failures[0]-- > 0)
                throw new RuntimeException("unavailable");
        };
        long failed = BackfillTask.stats.writeFailures.get();
        task().runPending();
        assertEquals(6, BackfillTask.stats.writeFailures.get() - failed);
        assertEquals("doubling up to the bound", Lists.newArrayList(10L, 20L, 40L, 80L, 80L, 80L), env.sleeps);
        assertEquals(1, env.reports.size());
        assertCopied(1);
        // a repeated pass continues after the last acknowledged chunk: 5 chunks of 10 rows cover 48 rows
        assertEquals(5, env.writes.size());
    }

    @Test
    public void passThatWentStaleStopsAndDoesNotReport() throws Throwable
    {
        setUp("");
        load(0);
        Map<String, Supplier<ClusterMetadata>> changes = new TreeMap<>();
        changes.put("phase left", () -> keyIn(Phase.ABORTING, 1, 1, 30));
        changes.put("verify", () -> keyIn(Phase.VERIFY, 1, 1, 30));
        changes.put("key removed", () -> ClusterMetadata.current());
        changes.put("registered again", () -> keyIn(Phase.BACKFILL, 2, 1, 55, SELF));
        for (Map.Entry<String, Supplier<ClusterMetadata>> change : changes.entrySet())
        {
            env.metadata = backfill(1);
            env.chunkRows = 10;
            env.writes.clear();
            // each case is the same generation of the same attempt again: without this it would resume the last one
            env.state.clear();
            int[] calls = { 0 };
            env.beforeWrite = () -> {
                if (++calls[0] == 2)
                    env.metadata = change.getValue().get();
            };
            long abandoned = BackfillTask.stats.passesAbandoned.get();
            task().runPending();
            assertEquals(change.getKey(), 2, env.writes.size());
            assertTrue(change.getKey(), env.reports.isEmpty());
            assertTrue(change.getKey(), env.sleeps.isEmpty());
            assertEquals(change.getKey(), 1, BackfillTask.stats.passesAbandoned.get() - abandoned);
        }
    }

    @Test
    public void changeBetweenTheLastChunkAndTheReportIsSeen() throws Throwable
    {
        setUp("");
        load(0);
        env.metadata = backfill(1);
        // the registry moves on while the only chunk is being written
        env.beforeWrite = () -> env.metadata = keyIn(Phase.BACKFILL, 1, 2, 31);
        task().runPending();
        // generation 1 is not reported; generation 2 is run from the beginning and reported
        assertEquals(1, env.reports.size());
        assertEquals(2, env.reports.get(0).generation);
        assertEquals(Epoch.create(31), env.reports.get(0).generationEpoch);
        assertEquals(2, env.writes.size());
    }

    @Test
    public void newGenerationStartsFromTheBeginning() throws Throwable
    {
        setUp("");
        load(0);
        env.metadata = backfill(1);
        env.chunkRows = 10;
        int[] calls = { 0 };
        env.beforeWrite = () -> {
            if (++calls[0] == 4)
                env.metadata = keyIn(Phase.BACKFILL, 1, 2, 31);
        };
        task().runPending();
        // 4 chunks of generation 1 (the fourth written when the restart was committed), then all 5 of generation 2
        assertEquals(9, env.writes.size());
        assertEquals(1, env.reports.size());
        assertEquals(2, env.reports.get(0).generation);
        assertTrue(env.state.isEmpty());
    }

    @Test
    public void rejectedReportIsRepeatedLaterWithoutCopyingAgain() throws Throwable
    {
        setUp("");
        load(0);
        env.metadata = backfill(1);
        env.chunkRows = 10;
        // rejected although the pass is current as far as this node knows, twice
        env.acceptReports = false;
        env.onSleep = () -> env.acceptReports = env.sleeps.size() == 2;
        long resumed = BackfillTask.stats.passesResumed.get();
        task().runPending();
        assertEquals(3, env.reports.size());
        assertEquals(Lists.newArrayList(10L, 20L), env.sleeps);
        assertEquals("the partition is copied once", 5, env.writes.size());
        assertEquals(2, BackfillTask.stats.passesResumed.get() - resumed);
        assertTrue(BackfillTask.stats.lastResumeRecord, BackfillTask.stats.lastResumeRecord.endsWith(" DONE"));
        assertTrue(env.state.isEmpty());
    }

    @Test
    public void reportThatCannotBeCommittedIsRepeated() throws Throwable
    {
        setUp("");
        load(0);
        env.metadata = backfill(1);
        int[] failures = { 2 };
        BackfillTask.Env failing = new DelegatingEnv(env)
        {
            @Override
            public boolean report(ReportBackfill report)
            {
                if (failures[0]-- > 0)
                    throw new IllegalStateException("the CMS does not answer");
                return super.report(report);
            }
        };
        new BackfillTask(failing, 10, 80).runPending();
        assertEquals(Lists.newArrayList(10L, 20L), env.sleeps);
        assertEquals(1, env.reports.size());
        assertEquals("the partition is copied once", 1, env.writes.size());
    }

    /** Forwards everything; a test overrides what it wants to fail. */
    private static class DelegatingEnv implements BackfillTask.Env
    {
        private final BackfillTask.Env to;

        DelegatingEnv(BackfillTask.Env to)
        {
            this.to = to;
        }

        @Override
        public ClusterMetadata metadata()
        {
            return to.metadata();
        }

        @Override
        public NodeId self(ClusterMetadata cm)
        {
            return to.self(cm);
        }

        @Override
        public boolean isNaturalReplica(ClusterMetadata cm, TableMetadata logical, DecoratedKey key)
        {
            return to.isNaturalReplica(cm, logical, key);
        }

        @Override
        public void write(List<Mutation> mutations)
        {
            to.write(mutations);
        }

        @Override
        public boolean report(ReportBackfill report)
        {
            return to.report(report);
        }

        @Override
        public Optional<String> getState(String key)
        {
            return to.getState(key);
        }

        @Override
        public boolean putState(String key, String value)
        {
            return to.putState(key, value);
        }

        @Override
        public void deleteState(String key)
        {
            to.deleteState(key);
        }

        @Override
        public Map<String, String> scanState(String prefix)
        {
            return to.scanState(prefix);
        }

        @Override
        public long throughputBytesPerSecond()
        {
            return to.throughputBytesPerSecond();
        }

        @Override
        public int chunkRows()
        {
            return to.chunkRows();
        }

        @Override
        public boolean sleep(long millis)
        {
            return to.sleep(millis);
        }

        @Override
        public long nowMillis()
        {
            return to.nowMillis();
        }

        @Override
        public long writeTimeoutMillis()
        {
            return to.writeTimeoutMillis();
        }
    }

    @Test
    public void stalledKeyDoesNotHoldUpAnother() throws Throwable
    {
        setUp("");
        load(0);
        execute("INSERT INTO %s (p, c1, c2, v) VALUES (2, 2, 2, 'two') USING TIMESTAMP 1000");
        ByteBuffer k2 = Int32Type.instance.decompose(2);
        KeyState one = new KeyState(Phase.BACKFILL, Epoch.create(30), 1000, 1, 1, Epoch.create(30), ImmutableSet.of(), ACTION, DUAL_WRITE_SINCE);
        KeyState two = new KeyState(Phase.BACKFILL, Epoch.create(31), 1000, 2, 1, Epoch.create(31), ImmutableSet.of(), ACTION, DUAL_WRITE_SINCE);
        LayoutRegistry registry = new LayoutRegistry(LayoutRegistry.FORMAT_VERSION,
                                                     ImmutableMap.of(logical.id, new TableLayout(header, 3, ImmutableMap.of(key(), one, k2, two), 5)));
        env.metadata = ClusterMetadata.current().transformer().with(LayoutRegistryValue.KEY, new LayoutRegistryValue(registry)).build().metadata;
        // the writes of key 1 fail until the third sleep
        boolean[] down = { true };
        BackfillTask.Env partial = new DelegatingEnv(env)
        {
            @Override
            public void write(List<Mutation> mutations)
            {
                if (down[0] && mutations.get(0).getPartitionUpdates().iterator().next().rowCount() > 1)
                    throw new RuntimeException("unavailable");
                super.write(mutations);
            }
        };
        env.onSleep = () -> {
            assertEquals("key 2 was reported while key 1 waits", 1, env.reports.size());
            assertEquals(k2, env.reports.get(0).keyBytes);
            if (env.sleeps.size() == 3)
                down[0] = false;
        };
        new BackfillTask(partial, 10, 80).runPending();
        assertEquals(3, env.sleeps.size());
        assertEquals(2, env.reports.size());
        assertEquals(key(), env.reports.get(1).keyBytes);
        assertCopied(1);
    }

    // ---------------------------------------------------------------------------------------------------------
    // the tombstone deadline (I9, ADR-031) and the reconcile generation
    // ---------------------------------------------------------------------------------------------------------

    /** gc_grace_seconds 1000: nothing is copied later than 500 s after dual write started, less the write timeout. */
    private static final long LIMIT = DUAL_WRITE_SINCE + 500_000 - WRITE_TIMEOUT;

    @Test
    public void passThatCrossesTheDeadlineStopsBeforeTheNextChunkAndDoesNotReport() throws Throwable
    {
        setUp(" WITH gc_grace_seconds = 1000");
        load(0);
        env.metadata = backfill(1);
        env.chunkRows = 10;
        env.now = LIMIT - 1;
        int[] calls = { 0 };
        // the clock passes the limit while the third chunk is being written
        env.beforeWrite = () -> {
            if (++calls[0] == 3)
                env.now = LIMIT;
        };
        long refused = BackfillTask.stats.refusedPastDeadline.get();
        task().runPending();
        assertEquals("the chunk in flight is the last", 3, env.writes.size());
        assertTrue(env.reports.isEmpty());
        assertTrue("it is not retried", env.sleeps.isEmpty());
        assertEquals(1, BackfillTask.stats.refusedPastDeadline.get() - refused);
        assertEquals(30, overflow(1).rowCount());

        // not later either, by the same task or after a restart, for as long as the key is in this incarnation
        BackfillTask sameTask = task();
        for (BackfillTask task : new BackfillTask[]{ sameTask, sameTask, task() })
        {
            env.now += 1000;
            task.runPending();
        }
        env.metadata = keyIn(Phase.BACKFILL, 1, 2, 31);
        task().runPending();
        assertEquals(3, env.writes.size());
        assertTrue(env.reports.isEmpty());
        assertTrue(env.sleeps.isEmpty());

        // one millisecond earlier a chunk is still written
        env.metadata = backfill(2);
        env.now = LIMIT - 1;
        env.writes.clear();
        task().runPending();
        assertEquals(5, env.writes.size());
        assertEquals(1, env.reports.size());
    }

    @Test
    public void passWhoseLastChunkCrossesTheDeadlineDoesNotReport() throws Throwable
    {
        setUp(" WITH gc_grace_seconds = 1000");
        load(0);
        env.metadata = backfill(1);
        env.now = LIMIT - 1;
        env.beforeWrite = () -> env.now = LIMIT + 5;
        task().runPending();
        assertEquals(1, env.writes.size());
        assertTrue(env.reports.isEmpty());
        // nor does a pass that had copied everything and only has its report left
        env.beforeWrite = () -> {};
        task().runPending();
        assertEquals(1, env.writes.size());
        assertTrue(env.reports.isEmpty());
        assertTrue(env.sleeps.isEmpty());
    }

    @Test
    public void nothingIsCopiedIfTheRegistryDoesNotSayWhenDualWriteStarted() throws Throwable
    {
        setUp("");
        load(0);
        env.metadata = with(new KeyState(Phase.BACKFILL, Epoch.create(30), 1000, 1, 1, Epoch.create(30), ImmutableSet.of(), ACTION));
        long refused = BackfillTask.stats.refusedPastDeadline.get();
        task().runPending();
        assertTrue(env.writes.isEmpty());
        assertTrue(env.reports.isEmpty());
        assertEquals(1, BackfillTask.stats.refusedPastDeadline.get() - refused);
    }

    @Test
    public void cleanupWaitsForTheReconcileGeneration() throws Throwable
    {
        setUp("");
        load(0);
        // CLEANUP entered at epoch 50 with generation 1 of the backfill, started at epoch 30; this node is not in backfillDone
        env.metadata = with(new KeyState(Phase.CLEANUP, Epoch.create(50), 1000, 1, 1, Epoch.create(30), ImmutableSet.of(), ACTION, DUAL_WRITE_SINCE));
        task().runPending();
        assertTrue(env.writes.isEmpty());
        assertTrue(env.reports.isEmpty());
        assertEquals(0, overflow(1).rowCount());
        // the owner starts the reconcile generation: phase epoch and generation epoch are the same
        env.metadata = with(new KeyState(Phase.CLEANUP, Epoch.create(51), 2000, 1, 2, Epoch.create(51), ImmutableSet.of(), ACTION, DUAL_WRITE_SINCE));
        task().runPending();
        assertEquals(1, env.reports.size());
        assertEquals(2, env.reports.get(0).generation);
        assertCopied(1);
    }

    // ---------------------------------------------------------------------------------------------------------
    // the write path and the throttle
    // ---------------------------------------------------------------------------------------------------------

    @Test
    public void backfillWritesAreNotRouted() throws Throwable
    {
        setUp("");
        load(0);
        env.metadata = backfill(1);
        PartitionUpdate chunk = BackfillTask.readChunk(logical, logical.partitioner.decorateKey(key()), null, true, 5, Long.MAX_VALUE).update;
        List<Mutation> mutations = new ArrayList<>();
        for (PartitionUpdate update : RowMapper.toPhysical(chunk, physical, LayoutRegistry.of(env.metadata).layout(logical.id), 1).values())
            mutations.add(new Mutation(update));
        try
        {
            StorageProxy.mutateWithTriggers(mutations, ConsistencyLevel.ALL, false, Dispatcher.RequestTime.forImmediateExecution(), PreserveTimestamp.yes);
            fail("a direct write of the overflow table is rejected by the routing");
        }
        catch (InvalidRequestException e)
        {
            assertTrue(e.getMessage(), e.getMessage().contains("cannot be written directly"));
        }
        assertEquals(0, overflow(1).rowCount());
        LayoutWrites.writeUnrouted(mutations, ConsistencyLevel.ALL, Dispatcher.RequestTime.forImmediateExecution());
        assertEquals(5, overflow(1).rowCount());
        // the flag is the thread's and is gone afterwards
        try
        {
            StorageProxy.mutateWithTriggers(mutations, ConsistencyLevel.ALL, false, Dispatcher.RequestTime.forImmediateExecution(), PreserveTimestamp.yes);
            fail();
        }
        catch (InvalidRequestException e)
        {
            // expected
        }
    }

    @Test
    public void throughputIsBounded() throws Throwable
    {
        setUp("");
        String big = "x".repeat(1000);
        for (int c1 = 0; c1 < 40; c1++)
            execute("INSERT INTO %s (p, c1, c2, v) VALUES (?, ?, 0, ?) USING TIMESTAMP 1000", K, c1, big);
        env.chunkRows = 5;

        env.metadata = backfill(1);
        long written = BackfillTask.stats.bytesWritten.get();
        long start = System.nanoTime();
        task().runPending();
        long unthrottled = System.nanoTime() - start;
        long bytes = BackfillTask.stats.bytesWritten.get() - written;
        assertTrue("about 40 KiB: " + bytes, bytes > 40_000 && bytes < 60_000);

        // 20 kB/s, 8 chunks of about 5 kB: nothing waits for the first, and nothing is written after the last
        env.metadata = backfill(2);
        env.throughput = 20_000;
        long waited = BackfillTask.stats.throttledMicros.get();
        start = System.nanoTime();
        task().runPending();
        long throttled = System.nanoTime() - start;
        assertCopied(2);
        double expectedSeconds = bytes * 6 / 8.0 / 20_000;
        assertTrue("took " + throttled / 1_000_000 + " ms, unthrottled " + unthrottled / 1_000_000 + " ms",
                   throttled >= (long) (expectedSeconds * 0.8 * 1_000_000_000L) && throttled > unthrottled);
        assertTrue(BackfillTask.stats.throttledMicros.get() - waited >= (long) (expectedSeconds * 0.7 * 1_000_000));
    }

    @Test
    public void singleNodeIsANaturalReplicaOfEveryKey() throws Throwable
    {
        setUp("");
        assertTrue(BackfillTask.isNaturalReplica(ClusterMetadata.current(), logical, logical.partitioner.decorateKey(key())));
    }
}
