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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.IntPredicate;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RangeTombstone;
import org.apache.cassandra.db.ReadExecutionController;
import org.apache.cassandra.db.RegularAndStaticColumns;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.transport.Dispatcher;

import static org.apache.cassandra.autonomic.layout.KeyState.BaseState.RESIDUE;
import static org.apache.cassandra.autonomic.layout.KeyState.ReverseMode.FAST;
import static org.apache.cassandra.autonomic.layout.KeyState.ReverseMode.SLOW;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * T6.17c: the reverse pass of {@link BackfillTask} on one node, from the bucket partitions of the overflow table to the
 * base partition (design 11.4). Both tables are real and the writes take the stock path; the layout registry, the
 * replica test (per bucket), the resume records and the reports are the test's.
 *
 * <p>"Copied" means: what the node holds of the base partition, read unpurged, equals what it holds in the buckets it
 * replicates, mapped back and merged, row for row and cell for cell, with the same partition deletion and the same
 * range tombstone over every clustering of a grid wider than the data. The buckets are filled by mapping a partition of
 * a twin of the logical table with {@link RowMapper#toPhysical}, so that the base partition starts out empty.
 */
public class ReverseBackfillTaskTest extends CQLTester
{
    private static final UUID LAYOUT_ID = UUID.fromString("1a2b3c4d-0000-4000-8000-000000000017");
    private static final UUID ACTION = new UUID(17, 17);
    private static final int BUCKETS = 4;
    private static final int K = 1;
    private static final NodeId SELF = new NodeId(1);
    private static final long DUAL_WRITE_SINCE = 1_000_000_000L;
    private static final long FROZEN = DUAL_WRITE_SINCE + 100_000;
    private static final long REVERSE_DUAL = FROZEN + 50_000;
    private static final long WRITE_TIMEOUT = 2000;

    private TableMetadata logical;
    private TableMetadata physical;
    private TableMetadata twin;
    private LayoutHeader header;
    private FakeEnv env;

    private void setUp(String options)
    {
        String schema = "CREATE TABLE %s (p int, c1 int, c2 int, v text, w text, m map<int, text>, PRIMARY KEY (p, c1, c2))" + options;
        String t = createTable(schema);
        disableCompaction();
        logical = Schema.instance.getTableMetadata(keyspace(), t);
        schemaChange(PhysicalTables.derive(logical, LAYOUT_ID, TableId.generate()));
        physical = Schema.instance.getTableMetadata(keyspace(), PhysicalTables.physicalName(t, LAYOUT_ID));
        disableCompaction(keyspace(), physical.name);
        // the source of the bucket content; the current table of execute() from here on
        twin = Schema.instance.getTableMetadata(keyspace(), createTable(schema));
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
        /** Whether this node replicates a bucket of the overflow table. */
        IntPredicate bucketReplica = bucket -> true;
        int chunkRows = 1000;
        final Map<String, String> state = new TreeMap<>();
        final List<ReportBackfill> reports = new ArrayList<>();
        final List<List<Mutation>> writes = new ArrayList<>();
        Runnable beforeWrite = () -> {};
        boolean acceptReports = true;
        long now = REVERSE_DUAL + 1;
        int sleeps;
        /** A sleep ends the run, as an interrupt would: for a pass whose report is rejected, which is retried forever. */
        boolean stopOnSleep;

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
            if (!table.id.equals(physical.id))
                return true;
            return bucketReplica.test(RowMapper.bucketValue(physical, key) % 256);
        }

        @Override
        public void write(List<Mutation> mutations)
        {
            beforeWrite.run();
            for (Mutation m : mutations)
                for (PartitionUpdate u : m.getPartitionUpdates())
                    assertEquals("a reverse pass writes the base table only", logical.id, u.metadata().id);
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
            return 0;
        }

        @Override
        public int chunkRows()
        {
            return chunkRows;
        }

        @Override
        public boolean sleep(long millis)
        {
            if (++sleeps > 500)
                throw new AssertionError("the task does not end");
            return !stopOnSleep;
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
    }

    private BackfillTask task()
    {
        return new BackfillTask(env, 10, 80);
    }

    private static ByteBuffer key()
    {
        return Int32Type.instance.decompose(K);
    }

    /** The key in {@code phase} of a fast reverse of incarnation 1, at generation {@code generation}. */
    private KeyState reverse(Phase phase, long phaseEpoch, int generation, long generationEpoch, KeyState.ReverseMode mode, long frozen, long dual)
    {
        return new KeyState(phase, Epoch.create(phaseEpoch), 1000, 1, generation, Epoch.create(generationEpoch), ImmutableSet.of(), ACTION,
                            DUAL_WRITE_SINCE, ImmutableSet.of(), RESIDUE, frozen, 0, mode, dual);
    }

    private KeyState reverseBackfill(int generation, long epoch)
    {
        return reverse(Phase.REVERSE_BACKFILL, epoch, generation, epoch, FAST, FROZEN, REVERSE_DUAL);
    }

    private ClusterMetadata with(KeyState state)
    {
        Map<ByteBuffer, KeyState> keys = state == null ? ImmutableMap.of() : ImmutableMap.of(key(), state);
        LayoutRegistry registry = new LayoutRegistry(LayoutRegistry.FORMAT_VERSION_2, ImmutableMap.of(logical.id, new TableLayout(header, 2, keys, 5)));
        return ClusterMetadata.current().transformer().with(LayoutRegistryValue.KEY, new LayoutRegistryValue(registry)).build().metadata;
    }

    private TableLayout layout()
    {
        return new TableLayout(header, 2, ImmutableMap.of(), 5);
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

    /** The buckets of incarnation 1 that {@code which} accepts, mapped back to the logical table and merged. */
    private PartitionUpdate overflow(IntPredicate which)
    {
        List<PartitionUpdate> buckets = new ArrayList<>();
        for (int bucket = 0; bucket < BUCKETS; bucket++)
            if (which.test(bucket))
                buckets.add(RowMapper.toLogical(held(physical, RowMapper.physicalKey(logical, physical, key(), 1, bucket)), logical));
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

    private void assertCopied(IntPredicate which)
    {
        PartitionUpdate base = base();
        PartitionUpdate copy = overflow(which);
        assertEquals(copy.partitionLevelDeletion(), base.partitionLevelDeletion());
        assertEquals(rows(copy, logical), rows(base, logical));
        assertEquals(Lists.newArrayList(copy.iterator()), Lists.newArrayList(base.iterator()));
        for (int c1 = -2; c1 <= 14; c1++)
            for (int c2 = -2; c2 <= 8; c2++)
            {
                Clustering<?> clustering = Clustering.make(Int32Type.instance.decompose(c1), Int32Type.instance.decompose(c2));
                assertEquals("range tombstone over " + clustering.toString(logical), covering(copy, clustering), covering(base, clustering));
            }
    }

    // ---- data ----

    /** Writes part {@code part} into the twin, then the twin's partition into the buckets of incarnation 1. */
    private void load(int part) throws Throwable
    {
        if (part == 0)
        {
            for (int c1 = 0; c1 < 12; c1++)
                for (int c2 = 0; c2 < 4; c2++)
                    execute("INSERT INTO %s (p, c1, c2, v, w, m) VALUES (?, ?, ?, ?, ?, ?) USING TIMESTAMP ?",
                            K, c1, c2, "v" + c1 + '.' + c2, "w", ImmutableMap.of(1, "a", 2, "b"), 1000L + c1 * 10 + c2);
            // a partition deletion while split: it is in every bucket, and shadows the four oldest rows
            execute("DELETE FROM %s USING TIMESTAMP 1005 WHERE p = ?", K);
        }
        else if (part == 1)
        {
            execute("INSERT INTO %s (p, c1, c2, v) VALUES (?, 0, 1, 'ttl') USING TIMESTAMP 2000 AND TTL 100000", K);
            execute("UPDATE %s USING TIMESTAMP 2001 AND TTL 50000 SET w = 'ttlcell' WHERE p = ? AND c1 = 0 AND c2 = 2", K);
            execute("DELETE FROM %s USING TIMESTAMP 2002 WHERE p = ? AND c1 = 1 AND c2 = 0", K);
            execute("DELETE v FROM %s USING TIMESTAMP 2003 WHERE p = ? AND c1 = 1 AND c2 = 1", K);
            execute("DELETE FROM %s USING TIMESTAMP 2004 WHERE p = ? AND c1 = 3", K);
            execute("DELETE FROM %s USING TIMESTAMP 2005 WHERE p = ? AND c1 >= 5 AND c1 < 9", K);
            execute("DELETE FROM %s USING TIMESTAMP 2006 WHERE p = ? AND c1 = 10 AND c2 > 0 AND c2 <= 2", K);
            execute("UPDATE %s USING TIMESTAMP 2007 SET m = ? WHERE p = ? AND c1 = 2 AND c2 = 0", ImmutableMap.of(7, "seven"), K);
            execute("DELETE m[1] FROM %s USING TIMESTAMP 2008 WHERE p = ? AND c1 = 2 AND c2 = 1", K);
        }
        else
        {
            for (int c1 = 5; c1 < 9; c1++)
                for (int c2 = 0; c2 < 4; c2 += c1 % 2 + 1)
                    execute("INSERT INTO %s (p, c1, c2, v) VALUES (?, ?, ?, 'after') USING TIMESTAMP 3000", K, c1, c2);
            execute("INSERT INTO %s (p, c1, c2, v) VALUES (?, 10, 1, 'after') USING TIMESTAMP 3000", K);
            execute("DELETE FROM %s USING TIMESTAMP 1500 WHERE p = ? AND c1 = 6", K);
            execute("DELETE FROM %s USING TIMESTAMP 3001 WHERE p = ? AND c1 = 7 AND c2 >= 1", K);
        }
        PartitionUpdate source = held(twin, twin.partitioner.decorateKey(key()));
        for (PartitionUpdate update : RowMapper.toPhysical(source, physical, layout(), 1).values())
            new Mutation(update).apply();
    }

    private void loadAll(boolean flushBetween) throws Throwable
    {
        for (int part = 0; part < 3; part++)
        {
            load(part);
            if (flushBetween && part < 2)
                flush(keyspace(), physical.name);
        }
    }

    private long bucketRecords()
    {
        return env.state.keySet().stream().filter(k -> k.startsWith(BackfillTask.STATE_PREFIX)).count();
    }

    // ---------------------------------------------------------------------------------------------------------
    // what is copied
    // ---------------------------------------------------------------------------------------------------------

    private void copiesEveryBucketInChunksOfEverySize(boolean flushBetween) throws Throwable
    {
        loadAll(flushBetween);
        assertFalse("the buckets hold a partition deletion", overflow(b -> true).partitionLevelDeletion().isLive());
        int generation = 4;
        for (int chunkRows : new int[]{ 1, 2, 3, 7, 1000 })
        {
            env.metadata = with(reverseBackfill(generation, 40 + generation));
            env.chunkRows = chunkRows;
            env.reports.clear();
            env.writes.clear();
            long buckets = BackfillTask.stats.bucketsCopied.get();
            task().runPending();
            assertCopied(b -> true);
            assertEquals("chunks of " + chunkRows, 1, env.reports.size());
            assertEquals(generation, env.reports.get(0).generation);
            assertEquals(Epoch.create(40 + generation), env.reports.get(0).generationEpoch);
            assertEquals(SELF, env.reports.get(0).nodeId);
            assertEquals(BUCKETS, BackfillTask.stats.bucketsCopied.get() - buckets);
            assertTrue("chunks of " + chunkRows + ": " + env.writes.size(), env.writes.size() >= (chunkRows == 1000 ? BUCKETS : 48 / (chunkRows + 2)));
            assertEquals("the records are removed with the report", 0, bucketRecords());
            generation++;
        }
        // the base partition deletion is the one of the buckets, and the rows it shadows are not in base
        assertEquals(1005, base().partitionLevelDeletion().markedForDeleteAt());
        assertTrue(execute("SELECT * FROM " + keyspace() + '.' + logical.name + " WHERE p = ? AND c1 = 0 AND c2 = 0", K).isEmpty());
    }

    @Test
    public void copiesFromMemtables() throws Throwable
    {
        setUp("");
        copiesEveryBucketInChunksOfEverySize(false);
    }

    @Test
    public void copiesFromSSTablesAndMemtables() throws Throwable
    {
        setUp("");
        copiesEveryBucketInChunksOfEverySize(true);
    }

    @Test
    public void copiesWithReversedClustering() throws Throwable
    {
        setUp(" WITH CLUSTERING ORDER BY (c1 DESC, c2 ASC)");
        copiesEveryBucketInChunksOfEverySize(true);
    }

    @Test
    public void copiesOnlyTheBucketsThisNodeReplicatesAndReportsOnce() throws Throwable
    {
        setUp("");
        loadAll(true);
        env.bucketReplica = bucket -> bucket % 2 == 0;
        env.chunkRows = 3;
        env.metadata = with(reverseBackfill(4, 40));
        task().runPending();
        assertCopied(bucket -> bucket % 2 == 0);
        assertEquals(1, env.reports.size());

        // a node that replicates no bucket owes nothing
        env.bucketReplica = bucket -> false;
        env.reports.clear();
        env.writes.clear();
        env.metadata = with(reverseBackfill(5, 50));
        task().runPending();
        assertTrue(env.writes.isEmpty());
        assertTrue(env.reports.isEmpty());
    }

    // ---------------------------------------------------------------------------------------------------------
    // records and resumption
    // ---------------------------------------------------------------------------------------------------------

    @Test
    public void aFailedWriteResumesFromTheRecordOfItsBucket() throws Throwable
    {
        setUp("");
        loadAll(false);
        env.chunkRows = 3;
        env.metadata = with(reverseBackfill(4, 40));
        task().runPending();
        int clean = env.writes.size();
        assertTrue(clean > 2 * BUCKETS);

        // the same pass in a new generation, with the write in the middle of the second bucket failing once
        env.writes.clear();
        env.reports.clear();
        env.metadata = with(reverseBackfill(5, 50));
        int[] attempts = { 0 };
        List<String> recordsAtFailure = new ArrayList<>();
        env.beforeWrite = () -> {
            if (++attempts[0] == clean / BUCKETS + 2)
            {
                recordsAtFailure.addAll(env.state.values());
                throw new RuntimeException("not acknowledged by every replica");
            }
        };
        long resumed = BackfillTask.stats.passesResumed.get();
        task().runPending();
        assertEquals("nothing is copied twice", clean, env.writes.size());
        assertEquals(1, env.reports.size());
        assertCopied(b -> true);
        // the first bucket was done, the second in the middle: both are resumed from their records
        assertTrue(recordsAtFailure.toString(), recordsAtFailure.stream().anyMatch(r -> r.endsWith(" DONE")));
        assertTrue(recordsAtFailure.toString(), recordsAtFailure.stream().anyMatch(r -> !r.endsWith(" DONE")));
        assertEquals(2, BackfillTask.stats.passesResumed.get() - resumed);
        assertEquals(0, bucketRecords());
    }

    @Test
    public void aReportThatIsRepeatedDoesNotCopyAgain() throws Throwable
    {
        setUp("");
        loadAll(false);
        env.metadata = with(reverseBackfill(4, 40));
        env.acceptReports = false;
        env.stopOnSleep = true;
        task().runPending();
        assertEquals(BUCKETS, env.writes.size());
        assertEquals("one record per bucket, each DONE", BUCKETS, env.state.values().stream().filter(r -> r.endsWith(" DONE")).count());
        assertTrue(env.state.keySet().stream().allMatch(k -> k.contains('/' + BackfillTask.REVERSE_MARK + "4/")));

        // as after a restart: a new task, the same records
        env.acceptReports = true;
        env.writes.clear();
        env.reports.clear();
        task().runPending();
        assertTrue(env.writes.isEmpty());
        assertEquals(1, env.reports.size());
        assertEquals(0, bucketRecords());
    }

    @Test
    public void aRecordOfAnotherGenerationStartsTheBucketOver() throws Throwable
    {
        setUp("");
        loadAll(false);
        KeyState state = reverseBackfill(4, 40);
        BackfillTask.Pass pass = new BackfillTask.Pass(layout(), key(), state, true, List.of(0, 1, 2, 3));
        // the same generation number of an earlier attempt (another epoch): not this pass's record
        env.state.put(pass.bucketStateKey(0), "1.39 DONE");
        env.metadata = with(state);
        task().runPending();
        assertEquals(BUCKETS, env.writes.size());
        assertCopied(b -> true);
    }

    @Test
    public void pruneKeepsTheRecordsOfTheCurrentReverseGenerationOnly() throws Throwable
    {
        setUp("");
        KeyState state = reverseBackfill(4, 40);
        BackfillTask.Pass pass = new BackfillTask.Pass(layout(), key(), state, true, List.of(0, 1));
        env.state.put(pass.bucketStateKey(0), "1.40 DONE");
        env.state.put(pass.bucketStateKey(1), "1.40 DONE");
        env.state.put(new BackfillTask.Pass(layout(), key(), reverseBackfill(3, 30), true, List.of(0)).bucketStateKey(0), "1.30 DONE");
        env.state.put(pass.stateKey(), "1.40 DONE");
        env.state.put(BackfillTask.STATE_PREFIX + "elsewhere/0", "1.1 DONE");
        env.bucketReplica = bucket -> false;
        env.metadata = with(state);
        task().runPending();
        assertEquals(ImmutableSet.of(pass.bucketStateKey(0), pass.bucketStateKey(1)), env.state.keySet());
    }

    @Test
    public void aPassWhoseGenerationIsRestartedIsAbandonedAndTheNewOneReported() throws Throwable
    {
        setUp("");
        loadAll(false);
        env.chunkRows = 3;
        env.metadata = with(reverseBackfill(4, 40));
        int[] writes = { 0 };
        env.beforeWrite = () -> {
            if (++writes[0] == 3)
                env.metadata = with(reverseBackfill(5, 50));
        };
        long abandoned = BackfillTask.stats.passesAbandoned.get();
        task().runPending();
        assertEquals(1, BackfillTask.stats.passesAbandoned.get() - abandoned);
        assertEquals(1, env.reports.size());
        assertEquals(5, env.reports.get(0).generation);
        assertCopied(b -> true);
    }

    @Test
    public void aPlacementChangeOfABucketAbandonsThePass() throws Throwable
    {
        setUp("");
        loadAll(false);
        env.chunkRows = 3;
        env.metadata = with(reverseBackfill(4, 40));
        env.acceptReports = false;
        env.stopOnSleep = true;
        int[] writes = { 0 };
        env.beforeWrite = () -> {
            if (++writes[0] == 2)
                env.bucketReplica = bucket -> bucket != 3;
        };
        long abandoned = BackfillTask.stats.passesAbandoned.get();
        task().runPending();
        assertTrue(BackfillTask.stats.passesAbandoned.get() - abandoned >= 1);
        // the pass over buckets 0..2 that follows is reported (and rejected here), never one over all four
        assertTrue(env.reports.size() >= 1);
    }

    // ---------------------------------------------------------------------------------------------------------
    // the replica limit (design 11.5, I16, I18)
    // ---------------------------------------------------------------------------------------------------------

    private static final long G = 864000 * 1000L / 2;

    @Test
    public void pastTheLimitNothingIsWrittenAndNothingReported() throws Throwable
    {
        setUp("");
        loadAll(false);
        long refused = BackfillTask.stats.refusedPastDeadline.get();
        // fast: frozenSince + G - wt
        env.metadata = with(reverseBackfill(4, 40));
        env.now = FROZEN + G - WRITE_TIMEOUT;
        task().runPending();
        assertTrue(env.writes.isEmpty());
        assertTrue(env.reports.isEmpty());
        assertTrue(base().isEmpty());
        assertTrue(BackfillTask.stats.refusedPastDeadline.get() > refused);
        assertEquals(0, bucketRecords());

        // just before it, the pass runs
        env.now = FROZEN + G - WRITE_TIMEOUT - 1;
        task().runPending();
        assertEquals(1, env.reports.size());
    }

    @Test
    public void theLimitPassingDuringThePassStopsItWithoutAReport() throws Throwable
    {
        setUp("");
        loadAll(false);
        env.chunkRows = 3;
        env.metadata = with(reverseBackfill(4, 40));
        env.beforeWrite = () -> env.now = FROZEN + G - WRITE_TIMEOUT;
        task().runPending();
        assertEquals("only the chunk judged in time", 1, env.writes.size());
        assertTrue(env.reports.isEmpty());

        // and if the limit passes after the last chunk, the copy is not vouched for
        env.writes.clear();
        env.now = REVERSE_DUAL + 1;
        env.chunkRows = 1000;
        env.metadata = with(reverseBackfill(5, 50));
        int[] writes = { 0 };
        env.beforeWrite = () -> {
            if (++writes[0] == BUCKETS)
                env.now = FROZEN + G - WRITE_TIMEOUT;
        };
        task().runPending();
        assertEquals(BUCKETS, env.writes.size());
        assertTrue(env.reports.isEmpty());
    }

    @Test
    public void slowModeCountsFromReverseDualWriteAndFastFromTheEarlierFreeze() throws Throwable
    {
        setUp("");
        loadAll(false);
        long old = 5000;
        // fast with a freeze long ago: past its window
        env.metadata = with(reverse(Phase.REVERSE_BACKFILL, 40, 4, 40, FAST, old, REVERSE_DUAL));
        task().runPending();
        assertTrue(env.writes.isEmpty());
        // slow: only reverse dual write counts
        env.metadata = with(reverse(Phase.REVERSE_BACKFILL, 50, 5, 50, SLOW, old, REVERSE_DUAL));
        task().runPending();
        assertEquals(1, env.reports.size());
        env.reports.clear();
        env.writes.clear();
        env.now = REVERSE_DUAL + G - WRITE_TIMEOUT;
        env.metadata = with(reverse(Phase.REVERSE_BACKFILL, 60, 6, 60, SLOW, old, REVERSE_DUAL));
        task().runPending();
        assertTrue(env.writes.isEmpty());
        assertTrue(env.reports.isEmpty());
        // no reverse dual write time: nothing
        env.now = REVERSE_DUAL + 1;
        env.metadata = with(reverse(Phase.REVERSE_BACKFILL, 70, 7, 70, FAST, FROZEN, 0));
        task().runPending();
        assertTrue(env.writes.isEmpty());
        assertTrue(env.reports.isEmpty());
    }

    // ---------------------------------------------------------------------------------------------------------
    // when a pass is owed
    // ---------------------------------------------------------------------------------------------------------

    @Test
    public void onlyAGenerationStartedInReverseBackfillOrReverseCleanupIsCopied() throws Throwable
    {
        setUp("");
        loadAll(false);
        for (Phase phase : new Phase[]{ Phase.REVERSE_ANNOUNCED, Phase.REVERSE_DUAL_WRITE, Phase.REVERSE_VERIFY, Phase.CUTBACK, Phase.BASE_ONLY,
                                        Phase.REVERSE_ROLLBACK })
        {
            env.metadata = with(reverse(phase, 40, 4, 40, FAST, FROZEN, phase == Phase.REVERSE_ANNOUNCED ? 0 : REVERSE_DUAL));
            task().runPending();
            assertTrue(phase.toString(), env.writes.isEmpty());
            assertTrue(phase.toString(), env.reports.isEmpty());
        }
        // REVERSE_BACKFILL entered from REVERSE_DUAL_WRITE keeps the generation of BeginUnsplit (and its purge
        // reports): not copied until the owner starts one
        env.metadata = with(reverse(Phase.REVERSE_BACKFILL, 45, 4, 40, FAST, FROZEN, REVERSE_DUAL));
        task().runPending();
        assertTrue(env.writes.isEmpty());
        // REVERSE_CLEANUP with the generation of the backfill, likewise
        env.metadata = with(reverse(Phase.REVERSE_CLEANUP, 47, 4, 40, FAST, FROZEN, REVERSE_DUAL));
        task().runPending();
        assertTrue(env.writes.isEmpty());
        assertTrue(env.reports.isEmpty());
        // a node that already reported the generation owes nothing
        env.metadata = with(new KeyState(Phase.REVERSE_BACKFILL, Epoch.create(48), 1000, 1, 5, Epoch.create(48), ImmutableSet.of(SELF), ACTION,
                                         DUAL_WRITE_SINCE, ImmutableSet.of(), RESIDUE, FROZEN, 0, FAST, REVERSE_DUAL));
        task().runPending();
        assertTrue(env.writes.isEmpty());
        // the reconcile generation of REVERSE_CLEANUP is copied
        env.metadata = with(reverse(Phase.REVERSE_CLEANUP, 49, 6, 49, FAST, FROZEN, REVERSE_DUAL));
        task().runPending();
        assertEquals(BUCKETS, env.writes.size());
        assertEquals(1, env.reports.size());
        assertCopied(b -> true);

        assertTrue(LayoutService.needsBackfill(LayoutRegistry.of(with(reverseBackfill(4, 40)))));
        assertTrue(LayoutService.needsBackfill(LayoutRegistry.of(with(reverse(Phase.REVERSE_CLEANUP, 49, 6, 49, FAST, FROZEN, REVERSE_DUAL)))));
        assertFalse(LayoutService.needsBackfill(LayoutRegistry.of(with(reverse(Phase.REVERSE_VERIFY, 49, 6, 49, FAST, FROZEN, REVERSE_DUAL)))));
    }

    // ---------------------------------------------------------------------------------------------------------
    // a bucket partition deletion is a base partition deletion (I20)
    // ---------------------------------------------------------------------------------------------------------

    @Test
    public void aBucketPartitionDeletionMapsToABasePartitionDeletion() throws Throwable
    {
        setUp("");
        // RowMapper: the deletion of a bucket partition is the deletion of the whole logical partition
        DeletionTime deletion = DeletionTime.build(9000, 1_700_000_000L);
        DecoratedKey bucket2 = RowMapper.physicalKey(logical, physical, key(), 1, 2);
        PartitionUpdate.Builder builder = new PartitionUpdate.Builder(physical, bucket2, RegularAndStaticColumns.NONE, 1);
        builder.addPartitionDeletion(deletion);
        PartitionUpdate mapped = RowMapper.toLogical(builder.build(), logical);
        assertEquals(logical.id, mapped.metadata().id);
        assertEquals(key(), mapped.partitionKey().getKey());
        assertEquals(deletion, mapped.partitionLevelDeletion());

        // the bucket partitions of the incarnation deleted after the content was written, as reverse cleanup does
        loadAll(false);
        for (int b = 0; b < BUCKETS; b++)
        {
            PartitionUpdate.Builder delete = new PartitionUpdate.Builder(physical, RowMapper.physicalKey(logical, physical, key(), 1, b),
                                                                         RegularAndStaticColumns.NONE, 1);
            delete.addPartitionDeletion(deletion);
            new Mutation(delete.build()).apply();
        }
        // I20: the deletion comes only after RemoveKey was acknowledged; then the key is absent and no pass is owed
        env.metadata = with(null);
        task().runPending();
        assertTrue(env.writes.isEmpty());
        assertTrue(base().isEmpty());

        // were a pass still to run (the order I20 forbids), it would wipe the base partition
        env.metadata = with(reverse(Phase.REVERSE_CLEANUP, 49, 6, 49, FAST, FROZEN, REVERSE_DUAL));
        task().runPending();
        assertEquals(deletion, base().partitionLevelDeletion());
        assertFalse(base().iterator().hasNext());
        assertTrue(execute("SELECT * FROM " + keyspace() + '.' + logical.name + " WHERE p = ?", K).isEmpty());
    }
}
