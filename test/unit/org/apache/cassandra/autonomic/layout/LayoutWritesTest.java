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
import java.util.AbstractList;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.PreserveTimestamp;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.transport.Dispatcher;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * T6.3: {@link LayoutWrites} against a registry injected into test metadata, with the calls it makes to
 * {@code StorageProxy} recorded instead of executed.
 *
 * <p>A recorded call reads {@code plain|atomic CL [mutations]}: {@code plain} is the stock tail without the batchlog
 * ({@code StorageProxy.mutate}), {@code atomic} with it ({@code StorageProxy.mutateAtomically}). A mutation reads
 * {@code t(key)} for the base table, {@code o(key/bucket)} for the overflow table, {@code u(key)} for an unrelated
 * table, followed by the number of rows, {@code R} per range tombstone and {@code P} for a partition deletion.
 */
public class LayoutWritesTest extends CQLTester
{
    private static final UUID LAYOUT_ID = UUID.fromString("1a2b3c4d-0000-4000-8000-000000000003");
    private static final UUID ACTION = new UUID(7, 7);
    private static final int BUCKETS = 4;
    private static final int INCARNATION = 3;
    private static final int K = 7;
    private static final Dispatcher.RequestTime TIME = Dispatcher.RequestTime.forImmediateExecution();

    private TableMetadata logical;
    private TableMetadata physical;
    private TableMetadata unrelated;
    private LayoutHeader header;
    private final Recorder recorder = new Recorder();
    private final Metadata metadata = new Metadata();

    /** Records the calls; optionally runs something inside a call, where the re-entrancy flag is set. */
    private final class Recorder implements LayoutWrites.Sink
    {
        final List<String> calls = new ArrayList<>();
        final List<List<? extends IMutation>> lists = new ArrayList<>();
        Runnable inside = () -> {};

        @Override
        public void write(List<? extends IMutation> mutations, ConsistencyLevel consistencyLevel, boolean atomic,
                          Dispatcher.RequestTime requestTime, PreserveTimestamp preserveTimestamps)
        {
            List<String> described = new ArrayList<>();
            for (IMutation m : mutations)
                described.add(describe(m));
            calls.add((atomic ? "atomic " : "plain ") + consistencyLevel + ' ' + described);
            lists.add(mutations);
            assertSame(TIME, requestTime);
            assertSame(PreserveTimestamp.yes, preserveTimestamps);
            inside.run();
        }

        @Override
        public void truncate(String keyspace, String table)
        {
            calls.add("truncate " + (table.equals(physical.name) ? "overflow" : table));
            inside.run();
        }
    }

    /** The metadata the class under test sees: the queued ones in order, the last one for ever; lookups are counted. */
    private static final class Metadata implements Supplier<ClusterMetadata>
    {
        final Deque<ClusterMetadata> queue = new ArrayDeque<>();
        int lookups;

        @Override
        public ClusterMetadata get()
        {
            lookups++;
            return queue.size() > 1 ? queue.poll() : queue.peek();
        }

        void set(ClusterMetadata... sequence)
        {
            queue.clear();
            queue.addAll(Arrays.asList(sequence));
            lookups = 0;
        }
    }

    @Before
    public void setUp() throws Throwable
    {
        String t = createTable("CREATE TABLE %s (p int, c int, v int, PRIMARY KEY (p, c))");
        logical = Schema.instance.getTableMetadata(keyspace(), t);
        schemaChange(PhysicalTables.derive(logical, LAYOUT_ID, TableId.generate()));
        physical = Schema.instance.getTableMetadata(keyspace(), PhysicalTables.physicalName(t, LAYOUT_ID));
        String u = createTable("CREATE TABLE %s (p int, c int, v int, PRIMARY KEY (p, c))");
        unrelated = Schema.instance.getTableMetadata(keyspace(), u);
        header = new LayoutHeader(LAYOUT_ID, logical.id, physical.id, keyspace(), t, physical.name,
                                  TableLayout.Transform.HASH_SPLIT, LayoutRegistry.HASH_ALGO, BUCKETS, 1);
        recorder.calls.clear();
        recorder.lists.clear();
        recorder.inside = () -> {};
        LayoutWrites.setForTesting(recorder, metadata);
    }

    @After
    public void restore()
    {
        LayoutWrites.setForTesting(null, null);
    }

    // ---- metadata ----

    private static ByteBuffer key(int p)
    {
        return Int32Type.instance.decompose(p);
    }

    private static KeyState state(Phase phase)
    {
        return new KeyState(phase, Epoch.create(10 + phase.id), 1000, INCARNATION, 1, Epoch.create(10), ImmutableSet.of(),
                            phase == Phase.STEADY ? null : ACTION);
    }

    private ClusterMetadata with(LayoutHeader h, long version, Map<ByteBuffer, KeyState> keys)
    {
        LayoutRegistry registry = keys == null
                                  ? LayoutRegistry.EMPTY
                                  : new LayoutRegistry(LayoutRegistry.FORMAT_VERSION, ImmutableMap.of(h.logical, new TableLayout(h, INCARNATION + 1, keys, version)));
        return ClusterMetadata.current().transformer().with(LayoutRegistryValue.KEY, new LayoutRegistryValue(registry)).build().metadata;
    }

    /** Metadata with key {@link #K} in the given phase. */
    private ClusterMetadata keyIn(Phase phase)
    {
        return with(header, 5, ImmutableMap.of(key(K), state(phase)));
    }

    /** Metadata with a layout that has a key, but not {@link #K}. */
    private ClusterMetadata otherKeyOnly()
    {
        return with(header, 5, ImmutableMap.of(key(1000), state(Phase.BACKFILL)));
    }

    private ClusterMetadata emptyRegistry()
    {
        return with(header, 0, null);
    }

    // ---- mutations ----

    private static int bucket(int c)
    {
        return BucketFunction.bucket(Clustering.make(Int32Type.instance.decompose(c)), 1, BUCKETS);
    }

    private static Mutation rows(TableMetadata table, int p, int... cs)
    {
        PartitionUpdate.SimpleBuilder b = PartitionUpdate.simpleBuilder(table, p).timestamp(50);
        for (int c : cs)
            b.row(c).add("v", c);
        return new Mutation(b.build());
    }

    private Mutation rangeDelete(int p)
    {
        PartitionUpdate.SimpleBuilder b = PartitionUpdate.simpleBuilder(logical, p).timestamp(50);
        b.addRangeTombstone().start(5).inclStart().end(20).exclEnd();
        return new Mutation(b.build());
    }

    private Mutation partitionDelete(int p)
    {
        return new Mutation(PartitionUpdate.simpleBuilder(logical, p).timestamp(50).delete().build());
    }

    private String describe(IMutation mutation)
    {
        List<String> parts = new ArrayList<>();
        for (PartitionUpdate u : ((Mutation) mutation).getPartitionUpdates())
        {
            StringBuilder sb = new StringBuilder();
            if (u.metadata().id.equals(physical.id))
            {
                int value = RowMapper.bucketValue(physical, u.partitionKey());
                assertEquals("incarnation", INCARNATION, value / BucketFunction.BUCKET_RADIX);
                ByteBuffer logicalKey = RowMapper.toLogical(u, logical).partitionKey().getKey();
                sb.append("o(").append(Int32Type.instance.compose(logicalKey)).append('/').append(value % BucketFunction.BUCKET_RADIX).append(')');
            }
            else
            {
                sb.append(u.metadata().id.equals(logical.id) ? "t(" : "u(").append(Int32Type.instance.compose(u.partitionKey().getKey())).append(')');
            }
            sb.append(u.rowCount());
            for (int i = 0; i < u.deletionInfo().rangeCount(); i++)
                sb.append('R');
            if (!u.partitionLevelDeletion().isLive())
                sb.append('P');
            parts.add(sb.toString());
        }
        return parts.size() == 1 ? parts.get(0) : parts.toString();
    }

    private static String shadow(int c)
    {
        return "o(" + K + '/' + bucket(c) + ")1";
    }

    /** The shadows of a delete of a range or of the partition: one per bucket. */
    private static String everyBucket(String suffix)
    {
        List<String> all = new ArrayList<>();
        for (int b = 0; b < BUCKETS; b++)
            all.add("o(" + K + '/' + b + ")0" + suffix);
        return String.join(", ", all);
    }

    private boolean route(List<? extends IMutation> mutations, ConsistencyLevel cl, boolean atomic)
    {
        return LayoutWrites.route(mutations, cl, atomic, TIME, PreserveTimestamp.yes);
    }

    private void assertCalls(String... expected)
    {
        assertEquals(String.join("\n", expected), String.join("\n", recorder.calls));
    }

    /** Routes under the phase and returns the recorded calls, which must have been validated by a second lookup. */
    private void routed(Phase phase, boolean atomic, IMutation... mutations)
    {
        recorder.calls.clear();
        recorder.lists.clear();
        metadata.set(keyIn(phase));
        assertTrue(route(Arrays.asList(mutations), ConsistencyLevel.QUORUM, atomic));
        assertEquals("one lookup before and one after", 2, metadata.lookups);
    }

    // ---- fast path ----

    /** A list that cannot be looked at. */
    private static final class Untouchable extends AbstractList<IMutation>
    {
        @Override
        public IMutation get(int index)
        {
            throw new AssertionError("the mutations were inspected");
        }

        @Override
        public int size()
        {
            throw new AssertionError("the mutations were inspected");
        }
    }

    @Test
    public void emptyRegistryCostsOneLookup() throws Exception
    {
        for (ClusterMetadata md : new ClusterMetadata[]{ emptyRegistry(), ClusterMetadata.current() })
        {
            metadata.set(md);
            assertFalse(route(new Untouchable(), ConsistencyLevel.ANY, false));
            assertEquals(1, metadata.lookups);

            metadata.set(md);
            LayoutWrites.checkCas(keyspace(), logical.name, logical.partitioner.decorateKey(key(K)));
            assertEquals(1, metadata.lookups);

            metadata.set(md);
            LayoutWrites.beforeTruncate(keyspace(), logical.name);
            assertEquals(1, metadata.lookups);
        }
        assertCalls();
    }

    @Test
    public void keysThatAreNotInTheRegistryTakeTheStockPath()
    {
        metadata.set(otherKeyOnly());
        assertFalse(route(List.of(rows(logical, K, 1), rows(unrelated, K, 1), rangeDelete(K)), ConsistencyLevel.ANY, false));
        assertFalse(route(List.of(rows(logical, K, 1)), ConsistencyLevel.ANY, true));
        // decision: nothing routed, nothing validated
        assertEquals(2, metadata.lookups);
        assertCalls();
    }

    @Test
    public void orphanLayoutIsIgnored()
    {
        // the registry names a logical table id that is not in the schema
        LayoutHeader orphan = new LayoutHeader(LAYOUT_ID, TableId.generate(), physical.id, keyspace(), logical.name, physical.name,
                                               TableLayout.Transform.HASH_SPLIT, LayoutRegistry.HASH_ALGO, BUCKETS, 1);
        metadata.set(with(orphan, 5, ImmutableMap.of(key(K), state(Phase.NEW_ONLY))));
        assertFalse(route(List.of(rows(logical, K, 1)), ConsistencyLevel.ONE, false));
        LayoutWrites.checkCas(keyspace(), logical.name, logical.partitioner.decorateKey(key(K)));
        assertCalls();
    }

    // ---- class table of design 3.3 ----

    @Test
    public void classC0WritesTheOriginalAndValidates()
    {
        for (Phase phase : new Phase[]{ Phase.ANNOUNCED, Phase.ABORTING })
        {
            for (boolean atomic : new boolean[]{ false, true })
            {
                String kind = atomic ? "atomic" : "plain";
                routed(phase, atomic, rows(logical, K, 1));
                assertCalls(kind + " QUORUM [t(7)1]");
                routed(phase, atomic, rows(logical, K, 1, 2), rows(logical, 8, 1), rows(unrelated, K, 1));
                assertCalls(kind + " QUORUM [t(7)2, t(8)1, u(7)1]");
                routed(phase, atomic, rangeDelete(K));
                assertCalls(kind + " QUORUM [t(7)0R]");
                routed(phase, atomic, partitionDelete(K));
                assertCalls(kind + " QUORUM [t(7)0P]");
            }
        }
    }

    @Test
    public void classC0PassesTheCallersListUnchanged()
    {
        List<IMutation> original = List.of(rows(logical, K, 1));
        metadata.set(keyIn(Phase.ANNOUNCED));
        assertTrue(route(original, ConsistencyLevel.QUORUM, false));
        assertSame(original, recorder.lists.get(0));
    }

    @Test
    public void classC1WritesShadowsThenTheOriginal()
    {
        for (Phase phase : new Phase[]{ Phase.DUAL_WRITE, Phase.BACKFILL, Phase.VERIFY, Phase.ROLLBACK_DUAL })
        {
            // single statement, one row
            routed(phase, false, rows(logical, K, 1));
            assertCalls("plain QUORUM [" + shadow(1) + "]",
                        "plain QUORUM [t(7)1]");
            // unlogged batch: a split key, another key of the table, another table
            routed(phase, false, rows(logical, K, 1), rows(logical, 8, 1), rows(unrelated, K, 1));
            assertCalls("plain QUORUM [" + shadow(1) + "]",
                        "plain QUORUM [t(7)1, t(8)1, u(7)1]");
            // logged batch
            routed(phase, true, rows(logical, K, 1), rows(logical, 8, 1), rows(unrelated, K, 1));
            assertCalls("atomic QUORUM [t(7)1, t(8)1, u(7)1, " + shadow(1) + "]");
            // range delete and partition delete: every bucket, through the batchlog
            routed(phase, false, rangeDelete(K));
            assertCalls("atomic QUORUM [" + everyBucket("R") + "]",
                        "plain QUORUM [t(7)0R]");
            routed(phase, false, partitionDelete(K));
            assertCalls("atomic QUORUM [" + everyBucket("P") + "]",
                        "plain QUORUM [t(7)0P]");
            routed(phase, true, rangeDelete(K));
            assertCalls("atomic QUORUM [t(7)0R, " + everyBucket("R") + "]");
            routed(phase, true, partitionDelete(K));
            assertCalls("atomic QUORUM [t(7)0P, " + everyBucket("P") + "]");
        }
    }

    @Test
    public void classC2WritesTheOriginalThenShadows()
    {
        routed(Phase.CUTOVER, false, rows(logical, K, 1));
        assertCalls("plain QUORUM [t(7)1]",
                    "plain QUORUM [" + shadow(1) + "]");
        routed(Phase.CUTOVER, false, rows(logical, K, 1), rows(logical, 8, 1), rows(unrelated, K, 1));
        assertCalls("plain QUORUM [t(7)1, t(8)1, u(7)1]",
                    "plain QUORUM [" + shadow(1) + "]");
        routed(Phase.CUTOVER, true, rows(logical, K, 1), rows(logical, 8, 1), rows(unrelated, K, 1));
        assertCalls("atomic QUORUM [t(7)1, t(8)1, u(7)1, " + shadow(1) + "]");
        routed(Phase.CUTOVER, false, rangeDelete(K));
        assertCalls("plain QUORUM [t(7)0R]",
                    "atomic QUORUM [" + everyBucket("R") + "]");
        routed(Phase.CUTOVER, false, partitionDelete(K));
        assertCalls("plain QUORUM [t(7)0P]",
                    "atomic QUORUM [" + everyBucket("P") + "]");
        routed(Phase.CUTOVER, true, rangeDelete(K));
        assertCalls("atomic QUORUM [t(7)0R, " + everyBucket("R") + "]");
        routed(Phase.CUTOVER, true, partitionDelete(K));
        assertCalls("atomic QUORUM [t(7)0P, " + everyBucket("P") + "]");
    }

    @Test
    public void classC3WritesShadowsOnly()
    {
        for (Phase phase : new Phase[]{ Phase.NEW_ONLY, Phase.CLEANUP, Phase.STEADY })
        {
            routed(phase, false, rows(logical, K, 1));
            assertCalls("plain QUORUM [" + shadow(1) + "]");
            // unlogged batch: the base update of the split key is replaced in place, the result is several mutations
            routed(phase, false, rows(logical, 8, 1), rows(logical, K, 1), rows(unrelated, K, 1));
            assertCalls("atomic QUORUM [t(8)1, " + shadow(1) + ", u(7)1]");
            routed(phase, true, rows(logical, 8, 1), rows(logical, K, 1), rows(unrelated, K, 1));
            assertCalls("atomic QUORUM [t(8)1, " + shadow(1) + ", u(7)1]");
            routed(phase, true, rows(logical, K, 1));
            assertCalls("atomic QUORUM [" + shadow(1) + "]");
            routed(phase, false, rangeDelete(K));
            assertCalls("atomic QUORUM [" + everyBucket("R") + "]");
            routed(phase, false, partitionDelete(K));
            assertCalls("atomic QUORUM [" + everyBucket("P") + "]");
            routed(phase, true, partitionDelete(K));
            assertCalls("atomic QUORUM [" + everyBucket("P") + "]");
        }
    }

    @Test
    public void rowsOfOneStatementGoToTheirBuckets()
    {
        // find two clusterings in different buckets
        int other = 2;
        while (bucket(other) == bucket(1))
            other++;
        routed(Phase.NEW_ONLY, false, rows(logical, K, 1, other));
        List<String> expected = new ArrayList<>(List.of(shadow(1), shadow(other)));
        expected.sort(null); // RowMapper returns buckets in order
        assertCalls("atomic QUORUM " + expected);
        // the shadows are what RowMapper says, cell for cell
        PartitionUpdate source = rows(logical, K, 1, other).getPartitionUpdates().iterator().next();
        TableLayout layout = LayoutRegistry.of(keyIn(Phase.NEW_ONLY)).layout(logical.id);
        List<PartitionUpdate> mapped = new ArrayList<>(RowMapper.toPhysical(source, physical, layout, INCARNATION).values());
        List<? extends IMutation> sent = recorder.lists.get(0);
        assertEquals(mapped.size(), sent.size());
        for (int i = 0; i < mapped.size(); i++)
            assertEquals(mapped.get(i).toString(), ((Mutation) sent.get(i)).getPartitionUpdates().iterator().next().toString());
    }

    @Test
    public void mutationOverTwoTablesKeepsTheOtherTableInC3()
    {
        PartitionUpdate a = rows(logical, K, 1).getPartitionUpdates().iterator().next();
        PartitionUpdate b = rows(unrelated, K, 1).getPartitionUpdates().iterator().next();
        Map<TableId, PartitionUpdate> both = new LinkedHashMap<>();
        both.put(a.metadata().id, a);
        both.put(b.metadata().id, b);
        Mutation joint = new Mutation(keyspace(), a.partitionKey(), ImmutableMap.copyOf(both), 0, rows(logical, K, 1).potentialTxnConflicts());
        routed(Phase.STEADY, false, joint);
        assertCalls("atomic QUORUM [u(7)1, " + shadow(1) + "]");
        routed(Phase.BACKFILL, false, joint);
        assertEquals(2, recorder.calls.size());
        assertEquals("plain QUORUM [" + shadow(1) + "]", recorder.calls.get(0));
        assertSame("the original is untouched", joint, recorder.lists.get(1).get(0));
    }

    @Test
    public void batchOverKeysInDifferentClasses()
    {
        int k1 = 11;
        int k2 = 12;
        int k3 = 13;
        ClusterMetadata md = with(header, 5, ImmutableMap.of(key(k1), state(Phase.BACKFILL), key(k2), state(Phase.CUTOVER), key(k3), state(Phase.STEADY),
                                                             key(K), state(Phase.ANNOUNCED)));
        IMutation[] batch = { rows(logical, k3, 1), rows(logical, k2, 1), rows(logical, k1, 1), rows(logical, K, 1), rows(logical, 99, 1) };
        metadata.set(md);
        assertTrue(route(Arrays.asList(batch), ConsistencyLevel.LOCAL_QUORUM, false));
        String s1 = "o(11/" + bucket(1) + ")1";
        String s2 = "o(12/" + bucket(1) + ")1";
        String s3 = "o(13/" + bucket(1) + ")1";
        assertCalls("plain LOCAL_QUORUM [" + s1 + "]",
                    "atomic LOCAL_QUORUM [" + s3 + ", t(12)1, t(11)1, t(7)1, t(99)1]",
                    "plain LOCAL_QUORUM [" + s2 + "]");

        recorder.calls.clear();
        metadata.set(md);
        assertTrue(route(Arrays.asList(batch), ConsistencyLevel.LOCAL_QUORUM, true));
        assertCalls("atomic LOCAL_QUORUM [" + s3 + ", t(12)1, t(11)1, t(7)1, t(99)1, " + s1 + ", " + s2 + "]");
    }

    // ---- consistency level ----

    @Test
    public void anyIsRaisedToOneForRoutedWrites()
    {
        metadata.set(keyIn(Phase.ANNOUNCED));
        assertTrue(route(List.of(rows(logical, K, 1)), ConsistencyLevel.ANY, false));
        assertCalls("plain ONE [t(7)1]");

        recorder.calls.clear();
        metadata.set(keyIn(Phase.DUAL_WRITE));
        assertTrue(route(List.of(rows(logical, K, 1), rows(logical, 8, 1)), ConsistencyLevel.ANY, false));
        assertCalls("plain ONE [" + shadow(1) + "]",
                    "plain ONE [t(7)1, t(8)1]");

        recorder.calls.clear();
        metadata.set(keyIn(Phase.STEADY));
        assertTrue(route(List.of(rows(logical, K, 1)), ConsistencyLevel.ANY, true));
        assertCalls("atomic ONE [" + shadow(1) + "]");

        for (ConsistencyLevel cl : new ConsistencyLevel[]{ ConsistencyLevel.ONE, ConsistencyLevel.ALL, ConsistencyLevel.EACH_QUORUM })
        {
            recorder.calls.clear();
            metadata.set(keyIn(Phase.CUTOVER));
            assertTrue(route(List.of(rows(logical, K, 1)), cl, false));
            assertCalls("plain " + cl + " [t(7)1]",
                        "plain " + cl + " [" + shadow(1) + "]");
        }
    }

    // ---- validate after execute ----

    @Test
    public void routingChangeCausesReExecutionUnderTheNewMetadata()
    {
        metadata.set(keyIn(Phase.VERIFY), keyIn(Phase.CUTOVER));
        assertTrue(route(List.of(rows(logical, K, 1)), ConsistencyLevel.QUORUM, false));
        assertCalls("plain QUORUM [" + shadow(1) + "]",
                    "plain QUORUM [t(7)1]",
                    // again, as class C2
                    "plain QUORUM [t(7)1]",
                    "plain QUORUM [" + shadow(1) + "]");
        assertEquals("before, after the first attempt, after the second", 3, metadata.lookups);
    }

    @Test
    public void announcedKeyIsValidated()
    {
        // class C0 executes the original mutations and must still be re-checked
        metadata.set(keyIn(Phase.ANNOUNCED), keyIn(Phase.DUAL_WRITE));
        List<IMutation> original = List.of(rows(logical, K, 1));
        assertTrue(route(original, ConsistencyLevel.QUORUM, false));
        assertCalls("plain QUORUM [t(7)1]",
                    "plain QUORUM [" + shadow(1) + "]",
                    "plain QUORUM [t(7)1]");
        assertEquals(3, metadata.lookups);

        // the same for a logged batch, and for the way out of ABORTING: the key leaves the registry
        recorder.calls.clear();
        metadata.set(keyIn(Phase.ABORTING), otherKeyOnly());
        assertFalse("the key is gone: the stock path executes the write", route(original, ConsistencyLevel.QUORUM, true));
        assertCalls("atomic QUORUM [t(7)1]");
        assertEquals(2, metadata.lookups);

        recorder.calls.clear();
        metadata.set(keyIn(Phase.ABORTING), emptyRegistry());
        assertFalse(route(original, ConsistencyLevel.QUORUM, false));
        assertCalls("plain QUORUM [t(7)1]");
    }

    @Test
    public void threeChangesCauseWriteTimeout()
    {
        metadata.set(keyIn(Phase.ANNOUNCED), keyIn(Phase.DUAL_WRITE), keyIn(Phase.BACKFILL), keyIn(Phase.VERIFY));
        try
        {
            route(List.of(rows(logical, K, 1)), ConsistencyLevel.ANY, false);
            fail("expected a write timeout");
        }
        catch (WriteTimeoutException e)
        {
            assertEquals(WriteType.SIMPLE, e.writeType);
            assertEquals(ConsistencyLevel.ONE, e.consistency);
            assertEquals(0, e.received);
            assertTrue(e.getMessage(), e.getMessage().contains("3 attempts"));
        }
        assertCalls("plain ONE [t(7)1]",
                    "plain ONE [" + shadow(1) + "]",
                    "plain ONE [t(7)1]",
                    "plain ONE [" + shadow(1) + "]",
                    "plain ONE [t(7)1]");
        assertEquals(4, metadata.lookups);

        // two changes are survived
        recorder.calls.clear();
        metadata.set(keyIn(Phase.ANNOUNCED), keyIn(Phase.DUAL_WRITE), keyIn(Phase.BACKFILL));
        assertTrue(route(List.of(rows(logical, K, 1)), ConsistencyLevel.QUORUM, true));
        assertEquals(3, recorder.calls.size());

        // the write type of the timeout follows the caller
        metadata.set(keyIn(Phase.ANNOUNCED), keyIn(Phase.DUAL_WRITE), keyIn(Phase.BACKFILL), keyIn(Phase.VERIFY));
        try
        {
            route(List.of(rows(logical, K, 1)), ConsistencyLevel.QUORUM, true);
            fail("expected a write timeout");
        }
        catch (WriteTimeoutException e)
        {
            assertEquals(WriteType.BATCH, e.writeType);
        }
    }

    @Test
    public void changesThatAreNotRoutingChangesDoNotReExecute()
    {
        KeyState backfill = state(Phase.BACKFILL);
        // ReportBackfill: version and backfillDone
        KeyState reported = new KeyState(Phase.BACKFILL, backfill.phaseEpoch, 1000, INCARNATION, 1, backfill.generationEpoch,
                                         ImmutableSet.of(new NodeId(1)), ACTION);
        // a generation restart as AdvanceLayout commits it: new phase epoch, generation and generation epoch, same phase
        KeyState restarted = new KeyState(Phase.BACKFILL, Epoch.create(99), 2000, INCARNATION, 2, Epoch.create(99), ImmutableSet.of(), ACTION);
        // another key of the layout changes phase
        Map<ByteBuffer, KeyState> otherKeyMoved = ImmutableMap.of(key(K), backfill, key(1000), state(Phase.CUTOVER));
        ClusterMetadata before = with(header, 5, ImmutableMap.of(key(K), backfill, key(1000), state(Phase.VERIFY)));
        for (ClusterMetadata after : new ClusterMetadata[]{ with(header, 6, ImmutableMap.of(key(K), reported)),
                                                            with(header, 6, ImmutableMap.of(key(K), restarted)),
                                                            with(header, 6, otherKeyMoved) })
        {
            recorder.calls.clear();
            metadata.set(before, after);
            assertTrue(route(List.of(rows(logical, K, 1)), ConsistencyLevel.QUORUM, false));
            assertEquals(2, recorder.calls.size());
            assertEquals(2, metadata.lookups);
        }
    }

    @Test
    public void sameIncarnationInANewLayoutIsARoutingChange()
    {
        // the key was removed with its layout and registered again: incarnations start over, the overflow table differs
        LayoutHeader recreated = new LayoutHeader(new UUID(5, 5), logical.id, physical.id, keyspace(), logical.name, physical.name,
                                                  TableLayout.Transform.HASH_SPLIT, LayoutRegistry.HASH_ALGO, BUCKETS, 1);
        metadata.set(keyIn(Phase.BACKFILL), with(recreated, 3, ImmutableMap.of(key(K), state(Phase.BACKFILL))));
        assertTrue(route(List.of(rows(logical, K, 1)), ConsistencyLevel.QUORUM, false));
        assertEquals(4, recorder.calls.size());

        // and so is a new incarnation in the same layout
        recorder.calls.clear();
        KeyState next = new KeyState(Phase.BACKFILL, Epoch.create(12), 1000, INCARNATION + 1, 1, Epoch.create(10), ImmutableSet.of(), ACTION);
        ClusterMetadata again = ClusterMetadata.current().transformer().with(LayoutRegistryValue.KEY, new LayoutRegistryValue(
            new LayoutRegistry(LayoutRegistry.FORMAT_VERSION, ImmutableMap.of(logical.id, new TableLayout(header, INCARNATION + 2, ImmutableMap.of(key(K), next), 9))))).build().metadata;
        metadata.set(keyIn(Phase.BACKFILL), again);
        recorder.inside = () -> {};
        try
        {
            route(List.of(rows(logical, K, 1)), ConsistencyLevel.QUORUM, false);
            fail("the recorder checks the incarnation of the shadows");
        }
        catch (AssertionError expected)
        {
            assertTrue(expected.getMessage(), expected.getMessage().contains("incarnation"));
        }
        assertEquals("the second attempt started", 2, recorder.calls.size());
    }

    @Test
    public void failedWriteIsNotValidatedAndLeavesNoFlag()
    {
        metadata.set(keyIn(Phase.DUAL_WRITE));
        recorder.inside = () -> {
            throw new WriteTimeoutException(WriteType.SIMPLE, ConsistencyLevel.QUORUM, 0, 2);
        };
        try
        {
            route(List.of(rows(logical, K, 1)), ConsistencyLevel.QUORUM, false);
            fail("expected the failure of the shadow write");
        }
        catch (WriteTimeoutException expected)
        {
            assertEquals(2, expected.blockFor);
        }
        // the shadow failed: the base copy was not written, nothing was re-checked
        assertCalls("plain QUORUM [" + shadow(1) + "]");
        assertEquals(1, metadata.lookups);

        recorder.inside = () -> {};
        recorder.calls.clear();
        assertTrue("the re-entrancy flag was reset", route(List.of(rows(logical, K, 1)), ConsistencyLevel.QUORUM, false));
        assertEquals(2, recorder.calls.size());
    }

    // ---- re-entrancy and the overflow table guard ----

    @Test
    public void callsMadeByTheRouterPassThroughTheHooks()
    {
        metadata.set(keyIn(Phase.STEADY));
        List<Boolean> inner = new ArrayList<>();
        recorder.inside = () -> {
            // what StorageProxy.mutateWithTriggers and truncateBlocking do when the router calls them
            inner.add(LayoutWrites.route(recorder.lists.isEmpty() ? new Untouchable() : recorder.lists.get(recorder.lists.size() - 1),
                                         ConsistencyLevel.QUORUM, false, TIME, PreserveTimestamp.yes));
            try
            {
                LayoutWrites.beforeTruncate(keyspace(), physical.name);
            }
            catch (TimeoutException e)
            {
                throw new AssertionError(e);
            }
        };
        assertTrue(route(List.of(rows(logical, K, 1)), ConsistencyLevel.QUORUM, false));
        assertEquals(List.of(false), inner);
        assertCalls("plain QUORUM [" + shadow(1) + "]");
    }

    @Test
    public void directWritesToTheOverflowTableAreRejected()
    {
        PartitionUpdate.SimpleBuilder overflow = PartitionUpdate.simpleBuilder(physical, K, 3 * 256).timestamp(1);
        overflow.row(1).add("v", 1);
        Mutation direct = new Mutation(overflow.build());
        for (ClusterMetadata md : new ClusterMetadata[]{ keyIn(Phase.STEADY), keyIn(Phase.ANNOUNCED), otherKeyOnly() })
        {
            for (boolean atomic : new boolean[]{ false, true })
            {
                metadata.set(md);
                try
                {
                    route(List.of(rows(logical, K, 1), direct), ConsistencyLevel.QUORUM, atomic);
                    fail("expected a rejection");
                }
                catch (InvalidRequestException e)
                {
                    assertTrue(e.getMessage(), e.getMessage().contains(physical.name + " is managed by the adapted layout of "
                                                                       + keyspace() + '.' + logical.name));
                }
            }
        }
        // nothing was written before the rejection
        assertCalls();
    }

    // ---- H19 ----

    @Test
    public void lightweightTransactionsAreRejectedOnAdaptedKeys()
    {
        for (Phase phase : Phase.values())
        {
            metadata.set(keyIn(phase));
            try
            {
                LayoutWrites.checkCas(keyspace(), logical.name, logical.partitioner.decorateKey(key(K)));
                fail("expected a rejection in " + phase);
            }
            catch (InvalidRequestException e)
            {
                assertEquals("Lightweight transactions are not supported on partition 0x00000007 of " + keyspace() + '.' + logical.name
                             + " while it is adapted", e.getMessage());
            }
            // other keys and other tables are not concerned
            LayoutWrites.checkCas(keyspace(), logical.name, logical.partitioner.decorateKey(key(8)));
            LayoutWrites.checkCas(keyspace(), unrelated.name, unrelated.partitioner.decorateKey(key(K)));
            LayoutWrites.checkCas("another_keyspace", logical.name, logical.partitioner.decorateKey(key(K)));
            try
            {
                LayoutWrites.checkCas(keyspace(), physical.name, physical.partitioner.decorateKey(key(K)));
                fail("expected a rejection of the overflow table");
            }
            catch (InvalidRequestException e)
            {
                assertTrue(e.getMessage(), e.getMessage().contains("is managed"));
            }
        }
    }

    // ---- H20 ----

    @Test
    public void truncateIsRejectedDuringAMigrationAndCoversTheOverflowTableAfterIt() throws Exception
    {
        for (Phase phase : Phase.values())
        {
            if (phase == Phase.STEADY)
                continue;
            // one key in STEADY does not make up for another that is not
            metadata.set(with(header, 5, ImmutableMap.of(key(K), state(phase), key(8), state(Phase.STEADY))));
            try
            {
                LayoutWrites.beforeTruncate(keyspace(), logical.name);
                fail("expected a rejection in " + phase);
            }
            catch (InvalidRequestException e)
            {
                assertTrue(e.getMessage(), e.getMessage().contains("cannot be truncated while a partition is being adapted (phase " + phase + ')'));
            }
        }
        assertCalls();

        metadata.set(with(header, 5, ImmutableMap.of(key(K), state(Phase.STEADY), key(8), state(Phase.STEADY))));
        LayoutWrites.beforeTruncate(keyspace(), logical.name);
        assertCalls("truncate overflow");
        assertEquals(1, metadata.lookups);

        // other tables are not concerned; the overflow table cannot be truncated by a client
        recorder.calls.clear();
        LayoutWrites.beforeTruncate(keyspace(), unrelated.name);
        LayoutWrites.beforeTruncate("another_keyspace", logical.name);
        assertCalls();
        try
        {
            LayoutWrites.beforeTruncate(keyspace(), physical.name);
            fail("expected a rejection of the overflow table");
        }
        catch (InvalidRequestException e)
        {
            assertTrue(e.getMessage(), e.getMessage().contains("is managed"));
        }
    }
}
