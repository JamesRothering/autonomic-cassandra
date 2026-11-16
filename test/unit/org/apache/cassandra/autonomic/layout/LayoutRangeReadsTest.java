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
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Supplier;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.ExcludingBounds;
import org.apache.cassandra.dht.IncludingExcludingBounds;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * T6.5, unit level: the range path of design 3.5 against a twin that is never split, with the registry steered by hand.
 * The partitions 1 to 6 are written while the keys are in class C1, so both copies hold them; two of them are split.
 */
public class LayoutRangeReadsTest extends CQLTester
{
    private static final UUID LAYOUT_ID = UUID.fromString("1a2b3c4d-0000-4000-8000-000000000005");
    private static final UUID ACTION = new UUID(8, 8);
    private static final int BUCKETS = 4;
    private static final long NOW = FBUtilities.nowInSeconds();
    private static final String COLUMNS = "(p int, c1 int, c2 int, v int, PRIMARY KEY (p, c1, c2))";
    private static final int PARTITIONS = 6;

    private TableMetadata logical;
    private TableMetadata physical;
    private String t;
    private String twin;
    private LayoutHeader header;
    private int s1;
    private int s2;
    private final Steered metadata = new Steered();
    /** The key ranges of the stock range reads the router made, in order. */
    private final List<String> ranges = new ArrayList<>();
    private int eagerRows;
    private int chunkRows;

    /** The metadata the hooks see: the queued ones in order, the last one for ever; reads made by the router see the last one. */
    private static final class Steered implements Supplier<ClusterMetadata>
    {
        final Deque<ClusterMetadata> queue = new ArrayDeque<>();
        ClusterMetadata last;
        int lookups;

        @Override
        public ClusterMetadata get()
        {
            if (LayoutReads.isReentrant() && last != null)
                return last;
            lookups++;
            last = queue.size() > 1 ? queue.poll() : queue.peek();
            return last;
        }

        void set(ClusterMetadata... sequence)
        {
            queue.clear();
            queue.addAll(Arrays.asList(sequence));
            last = null;
            lookups = 0;
        }
    }

    @Before
    public void setUp() throws Throwable
    {
        t = createTable("CREATE TABLE %s " + COLUMNS);
        logical = Schema.instance.getTableMetadata(keyspace(), t);
        schemaChange(PhysicalTables.derive(logical, LAYOUT_ID, TableId.generate()));
        physical = Schema.instance.getTableMetadata(keyspace(), PhysicalTables.physicalName(t, LAYOUT_ID));
        twin = createTable("CREATE TABLE %s " + COLUMNS);
        header = new LayoutHeader(LAYOUT_ID, logical.id, physical.id, keyspace(), t, physical.name,
                                  TableLayout.Transform.HASH_SPLIT, LayoutRegistry.HASH_ALGO, BUCKETS, 2);
        // the split partitions: the second and the fifth in ring order, so that there is a gap before, between and after
        List<Integer> ring = new ArrayList<>();
        for (int p = 1; p <= PARTITIONS; p++)
            ring.add(p);
        ring.sort((a, b) -> Murmur3Partitioner.instance.getToken(key(a)).compareTo(Murmur3Partitioner.instance.getToken(key(b))));
        s1 = ring.get(1);
        s2 = ring.get(4);

        LayoutWrites.setForTesting(null, metadata);
        LayoutReads.setForTesting(null, metadata);
        LayoutReads.setRangeSinkForTesting((command, cl, coordinator, requestTime) -> {
            ranges.add(command.dataRange().keyRange().toString());
            return StorageProxy.getRangeSlice(command, cl, coordinator, requestTime);
        });
        org.apache.cassandra.autonomic.AutonomicConfig.LayoutConfig config = DatabaseDescriptor.getAutonomicConfig().layout;
        eagerRows = config.fanout_eager_rows;
        chunkRows = config.fanout_chunk_rows;

        metadata.set(keys(Phase.BACKFILL, Phase.BACKFILL));
        for (int p = 1; p <= PARTITIONS; p++)
            for (int c1 = 0; c1 < 3; c1++)
                for (int c2 = 0; c2 < 4; c2++)
                    write("INSERT INTO %s (p, c1, c2, v) VALUES (" + p + ", " + c1 + ", " + c2 + ", " + (p * 10 + c1 * 4 + c2) + ") USING TIMESTAMP " + (1000 + c2));
        ranges.clear();
    }

    @After
    public void restore()
    {
        LayoutWrites.setForTesting(null, null);
        LayoutReads.setForTesting(null, null);
        LayoutReads.setRangeSinkForTesting(null);
        DatabaseDescriptor.getAutonomicConfig().layout.fanout_eager_rows = eagerRows;
        DatabaseDescriptor.getAutonomicConfig().layout.fanout_chunk_rows = chunkRows;
    }

    private static ByteBuffer key(int p)
    {
        return Int32Type.instance.decompose(p);
    }

    private static KeyState state(Phase phase)
    {
        return new KeyState(phase, Epoch.create(10 + phase.id), 1000, 3, 1, Epoch.create(10), ImmutableSet.of(), phase == Phase.STEADY ? null : ACTION);
    }

    private ClusterMetadata keys(Phase first, Phase second)
    {
        Map<ByteBuffer, KeyState> keys = ImmutableMap.of(key(s1), state(first), key(s2), state(second));
        LayoutRegistry registry = new LayoutRegistry(LayoutRegistry.FORMAT_VERSION,
                                                     ImmutableMap.of(logical.id, new TableLayout(header, 5, keys, 5)));
        return ClusterMetadata.current().transformer().with(LayoutRegistryValue.KEY, new LayoutRegistryValue(registry)).build().metadata;
    }

    private ResultMessage execute(String cql, int pageSize, PagingState state)
    {
        QueryState queryState = QueryState.forInternalCalls();
        QueryOptions options = QueryOptions.create(ConsistencyLevel.ONE, List.of(), false, pageSize > 0 ? PageSize.inRows(pageSize) : PageSize.NONE, state,
                                                   ConsistencyLevel.SERIAL, ProtocolVersion.CURRENT, keyspace(), Long.MIN_VALUE, NOW);
        CQLStatement statement = QueryProcessor.instance.parse(cql, queryState, options);
        return QueryProcessor.instance.process(statement, queryState, options, Dispatcher.RequestTime.forImmediateExecution());
    }

    private void write(String cql)
    {
        for (String table : new String[]{ t, twin })
            execute(String.format(cql, keyspace() + '.' + table), 0, null);
    }

    private List<String> rows(String cql, String table, int pageSize)
    {
        List<String> all = new ArrayList<>();
        PagingState state = null;
        do
        {
            ResultMessage.Rows page = (ResultMessage.Rows) execute(String.format(cql, keyspace() + '.' + table), pageSize, state);
            if (pageSize > 0)
                assertTrue("page of " + page.result.size() + " rows", page.result.size() <= pageSize);
            for (List<byte[]> row : page.result.rows)
            {
                StringBuilder sb = new StringBuilder();
                for (byte[] cell : row)
                    sb.append(cell == null ? "null" : ByteBufferUtil.bytesToHex(ByteBuffer.wrap(cell))).append(' ');
                all.add(sb.toString());
            }
            state = page.result.metadata.getPagingState();
        }
        while (state != null);
        return all;
    }

    private static long token(int p)
    {
        return (Long) Murmur3Partitioner.instance.getToken(key(p)).getTokenValue();
    }

    private List<String> catalogue()
    {
        List<String> cql = new ArrayList<>(List.of("SELECT * FROM %s", "SELECT p, c1 FROM %s", "SELECT DISTINCT p FROM %s", "SELECT count(*) FROM %s",
                                                   "SELECT p, c1, count(*) FROM %s GROUP BY p, c1", "SELECT * FROM %s PER PARTITION LIMIT 5",
                                                   "SELECT * FROM %s WHERE v > 30 ALLOW FILTERING", "SELECT * FROM %s WHERE c1 = 1 ALLOW FILTERING"));
        for (int limit : new int[]{ 1, 5, 12, 13, 24, 30, 1000 })
            cql.add("SELECT * FROM %s LIMIT " + limit);
        for (int p : new int[]{ s1, s2 })
            for (String op : new String[]{ ">=", ">", "<=", "<" })
                cql.add("SELECT * FROM %s WHERE token(p) " + op + ' ' + token(p));
        cql.add("SELECT * FROM %s WHERE token(p) >= " + Math.min(token(s1), token(s2)) + " AND token(p) <= " + Math.max(token(s1), token(s2)));
        cql.add("SELECT * FROM %s WHERE token(p) > " + Math.min(token(s1), token(s2)) + " AND token(p) < " + Math.max(token(s1), token(s2)) + " LIMIT 7");
        return cql;
    }

    private void assertSameAsTwin(String what)
    {
        for (String cql : catalogue())
        {
            List<String> expected = rows(cql, twin, 0);
            assertEquals(what + ": " + cql, expected, rows(cql, t, 0));
            for (int pageSize : new int[]{ 1, 5, 12, 13, 5000 })
                assertEquals(what + ", page size " + pageSize + ": " + cql, expected, rows(cql, t, pageSize));
        }
    }

    @Test
    public void everyScanReturnsWhatTheTwinReturns()
    {
        // base table authoritative: C0 and C1, and one key in each class
        for (Phase[] phases : new Phase[][]{ { Phase.ANNOUNCED, Phase.BACKFILL }, { Phase.DUAL_WRITE, Phase.VERIFY } })
        {
            metadata.set(keys(phases[0], phases[1]));
            assertSameAsTwin(Arrays.toString(phases));
        }
        // the overflow table authoritative
        for (Phase[] phases : new Phase[][]{ { Phase.CUTOVER, Phase.NEW_ONLY }, { Phase.STEADY, Phase.CLEANUP }, { Phase.BACKFILL, Phase.NEW_ONLY } })
        {
            metadata.set(keys(phases[0], phases[1]));
            assertSameAsTwin(Arrays.toString(phases));
        }
        // lazily
        DatabaseDescriptor.getAutonomicConfig().layout.fanout_eager_rows = 5;
        DatabaseDescriptor.getAutonomicConfig().layout.fanout_chunk_rows = 3;
        for (Phase[] phases : new Phase[][]{ { Phase.BACKFILL, Phase.BACKFILL }, { Phase.NEW_ONLY, Phase.NEW_ONLY }, { Phase.BACKFILL, Phase.NEW_ONLY } })
        {
            metadata.set(keys(phases[0], phases[1]));
            assertSameAsTwin("lazy " + Arrays.toString(phases));
        }
    }

    @Test
    public void strayRowsOfABaseCopyAreNotReturned()
    {
        // written with no registry in sight, so that they reach the base table only
        metadata.set(ClusterMetadata.current().transformer().with(LayoutRegistryValue.KEY, new LayoutRegistryValue(LayoutRegistry.EMPTY)).build().metadata);
        for (int p : new int[]{ s1, s2 })
        {
            execute("INSERT INTO " + keyspace() + '.' + t + " (p, c1, c2, v) VALUES (" + p + ", 7, 7, 7)", 0, null);
        }
        // the stray rows are in the base copy ...
        metadata.set(keys(Phase.ANNOUNCED, Phase.ANNOUNCED));
        assertTrue(rows("SELECT * FROM %s", t, 0).size() > rows("SELECT * FROM %s", twin, 0).size());
        // ... and no scan of the adapted partitions returns them
        metadata.set(keys(Phase.NEW_ONLY, Phase.STEADY));
        assertSameAsTwin("stray rows");
    }

    @Test
    public void aScanWithoutARoutedKeyIsNotRouted()
    {
        // a key of another table in the registry; the table scanned has none inside the range
        metadata.set(keys(Phase.NEW_ONLY, Phase.NEW_ONLY));
        assertEquals(rows("SELECT * FROM %s", twin, 0), rows("SELECT * FROM %s", twin, 0));
        ranges.clear();
        assertEquals(0, ranges.size());
        // outside the token range of both split keys
        long lo = Math.min(token(s1), token(s2));
        long outside = lo - 1;
        List<String> result = rows("SELECT * FROM %s WHERE token(p) <= " + outside, t, 0);
        assertEquals(rows("SELECT * FROM %s WHERE token(p) <= " + outside, twin, 0), result);
    }

    @Test
    public void scanThatStartedInC1IsNotReturnedFromTheBaseCopyOnceTheKeyIsInC3()
    {
        // the base copy of a split partition gets a row that the overflow table does not have
        metadata.set(keys(Phase.NEW_ONLY, Phase.NEW_ONLY));
        execute("INSERT INTO " + keyspace() + '.' + t + " (p, c1, c2, v) VALUES (" + s1 + ", 9, 9, 99)", 0, null);
        List<String> overflow = rows("SELECT * FROM %s", t, 0);
        metadata.set(keys(Phase.ANNOUNCED, Phase.ANNOUNCED));
        List<String> base = rows("SELECT * FROM %s", t, 0);
        assertFalse(overflow.equals(base));

        // the coordinator sees C1 when the scan starts and C3 when the rows of the base copy are in: the scan is repeated
        metadata.set(keys(Phase.BACKFILL, Phase.BACKFILL), keys(Phase.STEADY, Phase.STEADY));
        assertEquals(overflow, rows("SELECT * FROM %s", t, 0));
        // C0 too: the first key of the registry is the one that changes
        metadata.set(keys(Phase.ANNOUNCED, Phase.ANNOUNCED), keys(Phase.NEW_ONLY, Phase.ANNOUNCED));
        assertEquals(overflow, rows("SELECT * FROM %s", t, 0));
        // without a change the base copy is returned
        metadata.set(keys(Phase.BACKFILL, Phase.BACKFILL));
        assertEquals(base, rows("SELECT * FROM %s", t, 0));
    }

    @Test
    public void threeChangesCauseReadTimeout()
    {
        metadata.set(keys(Phase.VERIFY, Phase.VERIFY), keys(Phase.CUTOVER, Phase.VERIFY), keys(Phase.ROLLBACK_DUAL, Phase.VERIFY),
                     keys(Phase.ABORTING, Phase.VERIFY));
        try
        {
            rows("SELECT * FROM %s", t, 0);
            fail("expected a read timeout");
        }
        catch (ReadTimeoutException e)
        {
            assertTrue(e.getMessage(), e.getMessage().contains("3 attempts"));
        }
    }

    private List<LayoutReads.RoutedInRange> routed(Phase first, Phase second)
    {
        ClusterMetadata cm = keys(first, second);
        LayoutRegistry registry = LayoutRegistry.of(cm);
        List<LayoutReads.RoutedInRange> routed = new ArrayList<>();
        for (int p : new int[]{ s1, s2 })
            routed.add(new LayoutReads.RoutedInRange(Murmur3Partitioner.instance.decorateKey(key(p)), RoutedKey.of(cm, registry, logical.id, key(p))));
        return routed;
    }

    /**
     * What the plan promises, over many ranges: the stock reads (gaps and base ranges) hold no key of class C2 or C3,
     * the keys of C2 and C3 are in the plan once each and in ring order, and every other partition of the range is in
     * exactly one stock read. Checked by position, so independent of the ring of the node.
     */
    @Test
    public void planCoversEveryPartitionOnceAndNeverReadsAKeyOfTheOverflowTable()
    {
        Murmur3Partitioner partitioner = Murmur3Partitioner.instance;
        PartitionPosition min = partitioner.getMinimumToken().minKeyBound();
        List<DecoratedKey> all = new ArrayList<>();
        for (int p = 1; p <= PARTITIONS; p++)
            all.add(partitioner.decorateKey(key(p)));
        all.sort(DecoratedKey::compareTo);

        List<AbstractBounds<PartitionPosition>> ranges = new ArrayList<>();
        ranges.add(new Range<>(min, min));
        ranges.add(new Bounds<>(min, min));
        for (DecoratedKey a : all)
        {
            ranges.add(new Range<>(min, a));
            ranges.add(new Bounds<>(min, a));
            ranges.add(new IncludingExcludingBounds<>(min, a));
            ranges.add(new ExcludingBounds<>(min, a));
            ranges.add(new Range<>(a, min));
            ranges.add(new Bounds<>(a, min));
            ranges.add(new Range<>(a.getToken().minKeyBound(), a.getToken().maxKeyBound()));
            for (DecoratedKey b : all)
                if (a.compareTo(b) < 0)
                {
                    ranges.add(new Range<>(a, b));
                    ranges.add(new Bounds<>(a, b));
                    ranges.add(new IncludingExcludingBounds<>(a, b));
                    ranges.add(new ExcludingBounds<>(a, b));
                }
        }
        Phase[][] classes = { { Phase.CUTOVER, Phase.NEW_ONLY }, { Phase.BACKFILL, Phase.NEW_ONLY }, { Phase.NEW_ONLY, Phase.DUAL_WRITE },
                              { Phase.ANNOUNCED, Phase.BACKFILL }, { Phase.STEADY, Phase.CLEANUP } };
        for (AbstractBounds<PartitionPosition> range : ranges)
        {
            for (Phase[] phases : classes)
            {
                List<LayoutReads.RoutedInRange> routed = new ArrayList<>();
                for (LayoutReads.RoutedInRange r : routed(phases[0], phases[1]))
                    if (range.contains(r.key))
                        routed.add(r);
                routed.sort((x, y) -> x.key.compareTo(y.key));
                if (routed.isEmpty())
                    continue;
                List<LayoutReads.Piece> plan = LayoutReads.plan(range, routed);
                String what = range + " " + Arrays.toString(phases) + " " + plan;
                List<DecoratedKey> fanned = new ArrayList<>();
                for (DecoratedKey key : all)
                {
                    if (!range.contains(key))
                        continue;
                    boolean overflow = false;
                    for (LayoutReads.RoutedInRange r : routed)
                        overflow |= r.key.equals(key) && r.routed.coordinatorClass() >= 2;
                    int reads = 0;
                    for (LayoutReads.Piece piece : plan)
                        if (piece.kind != LayoutReads.Piece.Kind.FAN_OUT && piece.bounds.contains(key))
                            reads++;
                    assertEquals(what + " key " + ByteBufferUtil.bytesToHex(key.getKey()), overflow ? 0 : 1, reads);
                }
                for (LayoutReads.Piece piece : plan)
                    if (piece.kind == LayoutReads.Piece.Kind.FAN_OUT)
                        fanned.add(piece.key.key);
                List<DecoratedKey> expected = new ArrayList<>();
                for (LayoutReads.RoutedInRange r : routed)
                    if (r.routed.coordinatorClass() >= 2)
                        expected.add(r.key);
                assertEquals(what, expected, fanned);
                // every piece lies inside the range, in ring order
                PartitionPosition at = null;
                for (LayoutReads.Piece piece : plan)
                {
                    if (piece.bounds == null)
                        continue;
                    assertTrue(what, range.left.compareTo(piece.bounds.left) <= 0);
                    if (at != null)
                        assertTrue(what + " order", at.compareTo(piece.bounds.left) <= 0);
                    at = piece.bounds.left;
                }
            }
        }
    }
}
