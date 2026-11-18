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

import org.apache.cassandra.autonomic.AutonomicConfig;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.filter.ClusteringIndexSliceFilter;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.partitions.PartitionIterator;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * T6.4: {@link LayoutReads} on one node, through the real statement and {@code StorageProxy} code, with a registry
 * injected into the metadata the routing hooks see. Statements run on the test thread the way the native transport
 * runs them, so pages and paging states are the real ones. Every result is compared with the result of the same
 * statement on a twin table that is never split.
 */
public class LayoutReadsTest extends CQLTester
{
    private static final UUID LAYOUT_ID = UUID.fromString("1a2b3c4d-0000-4000-8000-000000000004");
    private static final UUID ACTION = new UUID(7, 7);
    private static final int BUCKETS = 4;
    private static final int INCARNATION = 3;
    private static final int K = 1;
    private static final long NOW = FBUtilities.nowInSeconds();
    private static final String COLUMNS = "(p int, c1 int, c2 int, v int, s text, l list<int>, m map<text, int>, PRIMARY KEY (p, c1, c2))";

    private TableMetadata logical;
    private TableMetadata physical;
    private String t;
    private String twin;
    private LayoutHeader header;
    private final Metadata metadata = new Metadata();
    /** The tables the router read, in order: "base", "overflow" or the table name. The chunks of a lazy fan-out are not in it. */
    private final List<String> reads = new ArrayList<>();
    private int eagerRows;
    private int chunkRows;

    /**
     * The metadata the hooks see: the queued ones in order, the last one for ever; lookups are counted. The lookups of
     * the hook when the router itself calls {@code StorageProxy.read} see what the router saw last and do not count.
     */
    private static final class Metadata implements Supplier<ClusterMetadata>
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
        LayoutWrites.setForTesting(null, metadata);
        LayoutReads.setForTesting((group, cl, requestTime) -> {
            TableId id = group.queries.get(0).metadata().id;
            reads.add(id.equals(logical.id) ? "base" : id.equals(physical.id) ? "overflow" : group.queries.get(0).metadata().name);
            return StorageProxy.read(group, cl, requestTime);
        }, metadata);
        AutonomicConfig.LayoutConfig config = DatabaseDescriptor.getAutonomicConfig().layout;
        eagerRows = config.fanout_eager_rows;
        chunkRows = config.fanout_chunk_rows;

        // everything is written while the key is in class C1, so both copies hold it
        metadata.set(keyIn(Phase.BACKFILL));
        load();
        reads.clear();
    }

    @After
    public void restore()
    {
        LayoutProfiler.clear();
        LayoutWrites.setForTesting(null, null);
        LayoutReads.setForTesting(null, null);
        DatabaseDescriptor.getAutonomicConfig().layout.fanout_eager_rows = eagerRows;
        DatabaseDescriptor.getAutonomicConfig().layout.fanout_chunk_rows = chunkRows;
    }

    // ---- metadata ----

    private static ByteBuffer key(int p)
    {
        return Int32Type.instance.decompose(p);
    }

    private static KeyState state(Phase phase, int incarnation)
    {
        return new KeyState(phase, Epoch.create(10 + phase.id), 1000, incarnation, 1, Epoch.create(10), ImmutableSet.of(),
                            phase == Phase.STEADY ? null : ACTION);
    }

    private ClusterMetadata with(Map<ByteBuffer, KeyState> keys)
    {
        LayoutRegistry registry = keys == null
                                  ? LayoutRegistry.EMPTY
                                  : new LayoutRegistry(LayoutRegistry.FORMAT_VERSION, ImmutableMap.of(logical.id, new TableLayout(header, INCARNATION + 2, keys, 5)));
        return ClusterMetadata.current().transformer().with(LayoutRegistryValue.KEY, new LayoutRegistryValue(registry)).build().metadata;
    }

    private ClusterMetadata keyIn(Phase phase)
    {
        return with(ImmutableMap.of(key(K), state(phase, INCARNATION)));
    }

    // ---- statements ----

    private ResultMessage execute(String cql, ConsistencyLevel cl, int pageSize, PagingState state)
    {
        QueryState queryState = QueryState.forInternalCalls();
        QueryOptions options = QueryOptions.create(cl, List.of(), false, pageSize > 0 ? PageSize.inRows(pageSize) : PageSize.NONE, state,
                                                   ConsistencyLevel.SERIAL, ProtocolVersion.CURRENT, keyspace(), Long.MIN_VALUE, NOW);
        CQLStatement statement = QueryProcessor.instance.parse(cql, queryState, options);
        return QueryProcessor.instance.process(statement, queryState, options, Dispatcher.RequestTime.forImmediateExecution());
    }

    /** Runs the statement on the split table and on its twin. */
    private void write(String cql)
    {
        for (String table : new String[]{ t, twin })
            execute(String.format(cql, keyspace() + '.' + table), ConsistencyLevel.ONE, 0, null);
    }

    private void load()
    {
        for (int p = K; p <= K + 1; p++)
        {
            for (int c1 = 0; c1 < 6; c1++)
            {
                for (int c2 = 0; c2 < 7; c2++)
                {
                    int v = c1 * 10 + c2;
                    String ttl = c2 == 5 ? " AND TTL 100000" : "";
                    write("INSERT INTO %s (p, c1, c2, v, s, l, m) VALUES (" + p + ", " + c1 + ", " + c2 + ", " + (c2 == 6 ? "null" : String.valueOf(v))
                          + ", 's" + v + "', [" + v + ", 1, 2], {'a': " + v + ", 'b': 2}) USING TIMESTAMP " + (1000 + v) + ttl);
                }
            }
            // an update without a row marker, a row delete, a range delete and a cell delete
            write("UPDATE %s USING TIMESTAMP 2000 SET v = 777 WHERE p = " + p + " AND c1 = 9 AND c2 = 9");
            write("DELETE FROM %s USING TIMESTAMP 3000 WHERE p = " + p + " AND c1 = 1 AND c2 = 1");
            write("DELETE FROM %s USING TIMESTAMP 3000 WHERE p = " + p + " AND c1 = 3 AND c2 > 4");
            write("DELETE s FROM %s USING TIMESTAMP 3000 WHERE p = " + p + " AND c1 = 2 AND c2 = 2");
        }
    }

    /** Every page of the statement, cell by cell; pages are fetched with the paging state of the previous one. */
    private List<String> rows(String cql, String table, int pageSize)
    {
        List<String> all = new ArrayList<>();
        PagingState state = null;
        do
        {
            ResultMessage.Rows page = (ResultMessage.Rows) execute(String.format(cql, keyspace() + '.' + table), ConsistencyLevel.ONE, pageSize, state);
            if (pageSize > 0)
                assertTrue("page of " + page.result.size() + " rows", page.result.size() <= pageSize);
            all.addAll(render(page));
            state = page.result.metadata.getPagingState();
        }
        while (state != null);
        return all;
    }

    private static List<String> render(ResultMessage.Rows page)
    {
        List<String> rendered = new ArrayList<>();
        for (List<byte[]> row : page.result.rows)
        {
            StringBuilder sb = new StringBuilder();
            for (byte[] cell : row)
                sb.append(cell == null ? "null" : ByteBufferUtil.bytesToHex(ByteBuffer.wrap(cell))).append(' ');
            rendered.add(sb.toString());
        }
        return rendered;
    }

    private static final String[] CATALOGUE = {
        // by full primary key, IN on clustering
        "SELECT * FROM %s WHERE p = 1 AND c1 = 2 AND c2 = 3",
        "SELECT * FROM %s WHERE p = 1 AND c1 = 1 AND c2 = 1",
        "SELECT * FROM %s WHERE p = 1 AND c1 = 9 AND c2 = 9",
        "SELECT * FROM %s WHERE p = 1 AND c1 = 77 AND c2 = 3",
        "SELECT * FROM %s WHERE p = 1 AND c1 = 2 AND c2 IN (5, 1, 3, 99)",
        "SELECT * FROM %s WHERE p = 1 AND (c1, c2) IN ((0, 1), (4, 4), (5, 6), (3, 6))",
        "SELECT * FROM %s WHERE p = 1 AND c1 IN (4, 0) AND c2 IN (2, 1) ORDER BY c1 DESC",
        // by partition key only, and with clustering slices
        "SELECT * FROM %s WHERE p = 1",
        "SELECT * FROM %s WHERE p = 1 AND c1 = 2",
        "SELECT * FROM %s WHERE p = 1 AND c1 >= 1 AND c1 < 4",
        "SELECT * FROM %s WHERE p = 1 AND c1 = 2 AND c2 > 2",
        "SELECT * FROM %s WHERE p = 1 AND (c1, c2) > (2, 3) AND (c1, c2) <= (4, 1)",
        "SELECT * FROM %s WHERE p = 1 AND c1 > 100",
        // order, limits
        "SELECT * FROM %s WHERE p = 1 ORDER BY c1 DESC",
        "SELECT * FROM %s WHERE p = 1 ORDER BY c1 DESC, c2 DESC LIMIT 5",
        "SELECT * FROM %s WHERE p = 1 AND c1 < 4 ORDER BY c1 DESC LIMIT 9",
        "SELECT * FROM %s WHERE p = 1 LIMIT 1",
        "SELECT * FROM %s WHERE p = 1 LIMIT 7",
        "SELECT * FROM %s WHERE p = 1 LIMIT 1000",
        "SELECT * FROM %s WHERE p = 1 PER PARTITION LIMIT 4",
        "SELECT * FROM %s WHERE p = 1 PER PARTITION LIMIT 10 LIMIT 6",
        // IN on the partition key: split and normal keys mixed, a key that does not exist
        "SELECT * FROM %s WHERE p IN (1, 2)",
        "SELECT * FROM %s WHERE p IN (2, 1, 3)",
        "SELECT * FROM %s WHERE p IN (2, 1) LIMIT 50",
        "SELECT * FROM %s WHERE p IN (1, 2) LIMIT 3",
        "SELECT * FROM %s WHERE p IN (1, 2) PER PARTITION LIMIT 3",
        "SELECT * FROM %s WHERE p IN (1, 2) AND c1 = 5 AND c2 IN (0, 6)",
        "SELECT * FROM %s WHERE p IN (2, 1) ORDER BY c1 DESC LIMIT 20",
        // aggregates, GROUP BY, DISTINCT
        "SELECT count(*) FROM %s WHERE p = 1",
        "SELECT count(*), sum(v), max(c2) FROM %s WHERE p = 1 AND c1 > 2",
        "SELECT count(*) FROM %s WHERE p IN (1, 2)",
        "SELECT c1, count(*), sum(v), max(c2) FROM %s WHERE p = 1 GROUP BY c1",
        "SELECT c1, count(*) FROM %s WHERE p = 1 GROUP BY c1 LIMIT 3",
        "SELECT c1, c2, max(v) FROM %s WHERE p = 1 GROUP BY c1, c2 PER PARTITION LIMIT 5",
        "SELECT p, c1, count(*) FROM %s WHERE p IN (1, 2) GROUP BY p, c1",
        "SELECT p, count(*) FROM %s WHERE p IN (1, 2) GROUP BY p",
        "SELECT DISTINCT p FROM %s WHERE p IN (1, 2)",
        "SELECT DISTINCT p FROM %s WHERE p = 1",
        // cells: write time, ttl, collections, a subset of the columns
        "SELECT c1, c2, writetime(v), ttl(v), writetime(s) FROM %s WHERE p = 1",
        "SELECT c1, c2, l, m, m['a'] FROM %s WHERE p = 1 AND c1 = 2",
        "SELECT v FROM %s WHERE p = 1",
        "SELECT s FROM %s WHERE p = 1 AND c1 = 2 AND c2 = 2",
        "SELECT JSON * FROM %s WHERE p = 1 AND c1 = 0",
        // restrictions that need filtering
        "SELECT c1, c2, v FROM %s WHERE p = 1 AND v > 20 ALLOW FILTERING",
        "SELECT c1, c2, v FROM %s WHERE p = 1 AND v > 20 LIMIT 3 ALLOW FILTERING",
        "SELECT c1, c2 FROM %s WHERE p = 1 AND c2 = 3 ALLOW FILTERING",
        "SELECT c1, c2 FROM %s WHERE p = 1 AND s = 's23' ALLOW FILTERING",
        "SELECT c1, c2 FROM %s WHERE p = 1 AND m CONTAINS KEY 'a' AND v < 30 ORDER BY c1 DESC LIMIT 4 ALLOW FILTERING",
        "SELECT count(*) FROM %s WHERE p = 1 AND v >= 10 ALLOW FILTERING",
        };

    private static final int[] PAGE_SIZES = { 0, 1, 7, 5000 };

    private void assertSameAsTwin(String what)
    {
        for (String cql : CATALOGUE)
        {
            for (int pageSize : PAGE_SIZES)
            {
                String expected;
                try
                {
                    expected = String.join("\n", rows(cql, twin, pageSize));
                }
                catch (InvalidRequestException e)
                {
                    // statements that cannot be paged
                    expected = e.getMessage();
                }
                String actual;
                try
                {
                    actual = String.join("\n", rows(cql, t, pageSize));
                }
                catch (InvalidRequestException e)
                {
                    actual = e.getMessage();
                }
                assertEquals(what + ", page size " + pageSize + ": " + cql, expected, actual);
            }
        }
        assertFalse("the catalogue reads rows", rows(CATALOGUE[7], twin, 0).isEmpty());
    }

    // ---- fast path ----

    @Test
    public void whatIsNotRoutedCostsOneLookup()
    {
        long hooks = LayoutProfiler.hookCalls();
        assertNull("nothing is watched", LayoutProfiler.active());
        SinglePartitionReadCommand.Group split = group(logical, K);
        for (ClusterMetadata md : new ClusterMetadata[]{ with(null), ClusterMetadata.current() })
        {
            metadata.set(md);
            assertNull(LayoutReads.read(split, ConsistencyLevel.ONE, Dispatcher.RequestTime.forImmediateExecution()));
            assertEquals(1, metadata.lookups);
        }
        // a key that is not in the registry, another table, the overflow table itself: stock path, not validated
        metadata.set(keyIn(Phase.STEADY));
        assertNull(LayoutReads.read(group(logical, 2), ConsistencyLevel.ONE, Dispatcher.RequestTime.forImmediateExecution()));
        assertNull(LayoutReads.read(group(Schema.instance.getTableMetadata(keyspace(), twin), K), ConsistencyLevel.ONE, Dispatcher.RequestTime.forImmediateExecution()));
        assertNull(LayoutReads.read(group(physical, K), ConsistencyLevel.SERIAL, Dispatcher.RequestTime.forImmediateExecution()));
        assertEquals(3, metadata.lookups);
        assertEquals(List.of(), reads);
        assertEquals("FC8: the profiler was not looked at", hooks, LayoutProfiler.hookCalls());
    }

    /** FC8 and design 7.2 step 3: a watched key is counted by shape and its reads are timed; others are not looked at. */
    @Test
    public void aWatchedKeyIsProfiledAndItsReadsAreTimed()
    {
        metadata.set(with(null));
        LayoutProfiler.watch(logical.id, key(K), 60_000);
        long hooks = LayoutProfiler.hookCalls();
        LayoutProfiler.Shape before = LayoutProfiler.get(logical.id, key(K)).shape();
        assertEquals(0, before.operations());

        PartitionIterator result = LayoutReads.read(group(logical, K), ConsistencyLevel.ONE, Dispatcher.RequestTime.forImmediateExecution());
        assertNotNull("the hook took the read so that it could time it", result);
        result.close();
        java.util.TreeSet<org.apache.cassandra.db.Clustering<?>> names = new java.util.TreeSet<>(logical.comparator);
        names.add(org.apache.cassandra.db.Clustering.make(key(1), key(1)));
        SinglePartitionReadCommand named = SinglePartitionReadCommand.create(logical, NOW, ColumnFilter.all(logical), RowFilter.none(), DataLimits.NONE,
                                                                             logical.partitioner.decorateKey(key(K)),
                                                                             new org.apache.cassandra.db.filter.ClusteringIndexNamesFilter(names, false));
        LayoutReads.read(SinglePartitionReadCommand.Group.one(named), ConsistencyLevel.ONE, Dispatcher.RequestTime.forImmediateExecution()).close();
        LayoutProfiler.Shape shape = LayoutProfiler.get(logical.id, key(K)).shape();
        assertEquals(1, shape.sliceReads);
        assertEquals(1, shape.namedReads);
        assertEquals(2, shape.timedReads);
        assertEquals(0.5, shape.pointFraction(), 0.0);
        assertEquals(2, LayoutProfiler.hookCalls() - hooks);

        // another key of the same table is looked at by the hook but not counted or taken over
        assertNull(LayoutReads.read(group(logical, K + 1), ConsistencyLevel.ONE, Dispatcher.RequestTime.forImmediateExecution()));
        assertEquals(2, LayoutProfiler.get(logical.id, key(K)).shape().timedReads);
    }

    /** The whole partition with a limit of 1000 rows: with the default settings an eager read, which the test can see. */
    private static SinglePartitionReadCommand.Group group(TableMetadata table, int p)
    {
        return SinglePartitionReadCommand.Group.one(SinglePartitionReadCommand.create(table, NOW, ColumnFilter.all(table), RowFilter.none(),
                                                                                      DataLimits.cqlLimits(1000), table.partitioner.decorateKey(key(p)),
                                                                                      new ClusteringIndexSliceFilter(Slices.ALL, false)));
    }

    // ---- the table of design 3.4, against the twin, in every class ----

    @Test
    public void everyStatementReturnsWhatTheTwinReturns()
    {
        // C0 and C1 read the base table
        for (Phase phase : new Phase[]{ Phase.ANNOUNCED, Phase.BACKFILL })
        {
            reads.clear();
            metadata.set(keyIn(phase));
            assertSameAsTwin(phase.name());
            assertFalse(phase.name(), reads.contains("overflow"));
            assertTrue(phase.name(), reads.contains("base"));
        }
        // C2 reads the overflow table
        metadata.set(keyIn(Phase.CUTOVER));
        assertSameAsTwin("CUTOVER");

        // C3: from here writes reach the overflow table only, so a read of the base table would differ from the twin
        metadata.set(keyIn(Phase.STEADY));
        write("INSERT INTO %s (p, c1, c2, v, s) VALUES (1, 2, 50, 250, 'late') USING TIMESTAMP 5000");
        write("INSERT INTO %s (p, c1, c2, v) VALUES (1, 0, -1, -1) USING TIMESTAMP 5000");
        write("DELETE FROM %s USING TIMESTAMP 5000 WHERE p = 1 AND c1 = 4 AND c2 = 0");
        write("DELETE FROM %s USING TIMESTAMP 5000 WHERE p = 1 AND c1 = 5 AND c2 >= 3");
        // the read before the write of a list element goes through the read hook
        write("UPDATE %s USING TIMESTAMP 6000 SET l[1] = 99 WHERE p = 1 AND c1 = 2 AND c2 = 3");
        write("DELETE l[0] FROM %s USING TIMESTAMP 6000 WHERE p = 1 AND c1 = 0 AND c2 = 0");
        reads.clear();
        assertSameAsTwin("STEADY");
        // the IN queries read partition 2, which is not split, from the base table; partition 1 never
        assertFalse(rows("SELECT l FROM %s WHERE p = 1 AND c1 = 2 AND c2 = 3", t, 0).isEmpty());
        reads.clear();
        rows("SELECT * FROM %s WHERE p = 1", t, 7);
        assertFalse(reads.contains("base"));

        // the same with the lazy fan-out: every unpaged read, and every page of more than 5 rows
        DatabaseDescriptor.getAutonomicConfig().layout.fanout_eager_rows = 5;
        DatabaseDescriptor.getAutonomicConfig().layout.fanout_chunk_rows = 3;
        assertSameAsTwin("STEADY, lazy");
        DatabaseDescriptor.getAutonomicConfig().layout.fanout_chunk_rows = 1;
        assertSameAsTwin("STEADY, lazy, chunks of one row");
    }

    @Test
    public void pagingStateSurvivesEveryPhaseChange()
    {
        for (String cql : new String[]{ "SELECT * FROM %s WHERE p = 1", "SELECT * FROM %s WHERE p = 1 ORDER BY c1 DESC",
                                        "SELECT * FROM %s WHERE p IN (1, 2)", "SELECT c1, count(*) FROM %s WHERE p = 1 GROUP BY c1" })
        {
            List<String> expected = rows(cql, twin, 0);
            Phase[] path = { Phase.BACKFILL, Phase.VERIFY, Phase.CUTOVER, Phase.NEW_ONLY, Phase.CLEANUP, Phase.STEADY };
            List<String> all = new ArrayList<>();
            PagingState state = null;
            int page = 0;
            do
            {
                // a page per phase, then the rest in STEADY
                metadata.set(keyIn(path[Math.min(page++, path.length - 1)]));
                ResultMessage.Rows rows = (ResultMessage.Rows) execute(String.format(cql, keyspace() + '.' + t), ConsistencyLevel.ONE, 3, state);
                all.addAll(render(rows));
                state = rows.result.metadata.getPagingState();
            }
            while (state != null);
            assertTrue("pages: " + page, page > 2);
            assertEquals(cql, String.join("\n", expected), String.join("\n", all));
        }
    }

    // ---- validate after execute ----

    private List<String> clusterings(PartitionIterator result)
    {
        List<String> rows = new ArrayList<>();
        try (PartitionIterator partitions = result)
        {
            while (partitions.hasNext())
            {
                try (RowIterator partition = partitions.next())
                {
                    assertEquals(logical.id, partition.metadata().id);
                    assertEquals(key(K), partition.partitionKey().getKey());
                    while (partition.hasNext())
                    {
                        Row row = partition.next();
                        rows.add(row.clustering().toString(logical));
                        row.cells().forEach(cell -> assertEquals(logical.getColumn(cell.column().name), cell.column()));
                    }
                }
            }
        }
        return rows;
    }

    /** Writes a row that only the overflow table has (class C3) and returns the clusterings of the whole partition there. */
    private List<String> divergeCopies()
    {
        metadata.set(keyIn(Phase.STEADY));
        execute("INSERT INTO " + keyspace() + '.' + t + " (p, c1, c2, v) VALUES (1, 2, 50, 250) USING TIMESTAMP 5000", ConsistencyLevel.ONE, 0, null);
        List<String> overflow = clusterings(LayoutReads.read(group(logical, K), ConsistencyLevel.ONE, Dispatcher.RequestTime.forImmediateExecution()));
        assertTrue(overflow.toString(), overflow.contains("c1=2, c2=50"));
        reads.clear();
        return overflow;
    }

    @Test
    public void readThatStartedInC1IsNotReturnedFromTheBaseCopyOnceTheKeyIsInC3()
    {
        List<String> overflow = divergeCopies();
        // the coordinator sees BACKFILL when the read starts and STEADY when it has the rows of the base table
        metadata.set(keyIn(Phase.BACKFILL), keyIn(Phase.STEADY));
        List<String> result = clusterings(LayoutReads.read(group(logical, K), ConsistencyLevel.ONE, Dispatcher.RequestTime.forImmediateExecution()));
        assertEquals(List.of("base", "overflow"), reads);
        assertEquals(overflow, result);
        assertEquals("before, after the base read, after the fan-out", 3, metadata.lookups);

        // without the change the base copy is returned, after one check
        reads.clear();
        metadata.set(keyIn(Phase.BACKFILL));
        result = clusterings(LayoutReads.read(group(logical, K), ConsistencyLevel.ONE, Dispatcher.RequestTime.forImmediateExecution()));
        assertEquals(List.of("base"), reads);
        assertFalse(result.contains("c1=2, c2=50"));
        assertEquals(overflow.size() - 1, result.size());
        assertEquals(2, metadata.lookups);

        // the same through a statement
        reads.clear();
        metadata.set(keyIn(Phase.ANNOUNCED), keyIn(Phase.STEADY));
        assertEquals(overflow.size(), rows("SELECT * FROM %s WHERE p = 1 LIMIT 1000", t, 0).size());
        assertEquals(List.of("base", "overflow"), reads);
    }

    @Test
    public void changesThatAreNotRoutingChangesDoNotReExecute()
    {
        KeyState backfill = state(Phase.BACKFILL, INCARNATION);
        KeyState reported = new KeyState(Phase.BACKFILL, backfill.phaseEpoch, 1000, INCARNATION, 1, backfill.generationEpoch,
                                         ImmutableSet.of(new NodeId(1)), ACTION);
        KeyState restarted = new KeyState(Phase.BACKFILL, Epoch.create(99), 2000, INCARNATION, 2, Epoch.create(99), ImmutableSet.of(), ACTION);
        for (KeyState after : new KeyState[]{ reported, restarted })
        {
            reads.clear();
            metadata.set(keyIn(Phase.BACKFILL), with(ImmutableMap.of(key(K), after)));
            assertNotNull(LayoutReads.read(group(logical, K), ConsistencyLevel.ONE, Dispatcher.RequestTime.forImmediateExecution()));
            assertEquals(List.of("base"), reads);
        }
    }

    @Test
    public void keyThatLeftTheRegistryIsReadOnTheStockPath()
    {
        metadata.set(keyIn(Phase.ABORTING), with(ImmutableMap.of(key(1000), state(Phase.BACKFILL, 1))));
        assertNull(LayoutReads.read(group(logical, K), ConsistencyLevel.ONE, Dispatcher.RequestTime.forImmediateExecution()));
        assertEquals("the first result was discarded", List.of("base"), reads);
        assertEquals(2, metadata.lookups);
    }

    @Test
    public void threeChangesCauseReadTimeout()
    {
        metadata.set(keyIn(Phase.VERIFY), keyIn(Phase.CUTOVER), keyIn(Phase.ROLLBACK_DUAL), keyIn(Phase.ABORTING));
        try
        {
            LayoutReads.read(group(logical, K), ConsistencyLevel.ONE, Dispatcher.RequestTime.forImmediateExecution());
            fail("expected a read timeout");
        }
        catch (ReadTimeoutException e)
        {
            assertTrue(e.getMessage(), e.getMessage().contains("3 attempts"));
            assertEquals(ConsistencyLevel.ONE, e.consistency);
        }
        assertEquals(List.of("base", "overflow", "base"), reads);
        assertEquals(4, metadata.lookups);

        // a new incarnation is a change too: nothing is merged from the buckets of two incarnations
        reads.clear();
        metadata.set(keyIn(Phase.STEADY), with(ImmutableMap.of(key(K), state(Phase.STEADY, INCARNATION + 1))));
        assertEquals(List.of(), clusterings(LayoutReads.read(group(logical, K), ConsistencyLevel.ONE, Dispatcher.RequestTime.forImmediateExecution())));
        assertEquals(List.of("overflow", "overflow"), reads);
    }

    @Test
    public void lazyFanOutChecksEveryChunkBeforeItsRowsAreReturned()
    {
        DatabaseDescriptor.getAutonomicConfig().layout.fanout_eager_rows = 5;
        DatabaseDescriptor.getAutonomicConfig().layout.fanout_chunk_rows = 3;
        int total = rows("SELECT * FROM %s WHERE p = 1", twin, 0).size();

        // unchanged: one lookup before, one after the first chunks, one per later chunk
        metadata.set(keyIn(Phase.CUTOVER));
        assertEquals(total, clusterings(LayoutReads.read(group(logical, K), ConsistencyLevel.ONE, Dispatcher.RequestTime.forImmediateExecution())).size());
        assertTrue("lookups " + metadata.lookups, metadata.lookups > 2 + (total - 3 * BUCKETS) / 3);

        // a change while the first chunks are fetched is an ordinary re-execution
        ClusterMetadata cutover = keyIn(Phase.CUTOVER);
        metadata.set(keyIn(Phase.VERIFY), cutover, cutover);
        assertEquals(total, clusterings(LayoutReads.read(group(logical, K), ConsistencyLevel.ONE, Dispatcher.RequestTime.forImmediateExecution())).size());

        // a change after rows were returned fails the read; no row of the chunk fetched under the change is returned
        metadata.set(cutover, cutover, cutover, keyIn(Phase.ROLLBACK_DUAL));
        PartitionIterator result = LayoutReads.read(group(logical, K), ConsistencyLevel.ONE, Dispatcher.RequestTime.forImmediateExecution());
        int returned = 0;
        try (RowIterator partition = result.next())
        {
            while (partition.hasNext())
            {
                partition.next();
                returned++;
            }
            fail("expected a read timeout after " + returned + " rows");
        }
        catch (ReadTimeoutException e)
        {
            assertTrue(e.getMessage(), e.getMessage().contains("changed while it was read"));
        }
        assertTrue("rows returned before the change: " + returned, returned > 0 && returned <= 3 * BUCKETS + 3);

        // through a statement the client gets the timeout and no rows
        metadata.set(cutover, cutover, cutover, keyIn(Phase.ROLLBACK_DUAL));
        try
        {
            rows("SELECT * FROM %s WHERE p = 1", t, 0);
            fail("expected a read timeout");
        }
        catch (ReadTimeoutException expected)
        {
            // the request failed as a whole
        }
    }

    /**
     * T6.11 review: the probe of a watched key ends with its result, so the later chunks of a lazy fan-out are part of
     * the read's latency, and a failure while it is consumed is a failed read.
     */
    @Test
    public void aLazyFanOutIsTimedUntilItsResultIsClosedAndAFailureWhileConsumedCounts()
    {
        DatabaseDescriptor.getAutonomicConfig().layout.fanout_eager_rows = 5;
        DatabaseDescriptor.getAutonomicConfig().layout.fanout_chunk_rows = 3;
        LayoutProfiler.watch(logical.id, key(K), 60_000);
        ClusterMetadata cutover = keyIn(Phase.CUTOVER);
        metadata.set(cutover);
        PartitionIterator result = LayoutReads.read(group(logical, K), ConsistencyLevel.ONE, Dispatcher.RequestTime.forImmediateExecution());
        assertEquals("the read has not ended while its rows are still to be fetched", 0, LayoutProfiler.get(logical.id, key(K)).shape().timedReads);
        int rows = clusterings(result).size();
        assertTrue(rows > 3 * BUCKETS);
        LayoutProfiler.Shape done = LayoutProfiler.get(logical.id, key(K)).shape();
        assertEquals(1, done.timedReads);
        assertEquals(0, done.readErrors);

        // a routing change after rows were returned fails a later chunk: a failed read, not a timed success
        metadata.set(cutover, cutover, cutover, keyIn(Phase.ROLLBACK_DUAL));
        PartitionIterator failing = LayoutReads.read(group(logical, K), ConsistencyLevel.ONE, Dispatcher.RequestTime.forImmediateExecution());
        try (RowIterator partition = failing.next())
        {
            while (partition.hasNext())
                partition.next();
            fail("expected a read timeout");
        }
        catch (ReadTimeoutException expected)
        {
            failing.close();
        }
        LayoutProfiler.Shape failed = LayoutProfiler.get(logical.id, key(K)).shape();
        assertEquals(2, failed.timedReads);
        assertEquals(1, failed.readErrors);
        assertEquals("the failed read adds no latency", done.readMicros, failed.readMicros);
    }

    // ---- serial reads ----

    @Test
    public void serialReadsAreRejectedInEveryPhase()
    {
        // ADR-032: from ANNOUNCED on, like a lightweight transaction, whichever copy the key is read from
        for (Phase phase : Phase.values())
        {
            for (ConsistencyLevel cl : new ConsistencyLevel[]{ ConsistencyLevel.SERIAL, ConsistencyLevel.LOCAL_SERIAL })
            {
                for (String where : new String[]{ " WHERE p = 1 AND c1 = 2 AND c2 = 3", " WHERE p = 1" })
                {
                    metadata.set(keyIn(phase));
                    reads.clear();
                    try
                    {
                        execute("SELECT * FROM " + keyspace() + '.' + t + where, cl, 0, null);
                        fail(phase + " " + cl + where + ": a serial read of an adapted key must be rejected");
                    }
                    catch (InvalidRequestException e)
                    {
                        assertEquals("Lightweight transactions are not supported on partition 0x00000001 of " + keyspace() + '.' + t + " while it is adapted",
                                     e.getMessage());
                        assertEquals(phase + ": nothing was read", List.of(), reads);
                    }
                }
            }
        }
        // a partition that is not split is not concerned
        execute("SELECT * FROM " + keyspace() + '.' + t + " WHERE p = 2 AND c1 = 2 AND c2 = 3", ConsistencyLevel.SERIAL, 0, null);
    }
}
