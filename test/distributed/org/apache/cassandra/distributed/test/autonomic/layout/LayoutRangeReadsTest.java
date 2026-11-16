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

package org.apache.cassandra.distributed.test.autonomic.layout;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;

import org.junit.Test;

import org.apache.cassandra.autonomic.AutonomicService;
import org.apache.cassandra.autonomic.coord.Claim;
import org.apache.cassandra.autonomic.coord.ClusterCoordinator;
import org.apache.cassandra.autonomic.coord.Lane;
import org.apache.cassandra.autonomic.coord.TcmEnvironment;
import org.apache.cassandra.autonomic.layout.AdvanceLayout;
import org.apache.cassandra.autonomic.layout.LayoutHeader;
import org.apache.cassandra.autonomic.layout.LayoutRegistry;
import org.apache.cassandra.autonomic.layout.LayoutService;
import org.apache.cassandra.autonomic.layout.Phase;
import org.apache.cassandra.autonomic.layout.PhysicalTables;
import org.apache.cassandra.autonomic.layout.RegisterKey;
import org.apache.cassandra.autonomic.layout.TableLayout;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.test.autonomic.layout.LayoutWritesTest.LAYOUT_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * T6.5: range and token scans over a table with two split partitions, on a real cluster. The registry is committed by
 * hand, as in {@link LayoutReadsTest}; the table is compared with a twin that is never split, through both nodes (node
 * 2 runs with the autonomic layer disabled), in class C1 (the base table is authoritative) and in C3 (the overflow
 * table is), with stray rows left in the base table of the split partitions, with the fan-out lazy as well as eager.
 */
public class LayoutRangeReadsTest extends TestBaseImpl
{
    private static final String COLUMNS = " (p int, c1 int, c2 int, v int, PRIMARY KEY (p, c1, c2))";
    private static final int PARTITIONS = 10;
    private static final int[] PAGE_SIZES = { 1, 3, 7, 19, 20, 21, 5000 };

    private static ClusterCoordinator coordinator()
    {
        return new ClusterCoordinator(new TcmEnvironment(Clock.Global::currentTimeMillis, DatabaseDescriptor::getAutonomicConfig));
    }

    private static ByteBuffer key(int p)
    {
        return Int32Type.instance.decompose(p);
    }

    /** The partitions 1..PARTITIONS in token order. */
    private static List<Integer> ringOrder()
    {
        List<Integer> order = new ArrayList<>();
        for (int p = 1; p <= PARTITIONS; p++)
            order.add(p);
        order.sort((a, b) -> Murmur3Partitioner.instance.getToken(key(a)).compareTo(Murmur3Partitioner.instance.getToken(key(b))));
        return order;
    }

    private static long token(int p)
    {
        return (Long) Murmur3Partitioner.instance.getToken(key(p)).getTokenValue();
    }

    private static String registerKeys(IInvokableInstance node, int... keys)
    {
        node.runOnInstance(() -> AutonomicService.instance.enableClusterActions(true));
        String id = node.callOnInstance(() -> TimeUUID.Generator.nextTimeUUID().asUUID().toString());
        node.runOnInstance(() -> {
            ClusterCoordinator.Outcome outcome = coordinator().claim(Lane.SCHEMA_LAYOUT, UUID.fromString(id), "partition_split", "TABLE:t",
                                                                    "CLAIMED", ImmutableMap.of());
            assertTrue(String.valueOf(outcome.detail), outcome.ok());
        });
        for (int p : keys)
        {
            node.runOnInstance(() -> {
                ClusterCoordinator c = coordinator();
                Claim own = c.ownClaim(UUID.fromString(id));
                assertNotNull(own);
                TableId logical = Schema.instance.getTableMetadata(KEYSPACE, "t").id;
                String physicalName = PhysicalTables.physicalName("t", LAYOUT_ID);
                TableId physical = Schema.instance.getTableMetadata(KEYSPACE, physicalName).id;
                LayoutHeader header = new LayoutHeader(LAYOUT_ID, logical, physical, KEYSPACE, "t", physicalName,
                                                       TableLayout.Transform.HASH_SPLIT, LayoutRegistry.HASH_ALGO, 4, 2);
                RegisterKey t = new RegisterKey(own.actionId, own.owner, own.step, header, key(p), Clock.Global.currentTimeMillis(), 600_000);
                assertTrue(c.commitInstead(own, t.wrap(), Phase.ANNOUNCED.name(), own.state).ok());
            });
        }
        return id;
    }

    /** One step of the claim per key, so the keys advance in turn. */
    private static void advance(Cluster cluster, String id, Phase from, Phase to, int... keys)
    {
        IInvokableInstance node = cluster.get(1);
        String fromName = from.name();
        String toName = to.name();
        for (int p : keys)
        {
            node.runOnInstance(() -> {
                ClusterCoordinator c = coordinator();
                Claim now = c.ownClaim(UUID.fromString(id));
                TableId logical = Schema.instance.getTableMetadata(KEYSPACE, "t").id;
                AdvanceLayout a = new AdvanceLayout(now.actionId, now.owner, now.step, logical, key(p), Phase.valueOf(fromName), Phase.valueOf(toName), 1,
                                                    Clock.Global.currentTimeMillis(), 600_000);
                assertTrue(c.commitInstead(now, a.wrap(), toName, now.state).ok());
            });
        }
        ClusterUtils.waitForCMSToQuiesce(cluster, node);
    }

    private static void write(Cluster cluster, int node, String cql)
    {
        for (String table : new String[]{ "t", "twin" })
            cluster.coordinator(node).execute(String.format(cql, KEYSPACE + '.' + table), ConsistencyLevel.ALL);
    }

    private static List<String> render(Iterator<Object[]> rows)
    {
        List<String> rendered = new ArrayList<>();
        while (rows.hasNext())
            rendered.add(Arrays.deepToString(rows.next()));
        return rendered;
    }

    private static List<String> rows(Cluster cluster, int node, String cql, String table, int pageSize)
    {
        String query = String.format(cql, KEYSPACE + '.' + table);
        return pageSize == 0 ? render(Arrays.asList(cluster.coordinator(node).execute(query, ConsistencyLevel.QUORUM)).iterator())
                             : render(cluster.coordinator(node).executeWithPaging(query, ConsistencyLevel.QUORUM, pageSize));
    }

    /** One page, fetched the way the native transport does: each page is a request of its own, from an explicit paging state. */
    private static String[] page(Cluster cluster, int node, String query, int pageSize, String pagingState)
    {
        String result = cluster.get(node).callOnInstance(() -> {
            org.apache.cassandra.service.QueryState queryState = org.apache.cassandra.service.QueryState.forInternalCalls();
            org.apache.cassandra.service.pager.PagingState state = pagingState.isEmpty()
                ? null
                : org.apache.cassandra.service.pager.PagingState.deserialize(org.apache.cassandra.utils.ByteBufferUtil.hexToBytes(pagingState),
                                                                               org.apache.cassandra.transport.ProtocolVersion.CURRENT);
            org.apache.cassandra.cql3.QueryOptions options = org.apache.cassandra.cql3.QueryOptions.create(
                org.apache.cassandra.db.ConsistencyLevel.QUORUM, List.of(), false, org.apache.cassandra.cql3.PageSize.inRows(pageSize), state, null,
                org.apache.cassandra.transport.ProtocolVersion.CURRENT, KEYSPACE);
            org.apache.cassandra.cql3.CQLStatement statement = org.apache.cassandra.cql3.QueryProcessor.instance.parse(query, queryState, options);
            org.apache.cassandra.transport.messages.ResultMessage.Rows rows = (org.apache.cassandra.transport.messages.ResultMessage.Rows)
                org.apache.cassandra.cql3.QueryProcessor.instance.process(statement, queryState, options,
                                                                         org.apache.cassandra.transport.Dispatcher.RequestTime.forImmediateExecution());
            StringBuilder sb = new StringBuilder();
            for (Object[] row : org.apache.cassandra.distributed.impl.RowUtil.toObjects(rows, true))
                sb.append(Arrays.deepToString(row)).append('\n');
            org.apache.cassandra.service.pager.PagingState next = rows.result.metadata.getPagingState();
            return sb + "|" + (next == null ? "" : org.apache.cassandra.utils.ByteBufferUtil.bytesToHex(
                next.serialize(org.apache.cassandra.transport.ProtocolVersion.CURRENT)));
        });
        int split = result.lastIndexOf('|');
        return new String[]{ result.substring(0, split), result.substring(split + 1) };
    }

    /** The statements, each over the whole table or part of the ring; %s is the table, the other numbers are tokens. */
    private static List<String> catalogue(List<Integer> ring, int s1, int s2)
    {
        List<String> cql = new ArrayList<>();
        cql.add("SELECT * FROM %s");
        cql.add("SELECT p, c1, c2, v FROM %s");
        cql.add("SELECT DISTINCT p FROM %s");
        cql.add("SELECT count(*) FROM %s");
        cql.add("SELECT p, c1, count(*) FROM %s GROUP BY p, c1");
        cql.add("SELECT * FROM %s PER PARTITION LIMIT 3");
        cql.add("SELECT * FROM %s WHERE v > 20 ALLOW FILTERING");
        // LIMIT across segments: inside the first gap, exactly at the end of a partition, inside and after a split partition
        for (int limit : new int[]{ 1, 7, 20, 21, 33, 45, 61, 119, 400 })
            cql.add("SELECT * FROM %s LIMIT " + limit);
        cql.add("SELECT * FROM %s PER PARTITION LIMIT 5 LIMIT 22");
        // token bounds on, around and between the split partitions, and past the ends of the ring
        for (int p : new int[]{ s1, s2, ring.get(0), ring.get(ring.size() - 1) })
        {
            long t = token(p);
            cql.add("SELECT * FROM %s WHERE token(p) >= " + t);
            cql.add("SELECT * FROM %s WHERE token(p) > " + t);
            cql.add("SELECT * FROM %s WHERE token(p) <= " + t);
            cql.add("SELECT * FROM %s WHERE token(p) < " + t);
            cql.add("SELECT * FROM %s WHERE token(p) >= " + t + " LIMIT 25");
            cql.add("SELECT * FROM %s WHERE token(p) > " + t + " LIMIT 41");
        }
        long lo = Math.min(token(s1), token(s2));
        long hi = Math.max(token(s1), token(s2));
        cql.add("SELECT * FROM %s WHERE token(p) >= " + lo + " AND token(p) <= " + hi);
        cql.add("SELECT * FROM %s WHERE token(p) > " + lo + " AND token(p) < " + hi);
        cql.add("SELECT * FROM %s WHERE token(p) >= " + lo + " AND token(p) < " + hi);
        cql.add("SELECT * FROM %s WHERE token(p) > " + lo + " AND token(p) <= " + hi);
        cql.add("SELECT * FROM %s WHERE token(p) > " + lo + " AND token(p) <= " + hi + " LIMIT 30");
        cql.add("SELECT * FROM %s WHERE token(p) >= " + lo + " AND token(p) <= " + lo);
        // the same with clustering restrictions, which become the filter of the fan-out
        cql.add("SELECT * FROM %s WHERE c1 = 2 ALLOW FILTERING");
        cql.add("SELECT * FROM %s WHERE token(p) >= " + lo + " AND c1 > 1 AND c2 < 3 ALLOW FILTERING");
        return cql;
    }

    private static void assertSameAsTwin(Cluster cluster, List<String> catalogue, String what)
    {
        for (String cql : catalogue)
        {
            String expected = String.join("\n", rows(cluster, 1, cql, "twin", 0));
            for (int node = 1; node <= 2; node++)
            {
                assertEquals(what + ", node " + node + ": " + cql, expected, String.join("\n", rows(cluster, node, cql, "t", 0)));
                for (int pageSize : PAGE_SIZES)
                    assertEquals(what + ", node " + node + ", page size " + pageSize + ": " + cql,
                                 expected, String.join("\n", rows(cluster, node, cql, "t", pageSize)));
            }
        }
    }

    /**
     * Pages whose boundary lies inside a split partition: the first page ends {@code offset} rows into it, then pages
     * of 7 follow, each from the paging state of the previous one, through the other node.
     */
    private static void assertBoundariesInSplitPartitions(Cluster cluster, List<Integer> ring, int[] split, String what)
    {
        String query = "SELECT * FROM " + KEYSPACE + ".t";
        String expected = String.join("\n", rows(cluster, 1, "SELECT * FROM %s", "twin", 0)) + '\n';
        for (int p : split)
        {
            int before = 0;
            int size = 0;
            for (String row : rows(cluster, 1, "SELECT p FROM %s", "twin", 0))
            {
                int rowP = Integer.parseInt(row.replaceAll("[^0-9]", ""));
                if (ring.indexOf(rowP) < ring.indexOf(p))
                    before++;
                else if (rowP == p)
                    size++;
            }
            // before the first row, after it, in the middle, before the last row, after the last row, after the first row of the next partition
            for (int offset : new int[]{ 0, 1, size / 2, size - 1, size, size + 1 })
            {
                for (int node = 1; node <= 2; node++)
                {
                    String[] first = page(cluster, node, query, before + offset, "");
                    StringBuilder seen = new StringBuilder(first[0]);
                    String state = first[1];
                    int pages = 1;
                    while (!state.isEmpty())
                    {
                        String[] next = page(cluster, 3 - node, query, 7, state);
                        seen.append(next[0]);
                        state = next[1];
                        pages++;
                    }
                    assertEquals(what + ": partition " + p + ", first page " + (before + offset) + " rows (" + pages + " pages), node " + node,
                                 expected, seen.toString());
                }
            }
        }
    }

    @Test
    public void scansOverTwoSplitPartitions() throws Exception
    {
        List<Integer> ring = ringOrder();
        // one split partition in the middle of the ring, one near its end: gaps before, between and after
        int s1 = ring.get(3);
        int s2 = ring.get(7);
        try (Cluster cluster = init(Cluster.build(2)
                                           .withConfig(c -> c.with(NETWORK, GOSSIP).set("autonomic.enabled", c.num() == 1))
                                           .start()))
        {
            IInvokableInstance n1 = cluster.get(1);
            // the test leaves stray rows in the base copies: the replica backfill (T6.7) would reconcile them in CLEANUP
            cluster.forEach(node -> node.runOnInstance(() -> LayoutService.instance.setBackfillHook(null)));
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".t" + COLUMNS);
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".twin" + COLUMNS);
            String derived = n1.callOnInstance(() -> PhysicalTables.derive(Schema.instance.getTableMetadata(KEYSPACE, "t"), LAYOUT_ID, TableId.generate()));
            cluster.schemaChange(derived);
            ClusterUtils.waitForCMSToQuiesce(cluster, n1);

            String id = registerKeys(n1, s1, s2);
            ClusterUtils.waitForCMSToQuiesce(cluster, n1);
            advance(cluster, id, Phase.ANNOUNCED, Phase.DUAL_WRITE, s1, s2);

            // C1: everything is written to both copies by the write routing
            for (int p = 1; p <= PARTITIONS; p++)
                for (int c1 = 0; c1 < 4; c1++)
                    for (int c2 = 0; c2 < 5; c2++)
                        write(cluster, 1 + ((p + c1) % 2), "INSERT INTO %s (p, c1, c2, v) VALUES (" + p + ", " + c1 + ", " + c2 + ", " + (p * 100 + c1 * 10 + c2) % 47
                                                           + ") USING TIMESTAMP " + (1000 + c1 * 5 + c2));
            List<String> catalogue = catalogue(ring, s1, s2);
            assertSameAsTwin(cluster, catalogue, "DUAL_WRITE");

            advance(cluster, id, Phase.DUAL_WRITE, Phase.BACKFILL, s1, s2);
            advance(cluster, id, Phase.BACKFILL, Phase.VERIFY, s1, s2);
            advance(cluster, id, Phase.VERIFY, Phase.CUTOVER, s1, s2);
            advance(cluster, id, Phase.CUTOVER, Phase.NEW_ONLY, s1, s2);

            // C3: the overflow table is authoritative. Writes reach it only, so the base copies are out of date ...
            write(cluster, 1, "UPDATE %s USING TIMESTAMP 5000 SET v = 4242 WHERE p = " + s1 + " AND c1 = 1 AND c2 = 1");
            write(cluster, 2, "UPDATE %s USING TIMESTAMP 5000 SET v = 4343 WHERE p = " + s2 + " AND c1 = 3 AND c2 = 4");
            write(cluster, 1, "DELETE FROM %s USING TIMESTAMP 5000 WHERE p = " + s1 + " AND c1 = 0 AND c2 = 0");
            write(cluster, 2, "INSERT INTO %s (p, c1, c2, v) VALUES (" + s2 + ", 0, 99, 1) USING TIMESTAMP 5000");
            // ... and stray rows are left in the base copies, on every node, that no read may return
            for (int node = 1; node <= 2; node++)
                for (int p : new int[]{ s1, s2 })
                {
                    cluster.get(node).executeInternal("INSERT INTO " + KEYSPACE + ".t (p, c1, c2, v) VALUES (?, 7, 7, 7)", p);
                    cluster.get(node).executeInternal("INSERT INTO " + KEYSPACE + ".t (p, c1, c2, v) VALUES (?, -1, -1, -1)", p);
                }
            for (String row : rows(cluster, 1, "SELECT p, c1, c2 FROM %s", "t", 0))
                assertFalse(row, row.contains(", 7, 7]") || row.contains(", -1, -1]"));
            assertSameAsTwin(cluster, catalogue, "NEW_ONLY");
            assertBoundariesInSplitPartitions(cluster, ring, new int[]{ s1, s2 }, "NEW_ONLY");

            // the fan-out and the scan read lazily: every unpaged scan, and every page of more than 5 rows
            for (int node = 1; node <= 2; node++)
                cluster.get(node).runOnInstance(() -> {
                    DatabaseDescriptor.getAutonomicConfig().layout.fanout_eager_rows = 5;
                    DatabaseDescriptor.getAutonomicConfig().layout.fanout_chunk_rows = 3;
                });
            assertSameAsTwin(cluster, catalogue, "NEW_ONLY, lazy");
            assertBoundariesInSplitPartitions(cluster, ring, new int[]{ s1, s2 }, "NEW_ONLY, lazy");

            advance(cluster, id, Phase.NEW_ONLY, Phase.CLEANUP, s1, s2);
            assertSameAsTwin(cluster, catalogue, "CLEANUP, lazy");
        }
    }
}
