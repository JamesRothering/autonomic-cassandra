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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.autonomic.layout.Phase;
import org.apache.cassandra.autonomic.layout.PhysicalTables;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.test.autonomic.layout.LayoutWritesTest.K;
import static org.apache.cassandra.distributed.test.autonomic.layout.LayoutWritesTest.LAYOUT_ID;
import static org.apache.cassandra.distributed.test.autonomic.layout.LayoutWritesTest.advance;
import static org.apache.cassandra.distributed.test.autonomic.layout.LayoutWritesTest.register;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * T6.4: single-partition reads of an adapted partition on a real cluster. The registry is committed by hand, as in
 * {@link LayoutWritesTest}; the data is placed by the write routing of T6.3 while the key is in class C1 and C3. Every
 * statement of the table of design 3.4 is run against the split table and against a twin that is never split, through
 * both nodes; node 2 runs with the autonomic layer disabled.
 */
public class LayoutReadsTest extends TestBaseImpl
{
    private static final String COLUMNS = " (p int, c1 int, c2 int, v int, s text, l list<int>, m map<text, int>, PRIMARY KEY (p, c1, c2))";

    private static final String[] CATALOGUE = {
        // by full primary key, IN on clustering
        "SELECT * FROM %s WHERE p = 1 AND c1 = 2 AND c2 = 3",
        "SELECT * FROM %s WHERE p = 1 AND c1 = 77 AND c2 = 3",
        "SELECT * FROM %s WHERE p = 1 AND c1 = 2 AND c2 IN (5, 1, 3, 99)",
        "SELECT * FROM %s WHERE p = 1 AND (c1, c2) IN ((0, 1), (4, 4), (5, 6), (3, 6))",
        // by partition key only, and with clustering slices
        "SELECT * FROM %s WHERE p = 1",
        "SELECT * FROM %s WHERE p = 1 AND c1 = 2",
        "SELECT * FROM %s WHERE p = 1 AND c1 >= 1 AND c1 < 4",
        "SELECT * FROM %s WHERE p = 1 AND c1 = 2 AND c2 > 2",
        // ORDER BY DESC, LIMIT, PER PARTITION LIMIT
        "SELECT * FROM %s WHERE p = 1 ORDER BY c1 DESC",
        "SELECT * FROM %s WHERE p = 1 ORDER BY c1 DESC, c2 DESC LIMIT 5",
        "SELECT * FROM %s WHERE p = 1 LIMIT 1",
        "SELECT * FROM %s WHERE p = 1 LIMIT 9",
        "SELECT * FROM %s WHERE p = 1 PER PARTITION LIMIT 4",
        // IN on the partition key: split and normal keys mixed
        "SELECT * FROM %s WHERE p IN (1, 2)",
        "SELECT * FROM %s WHERE p IN (2, 1, 3) LIMIT 50",
        "SELECT * FROM %s WHERE p IN (1, 2) PER PARTITION LIMIT 3",
        // aggregates, GROUP BY, DISTINCT
        "SELECT count(*) FROM %s WHERE p = 1",
        "SELECT count(*), sum(v), max(c2) FROM %s WHERE p = 1 AND c1 > 2",
        "SELECT c1, count(*), sum(v), max(c2) FROM %s WHERE p = 1 GROUP BY c1",
        "SELECT c1, count(*) FROM %s WHERE p = 1 GROUP BY c1 LIMIT 3",
        "SELECT p, c1, count(*) FROM %s WHERE p IN (1, 2) GROUP BY p, c1",
        "SELECT DISTINCT p FROM %s WHERE p IN (1, 2)",
        // cells: write time, collections, a subset of the columns
        "SELECT c1, c2, writetime(v), writetime(s) FROM %s WHERE p = 1",
        "SELECT c1, c2, l, m, m['a'] FROM %s WHERE p = 1 AND c1 = 2",
        "SELECT v FROM %s WHERE p = 1",
        // restrictions that need filtering
        "SELECT c1, c2, v FROM %s WHERE p = 1 AND v > 20 ALLOW FILTERING",
        "SELECT c1, c2, v FROM %s WHERE p = 1 AND v > 20 LIMIT 3 ALLOW FILTERING",
        "SELECT c1, c2 FROM %s WHERE p = 1 AND c2 = 3 ALLOW FILTERING",
        };

    private static final int[] PAGE_SIZES = { 1, 7, 5000 };

    private static void write(Cluster cluster, int node, String cql)
    {
        for (String table : new String[]{ "t", "twin" })
            cluster.coordinator(node).execute(String.format(cql, KEYSPACE + '.' + table), ConsistencyLevel.ALL);
    }

    static List<String> render(Iterator<Object[]> rows)
    {
        List<String> rendered = new ArrayList<>();
        while (rows.hasNext())
            rendered.add(Arrays.deepToString(rows.next()));
        return rendered;
    }

    static List<String> rows(Cluster cluster, int node, String cql, String table, int pageSize)
    {
        String query = String.format(cql, KEYSPACE + '.' + table);
        return pageSize == 0 ? render(Arrays.asList(cluster.coordinator(node).execute(query, ConsistencyLevel.QUORUM)).iterator())
                             : render(cluster.coordinator(node).executeWithPaging(query, ConsistencyLevel.QUORUM, pageSize));
    }

    /**
     * One page of the statement, fetched the way the native transport does: the rows, then the paging state as hex
     * (empty after the last page). Each page is a request of its own, as it is for a driver.
     */
    static String[] page(Cluster cluster, int node, String query, int pageSize, String pagingState)
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

    private static void assertSameAsTwin(Cluster cluster, String what)
    {
        for (String cql : CATALOGUE)
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

    private static void assertSerialRejected(Cluster cluster, int node)
    {
        for (ConsistencyLevel cl : new ConsistencyLevel[]{ ConsistencyLevel.SERIAL, ConsistencyLevel.LOCAL_SERIAL })
        {
            try
            {
                cluster.coordinator(node).execute("SELECT * FROM " + KEYSPACE + ".t WHERE p = 1 AND c1 = 2 AND c2 = 3", cl);
                fail("expected a rejection of a " + cl + " read");
            }
            catch (Throwable t)
            {
                String text = t.getClass().getName() + ' ' + t.getMessage();
                assertTrue(text, text.contains("InvalidRequestException")
                                 && text.contains(cl + " reads are not supported on partition 0x00000001 of " + KEYSPACE + ".t while it is adapted"));
            }
        }
        // a partition that is not split can be read serially
        cluster.coordinator(node).execute("SELECT * FROM " + KEYSPACE + ".t WHERE p = 2 AND c1 = 2 AND c2 = 3", ConsistencyLevel.SERIAL);
    }

    @Test
    public void readsOfAnAdaptedPartition() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withConfig(c -> c.with(NETWORK, GOSSIP).set("autonomic.enabled", c.num() == 1))
                                           .start()))
        {
            IInvokableInstance n1 = cluster.get(1);
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".t" + COLUMNS);
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".twin" + COLUMNS);
            String derived = n1.callOnInstance(() -> PhysicalTables.derive(Schema.instance.getTableMetadata(KEYSPACE, "t"), LAYOUT_ID, TableId.generate()));
            cluster.schemaChange(derived);
            ClusterUtils.waitForCMSToQuiesce(cluster, n1);

            String id = register(n1, 2);
            ClusterUtils.waitForCMSToQuiesce(cluster, n1);
            advance(cluster, id, Phase.ANNOUNCED, Phase.DUAL_WRITE);

            // C1: everything is written to both copies by the write routing
            for (int p = K; p <= K + 1; p++)
            {
                for (int c1 = 0; c1 < 6; c1++)
                {
                    for (int c2 = 0; c2 < 7; c2++)
                    {
                        int v = c1 * 10 + c2;
                        write(cluster, 1 + (v % 2), "INSERT INTO %s (p, c1, c2, v, s, l, m) VALUES (" + p + ", " + c1 + ", " + c2 + ", "
                                                    + (c2 == 6 ? "null" : String.valueOf(v)) + ", 's" + v + "', [" + v + ", 1, 2], {'a': " + v + ", 'b': 2})"
                                                    + " USING TIMESTAMP " + (1000 + v) + (c2 == 5 ? " AND TTL 100000" : ""));
                    }
                }
                write(cluster, 1, "DELETE FROM %s USING TIMESTAMP 3000 WHERE p = " + p + " AND c1 = 1 AND c2 = 1");
                write(cluster, 2, "DELETE FROM %s USING TIMESTAMP 3000 WHERE p = " + p + " AND c1 = 3 AND c2 > 4");
            }
            assertSameAsTwin(cluster, "DUAL_WRITE");

            // two paging states taken in C1, one per node
            String paged = "SELECT * FROM %s WHERE p = 1";
            String expected = String.join("\n", rows(cluster, 1, paged, "twin", 0)) + '\n';
            String[][] pages = new String[2][];
            String[] seen = new String[2];
            for (int node = 1; node <= 2; node++)
            {
                pages[node - 1] = page(cluster, node, String.format(paged, KEYSPACE + ".t"), 7, "");
                seen[node - 1] = pages[node - 1][0];
                assertFalse(pages[node - 1][1].isEmpty());
            }

            advance(cluster, id, Phase.DUAL_WRITE, Phase.BACKFILL);
            advance(cluster, id, Phase.BACKFILL, Phase.VERIFY);
            advance(cluster, id, Phase.VERIFY, Phase.CUTOVER);
            assertSameAsTwin(cluster, "CUTOVER");
            for (int node = 1; node <= 2; node++)
                assertSerialRejected(cluster, node);
            advance(cluster, id, Phase.CUTOVER, Phase.NEW_ONLY);

            // ... resume in C3, from the overflow table
            for (int node = 1; node <= 2; node++)
            {
                // the state taken through one node is resumed through the other
                String state = pages[2 - node][1];
                while (!state.isEmpty())
                {
                    String[] next = page(cluster, node, String.format(paged, KEYSPACE + ".t"), 7, state);
                    seen[2 - node] += next[0];
                    state = next[1];
                }
                assertEquals("paging state taken in C1 and resumed in C3", expected, seen[2 - node]);
            }

            // C3: writes reach the overflow table only, so a read of the base table would differ from the twin
            write(cluster, 1, "INSERT INTO %s (p, c1, c2, v, s) VALUES (1, 2, 50, 250, 'late') USING TIMESTAMP 5000");
            write(cluster, 2, "INSERT INTO %s (p, c1, c2, v) VALUES (1, 0, -1, -1) USING TIMESTAMP 5000");
            write(cluster, 1, "DELETE FROM %s USING TIMESTAMP 5000 WHERE p = 1 AND c1 = 4 AND c2 = 0");
            write(cluster, 2, "DELETE FROM %s USING TIMESTAMP 5000 WHERE p = 1 AND c1 = 5 AND c2 >= 3");
            // the read before the write of a list element is a routed read
            write(cluster, 1, "UPDATE %s USING TIMESTAMP 6000 SET l[1] = 99 WHERE p = 1 AND c1 = 2 AND c2 = 3");
            write(cluster, 2, "UPDATE %s USING TIMESTAMP 6000 SET l[0] = 98 WHERE p = 1 AND c1 = 0 AND c2 = 0");
            assertSameAsTwin(cluster, "NEW_ONLY");
            assertEquals("[[" + "[23, 99, 2]" + "]]", Arrays.deepToString(cluster.coordinator(2).execute(
                "SELECT l FROM " + KEYSPACE + ".t WHERE p = 1 AND c1 = 2 AND c2 = 3", ConsistencyLevel.QUORUM)));
            // the base table does not have the late row: the result came from the overflow table
            for (int node = 1; node <= 2; node++)
            {
                assertEquals(0, cluster.get(node).executeInternal("SELECT * FROM " + KEYSPACE + ".t WHERE p = 1 AND c1 = 2 AND c2 = 50").length);
                assertFalse(rows(cluster, node, "SELECT * FROM %s WHERE p = 1 AND c1 = 2 AND c2 = 50", "t", 0).isEmpty());
                assertSerialRejected(cluster, node);
            }

            // ttl: the two statements run at different times, so the remaining time may differ by a little
            for (int node = 1; node <= 2; node++)
            {
                Object[][] split = cluster.coordinator(node).execute("SELECT c1, ttl(v) FROM " + KEYSPACE + ".t WHERE p = 1 AND c2 = 5 ALLOW FILTERING", ConsistencyLevel.QUORUM);
                Object[][] twin = cluster.coordinator(node).execute("SELECT c1, ttl(v) FROM " + KEYSPACE + ".twin WHERE p = 1 AND c2 = 5 ALLOW FILTERING", ConsistencyLevel.QUORUM);
                assertEquals(twin.length, split.length);
                assertTrue(split.length > 0);
                for (int i = 0; i < split.length; i++)
                {
                    assertEquals(twin[i][0], split[i][0]);
                    assertTrue(Arrays.toString(split[i]), Math.abs((Integer) twin[i][1] - (Integer) split[i][1]) <= 30 && (Integer) split[i][1] > 90000);
                }
            }

            // the lazy fan-out: every unpaged read, and every page of more than 5 rows
            for (int node = 1; node <= 2; node++)
                cluster.get(node).runOnInstance(() -> {
                    org.apache.cassandra.config.DatabaseDescriptor.getAutonomicConfig().layout.fanout_eager_rows = 5;
                    org.apache.cassandra.config.DatabaseDescriptor.getAutonomicConfig().layout.fanout_chunk_rows = 3;
                });
            assertSameAsTwin(cluster, "NEW_ONLY, lazy");

            advance(cluster, id, Phase.NEW_ONLY, Phase.CLEANUP);
            advance(cluster, id, Phase.CLEANUP, Phase.STEADY);
            assertSameAsTwin(cluster, "STEADY, lazy");
        }
    }
}
