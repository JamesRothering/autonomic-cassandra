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

import org.junit.Test;

import org.apache.cassandra.autonomic.layout.BucketFunction;
import org.apache.cassandra.autonomic.layout.LayoutVerifier;
import org.apache.cassandra.autonomic.layout.Phase;
import org.apache.cassandra.autonomic.layout.PhysicalTables;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.Clock;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.test.autonomic.layout.LayoutWritesTest.BUCKETS;
import static org.apache.cassandra.distributed.test.autonomic.layout.LayoutWritesTest.K;
import static org.apache.cassandra.distributed.test.autonomic.layout.LayoutWritesTest.LAYOUT_ID;
import static org.apache.cassandra.distributed.test.autonomic.layout.LayoutWritesTest.PHYSICAL;
import static org.apache.cassandra.distributed.test.autonomic.layout.LayoutWritesTest.advance;
import static org.apache.cassandra.distributed.test.autonomic.layout.LayoutWritesTest.register;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * T6.8: verification of a partition in phase VERIFY on a real cluster, through both tables at ALL. The copies were
 * written by the write routing of class C1; then rows of the overflow table are changed behind its back, on every
 * replica, and the verifier names them.
 */
public class LayoutVerifierTest extends TestBaseImpl
{
    private static int bucketValue(int c)
    {
        return BucketFunction.physicalValue(1, BucketFunction.bucket(Clustering.make(Int32Type.instance.decompose(c)), 1, BUCKETS));
    }

    /** Runs a verification of the partition on the node to its end; the report is the summary and one line per mismatch. */
    private static String verify(IInvokableInstance node, int pageRows)
    {
        return verify(node, pageRows, LayoutVerifier.Direction.FORWARD);
    }

    private static String verify(IInvokableInstance node, int pageRows, LayoutVerifier.Direction direction)
    {
        String directionName = direction.name();
        return node.callOnInstance(() -> {
            LayoutVerifier.Direction d = LayoutVerifier.Direction.valueOf(directionName);
            ClusterMetadata cm = ClusterMetadata.current();
            TableId logical = Schema.instance.getTableMetadata(KEYSPACE, "t").id;
            LayoutVerifier verifier = LayoutVerifier.forPartition(cm, logical, Int32Type.instance.decompose(K), d, LayoutVerifier.defaultTolerated(d),
                                                                  org.apache.cassandra.db.ConsistencyLevel.ALL, pageRows, 3, 50, Clock.Global::currentTimeMillis);
            long deadline = System.nanoTime() + 60_000_000_000L;
            while (verifier.step() == LayoutVerifier.Status.RUNNING)
            {
                if (System.nanoTime() > deadline)
                    throw new AssertionError("verification does not end: " + verifier.summary());
                long due = verifier.nextDueMillis();
                if (due > Clock.Global.currentTimeMillis())
                {
                    try
                    {
                        Thread.sleep(due - Clock.Global.currentTimeMillis());
                    }
                    catch (InterruptedException e)
                    {
                        throw new RuntimeException(e);
                    }
                }
            }
            StringBuilder sb = new StringBuilder(verifier.summary());
            for (LayoutVerifier.Mismatch m : verifier.mismatches())
                sb.append('\n').append(m.kind).append(' ').append(m.clustering.toString(cm.schema.getKeyspaces().getNullable(KEYSPACE).tables.getNullable("t")));
            return sb.toString();
        });
    }

    @Test
    public void corruptedOverflowRowFailsVerificationWithItsClustering() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withConfig(c -> c.with(NETWORK, GOSSIP).set("autonomic.enabled", c.num() == 1))
                                           .start()))
        {
            IInvokableInstance n1 = cluster.get(1);
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".t (p int, c int, v int, s text, PRIMARY KEY (p, c))");
            String derived = n1.callOnInstance(() -> PhysicalTables.derive(Schema.instance.getTableMetadata(KEYSPACE, "t"), LAYOUT_ID, TableId.generate()));
            cluster.schemaChange(derived);
            ClusterUtils.waitForCMSToQuiesce(cluster, n1);

            String id = register(n1, 1);
            ClusterUtils.waitForCMSToQuiesce(cluster, n1);
            advance(cluster, id, Phase.ANNOUNCED, Phase.DUAL_WRITE);
            // class C1: written to both copies; other partitions are not part of the verification
            for (int c = 0; c < 40; c++)
            {
                cluster.coordinator(1 + c % 2).execute("INSERT INTO " + KEYSPACE + ".t (p, c, v, s) VALUES (" + K + ", " + c + ", " + c + ", 's" + c + "') USING TIMESTAMP "
                                                       + (1000 + c) + (c % 7 == 0 ? " AND TTL 100000" : ""), ConsistencyLevel.ALL);
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".t (p, c, v) VALUES (2, " + c + ", 0)", ConsistencyLevel.ALL);
            }
            advance(cluster, id, Phase.DUAL_WRITE, Phase.BACKFILL);
            advance(cluster, id, Phase.BACKFILL, Phase.VERIFY);

            // equal copies pass, through either node and with pages of any size
            for (int node = 1; node <= 2; node++)
                for (int pageRows : new int[]{ 1, 7, 40, 1000 })
                {
                    String report = verify(cluster.get(node), pageRows);
                    assertTrue(report, report.startsWith("PASSED: 40 rows compared, 0 content mismatches, 0 missing from the overflow table, 0 missing from the base table"));
                }

            // change the overflow table behind the router's back, on every replica: a value, a row, an extra row
            for (int node = 1; node <= 2; node++)
            {
                IInvokableInstance n = cluster.get(node);
                n.executeInternal("INSERT INTO " + KEYSPACE + '.' + PHYSICAL + " (p, \"__al_bucket\", c, v, s) VALUES (?, ?, 13, 999, 's13') USING TIMESTAMP 1013",
                                  K, bucketValue(13));
                n.executeInternal("DELETE FROM " + KEYSPACE + '.' + PHYSICAL + " WHERE p = ? AND \"__al_bucket\" = ? AND c = 22", K, bucketValue(22));
                n.executeInternal("INSERT INTO " + KEYSPACE + '.' + PHYSICAL + " (p, \"__al_bucket\", c, v) VALUES (?, ?, 77, 1) USING TIMESTAMP 1077",
                                  K, bucketValue(77));
            }
            for (int node = 1; node <= 2; node++)
            {
                String report = verify(cluster.get(node), 7);
                String[] lines = report.split("\n");
                assertTrue(report, lines[0].startsWith("FAILED: "));
                assertTrue(report, lines[0].contains("1 content mismatches, 1 missing from the overflow table, 1 missing from the base table"));
                assertEquals(report, 4, lines.length);
                assertTrue(report, report.contains("CONTENT_DIFFERS c=13") || report.contains("CONTENT_DIFFERS 13"));
                assertTrue(report, report.contains("MISSING_FROM_OVERFLOW c=22") || report.contains("MISSING_FROM_OVERFLOW 22"));
                assertTrue(report, report.contains("MISSING_FROM_BASE c=77") || report.contains("MISSING_FROM_BASE 77"));
            }
        }
    }

    /**
     * T6.17e: the verifier with the roles swapped (overflow authoritative, base the target), at a phase in which the
     * read hook sends every read of the key to the overflow table, so that the base table is only reached by the
     * bypass. A base row changed behind the router's back fails it and is named; a base-only row and a base cell that
     * is newer are tolerated and counted.
     */
    @Test
    public void corruptedBaseRowFailsReverseVerificationWithItsClustering() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withConfig(c -> c.with(NETWORK, GOSSIP).set("autonomic.enabled", c.num() == 1))
                                           .start()))
        {
            IInvokableInstance n1 = cluster.get(1);
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".t (p int, c int, v int, s text, PRIMARY KEY (p, c))");
            String derived = n1.callOnInstance(() -> PhysicalTables.derive(Schema.instance.getTableMetadata(KEYSPACE, "t"), LAYOUT_ID, TableId.generate()));
            cluster.schemaChange(derived);
            ClusterUtils.waitForCMSToQuiesce(cluster, n1);

            String id = register(n1, 1);
            ClusterUtils.waitForCMSToQuiesce(cluster, n1);
            advance(cluster, id, Phase.ANNOUNCED, Phase.DUAL_WRITE);
            for (int c = 0; c < 40; c++)
                cluster.coordinator(1 + c % 2).execute("INSERT INTO " + KEYSPACE + ".t (p, c, v, s) VALUES (" + K + ", " + c + ", " + c + ", 's" + c + "') USING TIMESTAMP "
                                                       + (1000 + c), ConsistencyLevel.ALL);
            advance(cluster, id, Phase.DUAL_WRITE, Phase.BACKFILL);
            advance(cluster, id, Phase.BACKFILL, Phase.VERIFY);
            advance(cluster, id, Phase.VERIFY, Phase.CUTOVER);
            advance(cluster, id, Phase.CUTOVER, Phase.NEW_ONLY);

            for (int node = 1; node <= 2; node++)
            {
                String report = verify(cluster.get(node), 7, LayoutVerifier.Direction.REVERSE);
                assertTrue(report, report.startsWith("PASSED: 40 rows compared"));
            }

            for (int node = 1; node <= 2; node++)
            {
                IInvokableInstance n = cluster.get(node);
                n.executeInternal("INSERT INTO " + KEYSPACE + '.' + "t (p, c, v, s) VALUES (?, 13, 999, 's13') USING TIMESTAMP 1013", K);
                n.executeInternal("INSERT INTO " + KEYSPACE + '.' + "t (p, c, v, s) VALUES (?, 5, 5, 's5') USING TIMESTAMP 5000", K);
                n.executeInternal("INSERT INTO " + KEYSPACE + '.' + "t (p, c, v) VALUES (?, 77, 1) USING TIMESTAMP 1077", K);
            }
            for (int node = 1; node <= 2; node++)
            {
                String report = verify(cluster.get(node), 7, LayoutVerifier.Direction.REVERSE);
                String[] lines = report.split("\n");
                assertTrue(report, lines[0].startsWith("FAILED: "));
                assertTrue(report, lines[0].contains("1 content mismatches, 1 missing from the overflow table, 0 missing from the base table, 1 newer in the target, 2 tolerated"));
                assertTrue(report, report.contains("CONTENT_DIFFERS c=13") || report.contains("CONTENT_DIFFERS 13"));
                assertTrue(report, report.contains("NEWER_IN_TARGET c=5") || report.contains("NEWER_IN_TARGET 5"));
            }
        }
    }
}
