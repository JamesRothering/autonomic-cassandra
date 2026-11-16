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
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;

import org.junit.Test;

import org.apache.cassandra.autonomic.AutonomicService;
import org.apache.cassandra.autonomic.coord.Claim;
import org.apache.cassandra.autonomic.coord.ClusterCoordinator;
import org.apache.cassandra.autonomic.coord.Lane;
import org.apache.cassandra.autonomic.coord.TcmEnvironment;
import org.apache.cassandra.autonomic.layout.AdvanceLayout;
import org.apache.cassandra.autonomic.layout.BucketFunction;
import org.apache.cassandra.autonomic.layout.FinishKey;
import org.apache.cassandra.autonomic.layout.LayoutHeader;
import org.apache.cassandra.autonomic.layout.LayoutRegistry;
import org.apache.cassandra.autonomic.layout.LayoutService;
import org.apache.cassandra.autonomic.layout.Phase;
import org.apache.cassandra.autonomic.layout.PhysicalTables;
import org.apache.cassandra.autonomic.layout.RegisterKey;
import org.apache.cassandra.autonomic.layout.TableLayout;
import org.apache.cassandra.config.DatabaseDescriptor;
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
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * T6.3: write routing on a real cluster. There is no actuator yet (T6.9), so the registry is committed by hand and
 * moved through the phases without waiting for acknowledgements; the test waits for the log to quiesce instead.
 * Where data landed is read node-locally on every node, which bypasses the routing hooks (design 3.1).
 *
 * <p>Node 2 runs with the autonomic layer disabled: routing is unconditional (design 4.3).
 */
public class LayoutWritesTest extends TestBaseImpl
{
    static final UUID LAYOUT_ID = new UUID(0x1a2b3c4d00004000L, 0x8000000000000003L);
    static final String PHYSICAL = PhysicalTables.physicalName("t", LAYOUT_ID);
    static final int BUCKETS = 4;
    /** The split partition; partition 2 is never split. */
    static final int K = 1;

    private static ClusterCoordinator coordinator()
    {
        return new ClusterCoordinator(new TcmEnvironment(Clock.Global::currentTimeMillis, DatabaseDescriptor::getAutonomicConfig));
    }

    private static ByteBuffer key()
    {
        return Int32Type.instance.decompose(K);
    }

    /**
     * Claims the lane and registers key {@link #K} of table {@code t}, hashed on its first {@code hashed} clustering
     * columns: phase ANNOUNCED, incarnation 1. Returns the action id.
     */
    static String register(IInvokableInstance node, int hashed)
    {
        node.runOnInstance(() -> AutonomicService.instance.enableClusterActions(true));
        String id = node.callOnInstance(() -> TimeUUID.Generator.nextTimeUUID().asUUID().toString());
        node.runOnInstance(() -> {
            ClusterCoordinator.Outcome outcome = coordinator().claim(Lane.SCHEMA_LAYOUT, UUID.fromString(id), "partition_split", "TABLE:t",
                                                                    "CLAIMED", ImmutableMap.of());
            assertTrue(String.valueOf(outcome.detail), outcome.ok());
        });
        node.runOnInstance(() -> {
            ClusterCoordinator c = coordinator();
            Claim own = c.ownClaim(UUID.fromString(id));
            assertNotNull(own);
            TableId logical = Schema.instance.getTableMetadata(KEYSPACE, "t").id;
            TableId physical = Schema.instance.getTableMetadata(KEYSPACE, PHYSICAL).id;
            LayoutHeader header = new LayoutHeader(LAYOUT_ID, logical, physical, KEYSPACE, "t", PHYSICAL,
                                                   TableLayout.Transform.HASH_SPLIT, LayoutRegistry.HASH_ALGO, BUCKETS, hashed);
            RegisterKey t = new RegisterKey(own.actionId, own.owner, own.step, header, key(), Clock.Global.currentTimeMillis(), 600_000);
            assertTrue(c.commitInstead(own, t.wrap(), Phase.ANNOUNCED.name(), own.state).ok());
        });
        return id;
    }

    static void advance(Cluster cluster, String id, Phase fromPhase, Phase toPhase)
    {
        IInvokableInstance node = cluster.get(1);
        String fromName = fromPhase.name();
        String toName = toPhase.name();
        node.runOnInstance(() -> {
            Phase from = Phase.valueOf(fromName);
            Phase to = Phase.valueOf(toName);
            ClusterCoordinator c = coordinator();
            Claim now = c.ownClaim(UUID.fromString(id));
            TableId logical = Schema.instance.getTableMetadata(KEYSPACE, "t").id;
            if (to == Phase.STEADY)
            {
                FinishKey f = new FinishKey(now.actionId, now.owner, now.step, logical, key(), Clock.Global.currentTimeMillis(), 600_000);
                assertTrue(c.commitInstead(now, f.wrap(), to.name(), now.state).ok());
            }
            else
            {
                AdvanceLayout a = new AdvanceLayout(now.actionId, now.owner, now.step, logical, key(), from, to, 1,
                                                    Clock.Global.currentTimeMillis(), 600_000);
                assertTrue(c.commitInstead(now, a.wrap(), to.name(), now.state).ok());
            }
        });
        ClusterUtils.waitForCMSToQuiesce(cluster, node);
        for (IInvokableInstance n : new IInvokableInstance[]{ cluster.get(1), cluster.get(2) })
            assertEquals(toName, n.callOnInstance(() -> {
                LayoutRegistry registry = LayoutRegistry.of(ClusterMetadata.current());
                return registry.byLogical.values().iterator().next().keyState(key()).phase.name();
            }));
    }

    private static int bucketValue(int c)
    {
        return BucketFunction.physicalValue(1, BucketFunction.bucket(Clustering.make(Int32Type.instance.decompose(c)), 1, BUCKETS));
    }

    /** The clusterings of partition p in the base table of the node. */
    private static Set<Integer> base(IInvokableInstance node, int p)
    {
        Set<Integer> cs = new TreeSet<>();
        for (Object[] row : node.executeInternal("SELECT c FROM " + KEYSPACE + ".t WHERE p = ?", p))
            cs.add((Integer) row[0]);
        return cs;
    }

    /** The clusterings of partition p in the overflow table of the node; each must be in the bucket of its clustering. */
    private static Set<Integer> overflow(IInvokableInstance node, int p)
    {
        Set<Integer> cs = new TreeSet<>();
        for (Object[] row : node.executeInternal("SELECT p, \"__al_bucket\", c FROM " + KEYSPACE + '.' + PHYSICAL))
        {
            if ((Integer) row[0] != p)
                continue;
            assertEquals("bucket of clustering " + row[2], bucketValue((Integer) row[2]), (int) (Integer) row[1]);
            cs.add((Integer) row[2]);
        }
        return cs;
    }

    private static void assertLanded(Cluster cluster, String what, Set<Integer> expectedBase, Set<Integer> expectedOverflow)
    {
        for (int i = 1; i <= 2; i++)
        {
            assertEquals(what + ": base table of node " + i, expectedBase, base(cluster.get(i), K));
            assertEquals(what + ": overflow table of node " + i, expectedOverflow, overflow(cluster.get(i), K));
            assertEquals(what + ": overflow table of node " + i + " for the partition that is not split", Set.of(), overflow(cluster.get(i), 2));
        }
    }

    private static void rejected(Cluster cluster, int node, String cql, String message, Object... values)
    {
        try
        {
            cluster.coordinator(node).execute(cql, ConsistencyLevel.QUORUM, values);
            fail("expected a rejection of " + cql);
        }
        catch (Throwable t)
        {
            String text = t.getClass().getName() + ' ' + t.getMessage() + (t.getCause() == null ? "" : ' ' + t.getCause().getMessage());
            assertTrue(text, text.contains("InvalidRequestException") && text.contains(message));
        }
    }

    @Test
    public void dataLandsWhereTheClassSays() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withConfig(c -> c.with(NETWORK, GOSSIP).set("autonomic.enabled", c.num() == 1))
                                           .start()))
        {
            IInvokableInstance n1 = cluster.get(1);
            // the test places every row by hand: the replica backfill (T6.7) would copy the base rows on its own
            cluster.forEach(node -> node.runOnInstance(() -> LayoutService.instance.setBackfillHook(null)));
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".t (p int, c int, v int, PRIMARY KEY (p, c))");
            String derived = n1.callOnInstance(() -> PhysicalTables.derive(Schema.instance.getTableMetadata(KEYSPACE, "t"), LAYOUT_ID, TableId.generate()));
            cluster.schemaChange(derived);
            ClusterUtils.waitForCMSToQuiesce(cluster, n1);

            String insert = "INSERT INTO " + KEYSPACE + ".t (p, c, v) VALUES (?, ?, ?)";
            String lwt = insert + " IF NOT EXISTS";
            String adapted = "Lightweight transactions are not supported on partition 0x00000001 of " + KEYSPACE + ".t while it is adapted";
            Set<Integer> inBase = new TreeSet<>();
            Set<Integer> inOverflow = new TreeSet<>();

            // absent: stock behaviour, including lightweight transactions
            cluster.coordinator(1).execute(insert, ConsistencyLevel.ALL, K, 0, 0);
            cluster.coordinator(2).execute(lwt, ConsistencyLevel.QUORUM, K, 1, 1);
            inBase.add(0);
            inBase.add(1);
            assertLanded(cluster, "absent", inBase, inOverflow);

            String id = register(n1, 1);
            ClusterUtils.waitForCMSToQuiesce(cluster, n1);

            Phase[] path = { Phase.ANNOUNCED, Phase.DUAL_WRITE, Phase.BACKFILL, Phase.VERIFY, Phase.CUTOVER, Phase.NEW_ONLY, Phase.CLEANUP, Phase.STEADY };
            int c = 10;
            for (int i = 0; i < path.length; i++)
            {
                Phase phase = path[i];
                if (i > 0)
                    advance(cluster, id, path[i - 1], phase);
                boolean toBase = phase.coordinatorClass <= 2;
                boolean toOverflow = phase.coordinatorClass >= 1;

                // single statements through each node, the second one at ANY
                for (int node = 1; node <= 2; node++)
                {
                    cluster.coordinator(node).execute(insert, node == 1 ? ConsistencyLevel.ALL : ConsistencyLevel.ANY, K, c, c);
                    cluster.coordinator(node).execute(insert, ConsistencyLevel.ALL, 2, c, c);
                    if (toBase)
                        inBase.add(c);
                    if (toOverflow)
                        inOverflow.add(c);
                    c++;
                }
                // an unlogged and a logged batch over the split partition and another one
                for (String kind : new String[]{ "UNLOGGED ", "" })
                {
                    cluster.coordinator(2).execute("BEGIN " + kind + "BATCH "
                                                   + "INSERT INTO " + KEYSPACE + ".t (p, c, v) VALUES (" + K + ", " + c + ", 0); "
                                                   + "INSERT INTO " + KEYSPACE + ".t (p, c, v) VALUES (" + K + ", " + (c + 1) + ", 0); "
                                                   + "INSERT INTO " + KEYSPACE + ".t (p, c, v) VALUES (2, " + c + ", 0); "
                                                   + "APPLY BATCH", ConsistencyLevel.ALL);
                    for (int added = c; added < c + 2; added++)
                    {
                        if (toBase)
                            inBase.add(added);
                        if (toOverflow)
                            inOverflow.add(added);
                    }
                    c += 2;
                }
                // a range delete of the rows of the first two statements: every copy this class writes loses them
                cluster.coordinator(1).execute("DELETE FROM " + KEYSPACE + ".t WHERE p = ? AND c >= ? AND c < ?", ConsistencyLevel.ALL, K, c - 6, c - 4);
                if (toBase)
                    inBase.removeAll(Set.of(c - 6, c - 5));
                if (toOverflow)
                    inOverflow.removeAll(Set.of(c - 6, c - 5));
                assertLanded(cluster, phase.name(), inBase, inOverflow);
                for (int node = 1; node <= 2; node++)
                    assertTrue("partition 2 is never split", base(cluster.get(node), 2).contains(c - 2));

                // lightweight transactions and direct writes to the overflow table
                for (int node = 1; node <= 2; node++)
                {
                    rejected(cluster, node, lwt, adapted, K, 1000, 0);
                    rejected(cluster, node, "UPDATE " + KEYSPACE + ".t SET v = 1 WHERE p = ? AND c = ? IF v = 0", adapted, K, 0);
                    rejected(cluster, node, "INSERT INTO " + KEYSPACE + '.' + PHYSICAL + " (p, \"__al_bucket\", c, v) VALUES (?, ?, ?, ?)",
                             PHYSICAL + " is managed by the adapted layout of " + KEYSPACE + ".t", K, 256, 0, 0);
                    rejected(cluster, node, "INSERT INTO " + KEYSPACE + '.' + PHYSICAL + " (p, \"__al_bucket\", c, v) VALUES (?, ?, ?, ?) IF NOT EXISTS",
                             "is managed", K, 256, 0, 0);
                }
                cluster.coordinator(1).execute(lwt, ConsistencyLevel.QUORUM, 2, 5000 + i, 0);

                // truncate
                rejected(cluster, 2, "TRUNCATE " + KEYSPACE + '.' + PHYSICAL, "is managed");
                if (phase != Phase.STEADY)
                    rejected(cluster, 2, "TRUNCATE " + KEYSPACE + ".t", "cannot be truncated while a partition is being adapted (phase " + phase + ')');
            }

            // a partition delete in C3 empties every bucket and leaves the base table alone
            cluster.coordinator(2).execute("DELETE FROM " + KEYSPACE + ".t WHERE p = ?", ConsistencyLevel.ALL, K);
            assertLanded(cluster, "partition delete", inBase, Set.of());

            // in STEADY a truncate empties both tables; the key stays split
            cluster.coordinator(1).execute(insert, ConsistencyLevel.ALL, K, 1, 1);
            assertLanded(cluster, "before truncate", inBase, Set.of(1));
            cluster.coordinator(2).execute("TRUNCATE " + KEYSPACE + ".t", ConsistencyLevel.ALL);
            assertLanded(cluster, "after truncate", Set.of(), Set.of());
            for (int node = 1; node <= 2; node++)
                assertEquals(Set.of(), base(cluster.get(node), 2));
            cluster.coordinator(2).execute(insert, ConsistencyLevel.ALL, K, 7, 7);
            assertLanded(cluster, "after truncate", Set.of(), Set.of(7));
        }
    }
}
