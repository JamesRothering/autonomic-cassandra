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
import java.util.UUID;

import com.google.common.collect.ImmutableMap;

import org.junit.Test;

import org.apache.cassandra.autonomic.AutonomicService;
import org.apache.cassandra.autonomic.coord.Claim;
import org.apache.cassandra.autonomic.coord.ClusterCoordinator;
import org.apache.cassandra.autonomic.coord.ClusterLedger;
import org.apache.cassandra.autonomic.coord.Lane;
import org.apache.cassandra.autonomic.coord.TcmEnvironment;
import org.apache.cassandra.autonomic.layout.AdvanceLayout;
import org.apache.cassandra.autonomic.layout.LayoutHeader;
import org.apache.cassandra.autonomic.layout.LayoutRegistry;
import org.apache.cassandra.autonomic.layout.LayoutSchemaCoupling;
import org.apache.cassandra.autonomic.layout.Phase;
import org.apache.cassandra.autonomic.layout.RegisterKey;
import org.apache.cassandra.autonomic.layout.RemoveOrphanLayout;
import org.apache.cassandra.autonomic.layout.TableLayout;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * T6.10: client DDL against a table with an adapted layout (H21), on a real log. There is no actuator yet (T6.9), so
 * the registry is committed by hand. What T6.9 must still assert: that an action whose logical table is dropped ends
 * {@code FAILED(TABLE_DROPPED)} and releases its claim; here the claim simply stays where it was.
 */
public class LayoutSchemaCouplingTest extends TestBaseImpl
{
    private static final UUID LAYOUT_ID = new UUID(12, 12);
    private static final String KEY = "010203";
    private static final String PHYSICAL = "t_al_00000001";

    private static ClusterCoordinator coordinator()
    {
        return new ClusterCoordinator(new TcmEnvironment(Clock.Global::currentTimeMillis, DatabaseDescriptor::getAutonomicConfig));
    }

    private static boolean tableExists(IInvokableInstance node, String name)
    {
        return node.callOnInstance(() -> Schema.instance.getTableMetadata(KEYSPACE, name) != null);
    }

    private static String registry(IInvokableInstance node)
    {
        return node.callOnInstance(() -> LayoutRegistry.of(ClusterMetadata.current()).toString());
    }

    private static int orphans(IInvokableInstance node)
    {
        return node.callOnInstance(() -> LayoutSchemaCoupling.orphans(ClusterMetadata.current()).size());
    }

    private static String ledger(IInvokableInstance node)
    {
        return node.callOnInstance(() -> ClusterLedger.of(ClusterMetadata.current()).toString());
    }

    private static void rejected(Cluster cluster, String cql, String message)
    {
        try
        {
            cluster.schemaChange(cql);
            fail("expected a rejection of " + cql);
        }
        catch (Throwable t)
        {
            String text = String.valueOf(t.getMessage()) + (t.getCause() == null ? "" : t.getCause().getMessage());
            assertTrue(text, text.contains(message));
        }
    }

    private static void handCommitBackfillRegistry(IInvokableInstance node)
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
                                                   TableLayout.Transform.HASH_SPLIT, LayoutRegistry.HASH_ALGO, 8, 2);
            ByteBuffer key = ByteBufferUtil.hexToBytes(KEY);
            RegisterKey t = new RegisterKey(own.actionId, own.owner, own.step, header, key, Clock.Global.currentTimeMillis(), 600_000);
            assertTrue(c.commitInstead(own, t.wrap(), Phase.ANNOUNCED.name(), own.state).ok());
            for (Phase[] step : new Phase[][]{ { Phase.ANNOUNCED, Phase.DUAL_WRITE }, { Phase.DUAL_WRITE, Phase.BACKFILL } })
            {
                Claim now = c.ownClaim(UUID.fromString(id));
                AdvanceLayout a = new AdvanceLayout(now.actionId, now.owner, now.step, logical, key, step[0], step[1], 1,
                                                    Clock.Global.currentTimeMillis(), 600_000);
                assertTrue(c.commitInstead(now, a.wrap(), step[1].name(), now.state).ok());
            }
        });
    }

    @Test
    public void clientDdlOnAnAdaptedTable() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withConfig(c -> c.with(NETWORK, GOSSIP).set("autonomic.enabled", c.num() == 1))
                                           .start()))
        {
            IInvokableInstance n1 = cluster.get(1);
            IInvokableInstance n2 = cluster.get(2);
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".t (p int, c1 int, c2 text, v text, PRIMARY KEY (p, c1, c2))");
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + "." + PHYSICAL + " (p int, \"__al_bucket\" int, c1 int, c2 text, v text, "
                                 + "PRIMARY KEY ((p, \"__al_bucket\"), c1, c2))");
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".other (p int, c int, v int, PRIMARY KEY (p, c))");
            ClusterUtils.waitForCMSToQuiesce(cluster, n1);
            handCommitBackfillRegistry(n1);
            // since T6.7 both replicas report their (empty) backfill by themselves; the registry is at rest after that
            long deadline = System.nanoTime() + 60_000_000_000L;
            while (!registry(n1).matches("(?s).*backfillDone=\\[NodeId\\{id=\\d+\\}, NodeId\\{id=\\d+\\}\\].*") && System.nanoTime() < deadline)
                Thread.sleep(100);
            ClusterUtils.waitForCMSToQuiesce(cluster, n1);
            String expected = registry(n1);
            assertTrue(expected, expected.contains("}, NodeId{"));
            assertTrue(expected, expected.contains("BACKFILL"));

            // regular columns are mirrored since T6.14 (LayoutColumnMirrorTest); the registry is not touched by it
            cluster.schemaChange("ALTER TABLE " + KEYSPACE + ".t ADD w text");
            ClusterUtils.waitForCMSToQuiesce(cluster, n1);
            for (IInvokableInstance node : new IInvokableInstance[]{ n1, n2 })
                assertTrue(node.callOnInstance(() -> Schema.instance.getTableMetadata(KEYSPACE, PHYSICAL).getColumn(
                    ColumnIdentifier.getInterned("w", false)) != null));
            assertEquals(expected, registry(n1));
            rejected(cluster, "ALTER TABLE " + KEYSPACE + ".t ADD s int static", "STATIC_COLUMN");
            rejected(cluster, "ALTER TABLE " + KEYSPACE + "." + PHYSICAL + " ADD z text", "is managed");
            rejected(cluster, "DROP TABLE " + KEYSPACE + "." + PHYSICAL, "is managed");
            assertEquals(expected, registry(n1));

            // DDL on unrelated tables is unaffected, and so are parameters of the adapted table
            cluster.schemaChange("ALTER TABLE " + KEYSPACE + ".other ADD w int");
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".fresh (p int, c int, PRIMARY KEY (p, c))");
            cluster.schemaChange("ALTER TABLE " + KEYSPACE + ".t WITH comment = 'x' AND gc_grace_seconds = 50000");
            ClusterUtils.waitForCMSToQuiesce(cluster, n1);
            for (IInvokableInstance node : new IInvokableInstance[]{ n1, n2 })
                assertEquals(50000, (int) node.callOnInstance(() -> Schema.instance.getTableMetadata(KEYSPACE, PHYSICAL).params.gcGraceSeconds));

            // replay after restart gives the same schema and registry, on the node with the layer on and the one without
            ClusterUtils.restartUnchecked(n2);
            ClusterUtils.restartUnchecked(n1);
            for (IInvokableInstance node : new IInvokableInstance[]{ n1, n2 })
            {
                assertTrue(tableExists(node, "fresh"));
                assertTrue(node.callOnInstance(() -> Schema.instance.getTableMetadata(KEYSPACE, "other").getColumn(
                    ColumnIdentifier.getInterned("w", false)) != null));
                assertEquals(50000, (int) node.callOnInstance(() -> Schema.instance.getTableMetadata(KEYSPACE, PHYSICAL).params.gcGraceSeconds));
                assertEquals(0, orphans(node));
            }
            String afterRestart = registry(n1);
            assertTrue(afterRestart.contains("BACKFILL"));
            assertEquals(afterRestart, registry(n2));

            // DROP TABLE of the logical table during BACKFILL drops both tables in the same epoch
            cluster.schemaChange("DROP TABLE " + KEYSPACE + ".t");
            ClusterUtils.waitForCMSToQuiesce(cluster, n1);
            for (IInvokableInstance node : new IInvokableInstance[]{ n1, n2 })
            {
                assertFalse(tableExists(node, "t"));
                assertFalse(tableExists(node, PHYSICAL));
                assertTrue(tableExists(node, "other"));
                // the registry entry is an orphan that routers ignore; the claim is untouched (T6.9 ends the action)
                assertEquals(1, orphans(node));
                assertTrue(registry(node).contains("BACKFILL"));
            }
            assertTrue(ledger(n1), ledger(n1).contains("phase=BACKFILL"));

            // the orphan survives a restart unchanged, then any node may remove it
            ClusterUtils.restartUnchecked(n2);
            assertEquals(1, orphans(n2));
            assertFalse(tableExists(n2, PHYSICAL));
            n1.runOnInstance(() -> {
                LayoutRegistry reg = LayoutRegistry.of(ClusterMetadata.current());
                TableId logical = reg.byLogical.keySet().iterator().next();
                ClusterMetadataService.instance().commit(new RemoveOrphanLayout(logical).wrap());
            });
            ClusterUtils.waitForCMSToQuiesce(cluster, n1);
            assertEquals(0, orphans(n1));
            assertEquals(LayoutRegistry.EMPTY.toString(), registry(n1));
        }
    }
}
