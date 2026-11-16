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
import org.apache.cassandra.autonomic.layout.LayoutRegistryValue;
import org.apache.cassandra.autonomic.layout.Phase;
import org.apache.cassandra.autonomic.layout.RegisterKey;
import org.apache.cassandra.autonomic.layout.ReportBackfill;
import org.apache.cassandra.autonomic.layout.TableLayout;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * The layout registry on a real metadata log (T6.1): the CUSTOM transformations and the extension value through
 * restart replay, {@code nodetool cms snapshot} and snapshot-plus-replay, with a non-empty registry, on a node with
 * the layer enabled and one with it disabled.
 */
public class LayoutRegistryTest extends TestBaseImpl
{
    private static final UUID LAYOUT_ID = new UUID(12, 12);
    private static final long LEASE_MILLIS = 600_000;
    private static final String KEY_1 = "010203";
    private static final String KEY_2 = "0405";

    private static ClusterCoordinator coordinator()
    {
        return new ClusterCoordinator(new TcmEnvironment(Clock.Global::currentTimeMillis, DatabaseDescriptor::getAutonomicConfig));
    }

    private static ByteBuffer bytes(String hex)
    {
        return org.apache.cassandra.utils.ByteBufferUtil.hexToBytes(hex);
    }

    private static String registry(IInvokableInstance node)
    {
        return node.callOnInstance(() -> LayoutRegistry.of(ClusterMetadata.current()).toString());
    }

    private static long epoch(IInvokableInstance node)
    {
        return ClusterUtils.getCurrentEpoch(node).getEpoch();
    }

    private static boolean present(IInvokableInstance node)
    {
        return node.callOnInstance(() -> ClusterMetadata.current().extensions.containsKey(LayoutRegistryValue.KEY));
    }

    private static String newId(IInvokableInstance node)
    {
        return node.callOnInstance(() -> TimeUUID.Generator.nextTimeUUID().asUUID().toString());
    }

    private static void claimSchemaLayout(IInvokableInstance node, String id)
    {
        node.runOnInstance(() -> {
            ClusterCoordinator.Outcome outcome = coordinator().claim(Lane.SCHEMA_LAYOUT, UUID.fromString(id), "partition_split", "TABLE:t",
                                                                    "CLAIMED", ImmutableMap.of());
            assertTrue(String.valueOf(outcome.detail), outcome.ok());
        });
    }

    /** RegisterKey through the coordinator, which commits it instead of an AdvanceAction (FC2). */
    private static void register(IInvokableInstance node, String id, String keyHex)
    {
        node.runOnInstance(() -> {
            ClusterCoordinator c = coordinator();
            Claim own = c.ownClaim(UUID.fromString(id));
            assertNotNull(own);
            TableId logical = Schema.instance.getTableMetadata(KEYSPACE, "t").id;
            TableId physical = Schema.instance.getTableMetadata(KEYSPACE, "t_al_00000001").id;
            LayoutHeader header = new LayoutHeader(LAYOUT_ID, logical, physical, KEYSPACE, "t", "t_al_00000001",
                                                   TableLayout.Transform.HASH_SPLIT, LayoutRegistry.HASH_ALGO, 8, 2);
            RegisterKey t = new RegisterKey(own.actionId, own.owner, own.step, header, bytes(keyHex), Clock.Global.currentTimeMillis(), LEASE_MILLIS);
            ClusterCoordinator.Outcome outcome = c.commitInstead(own, t.wrap(), Phase.ANNOUNCED.name(), own.state);
            assertTrue(String.valueOf(outcome.detail), outcome.ok());
        });
    }

    private static void advance(IInvokableInstance node, String id, String keyHex, String from, String to, int generation)
    {
        node.runOnInstance(() -> {
            ClusterCoordinator c = coordinator();
            Claim own = c.ownClaim(UUID.fromString(id));
            assertNotNull(own);
            TableId logical = Schema.instance.getTableMetadata(KEYSPACE, "t").id;
            AdvanceLayout t = new AdvanceLayout(own.actionId, own.owner, own.step, logical, bytes(keyHex), Phase.valueOf(from), Phase.valueOf(to),
                                                generation, Clock.Global.currentTimeMillis(), LEASE_MILLIS);
            ClusterCoordinator.Outcome outcome = c.commitInstead(own, t.wrap(), to, own.state);
            assertTrue(String.valueOf(outcome.detail), outcome.ok());
        });
    }

    private static void reportBackfill(IInvokableInstance node, String keyHex, int generation, int nodeId)
    {
        node.runOnInstance(() -> {
            TableId logical = Schema.instance.getTableMetadata(KEYSPACE, "t").id;
            org.apache.cassandra.tcm.Epoch started = LayoutRegistry.of(ClusterMetadata.current()).layout(logical).keyState(bytes(keyHex)).generationEpoch;
            ClusterMetadataService.instance().commit(new ReportBackfill(logical, bytes(keyHex), generation, started,
                                                                        new org.apache.cassandra.tcm.membership.NodeId(nodeId)).wrap());
        });
    }

    private static Object[][] layoutRows(IInvokableInstance node)
    {
        return node.executeInternal("SELECT partition_key, phase, generation, backfill_done, buckets, physical_table, orphan "
                                    + "FROM system_views.autonomic_layouts");
    }

    @Test
    public void restartAndSnapshotWithANonEmptyRegistry() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withConfig(c -> c.with(NETWORK, GOSSIP).set("autonomic.enabled", c.num() == 1))
                                           .start()))
        {
            IInvokableInstance n1 = cluster.get(1);
            IInvokableInstance n2 = cluster.get(2);
            assertTrue(n1.callOnInstance(() -> AutonomicService.instance.isEnabled()));
            assertFalse(n2.callOnInstance(() -> AutonomicService.instance.isEnabled()));

            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".t (p int, c1 int, c2 text, v text, PRIMARY KEY (p, c1, c2))");
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".t_al_00000001 (p int, \"__al_bucket\" int, c1 int, c2 text, v text, "
                                 + "PRIMARY KEY ((p, \"__al_bucket\"), c1, c2))");
            ClusterUtils.waitForCMSToQuiesce(cluster, n1);
            assertFalse(present(n1));
            assertEquals(0, layoutRows(n1).length);

            // the operator gate, a claim on the schema layout lane, and the registry changing together with it
            n1.runOnInstance(() -> AutonomicService.instance.enableClusterActions(true));
            String id = newId(n1);
            claimSchemaLayout(n1, id);
            register(n1, id, KEY_1);
            register(n1, id, KEY_2);
            advance(n1, id, KEY_1, "ANNOUNCED", "DUAL_WRITE", 1);
            advance(n1, id, KEY_1, "DUAL_WRITE", "BACKFILL", 1);
            int n1Id = n1.callOnInstance(() -> ClusterMetadata.current().myNodeId().id());
            int n2Id = n2.callOnInstance(() -> ClusterMetadata.current().myNodeId().id());
            // since T6.7 both replicas report by themselves (the partition is empty); a repeated report changes nothing
            String both = "backfillDone=[NodeId{id=" + Math.min(n1Id, n2Id) + "}, NodeId{id=" + Math.max(n1Id, n2Id) + "}]";
            long deadline = System.nanoTime() + 60_000_000_000L;
            while (!registry(n1).contains(both) && System.nanoTime() < deadline)
                Thread.sleep(100);
            reportBackfill(n2, KEY_1, 1, n2Id);
            ClusterUtils.waitForCMSToQuiesce(cluster, n1);

            String expected = registry(n1);
            assertTrue(expected, expected.contains("BACKFILL") && expected.contains("ANNOUNCED") && expected.contains(both));
            assertEquals(expected, registry(n2));
            // the claim stepped once for each of the four registry commits
            String ledger = n1.callOnInstance(() -> org.apache.cassandra.autonomic.coord.ClusterLedger.of(ClusterMetadata.current()).toString());
            assertTrue(ledger, ledger.contains("step=4") && ledger.contains("phase=BACKFILL"));

            // the virtual table, on the node with the layer enabled and the one without it
            for (IInvokableInstance node : new IInvokableInstance[]{ n1, n2 })
            {
                Object[][] rows = layoutRows(node);
                assertEquals(2, rows.length);
                assertEquals(KEY_1, rows[0][0]);
                assertEquals("BACKFILL", rows[0][1]);
                assertEquals(1, rows[0][2]);
                assertEquals(2, ((Set<?>) rows[0][3]).size());
                assertEquals(8, rows[0][4]);
                assertEquals("t_al_00000001", rows[0][5]);
                assertEquals(false, rows[0][6]);
                assertEquals(KEY_2, rows[1][0]);
                assertEquals("ANNOUNCED", rows[1][1]);
            }

            // ---- restart: a node with the layer disabled, then the CMS node with it enabled ----
            ClusterUtils.restartUnchecked(n2);
            assertEquals(expected, registry(n2));
            ClusterUtils.restartUnchecked(n1);
            assertEquals(expected, registry(n1));
            ClusterUtils.waitForCMSToQuiesce(cluster, n1);
            assertEquals(epoch(n1), epoch(n2));
            assertEquals(2, layoutRows(n2).length);

            // ---- nodetool cms snapshot with a non-empty registry, then more entries, then restarts ----
            long before = epoch(n1);
            n1.nodetoolResult("cms", "snapshot").asserts().success();
            String inSnapshot = n1.callOnInstance(() -> LayoutRegistry.of(ClusterMetadataService.instance().snapshotManager()
                                                                                         .getSnapshot(ClusterMetadata.current().epoch)).toString());
            assertEquals(expected, inSnapshot);
            advance(n1, id, KEY_1, "BACKFILL", "VERIFY", 1);
            advance(n1, id, KEY_2, "ANNOUNCED", "ABORTING", 1);
            ClusterUtils.waitForCMSToQuiesce(cluster, n1);
            String afterSnapshot = registry(n1);
            assertTrue(afterSnapshot, afterSnapshot.contains("VERIFY") && afterSnapshot.contains("ABORTING"));
            assertTrue(epoch(n1) > before);
            assertEquals(afterSnapshot, registry(n2));

            ClusterUtils.restartUnchecked(n2);
            assertEquals(afterSnapshot, registry(n2));
            ClusterUtils.restartUnchecked(n1);
            assertEquals(afterSnapshot, registry(n1));
            ClusterUtils.waitForCMSToQuiesce(cluster, n1);

            // the registry still works after being loaded from a snapshot plus replay: the claim is at step 6
            advance(n1, id, KEY_1, "VERIFY", "CUTOVER", 1);
            ClusterUtils.waitForCMSToQuiesce(cluster, n1);
            assertTrue(registry(n1).contains("CUTOVER"));
            assertEquals(registry(n1), registry(n2));

            // a snapshot taken now and a node catching up on it
            Epoch snapshot = ClusterUtils.snapshotClusterMetadata(n1);
            assertTrue(snapshot.getEpoch() >= epoch(n1));
            ClusterUtils.restartUnchecked(n2);
            assertEquals(registry(n1), registry(n2));
        }
    }
}
