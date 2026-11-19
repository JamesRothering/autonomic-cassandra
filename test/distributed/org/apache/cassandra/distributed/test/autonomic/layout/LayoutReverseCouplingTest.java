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
import java.util.Map;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;

import org.junit.Test;

import org.apache.cassandra.autonomic.coord.Claim;
import org.apache.cassandra.autonomic.coord.ClusterCoordinator;
import org.apache.cassandra.autonomic.coord.Lane;
import org.apache.cassandra.autonomic.coord.TcmEnvironment;
import org.apache.cassandra.autonomic.layout.AdvanceLayout;
import org.apache.cassandra.autonomic.layout.BeginUnsplit;
import org.apache.cassandra.autonomic.layout.KeyState;
import org.apache.cassandra.autonomic.layout.LayoutRegistry;
import org.apache.cassandra.autonomic.layout.NaturalReplicas;
import org.apache.cassandra.autonomic.layout.Phase;
import org.apache.cassandra.autonomic.layout.PhysicalTables;
import org.apache.cassandra.autonomic.layout.RemoveKey;
import org.apache.cassandra.autonomic.layout.ReportBackfill;
import org.apache.cassandra.autonomic.layout.TableLayout;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.ClusterMetadataService;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.utils.Clock;
import org.apache.cassandra.utils.TimeUUID;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.apache.cassandra.distributed.test.autonomic.layout.LayoutWritesTest.K;
import static org.apache.cassandra.distributed.test.autonomic.layout.LayoutWritesTest.LAYOUT_ID;
import static org.apache.cassandra.distributed.test.autonomic.layout.LayoutWritesTest.PHYSICAL;
import static org.apache.cassandra.distributed.test.autonomic.layout.LayoutWritesTest.advance;
import static org.apache.cassandra.distributed.test.autonomic.layout.LayoutWritesTest.register;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * T6.17h, design 4.8 and 11.10: DDL and routing coupling during a reverse migration, on a real cluster. There is no
 * reverse actuator yet, so the key is moved by hand through registry transformations: forward to {@code NEW_ONLY},
 * {@code BeginUnsplit} owned by a second action, then the reverse phases. In {@code REVERSE_BACKFILL}: {@code ADD
 * COLUMN} is mirrored onto both tables, a rename and a lightweight transaction are rejected. After {@code RemoveKey}
 * of the last key the layout is gone: the rename and the lightweight transaction are accepted.
 */
public class LayoutReverseCouplingTest extends TestBaseImpl
{
    private static ClusterCoordinator coordinator()
    {
        return new ClusterCoordinator(new TcmEnvironment(Clock.Global::currentTimeMillis, DatabaseDescriptor::getAutonomicConfig));
    }

    private static ByteBuffer key()
    {
        return Int32Type.instance.decompose(K);
    }

    /** Releases the forward action's lane claim and begins the reverse as a second action. Returns its id. */
    private static String beginReverse(Cluster cluster, String forward)
    {
        IInvokableInstance n1 = cluster.get(1);
        // the forward action's cooldown (a release as COMMITTED) is configured short; the reverse is a new action
        n1.runOnInstance(() -> {
            ClusterCoordinator c = coordinator();
            assertTrue(c.release(c.ownClaim(UUID.fromString(forward)), "COMMITTED").ok());
        });
        try
        {
            Thread.sleep(2500);
        }
        catch (InterruptedException e)
        {
            throw new RuntimeException(e);
        }
        String id = n1.callOnInstance(() -> TimeUUID.Generator.nextTimeUUID().asUUID().toString());
        n1.runOnInstance(() -> {
            ClusterCoordinator c = coordinator();
            ClusterCoordinator.Outcome outcome = c.claim(Lane.SCHEMA_LAYOUT, UUID.fromString(id), "partition_split", "TABLE:t", "CLAIMED", ImmutableMap.of());
            assertTrue(String.valueOf(outcome.detail), outcome.ok());
            Claim own = c.ownClaim(UUID.fromString(id));
            TableId logical = Schema.instance.getTableMetadata(KEYSPACE, "t").id;
            BeginUnsplit begin = new BeginUnsplit(own.actionId, own.owner, own.step, logical, key(), KeyState.ReverseMode.FAST, UUID.fromString(forward),
                                                  Clock.Global.currentTimeMillis(), 600_000);
            assertTrue(c.commitInstead(own, begin.wrap(), Phase.REVERSE_ANNOUNCED.name(), own.state).ok());
        });
        ClusterUtils.waitForCMSToQuiesce(cluster, n1);
        return id;
    }

    private static void advanceReverse(Cluster cluster, String id, Phase from, Phase to)
    {
        String fromName = from.name();
        String toName = to.name();
        cluster.get(1).runOnInstance(() -> {
            ClusterCoordinator c = coordinator();
            Claim now = c.ownClaim(UUID.fromString(id));
            TableId logical = Schema.instance.getTableMetadata(KEYSPACE, "t").id;
            KeyState state = LayoutRegistry.of(ClusterMetadata.current()).layout(logical).keyState(key());
            AdvanceLayout a = new AdvanceLayout(now.actionId, now.owner, now.step, logical, key(), Phase.valueOf(fromName), Phase.valueOf(toName),
                                                state.generation + (from == to ? 1 : 0), Clock.Global.currentTimeMillis(), 600_000);
            assertTrue(c.commitInstead(now, a.wrap(), toName, now.state).ok());
        });
        ClusterUtils.waitForCMSToQuiesce(cluster, cluster.get(1));
    }

    /** Every natural replica of every bucket reports the reconcile generation of REVERSE_CLEANUP, then the last key is removed. */
    private static void removeKey(Cluster cluster, String id)
    {
        cluster.get(1).runOnInstance(() -> {
            ClusterMetadata cm = ClusterMetadata.current();
            TableMetadata logical = Schema.instance.getTableMetadata(KEYSPACE, "t");
            TableMetadata physical = Schema.instance.getTableMetadata(KEYSPACE, PHYSICAL);
            TableLayout layout = LayoutRegistry.of(cm).layout(logical.id);
            KeyState state = layout.keyState(key());
            for (NodeId replica : NaturalReplicas.ofBuckets(cm, logical, physical, key(), state.incarnation, layout.buckets))
                ClusterMetadataService.instance().commit(new ReportBackfill(logical.id, key(), state.generation, state.generationEpoch, replica).wrap());
            ClusterCoordinator c = coordinator();
            Claim now = c.ownClaim(UUID.fromString(id));
            assertTrue(c.commitInstead(now, new RemoveKey(now.actionId, now.owner, now.step, logical.id, key(), Clock.Global.currentTimeMillis(), 600_000).wrap(),
                                       "KEY_REMOVED", now.state).ok());
        });
        ClusterUtils.waitForCMSToQuiesce(cluster, cluster.get(1));
    }

    private static boolean hasColumn(IInvokableInstance node, String table, String column)
    {
        return node.executeInternal("SELECT column_name FROM system_schema.columns WHERE keyspace_name = ? AND table_name = ? AND column_name = ?",
                                    KEYSPACE, table, column).length == 1;
    }

    @Test
    public void ddlInAReversePhaseIsMirroredAndTheLastKeyFreesIt() throws Exception
    {
        try (Cluster cluster = init(Cluster.build(2)
                                           .withConfig(c -> c.with(NETWORK, GOSSIP).set("autonomic.enabled", c.num() == 1)
                                                  .set("autonomic.safety", Map.of("cooldown_after_commit", "1s", "cooldown_after_rollback", "1s")))
                                           .start()))
        {
            IInvokableInstance n1 = cluster.get(1);
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".t (p int, c int, v int, PRIMARY KEY (p, c))");
            String derived = n1.callOnInstance(() -> PhysicalTables.derive(Schema.instance.getTableMetadata(KEYSPACE, "t"), LAYOUT_ID, TableId.generate()));
            cluster.schemaChange(derived);
            ClusterUtils.waitForCMSToQuiesce(cluster, n1);

            String forward = register(n1, 1);
            ClusterUtils.waitForCMSToQuiesce(cluster, n1);
            advance(cluster, forward, Phase.ANNOUNCED, Phase.DUAL_WRITE);
            advance(cluster, forward, Phase.DUAL_WRITE, Phase.BACKFILL);
            advance(cluster, forward, Phase.BACKFILL, Phase.VERIFY);
            advance(cluster, forward, Phase.VERIFY, Phase.CUTOVER);
            advance(cluster, forward, Phase.CUTOVER, Phase.NEW_ONLY);

            String id = beginReverse(cluster, forward);
            advanceReverse(cluster, id, Phase.REVERSE_ANNOUNCED, Phase.REVERSE_DUAL_WRITE);
            advanceReverse(cluster, id, Phase.REVERSE_DUAL_WRITE, Phase.REVERSE_BACKFILL);
            assertEquals(Phase.REVERSE_BACKFILL.name(), n1.callOnInstance(() -> LayoutRegistry.of(ClusterMetadata.current()).byLogical.values().iterator().next()
                                                                                              .keyState(key()).phase.name()));

            // ADD is mirrored onto both tables, on both nodes
            cluster.schemaChange("ALTER TABLE " + KEYSPACE + ".t ADD extra int");
            ClusterUtils.waitForCMSToQuiesce(cluster, n1);
            for (int node = 1; node <= 2; node++)
            {
                assertTrue("logical table on node " + node, hasColumn(cluster.get(node), "t", "extra"));
                assertTrue("overflow table on node " + node, hasColumn(cluster.get(node), PHYSICAL, "extra"));
            }
            // a rename, a lightweight transaction, and a truncate are not allowed while the key is in the layout
            try
            {
                cluster.schemaChange("ALTER TABLE " + KEYSPACE + ".t RENAME c TO c2");
                fail("a rename during a reverse migration");
            }
            catch (Throwable e)
            {
                assertTrue(e.toString(), e.toString().contains("adapted layout"));
            }
            try
            {
                cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".t (p, c, v) VALUES (" + K + ", 1, 1) IF NOT EXISTS", ConsistencyLevel.QUORUM);
                fail("a lightweight transaction during a reverse migration");
            }
            catch (Throwable e)
            {
                assertTrue(e.toString(), e.toString().contains("Lightweight transactions are not supported"));
            }
            try
            {
                cluster.coordinator(1).execute("TRUNCATE " + KEYSPACE + ".t", ConsistencyLevel.ALL);
                fail("a truncate during a reverse migration");
            }
            catch (Throwable e)
            {
                assertTrue(e.toString(), e.toString().contains("cannot be truncated while a partition is being adapted"));
            }

            advanceReverse(cluster, id, Phase.REVERSE_BACKFILL, Phase.REVERSE_VERIFY);
            advanceReverse(cluster, id, Phase.REVERSE_VERIFY, Phase.CUTBACK);
            advanceReverse(cluster, id, Phase.CUTBACK, Phase.BASE_ONLY);
            advanceReverse(cluster, id, Phase.BASE_ONLY, Phase.REVERSE_CLEANUP);
            advanceReverse(cluster, id, Phase.REVERSE_CLEANUP, Phase.REVERSE_CLEANUP);
            removeKey(cluster, id);
            assertTrue(n1.callOnInstance(() -> LayoutRegistry.of(ClusterMetadata.current()).byLogical.isEmpty()));

            // the last key is gone: nothing restricts the table any more
            cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".t (p, c, v) VALUES (" + K + ", 1, 1) IF NOT EXISTS", ConsistencyLevel.QUORUM);
            cluster.schemaChange("ALTER TABLE " + KEYSPACE + ".t RENAME c TO c2");
            ClusterUtils.waitForCMSToQuiesce(cluster, n1);
            for (int node = 1; node <= 2; node++)
                assertTrue("renamed on node " + node, hasColumn(cluster.get(node), "t", "c2"));
        }
    }
}
