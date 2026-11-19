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
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.BooleanSupplier;

import com.google.common.collect.ImmutableMap;

import org.junit.Test;

import org.apache.cassandra.autonomic.coord.Claim;
import org.apache.cassandra.autonomic.coord.ClusterCoordinator;
import org.apache.cassandra.autonomic.coord.Lane;
import org.apache.cassandra.autonomic.coord.TcmEnvironment;
import org.apache.cassandra.autonomic.layout.BasePurge;
import org.apache.cassandra.autonomic.layout.BeginUnsplit;
import org.apache.cassandra.autonomic.layout.Eligibility;
import org.apache.cassandra.autonomic.layout.KeyState;
import org.apache.cassandra.autonomic.layout.LayoutRegistry;
import org.apache.cassandra.autonomic.layout.Phase;
import org.apache.cassandra.autonomic.layout.PhysicalTables;
import org.apache.cassandra.autonomic.layout.ReleaseKey;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.distributed.Cluster;
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
import static org.apache.cassandra.distributed.test.autonomic.layout.LayoutWritesTest.K;
import static org.apache.cassandra.distributed.test.autonomic.layout.LayoutWritesTest.LAYOUT_ID;
import static org.apache.cassandra.distributed.test.autonomic.layout.LayoutWritesTest.advance;
import static org.apache.cassandra.distributed.test.autonomic.layout.LayoutWritesTest.register;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * T6.17d: the replicas' purge and residue probe of a slow reverse in {@code REVERSE_ANNOUNCED} (design 11.5), on a real
 * cluster with {@code gc_grace_seconds = 2}. The key is moved by hand through registry transformations (there is no
 * reverse actuator yet): forward to {@code STEADY}, a base partition deletion written on both replicas at {@code Tt},
 * {@code ReleaseKey} as TOMBSTONED(Tt), then {@code BeginUnsplit} in slow mode. Each replica then waits out the
 * gc_grace of its deletion, flushes, forces a compaction of the key, probes, and reports or says why not.
 */
public class SlowReversePurgeTest extends TestBaseImpl
{
    private static final long GC_GRACE_SECONDS = 2;

    private static ClusterCoordinator coordinator()
    {
        return new ClusterCoordinator(new TcmEnvironment(Clock.Global::currentTimeMillis, DatabaseDescriptor::getAutonomicConfig));
    }

    private static ByteBuffer key()
    {
        return Int32Type.instance.decompose(K);
    }

    private static void await(String what, BooleanSupplier condition) throws InterruptedException
    {
        long deadline = System.nanoTime() + 120_000_000_000L;
        while (!condition.getAsBoolean())
        {
            if (System.nanoTime() > deadline)
                fail("timed out waiting for " + what);
            Thread.sleep(200);
        }
    }

    /**
     * Brings key K of table t (gc_grace 2 s, {@code options} appended) to REVERSE_ANNOUNCED in slow mode. The base
     * table holds five rows written at timestamp 1000, {@code extraCellTimestamp} (if positive) a sixth at that
     * timestamp, and a partition deletion at {@code Tt = now} on both nodes.
     */
    private static void toSlowReverse(Cluster cluster, String options, long extraCellTimestamp) throws Exception
    {
        IInvokableInstance n1 = cluster.get(1);
        // the eligibility minimum of gc_grace_seconds is a constant of the replayed state: lower it on every node for this test
        cluster.forEach(node -> node.runOnInstance(() -> Eligibility.setMinGcGraceSecondsForTesting(1)));
        cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".t (p int, c int, v int, PRIMARY KEY (p, c)) WITH gc_grace_seconds = " + GC_GRACE_SECONDS
                             + (options.isEmpty() ? "" : " AND" + options));
        String derived = n1.callOnInstance(() -> PhysicalTables.derive(Schema.instance.getTableMetadata(KEYSPACE, "t"), LAYOUT_ID, TableId.generate()));
        cluster.schemaChange(derived);
        ClusterUtils.waitForCMSToQuiesce(cluster, n1);

        String forward = register(n1, 1);
        ClusterUtils.waitForCMSToQuiesce(cluster, n1);
        Phase[] chain = { Phase.ANNOUNCED, Phase.DUAL_WRITE, Phase.BACKFILL, Phase.VERIFY, Phase.CUTOVER, Phase.NEW_ONLY, Phase.CLEANUP, Phase.STEADY };
        for (int i = 0; i + 1 < chain.length; i++)
            advance(cluster, forward, chain[i], chain[i + 1]);

        // the residue, and the base partition deletion of cleanup, written where the data plane does not look
        long tt = System.currentTimeMillis() * 1000;
        for (int node = 1; node <= 2; node++)
        {
            IInvokableInstance n = cluster.get(node);
            for (int c = 0; c < 5; c++)
                n.executeInternal("INSERT INTO " + KEYSPACE + ".t (p, c, v) VALUES (?, ?, ?) USING TIMESTAMP 1000", K, c, c);
            if (extraCellTimestamp > 0)
                n.executeInternal("INSERT INTO " + KEYSPACE + ".t (p, c, v) VALUES (?, 9, 9) USING TIMESTAMP " + extraCellTimestamp, K);
            n.executeInternal("DELETE FROM " + KEYSPACE + ".t USING TIMESTAMP " + tt + " WHERE p = ?", K);
        }

        n1.runOnInstance(() -> {
            ClusterCoordinator c = coordinator();
            Claim now = c.ownClaim(UUID.fromString(forward));
            TableId logical = Schema.instance.getTableMetadata(KEYSPACE, "t").id;
            ReleaseKey release = new ReleaseKey(now.actionId, now.owner, now.step, logical, key(), KeyState.BaseState.TOMBSTONED, tt,
                                                Clock.Global.currentTimeMillis(), 600_000);
            assertTrue(c.commitInstead(now, release.wrap(), "KEY_RELEASED", now.state).ok());
            assertTrue(c.release(c.ownClaim(UUID.fromString(forward)), "COMMITTED").ok());
        });
        ClusterUtils.waitForCMSToQuiesce(cluster, n1);
        Thread.sleep(2500); // the cooldown of the forward action, configured to 1 s

        String id = n1.callOnInstance(() -> TimeUUID.Generator.nextTimeUUID().asUUID().toString());
        n1.runOnInstance(() -> {
            ClusterCoordinator c = coordinator();
            ClusterCoordinator.Outcome outcome = c.claim(Lane.SCHEMA_LAYOUT, UUID.fromString(id), "partition_split", "TABLE:t", "CLAIMED", ImmutableMap.of());
            assertTrue(String.valueOf(outcome.detail), outcome.ok());
            Claim own = c.ownClaim(UUID.fromString(id));
            TableId logical = Schema.instance.getTableMetadata(KEYSPACE, "t").id;
            BeginUnsplit begin = new BeginUnsplit(own.actionId, own.owner, own.step, logical, key(), KeyState.ReverseMode.SLOW, null,
                                                  Clock.Global.currentTimeMillis(), 600_000);
            assertTrue(c.commitInstead(own, begin.wrap(), Phase.REVERSE_ANNOUNCED.name(), own.state).ok());
        });
        ClusterUtils.waitForCMSToQuiesce(cluster, n1);
    }

    private static Cluster cluster() throws java.io.IOException
    {
        return init(Cluster.build(2)
                           .withConfig(c -> c.with(NETWORK, GOSSIP).set("autonomic.enabled", c.num() == 1)
                                             .set("autonomic.safety", Map.of("cooldown_after_commit", "1s", "cooldown_after_rollback", "1s")))
                           .start());
    }

    /** The nodes that have reported the current generation of the key. */
    private static int reported(IInvokableInstance node)
    {
        return node.callOnInstance(() -> {
            KeyState state = LayoutRegistry.of(ClusterMetadata.current()).byLogical.values().iterator().next().keyState(key());
            assertEquals(Phase.REVERSE_ANNOUNCED, state.phase);
            return state.backfillDone.size();
        });
    }

    private static List<String> reasons(IInvokableInstance node)
    {
        return node.callOnInstance(() -> BasePurge.instance.reasons());
    }

    private static boolean baseHoldsNothing(IInvokableInstance node)
    {
        return node.callOnInstance(() -> BasePurge.probe(Schema.instance.getTableMetadata(KEYSPACE, "t"),
                                                         Schema.instance.getTableMetadata(KEYSPACE, "t").partitioner.decorateKey(key())).clean());
    }

    @Test
    public void afterGcGraceTheProbeIsCleanAndEveryReplicaReports() throws Exception
    {
        try (Cluster cluster = cluster())
        {
            toSlowReverse(cluster, "", 0);
            await("both replicas to report", () -> reported(cluster.get(1)) == 2);
            for (int node = 1; node <= 2; node++)
            {
                assertTrue("the base partition is empty on node " + node, baseHoldsNothing(cluster.get(node)));
                assertTrue(reasons(cluster.get(node)).toString(), reasons(cluster.get(node)).isEmpty());
            }
        }
    }

    @Test
    public void aCellAboveTheTombstoneKeepsTheProbeDirtyAndTheReasonNamesIt() throws Exception
    {
        long future = 4_102_444_800_000_000L;
        try (Cluster cluster = cluster())
        {
            toSlowReverse(cluster, "", future);
            for (int node = 1; node <= 2; node++)
            {
                IInvokableInstance n = cluster.get(node);
                await("the reason of node " + node, () -> reasons(n).stream().anyMatch(r -> r.contains("RESIDUE_ABOVE_TT")));
                String reason = reasons(n).get(0);
                assertTrue(reason, reason.contains(Long.toString(future)));
                assertTrue(reason, reason.contains("needs the operator"));
                assertTrue(!baseHoldsNothing(n));
            }
            Thread.sleep(3000);
            assertEquals("nobody reports", 0, reported(cluster.get(1)));
        }
    }

    @Test
    public void onlyPurgeRepairedTombstonesWithoutRepairKeepsTheProbeDirty() throws Exception
    {
        try (Cluster cluster = cluster())
        {
            toSlowReverse(cluster, " compaction = {'class': 'SizeTieredCompactionStrategy', 'only_purge_repaired_tombstones': 'true'}", 0);
            for (int node = 1; node <= 2; node++)
            {
                IInvokableInstance n = cluster.get(node);
                await("the reason of node " + node, () -> reasons(n).stream().anyMatch(r -> r.contains("ONLY_PURGE_REPAIRED_TOMBSTONES")));
                assertTrue(!baseHoldsNothing(n));
            }
            Thread.sleep(3000);
            assertEquals("nobody reports", 0, reported(cluster.get(1)));
        }
    }
}
