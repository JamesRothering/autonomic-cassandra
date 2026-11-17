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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import org.apache.cassandra.autonomic.coord.AdvanceAction;
import org.apache.cassandra.autonomic.coord.Claim;
import org.apache.cassandra.autonomic.coord.ClaimAction;
import org.apache.cassandra.autonomic.coord.ClusterLedger;
import org.apache.cassandra.autonomic.coord.ClusterLedgerValue;
import org.apache.cassandra.autonomic.coord.Lane;
import org.apache.cassandra.autonomic.coord.SetClusterActionsEnabled;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.MultiStepOperation;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.extensions.ExtensionValue;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.membership.NodeVersion;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.tcm.transformations.CustomTransformation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The six layout transformations against hand-built metadata (design 2.4): every accept and reject case, the
 * transition table, the bounds, the serializers, and that the claim and the registry change in one commit.
 */
public class LayoutTransformationTest
{
    static final Version VERSION = NodeVersion.CURRENT_METADATA_VERSION;
    static final NodeId N1 = new NodeId(1);
    static final NodeId N2 = new NodeId(2);
    static final UUID A = new UUID(1, 1);
    static final UUID B = new UUID(2, 2);
    static final long NOW = 1_000_000;
    static final long LEASE = 600_000;
    static final TableId LOGICAL = TableId.fromUUID(new UUID(10, 10));
    static final TableId PHYSICAL = TableId.fromUUID(new UUID(11, 11));
    static final UUID LAYOUT_ID = new UUID(12, 12);
    static final ByteBuffer K1 = ByteBuffer.wrap(new byte[]{ 1, 2, 3 });
    static final ByteBuffer K2 = ByteBuffer.wrap(new byte[]{ 4, 5 });

    @BeforeClass
    public static void init()
    {
        DatabaseDescriptor.toolInitialization();
    }

    // ---- hand-built metadata ----

    static TableMetadata logicalTable()
    {
        return TableMetadata.builder("ks", "t").id(LOGICAL)
                            .addPartitionKeyColumn("p", Int32Type.instance)
                            .addClusteringColumn("c1", Int32Type.instance)
                            .addClusteringColumn("c2", UTF8Type.instance)
                            .addRegularColumn("v", UTF8Type.instance)
                            .build();
    }

    static TableMetadata.Builder physicalBuilder()
    {
        return TableMetadata.builder("ks", "t_al_00000001").id(PHYSICAL)
                            .addPartitionKeyColumn("p", Int32Type.instance)
                            .addPartitionKeyColumn(TableLayout.BUCKET_COLUMN, Int32Type.instance)
                            .addClusteringColumn("c1", Int32Type.instance)
                            .addClusteringColumn("c2", UTF8Type.instance);
    }

    static ClusterMetadata withTables(TableMetadata... tables)
    {
        Keyspaces keyspaces = tables.length == 0 ? Keyspaces.none()
                                                 : Keyspaces.of(KeyspaceMetadata.create("ks", KeyspaceParams.simple(1), Tables.of(tables)));
        return new ClusterMetadata(Murmur3Partitioner.instance, Directory.EMPTY, new DistributedSchema(keyspaces));
    }

    static ClusterMetadata empty()
    {
        return withTables(logicalTable(), physicalBuilder().addRegularColumn("v", UTF8Type.instance).build());
    }

    static ClusterMetadata execute(ClusterMetadata prev, Transformation t)
    {
        Transformation.Result result = t instanceof LayoutTransformation ? ((LayoutTransformation) t).wrap().execute(prev) : t.execute(prev);
        assertTrue(String.valueOf(result), result.isSuccess());
        assertEquals(prev.epoch.nextEpoch(), result.success().metadata.epoch);
        return result.success().metadata;
    }

    static String rejection(ClusterMetadata prev, LayoutTransformation t)
    {
        Transformation.Result result = t.wrap().execute(prev);
        assertTrue("expected a rejection of " + t + " but got " + result, result.isRejected());
        return result.rejected().reason;
    }

    static void assertRejected(String expectedPart, ClusterMetadata prev, LayoutTransformation t)
    {
        String reason = rejection(prev, t);
        assertTrue("'" + reason + "' should contain '" + expectedPart + "'", reason.contains(expectedPart));
    }

    /** Cluster actions enabled and the action A of node 1 holding the schema layout lane at step 0. */
    static ClusterMetadata claimed(ClusterMetadata md)
    {
        md = execute(md, new SetClusterActionsEnabled(true).wrap());
        Claim claim = new Claim(A, "partition_split", "TABLE:ks.t", N1, "CLAIMED", 0, NOW, NOW + LEASE, ImmutableMap.of("request", "r1"));
        return execute(md, new ClaimAction(Lane.SCHEMA_LAYOUT, claim, NOW, 0).wrap());
    }

    static ClusterMetadata start()
    {
        return claimed(empty());
    }

    static LayoutHeader header(int buckets, int hashed)
    {
        return new LayoutHeader(LAYOUT_ID, LOGICAL, PHYSICAL, "ks", "t", "t_al_00000001", TableLayout.Transform.HASH_SPLIT,
                                LayoutRegistry.HASH_ALGO, buckets, hashed);
    }

    static LayoutHeader header()
    {
        return header(8, 2);
    }

    static RegisterKey register(ByteBuffer key, int step)
    {
        return new RegisterKey(A, N1, step, header(), key, NOW + 1, LEASE);
    }

    static AdvanceLayout advance(int step, Phase from, Phase to, int generation)
    {
        return new AdvanceLayout(A, N1, step, LOGICAL, K1, from, to, generation, NOW + 2, LEASE);
    }

    /** The report a replica commits for the generation of the key that is current in {@code md}. */
    static ReportBackfill report(ClusterMetadata md, TableId logical, ByteBuffer key, int generation, NodeId node)
    {
        TableLayout layout = LayoutRegistry.of(md).layout(logical);
        KeyState state = layout == null ? null : layout.keyState(key);
        return new ReportBackfill(logical, key, generation, state == null ? Epoch.EMPTY : state.generationEpoch, node);
    }

    static ClusterLedger ledger(ClusterMetadata md)
    {
        return ClusterLedger.of(md);
    }

    static LayoutRegistry registry(ClusterMetadata md)
    {
        return LayoutRegistry.of(md);
    }

    static KeyState key(ClusterMetadata md, ByteBuffer key)
    {
        return registry(md).layout(LOGICAL).keyState(key);
    }

    /** Metadata in which K1 is in the given phase, owned by A, with the claim of A at step 0. */
    static ClusterMetadata keyIn(Phase phase, int generation, Set<NodeId> done)
    {
        ClusterMetadata md = start();
        // the epoch of the metadata built below
        Epoch e = md.epoch.nextEpoch();
        KeyState state = new KeyState(phase, e, NOW, 1, generation, e, done, phase == Phase.STEADY ? null : A);
        TableLayout layout = new TableLayout(header(), 2, ImmutableMap.of(K1, state), 5);
        LayoutRegistry registry = new LayoutRegistry(LayoutRegistry.FORMAT_VERSION, ImmutableMap.of(LOGICAL, layout));
        return md.transformer().with(LayoutRegistryValue.KEY, new LayoutRegistryValue(registry)).build().metadata;
    }

    static ClusterMetadata keyIn(Phase phase)
    {
        return keyIn(phase, 1, ImmutableSet.of());
    }

    // ---- RegisterKey ----

    @Test
    public void registerKeyCreatesTheLayoutAndAdvancesTheClaimInOneCommit()
    {
        ClusterMetadata prev = start();
        ClusterMetadata next = execute(prev, register(K1, 0));

        TableLayout layout = registry(next).layout(LOGICAL);
        assertEquals(header(), layout.header());
        assertEquals(1, layout.version);
        assertEquals(2, layout.nextIncarnation);
        KeyState k = layout.keyState(K1);
        assertEquals(Phase.ANNOUNCED, k.phase);
        assertEquals(next.epoch, k.phaseEpoch);
        assertEquals(next.epoch, k.generationEpoch);
        assertEquals(NOW + 1, k.phaseSinceMillis);
        assertEquals(1, k.incarnation);
        assertEquals(1, k.generation);
        assertTrue(k.backfillDone.isEmpty());
        assertEquals(A, k.actionId);

        Claim claim = ledger(next).claim(A);
        assertEquals(1, claim.step);
        assertEquals("ANNOUNCED", claim.phase);
        assertEquals(NOW + 1 + LEASE, claim.leaseExpiresAtMillis);
        assertEquals("the claim state is kept", ImmutableMap.of("request", "r1"), claim.state);
        // both values carry the epoch of this one commit
        assertEquals(next.epoch, next.extensions.get(LayoutRegistryValue.KEY).lastModified());
        assertEquals(next.epoch, next.extensions.get(ClusterLedgerValue.KEY).lastModified());
        // the claim of the previous metadata is untouched
        assertEquals(0, ledger(prev).claim(A).step);
        assertFalse(LayoutRegistry.present(prev));
    }

    @Test
    public void secondKeyGetsTheNextIncarnation()
    {
        ClusterMetadata md = execute(start(), register(K1, 0));
        md = execute(md, register(K2, 1));
        TableLayout layout = registry(md).layout(LOGICAL);
        assertEquals(2, layout.keys.size());
        assertEquals(1, layout.keyState(K1).incarnation);
        assertEquals(2, layout.keyState(K2).incarnation);
        assertEquals(3, layout.nextIncarnation);
        assertEquals(2, layout.version);
        assertEquals(2, ledger(md).claim(A).step);
    }

    @Test
    public void registerKeyRejections()
    {
        ClusterMetadata md = start();
        // claim
        assertRejected("does not hold lane", empty(), register(K1, 0));
        assertRejected("owned by node 1", md, new RegisterKey(A, N2, 0, header(), K1, NOW + 1, LEASE));
        assertRejected("step 0, not 3", md, register(K1, 3));
        assertRejected("does not hold lane", md, new RegisterKey(B, N1, 0, header(), K1, NOW + 1, LEASE));
        assertRejected("lease is not positive", md, new RegisterKey(A, N1, 0, header(), K1, NOW + 1, 0));
        ClusterMetadata otherLane = execute(execute(empty(), new SetClusterActionsEnabled(true).wrap()),
                                            new ClaimAction(Lane.MAINTENANCE, new Claim(A, "x", "t", N1, "P", 0, NOW, NOW + LEASE, ImmutableMap.of()), NOW, 0).wrap());
        assertRejected("MAINTENANCE", otherLane, register(K1, 0));
        // a topology change in progress
        ClusterMetadata busy = md.transformer().with(md.inProgressSequences.with(new NodeId(77), Mockito.mock(MultiStepOperation.class)))
                                 .build().metadata;
        assertRejected("in progress", busy, register(K1, 0));
        // tables
        ClusterMetadata noLogical = claimed(withTables(physicalBuilder().addRegularColumn("v", UTF8Type.instance).build()));
        assertRejected("logical table", noLogical, register(K1, 0));
        ClusterMetadata noPhysical = claimed(withTables(logicalTable()));
        assertRejected("physical table", noPhysical, register(K1, 0));
        assertRejected("is ks.t, not ks.other", md, new RegisterKey(A, N1, 0, new LayoutHeader(LAYOUT_ID, LOGICAL, PHYSICAL, "ks", "other", "t_al_00000001",
                                                                                                TableLayout.Transform.HASH_SPLIT, 1, 8, 2), K1, NOW, LEASE));
        assertRejected("is ks.t_al_00000001, not ks.nope", md, new RegisterKey(A, N1, 0, new LayoutHeader(LAYOUT_ID, LOGICAL, PHYSICAL, "ks", "t", "nope",
                                                                                                         TableLayout.Transform.HASH_SPLIT, 1, 8, 2), K1, NOW, LEASE));
        // shape of the physical table
        TableMetadata noBucket = TableMetadata.builder("ks", "t_al_00000001").id(PHYSICAL)
                                              .addPartitionKeyColumn("p", Int32Type.instance)
                                              .addClusteringColumn("c1", Int32Type.instance).addClusteringColumn("c2", UTF8Type.instance)
                                              .addRegularColumn("v", UTF8Type.instance).build();
        assertRejected("partition key has 1 columns", claimed(withTables(logicalTable(), noBucket)), register(K1, 0));
        TableMetadata wrongBucketType = TableMetadata.builder("ks", "t_al_00000001").id(PHYSICAL)
                                                     .addPartitionKeyColumn("p", Int32Type.instance).addPartitionKeyColumn(TableLayout.BUCKET_COLUMN, UTF8Type.instance)
                                                     .addClusteringColumn("c1", Int32Type.instance).addClusteringColumn("c2", UTF8Type.instance)
                                                     .addRegularColumn("v", UTF8Type.instance).build();
        assertRejected("not an int named", claimed(withTables(logicalTable(), wrongBucketType)), register(K1, 0));
        assertRejected("regular columns differ", claimed(withTables(logicalTable(), physicalBuilder().addRegularColumn("w", UTF8Type.instance).build())), register(K1, 0));
        assertRejected("regular columns differ", claimed(withTables(logicalTable(), physicalBuilder().build())), register(K1, 0));
        assertRejected("static columns differ", claimed(withTables(logicalTable(), physicalBuilder().addRegularColumn("v", UTF8Type.instance)
                                                                                                    .addStaticColumn("s", UTF8Type.instance).build())), register(K1, 0));
        TableMetadata otherClustering = TableMetadata.builder("ks", "t_al_00000001").id(PHYSICAL)
                                                     .addPartitionKeyColumn("p", Int32Type.instance).addPartitionKeyColumn(TableLayout.BUCKET_COLUMN, Int32Type.instance)
                                                     .addClusteringColumn("c1", Int32Type.instance)
                                                     .addRegularColumn("v", UTF8Type.instance).build();
        assertRejected("clustering columns differ", claimed(withTables(logicalTable(), otherClustering)), register(K1, 0));
        // the claim is untouched by every rejection above, which is what a rejected commit means
        assertEquals(0, ledger(md).claim(A).step);
    }

    /** Design 2.4 and section 6: RegisterKey re-validates eligibility against the schema of the epoch it commits to. */
    @Test
    public void registerKeyRejectsAnIneligibleLogicalTable()
    {
        TableMetadata cdc = TableMetadata.builder("ks", "t").id(LOGICAL).params(org.apache.cassandra.schema.TableParams.builder().cdc(true).build())
                                         .addPartitionKeyColumn("p", Int32Type.instance).addClusteringColumn("c1", Int32Type.instance)
                                         .addClusteringColumn("c2", UTF8Type.instance).addRegularColumn("v", UTF8Type.instance).build();
        ClusterMetadata md = claimed(withTables(cdc, physicalBuilder().addRegularColumn("v", UTF8Type.instance).build()));
        assertRejected("is not eligible: CDC", md, register(K1, 0));

        TableMetadata withStatic = TableMetadata.builder("ks", "t").id(LOGICAL)
                                                .addPartitionKeyColumn("p", Int32Type.instance).addClusteringColumn("c1", Int32Type.instance)
                                                .addClusteringColumn("c2", UTF8Type.instance).addStaticColumn("s", Int32Type.instance)
                                                .addRegularColumn("v", UTF8Type.instance).build();
        ClusterMetadata staticMd = claimed(withTables(withStatic, physicalBuilder().addStaticColumn("s", Int32Type.instance)
                                                                                    .addRegularColumn("v", UTF8Type.instance).build()));
        assertRejected("STATIC_COLUMN", staticMd, register(K1, 0));
        assertEquals("the claim did not move", 0, ledger(staticMd).claim(A).step);
    }

    @Test
    public void registerKeyBoundsRejected()
    {
        ClusterMetadata md = start();
        assertRejected("buckets 1", md, new RegisterKey(A, N1, 0, header(1, 2), K1, NOW, LEASE));
        assertRejected("buckets 65", md, new RegisterKey(A, N1, 0, header(65, 2), K1, NOW, LEASE));
        assertTrue(execute(md, new RegisterKey(A, N1, 0, header(2, 2), K1, NOW, LEASE)).extensions.containsKey(LayoutRegistryValue.KEY));
        assertTrue(execute(md, new RegisterKey(A, N1, 0, header(64, 1), K1, NOW, LEASE)).extensions.containsKey(LayoutRegistryValue.KEY));
        assertRejected("hashed clustering columns 0", md, new RegisterKey(A, N1, 0, header(8, 0), K1, NOW, LEASE));
        assertRejected("exceeds the 2 clustering columns", md, new RegisterKey(A, N1, 0, header(8, 3), K1, NOW, LEASE));
        assertRejected("unknown hash algorithm 2", md, new RegisterKey(A, N1, 0, new LayoutHeader(LAYOUT_ID, LOGICAL, PHYSICAL, "ks", "t", "t_al_00000001",
                                                                                                  TableLayout.Transform.HASH_SPLIT, 2, 8, 2), K1, NOW, LEASE));
        assertRejected("same table", md, new RegisterKey(A, N1, 0, new LayoutHeader(LAYOUT_ID, LOGICAL, LOGICAL, "ks", "t", "t",
                                                                                   TableLayout.Transform.HASH_SPLIT, 1, 8, 2), K1, NOW, LEASE));
        // key bytes: 1 to 1024
        assertRejected("0 bytes", md, new RegisterKey(A, N1, 0, header(), ByteBuffer.allocate(0), NOW, LEASE));
        assertRejected("1025 bytes", md, new RegisterKey(A, N1, 0, header(), ByteBuffer.allocate(1025), NOW, LEASE));
        assertTrue(execute(md, new RegisterKey(A, N1, 0, header(), ByteBuffer.allocate(1024), NOW, LEASE)).extensions.containsKey(LayoutRegistryValue.KEY));
        assertTrue(execute(md, new RegisterKey(A, N1, 0, header(), ByteBuffer.allocate(1), NOW, LEASE)).extensions.containsKey(LayoutRegistryValue.KEY));
    }

    @Test
    public void registerKeyConflictsWithTheRegistry()
    {
        ClusterMetadata md = execute(start(), register(K1, 0));
        assertRejected("already registered", md, register(K1, 1));
        assertRejected("header differs", md, new RegisterKey(A, N1, 1, header(16, 2), K2, NOW, LEASE));
        // a copy of the bytes is the same key
        assertRejected("already registered", md, register(ByteBuffer.wrap(new byte[]{ 1, 2, 3 }), 1));
    }

    @Test
    public void maximumKeysPerLayout()
    {
        ClusterMetadata md = start();
        for (int i = 0; i < LayoutRegistry.MAX_KEYS; i++)
            md = execute(md, register(ByteBuffer.wrap(new byte[]{ (byte) i }), i));
        assertEquals(LayoutRegistry.MAX_KEYS, registry(md).layout(LOGICAL).keys.size());
        assertRejected("already has 8 keys", md, register(ByteBuffer.wrap(new byte[]{ 99 }), LayoutRegistry.MAX_KEYS));
    }

    @Test
    public void maximumLayoutsInTheRegistry()
    {
        // pure helper: layouts of eight other tables, then a ninth
        LayoutRegistry registry = LayoutRegistry.EMPTY;
        for (int i = 0; i < LayoutRegistry.MAX_LAYOUTS; i++)
        {
            LayoutHeader h = new LayoutHeader(new UUID(100, i), TableId.fromUUID(new UUID(101, i)), TableId.fromUUID(new UUID(102, i)), "ks",
                                              "t" + i, "p" + i, TableLayout.Transform.HASH_SPLIT, 1, 4, 1);
            LayoutRegistry.Result result = registry.withRegisteredKey(h, K1, A, Epoch.create(5), NOW);
            assertFalse(result.rejection, result.isRejected());
            registry = result.registry;
        }
        assertEquals(LayoutRegistry.MAX_LAYOUTS, registry.byLogical.size());
        LayoutHeader ninth = new LayoutHeader(new UUID(100, 9), TableId.fromUUID(new UUID(101, 9)), TableId.fromUUID(new UUID(102, 9)), "ks",
                                              "t9", "p9", TableLayout.Transform.HASH_SPLIT, 1, 4, 1);
        assertTrue(registry.withRegisteredKey(ninth, K1, A, Epoch.create(5), NOW).rejection.contains("already has 8 layouts"));
        // a layout id cannot serve two layouts
        LayoutRegistry one = LayoutRegistry.EMPTY.withRegisteredKey(header(), K1, A, Epoch.create(5), NOW).registry;
        LayoutHeader sameId = new LayoutHeader(LAYOUT_ID, TableId.fromUUID(new UUID(103, 1)), TableId.fromUUID(new UUID(103, 2)), "ks", "u", "u_al", TableLayout.Transform.HASH_SPLIT, 1, 4, 1);
        assertTrue(one.withRegisteredKey(sameId, K1, A, Epoch.create(6), NOW).rejection.contains("already uses"));
        // nor a physical table
        LayoutHeader samePhysical = new LayoutHeader(new UUID(7, 7), TableId.fromUUID(new UUID(103, 1)), PHYSICAL, "ks", "u", "u_al", TableLayout.Transform.HASH_SPLIT, 1, 4, 1);
        assertTrue(one.withRegisteredKey(samePhysical, K1, A, Epoch.create(6), NOW).rejection.contains("already uses"));
    }

    @Test
    public void incarnationsAreBounded()
    {
        LayoutRegistry registry = LayoutRegistry.EMPTY.withRegisteredKey(header(), K1, A, Epoch.create(5), NOW).registry;
        TableLayout layout = registry.layout(LOGICAL);
        TableLayout exhausted = new TableLayout(layout.header(), LayoutRegistry.MAX_INCARNATION, layout.keys, layout.version);
        LayoutRegistry full = new LayoutRegistry(1, ImmutableMap.of(LOGICAL, exhausted));
        assertTrue(full.withRegisteredKey(header(), K2, A, Epoch.create(6), NOW).rejection.contains("all incarnations"));
        TableLayout last = new TableLayout(layout.header(), LayoutRegistry.MAX_INCARNATION - 1, layout.keys, layout.version);
        LayoutRegistry.Result ok = new LayoutRegistry(1, ImmutableMap.of(LOGICAL, last)).withRegisteredKey(header(), K2, A, Epoch.create(6), NOW);
        assertEquals(LayoutRegistry.MAX_INCARNATION - 1, ok.registry.layout(LOGICAL).keyState(K2).incarnation);
        // incarnation * 256 + bucket still fits an int
        assertTrue((long) (LayoutRegistry.MAX_INCARNATION - 1) * 256 + 255 <= Integer.MAX_VALUE);
        try
        {
            new KeyState(Phase.ANNOUNCED, Epoch.create(1), 0, LayoutRegistry.MAX_INCARNATION, 1, Epoch.create(1), ImmutableSet.of(), A);
            fail();
        }
        catch (IllegalArgumentException expected)
        {
            // out of range
        }
    }

    // ---- AdvanceLayout ----

    private static final Set<Phase> NOWHERE = EnumSet.noneOf(Phase.class);

    /** The allowed pairs of design 4.2, written out independently of {@link Phase}. */
    private static Set<Phase> legalTargets(Phase from)
    {
        switch (from)
        {
            case ANNOUNCED: return EnumSet.of(Phase.DUAL_WRITE, Phase.ABORTING);
            case DUAL_WRITE: return EnumSet.of(Phase.BACKFILL, Phase.ABORTING);
            case BACKFILL: return EnumSet.of(Phase.VERIFY, Phase.ABORTING);
            case VERIFY: return EnumSet.of(Phase.CUTOVER, Phase.BACKFILL, Phase.ABORTING);
            case CUTOVER: return EnumSet.of(Phase.NEW_ONLY, Phase.ROLLBACK_DUAL);
            case NEW_ONLY: return EnumSet.of(Phase.CLEANUP);
            case ROLLBACK_DUAL: return EnumSet.of(Phase.ABORTING);
            default: return NOWHERE; // CLEANUP leaves through FinishKey; STEADY and ABORTING have no AdvanceLayout
        }
    }

    @Test
    public void everyPairOfPhasesIsAcceptedOrRejectedAsTheTable()
    {
        for (Phase from : Phase.values())
        {
            ClusterMetadata md = keyIn(from);
            for (Phase to : Phase.values())
            {
                boolean legal = legalTargets(from).contains(to);
                boolean restart = from == to && (from == Phase.BACKFILL || from == Phase.CLEANUP);
                boolean newGeneration = restart || (from == Phase.VERIFY && to == Phase.BACKFILL);
                int generation = newGeneration ? 2 : 1;
                String what = from + " -> " + to;
                if (legal || restart)
                {
                    ClusterMetadata next = execute(md, advance(0, from, to, generation));
                    KeyState k = key(next, K1);
                    assertEquals(what, to, k.phase);
                    assertEquals(what, generation, k.generation);
                    assertEquals(what, next.epoch, k.phaseEpoch);
                    assertEquals(what, NOW + 2, k.phaseSinceMillis);
                    assertEquals(what, newGeneration ? next.epoch : md.epoch, k.generationEpoch);
                    assertEquals(what, 6, registry(next).layout(LOGICAL).version);
                    assertEquals(what, A, k.actionId);
                    Claim claim = ledger(next).claim(A);
                    assertEquals(what, 1, claim.step);
                    assertEquals(what, to.name(), claim.phase);
                }
                else
                {
                    // a STEADY key is owned by nobody, which is rejected first
                    String reason = from == Phase.STEADY ? "owned by action null" : "illegal transition";
                    assertRejected(reason, md, advance(0, from, to, 1));
                    assertRejected(reason, md, advance(0, from, to, 2));
                    assertEquals(what, 0, ledger(md).claim(A).step);
                }
            }
        }
    }

    @Test
    public void theForwardChainEndToEndWithOneClaim()
    {
        ClusterMetadata md = execute(start(), register(K1, 0));
        Phase[] chain = { Phase.DUAL_WRITE, Phase.BACKFILL, Phase.VERIFY, Phase.CUTOVER, Phase.NEW_ONLY, Phase.CLEANUP };
        Phase from = Phase.ANNOUNCED;
        int step = 1;
        for (Phase to : chain)
        {
            md = execute(md, advance(step++, from, to, 1));
            from = to;
        }
        md = execute(md, new FinishKey(A, N1, step++, LOGICAL, K1, NOW + 3, LEASE));
        KeyState k = key(md, K1);
        assertEquals(Phase.STEADY, k.phase);
        assertNull(k.actionId);
        assertEquals(md.epoch, k.phaseEpoch);
        assertEquals(NOW + 3, k.phaseSinceMillis);
        assertEquals(step, ledger(md).claim(A).step);
        assertEquals("STEADY", ledger(md).claim(A).phase);
        // and no AdvanceLayout leaves STEADY
        for (Phase to : Phase.values())
            assertTrue(rejection(md, advance(step, Phase.STEADY, to, 1)).length() > 0);
    }

    @Test
    public void generationRules()
    {
        ClusterMetadata md = keyIn(Phase.BACKFILL, 3, ImmutableSet.of(N1, N2));
        // a transition keeps the generation
        assertRejected("requires generation 3, not 4", md, advance(0, Phase.BACKFILL, Phase.VERIFY, 4));
        assertRejected("requires generation 3, not 2", md, advance(0, Phase.BACKFILL, Phase.ABORTING, 2));
        // a restart takes the next one and clears the reports
        assertRejected("requires generation 4, not 3", md, advance(0, Phase.BACKFILL, Phase.BACKFILL, 3));
        assertRejected("requires generation 4, not 5", md, advance(0, Phase.BACKFILL, Phase.BACKFILL, 5));
        ClusterMetadata next = execute(md, advance(0, Phase.BACKFILL, Phase.BACKFILL, 4));
        assertTrue(key(next, K1).backfillDone.isEmpty());
        assertEquals(next.epoch, key(next, K1).generationEpoch);
        assertEquals(ImmutableSet.of(N1, N2), key(md, K1).backfillDone);
        // a plain transition keeps the reports and the generation epoch
        ClusterMetadata verify = execute(md, advance(0, Phase.BACKFILL, Phase.VERIFY, 3));
        assertEquals(ImmutableSet.of(N1, N2), key(verify, K1).backfillDone);
        assertEquals(md.epoch, key(verify, K1).generationEpoch);
        // VERIFY -> BACKFILL is a new generation
        ClusterMetadata again = execute(keyIn(Phase.VERIFY, 1, ImmutableSet.of(N1)), advance(0, Phase.VERIFY, Phase.BACKFILL, 2));
        assertTrue(key(again, K1).backfillDone.isEmpty());
        assertEquals(2, key(again, K1).generation);
        // only BACKFILL and CLEANUP restart
        assertRejected("illegal transition", keyIn(Phase.VERIFY), advance(0, Phase.VERIFY, Phase.VERIFY, 2));
        assertRejected("illegal transition", keyIn(Phase.NEW_ONLY), advance(0, Phase.NEW_ONLY, Phase.NEW_ONLY, 2));
        assertTrue(execute(keyIn(Phase.CLEANUP), advance(0, Phase.CLEANUP, Phase.CLEANUP, 2)).extensions.containsKey(LayoutRegistryValue.KEY));
    }

    @Test
    public void advanceLayoutRejections()
    {
        ClusterMetadata md = keyIn(Phase.DUAL_WRITE);
        assertRejected("is in phase DUAL_WRITE, not BACKFILL", md, advance(0, Phase.BACKFILL, Phase.VERIFY, 1));
        assertRejected("owned by node 1", md, new AdvanceLayout(A, N2, 0, LOGICAL, K1, Phase.DUAL_WRITE, Phase.BACKFILL, 1, NOW, LEASE));
        assertRejected("step 0, not 1", md, advance(1, Phase.DUAL_WRITE, Phase.BACKFILL, 1));
        assertRejected("does not hold lane", md, new AdvanceLayout(B, N1, 0, LOGICAL, K1, Phase.DUAL_WRITE, Phase.BACKFILL, 1, NOW, LEASE));
    }

    @Test
    public void advanceLayoutOfUnknownLayoutOrKeyOrOwner()
    {
        ClusterMetadata md = keyIn(Phase.DUAL_WRITE);
        assertRejected("key is not registered", md, new AdvanceLayout(A, N1, 0, LOGICAL, K2, Phase.DUAL_WRITE, Phase.BACKFILL, 1, NOW, LEASE));
        assertRejected("has no layout", md, new AdvanceLayout(A, N1, 0, TableId.fromUUID(new UUID(77, 77)), K1, Phase.DUAL_WRITE, Phase.BACKFILL, 1, NOW, LEASE));
        // the claim is A's, the key belongs to another action: only the owner of the migration may advance it
        ClusterMetadata foreign = keyIn(Phase.DUAL_WRITE);
        TableLayout layout = registry(foreign).layout(LOGICAL);
        KeyState k = layout.keyState(K1);
        KeyState other = new KeyState(k.phase, k.phaseEpoch, k.phaseSinceMillis, k.incarnation, k.generation, k.generationEpoch, k.backfillDone, B);
        foreign = foreign.transformer().with(LayoutRegistryValue.KEY, new LayoutRegistryValue(
            new LayoutRegistry(1, ImmutableMap.of(LOGICAL, layout.withKey(K1, other))))).build().metadata;
        assertRejected("owned by action " + B, foreign, advance(0, Phase.DUAL_WRITE, Phase.BACKFILL, 1));
    }

    @Test
    public void staleOwnerAfterTakeoverIsRejected()
    {
        // the claim moved on (another step): the proposer that read step 0 cannot commit
        ClusterMetadata md = execute(keyIn(Phase.DUAL_WRITE), new AdvanceAction(A, N1, 0, "WAIT", ImmutableMap.of(), NOW + 5, LEASE).wrap());
        assertEquals(1, ledger(md).claim(A).step);
        assertRejected("step 1, not 0", md, advance(0, Phase.DUAL_WRITE, Phase.BACKFILL, 1));
        assertEquals(Phase.DUAL_WRITE, key(md, K1).phase);
        assertEquals(Phase.BACKFILL, key(execute(md, advance(1, Phase.DUAL_WRITE, Phase.BACKFILL, 1)), K1).phase);
    }

    // ---- ReportBackfill ----

    @Test
    public void reportBackfill()
    {
        ClusterMetadata md = keyIn(Phase.BACKFILL);
        ClusterMetadata one = execute(md, report(md, LOGICAL, K1, 1, N2));
        assertEquals(ImmutableSet.of(N2), key(one, K1).backfillDone);
        assertEquals(6, registry(one).layout(LOGICAL).version);
        assertEquals("needs no claim and does not touch it", 0, ledger(one).claim(A).step);
        assertSame(md.extensions.get(ClusterLedgerValue.KEY), one.extensions.get(ClusterLedgerValue.KEY));
        // idempotent: nothing changes, the registry value is the same object
        ClusterMetadata twice = execute(one, report(one, LOGICAL, K1, 1, N2));
        assertSame(one.extensions.get(LayoutRegistryValue.KEY), twice.extensions.get(LayoutRegistryValue.KEY));
        assertEquals(6, registry(twice).layout(LOGICAL).version);
        ClusterMetadata both = execute(twice, report(twice, LOGICAL, K1, 1, N1));
        assertEquals(ImmutableSet.of(N1, N2), key(both, K1).backfillDone);
        // CLEANUP takes reports too
        assertEquals(ImmutableSet.of(N1), key(execute(keyIn(Phase.CLEANUP), report(keyIn(Phase.CLEANUP), LOGICAL, K1, 1, N1)), K1).backfillDone);
    }

    @Test
    public void reportBackfillRejections()
    {
        for (Phase phase : Phase.values())
        {
            if (phase == Phase.BACKFILL || phase == Phase.CLEANUP)
                continue;
            assertRejected("takes no backfill reports", keyIn(phase), report(keyIn(phase), LOGICAL, K1, 1, N1));
        }
        ClusterMetadata md = keyIn(Phase.BACKFILL, 2, ImmutableSet.of());
        assertRejected("generation 2, not 1", md, report(md, LOGICAL, K1, 1, N1));
        assertRejected("generation 2, not 3", md, report(md, LOGICAL, K1, 3, N1));
        assertRejected("key is not registered", md, report(md, LOGICAL, K2, 2, N1));
        assertRejected("has no layout", md, report(md, TableId.fromUUID(new UUID(5, 5)), K1, 2, N1));
        assertRejected("has no layout", empty(), report(empty(), LOGICAL, K1, 1, N1));
    }

    @Test
    public void reportOfAnAbandonedAttemptIsRejected()
    {
        // attempt 1 of K1 reached BACKFILL at generation 1; a replica finished and is about to report
        ClusterMetadata first = keyIn(Phase.BACKFILL);
        ReportBackfill late = report(first, LOGICAL, K1, 1, N2);
        // the attempt is aborted and K1 is registered again: a new incarnation, again generation 1, again BACKFILL
        Epoch e = Epoch.create(first.epoch.getEpoch() + 10);
        KeyState second = new KeyState(Phase.BACKFILL, e, NOW, 2, 1, e, ImmutableSet.of(), A);
        TableLayout layout = new TableLayout(header(), 3, ImmutableMap.of(K1, second), 9);
        ClusterMetadata md = first.transformer().with(LayoutRegistryValue.KEY, new LayoutRegistryValue(
            new LayoutRegistry(LayoutRegistry.FORMAT_VERSION, ImmutableMap.of(LOGICAL, layout)))).build().metadata;
        assertRejected("started at epoch " + e.getEpoch(), md, late);
        assertTrue(key(md, K1).backfillDone.isEmpty());
        // a generation restart within one attempt is told apart by the number already; the epoch agrees
        assertEquals(ImmutableSet.of(N2), key(execute(md, report(md, LOGICAL, K1, 1, N2)), K1).backfillDone);
    }

    private static AdvanceLayout advanceAt(int step, Phase from, Phase to, int generation, long nowMillis)
    {
        return new AdvanceLayout(A, N1, step, LOGICAL, K1, from, to, generation, nowMillis, LEASE);
    }

    /** ADR-031: the time dual write started is set by ANNOUNCED -> DUAL_WRITE and by nothing else. */
    @Test
    public void dualWriteStartIsSetOnceAndKeptByEveryTransition() throws IOException
    {
        long t = NOW + 1000;
        long started = t + 1;
        int step = 0;
        ClusterMetadata md = execute(start(), register(K1, step++));
        assertEquals(0, key(md, K1).dualWriteSinceMillis);
        assertEquals(Long.MIN_VALUE, key(md, K1).backfillLimitMillis(864000));
        md = execute(md, advanceAt(step++, Phase.ANNOUNCED, Phase.DUAL_WRITE, 1, started));
        assertEquals(started, key(md, K1).dualWriteSinceMillis);
        assertEquals(started + 432_000_000L, key(md, K1).backfillLimitMillis(864000));
        Object[][] forward = { { Phase.DUAL_WRITE, Phase.BACKFILL, 1 }, { Phase.BACKFILL, Phase.BACKFILL, 2 }, { Phase.BACKFILL, Phase.VERIFY, 2 },
                               { Phase.VERIFY, Phase.BACKFILL, 3 }, { Phase.BACKFILL, Phase.VERIFY, 3 }, { Phase.VERIFY, Phase.CUTOVER, 3 } };
        long later = started;
        for (Object[] move : forward)
        {
            later += 10_000;
            md = execute(md, advanceAt(step++, (Phase) move[0], (Phase) move[1], (int) move[2], later));
            assertEquals(move[0] + " -> " + move[1], started, key(md, K1).dualWriteSinceMillis);
            assertEquals(later, key(md, K1).phaseSinceMillis);
        }
        // the abort path from here
        ClusterMetadata abort = execute(md, advanceAt(step, Phase.CUTOVER, Phase.ROLLBACK_DUAL, 3, later + 1));
        assertEquals(started, key(abort, K1).dualWriteSinceMillis);
        abort = execute(abort, advanceAt(step + 1, Phase.ROLLBACK_DUAL, Phase.ABORTING, 3, later + 2));
        assertEquals(started, key(abort, K1).dualWriteSinceMillis);
        // and the forward path to the end
        Object[][] commit = { { Phase.CUTOVER, Phase.NEW_ONLY, 3 }, { Phase.NEW_ONLY, Phase.CLEANUP, 3 }, { Phase.CLEANUP, Phase.CLEANUP, 4 } };
        for (Object[] move : commit)
        {
            later += 10_000;
            md = execute(md, advanceAt(step++, (Phase) move[0], (Phase) move[1], (int) move[2], later));
            assertEquals(move[0] + " -> " + move[1], started, key(md, K1).dualWriteSinceMillis);
        }
        md = execute(md, report(md, LOGICAL, K1, 4, N2));
        assertEquals(started, key(md, K1).dualWriteSinceMillis);
        assertEquals(started, key(roundTrip(md), K1).dualWriteSinceMillis);
        assertEquals(registry(md), registry(roundTrip(md)));
        md = execute(md, new FinishKey(A, N1, step, LOGICAL, K1, later + 5, LEASE));
        assertEquals(Phase.STEADY, key(md, K1).phase);
        assertEquals(started, key(md, K1).dualWriteSinceMillis);
        // it is part of the state
        KeyState k = key(md, K1);
        KeyState other = new KeyState(k.phase, k.phaseEpoch, k.phaseSinceMillis, k.incarnation, k.generation, k.generationEpoch, k.backfillDone,
                                      k.actionId, started + 1);
        assertFalse(k.equals(other));
        assertTrue(k.toString(), k.toString().contains("dualWriteSince=" + started));
    }

    /** A key enters CLEANUP with the generation of its backfill; reports are due again only for a generation started there. */
    @Test
    public void cleanupTakesReportsForTheReconcileGenerationOnly()
    {
        ClusterMetadata md = keyIn(Phase.NEW_ONLY, 2, ImmutableSet.of(N1, N2));
        md = execute(md, advance(0, Phase.NEW_ONLY, Phase.CLEANUP, 2));
        assertFalse(key(md, K1).backfillDue());
        assertEquals("the reports of the backfill are still there", ImmutableSet.of(N1, N2), key(md, K1).backfillDone);
        assertRejected("the reconcile generation has not started", md, new ReportBackfill(LOGICAL, K1, 2, key(md, K1).generationEpoch, new NodeId(3)));
        md = execute(md, advance(1, Phase.CLEANUP, Phase.CLEANUP, 3));
        assertTrue(key(md, K1).backfillDue());
        assertTrue(key(md, K1).backfillDone.isEmpty());
        assertEquals(ImmutableSet.of(N2), key(execute(md, report(md, LOGICAL, K1, 3, N2)), K1).backfillDone);
        assertTrue(key(keyIn(Phase.BACKFILL), K1).backfillDue());
        for (Phase phase : Phase.values())
            if (phase != Phase.BACKFILL && phase != Phase.CLEANUP)
                assertFalse(phase.name(), key(keyIn(phase), K1).backfillDue());
    }

    @Test
    public void backfillDoneIsBoundedTo32Nodes()
    {
        List<NodeId> nodes = new ArrayList<>();
        for (int i = 1; i <= LayoutRegistry.MAX_BACKFILL_NODES; i++)
            nodes.add(new NodeId(i));
        ClusterMetadata md = keyIn(Phase.BACKFILL, 1, ImmutableSet.copyOf(nodes.subList(0, 31)));
        md = execute(md, report(md, LOGICAL, K1, 1, nodes.get(31)));
        assertEquals(32, key(md, K1).backfillDone.size());
        assertRejected("already has 32 nodes", md, report(md, LOGICAL, K1, 1, new NodeId(33)));
        // a node that is already in is still idempotent at the bound
        assertSame(md.extensions.get(LayoutRegistryValue.KEY),
                   execute(md, report(md, LOGICAL, K1, 1, N1)).extensions.get(LayoutRegistryValue.KEY));
        try
        {
            List<NodeId> tooMany = new ArrayList<>(nodes);
            tooMany.add(new NodeId(33));
            new KeyState(Phase.BACKFILL, Epoch.create(1), 0, 1, 1, Epoch.create(1), tooMany, A);
            fail();
        }
        catch (IllegalArgumentException expected)
        {
            // bounded
        }
    }

    // ---- RemoveKey, FinishKey, RemoveOrphanLayout ----

    @Test
    public void removeKeyOnlyInAbortingAndTheLayoutGoesWithItsLastKey()
    {
        for (Phase phase : Phase.values())
            if (phase != Phase.ABORTING)
                assertRejected("not ABORTING", keyIn(phase), new RemoveKey(A, N1, 0, LOGICAL, K1, NOW, LEASE));

        ClusterMetadata md = keyIn(Phase.ABORTING);
        ClusterMetadata next = execute(md, new RemoveKey(A, N1, 0, LOGICAL, K1, NOW + 4, LEASE));
        assertNull("no keys left, no layout", registry(next).layout(LOGICAL));
        assertTrue(registry(next).byLogical.isEmpty());
        assertEquals(1, ledger(next).claim(A).step);
        assertEquals("KEY_REMOVED", ledger(next).claim(A).phase);

        // with two keys the layout stays, at the next version, and keeps the incarnation counter
        ClusterMetadata two = execute(execute(start(), register(K1, 0)), register(K2, 1));
        two = execute(two, new AdvanceLayout(A, N1, 2, LOGICAL, K1, Phase.ANNOUNCED, Phase.ABORTING, 1, NOW, LEASE));
        long version = registry(two).layout(LOGICAL).version;
        two = execute(two, new RemoveKey(A, N1, 3, LOGICAL, K1, NOW, LEASE));
        TableLayout layout = registry(two).layout(LOGICAL);
        assertNull(layout.keyState(K1));
        assertEquals(Phase.ANNOUNCED, layout.keyState(K2).phase);
        assertEquals(version + 1, layout.version);
        assertEquals(3, layout.nextIncarnation);
    }

    @Test
    public void removeKeyAndFinishKeyRejections()
    {
        ClusterMetadata aborting = keyIn(Phase.ABORTING);
        assertRejected("owned by node 1", aborting, new RemoveKey(A, N2, 0, LOGICAL, K1, NOW, LEASE));
        assertRejected("step 0, not 2", aborting, new RemoveKey(A, N1, 2, LOGICAL, K1, NOW, LEASE));
        assertRejected("key is not registered", aborting, new RemoveKey(A, N1, 0, LOGICAL, K2, NOW, LEASE));
        assertRejected("does not hold lane", empty(), new RemoveKey(A, N1, 0, LOGICAL, K1, NOW, LEASE));

        for (Phase phase : Phase.values())
            if (phase != Phase.CLEANUP)
                assertRejected("not CLEANUP", keyIn(phase), new FinishKey(A, N1, 0, LOGICAL, K1, NOW, LEASE));
        ClusterMetadata cleanup = keyIn(Phase.CLEANUP);
        assertRejected("owned by node 1", cleanup, new FinishKey(A, N2, 0, LOGICAL, K1, NOW, LEASE));
        assertRejected("step 0, not 2", cleanup, new FinishKey(A, N1, 2, LOGICAL, K1, NOW, LEASE));
        assertRejected("key is not registered", cleanup, new FinishKey(A, N1, 0, LOGICAL, K2, NOW, LEASE));
        ClusterMetadata done = execute(cleanup, new FinishKey(A, N1, 0, LOGICAL, K1, NOW + 9, LEASE));
        assertEquals(Phase.STEADY, key(done, K1).phase);
        assertNull(key(done, K1).actionId);
        assertEquals(6, registry(done).layout(LOGICAL).version);
    }

    @Test
    public void removeOrphanLayoutOnlyWhenTheLogicalTableIsGone()
    {
        ClusterMetadata md = keyIn(Phase.DUAL_WRITE);
        assertRejected("is not an orphan", md, new RemoveOrphanLayout(LOGICAL));
        // the table is dropped by a client: the schema no longer has it
        ClusterMetadata dropped = md.transformer().with(new DistributedSchema(Keyspaces.of(KeyspaceMetadata.create("ks", KeyspaceParams.simple(1)))))
                                    .build().metadata;
        assertRejected("has no layout", dropped, new RemoveOrphanLayout(TableId.fromUUID(new UUID(8, 8))));
        ClusterMetadata next = execute(dropped, new RemoveOrphanLayout(LOGICAL));
        assertTrue(registry(next).byLogical.isEmpty());
        assertEquals("no claim involved", ledger(dropped), ledger(next));
        assertRejected("has no layout", next, new RemoveOrphanLayout(LOGICAL));
    }

    // ---- value, determinism, serialization ----

    @Test
    public void valueFromPrevIsNeverMutated()
    {
        ClusterMetadata prev = keyIn(Phase.BACKFILL);
        LayoutTransformation[] changes = { advance(0, Phase.BACKFILL, Phase.VERIFY, 1),
                                           report(prev, LOGICAL, K1, 1, N1),
                                           advance(0, Phase.BACKFILL, Phase.ABORTING, 1) };
        for (LayoutTransformation t : changes)
        {
            ExtensionValue<?> before = prev.extensions.get(LayoutRegistryValue.KEY);
            LayoutRegistry registryBefore = registry(prev);
            Epoch modifiedBefore = before.lastModified();
            String printed = before.toString();

            ClusterMetadata next = execute(prev, t);

            assertSame(t.toString(), before, prev.extensions.get(LayoutRegistryValue.KEY));
            assertSame(registryBefore, ((LayoutRegistryValue) before).getValue());
            assertEquals(modifiedBefore, before.lastModified());
            assertEquals(printed, before.toString());
            ExtensionValue<?> after = next.extensions.get(LayoutRegistryValue.KEY);
            assertNotSame(t.toString(), before, after);
            assertEquals(next.epoch, after.lastModified());
            assertTrue(next.epoch.isAfter(modifiedBefore));
        }
        // the layout and key objects of the previous registry were not edited either
        assertEquals(Phase.BACKFILL, key(prev, K1).phase);
        assertTrue(key(prev, K1).backfillDone.isEmpty());
        assertEquals(5, registry(prev).layout(LOGICAL).version);
    }

    @Test
    public void registryOfAValueCannotBeReplaced()
    {
        LayoutRegistryValue value = new LayoutRegistryValue(LayoutRegistry.EMPTY);
        try
        {
            value.setValue(LayoutRegistry.EMPTY);
            fail("setValue on an initialised value must fail");
        }
        catch (IllegalStateException expected)
        {
            // immutable
        }
    }

    @Test
    public void claimAndRegistryChangeTogetherOrNotAtAll()
    {
        ClusterMetadata prev = keyIn(Phase.DUAL_WRITE);
        // accepted: one epoch, both extensions
        ClusterMetadata next = execute(prev, advance(0, Phase.DUAL_WRITE, Phase.BACKFILL, 1));
        assertEquals(1, ledger(next).claim(A).step);
        assertEquals(Phase.BACKFILL, key(next, K1).phase);
        assertEquals(next.epoch, next.extensions.get(ClusterLedgerValue.KEY).lastModified());
        assertEquals(next.epoch, next.extensions.get(LayoutRegistryValue.KEY).lastModified());
        // a rejection by the registry (generation) leaves no commit, so the claim cannot advance alone
        Transformation.Result bad = advance(0, Phase.DUAL_WRITE, Phase.BACKFILL, 7).wrap().execute(prev);
        assertTrue(bad.isRejected());
        assertEquals(0, ledger(prev).claim(A).step);
        // a rejection by the claim (step) leaves no commit, so the registry cannot change alone
        assertTrue(advance(4, Phase.DUAL_WRITE, Phase.BACKFILL, 1).wrap().execute(prev).isRejected());
        assertEquals(Phase.DUAL_WRITE, key(prev, K1).phase);
        // replaying the claim half alone is not possible: the same advance applied to its own result is rejected
        assertTrue(advance(0, Phase.DUAL_WRITE, Phase.BACKFILL, 1).wrap().execute(next).isRejected());
    }

    @Test
    public void executeIsDeterministic() throws IOException
    {
        ClusterMetadata prev = keyIn(Phase.BACKFILL);
        LayoutTransformation[] all = { advance(0, Phase.BACKFILL, Phase.VERIFY, 1), report(prev, LOGICAL, K1, 1, N2),
                                       advance(0, Phase.BACKFILL, Phase.BACKFILL, 2) };
        for (LayoutTransformation t : all)
        {
            ClusterMetadata one = execute(prev, t);
            ClusterMetadata two = roundTrip(t).wrap().execute(roundTrip(prev)).success().metadata;
            assertEquals(registry(one), registry(two));
            assertEquals(ledger(one), ledger(two));
            assertEquals(bytes(one), bytes(two));
        }
    }

    /** No clock, configuration or local state on any path that {@code execute} or a serializer can reach. */
    @Test
    public void replayedCodeDoesNotReadClocksOrConfiguration() throws IOException
    {
        String[] sources = { "Phase", "KeyState", "TableLayout", "LayoutHeader", "LayoutRegistry", "LayoutRegistryValue", "LayoutTransformation", "ClaimStep", "LayoutValueBase", "Eligibility", "PhysicalTables",
                             "RegisterKey", "AdvanceLayout", "ReportBackfill", "RemoveKey", "FinishKey", "RemoveOrphanLayout" };
        String[] forbidden = { "Clock", "currentTimeMillis", "nanoTime", "DatabaseDescriptor", "AutonomicService", "AutonomicConfig",
                               "FBUtilities", "Random", "randomUUID", "TimeUUID", "System.", "ClusterMetadata.current", "ClusterMetadataService",
                               "StorageService", "Schema.instance", "Instant", "new Date" };
        for (String source : sources)
        {
            String text = new String(Files.readAllBytes(Paths.get("src/java/org/apache/cassandra/autonomic/layout", source + ".java")));
            String code = text.replaceAll("(?s)/\\*.*?\\*/", "").replaceAll("//.*", "");
            for (String word : forbidden)
                assertFalse(source + ".java must not use " + word, code.contains(word));
            for (String line : code.split("\n"))
                if (line.startsWith("import org.apache.cassandra."))
                    assertTrue(source + ".java: unexpected dependency " + line,
                               line.matches("import org\\.apache\\.cassandra\\.(tcm|io\\.util|db\\.TypeSizes|db\\.marshal\\.Int32Type|exceptions\\.ExceptionCode"
                                            + "|schema\\.(TableId|TableMetadata|ColumnMetadata|KeyspaceMetadata|SchemaConstants)|cql3\\.ColumnIdentifier"
                                            + "|service\\.consensus\\.(TransactionalMode|migration\\.TransactionalMigrationFromMode)|utils\\.ByteBufferUtil|autonomic\\.(coord|layout))[.;].*"));
        }
    }

    @Test
    public void registryRoundTrips() throws IOException
    {
        ClusterMetadata md = execute(start(), register(K1, 0));
        md = execute(md, register(K2, 1));
        md = execute(md, advance(2, Phase.ANNOUNCED, Phase.DUAL_WRITE, 1));
        md = execute(md, advance(3, Phase.DUAL_WRITE, Phase.BACKFILL, 1));
        md = execute(md, report(md, LOGICAL, K1, 1, N2));
        md = execute(md, report(md, LOGICAL, K1, 1, N1));

        ClusterMetadata read = roundTrip(md);
        assertEquals(registry(md), registry(read));
        assertEquals(md.extensions.get(LayoutRegistryValue.KEY), read.extensions.get(LayoutRegistryValue.KEY));
        assertEquals(md.extensions.get(LayoutRegistryValue.KEY).lastModified(), read.extensions.get(LayoutRegistryValue.KEY).lastModified());
        assertEquals(ledger(md), ledger(read));
        assertEquals(bytes(md).remaining(), ClusterMetadata.serializer.serializedSize(md, VERSION));
        assertEquals(bytes(md), bytes(read));
        assertEquals(ImmutableSet.of(N1, N2), key(read, K1).backfillDone);
        assertEquals(Phase.ANNOUNCED, key(read, K2).phase);

        // the empty registry, a registry in every phase, and the snapshot path that caps last-modified epochs
        ClusterMetadata on = md.transformer().with(LayoutRegistryValue.KEY, new LayoutRegistryValue(LayoutRegistry.EMPTY)).build().metadata;
        assertEquals(LayoutRegistry.EMPTY, registry(roundTrip(on)));
        for (Phase phase : Phase.values())
            assertEquals(registry(keyIn(phase)), registry(roundTrip(keyIn(phase))));
        Epoch earlier = Epoch.create(md.epoch.getEpoch() - 1);
        ClusterMetadata forced = md.forceEpoch(earlier);
        assertEquals(registry(md), registry(forced));
        assertTrue(forced.extensions.get(LayoutRegistryValue.KEY) instanceof LayoutRegistryValue);
        assertEquals(earlier, forced.extensions.get(LayoutRegistryValue.KEY).lastModified());
    }

    @Test
    public void unknownRegistryFormatFailsLoudly() throws IOException
    {
        DataOutputBuffer out = new DataOutputBuffer();
        new LayoutRegistryValue(LayoutRegistry.EMPTY).serialize(out, VERSION);
        ByteBuffer bytes = out.buffer();
        assertEquals(LayoutRegistry.FORMAT_VERSION, bytes.get(0));
        bytes.put(0, (byte) (LayoutRegistry.FORMAT_VERSION + 1));
        try
        {
            new LayoutRegistryValue().deserialize(new DataInputBuffer(bytes, false), VERSION);
            fail("an unknown format must not be read");
        }
        catch (IOException expected)
        {
            assertTrue(expected.getMessage().contains("format"));
        }
        try
        {
            new LayoutRegistry(2, ImmutableMap.of());
            DataOutputBuffer unwritable = new DataOutputBuffer();
            new LayoutRegistryValue(new LayoutRegistry(2, ImmutableMap.of())).serialize(unwritable, VERSION);
            fail("an unknown format must not be written");
        }
        catch (IOException expected)
        {
            assertTrue(expected.getMessage().contains("format"));
        }
    }

    @Test
    public void transformationRoundTrips() throws IOException
    {
        LayoutTransformation[] all = { new RegisterKey(A, N1, 3, header(), K1, NOW, LEASE),
                                       new RegisterKey(B, N2, 0, header(64, 1), ByteBuffer.allocate(1024), NOW, LEASE),
                                       advance(4, Phase.VERIFY, Phase.BACKFILL, 2),
                                       advance(0, Phase.CLEANUP, Phase.CLEANUP, 9),
                                       new ReportBackfill(LOGICAL, K1, 3, Epoch.create(41), N2),
                                       new RemoveKey(A, N1, 7, LOGICAL, K2, NOW, LEASE),
                                       new FinishKey(A, N1, 8, LOGICAL, K2, NOW, LEASE),
                                       new RemoveOrphanLayout(LOGICAL) };
        for (LayoutTransformation t : all)
        {
            assertEquals(t, roundTrip(t));
            assertEquals(t.hashCode(), roundTrip(t).hashCode());
            assertEquals(t.toString(), roundTrip(t).toString());

            CustomTransformation wrapped = t.wrap();
            DataOutputBuffer out = new DataOutputBuffer();
            CustomTransformation.serializer.serialize(wrapped, out, VERSION);
            assertEquals(out.getLength(), CustomTransformation.serializer.serializedSize(wrapped, VERSION));
            CustomTransformation read = CustomTransformation.serializer.deserialize(new DataInputBuffer(out.buffer(), false), VERSION);
            assertEquals(wrapped, read);
            assertEquals(t, read.child());
            assertEquals(t.getClass(), read.child().getClass());

            assertEquals(Transformation.Kind.CUSTOM, wrapped.kind());
            Transformation replayed = Transformation.Kind.CUSTOM.fromVersionedBytes(wrapped.kind().toVersionedBytes(wrapped));
            assertEquals(wrapped, replayed);
        }
        assertFalse(all[0].equals(all[1]));
    }

    @Test
    public void unknownTransformationFormatFailsLoudly() throws IOException
    {
        DataOutputBuffer out = new DataOutputBuffer();
        AdvanceLayout.serializer.serialize(advance(0, Phase.DUAL_WRITE, Phase.BACKFILL, 1), out, VERSION);
        ByteBuffer bytes = out.buffer();
        bytes.put(0, (byte) 2);
        try
        {
            AdvanceLayout.serializer.deserialize(new DataInputBuffer(bytes, false), VERSION);
            fail();
        }
        catch (IOException expected)
        {
            assertTrue(expected.getMessage().contains("format 2"));
        }
    }

    @Test
    public void namesAreStableAndRegistered()
    {
        // part of the log format
        assertEquals("org.apache.cassandra.autonomic.layouts", LayoutRegistryValue.KEY.id);
        assertEquals("org.apache.cassandra.autonomic.layout.LayoutRegistryValue", LayoutRegistryValue.class.getName());
        assertEquals("org.apache.cassandra.autonomic.layout.RegisterKey", RegisterKey.NAME);
        assertEquals("org.apache.cassandra.autonomic.layout.AdvanceLayout", AdvanceLayout.NAME);
        assertEquals("org.apache.cassandra.autonomic.layout.ReportBackfill", ReportBackfill.NAME);
        assertEquals("org.apache.cassandra.autonomic.layout.RemoveKey", RemoveKey.NAME);
        assertEquals("org.apache.cassandra.autonomic.layout.FinishKey", FinishKey.NAME);
        assertEquals("org.apache.cassandra.autonomic.layout.RemoveOrphanLayout", RemoveOrphanLayout.NAME);
        assertSame(RegisterKey.serializer, CustomTransformation.getSerializer(RegisterKey.NAME));
        assertSame(AdvanceLayout.serializer, CustomTransformation.getSerializer(AdvanceLayout.NAME));
        assertSame(ReportBackfill.serializer, CustomTransformation.getSerializer(ReportBackfill.NAME));
        assertSame(RemoveKey.serializer, CustomTransformation.getSerializer(RemoveKey.NAME));
        assertSame(FinishKey.serializer, CustomTransformation.getSerializer(FinishKey.NAME));
        assertSame(RemoveOrphanLayout.serializer, CustomTransformation.getSerializer(RemoveOrphanLayout.NAME));
        // phase ids are the serialized form
        int[] ids = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        Phase[] expected = { Phase.ANNOUNCED, Phase.DUAL_WRITE, Phase.BACKFILL, Phase.VERIFY, Phase.CUTOVER, Phase.NEW_ONLY, Phase.CLEANUP,
                             Phase.STEADY, Phase.ROLLBACK_DUAL, Phase.ABORTING };
        for (int i = 0; i < ids.length; i++)
            assertSame(expected[i], Phase.fromId(ids[i]));
        assertNull(Phase.fromId(10));
        assertEquals(0, TableLayout.Transform.HASH_SPLIT.id);
    }

    @Test
    public void phaseClassesAndTransitionsAreAdjacent()
    {
        // design 4.2: every transition moves between identical or adjacent classes C0-C1-C2-C3
        for (Phase from : Phase.values())
            for (Phase to : Phase.values())
                if (Phase.isTransition(from, to))
                    assertTrue(from + " -> " + to, Math.abs(from.coordinatorClass - to.coordinatorClass) <= 1);
        assertEquals(0, Phase.ANNOUNCED.coordinatorClass);
        assertEquals(0, Phase.ABORTING.coordinatorClass);
        assertEquals(2, Phase.CUTOVER.coordinatorClass);
        assertEquals(3, Phase.STEADY.coordinatorClass);
    }

    // ---- helpers ----

    static ByteBuffer bytes(ClusterMetadata metadata) throws IOException
    {
        DataOutputBuffer out = new DataOutputBuffer();
        ClusterMetadata.serializer.serialize(metadata, out, VERSION);
        return out.buffer();
    }

    static ClusterMetadata roundTrip(ClusterMetadata metadata)
    {
        try
        {
            return ClusterMetadata.serializer.deserialize(new DataInputBuffer(bytes(metadata), false), VERSION);
        }
        catch (IOException e)
        {
            throw new AssertionError(e);
        }
    }

    @SuppressWarnings("unchecked")
    static LayoutTransformation roundTrip(LayoutTransformation t) throws IOException
    {
        var serializer = CustomTransformation.getSerializer(t.name());
        DataOutputBuffer out = new DataOutputBuffer();
        serializer.serialize(t, out, VERSION);
        assertEquals(t + " size", out.getLength(), serializer.serializedSize(t, VERSION));
        return (LayoutTransformation) serializer.deserialize(new DataInputBuffer(out.buffer(), false), VERSION);
    }
}
