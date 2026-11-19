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

import java.util.Collections;
import java.util.Date;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.ImmutableSet;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.autonomic.knowledge.AutonomicVirtualTables;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.virtual.VirtualKeyspace;
import org.apache.cassandra.db.virtual.VirtualKeyspaceRegistry;
import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;

/** {@code system_views.autonomic_layouts}: one row per layout and key, read from the metadata of the node. */
public class LayoutsVirtualTableTest extends CQLTester
{
    private static final String KS = "vts_layouts";
    private static final AtomicReference<ClusterMetadata> METADATA = new AtomicReference<>();

    @BeforeClass
    public static void register()
    {
        addVirtualKeyspace();
        VirtualKeyspaceRegistry.instance.register(new VirtualKeyspace(KS, Collections.singletonList(
            AutonomicVirtualTables.layouts(KS, METADATA::get))));
    }

    @Test
    public void theRealTableIsSelectableAndEmptyWithoutALayout() throws Throwable
    {
        assertEmpty(execute("SELECT * FROM system_views.autonomic_layouts"));
        METADATA.set(null);
        assertEmpty(execute("SELECT * FROM " + KS + ".autonomic_layouts"));
        METADATA.set(LayoutTransformationTest.start());
        assertEmpty(execute("SELECT * FROM " + KS + ".autonomic_layouts"));
    }

    @Test
    public void oneRowPerLayoutAndKey() throws Throwable
    {
        ClusterMetadata md = LayoutTransformationTest.keyIn(Phase.BACKFILL, 3, ImmutableSet.of(LayoutTransformationTest.N2, LayoutTransformationTest.N1));
        METADATA.set(md);
        KeyState k = LayoutRegistry.of(md).layout(LayoutTransformationTest.LOGICAL).keyState(LayoutTransformationTest.K1);
        assertRows(execute("SELECT keyspace_name, table_name, partition_key, layout_id, physical_table, transform, buckets, layout_version, phase, epoch,"
                           + " phase_since, incarnation, generation, generation_epoch, backfill_done, action_id, orphan FROM " + KS + ".autonomic_layouts"),
                   row("ks", "t", ByteBufferUtil.bytesToHex(LayoutTransformationTest.K1), LayoutTransformationTest.LAYOUT_ID, "t_al_00000001",
                       "HASH_SPLIT", 8, 5L, "BACKFILL", k.phaseEpoch.getEpoch(), new Date(LayoutTransformationTest.NOW), 1, 3, k.generationEpoch.getEpoch(),
                       ImmutableSet.of(LayoutTransformationTest.N1.toUUID(), LayoutTransformationTest.N2.toUUID()), LayoutTransformationTest.A, false));

        // when dual write started: empty before DUAL_WRITE, the proposer's time afterwards
        assertRows(execute("SELECT dual_write_since FROM " + KS + ".autonomic_layouts"), row((Object) null));
        ClusterMetadata dual = LayoutTransformationTest.execute(LayoutTransformationTest.execute(LayoutTransformationTest.start(), LayoutTransformationTest.register(LayoutTransformationTest.K1, 0)),
                                                               LayoutTransformationTest.advance(1, Phase.ANNOUNCED, Phase.DUAL_WRITE, 1));
        METADATA.set(dual);
        assertRows(execute("SELECT phase, dual_write_since FROM " + KS + ".autonomic_layouts"), row("DUAL_WRITE", new Date(LayoutTransformationTest.NOW + 2)));

        // two keys: two rows, in key order
        ClusterMetadata two = LayoutTransformationTest.execute(LayoutTransformationTest.execute(LayoutTransformationTest.start(), LayoutTransformationTest.register(LayoutTransformationTest.K2, 0)),
                                                              LayoutTransformationTest.register(LayoutTransformationTest.K1, 1));
        METADATA.set(two);
        assertRows(execute("SELECT partition_key, phase, incarnation, backfill_done, orphan FROM " + KS + ".autonomic_layouts"),
                   row(ByteBufferUtil.bytesToHex(LayoutTransformationTest.K1), "ANNOUNCED", 2, ImmutableSet.of(), false),
                   row(ByteBufferUtil.bytesToHex(LayoutTransformationTest.K2), "ANNOUNCED", 1, ImmutableSet.of(), false));
    }

    @Test
    public void baseStateFrozenSinceAndReverseModeAreShown() throws Throwable
    {
        // a legacy key: the base state decodes from the phase, nothing is frozen or reversed
        METADATA.set(LayoutTransformationTest.keyIn(Phase.STEADY));
        assertRows(execute("SELECT phase, base_state, frozen_since, reverse_mode FROM " + KS + ".autonomic_layouts"),
                   row("STEADY", "TOMBSTONED", null, null));
        METADATA.set(LayoutTransformationTest.keyIn(Phase.BACKFILL));
        assertRows(execute("SELECT phase, base_state, frozen_since, reverse_mode FROM " + KS + ".autonomic_layouts"),
                   row("BACKFILL", "RESIDUE", null, null));

        // a key in a reverse phase of format 2
        ClusterMetadata md = LayoutTransformationTest.start();
        org.apache.cassandra.tcm.Epoch e = md.epoch.nextEpoch();
        KeyState state = new KeyState(Phase.REVERSE_BACKFILL, e, 1000, 1, 2, e, ImmutableSet.of(), LayoutTransformationTest.A, 400, ImmutableSet.of(),
                                      KeyState.BaseState.TOMBSTONED, 9000, 77, KeyState.ReverseMode.SLOW, 500);
        TableLayout layout = new TableLayout(LayoutTransformationTest.header(), 2, com.google.common.collect.ImmutableMap.of(LayoutTransformationTest.K1, state), 5);
        METADATA.set(md.transformer().with(LayoutRegistryValue.KEY, new LayoutRegistryValue(
            new LayoutRegistry(LayoutRegistry.FORMAT_VERSION_2, com.google.common.collect.ImmutableMap.of(LayoutTransformationTest.LOGICAL, layout)))).build().metadata);
        assertRows(execute("SELECT phase, base_state, frozen_since, reverse_mode FROM " + KS + ".autonomic_layouts"),
                   row("REVERSE_BACKFILL", "TOMBSTONED", new Date(9000), "SLOW"));
    }

    @Test
    public void orphanWhenTheLogicalTableIsGone() throws Throwable
    {
        ClusterMetadata md = LayoutTransformationTest.keyIn(Phase.STEADY);
        ClusterMetadata dropped = md.transformer().with(new DistributedSchema(Keyspaces.of(KeyspaceMetadata.create("ks", KeyspaceParams.simple(1)))))
                                    .build().metadata;
        METADATA.set(dropped);
        assertRows(execute("SELECT phase, action_id, orphan FROM " + KS + ".autonomic_layouts"), row("STEADY", null, true));
        assertEquals(1, execute("SELECT * FROM " + KS + ".autonomic_layouts WHERE keyspace_name = 'ks' AND table_name = 't'").size());
        assertEquals(0, execute("SELECT * FROM " + KS + ".autonomic_layouts WHERE keyspace_name = 'other'").size());
    }
}
