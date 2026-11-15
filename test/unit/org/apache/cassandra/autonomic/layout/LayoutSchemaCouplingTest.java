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

import java.util.List;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.DistributedSchema;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.Indexes;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Keyspaces;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableParams;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.schema.TriggerMetadata;
import org.apache.cassandra.schema.Triggers;
import org.apache.cassandra.tcm.ClusterMetadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/** T6.10: every row of the design 4.8 table against hand-built keyspaces, and purity. */
public class LayoutSchemaCouplingTest
{
    private static final TableId OTHER = TableId.fromUUID(new UUID(20, 20));
    private static final String ADAPTED = "ks.t has an adapted layout; ";

    @BeforeClass
    public static void init()
    {
        DatabaseDescriptor.toolInitialization();
    }

    private static TableMetadata.Builder logicalBuilder()
    {
        return TableMetadata.builder("ks", "t").id(LayoutTransformationTest.LOGICAL)
                            .addPartitionKeyColumn("p", Int32Type.instance)
                            .addClusteringColumn("c1", Int32Type.instance)
                            .addClusteringColumn("c2", UTF8Type.instance)
                            .addRegularColumn("v", UTF8Type.instance);
    }

    private static TableMetadata logical()
    {
        return logicalBuilder().build();
    }

    private static TableMetadata physical()
    {
        return LayoutTransformationTest.physicalBuilder().addRegularColumn("v", UTF8Type.instance).build();
    }

    private static TableMetadata other()
    {
        return TableMetadata.builder("ks", "other").id(OTHER)
                            .addPartitionKeyColumn("p", Int32Type.instance)
                            .addClusteringColumn("c", Int32Type.instance)
                            .build();
    }

    private static Keyspaces keyspaces(KeyspaceParams params, TableMetadata... tables)
    {
        return Keyspaces.of(KeyspaceMetadata.create("ks", params, Tables.of(tables)));
    }

    private static Keyspaces keyspaces(TableMetadata... tables)
    {
        return keyspaces(KeyspaceParams.simple(1), tables);
    }

    /** Metadata with a registered layout (K1 in BACKFILL) over the given schema. */
    private static ClusterMetadata prev(Keyspaces schema)
    {
        ClusterMetadata md = LayoutTransformationTest.keyIn(Phase.BACKFILL);
        return md.transformer().with(new DistributedSchema(schema)).build().metadata;
    }

    private static ClusterMetadata prev()
    {
        return prev(keyspaces(logical(), physical(), other()));
    }

    private static void assertInvalid(String expected, ClusterMetadata prev, Keyspaces next)
    {
        try
        {
            LayoutSchemaCoupling.apply(prev, next);
            fail("expected a rejection containing '" + expected + "'");
        }
        catch (InvalidRequestException e)
        {
            assertTrue("'" + e.getMessage() + "' should contain '" + expected + "'", e.getMessage().contains(expected));
        }
    }

    private static TableMetadata table(Keyspaces ks, String name)
    {
        return ks.getNullable("ks").tables.getNullable(name);
    }

    // ---- drops ----

    @Test
    public void droppingTheLogicalTableDropsTheOverflowTable()
    {
        Keyspaces out = LayoutSchemaCoupling.apply(prev(), keyspaces(physical(), other()));
        assertNull(table(out, "t"));
        assertNull(table(out, "t_al_00000001"));
        assertNotNull("unrelated tables stay", table(out, "other"));
    }

    @Test
    public void droppingTheKeyspaceIsLeftAlone()
    {
        assertEquals(Keyspaces.none(), LayoutSchemaCoupling.apply(prev(), Keyspaces.none()));
    }

    @Test
    public void droppingTheLogicalTableAfterTheOverflowTableIsGone()
    {
        Keyspaces out = LayoutSchemaCoupling.apply(prev(), keyspaces(other()));
        assertNull(table(out, "t"));
        assertNull(table(out, "t_al_00000001"));
    }

    // ---- the overflow table is managed ----

    @Test
    public void overflowTableCannotBeAlteredOrDroppedOnItsOwn()
    {
        TableMetadata altered = physical().withSwapped(TableParams.builder().gcGraceSeconds(90_000).build());
        assertInvalid("ks.t_al_00000001 is managed", prev(), keyspaces(logical(), altered, other()));
        assertInvalid("ks.t_al_00000001 is managed", prev(), keyspaces(logical(), other()));
        TableMetadata extraColumn = LayoutTransformationTest.physicalBuilder().addRegularColumn("v", UTF8Type.instance)
                                                            .addRegularColumn("w", UTF8Type.instance).build();
        assertInvalid("is managed", prev(), keyspaces(logical(), extraColumn, other()));
    }

    // ---- logical table: columns ----

    @Test
    public void columnChangesAreRejectedWithTheDocumentedMessage()
    {
        assertInvalid(ADAPTED, prev(), keyspaces(logicalBuilder().addRegularColumn("w", UTF8Type.instance).build(), physical(), other()));

        TableMetadata dropped = TableMetadata.builder("ks", "t").id(LayoutTransformationTest.LOGICAL)
                                             .addPartitionKeyColumn("p", Int32Type.instance)
                                             .addClusteringColumn("c1", Int32Type.instance)
                                             .addClusteringColumn("c2", UTF8Type.instance).build();
        assertInvalid(ADAPTED, prev(), keyspaces(dropped, physical(), other()));

        TableMetadata renamed = TableMetadata.builder("ks", "t").id(LayoutTransformationTest.LOGICAL)
                                             .addPartitionKeyColumn("p", Int32Type.instance)
                                             .addClusteringColumn("c1", Int32Type.instance)
                                             .addClusteringColumn("c2", UTF8Type.instance)
                                             .addRegularColumn("v2", UTF8Type.instance).build();
        assertInvalid(ADAPTED, prev(), keyspaces(renamed, physical(), other()));

        TableMetadata retyped = TableMetadata.builder("ks", "t").id(LayoutTransformationTest.LOGICAL)
                                             .addPartitionKeyColumn("p", Int32Type.instance)
                                             .addClusteringColumn("c1", Int32Type.instance)
                                             .addClusteringColumn("c2", UTF8Type.instance)
                                             .addRegularColumn("v", BytesType.instance).build();
        assertInvalid(ADAPTED, prev(), keyspaces(retyped, physical(), other()));
    }

    // ---- logical table: would become ineligible ----

    @Test
    public void changesThatMakeTheTableIneligibleAreRejected()
    {
        assertInvalid("STATIC_COLUMN", prev(), keyspaces(logicalBuilder().addStaticColumn("s", Int32Type.instance).build(), physical(), other()));
        assertInvalid("CDC", prev(), keyspaces(logicalBuilder().params(TableParams.builder().cdc(true).build()).build(), physical(), other()));
        assertInvalid("TRIGGER", prev(), keyspaces(logicalBuilder().triggers(Triggers.of(TriggerMetadata.create("trg", "org.example.Trigger"))).build(),
                                                  physical(), other()));
        IndexMetadata index = IndexMetadata.fromSchemaMetadata("i", IndexMetadata.Kind.KEYS, ImmutableMap.of("target", "v"));
        assertInvalid("INDEX", prev(), keyspaces(logicalBuilder().indexes(Indexes.of(index)).build(), physical(), other()));
        assertInvalid(ADAPTED, prev(), keyspaces(logicalBuilder().params(TableParams.builder().gcGraceSeconds(100).build()).build(), physical(), other()));
    }

    // ---- logical table: parameters ----

    @Test
    public void gcGraceSecondsIsMirroredToTheOverflowTable()
    {
        TableMetadata changed = logicalBuilder().params(TableParams.builder().gcGraceSeconds(50_000).build()).build();
        Keyspaces out = LayoutSchemaCoupling.apply(prev(), keyspaces(changed, physical(), other()));
        assertEquals(50_000, table(out, "t_al_00000001").params.gcGraceSeconds);
        assertEquals(50_000, table(out, "t").params.gcGraceSeconds);
        assertTrue(PhysicalTables.matches(table(out, "t"), table(out, "t_al_00000001")));
    }

    @Test
    public void otherParametersChangeTheBaseOnly()
    {
        TableMetadata changed = logicalBuilder().params(TableParams.builder().comment("c").defaultTimeToLive(100).build()).build();
        Keyspaces next = keyspaces(changed, physical(), other());
        Keyspaces out = LayoutSchemaCoupling.apply(prev(), next);
        assertEquals(next, out);
        assertEquals(0, table(out, "t_al_00000001").params.defaultTimeToLive);
    }

    @Test
    public void replicationChangesApplyToBothTables()
    {
        Keyspaces next = keyspaces(KeyspaceParams.simple(3), logical(), physical(), other());
        assertEquals(next, LayoutSchemaCoupling.apply(prev(), next));
    }

    // ---- unaffected ----

    @Test
    public void ddlOnUnrelatedTablesIsUnaffected()
    {
        ClusterMetadata p = prev();
        Keyspaces added = keyspaces(logical(), physical(), other(), TableMetadata.builder("ks", "fresh").id(TableId.fromUUID(new UUID(21, 21))).addPartitionKeyColumn("p", Int32Type.instance).addClusteringColumn("c", Int32Type.instance).build());
        assertSame(added, LayoutSchemaCoupling.apply(p, added));
        Keyspaces dropped = keyspaces(logical(), physical());
        assertSame(dropped, LayoutSchemaCoupling.apply(p, dropped));
    }

    @Test
    public void emptyRegistryReturnsTheInputUntouched()
    {
        ClusterMetadata none = LayoutTransformationTest.empty();
        Keyspaces next = keyspaces(logical());
        assertSame(next, LayoutSchemaCoupling.apply(none, next));
    }

    @Test
    public void unchangedKeyspacesPassThrough()
    {
        Keyspaces same = keyspaces(logical(), physical(), other());
        assertEquals(same, LayoutSchemaCoupling.apply(prev(), same));
    }

    // ---- orphans and purity ----

    @Test
    public void orphansAreLayoutsWhoseLogicalTableIsGone()
    {
        assertTrue(LayoutSchemaCoupling.orphans(prev()).isEmpty());
        List<TableLayout> orphans = LayoutSchemaCoupling.orphans(prev(keyspaces(physical(), other())));
        assertEquals(1, orphans.size());
        assertEquals(LayoutTransformationTest.LOGICAL, orphans.get(0).logical);
        assertEquals(1, LayoutSchemaCoupling.orphans(prev(Keyspaces.none())).size());
    }

    @Test
    public void anOrphanKeepsItsOverflowTableProtected()
    {
        ClusterMetadata orphaned = prev(keyspaces(physical(), other()));
        assertInvalid("is managed", orphaned, keyspaces(other()));
        // while the keyspace itself may go
        assertEquals(Keyspaces.none(), LayoutSchemaCoupling.apply(orphaned, Keyspaces.none()));
    }

    @Test
    public void sameInputGivesTheSameOutput()
    {
        ClusterMetadata p = prev();
        Keyspaces next = keyspaces(logicalBuilder().params(TableParams.builder().gcGraceSeconds(50_000).build()).build(), physical(), other());
        assertEquals(LayoutSchemaCoupling.apply(p, next), LayoutSchemaCoupling.apply(p, next));
        Keyspaces drop = keyspaces(physical(), other());
        assertEquals(LayoutSchemaCoupling.apply(p, drop), LayoutSchemaCoupling.apply(p, drop));
        assertFalse(LayoutSchemaCoupling.apply(p, drop).equals(drop));
    }
}
