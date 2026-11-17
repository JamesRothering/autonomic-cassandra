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

import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableMap;

import org.junit.After;
import org.junit.Test;

import org.apache.cassandra.autonomic.layout.Eligibility.Blocker;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.virtual.VirtualKeyspaceRegistry;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableParams;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.schema.TriggerMetadata;
import org.apache.cassandra.schema.Triggers;
import org.apache.cassandra.service.consensus.TransactionalMode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** {@link Eligibility#structural}: each blocking feature of design section 6 is reported, and only that one. */
public class EligibilityTest extends CQLTester
{
    @After
    public void restore()
    {
        DatabaseDescriptor.setMaterializedViewsEnabled(false);
        DatabaseDescriptor.setDynamicDataMaskingEnabled(false);
        DatabaseDescriptor.setTransientReplicationEnabledUnsafe(false);
    }

    private static TableMetadata.Builder base()
    {
        return TableMetadata.builder("ks", "t")
                            .addPartitionKeyColumn("p", Int32Type.instance)
                            .addClusteringColumn("c", Int32Type.instance)
                            .addRegularColumn("v", UTF8Type.instance);
    }

    private TableMetadata table(String name)
    {
        return Schema.instance.getTableMetadata(keyspace(), name);
    }

    private List<Blocker> structural(String name)
    {
        return Eligibility.structural(table(name), Schema.instance.getKeyspaceMetadata(keyspace()));
    }

    private static List<Blocker> structural(TableMetadata table)
    {
        return Eligibility.structural(table, KeyspaceMetadata.create("ks", KeyspaceParams.simple(1), Tables.of(table)));
    }

    private static void assertBlocked(Blocker expected, List<Blocker> actual)
    {
        assertEquals(Arrays.asList(expected), actual);
        assertTrue(Eligibility.describe(actual).contains(expected.name()));
    }

    @Test
    public void anOrdinaryTableIsEligible() throws Throwable
    {
        String t = createTable("CREATE TABLE %s (p int, c int, v text, l list<int>, PRIMARY KEY (p, c)) WITH default_time_to_live = 30");
        assertEquals(List.of(), structural(t));
        assertNull(Eligibility.describe(structural(t)));
        assertEquals(List.of(), structural(base().build()));
        // the boundary of gc_grace_seconds
        assertEquals(List.of(), structural(base().params(TableParams.builder().gcGraceSeconds(Eligibility.MIN_GC_GRACE_SECONDS).build()).build()));
        assertEquals(28800, Eligibility.MIN_GC_GRACE_SECONDS);
    }

    @Test
    public void blockersOfTheSchema() throws Throwable
    {
        assertBlocked(Blocker.COUNTER, structural(createTable("CREATE TABLE %s (p int, c int, n counter, PRIMARY KEY (p, c))")));
        assertBlocked(Blocker.STATIC_COLUMN, structural(createTable("CREATE TABLE %s (p int, c int, s int static, v int, PRIMARY KEY (p, c))")));
        assertBlocked(Blocker.NO_CLUSTERING, structural(createTable("CREATE TABLE %s (p int PRIMARY KEY, v int)")));
        assertBlocked(Blocker.GC_GRACE_TOO_SMALL, structural(createTable("CREATE TABLE %s (p int, c int, v int, PRIMARY KEY (p, c)) WITH gc_grace_seconds = 28799")));
        assertBlocked(Blocker.RESERVED_COLUMN, structural(createTable("CREATE TABLE %s (p int, c int, \"__al_bucket\" int, PRIMARY KEY (p, c))")));

        String indexed = createTable("CREATE TABLE %s (p int, c int, v int, PRIMARY KEY (p, c))");
        createIndex("CREATE INDEX ON %s (v)");
        assertBlocked(Blocker.INDEX, structural(indexed));

        DatabaseDescriptor.setDynamicDataMaskingEnabled(true);
        assertBlocked(Blocker.MASKED_COLUMN, structural(createTable("CREATE TABLE %s (p int, c int, v text MASKED WITH DEFAULT, PRIMARY KEY (p, c))")));
    }

    @Test
    public void materializedView() throws Throwable
    {
        DatabaseDescriptor.setMaterializedViewsEnabled(true);
        String t = createTable("CREATE TABLE %s (p int, c int, v int, PRIMARY KEY (p, c))");
        String view = "view_of_" + t;
        // created by hand: the test helper waits for the build of the view, which this test does not need
        schemaChange("CREATE MATERIALIZED VIEW " + keyspace() + '.' + view + " AS SELECT * FROM " + keyspace() + '.' + t
                     + " WHERE p IS NOT NULL AND c IS NOT NULL AND v IS NOT NULL PRIMARY KEY (v, p, c)");
        try
        {
            // the base table, and the view itself
            assertBlocked(Blocker.MATERIALIZED_VIEW, structural(t));
            assertTrue(structural(view).contains(Blocker.MATERIALIZED_VIEW));
        }
        finally
        {
            schemaChange("DROP MATERIALIZED VIEW " + keyspace() + '.' + view);
        }
    }

    @Test
    public void systemAndVirtualTables()
    {
        addVirtualKeyspace();
        TableMetadata system = Schema.instance.getTableMetadata("system_schema", "tables");
        assertTrue(Eligibility.structural(system, Schema.instance.getKeyspaceMetadata("system_schema")).contains(Blocker.SYSTEM_KEYSPACE));
        TableMetadata virtual = VirtualKeyspaceRegistry.instance.getKeyspaceNullable("system_views").tables().iterator().next().metadata();
        assertTrue(virtual.isVirtual());
        List<Blocker> blockers = Eligibility.structural(virtual, null);
        assertTrue(blockers.toString(), blockers.contains(Blocker.VIRTUAL_TABLE) && blockers.contains(Blocker.SYSTEM_KEYSPACE));
    }

    @Test
    public void handBuiltSchema()
    {
        assertBlocked(Blocker.CDC, structural(base().params(TableParams.builder().cdc(true).build()).build()));
        assertBlocked(Blocker.TRIGGER, structural(base().triggers(Triggers.of(TriggerMetadata.create("trg", "org.example.Trigger"))).build()));
        assertBlocked(Blocker.ACCORD, structural(base().params(TableParams.builder().transactionalMode(TransactionalMode.full).build()).build()));
        assertBlocked(Blocker.GC_GRACE_TOO_SMALL, structural(base().params(TableParams.builder().gcGraceSeconds(0).build()).build()));
        assertBlocked(Blocker.COMPACT_STORAGE, structural(base().flags(EnumSet.of(TableMetadata.Flag.DENSE, TableMetadata.Flag.COMPOUND)).build()));
    }

    @Test
    public void transientReplication()
    {
        DatabaseDescriptor.setTransientReplicationEnabledUnsafe(true);
        TableMetadata table = base().build();
        Map<String, String> replication = ImmutableMap.of("class", "SimpleStrategy", "replication_factor", "3/1");
        KeyspaceMetadata keyspace = KeyspaceMetadata.create("ks", KeyspaceParams.create(true, replication), Tables.of(table));
        assertBlocked(Blocker.TRANSIENT_REPLICATION, Eligibility.structural(table, keyspace));
        assertEquals(List.of(), Eligibility.structural(table, KeyspaceMetadata.create("ks", KeyspaceParams.simple(3), Tables.of(table))));
    }

    @Test
    public void severalBlockersAreAllReportedInOrder() throws Throwable
    {
        TableMetadata table = base().params(TableParams.builder().cdc(true).gcGraceSeconds(1).build()).addStaticColumn("s", Int32Type.instance).build();
        assertEquals(Arrays.asList(Blocker.CDC, Blocker.STATIC_COLUMN, Blocker.GC_GRACE_TOO_SMALL), structural(table));
        String text = Eligibility.describe(structural(table));
        assertTrue(text, text.indexOf("CDC") < text.indexOf("STATIC_COLUMN") && text.indexOf("STATIC_COLUMN") < text.indexOf("GC_GRACE_TOO_SMALL"));
    }

    @Test
    public void runtimeBlocksOnLwtActivityOnTheTableOrInTheProfile()
    {
        assertNull(Eligibility.runtime(0, 0));
        String onTable = Eligibility.runtime(3, 0);
        assertTrue(onTable, onTable.contains("3 Paxos operations"));
        assertTrue(Eligibility.runtime(0, 2).contains("2 on the key in its profile"));
        assertNull(LayoutProfiler.runtime(base().build(), null));
    }

    @Test
    public void runtimeReadsThePaxosCountersOfTheTable() throws Throwable
    {
        String t = createTable("CREATE TABLE %s (p int, c int, v int, PRIMARY KEY (p, c))");
        TableMetadata table = table(t);
        assertEquals(0, LayoutProfiler.paxosOperations(table));
        assertNull(LayoutProfiler.runtime(table, null));
        // a replica counts the Paxos phases it serves in the table's metrics
        Schema.instance.getColumnFamilyStoreInstance(table.id).metric.casPropose.addNano(1000);
        assertEquals(1, LayoutProfiler.paxosOperations(table));
        assertTrue(LayoutProfiler.runtime(table, null).contains("lightweight transactions"));
    }
}
