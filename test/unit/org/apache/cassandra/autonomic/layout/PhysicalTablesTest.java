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

import java.util.UUID;

import org.junit.Test;

import org.apache.cassandra.cql3.CQLTester;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/** {@link PhysicalTables}: the derived CQL parses into a table that matches, for the column kinds of design 2.1. */
public class PhysicalTablesTest extends CQLTester
{
    private static final UUID LAYOUT = UUID.fromString("1a2b3c4d-0000-4000-8000-000000000001");

    private TableMetadata table(String name)
    {
        return Schema.instance.getTableMetadata(keyspace(), name);
    }

    /** Derives the overflow table of {@code logical} and creates it; returns its metadata. */
    private TableMetadata createPhysical(TableMetadata logical, TableId id) throws Throwable
    {
        String cql = PhysicalTables.derive(logical, LAYOUT, id);
        schemaChange(cql);
        TableMetadata physical = table(PhysicalTables.physicalName(logical.name, LAYOUT));
        assertNotNull(cql, physical);
        return physical;
    }

    @Test
    public void compositeKeysReversedClusteringCollectionsAndUdts() throws Throwable
    {
        String udt = createType("CREATE TYPE %s (x int, y text)");
        String t = createTable("CREATE TABLE %s (a int, b text, c int, d text, e timeuuid, "
                               + "l list<int>, s set<text>, m map<text, int>, fl frozen<list<int>>, "
                               + "fu frozen<" + udt + ">, nu " + udt + ", tp tuple<int, text>, v vector<float, 3>, "
                               + "PRIMARY KEY ((a, b), c, d, e)) "
                               + "WITH CLUSTERING ORDER BY (c DESC, d ASC, e DESC) "
                               + "AND default_time_to_live = 50 AND gc_grace_seconds = 100000 "
                               + "AND compaction = {'class': 'LeveledCompactionStrategy', 'sstable_size_in_mb': '80'} "
                               + "AND compression = {'class': 'LZ4Compressor', 'chunk_length_in_kb': '16'} "
                               + "AND caching = {'keys': 'NONE', 'rows_per_partition': '10'}");
        TableMetadata logical = table(t);
        TableId id = TableId.generate();
        TableMetadata physical = createPhysical(logical, id);

        assertEquals(id, physical.id);
        assertEquals(PhysicalTables.physicalName(t, LAYOUT), physical.name);
        assertEquals(keyspace(), physical.keyspace);
        assertTrue(PhysicalTables.mismatch(logical, physical), PhysicalTables.matches(logical, physical));
        assertNull(PhysicalTables.mismatch(logical, physical));

        // partition key: the logical one plus the bucket, which is an int
        assertEquals(3, physical.partitionKeyColumns().size());
        ColumnMetadata bucket = physical.partitionKeyColumns().get(2);
        assertEquals(TableLayout.BUCKET_COLUMN, bucket.name.toString());
        assertEquals("int", bucket.type.asCQL3Type().toString());
        // clustering columns and their order, regular columns and their types
        assertTrue(physical.clusteringColumns().get(0).type.isReversed());
        assertFalse(physical.clusteringColumns().get(1).type.isReversed());
        assertTrue(physical.clusteringColumns().get(2).type.isReversed());
        for (ColumnMetadata column : logical.regularColumns())
        {
            ColumnMetadata copy = physical.getColumn(column.name);
            assertNotNull(column.name.toString(), copy);
            assertEquals(column.type, copy.type);
        }
        assertEquals(logical.regularColumns().size(), physical.regularColumns().size());
        // the options of the derivation
        assertEquals(0, physical.params.defaultTimeToLive);
        assertEquals(50, logical.params.defaultTimeToLive);
        assertEquals(100000, physical.params.gcGraceSeconds);
        assertEquals(logical.params.compaction, physical.params.compaction);
        assertEquals(logical.params.compression, physical.params.compression);
        assertEquals(logical.params.caching, physical.params.caching);
        assertEquals("Managed by autonomic layout of " + keyspace() + '.' + t + ". Do not access directly.", physical.params.comment);
        // pure: the same input gives the same text
        assertEquals(PhysicalTables.derive(logical, LAYOUT, id), PhysicalTables.derive(logical, LAYOUT, id));
    }

    @Test
    public void singleColumnKey() throws Throwable
    {
        String t = createTable("CREATE TABLE %s (k int, c1 int, c2 text, v blob, PRIMARY KEY (k, c1, c2))");
        TableMetadata logical = table(t);
        TableMetadata physical = createPhysical(logical, TableId.generate());
        assertTrue(PhysicalTables.mismatch(logical, physical), PhysicalTables.matches(logical, physical));
        assertEquals(2, physical.partitionKeyColumns().size());
        assertEquals("k", physical.partitionKeyColumns().get(0).name.toString());
        assertFalse(physical.clusteringColumns().get(0).type.isReversed());
    }

    @Test
    public void quotedNames() throws Throwable
    {
        String t = createTable("CREATE TABLE %s (\"Key\" int, \"Clust\" int, \"Val Col\" text, PRIMARY KEY (\"Key\", \"Clust\"))");
        TableMetadata logical = table(t);
        TableMetadata physical = createPhysical(logical, TableId.generate());
        assertTrue(PhysicalTables.mismatch(logical, physical), PhysicalTables.matches(logical, physical));
        assertNotNull(physical.getColumn(org.apache.cassandra.cql3.ColumnIdentifier.getInterned("Val Col", true)));
    }

    @Test
    public void mismatchesAreReported() throws Throwable
    {
        String t = createTable("CREATE TABLE %s (p int, c int, v text, PRIMARY KEY (p, c))");
        TableMetadata logical = table(t);
        TableMetadata physical = createPhysical(logical, TableId.generate());
        String name = keyspace() + '.' + physical.name;
        assertTrue(PhysicalTables.matches(logical, physical));

        // parameters that may differ: the derivation copies them, but they are changed on the base only (design 4.8)
        execute("ALTER TABLE " + name + " WITH compaction = {'class': 'SizeTieredCompactionStrategy'} AND caching = {'keys': 'NONE'}");
        assertTrue(PhysicalTables.mismatch(logical, table(physical.name)), PhysicalTables.matches(logical, table(physical.name)));

        execute("ALTER TABLE " + name + " WITH default_time_to_live = 5");
        assertEquals("the physical table has a default time to live", PhysicalTables.mismatch(logical, table(physical.name)));
        execute("ALTER TABLE " + name + " WITH default_time_to_live = 0 AND gc_grace_seconds = 777");
        assertTrue(PhysicalTables.mismatch(logical, table(physical.name)).startsWith("gc_grace_seconds is 777"));
        execute("ALTER TABLE " + name + " WITH gc_grace_seconds = " + logical.params.gcGraceSeconds);
        assertTrue(PhysicalTables.matches(logical, table(physical.name)));

        execute("ALTER TABLE " + name + " ADD extra int");
        assertEquals("regular columns differ", PhysicalTables.mismatch(logical, table(physical.name)));
        execute("ALTER TABLE " + name + " DROP extra");
        assertTrue(PhysicalTables.matches(logical, table(physical.name)));
        execute("ALTER TABLE " + name + " ADD st int static");
        assertEquals("static columns differ", PhysicalTables.mismatch(logical, table(physical.name)));

        // a table that is not an overflow table of this one
        String other = createTable("CREATE TABLE %s (p int, c int, v text, PRIMARY KEY (p, c))");
        assertTrue(PhysicalTables.mismatch(logical, table(other)).startsWith("partition key has 1 columns"));
        assertEquals("regular columns differ", PhysicalTables.mismatch(table(other), createPhysicalOf("p int, \"__al_bucket\" int, c int, w text, PRIMARY KEY ((p, \"__al_bucket\"), c)")));
        assertEquals("clustering columns differ", PhysicalTables.mismatch(table(other), createPhysicalOf("p int, \"__al_bucket\" int, c text, v text, PRIMARY KEY ((p, \"__al_bucket\"), c)")));
        assertEquals("last partition key column is not an int named " + TableLayout.BUCKET_COLUMN,
                     PhysicalTables.mismatch(table(other), createPhysicalOf("p int, b int, c int, v text, PRIMARY KEY ((p, b), c)")));
        assertEquals("partition key column 0 differs",
                     PhysicalTables.mismatch(table(other), createPhysicalOf("p text, \"__al_bucket\" int, c int, v text, PRIMARY KEY ((p, \"__al_bucket\"), c)")));
        // reversed clustering order is part of the type
        assertEquals("clustering columns differ",
                     PhysicalTables.mismatch(table(other), createPhysicalOf("p int, \"__al_bucket\" int, c int, v text, PRIMARY KEY ((p, \"__al_bucket\"), c)", "WITH CLUSTERING ORDER BY (c DESC)")));
    }

    private TableMetadata createPhysicalOf(String definition) throws Throwable
    {
        return createPhysicalOf(definition, "");
    }

    private TableMetadata createPhysicalOf(String definition, String options) throws Throwable
    {
        return table(createTable("CREATE TABLE %s (" + definition + ") " + options));
    }

    @Test
    public void names()
    {
        assertEquals("t_al_1a2b3c4d", PhysicalTables.physicalName("t", LAYOUT));
        String longName = "abcdefghijklmnopqrstuvwxyz0123456789ABCD"; // 40 characters
        String name = PhysicalTables.physicalName(longName, LAYOUT);
        assertEquals(longName.substring(0, 32) + "_al_1a2b3c4d", name);
        assertEquals(44, name.length());
        assertTrue(name.length() < 48);
        // the first 8 hex digits of the layout id, not of a hash
        assertEquals("t_al_ffffffff", PhysicalTables.physicalName("t", new UUID(-1L, -1L)));
        assertEquals("t_al_00000000", PhysicalTables.physicalName("t", new UUID(0, 1)));
    }
}
