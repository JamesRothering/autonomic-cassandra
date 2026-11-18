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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.junit.Test;

import org.apache.cassandra.config.DataRateSpec;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * T6.14, design 4.8: {@code ALTER TABLE ADD} and {@code DROP} of regular columns on an adapted table are mirrored onto
 * the overflow table inside the same {@code AlterSchema}. Three nodes, replication factor 3, one split key (K) and one
 * normal partition. In {@code BACKFILL} (held there by throttling the backfill, as in SplitCmsUnavailableTest) and again
 * in {@code STEADY}: a column is added and one is dropped; on every node both tables have the same regular columns and
 * the same table epoch; writes and reads of the new column work on the adapted key and on the normal partition, at
 * every coordinator, and data written to the column in {@code BACKFILL} is still there after the cutover.
 */
public class LayoutColumnMirrorTest extends SplitFailureSupport
{
    private static final int K = 1;
    private static final int OTHER = 2;
    private static final int ROWS = 120;
    private static final String TABLE = "mirror";

    private static void backfillRate(Cluster cluster, String rate, int chunkRows)
    {
        for (int node = 1; node <= 3; node++)
            cluster.get(node).runOnInstance(() -> {
                DatabaseDescriptor.getAutonomicConfig().layout.backfill_throughput = new DataRateSpec.LongBytesPerSecondBound(rate);
                DatabaseDescriptor.getAutonomicConfig().layout.backfill_chunk_rows = chunkRows;
            });
    }

    private static String overflow(IInvokableInstance node)
    {
        Object[][] rows = node.executeInternal("SELECT physical_table FROM system_views.autonomic_layouts WHERE keyspace_name = ?", KS);
        String name = (String) rows[0][0];
        return name.contains(".") ? name.substring(name.indexOf('.') + 1) : name;
    }

    /** "regular columns : epoch" of a table as the node sees it. */
    private static String shape(IInvokableInstance node, String table)
    {
        return node.callOnInstance(() -> {
            TableMetadata t = Schema.instance.getTableMetadata(KS, table);
            Set<String> names = new TreeSet<>();
            t.regularColumns().forEach(c -> names.add(c.name.toString()));
            return names + ":" + t.epoch.getEpoch();
        });
    }

    private static Set<String> columns(IInvokableInstance node, String table)
    {
        String shape = shape(node, table);
        Set<String> names = new TreeSet<>();
        for (String name : shape.substring(1, shape.indexOf(']')).split(", "))
            names.add(name);
        return names;
    }

    /** Both tables have the same regular columns, and the overflow table was changed in the epoch of the logical one. */
    private static void assertMirrored(Cluster cluster, String table, String... expected)
    {
        assertMirrored(cluster, true, table, expected);
    }

    /** The epochs are compared right after the DDL only: later schema changes of the layer may move the overflow table's epoch. */
    private static void assertMirrored(Cluster cluster, boolean sameEpoch, String table, String... expected)
    {
        for (int n = 1; n <= 3; n++)
        {
            IInvokableInstance node = cluster.get(n);
            String physical = overflow(node);
            assertEquals("node " + n, new TreeSet<>(List.of(expected)), columns(node, table));
            assertEquals("node " + n, columns(node, table), columns(node, physical));
            if (sameEpoch)
                assertEquals("node " + n + " logical and overflow table changed in one epoch",
                             shape(node, table).substring(shape(node, table).indexOf(':')),
                             shape(node, physical).substring(shape(node, physical).indexOf(':')));
        }
    }

    private static String describe(Object[][] rows)
    {
        List<String> out = new ArrayList<>();
        for (Object[] row : rows)
            out.add(java.util.Arrays.toString(row));
        return out.toString();
    }

    /** Writes the column on the adapted key and on the normal partition through each coordinator and reads it back through each. */
    private static void exercise(Cluster cluster, String column, int base)
    {
        for (int coordinator = 1; coordinator <= 3; coordinator++)
        {
            int c = base + coordinator;
            for (int p : new int[]{ K, OTHER })
                cluster.coordinator(coordinator).execute("INSERT INTO " + KS + '.' + TABLE + " (p, c, v, " + column + ") VALUES (" + p + ", " + c
                                                         + ", " + c + ", 'new-" + p + '-' + c + "')", ConsistencyLevel.ALL);
        }
        for (int reader = 1; reader <= 3; reader++)
            for (int p : new int[]{ K, OTHER })
                for (int coordinator = 1; coordinator <= 3; coordinator++)
                {
                    int c = base + coordinator;
                    Object[][] rows = cluster.coordinator(reader).execute("SELECT c, " + column + " FROM " + KS + '.' + TABLE + " WHERE p = " + p + " AND c = " + c,
                                                                         ConsistencyLevel.ALL);
                    assertEquals(describe(rows), 1, rows.length);
                    assertEquals("new-" + p + '-' + c, rows[0][1]);
                }
        // a slice over the column, and an update of a pre-existing row
        cluster.coordinator(2).execute("UPDATE " + KS + '.' + TABLE + " SET " + column + " = 'upd' WHERE p = " + K + " AND c = 0", ConsistencyLevel.ALL);
        cluster.coordinator(3).execute("UPDATE " + KS + '.' + TABLE + " SET " + column + " = 'upd' WHERE p = " + OTHER + " AND c = 0", ConsistencyLevel.ALL);
        for (int p : new int[]{ K, OTHER })
        {
            Object[][] rows = cluster.coordinator(1).execute("SELECT c, " + column + " FROM " + KS + '.' + TABLE + " WHERE p = " + p, ConsistencyLevel.ALL);
            assertEquals(describe(rows), "upd", rows[0][1]);
            assertTrue(describe(rows), java.util.Arrays.stream(rows).filter(r -> r[1] != null && r[1].toString().startsWith("new-") && (int) r[0] / 1000 == base / 1000).count() == 3);
        }
    }

    private static void assertDropped(Cluster cluster, String column)
    {
        for (int p : new int[]{ K, OTHER })
        {
            Object[][] rows = cluster.coordinator(1).execute("SELECT * FROM " + KS + '.' + TABLE + " WHERE p = " + p, ConsistencyLevel.ALL);
            assertFalse(describe(rows), rows.length == 0);
            try
            {
                cluster.coordinator(1).execute("SELECT " + column + " FROM " + KS + '.' + TABLE + " WHERE p = " + p, ConsistencyLevel.ALL);
                throw new AssertionError("column " + column + " should be gone");
            }
            catch (AssertionError e)
            {
                throw e;
            }
            catch (Throwable expected)
            {
                // unknown identifier
            }
        }
    }

    @Test
    public void columnsAreMirroredInBackfillAndSteady() throws Throwable
    {
        try (Cluster cluster = start(3, config(nodes(1), settings(), settings())))
        {
            IInvokableInstance n1 = cluster.get(1);
            n1.nodetoolResult("cms", "reconfigure", "3").asserts().success();
            cluster.schemaChange("CREATE KEYSPACE " + KS + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 3}");
            cluster.schemaChange("CREATE TABLE " + KS + '.' + TABLE + " (p int, c int, v int, s text, drop_me text, PRIMARY KEY (p, c))");
            arm(n1);
            for (int p : new int[]{ K, OTHER })
                for (int c = 0; c < ROWS; c++)
                    cluster.coordinator(1).execute("INSERT INTO " + KS + '.' + TABLE + " (p, c, v, s, drop_me) VALUES (" + p + ", " + c + ", " + c
                                                   + ", 'row-" + c + "-" + "x".repeat(60) + "', 'd" + c + "')", ConsistencyLevel.ALL);

            backfillRate(cluster, "3KiB/s", 10);
            split(n1, TABLE, K, 4);
            for (int node = 1; node <= 3; node++)
                awaitPhase(cluster.get(node), TABLE, K, "BACKFILL");
            assertMirrored(cluster, false, TABLE, "drop_me", "s", "v");

            // ---- BACKFILL: add a column ----
            cluster.schemaChange("ALTER TABLE " + KS + '.' + TABLE + " ADD w text");
            assertMirrored(cluster, TABLE, "drop_me", "s", "v", "w");
            exercise(cluster, "w", 1000);
            for (int node = 1; node <= 3; node++)
                assertEquals("BACKFILL", phaseOf(cluster.get(node), TABLE, K));

            // ---- BACKFILL: drop a column ----
            cluster.schemaChange("ALTER TABLE " + KS + '.' + TABLE + " DROP drop_me");
            assertMirrored(cluster, TABLE, "s", "v", "w");
            assertDropped(cluster, "drop_me");
            Object[][] before = cluster.coordinator(1).execute("SELECT c, v, s, w FROM " + KS + '.' + TABLE + " WHERE p = " + K, ConsistencyLevel.ALL);
            assertEquals(ROWS + 3, before.length);
            // a rename-free sequence the other way round: the dropped name can be added again, empty
            cluster.schemaChange("ALTER TABLE " + KS + '.' + TABLE + " ADD drop_me text");
            assertMirrored(cluster, TABLE, "drop_me", "s", "v", "w");
            assertNotNull(cluster.coordinator(1).execute("SELECT drop_me FROM " + KS + '.' + TABLE + " WHERE p = " + K + " AND c = 5", ConsistencyLevel.ALL)[0]);
            assertEquals(null, cluster.coordinator(1).execute("SELECT drop_me FROM " + KS + '.' + TABLE + " WHERE p = " + K + " AND c = 5", ConsistencyLevel.ALL)[0][0]);
            cluster.schemaChange("ALTER TABLE " + KS + '.' + TABLE + " DROP drop_me");
            assertMirrored(cluster, TABLE, "s", "v", "w");

            // ---- the migration finishes; data of the column written in BACKFILL survives the cutover ----
            backfillRate(cluster, "0MiB/s", 1000);
            for (int node = 1; node <= 3; node++)
                awaitPhase(cluster.get(node), TABLE, K, "STEADY");
            await("the action committed", () -> history(n1).contains("state=COMMITTED"));
            assertMirrored(cluster, false, TABLE, "s", "v", "w");
            Object[][] after = cluster.coordinator(2).execute("SELECT c, v, s, w FROM " + KS + '.' + TABLE + " WHERE p = " + K, ConsistencyLevel.ALL);
            assertEquals(describe(before), describe(after));

            // ---- STEADY: add and drop ----
            cluster.schemaChange("ALTER TABLE " + KS + '.' + TABLE + " ADD y int");
            assertMirrored(cluster, TABLE, "s", "v", "w", "y");
            for (int coordinator = 1; coordinator <= 3; coordinator++)
                for (int p : new int[]{ K, OTHER })
                    cluster.coordinator(coordinator).execute("INSERT INTO " + KS + '.' + TABLE + " (p, c, v, y) VALUES (" + p + ", " + (2000 + coordinator)
                                                             + ", 1, " + (p * 10 + coordinator) + ")", ConsistencyLevel.ALL);
            for (int reader = 1; reader <= 3; reader++)
                for (int p : new int[]{ K, OTHER })
                {
                    Object[][] rows = cluster.coordinator(reader).execute("SELECT c, y FROM " + KS + '.' + TABLE + " WHERE p = " + p + " AND c > 1999 AND c < 2100", ConsistencyLevel.ALL);
                    assertEquals(describe(rows), 3, rows.length);
                    assertEquals(describe(rows), p * 10 + 1, rows[0][1]);
                }
            exercise(cluster, "w", 3000);
            cluster.schemaChange("ALTER TABLE " + KS + '.' + TABLE + " DROP w");
            assertMirrored(cluster, TABLE, "s", "v", "y");
            assertDropped(cluster, "w");
            for (int p : new int[]{ K, OTHER })
            {
                Object[][] rows = cluster.coordinator(3).execute("SELECT c, v, s, y FROM " + KS + '.' + TABLE + " WHERE p = " + p, ConsistencyLevel.ALL);
                assertTrue(describe(rows), rows.length >= ROWS);
            }
            assertEquals("STEADY", phaseOf(cluster.get(3), TABLE, K));

            // the schema and the data survive a restart of every node
            for (int node = 1; node <= 3; node++)
                org.apache.cassandra.distributed.shared.ClusterUtils.restartUnchecked(cluster.get(node));
            assertMirrored(cluster, false, TABLE, "s", "v", "y");
            Object[][] rows = cluster.coordinator(1).execute("SELECT c, y FROM " + KS + '.' + TABLE + " WHERE p = " + K + " AND c > 1999 AND c < 2100", ConsistencyLevel.ALL);
            assertEquals(describe(rows), 3, rows.length);
        }
    }
}
