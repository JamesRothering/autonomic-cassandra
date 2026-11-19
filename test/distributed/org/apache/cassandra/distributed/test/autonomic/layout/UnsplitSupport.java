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
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import org.apache.cassandra.autonomic.AutonomicService;
import org.apache.cassandra.autonomic.analyze.Diagnosis;
import org.apache.cassandra.autonomic.layout.BucketFunction;
import org.apache.cassandra.autonomic.layout.LayoutRegistry;
import org.apache.cassandra.autonomic.layout.NaturalReplicas;
import org.apache.cassandra.autonomic.layout.RowMapper;
import org.apache.cassandra.autonomic.layout.TableLayout;
import org.apache.cassandra.config.DataRateSpec;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.NodeId;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * T6.17k: what the end-to-end and failure suite of the reverse migration shares, on top of {@link SplitFailureSupport}:
 * the release of a split, the layout row of the virtual table, the R1 check of {@code writetime} and {@code ttl}
 * against the model, the bucket replicas of a clustering, and the backfill rate.
 *
 * <p>Invariants: {@link Model#check} asserts I5 (every acknowledged write with its own timestamp, every acknowledged
 * delete held; with the model's unknown outcomes this is I22 at client level), I7, I14, and through the phase recorder
 * I10 as amended by 11.6 and I21 (after BASE_ONLY no node is seen in a class above C0).
 */
abstract class UnsplitSupport extends SplitFailureSupport
{
    /** A column of the key's row of {@code system_views.autonomic_layouts} on the node; null if there is none. */
    static String layoutRow(IInvokableInstance node, String table, String column)
    {
        Object[][] rows = node.executeInternal("SELECT " + column + " FROM system_views.autonomic_layouts WHERE keyspace_name = ? AND table_name = ?", KS, table);
        return rows.length == 0 || rows[0][0] == null ? null : String.valueOf(rows[0][0]);
    }

    /** Waits until the split of the key is released (no owner) with the given base state. */
    static void awaitReleased(IInvokableInstance node, String table, String baseState) throws InterruptedException
    {
        await(table + " released " + baseState, () -> "STEADY".equals(layoutRow(node, table, "phase")) && layoutRow(node, table, "action_id") == null
                                                     && baseState.equals(layoutRow(node, table, "base_state")));
    }

    /** The overflow table of the table's layout, without the keyspace; null if there is no layout. */
    static String physicalOf(IInvokableInstance node, String table)
    {
        String physical = layoutRow(node, table, "physical_table");
        return physical == null ? null : physical.substring(physical.indexOf('.') + 1);
    }

    static boolean tableExists(IInvokableInstance node, String table)
    {
        return node.callOnInstance(() -> Schema.instance.getTableMetadata(KS, table) != null);
    }

    static boolean layoutGone(IInvokableInstance node, String table)
    {
        return node.callOnInstance(() -> LayoutRegistry.of(ClusterMetadata.current()).layout(Schema.instance.getTableMetadata(KS, table).id) == null);
    }

    static void backfillRate(Cluster cluster, String rate, int chunkRows)
    {
        for (int node = 1; node <= cluster.size(); node++)
            if (!cluster.get(node).isShutdown())
                cluster.get(node).runOnInstance(() -> {
                    DatabaseDescriptor.getAutonomicConfig().layout.backfill_throughput = new DataRateSpec.LongBytesPerSecondBound(rate);
                    DatabaseDescriptor.getAutonomicConfig().layout.backfill_chunk_rows = chunkRows;
                });
    }

    /** The last-octet numbers of the nodes that are natural replicas of the bucket partition holding clustering c. */
    static Set<Integer> bucketReplicasOf(IInvokableInstance node, String table, int key, int c)
    {
        Set<Integer> out = new TreeSet<>();
        for (String a : node.callOnInstance(() -> {
            ClusterMetadata cm = ClusterMetadata.current();
            TableMetadata logical = Schema.instance.getTableMetadata(KS, table);
            TableLayout layout = LayoutRegistry.of(cm).layout(logical.id);
            TableMetadata physical = Schema.instance.getTableMetadata(layout.physical);
            int bucket = BucketFunction.bucket(Clustering.make(Int32Type.instance.decompose(c)), layout.hashedClusteringColumns, layout.buckets);
            List<String> addresses = new ArrayList<>();
            for (NodeId id : NaturalReplicas.of(cm, physical, RowMapper.physicalKey(logical, physical, bytes(key), layout.keyState(bytes(key)).incarnation, bucket).getKey()))
            {
                InetAddressAndPort endpoint = cm.directory.endpoint(id);
                addresses.add(endpoint.getAddress().getHostAddress());
            }
            return addresses;
        }))
            out.add(Integer.parseInt(a.substring(a.lastIndexOf('.') + 1)));
        return out;
    }

    /**
     * R1 against the model: for every clustering whose outcome is settled, the table and the twin agree on the
     * presence of the row and on {@code writetime} of {@code v} and {@code s}; their {@code ttl} differ by at most the
     * few seconds between the two writes of an acknowledged statement. Returns the number of rows compared.
     */
    static int assertWritetimeAndTtl(Model model, String label)
    {
        String select = "SELECT c, writetime(v), ttl(v), writetime(s), ttl(s) FROM " + KS + ".%s WHERE p = " + model.key;
        Map<Integer, Object[]> table = byC(model.cluster.coordinator(1).execute(String.format(select, model.table), ConsistencyLevel.ALL));
        Map<Integer, Object[]> twin = byC(model.cluster.coordinator(1).execute(String.format(select, model.table + "_twin"), ConsistencyLevel.ALL));
        Set<Integer> all = new TreeSet<>(table.keySet());
        all.addAll(twin.keySet());
        List<String> failures = new ArrayList<>();
        int compared = 0;
        for (int c : all)
        {
            if (c == POISON || !model.alternatives(c).isEmpty())
                continue;
            Object[] t = table.get(c);
            Object[] m = twin.get(c);
            if (t == null || m == null)
            {
                failures.add("clustering " + c + (t == null ? " is missing" : " should not be there"));
                continue;
            }
            compared++;
            for (int i : new int[]{ 1, 3 })
                if (!java.util.Objects.equals(t[i], m[i]))
                    failures.add("clustering " + c + ": writetime " + t[i] + ", model " + m[i]);
            for (int i : new int[]{ 2, 4 })
            {
                Integer a = (Integer) t[i];
                Integer b = (Integer) m[i];
                if ((a == null) != (b == null) || (a != null && Math.abs(a - b) > 3))
                    failures.add("clustering " + c + ": ttl " + a + ", model " + b);
            }
        }
        assertTrue(label + ": R1: " + failures.size() + " differences:\n" + String.join("\n", failures.subList(0, Math.min(30, failures.size()))),
                   failures.isEmpty());
        return compared;
    }

    private static Map<Integer, Object[]> byC(Object[][] rows)
    {
        Map<Integer, Object[]> m = new TreeMap<>();
        for (Object[] row : rows)
            m.put((Integer) row[0], row);
        return m;
    }

    /** The active diagnoses of the type on the node, as "subject | evidence". */
    static List<String> diagnoses(IInvokableInstance node, String type)
    {
        return node.callOnInstance(() -> {
            List<String> out = new ArrayList<>();
            for (Diagnosis d : AutonomicService.instance.activeDiagnoses())
                if (d.type.equals(type))
                    out.add(d.subject + " | " + d.evidence);
            return out;
        });
    }

    static void assertRow(Cluster cluster, String table, int key, int c, Integer v)
    {
        for (int node = 1; node <= cluster.size(); node++)
            if (!cluster.get(node).isShutdown())
                assertEquals("clustering " + c + " through node " + node, v, readV(cluster, node, table, key, c));
    }

    /**
     * {@link Model#check} while the owner moves the key: a read that meets more phase changes than its re-executions
     * allow ends with a timeout ("changed while it was read"), and a read around a node going down may time out: both
     * are allowed outcomes of a read, not invariant violations; the check is then taken again.
     */
    static void checkWhileMoving(Model model, String label) throws InterruptedException
    {
        for (int attempt = 0; ; attempt++)
        {
            try
            {
                model.check(label);
                return;
            }
            catch (AssertionError e)
            {
                throw e;
            }
            catch (RuntimeException e)
            {
                String message = String.valueOf(e.getMessage());
                if (attempt >= 10 || (!message.contains("changed while it was read") && !message.contains("timed out")))
                    throw e;
                Thread.sleep(POLL_MILLIS);
            }
        }
    }
}
