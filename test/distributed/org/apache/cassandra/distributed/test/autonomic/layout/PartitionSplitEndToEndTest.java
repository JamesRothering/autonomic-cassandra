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
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;

import org.junit.Test;

import org.apache.cassandra.autonomic.AutonomicService;
import org.apache.cassandra.cql3.PageSize;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.transport.Dispatcher;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.apache.cassandra.distributed.api.Feature.GOSSIP;
import static org.apache.cassandra.distributed.api.Feature.NETWORK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * The demonstration of adaptive-schema design section 8 (T6.9): three nodes, replication factor 2, a table
 * {@code ((id), seq) -> v} with one partition of 5000 rows, split into 8 buckets by {@code nodetool autonomic layout
 * split} under manual approval and a second approval at the commit point, while a background thread writes, reads
 * single rows, slices, and scans the table through all three coordinators and checks every result against a model.
 *
 * <p>Asserts: every phase was committed (the action's audit trail); no result ever differed from the model; a paging
 * state taken before CUTOVER continues correctly after STEADY; afterwards the overflow table holds the 8 bucket
 * partitions, and they are not all on the same nodes. The base partition is kept as residue in STEADY until a reclaim
 * deletes it (T6.17b), which {@code SplitReclaimTest} covers.
 *
 * <p>The proposal comes from the operator command, not from a diagnosis: the policy that turns {@code hot_partition}
 * into a proposal is T6.11. One cluster start; phase waits are hundreds of milliseconds.
 */
public class PartitionSplitEndToEndTest extends TestBaseImpl
{
    private static final String KS = "split_ks";
    private static final int HOT = 1;
    private static final int ROWS = 5000;
    private static final int BUCKETS = 8;
    private static final long DEADLINE_MILLIS = 240_000;

    static void configure(IInstanceConfig c)
    {
        c.with(NETWORK, GOSSIP)
         .set("write_request_timeout", "2000ms")
         .set("counter_write_request_timeout", "2000ms")
         .set("cas_contention_timeout", "1000ms")
         .set("range_request_timeout", "5000ms")
         .set("truncate_request_timeout", "2000ms")
         .set("autonomic.layout", Map.of("ack_poll_interval", "200ms", "cas_drain", "100ms", "cutover_soak", "1s",
                                         "straggler_window", "1s", "commit_approval", "manual", "verify_recheck_delay", "200ms",
                                         "backfill_throughput", "0MiB/s"));
        if (c.num() != 1)
            return;
        c.set("autonomic.enabled", true)
         .set("autonomic.dry_run", false)
         .set("autonomic.startup_grace", "1s")
         .set("autonomic.monitor_interval", "500ms")
         .set("autonomic.analyze_interval", "500ms")
         .set("autonomic.sampling.enabled", false)
         .set("autonomic.actuators", Map.of("partition_split", Map.of("enabled", true)));
    }

    /** The model: seq to v of the hot partition, and the rows of the other partitions. */
    private static final class Model
    {
        final ConcurrentSkipListMap<Integer, Integer> hot = new ConcurrentSkipListMap<>();
        /** Seqs with a write whose outcome is unknown (a timeout): either value is legal. */
        final Set<Integer> uncertain = new ConcurrentSkipListSet<>();
        final Map<Integer, Integer> others = new HashMap<>();
    }

    private static void await(String what, BooleanSupplier condition) throws InterruptedException
    {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(DEADLINE_MILLIS);
        while (!condition.getAsBoolean())
        {
            if (System.nanoTime() > deadline)
                throw new AssertionError("timed out waiting for " + what);
            Thread.sleep(100);
        }
    }

    private static String phase(IInvokableInstance node)
    {
        Object[][] rows = node.executeInternal("SELECT phase FROM system_views.autonomic_layouts WHERE keyspace_name = ?", KS);
        return rows.length == 0 ? null : (String) rows[0][0];
    }

    private static String overflowTable(IInvokableInstance node)
    {
        Object[][] rows = node.executeInternal("SELECT physical_table FROM system_views.autonomic_layouts WHERE keyspace_name = ?", KS);
        String name = (String) rows[0][0];
        return name.contains(".") ? name.substring(name.indexOf('.') + 1) : name;
    }

    /** One page of the hot partition, read on the node with a client paging state; returns {seqs, next state or null}. */
    private static Object[] page(IInvokableInstance node, byte[] state, int pageSize)
    {
        return node.callOnInstance(() -> {
            ClientState client = ClientState.forInternalCalls();
            SelectStatement select = (SelectStatement) QueryProcessor.getStatement("SELECT seq FROM " + KS + ".t WHERE id = " + HOT, client);
            QueryOptions options = QueryOptions.create(ConsistencyLevel.QUORUM, Collections.emptyList(), false, PageSize.inRows(pageSize),
                                                       state == null ? null : PagingState.deserialize(ByteBuffer.wrap(state), ProtocolVersion.CURRENT),
                                                       null, ProtocolVersion.CURRENT, KS);
            ResultMessage.Rows rows = select.execute(new QueryState(client), options, Dispatcher.RequestTime.forImmediateExecution());
            ArrayList<Integer> seqs = new ArrayList<>();
            for (List<byte[]> row : rows.result.rows)
                seqs.add(Int32Type.instance.compose(ByteBuffer.wrap(row.get(0))));
            PagingState next = rows.result.metadata.getPagingState();
            byte[] bytes = next == null ? null : ByteBufferUtil.getArray(next.serialize(ProtocolVersion.CURRENT));
            return new Object[]{ seqs, bytes };
        });
    }

    @Test
    public void splitAHotPartitionWhileClientsUseIt() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(3).withConfig(PartitionSplitEndToEndTest::configure).start()))
        {
            IInvokableInstance n1 = cluster.get(1);
            cluster.schemaChange("CREATE KEYSPACE " + KS + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 2}");
            cluster.schemaChange("CREATE TABLE " + KS + ".t (id int, seq int, v int, PRIMARY KEY (id, seq))");
            Model model = new Model();
            String insert = "INSERT INTO " + KS + ".t (id, seq, v) VALUES (?, ?, ?)";
            for (int start = 0; start < ROWS; start += 250)
            {
                StringBuilder batch = new StringBuilder("BEGIN UNLOGGED BATCH ");
                for (int seq = start; seq < start + 250; seq++)
                {
                    batch.append("INSERT INTO ").append(KS).append(".t (id, seq, v) VALUES (").append(HOT).append(", ").append(seq).append(", ").append(seq).append("); ");
                    model.hot.put(seq, seq);
                }
                cluster.coordinator(1).execute(batch.append("APPLY BATCH").toString(), org.apache.cassandra.distributed.api.ConsistencyLevel.ALL);
            }
            for (int id = 2; id <= 20; id++)
            {
                cluster.coordinator(1).execute(insert, org.apache.cassandra.distributed.api.ConsistencyLevel.ALL, id, 0, id);
                model.others.put(id, id);
            }

            await("the layer to be armed", () -> n1.nodetoolResult("autonomic", "status").getStdout().contains("Armed: true"));
            n1.nodetoolResult("autonomic", "cluster", "enable", "--yes").asserts().success();

            // the background workload, checked against the model
            AtomicBoolean stop = new AtomicBoolean();
            AtomicReference<String> mismatch = new AtomicReference<>();
            AtomicLong operations = new AtomicLong();
            AtomicLong failures = new AtomicLong();
            Thread workload = new Thread(() -> {
                Random random = new Random(7);
                org.apache.cassandra.distributed.api.ConsistencyLevel quorum = org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM;
                while (!stop.get() && mismatch.get() == null)
                {
                    int node = 1 + random.nextInt(3);
                    int op = random.nextInt(20);
                    int seq = random.nextInt(ROWS + 500);
                    try
                    {
                        if (op < 8)
                        {
                            int v = random.nextInt(1_000_000);
                            model.uncertain.add(seq);
                            cluster.coordinator(node).execute(insert, quorum, HOT, seq, v);
                            model.hot.put(seq, v);
                            model.uncertain.remove(seq);
                        }
                        else if (op < 14)
                        {
                            Object[][] rows = cluster.coordinator(node).execute("SELECT v FROM " + KS + ".t WHERE id = ? AND seq = ?", quorum, HOT, seq);
                            Integer expected = model.hot.get(seq);
                            Integer actual = rows.length == 0 ? null : (Integer) rows[0][0];
                            if (!model.uncertain.contains(seq) && !java.util.Objects.equals(expected, actual))
                                mismatch.compareAndSet(null, "point read of " + seq + " on node " + node + ": " + actual + ", model " + expected);
                        }
                        else if (op < 19)
                        {
                            int from = random.nextInt(ROWS);
                            Object[][] rows = cluster.coordinator(node).execute("SELECT seq, v FROM " + KS + ".t WHERE id = ? AND seq >= ? AND seq < ?",
                                                                                quorum, HOT, from, from + 40);
                            NavigableMap<Integer, Integer> expected = model.hot.subMap(from, true, from + 40, false);
                            TreeMap<Integer, Integer> actual = new TreeMap<>();
                            int previous = Integer.MIN_VALUE;
                            for (Object[] row : rows)
                            {
                                int s = (Integer) row[0];
                                if (s <= previous)
                                    mismatch.compareAndSet(null, "slice out of order at " + s + " on node " + node);
                                previous = s;
                                actual.put(s, (Integer) row[1]);
                            }
                            for (int s = from; s < from + 40; s++)
                                if (!model.uncertain.contains(s) && !java.util.Objects.equals(expected.get(s), actual.get(s)))
                                    mismatch.compareAndSet(null, "slice [" + from + ", " + (from + 40) + ") on node " + node + " at " + s + ": "
                                                                 + actual.get(s) + ", model " + expected.get(s));
                        }
                        else
                        {
                            // a scan of the whole table: every partition once, contiguously, with every row
                            Object[][] rows = cluster.coordinator(node).execute("SELECT id, seq FROM " + KS + ".t", quorum);
                            Map<Integer, Integer> counts = new HashMap<>();
                            Set<Integer> finished = new HashSet<>();
                            Integer current = null;
                            for (Object[] row : rows)
                            {
                                int id = (Integer) row[0];
                                if (current == null || current != id)
                                {
                                    if (current != null && !finished.add(current))
                                        mismatch.compareAndSet(null, "scan returned partition " + current + " twice");
                                    if (finished.contains(id))
                                        mismatch.compareAndSet(null, "scan returned partition " + id + " in two pieces");
                                    current = id;
                                }
                                counts.merge(id, 1, Integer::sum);
                            }
                            int hot = counts.getOrDefault(HOT, 0);
                            int known = model.hot.size();
                            if (hot < known - model.uncertain.size() || hot > known + model.uncertain.size())
                                mismatch.compareAndSet(null, "scan on node " + node + " returned " + hot + " rows of the hot partition, model " + known);
                            for (int id = 2; id <= 20; id++)
                                if (counts.getOrDefault(id, 0) != 1)
                                    mismatch.compareAndSet(null, "scan on node " + node + " returned " + counts.get(id) + " rows of partition " + id);
                        }
                        operations.incrementAndGet();
                    }
                    catch (Throwable t)
                    {
                        // a timeout around a phase change is allowed (contract D4); a write's outcome is then unknown
                        failures.incrementAndGet();
                    }
                }
            }, "split-workload");
            workload.setDaemon(true);
            workload.start();

            try
            {
                String keyHex = ByteBufferUtil.bytesToHex(Int32Type.instance.decompose(HOT));
                org.apache.cassandra.distributed.api.NodeToolResult split = n1.nodetoolResult("autonomic", "layout", "split", KS, "t", keyHex,
                                                                                              "--buckets", Integer.toString(BUCKETS));
                split.asserts().success();
                String out = split.getStdout();
                assertTrue(out, out.contains("state=PROPOSED"));
                String id = out.substring(out.indexOf("id=") + 3, out.indexOf("id=") + 3 + 36);
                n1.nodetoolResult("autonomic", "approve", id).asserts().success();

                // a paging state taken before CUTOVER
                await("BACKFILL or later", () -> phase(n1) != null && !"ANNOUNCED".equals(phase(n1)) && !"DUAL_WRITE".equals(phase(n1)));
                List<Integer> paged = new ArrayList<>();
                Object[] first = page(cluster.get(2), null, 100);
                @SuppressWarnings("unchecked") List<Integer> firstSeqs = (List<Integer>) first[0];
                paged.addAll(firstSeqs);
                byte[] state = (byte[]) first[1];
                String phaseAtFirstPage = phase(n1);

                // the commit point waits for the second approval; the claim's action id is the one to approve
                await("CUTOVER", () -> "CUTOVER".equals(phase(n1)));
                String status = n1.nodetoolResult("autonomic", "layout", "status").getStdout();
                assertTrue(status, status.contains("phase=CUTOVER"));
                await("the soak", () -> n1.nodetoolResult("autonomic", "layout", "status").getStdout().contains("waits for `nodetool autonomic approve"));
                n1.nodetoolResult("autonomic", "approve", id).asserts().success();
                await("STEADY", () -> "STEADY".equals(phase(n1)));
                // past the commit point an abort is refused
                org.apache.cassandra.distributed.api.NodeToolResult refused = n1.nodetoolResult("autonomic", "layout", "abort", KS, "t", keyHex);
                String abortRefusedLater = refused.getStdout() + refused.getStderr() + refused.getError();
                assertTrue(abortRefusedLater, abortRefusedLater.contains("unsplit"));
                await("the action to be committed", () -> n1.nodetoolResult("autonomic", "history").getStdout().contains("state=COMMITTED"));
                long before = operations.get();
                await("more workload after STEADY", () -> operations.get() > before + 50);

                // the paging state continues after STEADY
                while (state != null)
                {
                    Object[] next = page(cluster.get(3), state, 500);
                    @SuppressWarnings("unchecked") List<Integer> seqs = (List<Integer>) next[0];
                    paged.addAll(seqs);
                    state = (byte[]) next[1];
                }
                for (int i = 1; i < paged.size(); i++)
                    assertTrue("paged read out of order at " + i + " (first page in " + phaseAtFirstPage + ')', paged.get(i) > paged.get(i - 1));
                assertTrue("every row that existed before the read is in it", new HashSet<>(paged).containsAll(model.hot.headMap(ROWS).keySet()));

                // every phase was committed
                List<Map<String, String>> events = n1.callOnInstance(() -> AutonomicService.instance.getActionEvents(id));
                StringBuilder trail = new StringBuilder();
                for (Map<String, String> e : events)
                    trail.append(e).append('\n');
                for (String p : new String[]{ "ANNOUNCED", "DUAL_WRITE", "BACKFILL", "VERIFY", "CUTOVER", "NEW_ONLY", "CLEANUP", "STEADY" })
                    assertTrue(p + " is not in the audit trail:\n" + trail, trail.toString().contains("phase " + p + " "));
            }
            finally
            {
                stop.set(true);
                workload.join(30_000);
            }
            assertEquals(null, mismatch.get());
            assertTrue("the workload ran: " + operations.get() + " operations, " + failures.get() + " failures", operations.get() > 200);

            // STEADY keeps the base partition as residue until a reclaim deletes it (T6.17b, ADR-036; SplitReclaimTest),
            // so it is not checked here; the overflow table has the buckets, not all on the same nodes
            String overflow = overflowTable(n1);
            Map<Integer, Set<Integer>> holders = new TreeMap<>();
            for (int i = 1; i <= 3; i++)
            {
                IInvokableInstance node = cluster.get(i);
                for (Object[] row : node.executeInternal("SELECT DISTINCT id, \"__al_bucket\" FROM " + KS + '.' + overflow))
                    holders.computeIfAbsent((Integer) row[1], b -> new HashSet<>()).add(i);
            }
            assertEquals(holders.toString(), BUCKETS, holders.size());
            assertTrue("buckets spread over different replica sets: " + holders, new HashSet<>(holders.values()).size() > 1);
            // reads still see everything
            Object[][] all = cluster.coordinator(2).execute("SELECT seq, v FROM " + KS + ".t WHERE id = ?", org.apache.cassandra.distributed.api.ConsistencyLevel.QUORUM, HOT);
            int checked = 0;
            for (Object[] row : all)
                if (!model.uncertain.contains((Integer) row[0]))
                {
                    assertEquals("row " + row[0], model.hot.get((Integer) row[0]), row[1]);
                    checked++;
                }
            assertTrue(checked >= ROWS - model.uncertain.size());
        }
    }
}
