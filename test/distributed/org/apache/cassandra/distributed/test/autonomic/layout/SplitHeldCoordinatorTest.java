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

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.junit.Test;

import org.apache.cassandra.autonomic.layout.EpochWatermarks;
import org.apache.cassandra.autonomic.layout.LayoutWrites;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.shared.ClusterUtils;
import org.apache.cassandra.tcm.Epoch;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * T6.13, design 4.11 "stale coordinator", rules O1 and C1 with the real actuator, three nodes, replication factor 3,
 * owner node 1. The key is held in CUTOVER by {@code commit_approval: manual}. Node 3 starts a write of the key and is
 * paused inside it, after its first copy (the base table, class C2), by the test seam
 * {@link LayoutWrites#betweenCopiesForTesting}; then its log delivery is held back with a message filter
 * ({@link LayoutCompatibilitySuite}) and the commit point is approved.
 *
 * <ul>
 * <li>The owner commits NEW_ONLY; node 3 still sees CUTOVER. The owner must not advance further while node 3 has not
 * acknowledged the epoch: it stays in NEW_ONLY for 25 poll intervals, and the watermark is false. Meanwhile both
 * coordinators serve the workload (classes C2 and C3 are adjacent) and every check passes.</li>
 * <li>The filter is lifted and node 3 catches up; the paused write then finishes its second copy, sees that the
 * key's routing changed and executes again under C3 (ADR-027): {@link LayoutWrites#reexecutions} grows and the write
 * is acknowledged.</li>
 * <li>The migration then finishes.</li>
 * </ul>
 * The write timeout is 20 s so that the paused write's re-execution, whose deadline counts from the request's start,
 * is not cut short.
 */
public class SplitHeldCoordinatorTest extends SplitFailureSupport
{
    private static final int K = 1;
    private static final int ROWS = 200;
    /** On node 3: 0 armed, 1 paused in the seam, 2 released. */
    static volatile int seam;

    @Test
    public void ownerHoldsWhileACoordinatorIsBehindAndThePausedWriteExecutesAgain() throws Throwable
    {
        try (Cluster cluster = start(3, config(nodes(1), settings("commit_approval", "manual"),
                                               settings("write_request_timeout", "20000ms"))))
        {
            IInvokableInstance n1 = cluster.get(1);
            IInvokableInstance n3 = cluster.get(3);
            String table = "held";
            createTables(cluster, 3, table);
            arm(n1);
            Model model = new Model(cluster, table, K, 1, 2, 3);
            model.initialData(ROWS);
            model.startRecordingPhases();
            Workload workload = new Workload(model, ROWS, 5);
            try
            {
                String id = split(n1, table, K, 4);
                awaitPhase(n1, table, K, "CUTOVER");
                await("the commit point waits for approval", () -> layoutStatus(n1).contains("waits for `nodetool autonomic approve"));
                workload.awaitAcked(20);
                model.check("held, CUTOVER");

                // ---- node 3 pauses inside a write, after the base copy ----
                workload.pause();
                n3.runOnInstance(() -> {
                    seam = 0;
                    LayoutWrites.betweenCopiesForTesting = () -> {
                        if (seam != 0)
                            return;
                        seam = 1;
                        long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
                        while (seam == 1 && System.nanoTime() < end)
                            sleep(5);
                    };
                });
                long reexecutedBefore = n3.callOnInstance(() -> LayoutWrites.reexecutions.get());
                int c = 1001;
                long ts = model.begin(c, 4242);
                CompletableFuture<Throwable> paused = CompletableFuture.supplyAsync(() -> {
                    try
                    {
                        cluster.coordinator(3).execute(model.statement(c, 4242, ts), ConsistencyLevel.QUORUM);
                        return null;
                    }
                    catch (Throwable t)
                    {
                        return t;
                    }
                });
                await("node 3 to pause inside the write", () -> n3.callOnInstance(() -> seam == 1));
                workload.resume();

                // ---- node 3 is held at CUTOVER while the owner commits NEW_ONLY ----
                holdLog(cluster, 3);
                n1.nodetoolResult("autonomic", "approve", id).asserts().success();
                awaitPhase(n1, table, K, "NEW_ONLY");
                assertEquals("node 3 is one class behind", "CUTOVER", phaseOf(n3, table, K));
                long newOnly = phaseEpoch(n1, table, K);
                assertFalse("O1: node 3 has not acknowledged NEW_ONLY", n1.callOnInstance(() -> EpochWatermarks.allAtLeast(Epoch.create(newOnly))));
                assertStays("O1: the owner does not leave NEW_ONLY while node 3 is behind", 25,
                            () -> "NEW_ONLY".equals(phaseOf(n1, table, K)) && "CUTOVER".equals(phaseOf(n3, table, K)));
                workload.awaitAcked(10);
                model.check("held, NEW_ONLY with node 3 at CUTOVER");

                // ---- release: node 3 catches up, then the paused write goes on ----
                cluster.filters().reset();
                await("node 3 to catch up", () -> {
                    long target = ClusterUtils.getCurrentEpoch(n1).getEpoch();
                    return n1.callOnInstance(() -> EpochWatermarks.allAtLeast(Epoch.create(target)))
                           && ClusterUtils.getCurrentEpoch(n3).getEpoch() >= target;
                });
                assertFalse("node 3 has left CUTOVER", "CUTOVER".equals(phaseOf(n3, table, K)));
                assertFalse("the write is still paused", paused.isDone());
                n3.runOnInstance(() -> seam = 2);
                Throwable outcome = paused.get(60, TimeUnit.SECONDS);
                long reexecuted = n3.callOnInstance(() -> LayoutWrites.reexecutions.get()) - reexecutedBefore;
                System.out.println("T6.13 held: the paused write ended with " + outcome + " after " + reexecuted + " re-executions; node 3 sees "
                                   + phaseOf(n3, table, K));
                assertEquals("the paused write is acknowledged", null, outcome);
                assertTrue("C1: the write was executed again under the new routing", reexecuted >= 1);
                model.acknowledged(c, 4242, ts);
                n3.runOnInstance(() -> LayoutWrites.betweenCopiesForTesting = null);
                for (int node = 1; node <= 3; node++)
                    assertEquals(Integer.valueOf(4242), readV(cluster, node, table, K, c));
                model.check("held, after the release");

                awaitPhase(n1, table, K, "STEADY");
                await("the action committed", () -> history(n1).contains("state=COMMITTED"));
                workload.awaitAcked(20);
                model.check("held, STEADY");
                System.out.println("T6.13 held: trail\n" + trail(n1, id));
            }
            finally
            {
                workload.stop();
                model.stopRecordingPhases();
            }
            model.check("held, end");
            System.out.println("T6.13 held: phases seen " + model.observedPhases() + "; acknowledged " + model.acked + ", failed " + model.failed
                               + ", unknown outcomes left " + model.uncertainCount() + "; workload: " + workload);
        }
    }
}
