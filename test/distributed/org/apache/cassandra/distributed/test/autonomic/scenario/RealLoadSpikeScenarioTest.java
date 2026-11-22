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
package org.apache.cassandra.distributed.test.autonomic.scenario;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import net.bytebuddy.ByteBuddy;
import net.bytebuddy.dynamic.loading.ClassLoadingStrategy;
import net.bytebuddy.implementation.MethodDelegation;
import net.bytebuddy.implementation.bind.annotation.SuperCall;
import net.bytebuddy.implementation.bind.annotation.This;

import org.junit.Test;

import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.FBUtilities;

import static net.bytebuddy.matcher.ElementMatchers.named;
import static net.bytebuddy.matcher.ElementMatchers.takesArguments;
import static org.apache.cassandra.distributed.test.autonomic.scenario.ScenarioSupport.actions;
import static org.apache.cassandra.distributed.test.autonomic.scenario.ScenarioSupport.await;
import static org.apache.cassandra.distributed.test.autonomic.scenario.ScenarioSupport.describe;
import static org.apache.cassandra.distributed.test.autonomic.scenario.ScenarioSupport.diagnoses;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * D7 scenario, load spike driven by real client load (T7.9): nothing is injected into the layer. A client sends reads
 * through the coordinator of node 1 at a fixed rate, above what the four {@code ReadStage} threads can serve, so the
 * stage's queue grows and the coordinator latency passes its SLO. The real sensors ({@code thread_pool},
 * {@code client_request}, {@code cpu}) see it and the whole ladder of overview 8.1 runs on what they report.
 *
 * <p>Reads of the test's table take 20 ms on the stage (a ByteBuddy delay of the read itself, as an I/O bound read
 * would; the layer's own reads are not slowed), so the stage's capacity is {@code threads / 20 ms}: 200 reads a second
 * with 4 threads, 250 with 5. The load of 230 reads a second is just above the first and below the second. Asserts the
 * {@code pool_saturation} diagnosis, the action ({@code read_pool} grown by a step inside its envelope and committed
 * with the verdict {@code IMPROVED}) and, once the load stops, the pool back at its configured size. Node 2 is not
 * touched.
 *
 * <p>The rate limiter is not part of this scenario: it sits in the native transport, which the in-JVM coordinator
 * bypasses, so a limit would not slow this client and its verdict could not be judged by real load. The injected
 * {@link LoadSpikeScenarioTest} covers the throttle step.
 */
public class RealLoadSpikeScenarioTest extends TestBaseImpl
{
    private static final String KS = "scn_load";
    private static final int CONFIGURED = 4;
    private static final int RATE = 230;
    private static final int KEYS = 200;
    /**
     * Evaluations the saturation must last before it is diagnosed: one per sample of the sensors, so about 20 s. A shorter spike is not worth
     * acting on, and it lets the p99 of the coordinator reach its plateau. Acted on at the first sample, the baseline
     * of the verification holds the ramp, a p99 well below the plateau, and the growth of the pool, which only
     * keeps the p99 where it is, is aborted early as a regression of its guard (found by this scenario).
     */
    private static final int SUSTAINED = 4;
    /**
     * The most requests the client has in flight, like the connections of a real client; what it cannot send is not
     * sent. With the stage saturated the queue grows to this bound and stays there, so the coordinator's p99 reaches
     * a plateau before the layer acts (while a queue grows, the p99 rises with it, and the verification of any
     * action would compare the rising p99 with an earlier, lower one).
     */
    private static final int MAX_IN_FLIGHT = 40;

    /** Makes a read of {@link #KS} take {@code READ_MILLIS} on its stage, once enabled. */
    public static class SlowRead
    {
        static final AtomicBoolean enabled = new AtomicBoolean();
        static final int READ_MILLIS = 20;

        static void install(ClassLoader cl, int nodeNumber)
        {
            new ByteBuddy().rebase(ReadCommand.class)
                           .method(named("executeLocally").and(takesArguments(2)))
                           .intercept(MethodDelegation.to(SlowRead.class))
                           .make()
                           .load(cl, ClassLoadingStrategy.Default.INJECTION);
        }

        @SuppressWarnings("unused")
        public static UnfilteredPartitionIterator executeLocally(@This ReadCommand command, @SuperCall Callable<UnfilteredPartitionIterator> zuper) throws Exception
        {
            if (enabled.get() && KS.equals(command.metadata().keyspace))
                TimeUnit.MILLISECONDS.sleep(READ_MILLIS);
            return zuper.call();
        }
    }

    private static void fast(IInstanceConfig c)
    {
        c.set("concurrent_reads", CONFIGURED)
         .set("read_request_timeout", "10000ms")
         .set("autonomic.enabled", true)
         .set("autonomic.dry_run", false)
         .set("autonomic.startup_grace", "1s")
         .set("autonomic.monitor_interval", "500ms")
         .set("autonomic.analyze_interval", "500ms")
         .set("autonomic.sampling.enabled", false)
         .set("autonomic.safety.cooldown_after_commit", "1s")
         .set("autonomic.safety.max_actions_per_hour", 100)
         .set("autonomic.safety.max_actions_per_actuator_per_hour", 100)
         .set("autonomic.detectors", Map.of("pool_saturation", Map.of("pending_tasks", 10, "for_evaluations", SUSTAINED),
                                            "latency_slo_breach", Map.of("for_evaluations", 2)))
         // the sensors report every 5 s, so a window holds its three samples (MetricVerifier.MIN_SAMPLES) from 16 s
         .set("autonomic.actuators", Map.of("read_pool", Map.of("enabled", true, "baseline_window", "17s", "observation_window", "17s")));
    }

    private static int readPool(Cluster cluster, int node)
    {
        return cluster.get(node).callOnInstance(() -> StorageService.instance.getConcurrency(List.of("ReadStage")).get("ReadStage").get(1));
    }

    private static Object[] committedImproved(Cluster cluster)
    {
        for (Object[] row : actions(cluster, 1, "read_pool"))
            if ("COMMITTED".equals(row[1]) && "IMPROVED".equals(row[3]))
                return row;
        return null;
    }

    @Test
    public void aRealReadSpikeGrowsTheReadPoolAndTheLayerRecovers() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(2).withConfig(RealLoadSpikeScenarioTest::fast).withInstanceInitializer((cl, n) -> SlowRead.install(cl, n)).start()))
        {
            cluster.schemaChange("CREATE KEYSPACE " + KS + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
            cluster.schemaChange("CREATE TABLE " + KS + ".t (id int PRIMARY KEY, v int)");
            // the keys node 1 owns, so that the whole spike lands on its stage
            int[] owned = cluster.get(1).callOnInstance(() -> {
                List<Integer> ids = new ArrayList<>();
                for (int id = 0; id < 5 * KEYS && ids.size() < KEYS; id++)
                    if (StorageService.instance.getNaturalEndpoints(KS, "t", String.valueOf(id)).contains(FBUtilities.getBroadcastAddressAndPort().getAddress()))
                        ids.add(id);
                return ids.stream().mapToInt(Integer::intValue).toArray();
            });
            assertEquals(KEYS, owned.length);
            for (int id : owned)
                cluster.coordinator(1).execute("INSERT INTO " + KS + ".t (id, v) VALUES (?, ?)", ConsistencyLevel.ONE, id, id);
            assertEquals(CONFIGURED, readPool(cluster, 1));
            cluster.forEach(i -> i.runOnInstance(() -> SlowRead.enabled.set(true)));

            // the spike: RATE reads a second through node 1, whatever the stage can do (open loop, at most MAX_IN_FLIGHT outstanding; the rest is dropped)
            AtomicBoolean stop = new AtomicBoolean();
            AtomicInteger inFlight = new AtomicInteger();
            AtomicLong sent = new AtomicLong();
            AtomicLong failed = new AtomicLong();
            Thread client = new Thread(() -> {
                long start = System.nanoTime();
                while (!stop.get())
                {
                    long due = (System.nanoTime() - start) * RATE / 1_000_000_000L;
                    for (long n = sent.get(); n < due; n = sent.incrementAndGet())
                    {
                        if (inFlight.get() >= MAX_IN_FLIGHT)
                        {
                            sent.set(due);
                            break;
                        }
                        inFlight.incrementAndGet();
                        cluster.coordinator(1).executeWithResult((result, error) -> {
                            inFlight.decrementAndGet();
                            if (error != null)
                                failed.incrementAndGet();
                        }, "SELECT v FROM " + KS + ".t WHERE id = ?", ConsistencyLevel.ONE, owned[(int) (n % KEYS)]);
                    }
                    try
                    {
                        Thread.sleep(5);
                    }
                    catch (InterruptedException e)
                    {
                        return;
                    }
                }
            }, "real-load-spike-client");
            client.start();

            Object[] grown = null;
            int highest = CONFIGURED;
            try
            {
                long end = System.currentTimeMillis() + 2 * ScenarioSupport.DEADLINE_MILLIS;
                while (System.currentTimeMillis() < end && grown == null)
                {
                    Thread.sleep(500);
                    int size = readPool(cluster, 1);
                    assertTrue("pool size " + size + " is outside the envelope", size >= CONFIGURED / 2 && size <= 2 * CONFIGURED);
                    highest = Math.max(highest, size);
                    for (Object[] row : actions(cluster, 1, "read_pool"))
                        assertTrue("action failed or rolled back: " + describe(actions(cluster, 1, "read_pool")),
                                   !List.of("FAILED", "ROLLED_BACK", "ABORTED").contains(String.valueOf(row[1])));
                    grown = committedImproved(cluster);
                }
            }
            finally
            {
                stop.set(true);
                client.join(30_000);
            }
            String trail = "read_pool " + describe(actions(cluster, 1, "read_pool")) + "\nsent " + sent + " failed " + failed;
            assertTrue("no improving committed read_pool action:\n" + trail, grown != null);
            assertTrue("the pool did not grow:\n" + trail, highest > CONFIGURED);
            @SuppressWarnings("unchecked")
            Map<String, String> params = (Map<String, String>) grown[4];
            assertTrue("the action is not a read_pool step: " + params, Double.parseDouble(params.get("value")) > CONFIGURED);

            // diagnoses of the spike, from the real sensors
            assertTrue("no pool_saturation diagnosis of ReadStage: " + describe(diagnoses(cluster, 1, "pool_saturation")),
                       Arrays.stream(diagnoses(cluster, 1, "pool_saturation")).anyMatch(r -> String.valueOf(r[0]).contains("ReadStage")));
            assertTrue("no latency_slo_breach diagnosis: " + describe(diagnoses(cluster, 1, "latency_slo_breach")), diagnoses(cluster, 1, "latency_slo_breach").length > 0);

            // the spike is over: the pool returns to its configured size
            await("the pool to return to " + CONFIGURED + ":\n" + describe(actions(cluster, 1, "read_pool")), () -> readPool(cluster, 1) == CONFIGURED);

            // node 2 saw none of it
            assertEquals(CONFIGURED, readPool(cluster, 2));
            assertEquals(0, actions(cluster, 2, "read_pool").length);
        }
    }
}
