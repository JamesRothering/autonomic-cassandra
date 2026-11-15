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
package org.apache.cassandra.distributed.test.autonomic;

import java.util.List;
import java.util.Map;

import org.junit.Test;

import org.apache.cassandra.autonomic.AutonomicService;
import org.apache.cassandra.autonomic.monitor.InjectedSensor;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.monitor.sensors.Signals;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.IInstanceConfig;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.service.StorageService;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * D5 request pool elasticity on one node: while an injected {@code pool.pending_tasks} stays above the threshold the
 * layer grows the {@code ReadStage} pool step by step up to twice its configured size and never beyond; when the
 * queue is gone it steps the pool back to the configured size. The pool sensor is replaced by an injected one whose
 * queue depends on the live pool size, so each growth step verifies as an improvement.
 */
public class RequestPoolElasticityTest extends TestBaseImpl
{
    private static final long DEADLINE_MILLIS = 60_000;
    private static final int CONFIGURED = 4;
    private static InjectedSensor injected;

    private static void fast(IInstanceConfig c)
    {
        c.set("concurrent_reads", CONFIGURED)
         .set("autonomic.enabled", true)
         .set("autonomic.dry_run", false)
         .set("autonomic.startup_grace", "1s")
         .set("autonomic.monitor_interval", "500ms")
         .set("autonomic.analyze_interval", "500ms")
         .set("autonomic.sampling.enabled", false)
         .set("autonomic.safety.cooldown_after_commit", "1s")
         .set("autonomic.safety.max_actions_per_hour", 100)
         .set("autonomic.safety.max_actions_per_actuator_per_hour", 100)
         .set("autonomic.detectors", Map.of("pool_saturation", Map.of("pending_tasks", 10, "for_evaluations", 3)))
         .set("autonomic.actuators", Map.of("read_pool", Map.of("enabled", true, "baseline_window", "3s", "observation_window", "3s")));
    }

    private static int readPoolSize(Cluster cluster)
    {
        return cluster.get(1).callOnInstance(() -> StorageService.instance.getConcurrency(List.of("ReadStage")).get("ReadStage").get(1));
    }

    private static String actions(Cluster cluster)
    {
        return java.util.Arrays.deepToString(cluster.coordinator(1).execute("SELECT state, state_reason, verdict, params, baseline, result FROM system_autonomic.actions "
                                                                            + "WHERE actuator = 'read_pool' ALLOW FILTERING", ConsistencyLevel.ONE));
    }

    private static void push(Cluster cluster, double pending)
    {
        cluster.get(1).runOnInstance(() -> {
            Subject node = Subject.node();
            injected.push(Signals.POOL_PENDING_TASKS, Subject.threadPool("ReadStage"), pending)
                    .push(Signals.OS_LOAD_PER_PROCESSOR, node, 0.2)
                    .push(Signals.CLIENT_READ_LATENCY_P99_MICROS, node, 1000)
                    .push(Signals.CLIENT_WRITE_LATENCY_P99_MICROS, node, 1000);
        });
    }

    @Test
    public void poolsGrowInsideTheEnvelopeAndReturnOnRecovery() throws Throwable
    {
        try (Cluster cluster = init(Cluster.build(1).withConfig(RequestPoolElasticityTest::fast).start()))
        {
            cluster.get(1).runOnInstance(() -> {
                AutonomicService.instance.removeSensor("thread_pool");
                AutonomicService.instance.removeSensor("client_request");
                AutonomicService.instance.removeSensor("cpu");
                injected = new InjectedSensor();
                AutonomicService.instance.registerSensor(injected);
            });
            assertEquals(CONFIGURED, readPoolSize(cluster));
            cluster.get(1).nodetoolResult("autonomic", "actuators").asserts().success().stdoutContains("read_pool")
                   .stdoutContains("mutation_pool").stdoutContains("counter_mutation_pool").stdoutContains("view_mutation_pool");

            // saturation: the queue shrinks as the pool grows, 100 tasks at the configured size, falling with the cube of the size
            int highest = CONFIGURED;
            long end = System.currentTimeMillis() + DEADLINE_MILLIS;
            while (System.currentTimeMillis() < end && highest < 2 * CONFIGURED)
            {
                int size = readPoolSize(cluster);
                assertTrue("pool size " + size + " is outside the envelope", size >= CONFIGURED / 2 && size <= 2 * CONFIGURED);
                highest = Math.max(highest, size);
                push(cluster, 100.0 * Math.pow((double) CONFIGURED / size, 3));
                Thread.sleep(500);
            }
            assertEquals("the pool did not reach the top of its envelope: " + actions(cluster), 2 * CONFIGURED, highest);
            // saturation goes on at the top: the size stays inside the envelope
            for (int i = 0; i < 12; i++)
            {
                push(cluster, 12);
                Thread.sleep(500);
                assertEquals(2 * CONFIGURED, readPoolSize(cluster));
            }

            // recovery: no queue, the pool returns to the configured size
            end = System.currentTimeMillis() + DEADLINE_MILLIS;
            while (System.currentTimeMillis() < end && readPoolSize(cluster) != CONFIGURED)
            {
                assertTrue(readPoolSize(cluster) >= CONFIGURED);
                push(cluster, 0);
                Thread.sleep(500);
            }
            assertEquals("the pool did not return to its configured size: " + actions(cluster), CONFIGURED, readPoolSize(cluster));

            int committed = 0;
            for (Object[] row : cluster.coordinator(1).execute("SELECT id, state, params FROM system_autonomic.actions WHERE actuator = 'read_pool' ALLOW FILTERING",
                                                               ConsistencyLevel.ONE))
            {
                @SuppressWarnings("unchecked")
                int target = Integer.parseInt(((Map<String, String>) row[2]).get("value"));
                assertTrue("target " + target + " outside the envelope", target >= CONFIGURED / 2 && target <= 2 * CONFIGURED);
                if ("COMMITTED".equals(row[1]))
                    committed++;
            }
            assertTrue("committed actions: " + committed, committed >= 3);
        }
    }
}
