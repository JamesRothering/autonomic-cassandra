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

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

import org.junit.Assume;
import org.junit.Test;

import org.apache.cassandra.autonomic.AutonomicService;
import org.apache.cassandra.autonomic.knowledge.Series;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.monitor.sensors.Signals;
import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.metrics.ClientRequestsMetricsHolder;

/**
 * SPIKE CODE, NOT A TEST OF PRODUCTION BEHAVIOUR (docs/spikes/guard-traffic-floor.md).
 *
 * One node with the layer in dry run and production sensor timing. A client writes through its coordinator at a few
 * Poisson rates, one after the other; for each rate, after a minute of settling, it records the layer's own
 * {@code client.write_latency_p99_micros} and {@code client.write_ops_per_sec} samples and the non-decaying bucket
 * counts of the write latency histogram over the same period. The file (SPIKE_GUARD_FLOOR_REAL_OUT, default
 * build/test/spike-guard-floor-real.txt) is read by {@code GuardTrafficFloorSpikeTest#compareWithRealNode}. Takes about
 * 13 minutes, so it is skipped unless SPIKE_GUARD_FLOOR is set.
 */
public class GuardTrafficFloorRealNodeSpikeTest extends TestBaseImpl
{
    private static final double[] RATES = { 2, 10, 50 };
    private static final long PHASE_MILLIS = 250_000;
    private static final long SETTLE_MILLIS = 60_000;
    private static final int KEYS = 10_000;

    @Test
    public void recordWriteP99AtLowRates() throws IOException
    {
        // spike-only switches, read from the environment on purpose
        Assume.assumeTrue("spike code, set SPIKE_GUARD_FLOOR to run", System.getenv("SPIKE_GUARD_FLOOR") != null); // checkstyle: suppress nearby 'blockSystemPropertyUsage'
        String path = System.getenv().getOrDefault("SPIKE_GUARD_FLOOR_REAL_OUT", "build/test/spike-guard-floor-real.txt"); // checkstyle: suppress nearby 'blockSystemPropertyUsage'
        try (Cluster cluster = init(Cluster.build(1)
                                           .withConfig(c -> c.set("autonomic.enabled", true)
                                                             .set("autonomic.dry_run", true)
                                                             .set("autonomic.startup_grace", "1s")
                                                             .set("autonomic.sampling.enabled", false))
                                           .start());
             PrintWriter out = new PrintWriter(new FileWriter(path, false)))
        {
            cluster.schemaChange("CREATE TABLE " + KEYSPACE + ".t (id int PRIMARY KEY, v int)");
            Random random = new Random(42);
            for (double rate : RATES)
            {
                long phaseStart = System.currentTimeMillis();
                long[] bucketsAtStart = null;
                long analysedFrom = 0;
                long next = System.nanoTime();
                while (System.currentTimeMillis() - phaseStart < PHASE_MILLIS)
                {
                    if (bucketsAtStart == null && System.currentTimeMillis() - phaseStart >= SETTLE_MILLIS)
                    {
                        bucketsAtStart = buckets(cluster);
                        analysedFrom = System.currentTimeMillis();
                    }
                    next += (long) (-Math.log(1 - random.nextDouble()) / rate * 1e9);
                    long wait = next - System.nanoTime();
                    if (wait > 0)
                        LockSupport.parkNanos(wait);
                    cluster.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".t (id, v) VALUES (?, ?)", ConsistencyLevel.ONE,
                                                   random.nextInt(KEYS), random.nextInt());
                }
                long lookback = System.currentTimeMillis() - analysedFrom;
                long[] bucketsAtEnd = buckets(cluster);
                long[] delta = new long[bucketsAtEnd.length];
                for (int i = 0; i < delta.length; i++)
                    delta[i] = bucketsAtEnd[i] - bucketsAtStart[i];
                out.println("rate " + rate);
                out.println("p99 " + join(series(cluster, Signals.CLIENT_WRITE_LATENCY_P99_MICROS, lookback)));
                out.println("ops " + join(series(cluster, Signals.CLIENT_WRITE_OPS_PER_SEC, lookback)));
                StringBuilder b = new StringBuilder("buckets");
                for (long d : delta)
                    b.append(' ').append(d);
                out.println(b);
                out.flush();
                System.out.printf("rate %s done in %d s%n", rate, TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis() - phaseStart));
            }
        }
    }

    private static long[] buckets(Cluster cluster)
    {
        return cluster.get(1).callOnInstance(() -> ClientRequestsMetricsHolder.writeMetrics.latency.getSnapshot().getValues());
    }

    private static double[] series(Cluster cluster, SignalId signal, long lookbackMillis)
    {
        String name = signal.name;
        return cluster.get(1).callOnInstance(() -> {
            Series s = AutonomicService.instance.knowledge().series(SignalId.of(name), Subject.node(), lookbackMillis);
            return s.values.clone();
        });
    }

    private static String join(double[] values)
    {
        StringBuilder b = new StringBuilder();
        for (double v : values)
            b.append(b.length() == 0 ? "" : " ").append(v);
        return b.toString();
    }
}
