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
package org.apache.cassandra.autonomic.plan.policies;

import java.util.Map;

import org.apache.cassandra.autonomic.plan.VerifySpec;
import org.apache.cassandra.config.DurationSpec;

/** Baseline and observation windows of a policy's {@link org.apache.cassandra.autonomic.plan.VerifySpec}. */
public final class Windows
{
    public static final String BASELINE_KEY = "baseline_window";
    public static final String OBSERVATION_KEY = "observation_window";
    public static final long DEFAULT_BASELINE_MILLIS = 120_000;
    public static final long DEFAULT_OBSERVATION_MILLIS = 300_000;

    public final long baselineMillis;
    public final long observationMillis;
    /** Traffic floor of latency guards ({@code safety.guard_min_ops_per_sec}); travels with the windows into every spec. */
    public final double guardMinOpsPerSec;

    public Windows(long baselineMillis, long observationMillis)
    {
        this(baselineMillis, observationMillis, VerifySpec.DEFAULT_GUARD_MIN_OPS_PER_SEC);
    }

    public Windows(long baselineMillis, long observationMillis, double guardMinOpsPerSec)
    {
        this.baselineMillis = baselineMillis;
        this.observationMillis = observationMillis;
        this.guardMinOpsPerSec = guardMinOpsPerSec;
    }

    public Windows withGuardMinOps(double opsPerSec)
    {
        return new Windows(baselineMillis, observationMillis, opsPerSec);
    }

    public static Windows defaults()
    {
        return new Windows(DEFAULT_BASELINE_MILLIS, DEFAULT_OBSERVATION_MILLIS);
    }

    /** From the options of an actuator; each value is a duration such as {@code 2m} or a number of milliseconds. */
    public static Windows fromOptions(Map<String, Object> options)
    {
        return new Windows(millis(options, BASELINE_KEY, DEFAULT_BASELINE_MILLIS),
                           millis(options, OBSERVATION_KEY, DEFAULT_OBSERVATION_MILLIS));
    }

    private static long millis(Map<String, Object> options, String key, long def)
    {
        Object o = options == null ? null : options.get(key);
        if (o instanceof Number)
            return ((Number) o).longValue();
        return o == null ? def : new DurationSpec.LongMillisecondsBound(o.toString()).toMilliseconds();
    }
}
