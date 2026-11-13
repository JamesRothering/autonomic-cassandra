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
package org.apache.cassandra.autonomic.monitor.sensors;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Predicate;

import org.apache.cassandra.autonomic.monitor.Observation;
import org.apache.cassandra.autonomic.monitor.ObservationSink;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;

/** Helpers shared by the sensors. Not thread-safe: sensors are collected on the single monitor thread. */
final class SensorSupport
{
    private SensorSupport()
    {
    }

    /** Emits the value unless it is NaN or infinite. */
    static void emit(ObservationSink sink, SignalId signal, Subject subject, long now, double value)
    {
        if (Double.isNaN(value) || Double.isInfinite(value))
            return;
        sink.accept(new Observation(signal, subject, now, value));
    }

    /** Per-key counter baselines, to turn monotonically increasing counters into deltas. */
    static final class Deltas
    {
        private final Map<String, Long> previous = new HashMap<>();

        /**
         * Returns the increase of the counter since the previous call for this key, or -1 when there is no usable
         * baseline (first sight, or the counter went backwards, which makes the new value the baseline).
         */
        long delta(String key, long current)
        {
            Long prev = previous.put(key, current);
            if (prev == null || current < prev)
                return -1;
            return current - prev;
        }

        /** Forgets the baselines of keys the predicate rejects. */
        void retainWhere(Predicate<String> keep)
        {
            previous.keySet().removeIf(k -> !keep.test(k));
        }
    }

    /** Per second rate of a delta over an elapsed time; negative delta (no baseline) gives NaN. */
    static double perSec(long delta, long elapsedMillis)
    {
        if (delta < 0 || elapsedMillis <= 0)
            return Double.NaN;
        return delta * 1000.0 / elapsedMillis;
    }
}
