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
package org.apache.cassandra.autonomic.knowledge;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalDouble;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.cassandra.autonomic.monitor.Observation;
import org.apache.cassandra.autonomic.monitor.ObservationSink;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;

/**
 * In-memory ring buffers per (signal, subject). At most {@code maxSeries} series exist; when a new series would
 * exceed the cap the least recently updated one is evicted and counted. Written by the monitor thread, read
 * concurrently by the control thread; each series is guarded by its own monitor.
 */
public class ObservationStore implements ObservationSink
{
    private static final class Key
    {
        final SignalId signal;
        final Subject subject;

        Key(SignalId signal, Subject subject)
        {
            this.signal = signal;
            this.subject = subject;
        }

        @Override
        public boolean equals(Object o)
        {
            return o instanceof Key && ((Key) o).signal.equals(signal) && ((Key) o).subject.equals(subject);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(signal, subject);
        }
    }

    private static final class Ring
    {
        final long[] times;
        final double[] values;
        int head; // index of oldest
        int size;
        volatile long lastUpdateSeq;

        Ring(int capacity)
        {
            times = new long[capacity];
            values = new double[capacity];
        }

        synchronized void add(long time, double value)
        {
            int idx;
            if (size < times.length)
            {
                idx = (head + size++) % times.length;
            }
            else
            {
                idx = head;
                head = (head + 1) % times.length;
            }
            times[idx] = time;
            values[idx] = value;
        }

        synchronized OptionalDouble latest()
        {
            return size == 0 ? OptionalDouble.empty() : OptionalDouble.of(values[(head + size - 1) % times.length]);
        }

        synchronized Series since(long minTime)
        {
            long[] t = new long[size];
            double[] v = new double[size];
            int n = 0;
            for (int i = 0; i < size; i++)
            {
                int idx = (head + i) % times.length;
                if (times[idx] >= minTime)
                {
                    t[n] = times[idx];
                    v[n++] = values[idx];
                }
            }
            return new Series(java.util.Arrays.copyOf(t, n), java.util.Arrays.copyOf(v, n));
        }

        synchronized SeriesSummary summary(SignalId signal, Subject subject)
        {
            if (size == 0)
                return null;
            int idx = (head + size - 1) % times.length;
            return new SeriesSummary(signal, subject, times[idx], values[idx], size);
        }
    }

    /** The newest point of one series and the number of points it holds. */
    public static final class SeriesSummary
    {
        public final SignalId signal;
        public final Subject subject;
        public final long atMillis;
        public final double value;
        public final int samples;

        SeriesSummary(SignalId signal, Subject subject, long atMillis, double value, int samples)
        {
            this.signal = signal;
            this.subject = subject;
            this.atMillis = atMillis;
            this.value = value;
            this.samples = samples;
        }
    }

    private final int capacity;
    private final int maxSeries;
    private final Map<Key, Ring> series = new ConcurrentHashMap<>();
    private final AtomicLong sequence = new AtomicLong();
    private final AtomicLong evictions = new AtomicLong();

    public ObservationStore(int capacityPerSeries, int maxSeries)
    {
        if (capacityPerSeries < 1 || maxSeries < 1)
            throw new IllegalArgumentException("capacity and max_series must be positive");
        this.capacity = capacityPerSeries;
        this.maxSeries = maxSeries;
    }

    /** Single writer (the monitor thread). */
    @Override
    public void accept(Observation o)
    {
        Key key = new Key(o.signal, o.subject);
        Ring ring = series.get(key);
        if (ring == null)
        {
            while (series.size() >= maxSeries)
                if (!evictOldest())
                    break;
            ring = new Ring(capacity);
            series.put(key, ring);
        }
        ring.add(o.atMillis, o.value);
        ring.lastUpdateSeq = sequence.incrementAndGet();
    }

    private boolean evictOldest()
    {
        Key oldest = null;
        long oldestSeq = Long.MAX_VALUE;
        for (Map.Entry<Key, Ring> e : series.entrySet())
        {
            long s = e.getValue().lastUpdateSeq;
            if (s < oldestSeq)
            {
                oldestSeq = s;
                oldest = e.getKey();
            }
        }
        if (oldest == null)
            return false;
        if (series.remove(oldest) != null)
            evictions.incrementAndGet();
        return true;
    }

    public OptionalDouble latest(SignalId signal, Subject subject)
    {
        Ring ring = series.get(new Key(signal, subject));
        return ring == null ? OptionalDouble.empty() : ring.latest();
    }

    /** Points with {@code time >= minTimeMillis}. */
    public Series series(SignalId signal, Subject subject, long minTimeMillis)
    {
        Ring ring = series.get(new Key(signal, subject));
        return ring == null ? Series.EMPTY : ring.since(minTimeMillis);
    }

    public List<Subject> subjects(SignalId signal)
    {
        List<Subject> result = new ArrayList<>();
        for (Key k : series.keySet())
            if (k.signal.equals(signal))
                result.add(k.subject);
        return result;
    }

    /** The newest point and point count of every non-empty series. */
    public List<SeriesSummary> summaries()
    {
        List<SeriesSummary> out = new ArrayList<>();
        for (Map.Entry<Key, Ring> e : series.entrySet())
        {
            SeriesSummary s = e.getValue().summary(e.getKey().signal, e.getKey().subject);
            if (s != null)
                out.add(s);
        }
        return out;
    }

    public int seriesCount()
    {
        return series.size();
    }

    /** Series evicted because of {@code max_series}; exported as {@code autonomic.series_evicted}. */
    public long evictions()
    {
        return evictions.get();
    }
}
