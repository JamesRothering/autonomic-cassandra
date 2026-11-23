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
package org.apache.cassandra.autonomic.analyze.detectors;

import java.util.ArrayDeque;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.Set;

import org.apache.cassandra.autonomic.analyze.ThresholdDetector;
import org.apache.cassandra.autonomic.knowledge.KnowledgeView;
import org.apache.cassandra.autonomic.knowledge.Series;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.monitor.sensors.Signals;

/**
 * A cache's hit rate is at least {@code drop} (default 0.2, points of hit rate) below its own long-run level
 * (ADR-046). The long-run level is the mean of the cache's hit rate over the last {@code reference_window_ms}
 * (default 1 hour), kept by the detector itself because the observation store keeps less. A point that breaches is
 * not added to it, so a sustained drop does not become its own reference: during a drop the reference is the mean
 * of the last window of non-breaching history, ending at the last non-breaching point, and it is not aged out while
 * the drop lasts. After {@code relearn_after_ms} (default 24 hours) of continuous breach, breaching points are added
 * again, so the new level becomes the reference and the diagnosis clears. There is no reference, and so no
 * diagnosis, until the detector has seen a window's worth of history (a restart starts over).
 *
 * <p>It is built on {@link ThresholdDetector}: the measure is the drop (reference minus current hit rate) of the
 * cache with the largest one, so {@code for_evaluations}, clearing and {@code breach_since} are inherited. There is
 * one diagnosis on the node, which names that cache in its evidence.
 */
public class CacheMissPressureDetector extends ThresholdDetector
{
    public static final String NAME = "cache_miss_pressure";
    public static final String THRESHOLD_KEY = "drop";
    public static final String REFERENCE_WINDOW_KEY = "reference_window_ms";
    public static final String RELEARN_AFTER_KEY = "relearn_after_ms";
    public static final double DEFAULT_DROP = 0.2;
    public static final long DEFAULT_REFERENCE_WINDOW_MILLIS = 3_600_000;
    public static final long DEFAULT_RELEARN_AFTER_MILLIS = 86_400_000;

    private static final String[] CACHES = { "key", "row", "chunk" };
    private static final SignalId[] HIT_RATES = { Signals.CACHE_KEY_HIT_RATE, Signals.CACHE_ROW_HIT_RATE, Signals.CACHE_CHUNK_HIT_RATE };
    private static final SignalId[] REQUESTS = { Signals.CACHE_KEY_REQUESTS_PER_SEC, Signals.CACHE_ROW_REQUESTS_PER_SEC,
                                                 Signals.CACHE_CHUNK_REQUESTS_PER_SEC };

    /** The newest hit rate of a cache and the level it is compared with. */
    private static final class Reading
    {
        final int cache;
        final long at;
        final double hitRate;
        final double reference;

        Reading(int cache, long at, double hitRate, double reference)
        {
            this.cache = cache;
            this.at = at;
            this.hitRate = hitRate;
            this.reference = reference;
        }

        double drop()
        {
            return reference - hitRate;
        }
    }

    private final long referenceWindowMillis;
    private final long relearnAfterMillis;
    @SuppressWarnings("unchecked")
    private final ArrayDeque<double[]>[] history = new ArrayDeque[] { new ArrayDeque<>(), new ArrayDeque<>(), new ArrayDeque<>() };
    private final long[] lastPoint = { Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE };
    private final long[] breachStart = { Long.MIN_VALUE, Long.MIN_VALUE, Long.MIN_VALUE };
    private final OptionalDouble[] lastReference = { OptionalDouble.empty(), OptionalDouble.empty(), OptionalDouble.empty() };
    private final Reading[] current = new Reading[3];
    private Reading worst;

    public CacheMissPressureDetector(Settings settings, long referenceWindowMillis, long relearnAfterMillis)
    {
        super(NAME, NAME, Signals.CACHE_KEY_HIT_RATE, settings);
        if (referenceWindowMillis < 1)
            throw new IllegalArgumentException("reference_window_ms must be positive");
        if (relearnAfterMillis < 1)
            throw new IllegalArgumentException("relearn_after_ms must be positive");
        this.referenceWindowMillis = referenceWindowMillis;
        this.relearnAfterMillis = relearnAfterMillis;
    }

    public static CacheMissPressureDetector fromConfig(Map<String, Object> options)
    {
        Settings settings = Settings.fromConfig(options, THRESHOLD_KEY, DEFAULT_DROP);
        return new CacheMissPressureDetector(settings, positive(options, REFERENCE_WINDOW_KEY, DEFAULT_REFERENCE_WINDOW_MILLIS),
                                             positive(options, RELEARN_AFTER_KEY, DEFAULT_RELEARN_AFTER_MILLIS));
    }

    private static long positive(Map<String, Object> options, String key, long defaultValue)
    {
        Object o = options == null ? null : options.get(key);
        if (o == null)
            return defaultValue;
        try
        {
            long v = o instanceof Number ? ((Number) o).longValue() : Long.parseLong(o.toString().trim());
            return v > 0 ? v : defaultValue;
        }
        catch (NumberFormatException e)
        {
            return defaultValue;
        }
    }

    @Override
    public Set<SignalId> requires()
    {
        return Set.of(HIT_RATES[0], HIT_RATES[1], HIT_RATES[2], REQUESTS[0], REQUESTS[1], REQUESTS[2]);
    }

    @Override
    protected Collection<Subject> subjects(KnowledgeView knowledge)
    {
        return List.of(Subject.node());
    }

    @Override
    protected void beginEvaluation(KnowledgeView knowledge)
    {
        worst = null;
        for (int c = 0; c < CACHES.length; c++)
        {
            current[c] = null;
            Series s = knowledge.series(HIT_RATES[c], Subject.node(), settings().maxSignalAgeMillis);
            for (int i = 0; i < s.size(); i++)
            {
                if (s.times[i] <= lastPoint[c])
                    continue;
                lastPoint[c] = s.times[i];
                OptionalDouble ref = reference(c);
                lastReference[c] = ref;
                boolean breaching = ref.isPresent() && ref.getAsDouble() - s.values[i] >= settings().raise;
                if (!breaching)
                {
                    breachStart[c] = Long.MIN_VALUE;
                    history[c].addLast(new double[]{ s.times[i], s.values[i] });
                    continue;
                }
                if (breachStart[c] == Long.MIN_VALUE)
                    breachStart[c] = s.times[i];
                // After relearn_after_ms of continuous breach the new level is taken as normal (ADR-046)
                if (s.times[i] - breachStart[c] >= relearnAfterMillis)
                    history[c].addLast(new double[]{ s.times[i], s.values[i] });
            }
            if (s.isEmpty() || !lastReference[c].isPresent())
                continue;
            current[c] = new Reading(c, s.times[s.size() - 1], s.values[s.size() - 1], lastReference[c].getAsDouble());
            if (worst == null || current[c].drop() > worst.drop())
                worst = current[c];
        }
    }

    /**
     * Mean of the last {@code reference_window_ms} of the cache's history, ending at its newest point, if the history
     * reaches back a whole window. The history holds only points that did not breach (until a breach has lasted
     * {@code relearn_after_ms}), so during a drop its newest point is the last one before it and the reference stays
     * at the level from before the drop however long the drop lasts. Only points older than that window are dropped,
     * keeping one before its start so the reach-back stays known.
     */
    private OptionalDouble reference(int cache)
    {
        ArrayDeque<double[]> h = history[cache];
        if (h.isEmpty())
            return OptionalDouble.empty();
        long end = (long) h.peekLast()[0];
        long start = end - referenceWindowMillis;
        while (h.size() > 1 && secondTime(h) <= start)
            h.removeFirst();
        if (h.peekFirst()[0] > start)
            return OptionalDouble.empty();
        double sum = 0;
        int n = 0;
        for (double[] p : h)
            if (p[0] >= start)
            {
                sum += p[1];
                n++;
            }
        return n == 0 ? OptionalDouble.empty() : OptionalDouble.of(sum / n);
    }

    private static double secondTime(ArrayDeque<double[]> h)
    {
        java.util.Iterator<double[]> it = h.iterator();
        it.next();
        return it.next()[0];
    }

    @Override
    protected OptionalDouble measure(Subject subject, KnowledgeView knowledge)
    {
        if (worst == null)
            return OptionalDouble.empty();
        observedAt(worst.at);
        return OptionalDouble.of(worst.drop());
    }

    @Override
    protected String summary(Subject subject, double value)
    {
        if (worst == null)
            return "A cache's hit rate is well below its usual level";
        return String.format("The %s cache hit rate is %.0f%%, %.0f points below its usual %.0f%%; reads that miss go to disk",
                             CACHES[worst.cache], worst.hitRate * 100, worst.drop() * 100, worst.reference * 100);
    }

    @Override
    protected void addEvidence(Subject subject, double value, Map<String, String> evidence, KnowledgeView knowledge)
    {
        Reading r = worst;
        if (r == null)
            return;
        evidence.put("cache", CACHES[r.cache]);
        evidence.put(HIT_RATES[r.cache].name, format(r.hitRate));
        evidence.put("reference_hit_rate", format(r.reference));
        evidence.put("drop", format(r.drop()) + " (raise " + format(settings().raise) + ", clear " + format(settings().clear) + ')');
        latestWithin(REQUESTS[r.cache], subject, knowledge).ifPresent(v -> evidence.put(REQUESTS[r.cache].name, format(v)));
    }
}
