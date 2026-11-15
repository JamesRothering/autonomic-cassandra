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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.Set;

import org.apache.cassandra.autonomic.analyze.Diagnosis;
import org.apache.cassandra.autonomic.analyze.Severity;
import org.apache.cassandra.autonomic.analyze.ThresholdDetector;
import org.apache.cassandra.autonomic.knowledge.KnowledgeView;
import org.apache.cassandra.autonomic.knowledge.Series;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.monitor.sensors.Signals;

/**
 * The node cannot answer its load with what it has (overview 8.3, the trigger of scale-out). Two cases, told apart
 * by the subject and by evidence {@code trigger}:
 * <ul>
 * <li>{@code saturation}, subject the thread pool: a {@code pool_saturation} diagnosis is open and the load spike
 * ladder has nothing left to propose for it in the node: every enabled in-node step is at its envelope limit,
 * locked, overridden or was ineffective. The measure is 1 while that holds and 0 otherwise.</li>
 * <li>{@code disk_trend}, subject the node: a least-squares line through {@code disk.used_fraction} over
 * {@code trend_window_ms} reaches {@code full_fraction} within {@code horizon_hours}. The measure is the horizon
 * divided by the predicted time to full: 1 and above breaches, below the clear threshold (0.8, a time to full of
 * 1.25 horizons) clears. No trend is computed from fewer than {@code min_samples} points, points spanning less
 * than half the window, or a disk below {@code min_used_fraction}.</li>
 * </ul>
 * Both must persist for {@code for_evaluations} (default 8): evaluations for saturation, distinct disk samples for
 * the trend. A pure function of the knowledge and of the knob actuators' in-memory state; no I/O.
 */
public class CapacityExhaustedDetector extends ThresholdDetector
{
    public static final String NAME = "capacity_exhausted";
    public static final String EVIDENCE_TRIGGER = "trigger";
    public static final String TRIGGER_SATURATION = "saturation";
    public static final String TRIGGER_DISK = "disk_trend";
    public static final String EVIDENCE_IN_NODE = "in_node_steps";
    public static final String EVIDENCE_HOURS_TO_FULL = "hours_to_full";

    public static final String HORIZON_HOURS = "horizon_hours";
    public static final String FULL_FRACTION = "full_fraction";
    public static final String TREND_WINDOW_MILLIS = "trend_window_ms";
    public static final String MIN_SAMPLES = "min_samples";
    public static final String MIN_USED_FRACTION = "min_used_fraction";
    public static final int DEFAULT_FOR_EVALUATIONS = 8;
    private static final double MAX_MEASURE = 100;
    /** Twice as fast as the horizon allows. */
    private static final double CRITICAL = 2;

    /** What the detector asks of the load spike ladder; see {@code LoadSpikePolicy.inNodeExhausted}. */
    public interface Ladder
    {
        /** The state of the in-node steps if none has anything left to propose for the lead, otherwise empty. */
        Optional<String> exhausted(Diagnosis lead, KnowledgeView knowledge);
    }

    /** Options of the disk trend. */
    public static final class Trend
    {
        public final double horizonHours;
        public final double fullFraction;
        public final long windowMillis;
        public final int minSamples;
        public final double minUsedFraction;

        public Trend(double horizonHours, double fullFraction, long windowMillis, int minSamples, double minUsedFraction)
        {
            if (!(horizonHours > 0) || !(fullFraction > 0) || windowMillis < 1 || minSamples < 2)
                throw new IllegalArgumentException("horizon_hours, full_fraction and trend_window_ms must be positive and min_samples at least 2");
            this.horizonHours = horizonHours;
            this.fullFraction = fullFraction;
            this.windowMillis = windowMillis;
            this.minSamples = minSamples;
            this.minUsedFraction = minUsedFraction;
        }

        public static Trend defaults()
        {
            return new Trend(12, 0.9, 1_800_000, 10, 0.5);
        }
    }

    private final Trend trend;
    private final Ladder ladder;
    // per evaluation, for evidence
    private String inNode;
    private double slopePerHour;
    private double hoursToFull;
    private int samples;

    public CapacityExhaustedDetector(Settings settings, Trend trend, Ladder ladder)
    {
        super(NAME, NAME, Signals.DISK_USED_FRACTION, settings);
        this.trend = trend;
        this.ladder = ladder;
    }

    public static CapacityExhaustedDetector fromConfig(Map<String, Object> options, Ladder ladder)
    {
        Settings s = Settings.fromConfig(options, "raise", 1.0);
        if (options == null || !options.containsKey(FOR_EVALUATIONS))
            s = new Settings(s.raise, s.clear, DEFAULT_FOR_EVALUATIONS, true, s.maxSignalAgeMillis, s.maxSubjects);
        Trend d = Trend.defaults();
        Trend t;
        try
        {
            t = new Trend(number(options, HORIZON_HOURS, d.horizonHours), number(options, FULL_FRACTION, d.fullFraction),
                          (long) number(options, TREND_WINDOW_MILLIS, d.windowMillis), (int) number(options, MIN_SAMPLES, d.minSamples),
                          number(options, MIN_USED_FRACTION, d.minUsedFraction));
        }
        catch (RuntimeException e)
        {
            t = d;
        }
        return new CapacityExhaustedDetector(s, t, ladder);
    }

    private static double number(Map<String, Object> options, String key, double def)
    {
        Object o = options == null ? null : options.get(key);
        return o == null ? def : o instanceof Number ? ((Number) o).doubleValue() : Double.parseDouble(o.toString().trim());
    }

    @Override
    public Set<SignalId> requires()
    {
        return Set.of(Signals.DISK_USED_FRACTION, Signals.POOL_PENDING_TASKS);
    }

    @Override
    protected Collection<Subject> subjects(KnowledgeView knowledge)
    {
        List<Subject> subjects = new ArrayList<>();
        subjects.add(Subject.node());
        for (Diagnosis d : knowledge.activeDiagnoses())
            if (isLead(d))
                subjects.add(d.subject);
        return subjects;
    }

    private static boolean isLead(Diagnosis d)
    {
        return d.type.equals(PoolSaturationDetector.NAME) && d.rootCause == null && d.subject.kind == Subject.Kind.THREAD_POOL;
    }

    @Override
    protected OptionalDouble measure(Subject subject, KnowledgeView knowledge)
    {
        return subject.kind == Subject.Kind.THREAD_POOL ? saturation(subject, knowledge) : diskTrend(subject, knowledge);
    }

    private OptionalDouble saturation(Subject pool, KnowledgeView knowledge)
    {
        inNode = null;
        if (ladder == null)
            return OptionalDouble.empty();
        for (Diagnosis d : knowledge.activeDiagnoses())
        {
            if (!isLead(d) || !d.subject.equals(pool))
                continue;
            Optional<String> exhausted = ladder.exhausted(d, knowledge);
            if (!exhausted.isPresent())
                return OptionalDouble.of(0);
            inNode = exhausted.get();
            return OptionalDouble.of(1);
        }
        return OptionalDouble.of(0);
    }

    private OptionalDouble diskTrend(Subject node, KnowledgeView knowledge)
    {
        // missing or stale signal: no measurement; also records the observation time, so a sample counts once
        OptionalDouble latest = latestWithin(Signals.DISK_USED_FRACTION, node, knowledge);
        if (!latest.isPresent())
            return OptionalDouble.empty();
        Series s = knowledge.series(Signals.DISK_USED_FRACTION, node, trend.windowMillis);
        samples = s.size();
        slopePerHour = 0;
        hoursToFull = Double.POSITIVE_INFINITY;
        double used = latest.getAsDouble();
        if (s.size() < trend.minSamples || s.times[s.size() - 1] - s.times[0] < trend.windowMillis / 2 || used < trend.minUsedFraction)
            return OptionalDouble.of(0);
        // least squares, time in hours from the first point
        double meanT = 0, meanV = 0;
        for (int i = 0; i < s.size(); i++)
        {
            meanT += (s.times[i] - s.times[0]) / 3_600_000.0;
            meanV += s.values[i];
        }
        meanT /= s.size();
        meanV /= s.size();
        double num = 0, den = 0;
        for (int i = 0; i < s.size(); i++)
        {
            double t = (s.times[i] - s.times[0]) / 3_600_000.0 - meanT;
            num += t * (s.values[i] - meanV);
            den += t * t;
        }
        slopePerHour = den == 0 ? 0 : num / den;
        if (used >= trend.fullFraction)
        {
            hoursToFull = 0;
            return OptionalDouble.of(MAX_MEASURE);
        }
        if (!(slopePerHour > 0))
            return OptionalDouble.of(0);
        hoursToFull = (trend.fullFraction - used) / slopePerHour;
        return OptionalDouble.of(Math.min(MAX_MEASURE, trend.horizonHours / hoursToFull));
    }

    @Override
    protected Severity severity(Subject subject, double value, KnowledgeView knowledge)
    {
        if (subject.kind != Subject.Kind.THREAD_POOL)
            return value >= CRITICAL ? Severity.CRITICAL : Severity.WARN;
        for (Diagnosis d : knowledge.activeDiagnoses())
            if (isLead(d) && d.subject.equals(subject))
                return d.severity;
        return Severity.WARN;
    }

    @Override
    protected String summary(Subject subject, double value)
    {
        if (subject.kind == Subject.Kind.THREAD_POOL)
            return "Thread pool " + subject.detail + " stays saturated and nothing is left to adjust in this node";
        if (Double.isInfinite(hoursToFull))
            return "Data disk usage was growing towards exhaustion and no longer is";
        return String.format(Locale.ROOT, "Data disks are predicted to be %.0f%% full in %.1f hours", trend.fullFraction * 100, hoursToFull);
    }

    @Override
    protected void addEvidence(Subject subject, double value, Map<String, String> evidence, KnowledgeView knowledge)
    {
        if (subject.kind == Subject.Kind.THREAD_POOL)
        {
            evidence.put(EVIDENCE_TRIGGER, TRIGGER_SATURATION);
            evidence.put("pool", String.valueOf(subject.detail));
            if (inNode != null)
                evidence.put(EVIDENCE_IN_NODE, inNode);
            latestWithin(Signals.POOL_PENDING_TASKS, subject, knowledge).ifPresent(v -> evidence.put(Signals.POOL_PENDING_TASKS.name, format(v)));
            return;
        }
        evidence.put(EVIDENCE_TRIGGER, TRIGGER_DISK);
        latestWithin(Signals.DISK_USED_FRACTION, subject, knowledge).ifPresent(v -> evidence.put(Signals.DISK_USED_FRACTION.name, format(v)));
        evidence.put("used_fraction_per_hour", format(slopePerHour));
        evidence.put(EVIDENCE_HOURS_TO_FULL, format(hoursToFull));
        evidence.put(HORIZON_HOURS, format(trend.horizonHours));
        evidence.put(FULL_FRACTION, format(trend.fullFraction));
        evidence.put("samples", samples + " over " + trend.windowMillis + "ms");
    }
}
