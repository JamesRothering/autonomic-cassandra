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
package org.apache.cassandra.autonomic.analyze;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.autonomic.knowledge.KnowledgeView;
import org.apache.cassandra.autonomic.knowledge.Series;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.utils.TimeUUID;

/**
 * Base for detectors that compare one measurement per subject with a threshold (overview 7.4).
 *
 * <p>Per subject, one {@link #analyze} call is one evaluation:
 * <ul>
 * <li>Only a measurement newer than the last one counted advances a count, so a slow sensor does not satisfy
 * {@code for_evaluations} by being re-read; a repeat is a no-op (an open diagnosis is still returned, with a fresh
 * {@code lastSeenMillis}). A missing or stale measurement counts once per evaluation.</li>
 * <li>A diagnosis opens after {@code for_evaluations} consecutive evaluations that breach the raise threshold
 * (at or beyond it). An evaluation that does not breach, including a missing or stale measurement, resets the count.</li>
 * <li>An open diagnosis keeps its id and {@code firstSeenMillis} and is returned on every evaluation, with
 * refreshed severity, summary and evidence. It clears after {@code for_evaluations} consecutive evaluations
 * strictly inside the clear threshold, or with a missing or stale measurement. A value between the clear and raise
 * thresholds neither clears nor counts towards clearing.</li>
 * <li>With {@link Settings#breachWhenAbove} false the comparisons are mirrored (low values are bad).</li>
 * </ul>
 * A diagnosis that is no longer returned is cleared; this is how {@code AutonomicService} sees the end of a condition.
 *
 * <p>Subclasses supply the signal and wording, and may override {@link #measure}, {@link #subjects},
 * {@link #severity} and {@link #addEvidence} (for example {@code hot_partition} adds {@code key_hex}).
 * State is confined to the control thread, which is the only caller of {@link #analyze}. It is bounded by
 * {@link Settings#maxSubjects}: only subjects that breached, or have an open diagnosis, are tracked, and when
 * full the least recently touched pending subject is dropped; open diagnoses are never dropped to make room.
 * No wall clock is read: time is {@link KnowledgeView#nowMillis()}.
 */
public abstract class ThresholdDetector implements Detector
{
    private static final Logger logger = LoggerFactory.getLogger(ThresholdDetector.class);

    /** Evidence key: epoch millis of the first breaching check of the run of breaches that opened the diagnosis. */
    public static final String BREACH_SINCE = "breach_since";
    public static final String FOR_EVALUATIONS = "for_evaluations";
    public static final String CLEAR_THRESHOLD = "clear_threshold";
    public static final String MAX_SIGNAL_AGE_MILLIS = "max_signal_age_ms";
    public static final String MAX_SUBJECTS = "max_subjects";
    public static final int DEFAULT_FOR_EVALUATIONS = 4;
    public static final double DEFAULT_CLEAR_FRACTION = 0.8;
    public static final long DEFAULT_MAX_SIGNAL_AGE_MILLIS = 120_000;
    public static final int DEFAULT_MAX_SUBJECTS = 1000;

    /** Thresholds and persistence of one detector. */
    public static final class Settings
    {
        public final double raise;
        public final double clear;
        public final int forEvaluations;
        public final boolean breachWhenAbove;
        /** A measurement older than this counts as missing. */
        public final long maxSignalAgeMillis;
        public final int maxSubjects;

        public Settings(double raise, double clear, int forEvaluations, boolean breachWhenAbove, long maxSignalAgeMillis, int maxSubjects)
        {
            if (Double.isNaN(raise) || Double.isNaN(clear))
                throw new IllegalArgumentException("thresholds must be numbers");
            if (breachWhenAbove ? clear > raise : clear < raise)
                throw new IllegalArgumentException("clear threshold " + clear + " is on the wrong side of raise threshold " + raise);
            if (forEvaluations < 1 || maxSubjects < 1 || maxSignalAgeMillis < 1)
                throw new IllegalArgumentException("for_evaluations, max_subjects and max_signal_age_ms must be positive");
            this.raise = raise;
            this.clear = clear;
            this.forEvaluations = forEvaluations;
            this.breachWhenAbove = breachWhenAbove;
            this.maxSignalAgeMillis = maxSignalAgeMillis;
            this.maxSubjects = maxSubjects;
        }

        /** Breach when at or above {@code raise}; clear at 80% of it; defaults for the rest. */
        public static Settings above(double raise)
        {
            return new Settings(raise, raise * DEFAULT_CLEAR_FRACTION, DEFAULT_FOR_EVALUATIONS, true,
                                DEFAULT_MAX_SIGNAL_AGE_MILLIS, DEFAULT_MAX_SUBJECTS);
        }

        /**
         * Reads one entry of {@code autonomic.detectors}: the raise threshold under {@code raiseKey} (default
         * {@code defaultRaise}), and optional {@code clear_threshold} (default 80% of raise), {@code for_evaluations},
         * {@code max_signal_age_ms} and {@code max_subjects}. A malformed value falls back to its default with a warning.
         */
        public static Settings fromConfig(Map<String, Object> options, String raiseKey, double defaultRaise)
        {
            return fromConfig(options, raiseKey, defaultRaise, DEFAULT_CLEAR_FRACTION);
        }

        /** As above, with a detector-specific default clear threshold as a fraction of raise. */
        public static Settings fromConfig(Map<String, Object> options, String raiseKey, double defaultRaise, double defaultClearFraction)
        {
            double raise = number(options, raiseKey, defaultRaise);
            double clear = number(options, CLEAR_THRESHOLD, raise * defaultClearFraction);
            int n = (int) number(options, FOR_EVALUATIONS, DEFAULT_FOR_EVALUATIONS);
            long age = (long) number(options, MAX_SIGNAL_AGE_MILLIS, DEFAULT_MAX_SIGNAL_AGE_MILLIS);
            int max = (int) number(options, MAX_SUBJECTS, DEFAULT_MAX_SUBJECTS);
            try
            {
                return new Settings(raise, clear, n, true, age, max);
            }
            catch (IllegalArgumentException e)
            {
                logger.warn("Invalid autonomic detector options {}: {}; using defaults", options, e.getMessage());
                return new Settings(defaultRaise, defaultRaise * defaultClearFraction, DEFAULT_FOR_EVALUATIONS, true,
                                    DEFAULT_MAX_SIGNAL_AGE_MILLIS, DEFAULT_MAX_SUBJECTS);
            }
        }

        private static double number(Map<String, Object> options, String key, double def)
        {
            Object o = options == null ? null : options.get(key);
            if (o == null)
                return def;
            try
            {
                return o instanceof Number ? ((Number) o).doubleValue() : Double.parseDouble(o.toString().trim());
            }
            catch (NumberFormatException e)
            {
                logger.warn("Invalid autonomic detector option {}={}; using {}", key, o, def);
                return def;
            }
        }
    }

    /** Hysteresis state of one subject. */
    private static final class State
    {
        int breaches;
        int clears;
        double lastValue;
        long touchedMillis;
        UUID id;                 // non-null while open
        long firstSeenMillis;
        long breachSinceMillis;  // first breaching check of the current run of breaches
        long countedAtMillis = Long.MIN_VALUE;   // observation time of the last measurement that was counted
    }

    private final String name;
    private final String type;
    private final SignalId signal;
    private final Settings settings;
    private final Map<Subject, State> states = new LinkedHashMap<>();
    /** Newest observation time read by {@link #latestWithin} during the current {@link #measure}; -1 if none. */
    private long observedAt;

    /**
     * @param name       detector name (health accounting and logs)
     * @param type       diagnosis type, for example {@code hot_partition}
     * @param signal     the measured signal; also the only required signal unless {@link #requires} is overridden
     */
    protected ThresholdDetector(String name, String type, SignalId signal, Settings settings)
    {
        this.name = name;
        this.type = type;
        this.signal = signal;
        this.settings = settings;
    }

    @Override
    public String name()
    {
        return name;
    }

    @Override
    public Set<SignalId> requires()
    {
        return Set.of(signal);
    }

    public Settings settings()
    {
        return settings;
    }

    /** Number of subjects with state; for tests and metrics. */
    public int trackedSubjects()
    {
        return states.size();
    }

    /** Called once at the start of each evaluation, before any {@link #measure}; for per-evaluation aggregates. */
    protected void beginEvaluation(KnowledgeView knowledge)
    {
    }

    /** One sentence for operators. */
    protected abstract String summary(Subject subject, double value);

    /**
     * The current measurement of a subject, empty if there is none. The default is the newest point of the signal
     * younger than {@code max_signal_age_ms}. Derived measures (a share, a ratio) override this.
     */
    protected OptionalDouble measure(Subject subject, KnowledgeView knowledge)
    {
        return latestWithin(signal, subject, knowledge);
    }

    /** Records the observation time of a measurement that does not come from the signal store. */
    protected final void observedAt(long atMillis)
    {
        observedAt = Math.max(observedAt, atMillis);
    }

    /** The newest point of {@code signal} younger than {@code max_signal_age_ms}, if any. */
    protected final OptionalDouble latestWithin(SignalId signal, Subject subject, KnowledgeView knowledge)
    {
        Series s = knowledge.series(signal, subject, settings.maxSignalAgeMillis);
        if (s.isEmpty())
            return OptionalDouble.empty();
        observedAt = Math.max(observedAt, s.times[s.size() - 1]);
        return OptionalDouble.of(s.values[s.size() - 1]);
    }

    /** Subjects to evaluate this round, in addition to those already tracked. Default: subjects of the signal. */
    protected Collection<Subject> subjects(KnowledgeView knowledge)
    {
        return knowledge.subjects(signal);
    }

    /** Default WARN. */
    protected Severity severity(Subject subject, double value, KnowledgeView knowledge)
    {
        return Severity.WARN;
    }

    /** Default 1.0. */
    protected double confidence(Subject subject, double value)
    {
        return 1.0;
    }

    /**
     * Adds evidence (signal name to formatted value) to an open diagnosis. The default records the measured
     * signal with its thresholds; subclasses call super and add their own entries, for example {@code key_hex}.
     */
    protected void addEvidence(Subject subject, double value, Map<String, String> evidence, KnowledgeView knowledge)
    {
        evidence.put(signal.name, format(value) + " (raise " + format(settings.raise) + ", clear " + format(settings.clear) + ')');
    }

    protected static String format(double v)
    {
        return v == Math.rint(v) && Math.abs(v) < 1e15 ? Long.toString((long) v) : String.format(Locale.ROOT, "%.4g", v);
    }

    /** Overridable for tests. */
    protected UUID newDiagnosisId()
    {
        return TimeUUID.Generator.nextTimeUUID().asUUID();
    }

    @Override
    public final List<Diagnosis> analyze(KnowledgeView knowledge)
    {
        long now = knowledge.nowMillis();
        try
        {
            beginEvaluation(knowledge);
        }
        catch (RuntimeException e)
        {
            logger.debug("Detector {} failed to prepare an evaluation", name, e);
        }
        Set<Subject> candidates = new LinkedHashSet<>(states.keySet());
        try
        {
            candidates.addAll(subjects(knowledge));
        }
        catch (RuntimeException e)
        {
            logger.debug("Detector {} could not list subjects", name, e);
        }

        List<Diagnosis> open = new ArrayList<>();
        for (Subject subject : candidates)
        {
            try
            {
                Diagnosis d = evaluate(subject, knowledge, now);
                if (d != null)
                    open.add(d);
            }
            catch (RuntimeException e)
            {
                logger.debug("Detector {} failed on {}", name, subject, e);
            }
        }
        return open;
    }

    private Diagnosis evaluate(Subject subject, KnowledgeView knowledge, long now)
    {
        observedAt = -1;
        OptionalDouble measured = measure(subject, knowledge);
        State st = states.get(subject);
        boolean wasOpen = st != null && st.id != null;
        boolean present = measured.isPresent() && !Double.isNaN(measured.getAsDouble());
        double value = present ? measured.getAsDouble() : 0;
        boolean breach = present && (settings.breachWhenAbove ? value >= settings.raise : value <= settings.raise);
        boolean beyondClear = present && (settings.breachWhenAbove ? value < settings.clear : value > settings.clear);

        // Persistence counts independent observations: a measurement already counted is a no-op. A measure that
        // did not go through latestWithin has no observation time and counts on every evaluation.
        long at = observedAt >= 0 ? observedAt : now;
        if (present && st != null && at <= st.countedAtMillis)
        {
            if (!wasOpen)
                return null;
            st.touchedMillis = now;
            st.lastValue = value;
            return diagnosis(subject, st, now, knowledge);
        }
        if (present && st != null)
            st.countedAtMillis = at;

        if (!wasOpen)
        {
            if (!breach)
            {
                if (st != null)
                    states.remove(subject);
                return null;
            }
            if (st == null && (st = admit(subject)) == null)
                return null;
            st.countedAtMillis = at;
            if (st.breaches++ == 0)
                st.breachSinceMillis = now;
            st.lastValue = value;
            st.touchedMillis = now;
            if (st.breaches < settings.forEvaluations)
                return null;
            st.id = newDiagnosisId();
            st.firstSeenMillis = now;
            st.breaches = 0;
            st.clears = 0;
        }
        else
        {
            st.touchedMillis = now;
            if (breach)
            {
                st.clears = 0;
                st.lastValue = value;
            }
            else if (beyondClear || !present)
            {
                if (present)
                    st.lastValue = value;
                if (++st.clears >= settings.forEvaluations)
                {
                    states.remove(subject);
                    return null;
                }
            }
            else
            {
                // between clear and raise: hold
                st.clears = 0;
                st.lastValue = value;
            }
        }
        return diagnosis(subject, st, now, knowledge);
    }

    private Diagnosis diagnosis(Subject subject, State st, long now, KnowledgeView knowledge)
    {
        Map<String, String> evidence = new LinkedHashMap<>();
        addEvidence(subject, st.lastValue, evidence, knowledge);
        evidence.put(BREACH_SINCE, Long.toString(st.breachSinceMillis));
        return new Diagnosis(st.id, type, subject, severity(subject, st.lastValue, knowledge), confidence(subject, st.lastValue),
                             st.firstSeenMillis, now, summary(subject, st.lastValue), evidence, null);
    }

    /** Makes room for a new subject, or returns null if every tracked subject has an open diagnosis. */
    private State admit(Subject subject)
    {
        if (states.size() >= settings.maxSubjects)
        {
            Subject victim = null;
            long oldest = Long.MAX_VALUE;
            for (Iterator<Map.Entry<Subject, State>> it = states.entrySet().iterator(); it.hasNext(); )
            {
                Map.Entry<Subject, State> e = it.next();
                if (e.getValue().id == null && e.getValue().touchedMillis < oldest)
                {
                    oldest = e.getValue().touchedMillis;
                    victim = e.getKey();
                }
            }
            if (victim == null)
            {
                logger.debug("Detector {} tracks {} open diagnoses; ignoring {}", name, states.size(), subject);
                return null;
            }
            states.remove(victim);
        }
        State st = new State();
        states.put(subject, st);
        return st;
    }
}
