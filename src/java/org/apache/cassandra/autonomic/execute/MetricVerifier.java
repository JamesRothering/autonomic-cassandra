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
package org.apache.cassandra.autonomic.execute;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.autonomic.knowledge.KnowledgeView;
import org.apache.cassandra.autonomic.knowledge.Series;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.plan.VerifySpec;

/**
 * Baselines and verdicts of overview 7.3, rules 5, 6 and 8. Pure functions of a {@link KnowledgeView}; callable from
 * any thread.
 */
public final class MetricVerifier
{
    private static final Logger logger = LoggerFactory.getLogger(MetricVerifier.class);

    /** Fewer observations than this in a window make its mean unusable. */
    public static final int MIN_SAMPLES = 3;
    /** {@code abort_tolerance}: a guard regressing by this multiple of its tolerance aborts verification early. */
    public static final double ABORT_FACTOR = 2.0;

    /** Prefix of a guard's traffic mean in the {@code baseline} map (ADR-045). */
    public static final String TRAFFIC_PREFIX = "traffic:";

    private MetricVerifier()
    {
    }

    /** Key of a signal in the {@code baseline} and {@code result} maps. */
    public static String key(SignalId signal, Subject subject)
    {
        return signal.name + '@' + subject.asString();
    }

    /** Key of the baseline mean of a guard's traffic signal. */
    public static String trafficKey(SignalId signal, Subject subject)
    {
        return TRAFFIC_PREFIX + key(signal, subject);
    }

    /**
     * True if the guard names a traffic signal whose baseline mean is below the spec's floor. A guard without a
     * traffic signal, or whose baseline has no traffic samples, is judged (fail-safe).
     */
    public static boolean idle(VerifySpec spec, VerifySpec.Guard guard, Map<String, Double> baseline)
    {
        if (guard.trafficSignal == null)
            return false;
        Double traffic = baseline.get(trafficKey(guard.trafficSignal, guard.subject));
        return traffic != null && traffic < spec.guardMinOpsPerSec;
    }

    /**
     * Mean of the goal and of every guard over the last {@code lookbackMillis}. A signal with fewer than
     * {@link #MIN_SAMPLES} observations in the window is absent from the result.
     */
    public static Map<String, Double> means(VerifySpec spec, KnowledgeView knowledge, long lookbackMillis)
    {
        Map<String, Double> out = new LinkedHashMap<>();
        putMean(out, spec.goal, spec.goalSubject, knowledge, lookbackMillis);
        for (VerifySpec.Guard guard : spec.guards)
            putMean(out, guard.signal, guard.subject, knowledge, lookbackMillis);
        return out;
    }

    private static void putMean(Map<String, Double> out, SignalId signal, Subject subject, KnowledgeView knowledge, long lookbackMillis)
    {
        Series series = knowledge.series(signal, subject, lookbackMillis);
        if (series.size() < MIN_SAMPLES)
            return;
        double sum = 0;
        for (double v : series.values)
            sum += v;
        double mean = sum / series.size();
        if (!Double.isNaN(mean) && !Double.isInfinite(mean))
            out.put(key(signal, subject), mean);
    }

    /**
     * Rule 5: the baseline over {@code baselineWindowMillis}, or, if the spec has a {@code baselineSinceMillis}, from then
     * to now when that is shorter (ADR-044); empty if the goal or any guard lacks samples. It also holds, under
     * {@link #trafficKey}, the mean of each guard's traffic signal over the same period where that has enough samples;
     * these keys are optional and not counted in {@link #expectedKeys}. Without them the guard is judged.
     */
    public static Optional<Map<String, Double>> baseline(VerifySpec spec, KnowledgeView knowledge)
    {
        long lookback = spec.baselineWindowMillis;
        if (spec.baselineSinceMillis != VerifySpec.NO_BASELINE_SINCE)
            lookback = Math.max(0, Math.min(lookback, knowledge.nowMillis() - spec.baselineSinceMillis));
        Map<String, Double> means = means(spec, knowledge, lookback);
        if (means.size() != expectedKeys(spec))
            return Optional.empty();
        for (VerifySpec.Guard guard : spec.guards)
        {
            if (guard.trafficSignal == null)
                continue;
            Map<String, Double> traffic = new LinkedHashMap<>();
            putMean(traffic, guard.trafficSignal, guard.subject, knowledge, lookback);
            Double mean = traffic.get(key(guard.trafficSignal, guard.subject));
            if (mean != null)
                means.put(trafficKey(guard.trafficSignal, guard.subject), mean);
        }
        return Optional.of(means);
    }

    private static int expectedKeys(VerifySpec spec)
    {
        java.util.Set<String> keys = new java.util.HashSet<>();
        keys.add(key(spec.goal, spec.goalSubject));
        for (VerifySpec.Guard guard : spec.guards)
            keys.add(key(guard.signal, guard.subject));
        return keys.size();
    }

    /** Rule 6, over the last {@code observationWindowMillis} of {@code knowledge}. */
    public static Verdict compare(VerifySpec spec, Map<String, Double> baseline, KnowledgeView knowledge)
    {
        if (spec == null)
            return Verdict.INCONCLUSIVE;
        Map<String, Double> result = means(spec, knowledge, spec.observationWindowMillis);
        Verdict verdict = verdict(spec, baseline, result);
        logger.debug("Verdict {}: baseline {} (since {}), result {}", verdict, baseline, spec.baselineSinceMillis, result);
        return verdict;
    }

    /**
     * Rule 6, over the guards that are not idle (ADR-045): REGRESSED if any guard worsened beyond its tolerance or the goal worsened by more than
     * {@code minImprovement}; otherwise INCONCLUSIVE if any mean is missing; IMPROVED if the goal improved by at
     * least {@code minImprovement}; otherwise NEUTRAL.
     */
    public static Verdict verdict(VerifySpec spec, Map<String, Double> baseline, Map<String, Double> result)
    {
        boolean missing = false;
        for (VerifySpec.Guard guard : spec.guards)
        {
            if (idle(spec, guard, baseline))
            {
                logger.debug("Guard {} idle: baseline traffic {} below {} ops/s, not judged", key(guard.signal, guard.subject),
                             baseline.get(trafficKey(guard.trafficSignal, guard.subject)), spec.guardMinOpsPerSec);
                continue;
            }
            Double change = improvement(baseline, result, key(guard.signal, guard.subject), guard.lowerIsBetter);
            if (change == null)
                missing = true;
            else if (change < -guard.tolerance)
                return Verdict.REGRESSED;
        }
        Double goal = improvement(baseline, result, key(spec.goal, spec.goalSubject), spec.lowerIsBetter);
        if (goal == null)
            return Verdict.INCONCLUSIVE;
        if (goal < -spec.minImprovement)
            return Verdict.REGRESSED;
        if (missing)
            return Verdict.INCONCLUSIVE;
        return goal >= spec.minImprovement ? Verdict.IMPROVED : Verdict.NEUTRAL;
    }

    /**
     * Rule 8: the guard, if any, whose mean since apply has regressed by more than {@link #ABORT_FACTOR} times its
     * tolerance. Idle guards (ADR-045) and guards with fewer than {@link #MIN_SAMPLES} observations since apply are not judged.
     */
    public static Optional<String> guardBreached(VerifySpec spec, Map<String, Double> baseline, KnowledgeView knowledge, long sinceApplyMillis)
    {
        if (spec == null || sinceApplyMillis <= 0)
            return Optional.empty();
        Map<String, Double> current = new LinkedHashMap<>();
        for (VerifySpec.Guard guard : spec.guards)
            putMean(current, guard.signal, guard.subject, knowledge, sinceApplyMillis);
        for (VerifySpec.Guard guard : spec.guards)
        {
            if (idle(spec, guard, baseline))
                continue;
            String key = key(guard.signal, guard.subject);
            Double change = improvement(baseline, current, key, guard.lowerIsBetter);
            if (change != null && change < -ABORT_FACTOR * guard.tolerance)
            {
                logger.debug("Guard {} breached: baseline {} (since {}), mean since apply {} over {} ms", key, baseline.get(key),
                             spec.baselineSinceMillis, current.get(key), sinceApplyMillis);
                return Optional.of(key);
            }
        }
        return Optional.empty();
    }

    /** Relative improvement (positive is better), or null if either mean is missing. */
    private static Double improvement(Map<String, Double> baseline, Map<String, Double> result, String key, boolean lowerIsBetter)
    {
        Double before = baseline.get(key);
        Double after = result.get(key);
        if (before == null || after == null)
            return null;
        double delta = lowerIsBetter ? before - after : after - before;
        if (before == 0)
            return delta == 0 ? 0.0 : (delta > 0 ? Double.POSITIVE_INFINITY : Double.NEGATIVE_INFINITY);
        return delta / Math.abs(before);
    }
}
