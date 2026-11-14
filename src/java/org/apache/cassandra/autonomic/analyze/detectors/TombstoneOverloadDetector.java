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

import java.util.Map;
import java.util.Set;

import org.apache.cassandra.autonomic.analyze.Severity;
import org.apache.cassandra.autonomic.analyze.ThresholdDetector;
import org.apache.cassandra.autonomic.knowledge.KnowledgeView;
import org.apache.cassandra.autonomic.knowledge.Series;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.monitor.sensors.Signals;

/**
 * Reads that scan many tombstones: p99 of tombstones scanned per read at or above {@code tombstones_scanned_p99}
 * (default 1000, the stock {@code tombstone_warn_threshold}). The p99 comes from a histogram, so unlike the
 * per-interval warning and failure counters it is steady enough for hysteresis. Severity is CRITICAL when a query
 * was aborted within {@code max_signal_age_ms} or p99 reaches {@code failure_threshold} (default 100000, the stock
 * {@code tombstone_failure_threshold}), else WARN. Evidence includes the warning and failure counts in that window.
 */
public class TombstoneOverloadDetector extends ThresholdDetector
{
    public static final String NAME = "tombstone_overload";
    public static final String THRESHOLD_KEY = "tombstones_scanned_p99";
    public static final String FAILURE_KEY = "failure_threshold";
    public static final double DEFAULT_THRESHOLD = 1000;
    public static final double DEFAULT_FAILURE_THRESHOLD = 100_000;

    private final double failureThreshold;

    public TombstoneOverloadDetector(Settings settings, double failureThreshold)
    {
        super(NAME, NAME, Signals.TABLE_TOMBSTONES_SCANNED_P99, settings);
        this.failureThreshold = failureThreshold;
    }

    public static TombstoneOverloadDetector fromConfig(Map<String, Object> options)
    {
        double failure = DEFAULT_FAILURE_THRESHOLD;
        Object o = options == null ? null : options.get(FAILURE_KEY);
        if (o instanceof Number)
            failure = ((Number) o).doubleValue();
        return new TombstoneOverloadDetector(Settings.fromConfig(options, THRESHOLD_KEY, DEFAULT_THRESHOLD), failure);
    }

    @Override
    public Set<SignalId> requires()
    {
        return Set.of(Signals.TABLE_TOMBSTONES_SCANNED_P99, Signals.TABLE_TOMBSTONE_WARNINGS_DELTA, Signals.TABLE_TOMBSTONE_FAILURES_DELTA);
    }

    private double windowSum(SignalId signal, Subject subject, KnowledgeView knowledge)
    {
        Series s = knowledge.series(signal, subject, settings().maxSignalAgeMillis);
        double sum = 0;
        for (double v : s.values)
            sum += v;
        return sum;
    }

    @Override
    protected Severity severity(Subject subject, double value, KnowledgeView knowledge)
    {
        boolean aborted = windowSum(Signals.TABLE_TOMBSTONE_FAILURES_DELTA, subject, knowledge) > 0;
        return aborted || value >= failureThreshold ? Severity.CRITICAL : Severity.WARN;
    }

    @Override
    protected String summary(Subject subject, double value)
    {
        return String.format("Reads of %s.%s scan %s tombstones at p99", subject.keyspace, subject.table, format(value));
    }

    @Override
    protected void addEvidence(Subject subject, double value, Map<String, String> evidence, KnowledgeView knowledge)
    {
        evidence.put(Signals.TABLE_TOMBSTONES_SCANNED_P99.name,
                     format(value) + " (raise " + format(settings().raise) + ", clear " + format(settings().clear) + ')');
        evidence.put(Signals.TABLE_TOMBSTONE_WARNINGS_DELTA.name, format(windowSum(Signals.TABLE_TOMBSTONE_WARNINGS_DELTA, subject, knowledge)));
        evidence.put(Signals.TABLE_TOMBSTONE_FAILURES_DELTA.name, format(windowSum(Signals.TABLE_TOMBSTONE_FAILURES_DELTA, subject, knowledge)));
        evidence.put("table", subject.keyspace + '.' + subject.table);
    }
}
