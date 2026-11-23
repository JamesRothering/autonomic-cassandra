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
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.monitor.sensors.Signals;

/** Fraction of wall time spent in GC at or above {@code time_fraction} (default 0.1); CRITICAL at 0.3 and above. */
public class GcPressureDetector extends ThresholdDetector
{
    public static final String NAME = "gc_pressure";
    public static final String THRESHOLD_KEY = "time_fraction";
    public static final double DEFAULT_THRESHOLD = 0.1;
    private static final double CRITICAL = 0.3;

    public GcPressureDetector(Settings settings)
    {
        super(NAME, NAME, Signals.GC_TIME_FRACTION, settings);
    }

    public static GcPressureDetector fromConfig(Map<String, Object> options)
    {
        return new GcPressureDetector(Settings.fromConfig(options, THRESHOLD_KEY, DEFAULT_THRESHOLD));
    }

    @Override
    public Set<SignalId> requires()
    {
        return Set.of(Signals.GC_TIME_FRACTION, Signals.GC_HEAP_USED_FRACTION, Signals.GC_TIME_MILLIS_DELTA);
    }

    @Override
    protected Severity severity(Subject subject, double value, KnowledgeView knowledge)
    {
        return value >= CRITICAL ? Severity.CRITICAL : Severity.WARN;
    }

    @Override
    protected String summary(Subject subject, double value)
    {
        return String.format("This node spends %.0f%% of its time in garbage collection", value * 100);
    }

    @Override
    protected void addEvidence(Subject subject, double value, Map<String, String> evidence, KnowledgeView knowledge)
    {
        super.addEvidence(subject, value, evidence, knowledge);
        latestWithin(Signals.GC_HEAP_USED_FRACTION, subject, knowledge).ifPresent(v -> evidence.put(Signals.GC_HEAP_USED_FRACTION.name, format(v)));
        latestWithin(Signals.GC_TIME_MILLIS_DELTA, subject, knowledge).ifPresent(v -> evidence.put(Signals.GC_TIME_MILLIS_DELTA.name, format(v)));
    }
}
