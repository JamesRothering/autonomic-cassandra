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

import org.apache.cassandra.autonomic.analyze.Severity;
import org.apache.cassandra.autonomic.analyze.ThresholdDetector;
import org.apache.cassandra.autonomic.knowledge.KnowledgeView;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.monitor.sensors.Signals;

/**
 * Data volumes fuller than {@code used_fraction} (default 0.85). The default clear threshold is 95% of raise
 * (about 0.81), not 80%, because 80% of a disk limit is not reachable by normal cleanup. CRITICAL at 0.95 and above.
 */
public class DiskPressureDetector extends ThresholdDetector
{
    public static final String NAME = "disk_pressure";
    public static final String THRESHOLD_KEY = "used_fraction";
    public static final double DEFAULT_THRESHOLD = 0.85;
    public static final double DEFAULT_CLEAR_FRACTION = 0.95;
    private static final double CRITICAL = 0.95;

    public DiskPressureDetector(Settings settings)
    {
        super(NAME, NAME, Signals.DISK_USED_FRACTION, settings);
    }

    public static DiskPressureDetector fromConfig(Map<String, Object> options)
    {
        return new DiskPressureDetector(Settings.fromConfig(options, THRESHOLD_KEY, DEFAULT_THRESHOLD, DEFAULT_CLEAR_FRACTION));
    }

    @Override
    protected Severity severity(Subject subject, double value, KnowledgeView knowledge)
    {
        return value >= CRITICAL ? Severity.CRITICAL : Severity.WARN;
    }

    @Override
    protected String summary(Subject subject, double value)
    {
        return String.format("Data disks are %.0f%% full", value * 100);
    }

    @Override
    protected void addEvidence(Subject subject, double value, Map<String, String> evidence, KnowledgeView knowledge)
    {
        super.addEvidence(subject, value, evidence, knowledge);
        latestWithin(Signals.DISK_FREE_BYTES, subject, knowledge).ifPresent(v -> evidence.put(Signals.DISK_FREE_BYTES.name, format(v)));
        latestWithin(Signals.DISK_TOTAL_BYTES, subject, knowledge).ifPresent(v -> evidence.put(Signals.DISK_TOTAL_BYTES.name, format(v)));
    }
}
