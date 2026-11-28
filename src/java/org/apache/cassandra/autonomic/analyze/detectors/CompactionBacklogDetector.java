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

/** Node-wide estimated pending compactions at or above {@code pending_tasks} (default 100); CRITICAL at five times that. */
public class CompactionBacklogDetector extends ThresholdDetector
{
    public static final String NAME = "compaction_backlog";
    public static final String THRESHOLD_KEY = "pending_tasks";
    public static final double DEFAULT_THRESHOLD = 100;

    public CompactionBacklogDetector(Settings settings)
    {
        super(NAME, NAME, Signals.COMPACTION_PENDING_TASKS, settings);
    }

    public static CompactionBacklogDetector fromConfig(Map<String, Object> options)
    {
        return new CompactionBacklogDetector(Settings.fromConfig(options, THRESHOLD_KEY, DEFAULT_THRESHOLD));
    }

    @Override
    public Set<SignalId> requires()
    {
        return Set.of(Signals.COMPACTION_PENDING_TASKS, Signals.COMPACTION_BYTES_PER_SEC);
    }

    @Override
    protected Severity severity(Subject subject, double value, KnowledgeView knowledge)
    {
        return value >= 5 * settings().raise ? Severity.CRITICAL : Severity.WARN;
    }

    @Override
    protected String summary(Subject subject, double value)
    {
        return format(value) + " compactions are pending on this node";
    }

    @Override
    protected void addEvidence(Subject subject, double value, Map<String, String> evidence, KnowledgeView knowledge)
    {
        super.addEvidence(subject, value, evidence, knowledge);
        latestWithin(Signals.COMPACTION_BYTES_PER_SEC, subject, knowledge)
            .ifPresent(v -> evidence.put(Signals.COMPACTION_BYTES_PER_SEC.name, format(v)));
    }
}
