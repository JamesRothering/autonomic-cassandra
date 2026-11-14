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

/**
 * Per thread pool: queued tasks at or above {@code pending_tasks} (default 50). CRITICAL when tasks are blocked
 * from queueing now or the queue is at five times the threshold.
 */
public class PoolSaturationDetector extends ThresholdDetector
{
    public static final String NAME = "pool_saturation";
    public static final String THRESHOLD_KEY = "pending_tasks";
    public static final double DEFAULT_THRESHOLD = 50;

    public PoolSaturationDetector(Settings settings)
    {
        super(NAME, NAME, Signals.POOL_PENDING_TASKS, settings);
    }

    public static PoolSaturationDetector fromConfig(Map<String, Object> options)
    {
        return new PoolSaturationDetector(Settings.fromConfig(options, THRESHOLD_KEY, DEFAULT_THRESHOLD));
    }

    @Override
    public Set<SignalId> requires()
    {
        return Set.of(Signals.POOL_PENDING_TASKS, Signals.POOL_ACTIVE_TASKS, Signals.POOL_MAX_POOL_SIZE,
                      Signals.POOL_CURRENTLY_BLOCKED_TASKS, Signals.POOL_OLDEST_TASK_QUEUE_MILLIS);
    }

    @Override
    protected Severity severity(Subject subject, double value, KnowledgeView knowledge)
    {
        boolean blocked = latestWithin(Signals.POOL_CURRENTLY_BLOCKED_TASKS, subject, knowledge).orElse(0) > 0;
        return blocked || value >= 5 * settings().raise ? Severity.CRITICAL : Severity.WARN;
    }

    @Override
    protected String summary(Subject subject, double value)
    {
        return "Thread pool " + subject.detail + " has " + format(value) + " queued tasks";
    }

    @Override
    protected void addEvidence(Subject subject, double value, Map<String, String> evidence, KnowledgeView knowledge)
    {
        super.addEvidence(subject, value, evidence, knowledge);
        evidence.put("pool", String.valueOf(subject.detail));
        for (SignalId id : new SignalId[]{ Signals.POOL_ACTIVE_TASKS, Signals.POOL_MAX_POOL_SIZE,
                                           Signals.POOL_CURRENTLY_BLOCKED_TASKS, Signals.POOL_OLDEST_TASK_QUEUE_MILLIS })
            latestWithin(id, subject, knowledge).ifPresent(v -> evidence.put(id.name, format(v)));
    }
}
