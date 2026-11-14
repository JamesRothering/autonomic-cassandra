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

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.Set;

import org.apache.cassandra.autonomic.analyze.Severity;
import org.apache.cassandra.autonomic.analyze.ThresholdDetector;
import org.apache.cassandra.autonomic.knowledge.KnowledgeView;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.monitor.sensors.PartitionKeys;
import org.apache.cassandra.autonomic.monitor.sensors.Signals;

/**
 * A partition that takes at least {@code share_of_table_ops} (default 0.3) of its table's reads or writes in the
 * sampler window. Subject {@code PARTITION:ks.t:<hex>}; evidence carries the raw key as {@code key_hex} (FC6).
 * The sampler runs once per {@code sampling.interval}, so the same sample is evaluated on several ticks and a
 * partition that stops being hot is only seen as gone when its last sample is older than {@code max_signal_age_ms}.
 */
public class HotPartitionDetector extends ThresholdDetector
{
    public static final String NAME = "hot_partition";
    public static final String SHARE_KEY = "share_of_table_ops";
    public static final double DEFAULT_SHARE = 0.3;
    private static final double CRITICAL_SHARE = 0.8;

    public HotPartitionDetector(Settings settings)
    {
        super(NAME, NAME, Signals.PARTITION_READ_SHARE, settings);
    }

    public static HotPartitionDetector fromConfig(Map<String, Object> options)
    {
        return new HotPartitionDetector(Settings.fromConfig(options, SHARE_KEY, DEFAULT_SHARE));
    }

    @Override
    public Set<SignalId> requires()
    {
        return Set.of(Signals.PARTITION_READ_SHARE, Signals.PARTITION_WRITE_SHARE);
    }

    @Override
    protected Collection<Subject> subjects(KnowledgeView knowledge)
    {
        Set<Subject> all = new LinkedHashSet<>(knowledge.subjects(Signals.PARTITION_READ_SHARE));
        all.addAll(knowledge.subjects(Signals.PARTITION_WRITE_SHARE));
        return all;
    }

    @Override
    protected OptionalDouble measure(Subject subject, KnowledgeView knowledge)
    {
        OptionalDouble r = latestWithin(Signals.PARTITION_READ_SHARE, subject, knowledge);
        OptionalDouble w = latestWithin(Signals.PARTITION_WRITE_SHARE, subject, knowledge);
        if (!r.isPresent())
            return w;
        return w.isPresent() ? OptionalDouble.of(Math.max(r.getAsDouble(), w.getAsDouble())) : r;
    }

    @Override
    protected Severity severity(Subject subject, double value, KnowledgeView knowledge)
    {
        return value >= CRITICAL_SHARE ? Severity.CRITICAL : Severity.WARN;
    }

    @Override
    protected String summary(Subject subject, double value)
    {
        return String.format("Partition %s in %s.%s takes %.0f%% of the table's operations",
                             keyLabel(subject), subject.keyspace, subject.table, value * 100);
    }

    @Override
    protected void addEvidence(Subject subject, double value, Map<String, String> evidence, KnowledgeView knowledge)
    {
        evidence.put("share", format(value) + " (raise " + format(settings().raise) + ", clear " + format(settings().clear) + ')');
        put(evidence, Signals.PARTITION_READ_SHARE, subject, knowledge);
        put(evidence, Signals.PARTITION_WRITE_SHARE, subject, knowledge);
        put(evidence, Signals.PARTITION_READ_OPS_PER_SEC, subject, knowledge);
        put(evidence, Signals.PARTITION_WRITE_OPS_PER_SEC, subject, knowledge);
        evidence.put("key_hex", subject.detail);
        evidence.put("table", subject.keyspace + '.' + subject.table);
    }

    private void put(Map<String, String> evidence, SignalId signal, Subject subject, KnowledgeView knowledge)
    {
        OptionalDouble v = latestWithin(signal, subject, knowledge);
        if (v.isPresent())
            evidence.put(signal.name, format(v.getAsDouble()));
    }

    /** The key in the table's type, or its hex when the schema cannot tell. */
    protected String keyLabel(Subject subject)
    {
        try
        {
            return PartitionKeys.cql(subject);
        }
        catch (RuntimeException | LinkageError e)
        {
            return String.valueOf(subject.detail);
        }
    }
}
