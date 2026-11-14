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

import org.apache.cassandra.autonomic.AutonomicConfig;
import org.apache.cassandra.autonomic.analyze.Severity;
import org.apache.cassandra.autonomic.analyze.ThresholdDetector;
import org.apache.cassandra.autonomic.knowledge.KnowledgeView;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.monitor.sensors.Signals;

/**
 * p99 read or write latency above {@code autonomic.slo.read_latency_p99} / {@code write_latency_p99}, per table
 * (local table latency) and for the node (coordinator latency). The measurement is the worse of read and write
 * p99 as a multiple of its SLO, so the raise threshold {@code slo_multiple} (default 1.0) and the clear threshold
 * (default 0.8, option {@code clear_threshold}) are multiples of the SLO. The SLO is read on every evaluation.
 */
public class LatencySloBreachDetector extends ThresholdDetector
{
    public static final String NAME = "latency_slo_breach";
    public static final String MULTIPLE_KEY = "slo_multiple";
    private static final double CRITICAL_MULTIPLE = 2.0;

    private static final SignalId[] TABLE_SIGNALS = { Signals.TABLE_READ_LATENCY_P99_MICROS, Signals.TABLE_WRITE_LATENCY_P99_MICROS };
    private static final SignalId[] NODE_SIGNALS = { Signals.CLIENT_READ_LATENCY_P99_MICROS, Signals.CLIENT_WRITE_LATENCY_P99_MICROS };

    private final AutonomicConfig.SloConfig slo;

    public LatencySloBreachDetector(Settings settings, AutonomicConfig.SloConfig slo)
    {
        super(NAME, NAME, Signals.TABLE_READ_LATENCY_P99_MICROS, settings);
        this.slo = slo;
    }

    public static LatencySloBreachDetector fromConfig(Map<String, Object> options, AutonomicConfig.SloConfig slo)
    {
        return new LatencySloBreachDetector(Settings.fromConfig(options, MULTIPLE_KEY, 1.0), slo);
    }

    @Override
    public Set<SignalId> requires()
    {
        Set<SignalId> s = new LinkedHashSet<>(Set.of(TABLE_SIGNALS));
        s.addAll(Set.of(NODE_SIGNALS));
        return s;
    }

    @Override
    protected Collection<Subject> subjects(KnowledgeView knowledge)
    {
        Set<Subject> all = new LinkedHashSet<>();
        for (SignalId id : TABLE_SIGNALS)
            all.addAll(knowledge.subjects(id));
        for (SignalId id : NODE_SIGNALS)
            all.addAll(knowledge.subjects(id));
        return all;
    }

    private static SignalId[] signals(Subject subject)
    {
        return subject.kind == Subject.Kind.TABLE ? TABLE_SIGNALS : NODE_SIGNALS;
    }

    private double sloMicros(int op)
    {
        return (op == 0 ? slo.read_latency_p99 : slo.write_latency_p99).toMilliseconds() * 1000.0;
    }

    private static final String[] OPS = { "read", "write" };

    @Override
    protected OptionalDouble measure(Subject subject, KnowledgeView knowledge)
    {
        if (subject.kind != Subject.Kind.TABLE && subject.kind != Subject.Kind.NODE)
            return OptionalDouble.empty();
        double worst = Double.NaN;
        SignalId[] ids = signals(subject);
        for (int op = 0; op < 2; op++)
        {
            OptionalDouble v = latestWithin(ids[op], subject, knowledge);
            if (v.isPresent() && sloMicros(op) > 0)
            {
                double ratio = v.getAsDouble() / sloMicros(op);
                worst = Double.isNaN(worst) ? ratio : Math.max(worst, ratio);
            }
        }
        return Double.isNaN(worst) ? OptionalDouble.empty() : OptionalDouble.of(worst);
    }

    @Override
    protected Severity severity(Subject subject, double value, KnowledgeView knowledge)
    {
        return value >= CRITICAL_MULTIPLE ? Severity.CRITICAL : Severity.WARN;
    }

    @Override
    protected String summary(Subject subject, double value)
    {
        String where = subject.kind == Subject.Kind.TABLE ? "table " + subject.keyspace + '.' + subject.table : "this node";
        return String.format("p99 latency on %s is %.1fx its SLO", where, value);
    }

    @Override
    protected void addEvidence(Subject subject, double value, Map<String, String> evidence, KnowledgeView knowledge)
    {
        evidence.put("slo_multiple", format(value) + " (raise " + format(settings().raise) + ", clear " + format(settings().clear) + ')');
        SignalId[] ids = signals(subject);
        for (int op = 0; op < 2; op++)
        {
            OptionalDouble v = latestWithin(ids[op], subject, knowledge);
            if (v.isPresent())
                evidence.put(ids[op].name, format(v.getAsDouble()) + " us (" + OPS[op] + " slo " + format(sloMicros(op)) + " us)");
        }
    }
}
