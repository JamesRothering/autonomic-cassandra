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
import java.util.Collections;
import java.util.Map;
import java.util.OptionalDouble;
import java.util.Set;

import org.apache.cassandra.autonomic.AutonomicConfig;
import org.apache.cassandra.autonomic.analyze.Severity;
import org.apache.cassandra.autonomic.knowledge.KnowledgeView;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.monitor.sensors.Signals;

/**
 * This node's own coordinator p99 latency (worse of read and write) as a multiple of the SLO, at least
 * {@code slo_multiple} (default 1.0), when the peers do not explain it: while any live peer's snitch score is
 * {@code score_ratio} (default 2.0) times the peer median or more, a slow replica is the likelier cause, the
 * measurement is withheld and the node is not blamed. With fewer than {@code min_peers} (default 3) scored peers
 * the node cannot be compared and nothing is reported. Limitation: snitch scores are relative to the node's own
 * view and liveness is gossip-age data; see {@link PeerDetector}. Subject is the node.
 */
public class DegradedSelfDetector extends PeerDetector
{
    public static final String NAME = "degraded_self";
    public static final String MULTIPLE_KEY = "slo_multiple";

    private static final SignalId[] LATENCY = { Signals.CLIENT_READ_LATENCY_P99_MICROS, Signals.CLIENT_WRITE_LATENCY_P99_MICROS };

    private final AutonomicConfig.SloConfig slo;
    private final double peerRatio;
    private int peersScored;
    private int peersDegraded;

    public DegradedSelfDetector(Settings settings, AutonomicConfig.SloConfig slo, int minPeers, double peerRatio)
    {
        super(NAME, Signals.CLIENT_READ_LATENCY_P99_MICROS, settings, minPeers);
        this.slo = slo;
        this.peerRatio = peerRatio;
    }

    public static DegradedSelfDetector fromConfig(Map<String, Object> options, AutonomicConfig.SloConfig slo)
    {
        Object o = options == null ? null : options.get(DegradedPeerDetector.RATIO_KEY);
        double ratio = o instanceof Number ? ((Number) o).doubleValue() : DegradedPeerDetector.DEFAULT_RATIO;
        return new DegradedSelfDetector(Settings.fromConfig(options, MULTIPLE_KEY, 1.0), slo, minPeers(options), ratio);
    }

    @Override
    public Set<SignalId> requires()
    {
        return Set.of(LATENCY[0], LATENCY[1], Signals.PEER_SNITCH_SCORE, Signals.PEER_ALIVE);
    }

    @Override
    protected void beginEvaluation(KnowledgeView knowledge)
    {
        Map<Subject, Double> scores = alivePeerValues(Signals.PEER_SNITCH_SCORE, knowledge);
        peersScored = scores.size();
        peersDegraded = 0;
        double median = median(scores.values());
        if (median > 0)
            for (double v : scores.values())
                if (v >= peerRatio * median)
                    peersDegraded++;
    }

    @Override
    protected Collection<Subject> subjects(KnowledgeView knowledge)
    {
        return Collections.singleton(Subject.node());
    }

    private double sloMicros(int op)
    {
        return (op == 0 ? slo.read_latency_p99 : slo.write_latency_p99).toMilliseconds() * 1000.0;
    }

    @Override
    protected OptionalDouble measure(Subject subject, KnowledgeView knowledge)
    {
        if (peersScored < minPeers || peersDegraded > 0)
            return OptionalDouble.empty();
        double worst = Double.NaN;
        for (int op = 0; op < 2; op++)
        {
            OptionalDouble v = latestWithin(LATENCY[op], subject, knowledge);
            if (v.isPresent() && sloMicros(op) > 0)
                worst = Double.isNaN(worst) ? v.getAsDouble() / sloMicros(op) : Math.max(worst, v.getAsDouble() / sloMicros(op));
        }
        return Double.isNaN(worst) ? OptionalDouble.empty() : OptionalDouble.of(worst);
    }

    @Override
    protected Severity severity(Subject subject, double value, KnowledgeView knowledge)
    {
        return value >= 2 * settings().raise ? Severity.CRITICAL : Severity.WARN;
    }

    @Override
    protected String summary(Subject subject, double value)
    {
        return String.format("This node's p99 latency is %.1fx its SLO while its peers look healthy", value);
    }

    @Override
    protected void addEvidence(Subject subject, double value, Map<String, String> evidence, KnowledgeView knowledge)
    {
        evidence.put("slo_multiple", format(value) + " (raise " + format(settings().raise) + ", clear " + format(settings().clear) + ')');
        for (int op = 0; op < 2; op++)
        {
            int o = op;
            latestWithin(LATENCY[op], subject, knowledge)
                .ifPresent(v -> evidence.put(LATENCY[o].name, format(v) + " us (slo " + format(sloMicros(o)) + " us)"));
        }
        evidence.put("peers_scored", Integer.toString(peersScored));
        evidence.put("peers_degraded", Integer.toString(peersDegraded));
    }
}
