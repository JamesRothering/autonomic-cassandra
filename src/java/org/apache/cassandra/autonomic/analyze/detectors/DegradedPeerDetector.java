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

import org.apache.cassandra.autonomic.analyze.Severity;
import org.apache.cassandra.autonomic.knowledge.KnowledgeView;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.monitor.sensors.Signals;

/**
 * A live peer whose dynamic snitch score (lower is better) is at least {@code score_ratio} (default 2.0) times the
 * median score of the live peers, with at least {@code min_peers} (default 3) scored peers. Subject is the peer.
 * Limitation: scores are this node's own latency view of the peer, and gossip liveness is seconds old, so a peer
 * that just failed or recovered is judged on old data; see {@link PeerDetector}. Needs the dynamic snitch.
 */
public class DegradedPeerDetector extends PeerDetector
{
    public static final String NAME = "degraded_peer";
    public static final String RATIO_KEY = "score_ratio";
    public static final double DEFAULT_RATIO = 2.0;

    private Map<Subject, Double> scores = Collections.emptyMap();
    private double median = Double.NaN;

    public DegradedPeerDetector(Settings settings, int minPeers)
    {
        super(NAME, Signals.PEER_SNITCH_SCORE, settings, minPeers);
    }

    public static DegradedPeerDetector fromConfig(Map<String, Object> options)
    {
        return new DegradedPeerDetector(Settings.fromConfig(options, RATIO_KEY, DEFAULT_RATIO), minPeers(options));
    }

    @Override
    public Set<SignalId> requires()
    {
        return Set.of(Signals.PEER_SNITCH_SCORE, Signals.PEER_ALIVE);
    }

    @Override
    protected void beginEvaluation(KnowledgeView knowledge)
    {
        scores = Collections.emptyMap();
        median = Double.NaN;
        Map<Subject, Double> s = alivePeerValues(Signals.PEER_SNITCH_SCORE, knowledge);
        if (s.size() >= minPeers)
        {
            scores = s;
            median = median(s.values());
        }
    }

    @Override
    protected Collection<Subject> subjects(KnowledgeView knowledge)
    {
        return scores.keySet();
    }

    @Override
    protected OptionalDouble measure(Subject subject, KnowledgeView knowledge)
    {
        Double score = scores.get(subject);
        if (score == null || !(median > 0))
            return OptionalDouble.empty();
        latestWithin(Signals.PEER_SNITCH_SCORE, subject, knowledge); // observation time
        return OptionalDouble.of(score / median);
    }

    @Override
    protected Severity severity(Subject subject, double value, KnowledgeView knowledge)
    {
        return value >= 2 * settings().raise ? Severity.CRITICAL : Severity.WARN;
    }

    @Override
    protected String summary(Subject subject, double value)
    {
        return String.format("Peer %s has a snitch score %.1fx the median of live peers", subject.detail, value);
    }

    @Override
    protected void addEvidence(Subject subject, double value, Map<String, String> evidence, KnowledgeView knowledge)
    {
        evidence.put("score_ratio", format(value) + " (raise " + format(settings().raise) + ", clear " + format(settings().clear) + ')');
        latestWithin(Signals.PEER_SNITCH_SCORE, subject, knowledge).ifPresent(v -> evidence.put(Signals.PEER_SNITCH_SCORE.name, format(v)));
        if (!Double.isNaN(median))
            evidence.put("median_snitch_score", format(median));
        latestWithin(Signals.PEER_SEVERITY, subject, knowledge).ifPresent(v -> evidence.put(Signals.PEER_SEVERITY.name, format(v)));
        evidence.put("peer", String.valueOf(subject.detail));
    }
}
