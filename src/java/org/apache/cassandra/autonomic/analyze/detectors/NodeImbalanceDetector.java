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
import java.util.function.Supplier;

import org.apache.cassandra.autonomic.analyze.Severity;
import org.apache.cassandra.autonomic.knowledge.KnowledgeView;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.monitor.sensors.PeerSensor;
import org.apache.cassandra.autonomic.monitor.sensors.Signals;

/**
 * A live peer whose gossiped LOAD is at least {@code load_ratio} (default 1.5) times the mean load of the live
 * nodes, this node included, when at least {@code min_peers} report and the mean is at least
 * {@code min_load_bytes} (default 1 GiB). The subject is the outlier, which may be this node. Limitation: gossiped load is refreshed about once a minute and can be stale; see {@link PeerDetector}.
 */
public class NodeImbalanceDetector extends PeerDetector
{
    public static final String NAME = "node_imbalance";
    public static final String RATIO_KEY = "load_ratio";
    public static final double DEFAULT_RATIO = 1.5;
    public static final String MIN_LOAD_KEY = "min_load_bytes";
    public static final double DEFAULT_MIN_LOAD_BYTES = 1L << 30;

    private final double minLoadBytes;
    private final Supplier<String> localId;
    private Map<Subject, Double> loads = Collections.emptyMap();
    private double mean = Double.NaN;

    /** @param localId subject detail of this node's own load, null if unknown (for the wording only) */
    public NodeImbalanceDetector(Settings settings, int minPeers, double minLoadBytes, Supplier<String> localId)
    {
        super(NAME, Signals.PEER_LOAD_BYTES, settings, minPeers);
        this.minLoadBytes = minLoadBytes;
        this.localId = localId;
    }

    private boolean isLocal(Subject subject)
    {
        String id = localId.get();
        return id != null && id.equals(subject.detail);
    }

    public static NodeImbalanceDetector fromConfig(Map<String, Object> options)
    {
        Object o = options == null ? null : options.get(MIN_LOAD_KEY);
        double min = o instanceof Number ? ((Number) o).doubleValue() : DEFAULT_MIN_LOAD_BYTES;
        return new NodeImbalanceDetector(Settings.fromConfig(options, RATIO_KEY, DEFAULT_RATIO), minPeers(options), min, PeerSensor::localPeerId);
    }

    @Override
    public Set<SignalId> requires()
    {
        return Set.of(Signals.PEER_LOAD_BYTES, Signals.PEER_ALIVE);
    }

    @Override
    protected void beginEvaluation(KnowledgeView knowledge)
    {
        loads = Collections.emptyMap();
        mean = Double.NaN;
        Map<Subject, Double> l = alivePeerValues(Signals.PEER_LOAD_BYTES, knowledge);
        if (l.size() < minPeers)
            return;
        double sum = 0;
        for (double v : l.values())
            sum += v;
        double m = sum / l.size();
        if (m >= minLoadBytes)
        {
            loads = l;
            mean = m;
        }
    }

    @Override
    protected Collection<Subject> subjects(KnowledgeView knowledge)
    {
        return loads.keySet();
    }

    @Override
    protected OptionalDouble measure(Subject subject, KnowledgeView knowledge)
    {
        Double load = loads.get(subject);
        if (load == null)
            return OptionalDouble.empty();
        latestWithin(Signals.PEER_LOAD_BYTES, subject, knowledge); // observation time
        return OptionalDouble.of(load / mean);
    }

    @Override
    protected Severity severity(Subject subject, double value, KnowledgeView knowledge)
    {
        return value >= 2 * settings().raise ? Severity.CRITICAL : Severity.WARN;
    }

    @Override
    protected String summary(Subject subject, double value)
    {
        return String.format("%s holds %.1fx the mean load of the nodes", isLocal(subject) ? "This node" : "Peer " + subject.detail, value);
    }

    @Override
    protected void addEvidence(Subject subject, double value, Map<String, String> evidence, KnowledgeView knowledge)
    {
        evidence.put("load_ratio", format(value) + " (raise " + format(settings().raise) + ", clear " + format(settings().clear) + ')');
        latestWithin(Signals.PEER_LOAD_BYTES, subject, knowledge).ifPresent(v -> evidence.put(Signals.PEER_LOAD_BYTES.name, format(v)));
        if (!Double.isNaN(mean))
            evidence.put("mean_peer_load_bytes", format(mean));
        evidence.put("peer", String.valueOf(subject.detail));
        evidence.put("local", Boolean.toString(isLocal(subject)));
    }
}
