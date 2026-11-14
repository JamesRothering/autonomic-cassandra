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

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.OptionalDouble;

import org.apache.cassandra.autonomic.analyze.ThresholdDetector;
import org.apache.cassandra.autonomic.knowledge.KnowledgeView;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.monitor.sensors.Signals;

/**
 * Base of the detectors that compare a peer with the other peers. Values come from gossip and the dynamic snitch
 * through {@code PeerSensor}: gossiped LOAD is refreshed by the peer about once a minute and the sensor re-emits the
 * last value on every collection, so a peer value can be older than its observation time says. Persistence counts
 * collections, not gossip rounds, and a dead or partitioned peer keeps its last value until gossip drops it. These
 * detectors are therefore slow, approximate signals; they never act on a single reading.
 */
abstract class PeerDetector extends ThresholdDetector
{
    /** Peers needed for a comparison to mean anything. */
    public static final String MIN_PEERS = "min_peers";
    public static final int DEFAULT_MIN_PEERS = 3;

    protected final int minPeers;

    PeerDetector(String name, SignalId signal, Settings settings, int minPeers)
    {
        super(name, name, signal, settings);
        this.minPeers = minPeers;
    }

    static int minPeers(Map<String, Object> options)
    {
        Object o = options == null ? null : options.get(MIN_PEERS);
        return o instanceof Number ? Math.max(2, ((Number) o).intValue()) : DEFAULT_MIN_PEERS;
    }

    /** Values of {@code signal} for the peers gossip considers alive (liveness unknown counts as alive). */
    protected Map<Subject, Double> alivePeerValues(SignalId signal, KnowledgeView knowledge)
    {
        Map<Subject, Double> out = new HashMap<>();
        for (Subject peer : knowledge.subjects(signal))
        {
            if (peer.kind != Subject.Kind.PEER || latestWithin(Signals.PEER_ALIVE, peer, knowledge).orElse(1) <= 0)
                continue;
            OptionalDouble v = latestWithin(signal, peer, knowledge);
            if (v.isPresent() && !Double.isNaN(v.getAsDouble()))
                out.put(peer, v.getAsDouble());
        }
        return out;
    }

    protected static double median(Iterable<Double> values)
    {
        double[] a = new double[0];
        int n = 0;
        for (double v : values)
        {
            if (n == a.length)
                a = Arrays.copyOf(a, Math.max(8, n * 2));
            a[n++] = v;
        }
        if (n == 0)
            return Double.NaN;
        Arrays.sort(a, 0, n);
        return n % 2 == 1 ? a[n / 2] : (a[n / 2 - 1] + a[n / 2]) / 2;
    }
}
