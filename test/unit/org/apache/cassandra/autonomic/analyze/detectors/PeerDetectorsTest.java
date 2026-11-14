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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

import org.apache.cassandra.autonomic.AutonomicConfig;
import org.apache.cassandra.autonomic.analyze.Detector;
import org.apache.cassandra.autonomic.analyze.Diagnosis;
import org.apache.cassandra.autonomic.analyze.Severity;
import org.apache.cassandra.autonomic.analyze.ThresholdDetector;
import org.apache.cassandra.autonomic.knowledge.InMemoryKnowledgeView;
import org.apache.cassandra.autonomic.knowledge.ObservationStore;
import org.apache.cassandra.autonomic.monitor.Observation;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.autonomic.monitor.sensors.Signals;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** node_imbalance, degraded_peer and degraded_self with synthetic peer signals. */
public class PeerDetectorsTest
{
    private static final long TICK = 15_000;
    private static final long GIB = 1L << 30;
    private static final Subject[] PEERS = { Subject.peer("p1"), Subject.peer("p2"), Subject.peer("p3"), Subject.peer("p4") };

    private ObservationStore store;
    private long now;
    private InMemoryKnowledgeView view;

    @Before
    public void setUp()
    {
        store = new ObservationStore(32, 1000);
        now = 1_000_000;
        view = new InMemoryKnowledgeView(store, () -> now);
    }

    private void put(SignalId signal, Subject subject, double value)
    {
        store.accept(new Observation(signal, subject, now, value));
    }

    private static Map<String, Object> opts(Object... kv)
    {
        Map<String, Object> m = new HashMap<>();
        for (int i = 0; i < kv.length; i += 2)
            m.put((String) kv[i], kv[i + 1]);
        return m;
    }

    /** One tick: a fresh sample of {@code signal} for each peer, then one evaluation. */
    private List<Diagnosis> tick(Detector d, SignalId signal, double... perPeer)
    {
        now += TICK;
        for (int i = 0; i < perPeer.length; i++)
            put(signal, PEERS[i], perPeer[i]);
        return d.analyze(view);
    }

    // ---- node_imbalance ----

    @Test
    public void imbalancedPeerIsNamedAfterPersistence()
    {
        Detector d = NodeImbalanceDetector.fromConfig(opts("for_evaluations", 2));
        // mean = 5 GiB, p4 at 8 GiB is 1.6x
        assertTrue(tick(d, Signals.PEER_LOAD_BYTES, 4 * GIB, 4 * GIB, 4 * GIB, 8 * GIB).isEmpty());
        List<Diagnosis> out = tick(d, Signals.PEER_LOAD_BYTES, 4 * GIB, 4 * GIB, 4 * GIB, 8 * GIB);
        assertEquals(1, out.size());
        Diagnosis x = out.get(0);
        assertEquals("node_imbalance", x.type);
        assertEquals(PEERS[3], x.subject);
        assertEquals(Severity.WARN, x.severity);
        assertEquals("p4", x.evidence.get("peer"));
        assertEquals("false", x.evidence.get("local"));
        assertEquals(Long.toString(8 * GIB), x.evidence.get("peer.load_bytes"));
        assertEquals(Long.toString(5 * GIB), x.evidence.get("mean_peer_load_bytes"));
    }

    @Test
    public void localNodeCanBeTheOutlier()
    {
        ThresholdDetector d = new NodeImbalanceDetector(ThresholdDetector.Settings.above(1.5), 3, GIB, () -> "p1");
        List<Diagnosis> out = tick(d, Signals.PEER_LOAD_BYTES, 12 * GIB, 4 * GIB, 4 * GIB);
        assertTrue(out.isEmpty());
        for (int i = 0; i < 3; i++)
            out = tick(d, Signals.PEER_LOAD_BYTES, 12 * GIB, 4 * GIB, 4 * GIB);
        assertEquals(1, out.size());
        assertEquals(PEERS[0], out.get(0).subject);
        assertEquals("true", out.get(0).evidence.get("local"));
        assertTrue(out.get(0).summary.startsWith("This node"));
    }

    @Test
    public void balancedOrSmallOrTooFewPeersDoNotTrigger()
    {
        Detector d = NodeImbalanceDetector.fromConfig(opts("for_evaluations", 1));
        assertTrue(tick(d, Signals.PEER_LOAD_BYTES, 5 * GIB, 5 * GIB, 6 * GIB, 6 * GIB).isEmpty());
        // large ratio but mean under min_load_bytes
        assertTrue(tick(d, Signals.PEER_LOAD_BYTES, 1000, 1000, 1000, 9000).isEmpty());
        // only two peers
        now += 200_000; // earlier samples of the other peers are stale
        assertTrue(tick(d, Signals.PEER_LOAD_BYTES, 2 * GIB, 20 * GIB).isEmpty());
    }

    @Test
    public void deadPeerIsIgnoredAndClears()
    {
        Detector d = NodeImbalanceDetector.fromConfig(opts("for_evaluations", 1));
        assertEquals(1, tick(d, Signals.PEER_LOAD_BYTES, 4 * GIB, 4 * GIB, 4 * GIB, 12 * GIB).size());
        now += TICK;
        for (int i = 0; i < 4; i++)
            put(Signals.PEER_LOAD_BYTES, PEERS[i], i == 3 ? 12 * GIB : 4 * GIB);
        put(Signals.PEER_ALIVE, PEERS[3], 0);
        assertTrue(d.analyze(view).isEmpty());
    }

    // ---- degraded_peer ----

    @Test
    public void degradedPeerByScoreAgainstMedian()
    {
        Detector d = DegradedPeerDetector.fromConfig(opts("for_evaluations", 2));
        assertTrue(tick(d, Signals.PEER_SNITCH_SCORE, 0.1, 0.1, 0.12, 0.1).isEmpty());
        assertTrue(tick(d, Signals.PEER_SNITCH_SCORE, 0.1, 0.1, 0.12, 0.1).isEmpty());
        assertTrue(tick(d, Signals.PEER_SNITCH_SCORE, 0.1, 0.1, 0.12, 0.5).isEmpty()); // 1 of 2
        put(Signals.PEER_SEVERITY, PEERS[3], 0.5);
        List<Diagnosis> out = tick(d, Signals.PEER_SNITCH_SCORE, 0.1, 0.1, 0.12, 0.5);
        assertEquals(1, out.size());
        Diagnosis x = out.get(0);
        assertEquals("degraded_peer", x.type);
        assertEquals(PEERS[3], x.subject);
        assertEquals(Severity.CRITICAL, x.severity); // 4.5x median 0.11
        assertEquals("0.5000", x.evidence.get("peer.snitch_score"));
        assertEquals("0.1100", x.evidence.get("median_snitch_score"));
        assertEquals("0.5000", x.evidence.get("peer.severity"));
    }

    @Test
    public void healthyPeersAndTooFewPeersDoNotTrigger()
    {
        Detector d = DegradedPeerDetector.fromConfig(opts("for_evaluations", 1));
        assertTrue(tick(d, Signals.PEER_SNITCH_SCORE, 0.1, 0.12, 0.15, 0.19).isEmpty());
        now += 200_000;
        assertTrue(tick(d, Signals.PEER_SNITCH_SCORE, 0.1, 0.9).isEmpty());
    }

    // ---- degraded_self ----

    private Detector self(Object... kv)
    {
        return DegradedSelfDetector.fromConfig(opts(kv), new AutonomicConfig().slo); // 50ms read, 20ms write
    }

    private List<Diagnosis> selfTick(Detector d, double readP99, double... peerScores)
    {
        now += TICK;
        put(Signals.CLIENT_READ_LATENCY_P99_MICROS, Subject.node(), readP99);
        for (int i = 0; i < peerScores.length; i++)
            put(Signals.PEER_SNITCH_SCORE, PEERS[i], peerScores[i]);
        return d.analyze(view);
    }

    @Test
    public void degradedSelfWhenSlowAndPeersLookHealthy()
    {
        Detector d = self("for_evaluations", 2);
        assertTrue(selfTick(d, 120_000, 0.1, 0.1, 0.12).isEmpty());
        List<Diagnosis> out = selfTick(d, 120_000, 0.1, 0.1, 0.12);
        assertEquals(1, out.size());
        Diagnosis x = out.get(0);
        assertEquals("degraded_self", x.type);
        assertEquals(Subject.node(), x.subject);
        assertEquals(Severity.CRITICAL, x.severity); // 2.4x SLO
        assertEquals("0", x.evidence.get("peers_degraded"));
        assertEquals("3", x.evidence.get("peers_scored"));
        assertTrue(x.evidence.get("client.read_latency_p99_micros").startsWith("120000 us"));
    }

    @Test
    public void notBlamedWhenAPeerIsTheLikelyCauseOrWhenWithinSlo()
    {
        Detector d = self("for_evaluations", 1);
        assertTrue(selfTick(d, 120_000, 0.1, 0.1, 0.5).isEmpty()); // a degraded peer explains it
        now += 200_000;
        assertTrue(selfTick(d, 120_000, 0.1, 0.1).isEmpty());      // too few peers to compare
        assertTrue(selfTick(d, 20_000, 0.1, 0.1, 0.12).isEmpty()); // fast enough
    }

    @Test
    public void buildRegistersPeerDetectors()
    {
        AutonomicConfig c = new AutonomicConfig();
        c.detectors.put("node_imbalance", opts("enabled", false));
        List<Detector> built = Detectors.build(c);
        assertEquals(9, built.size());
        assertTrue(built.stream().noneMatch(x -> x.name().equals("node_imbalance")));
        assertTrue(built.stream().anyMatch(x -> x.name().equals("degraded_peer")));
        assertTrue(built.stream().anyMatch(x -> x.name().equals("degraded_self")));
    }
}
