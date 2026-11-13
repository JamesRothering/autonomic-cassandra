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
package org.apache.cassandra.autonomic.monitor.sensors;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.cassandra.autonomic.monitor.ObservationSink;
import org.apache.cassandra.autonomic.monitor.Sensor;
import org.apache.cassandra.autonomic.monitor.SignalId;
import org.apache.cassandra.autonomic.monitor.Subject;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.gms.ApplicationState;
import org.apache.cassandra.gms.EndpointState;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.gms.VersionedValue;
import org.apache.cassandra.locator.DynamicEndpointSnitch;
import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.locator.NodeProximity;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.membership.Directory;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.utils.FBUtilities;

import static org.apache.cassandra.autonomic.monitor.sensors.SensorSupport.emit;

/**
 * Per remote endpoint: gossiped LOAD and SEVERITY, gossip liveness and dynamic snitch score. Reads only local gossip
 * state (no messages). Gossiped values can be stale; a peer that never gossiped a value gets no observation for it.
 * Endpoints are identified by host id (from the cluster directory) or, when unknown there, by address.
 */
public class PeerSensor implements Sensor
{
    private static final Set<SignalId> SIGNALS = Set.of(
        Signals.PEER_LOAD_BYTES, Signals.PEER_SEVERITY, Signals.PEER_ALIVE, Signals.PEER_SNITCH_SCORE);

    @Override
    public String name()
    {
        return "peer";
    }

    @Override
    public Set<SignalId> signals()
    {
        return SIGNALS;
    }

    @Override
    public long minIntervalMillis()
    {
        return 10_000;
    }

    @Override
    public void collect(ObservationSink sink, long now)
    {
        Directory directory = ClusterMetadata.current().directory;
        InetAddressAndPort self = FBUtilities.getBroadcastAddressAndPort();
        Map<String, Double> scores = snitchScores();
        for (Map.Entry<InetAddressAndPort, EndpointState> e : Gossiper.instance.getEndpointStates().entrySet())
        {
            InetAddressAndPort endpoint = e.getKey();
            if (endpoint.equals(self))
                continue;
            EndpointState state = e.getValue();
            Subject peer = Subject.peer(peerId(directory, endpoint));
            emit(sink, Signals.PEER_ALIVE, peer, now, state.isAlive() ? 1 : 0);
            emit(sink, Signals.PEER_LOAD_BYTES, peer, now, parse(state, ApplicationState.LOAD));
            emit(sink, Signals.PEER_SEVERITY, peer, now, parse(state, ApplicationState.SEVERITY));
            Double score = scores.get(endpoint.toString(true));
            if (score != null)
                emit(sink, Signals.PEER_SNITCH_SCORE, peer, now, score);
        }
    }

    /** Scores keyed by {@code InetAddressAndPort.toString(true)}; empty without a dynamic snitch. */
    private static Map<String, Double> snitchScores()
    {
        NodeProximity proximity = DatabaseDescriptor.getNodeProximity();
        if (proximity instanceof DynamicEndpointSnitch)
            return ((DynamicEndpointSnitch) proximity).getScoresWithPort();
        return Collections.emptyMap();
    }

    private static String peerId(Directory directory, InetAddressAndPort endpoint)
    {
        NodeId id = directory.peerId(endpoint);
        if (id != null)
        {
            UUID hostId = directory.hostId(id);
            if (hostId != null)
                return hostId.toString();
        }
        return endpoint.getHostAddressAndPort();
    }

    private static double parse(EndpointState state, ApplicationState key)
    {
        VersionedValue value = state.getApplicationState(key);
        if (value == null || value.value == null)
            return Double.NaN;
        try
        {
            return Double.parseDouble(value.value);
        }
        catch (NumberFormatException e)
        {
            return Double.NaN;
        }
    }
}
