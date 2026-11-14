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
package org.apache.cassandra.autonomic.coord;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import org.apache.cassandra.autonomic.plan.Action;
import org.apache.cassandra.autonomic.safety.ClusterGate;
import org.apache.cassandra.tcm.membership.NodeId;

/**
 * The governor's cluster checks, read from the ledger and the metadata this node has. They only keep proposals
 * that cannot succeed away from the operator and the log; the binding decision is always {@code ClaimAction}.
 */
public class LedgerClusterGate implements ClusterGate
{
    private final ClusterCoordinator coordinator;
    /** Lane of a cluster actuator by name; null if the name is not a cluster actuator. */
    private final Function<String, Lane> laneOf;

    public LedgerClusterGate(ClusterCoordinator coordinator, Function<String, Lane> laneOf)
    {
        this.coordinator = coordinator;
        this.laneOf = laneOf;
    }

    @Override
    public boolean enabled()
    {
        return coordinator.enabled();
    }

    @Override
    public Optional<String> nodeState(Action action)
    {
        Lane lane = laneOf.apply(action.actuator);
        if (lane == null)
            return Optional.of(action.actuator + " is not a cluster actuator");
        if (coordinator.self() == null)
            return Optional.of("this node is not registered");
        if (lane.requiresNoInProgressSequences && coordinator.sequencesInProgress())
            return Optional.of("a metadata operation is in progress; lane " + lane + " requires none");
        if (lane == Lane.SCHEMA_LAYOUT)
        {
            // FC3
            List<NodeId> down = coordinator.membersNotAlive();
            if (!down.isEmpty())
                return Optional.of("lane " + lane + " requires every member alive; not alive: " + down);
        }
        if (!coordinator.preferredProposer())
            return Optional.of("this node is not the preferred proposer");
        return Optional.empty();
    }

    @Override
    public Optional<String> concurrency(Action action)
    {
        Lane lane = laneOf.apply(action.actuator);
        if (lane == null)
            return Optional.of(action.actuator + " is not a cluster actuator");
        // an expired claim also blocks: it must be taken over and recovered, not overwritten by a new action
        Claim held = coordinator.ledger().claims.get(lane);
        return held == null ? Optional.empty()
                            : Optional.of("lane " + lane + " is held by action " + held.actionId + " of node " + held.owner.id());
    }
}
