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
package org.apache.cassandra.autonomic.safety;

import java.util.Optional;

import org.apache.cassandra.autonomic.plan.Action;

/**
 * T2.8 seam: the parts of the governor's checks that need the cluster ledger. Consulted for cluster-scoped actions
 * only. Until the ledger exists the gate is {@link #CLOSED}, so no cluster-scoped action passes the governor.
 */
public interface ClusterGate
{
    /** The fourth gate of overview 10.1: {@code nodetool autonomic cluster enable} was run. */
    boolean enabled();

    /**
     * Check 5 for the action's lane (FC3): no in-progress sequences and every directory member alive where the lane
     * requires it. Returns a rejection detail, or empty.
     */
    Optional<String> nodeState(Action action);

    /** Check 10: at most one cluster action per lane, from the ledger. Returns a rejection detail, or empty. */
    Optional<String> concurrency(Action action);

    ClusterGate CLOSED = new ClusterGate()
    {
        @Override
        public boolean enabled()
        {
            return false;
        }

        @Override
        public Optional<String> nodeState(Action action)
        {
            return Optional.of("no cluster ledger");
        }

        @Override
        public Optional<String> concurrency(Action action)
        {
            return Optional.of("no cluster ledger");
        }
    };
}
