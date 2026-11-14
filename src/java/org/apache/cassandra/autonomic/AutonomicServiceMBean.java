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
package org.apache.cassandra.autonomic;

import java.util.List;
import java.util.Map;

/**
 * Operator interface of the autonomic layer (overview 10.2). Records are returned as maps of strings so that any
 * JMX client can read them.
 */
public interface AutonomicServiceMBean
{
    String MBEAN_NAME = "org.apache.cassandra.db:type=AutonomicService";

    /** One of DISABLED, STARTING, RUNNING, PAUSED. */
    String getState();

    /** State, gates and armed flag as strings. */
    Map<String, String> getStatus();

    boolean isEnabled();

    /** Starts or shuts down the layer at runtime. */
    void setEnabled(boolean enabled);

    boolean isDryRun();

    void setDryRun(boolean dryRun);

    /** Stops approval of new actions; sensors and detectors keep running. */
    void pause(String reason);

    /** Allows new actions again; clears a kill, the circuit breaker and disabled components. */
    void resume();

    /** Pause, plus immediate rollback of every in-flight node-scoped action. Survives a restart until resumed. */
    void kill(String reason);

    /** Restores every managed knob to its configured value. Not available until knob actuators exist. */
    void revert();

    /** Approves an action that is awaiting manual approval; the governor's other checks are evaluated again. */
    void approve(String actionId);

    /** Rejects an action that is awaiting manual approval. */
    void reject(String actionId);

    /** Per-actuator gate; overrides the configuration until restart. */
    void setActuatorEnabled(String actuator, boolean enabled);

    /** Actuator name to scope and gate. */
    Map<String, String> getActuators();

    /** Actions awaiting approval, queued or in flight on this node. */
    List<Map<String, String>> getInFlightActions();

    /** Most recent actions of this node, newest first, from the last seven days. */
    List<Map<String, String>> getHistory(int limit);

    /** Audit trail of one action, oldest first. */
    List<Map<String, String>> getActionEvents(String actionId);

    /** Diagnoses of the last day on this node, open and cleared. */
    List<Map<String, String>> getDiagnosisHistory(int limit);

    /**
     * {@code cluster enable | disable}: commits the gate for cluster-scoped actions to the cluster metadata log.
     * Enabling is the operator's attestation that every node runs this fork: from then on the log contains entries
     * that stock Cassandra cannot read. It is refused while nodes run different versions. Disabling stops new
     * claims; claims in flight are not touched, and the log stays unreadable for stock nodes. Works whether or not
     * the layer is enabled on this node.
     *
     * @throws IllegalStateException if refused or if the commit did not go through
     */
    void enableClusterActions(boolean enabled);

    /** True if cluster-scoped actions are enabled in the cluster metadata this node has. */
    boolean isClusterActionsEnabled();

    /**
     * {@code cluster release <lane>}: forcibly removes the claim on a lane (TOPOLOGY, SCHEMA_LAYOUT or MAINTENANCE)
     * and returns the id of the action that held it. Nothing is undone; the owner, if it still runs, stops with
     * CLAIM_LOST.
     *
     * @throws IllegalStateException if the lane is free or if the commit did not go through
     */
    String releaseClaim(String lane);

    /** {@code cluster status}: the gate, this node's role, and one {@code lane.<LANE>} entry per lane. */
    Map<String, String> getClusterStatus();

    /** Claims in the cluster ledger, one map per claimed lane. */
    List<Map<String, String>> getClusterClaims();

    /** Most recently released claims of the cluster ledger, newest first. */
    List<Map<String, String>> getClusterRecentActions();
}
