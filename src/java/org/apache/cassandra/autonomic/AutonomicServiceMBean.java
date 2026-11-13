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
 * Operator interface of the autonomic layer (overview 10.2). Cluster operations ({@code enableClusterActions},
 * {@code releaseClaim}) are added with the cluster ledger. Records are returned as maps of strings so that any JMX
 * client can read them.
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
}
