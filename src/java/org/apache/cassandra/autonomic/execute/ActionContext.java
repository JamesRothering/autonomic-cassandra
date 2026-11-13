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
package org.apache.cassandra.autonomic.execute;

import java.util.Map;

/**
 * Passed to {@code Actuator.apply} and {@code rollback}; valid for one step on the actuator thread only.
 *
 * <p>T2.8 seam: step rescheduling and the cluster-claim handle of cluster actuators are added here (or in a
 * sub-interface handed to {@code ClusterActuator.step}); node-scoped actuators never need them.
 */
public interface ActionContext
{
    /** Time on the layer's clock by which the current step must have returned ({@code action_step_timeout}). */
    long stepDeadlineMillis();

    /** The layer's clock. */
    long nowMillis();

    /** True once kill or shutdown was requested; a long step should return or throw as soon as it safely can. */
    boolean killed();

    /**
     * Write-ahead of undo information in memory. {@code apply} must call this with everything rollback needs
     * <em>before</em> its first change if it can throw after changing something: when {@code apply} throws, the
     * executor calls {@code rollback} with the last recorded undo, and assumes nothing changed if none was recorded.
     */
    void recordUndo(Map<String, String> undo);
}
