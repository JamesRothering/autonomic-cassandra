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

/** {@code state_reason} values written by the governor and the executor. */
public final class Reasons
{
    private Reasons()
    {
    }

    // governor checks of overview 7.1, in evaluation order
    public static final String KILLED = "KILLED";
    public static final String PAUSED = "PAUSED";
    public static final String NOT_ARMED = "NOT_ARMED";
    public static final String DRY_RUN = "DRY_RUN";
    public static final String ACTUATOR_DISABLED = "ACTUATOR_DISABLED";
    /** The fourth gate of overview 10.1 for cluster-scoped actions; evaluated with check 3. */
    public static final String CLUSTER_DISABLED = "CLUSTER_DISABLED";
    public static final String VALIDATION = "VALIDATION";
    public static final String NODE_STATE = "NODE_STATE";
    public static final String STORE_UNAVAILABLE = "STORE_UNAVAILABLE";
    public static final String QUARANTINED = "QUARANTINED";
    public static final String COOLDOWN = "COOLDOWN";
    public static final String CONFLICT = "CONFLICT";
    public static final String CONCURRENCY = "CONCURRENCY";
    public static final String RATE = "RATE";
    public static final String BLAST_RADIUS = "BLAST_RADIUS";
    public static final String APPROVAL = "APPROVAL";

    // rejections by the executor
    public static final String INSUFFICIENT_BASELINE = "INSUFFICIENT_BASELINE";
    public static final String EXECUTOR_BUSY = "EXECUTOR_BUSY";
    public static final String PROPOSAL_TTL = "PROPOSAL_TTL";
    public static final String OPERATOR_REJECTED = "OPERATOR_REJECTED";
    public static final String NO_CLUSTER_EXECUTOR = "NO_CLUSTER_EXECUTOR";
    public static final String GOVERNOR_ERROR = "GOVERNOR_ERROR";

    // rollback reasons; a verdict name (NEUTRAL, REGRESSED, INCONCLUSIVE) is also one
    public static final String KILL = "KILL";
    public static final String SHUTDOWN = "SHUTDOWN";
    public static final String DEADLINE = "DEADLINE";
    public static final String EARLY_ABORT = "EARLY_ABORT";
    public static final String APPLY_FAILED = "APPLY_FAILED";
    public static final String RESTART = "RESTART";

    // other terminal reasons
    public static final String ROLLBACK_FAILED = "ROLLBACK_FAILED";
    public static final String STOPPED_WITHOUT_ROLLBACK = "STOPPED_WITHOUT_ROLLBACK";
    public static final String OVERRIDDEN = "OVERRIDDEN";
    public static final String IRREVERSIBLE = "IRREVERSIBLE";

    // cluster-scoped actions
    /** The claim was not won: the lane is taken, a cooldown applies, or the commit did not go through. */
    public static final String CLAIM_REJECTED = "CLAIM_REJECTED";
    /** The claim expired, was taken over or was force-released; the action stopped without further side effects. */
    public static final String CLAIM_LOST = "CLAIM_LOST";
    /** Terminal COMMITTED of an action stopped past its commit point with cleanup outstanding (FC5). */
    public static final String COMMITTED_CLEANUP_PENDING = "COMMITTED_CLEANUP_PENDING";
    /** Rolled back by the recovery of a claim taken over from another node. */
    public static final String RECOVERY = "RECOVERY";
    /** Rolled back by the cluster actuator's own verification. */
    public static final String ACTUATOR_ROLLBACK = "ACTUATOR_ROLLBACK";
    /** A cluster actuator ended its action as failed; the claim was released as FAILED. */
    public static final String ACTUATOR_FAILED = "ACTUATOR_FAILED";
    /** Abort or recovery of a cluster action kept throwing; its claim is left to expire. */
    public static final String RECOVERY_FAILED = "RECOVERY_FAILED";
    // self-pause reasons
    public static final String CIRCUIT_BREAKER = "CIRCUIT_BREAKER";
    public static final String ACTION_FAILED = "ACTION_FAILED";
    public static final String ACTUATOR_STALLED = "ACTUATOR_STALLED";

    /** True for rollbacks an operator caused; they do not count against the policy or the circuit breaker. */
    public static boolean operatorCaused(String rollbackReason)
    {
        return KILL.equals(rollbackReason) || SHUTDOWN.equals(rollbackReason) || RESTART.equals(rollbackReason);
    }
}
