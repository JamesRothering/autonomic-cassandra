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

import org.apache.cassandra.autonomic.coord.Claim;
import org.apache.cassandra.autonomic.coord.Lane;
import org.apache.cassandra.autonomic.plan.Action;
import org.apache.cassandra.autonomic.plan.ActionScope;

/**
 * A cluster-scoped actuator (overview 6.4, amended by FC2, FC4 and FC5). Its action runs under a {@link Claim} on
 * its {@link #lane} of the cluster ledger, as a sequence of bounded steps on the actuator thread of the claim owner.
 *
 * <p>Contract:
 * <ul>
 * <li>Every step must return within {@code ctx.stepDeadlineMillis()} and must be idempotent: after an ambiguous
 *     commit, a restart or a takeover it is called again with the same claim. Progress that must survive the owner
 *     belongs in the claim's phase and state (or in metadata the actuator owns), not in memory.</li>
 * <li>The actuator owns verification (FC4): it returns {@link StepResult#committed} with its verdict or
 *     {@link StepResult#rolledBack}. No baseline, {@code MetricVerifier} or dead-man deadline of the node-scoped
 *     path applies; lease expiry and {@link #recover} take the role of the dead-man rollback. {@code apply},
 *     {@code verify}, {@code rollback} and {@code overridden} of {@link Actuator} are never called.</li>
 * <li>When the claim is lost (expired, taken over or force-released) no method is called again for that action:
 *     the local record becomes FAILED with reason CLAIM_LOST and the new owner's {@link #recover} decides.</li>
 * </ul>
 */
public interface ClusterActuator extends Actuator
{
    Lane lane();

    @Override
    default ActionScope scope()
    {
        return ActionScope.CLUSTER;
    }

    /** One bounded step of the action in the claim's current phase. Owner only. */
    StepResult step(Action action, Claim claim, ActionContext ctx) throws Exception;

    /**
     * One bounded step of recovery. Called on a new owner after lease expiry, with its claim that replaced the
     * expired one (phase {@link Claim#RECOVERING}, actuator, target and state of the expired claim), and on the
     * owner itself after a restart with its own claim as it was. Until it returns a terminal result it is called
     * again with the then current claim. Must leave the cluster in the pre-action state or complete the action;
     * the action that was claimed is not available, only the claim.
     */
    StepResult recover(Claim claim, ActionContext ctx) throws Exception;

    /**
     * One bounded step of stopping the action on its owner, after a kill, a shutdown or a step that threw. Called
     * only while {@link #pastCommitPoint} is false. Until it returns a terminal result it is called again.
     * Default: the recovery path.
     */
    default StepResult abort(Action action, Claim claim, ActionContext ctx) throws Exception
    {
        return recover(claim, ctx);
    }

    /**
     * FC5: true if the action in this phase can no longer be undone. A kill, a shutdown or a restart of the owner
     * then releases the claim as COMMITTED_CLEANUP_PENDING without calling {@link #abort} or {@link #recover}.
     * Must be a pure function of the claim.
     */
    default boolean pastCommitPoint(Claim claim)
    {
        return false;
    }

    /** Called on every node where the layer runs when the claim's phase changes; for work that must happen locally. */
    default void onPhase(Claim claim, ActionContext ctx)
    {
    }

    @Override
    default Applied apply(Action action, ActionContext ctx)
    {
        throw new UnsupportedOperationException("cluster actuators are driven through step()");
    }

    @Override
    default void rollback(Action action, Applied applied, ActionContext ctx)
    {
        throw new UnsupportedOperationException("cluster actuators are driven through step()");
    }

    @Override
    default boolean overridden(Action action, Applied applied)
    {
        return false;
    }
}
