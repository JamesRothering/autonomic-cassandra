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
package org.apache.cassandra.autonomic.layout;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BooleanSupplier;

import org.apache.cassandra.autonomic.AutonomicConfig;
import org.apache.cassandra.autonomic.coord.Claim;
import org.apache.cassandra.autonomic.execute.ActionContext;
import org.apache.cassandra.autonomic.execute.StepResult;
import org.apache.cassandra.schema.TableId;

/**
 * The owner steps that the split ({@code PartitionSplitActuator}) and the unsplit ({@code PartitionUnsplitActuator})
 * share (design 4.1, 11.4): rule O1 and the acknowledgement time kept in the claim, the deadline arithmetic around
 * the commit point, and the driving of an equivalence check in bounded steps.
 */
public final class LayoutOwnerSteps
{
    /** {@code <phase>/<phase epoch>/<millis>}: when every node had acknowledged that phase epoch. */
    public static final String S_ACKED = "acked";

    /** Two all-node acknowledgements come between the commit point and the straggler wait. */
    public static final int COMMIT_ACK_POLLS = 2;

    private LayoutOwnerSteps()
    {
    }

    public static long pollMillis(AutonomicConfig.LayoutConfig lc)
    {
        return Math.max(1, lc.ack_poll_interval.toMilliseconds());
    }

    // ---------------------------------------------------------------------------------------------------------
    // rule O1 and the acknowledgement time
    // ---------------------------------------------------------------------------------------------------------

    /**
     * O1, then the transition with the claim advanced in the same commit: {@code AdvanceLayout} from the key's phase
     * to {@code to} at {@code generation}, or a STAY while {@code acknowledged} is false for the key's phase epoch.
     */
    public static StepResult advance(Claim claim, TableId logical, ByteBuffer keyBytes, KeyState key, Phase to, int generation, BooleanSupplier acknowledged,
                                     long nowMillis, long leaseMillis, long pollMillis)
    {
        if (!acknowledged.getAsBoolean())
            return StepResult.stay(pollMillis);
        AdvanceLayout advance = new AdvanceLayout(claim.actionId, claim.owner, claim.step, logical, keyBytes, key.phase, to, generation, nowMillis, leaseMillis);
        return StepResult.commit(advance.wrap(), to.name(), claim.state, 0);
    }

    /**
     * Null once the time at which every node acknowledged the key's current phase epoch is in the claim; otherwise the
     * step that waits for the acknowledgement or records it.
     */
    public static StepResult ackedFor(Claim claim, KeyState key, BooleanSupplier acknowledged, long nowMillis, long pollMillis)
    {
        if (ackedAt(claim, key) >= 0)
            return null;
        if (!acknowledged.getAsBoolean())
            return StepResult.stay(pollMillis);
        Map<String, String> state = new TreeMap<>(claim.state);
        state.put(S_ACKED, ackPrefix(key) + nowMillis);
        return StepResult.advance(claim.phase, state, 0);
    }

    public static String ackPrefix(KeyState key)
    {
        return key.phase.name() + '/' + key.phaseEpoch.getEpoch() + '/';
    }

    /** -1 if the claim has no acknowledgement time of the key's current phase epoch. */
    public static long ackedAt(Claim claim, KeyState key)
    {
        String acked = claim.state.get(S_ACKED);
        String prefix = ackPrefix(key);
        if (acked == null || !acked.startsWith(prefix))
            return -1;
        return Long.parseLong(acked.substring(prefix.length()));
    }

    // ---------------------------------------------------------------------------------------------------------
    // deadlines
    // ---------------------------------------------------------------------------------------------------------

    /** The time at which the replicas stop copying for a forward migration (ADR-031, {@code BackfillTask.pastDeadline}). */
    public static long forwardReplicaLimit(KeyState key, int gcGraceSeconds, long writeTimeoutMillis)
    {
        long limit = key.backfillLimitMillis(gcGraceSeconds);
        return limit == Long.MIN_VALUE ? Long.MIN_VALUE : limit - writeTimeoutMillis;
    }

    /**
     * What has to fit between a commit point and the replicas' limit: {@code straggler_window}, {@code reconcile_budget}
     * and {@link #COMMIT_ACK_POLLS} acknowledgement polls.
     */
    public static long afterCommitMillis(AutonomicConfig.LayoutConfig lc, long pollMillis)
    {
        return lc.straggler_window.toMilliseconds() + lc.reconcile_budget.toMilliseconds() + COMMIT_ACK_POLLS * pollMillis;
    }

    /** The latest time of a commit point for a replicas' limit, {@link Long#MIN_VALUE} if the limit is. */
    public static long commitWindowEnd(long replicaLimit, AutonomicConfig.LayoutConfig lc, long pollMillis)
    {
        if (replicaLimit == Long.MIN_VALUE)
            return Long.MIN_VALUE;
        return replicaLimit - afterCommitMillis(lc, pollMillis);
    }

    // ---------------------------------------------------------------------------------------------------------
    // driving an equivalence check
    // ---------------------------------------------------------------------------------------------------------

    /** One equivalence check; {@link LayoutVerifier} in production. */
    public interface Check
    {
        LayoutVerifier.Status step();

        /** See {@link LayoutVerifier#nextDueMillis}. */
        long nextDueMillis();

        long count(LayoutVerifier.Kind kind);

        boolean tooManyDifferences();

        String summary();
    }

    /** What one bounded run of a check came to. */
    public static final class Run
    {
        /** True if the check finished; its counts are then final. */
        public final boolean done;
        /** True if a read failed: the check is to be started again. */
        public final boolean failed;
        /** For a check still running: the delay of the next step. */
        public final long delayMillis;
        public final RuntimeException error;

        private Run(boolean done, boolean failed, long delayMillis, RuntimeException error)
        {
            this.done = done;
            this.failed = failed;
            this.delayMillis = delayMillis;
            this.error = error;
        }
    }

    /**
     * Runs the check under {@code action_step_timeout}: pages until half of the step's time is used, the check waits
     * for a recheck, or the action is killed.
     */
    public static Run run(Check check, ActionContext ctx, long pollMillis)
    {
        LayoutVerifier.Status status;
        long budgetEnd = ctx.nowMillis() + Math.max(1, (ctx.stepDeadlineMillis() - ctx.nowMillis()) / 2);
        try
        {
            do
            {
                status = check.step();
            }
            while (status == LayoutVerifier.Status.RUNNING && check.nextDueMillis() == 0 && ctx.nowMillis() < budgetEnd && !ctx.killed());
        }
        catch (RuntimeException e)
        {
            // a read at ALL that failed (a replica down or slow): start over later; the deadline bounds the wait
            return new Run(false, true, pollMillis, e);
        }
        if (status == LayoutVerifier.Status.RUNNING)
        {
            long due = check.nextDueMillis();
            return new Run(false, false, due == 0 ? 0 : Math.max(1, Math.min(pollMillis, due - ctx.nowMillis())), null);
        }
        return new Run(true, false, 0, null);
    }
}
