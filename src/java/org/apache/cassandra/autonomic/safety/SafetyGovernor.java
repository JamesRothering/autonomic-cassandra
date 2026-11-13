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

import org.apache.cassandra.autonomic.execute.ActionOutcome;
import org.apache.cassandra.autonomic.execute.ActionRecord;
import org.apache.cassandra.autonomic.execute.ActionState;
import org.apache.cassandra.autonomic.knowledge.KnowledgeView;
import org.apache.cassandra.autonomic.plan.Action;

/**
 * Safety. Single decision point; see overview 7.
 *
 * <p>An {@code APPROVE} or {@code HOLD_FOR_APPROVAL} decision reserves the action (it then counts for the conflict,
 * concurrency and rate checks) until {@link #recordOutcome} is called for it with a terminal state; the caller must
 * always make that call, with {@code REJECTED} if the action is never applied.
 */
public interface SafetyGovernor
{
    /** As {@link #evaluate(ActionRecord, KnowledgeView)} for a proposal with no policy and no diagnosis. */
    default Decision evaluate(Action action, KnowledgeView knowledge)
    {
        return evaluate(new ActionRecord(action, ActionState.PROPOSED, knowledge.nowMillis()), knowledge);
    }

    /** Runs the checks of overview 7.1 in order; the first failing one rejects with its name as reason. */
    Decision evaluate(ActionRecord proposal, KnowledgeView knowledge);

    /**
     * Operator approval of a proposal that {@link #evaluate} held: checks 1 to 12 are evaluated again, check 13 is
     * satisfied. Never returns {@code HOLD_FOR_APPROVAL}; a rejection releases the reservation.
     */
    Decision approve(ActionRecord proposal, KnowledgeView knowledge);

    /** Feeds cooldowns, rate windows, quarantine and the circuit breaker; releases the reservation. */
    void recordOutcome(Action action, ActionOutcome outcome);

    /** Restart recovery, overview 7.6 step 4: the post-restart cooldown of an action that was in flight. */
    default void recordRestart(String dedupeKey)
    {
    }

    /** Restart recovery: an action a previous run applied counts in the rate windows if recent enough. */
    default void restoreApproval(Action action, long approvedMillis)
    {
    }

    /** Restart recovery: one {@code cooldown/*} or {@code quarantine/*} row of {@code local_state}. */
    default void restoreState(String key, String value)
    {
    }
}
