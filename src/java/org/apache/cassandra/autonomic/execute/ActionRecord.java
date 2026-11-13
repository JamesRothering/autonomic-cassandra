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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import org.apache.cassandra.autonomic.plan.Action;

/**
 * Local record of an action and its current state; immutable. Mirrors the columns of
 * {@code system_autonomic.actions} (overview 5.1). {@code Action.verify} is not persisted, so a record read back
 * from the store has a null verify spec. The time of each transition is in {@code action_events}; the record keeps
 * only the proposal time and the time of the latest transition.
 */
public final class ActionRecord
{
    public final Action action;
    public final ActionState state;
    public final long updatedMillis;
    /** Nullable: diagnosis that led to the action. */
    public final UUID diagnosisId;
    /** Nullable: name of the policy that proposed it. */
    public final String policy;
    public final boolean dryRun;
    /** Nullable. */
    public final String stateReason;
    /** {@code Applied.undo}; empty until applied. */
    public final Map<String, String> undo;
    public final Map<String, Double> baseline;
    public final Map<String, Double> result;
    /** Nullable. */
    public final Verdict verdict;
    public final long proposedMillis;

    public ActionRecord(Action action, ActionState state, long updatedMillis)
    {
        this(action, state, updatedMillis, null, null, false, null, Collections.emptyMap(), Collections.emptyMap(),
             Collections.emptyMap(), null, updatedMillis);
    }

    public ActionRecord(Action action, ActionState state, long updatedMillis, UUID diagnosisId, String policy, boolean dryRun,
                        String stateReason, Map<String, String> undo, Map<String, Double> baseline, Map<String, Double> result,
                        Verdict verdict, long proposedMillis)
    {
        this.action = Objects.requireNonNull(action);
        this.state = Objects.requireNonNull(state);
        this.updatedMillis = updatedMillis;
        this.diagnosisId = diagnosisId;
        this.policy = policy;
        this.dryRun = dryRun;
        this.stateReason = stateReason;
        this.undo = Collections.unmodifiableMap(new LinkedHashMap<>(undo));
        this.baseline = Collections.unmodifiableMap(new LinkedHashMap<>(baseline));
        this.result = Collections.unmodifiableMap(new LinkedHashMap<>(result));
        this.verdict = verdict;
        this.proposedMillis = proposedMillis;
    }

    public UUID id()
    {
        return action.id;
    }

    public ActionRecord withState(ActionState newState, String reason, long atMillis)
    {
        return new ActionRecord(action, newState, atMillis, diagnosisId, policy, dryRun, reason, undo, baseline, result, verdict, proposedMillis);
    }

    public ActionRecord withUndo(Map<String, String> newUndo)
    {
        return new ActionRecord(action, state, updatedMillis, diagnosisId, policy, dryRun, stateReason, newUndo, baseline, result, verdict, proposedMillis);
    }

    public ActionRecord withBaseline(Map<String, Double> newBaseline)
    {
        return new ActionRecord(action, state, updatedMillis, diagnosisId, policy, dryRun, stateReason, undo, newBaseline, result, verdict, proposedMillis);
    }

    public ActionRecord withResult(Map<String, Double> newResult, Verdict newVerdict)
    {
        return new ActionRecord(action, state, updatedMillis, diagnosisId, policy, dryRun, stateReason, undo, baseline, newResult, newVerdict, proposedMillis);
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof ActionRecord))
            return false;
        ActionRecord r = (ActionRecord) o;
        return action.equals(r.action) && state == r.state && updatedMillis == r.updatedMillis
               && Objects.equals(diagnosisId, r.diagnosisId) && Objects.equals(policy, r.policy) && dryRun == r.dryRun
               && Objects.equals(stateReason, r.stateReason) && undo.equals(r.undo) && baseline.equals(r.baseline)
               && result.equals(r.result) && verdict == r.verdict && proposedMillis == r.proposedMillis;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(action, state, updatedMillis, diagnosisId, policy, dryRun, stateReason, undo, baseline, result, verdict, proposedMillis);
    }

    @Override
    public String toString()
    {
        return "ActionRecord{" + action.id + " " + state + '}';
    }
}
