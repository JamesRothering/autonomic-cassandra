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
import java.util.Objects;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.tcm.Transformation;

/**
 * What one bounded step of a {@link ClusterActuator} asks the layer to do next. Immutable.
 *
 * <p>A non-terminal result is carried out by a commit to the cluster ledger (except {@link #stay}); the step is
 * called again after {@code delayMillis} once that commit is visible. A terminal result releases the claim and ends
 * the local action (FC4: the actuator owns its verification and reports the verdict itself).
 */
public final class StepResult
{
    public enum Kind
    {
        /** No ledger change; call the step again later. */
        STAY,
        /** Commit {@code AdvanceAction} to the given phase and state. */
        ADVANCE,
        /** Commit the given transformation instead of {@code AdvanceAction} (FC2). */
        COMMIT,
        /** Terminal: release as COMMITTED. */
        COMMITTED,
        /** Terminal: release as COMMITTED_CLEANUP_PENDING; locally COMMITTED (FC5). */
        COMMITTED_CLEANUP_PENDING,
        /** Terminal: release as ROLLED_BACK. */
        ROLLED_BACK,
        /** Terminal: release as FAILED; the cluster is in a state an operator must judge. */
        FAILED
    }

    public final Kind kind;
    /** Phase and state the claim has after the commit; null for STAY and terminal results. */
    public final String phase;
    public final ImmutableMap<String, String> state;
    /** COMMIT only. */
    public final Transformation transformation;
    /** Terminal COMMITTED results; nullable. */
    public final Verdict verdict;
    /** Nullable; recorded in the audit trail. */
    public final String detail;
    public final long delayMillis;

    private StepResult(Kind kind, String phase, Map<String, String> state, Transformation transformation, Verdict verdict, String detail, long delayMillis)
    {
        this.kind = kind;
        this.phase = phase;
        this.state = state == null ? null : ImmutableMap.copyOf(state);
        this.transformation = transformation;
        this.verdict = verdict;
        this.detail = detail;
        this.delayMillis = Math.max(0, delayMillis);
    }

    /** Nothing to commit yet (for example, waiting for another node); the step runs again after the delay. */
    public static StepResult stay(long delayMillis)
    {
        return new StepResult(Kind.STAY, null, null, null, null, null, delayMillis);
    }

    /** Advance the claim to {@code phase} with {@code state}; the step runs again after the delay. */
    public static StepResult advance(String phase, Map<String, String> state, long delayMillis)
    {
        return new StepResult(Kind.ADVANCE, Objects.requireNonNull(phase), Objects.requireNonNull(state), null, null, null, delayMillis);
    }

    /**
     * FC2: commit {@code transformation} instead of {@code AdvanceAction}. The transformation must advance this
     * claim itself, with {@code ClusterLedger.withAdvancedClaim} and the step of the claim the step was given as
     * expected step, to exactly {@code phase} and {@code state}; that is how the layer recognises the commit in the
     * resulting ledger. It must be in the form in which it is written to the log (a custom transformation already
     * wrapped in {@code CustomTransformation}).
     */
    public static StepResult commit(Transformation transformation, String phase, Map<String, String> state, long delayMillis)
    {
        return new StepResult(Kind.COMMIT, Objects.requireNonNull(phase), Objects.requireNonNull(state),
                              Objects.requireNonNull(transformation), null, null, delayMillis);
    }

    /** The action is complete and verified by the actuator; {@code verdict} is recorded. */
    public static StepResult committed(Verdict verdict, String detail)
    {
        return new StepResult(Kind.COMMITTED, null, null, null, verdict, detail, 0);
    }

    /** Past its commit point and stopped before cleanup finished; safe to leave indefinitely (FC5). */
    public static StepResult committedCleanupPending(String detail)
    {
        return new StepResult(Kind.COMMITTED_CLEANUP_PENDING, null, null, null, null, detail, 0);
    }

    /** The cluster is back in its pre-action state. */
    public static StepResult rolledBack(String detail)
    {
        return new StepResult(Kind.ROLLED_BACK, null, null, null, null, detail, 0);
    }

    /**
     * The action can neither be completed nor undone by the layer (for example a join that started and did not
     * finish). The claim is released as FAILED, the local record becomes FAILED, a CRITICAL diagnosis with
     * {@code detail} is raised and the layer pauses, as for every FAILED action (overview 7.2).
     */
    public static StepResult failed(String detail)
    {
        return new StepResult(Kind.FAILED, null, null, null, null, detail, 0);
    }

    public boolean isTerminal()
    {
        return kind == Kind.COMMITTED || kind == Kind.COMMITTED_CLEANUP_PENDING || kind == Kind.ROLLED_BACK || kind == Kind.FAILED;
    }

    @Override
    public String toString()
    {
        return "StepResult{" + kind + (phase == null ? "" : " " + phase) + (detail == null ? "" : " " + detail) + '}';
    }
}
