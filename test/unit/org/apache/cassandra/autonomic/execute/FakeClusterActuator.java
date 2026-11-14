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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.autonomic.coord.Claim;
import org.apache.cassandra.autonomic.coord.Lane;
import org.apache.cassandra.autonomic.knowledge.KnowledgeView;
import org.apache.cassandra.autonomic.plan.Action;

/**
 * Test-only cluster actuator. By default its action goes CLAIMED, P1, P2 and commits with verdict IMPROVED;
 * recovery and abort report a rollback. Every call is recorded as {@code kind:phase:step}.
 */
public class FakeClusterActuator implements ClusterActuator
{
    private final String name;
    public volatile Lane lane = Lane.MAINTENANCE;
    public final List<String> calls = Collections.synchronizedList(new ArrayList<>());
    public final Set<String> phasesPastCommitPoint = new HashSet<>();
    public volatile Function<Claim, StepResult> onStep = FakeClusterActuator::defaultStep;
    public volatile Function<Claim, StepResult> onRecover = claim -> StepResult.rolledBack("undone by recover");
    /** Null: the default of the interface, which is the recovery path. */
    public volatile Function<Claim, StepResult> onAbort;
    public volatile RuntimeException failStep;
    public volatile RuntimeException failRecover;

    public FakeClusterActuator(String name)
    {
        this.name = name;
    }

    public static StepResult defaultStep(Claim claim)
    {
        switch (claim.phase)
        {
            case ClusterActionExecution.INITIAL_PHASE:
                return StepResult.advance("P1", ImmutableMap.of("a", "1"), 0);
            case "P1":
                return StepResult.advance("P2", ImmutableMap.of("a", "1", "b", "2"), 1000);
            default:
                return StepResult.committed(Verdict.IMPROVED, "verified by the actuator");
        }
    }

    public int count(String kind)
    {
        int n = 0;
        synchronized (calls)
        {
            for (String call : calls)
                if (call.startsWith(kind + ':'))
                    n++;
        }
        return n;
    }

    @Override
    public String name()
    {
        return name;
    }

    @Override
    public Lane lane()
    {
        return lane;
    }

    @Override
    public boolean reversible()
    {
        return true;
    }

    @Override
    public Optional<String> validate(Action action, KnowledgeView knowledge)
    {
        return Optional.empty();
    }

    @Override
    public StepResult step(Action action, Claim claim, ActionContext ctx)
    {
        calls.add("step:" + claim.phase + ':' + claim.step);
        if (failStep != null)
            throw failStep;
        return onStep.apply(claim);
    }

    @Override
    public StepResult recover(Claim claim, ActionContext ctx)
    {
        calls.add("recover:" + claim.phase + ':' + claim.step);
        if (failRecover != null)
            throw failRecover;
        return onRecover.apply(claim);
    }

    @Override
    public StepResult abort(Action action, Claim claim, ActionContext ctx) throws Exception
    {
        if (onAbort == null)
            return ClusterActuator.super.abort(action, claim, ctx);
        calls.add("abort:" + claim.phase + ':' + claim.step + (ctx.killed() ? ":killed" : ""));
        return onAbort.apply(claim);
    }

    @Override
    public boolean pastCommitPoint(Claim claim)
    {
        return phasesPastCommitPoint.contains(claim.phase);
    }

    @Override
    public void onPhase(Claim claim, ActionContext ctx)
    {
        calls.add("onPhase:" + claim.phase + ':' + claim.step);
    }
}
