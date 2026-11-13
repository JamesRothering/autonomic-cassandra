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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.apache.cassandra.autonomic.knowledge.KnowledgeView;
import org.apache.cassandra.autonomic.plan.Action;
import org.apache.cassandra.autonomic.plan.ActionScope;

/**
 * Test-only actuator over one in-memory knob. {@code params.value} is the value to set. Satisfies the contract of
 * overview 7.3 rules 1 to 4 unless a failure is injected.
 */
public class FakeKnobActuator implements Actuator
{
    public enum Fail { NONE, BEFORE_EFFECT, AFTER_PARTIAL_EFFECT }

    private final String name;
    public volatile long value = 64;
    public volatile ActionScope scope = ActionScope.NODE;
    public volatile boolean reversible = true;
    public volatile Fail failApply = Fail.NONE;
    public volatile boolean failRollback;
    public volatile String invalid;
    public volatile String blastRadius;
    public volatile Verdict verdict;
    public volatile Runnable duringApply = () -> {};
    public final List<String> calls = new ArrayList<>();
    private final Map<UUID, Applied> applied = new HashMap<>();

    public FakeKnobActuator(String name)
    {
        this.name = name;
    }

    @Override
    public String name()
    {
        return name;
    }

    @Override
    public ActionScope scope()
    {
        return scope;
    }

    @Override
    public boolean reversible()
    {
        return reversible;
    }

    @Override
    public Optional<String> validate(Action action, KnowledgeView knowledge)
    {
        return Optional.ofNullable(invalid);
    }

    @Override
    public Optional<String> blastRadius(Action action, KnowledgeView knowledge, double maxStepFraction)
    {
        return Optional.ofNullable(blastRadius);
    }

    @Override
    public Collection<String> knobs(Action action)
    {
        String knob = action.params.get("knob");
        return knob == null ? List.of() : List.of(knob);
    }

    @Override
    public synchronized Applied apply(Action action, ActionContext ctx) throws Exception
    {
        calls.add("apply");
        duringApply.run();
        if (failApply == Fail.BEFORE_EFFECT)
            throw new IllegalStateException("apply failed before any effect");
        Applied done = applied.get(action.id);
        if (done != null)
            return done;
        long target = Long.parseLong(action.params.get("value"));
        Applied result = new Applied(Map.of("previous", String.valueOf(value), "set", String.valueOf(target)));
        ctx.recordUndo(result.undo);
        value = target;
        if (failApply == Fail.AFTER_PARTIAL_EFFECT)
            throw new IllegalStateException("apply failed after changing the knob");
        applied.put(action.id, result);
        return result;
    }

    @Override
    public Verdict verify(Action action, Applied applied, Map<String, Double> baseline, KnowledgeView knowledge)
    {
        calls.add("verify");
        return verdict != null ? verdict : Actuator.super.verify(action, applied, baseline, knowledge);
    }

    @Override
    public synchronized void rollback(Action action, Applied applied, ActionContext ctx) throws Exception
    {
        calls.add("rollback");
        if (failRollback)
            throw new IllegalStateException("rollback failed");
        // compare-and-restore: only undo our own value
        if (value == Long.parseLong(applied.undo.get("set")))
            value = Long.parseLong(applied.undo.get("previous"));
    }

    @Override
    public boolean overridden(Action action, Applied applied)
    {
        return value != Long.parseLong(applied.undo.get("set"));
    }

    public synchronized int count(String call)
    {
        int n = 0;
        for (String c : calls)
            if (c.equals(call))
                n++;
        return n;
    }
}
