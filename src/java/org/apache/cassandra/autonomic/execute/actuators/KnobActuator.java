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
package org.apache.cassandra.autonomic.execute.actuators;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

import org.apache.cassandra.autonomic.execute.ActionContext;
import org.apache.cassandra.autonomic.execute.Actuator;
import org.apache.cassandra.autonomic.execute.Applied;
import org.apache.cassandra.autonomic.knowledge.KnowledgeView;
import org.apache.cassandra.autonomic.plan.Action;
import org.apache.cassandra.autonomic.plan.ActionScope;

/**
 * A node-local numeric setting moved inside an envelope around its {@code cassandra.yaml} value (overview 8.1).
 * Action params: {@code value} (the target) and optionally {@code diagnosis_id}.
 *
 * <ul>
 * <li>Envelope and step: {@link #validate} rejects a target outside {@code [min, max]}; {@link #blastRadius} rejects a
 * step larger than {@code max_step_fraction} of the current value, but never smaller than one {@code minStep}, so
 * that a knob worth 1 can still move.</li>
 * <li>Undo: {@code previous} and {@code set} are recorded before the write; rollback restores {@code previous} only if
 * the live value is still {@code set}.</li>
 * <li>Override: a live value other than the one this actuator last wrote means an operator changed it; the actuator
 * proposes and restores nothing until {@link #revert} or until the live value is back at the last written one.</li>
 * <li>Direction lock: after a rollback of an increase the knob cannot be increased again for the same diagnosis id;
 * a new diagnosis id means the condition cleared in between.</li>
 * </ul>
 *
 * The configured value is read once, when the actuator is built at layer start.
 */
public abstract class KnobActuator implements Actuator
{
    /** Live access to the setting. Reads and writes must be cheap and non-blocking. */
    public interface Handle
    {
        double get();

        void set(double value) throws Exception;
    }

    public enum Direction
    {
        UP, DOWN;

        public static Direction of(double from, double to)
        {
            return to > from ? UP : DOWN;
        }
    }

    public static final String PARAM_VALUE = "value";
    public static final String PARAM_DIAGNOSIS = "diagnosis_id";
    private static final double EPSILON = 1e-9;
    private static final int APPLIED_MEMORY = 64;

    private final String name;
    private final Handle handle;
    private final double configured;
    private final double min;
    private final double max;
    private final double minStep;
    private final boolean integral;
    private final Map<UUID, Applied> applied = Collections.synchronizedMap(new LinkedHashMap<UUID, Applied>()
    {
        @Override
        protected boolean removeEldestEntry(Map.Entry<UUID, Applied> eldest)
        {
            return size() > APPLIED_MEMORY;
        }
    });
    /** The value this actuator wrote last (apply, rollback or revert); null if it never wrote. */
    private volatile Double lastWritten;
    /** Diagnosis id of the last rolled back increase. */
    private volatile String increaseLock;

    /**
     * @param min  lowest allowed value; adjusted to be at most the configured value
     * @param max  highest allowed value; adjusted to be at least the configured value
     * @param minStep smallest step the step limit allows, for example 1 for an integer knob
     */
    protected KnobActuator(String name, Handle handle, double min, double max, double minStep, boolean integral)
    {
        this.name = name;
        this.handle = handle;
        this.configured = handle.get();
        this.integral = integral;
        this.minStep = minStep;
        this.min = Math.min(integral ? Math.ceil(min - EPSILON) : min, configured);
        this.max = Math.max(integral ? Math.floor(max + EPSILON) : max, configured);
    }

    @Override
    public String name()
    {
        return name;
    }

    @Override
    public ActionScope scope()
    {
        return ActionScope.NODE;
    }

    @Override
    public boolean reversible()
    {
        return true;
    }

    public double configured()
    {
        return configured;
    }

    public double current()
    {
        return handle.get();
    }

    public double min()
    {
        return min;
    }

    public double max()
    {
        return max;
    }

    /** False if the configured value does not allow managing the knob, for example 0 meaning unlimited. */
    public boolean managed()
    {
        return min < max;
    }

    protected static double option(Map<String, Object> options, String key, double def)
    {
        Object o = options == null ? null : options.get(key);
        return o instanceof Number ? ((Number) o).doubleValue() : def;
    }

    /** True if the live value differs from the one this actuator wrote last. */
    public boolean operatorOverride()
    {
        Double written = lastWritten;
        return written != null && differs(handle.get(), written);
    }

    private static boolean differs(double a, double b)
    {
        return Math.abs(a - b) > EPSILON;
    }

    /** True if the live value differs from the configured value. */
    public boolean drifted()
    {
        return differs(handle.get(), configured);
    }

    private boolean locked(Direction direction, String diagnosisId)
    {
        String lock = increaseLock;
        if (lock == null || direction != Direction.UP)
            return false;
        if (diagnosisId != null && !diagnosisId.equals(lock))
        {
            increaseLock = null;
            return false;
        }
        return true;
    }

    /**
     * The value a policy should propose to move one step in {@code direction}, or empty if the knob is not managed,
     * already at its envelope limit, overridden by an operator, or locked in that direction for this diagnosis.
     */
    public Optional<Double> nextTarget(Direction direction, double stepFraction, String diagnosisId)
    {
        if (!managed() || operatorOverride() || locked(direction, diagnosisId))
            return Optional.empty();
        double current = handle.get();
        double step = Math.max(Math.abs(current) * stepFraction, minStep);
        double target = direction == Direction.UP ? current + step : current - step;
        if (integral)
            target = direction == Direction.UP ? Math.ceil(target - EPSILON) : Math.floor(target + EPSILON);
        else
            target = Math.round(target * 1000) / 1000.0;
        target = Math.max(min, Math.min(max, target));
        return differs(target, current) ? Optional.of(target) : Optional.empty();
    }

    /** Params of an action that sets the knob to {@code target}. */
    public static Map<String, String> params(double target, String diagnosisId)
    {
        Map<String, String> p = new LinkedHashMap<>();
        p.put(PARAM_VALUE, format(target));
        if (diagnosisId != null)
            p.put(PARAM_DIAGNOSIS, diagnosisId);
        return p;
    }

    public static String format(double v)
    {
        return v == Math.rint(v) && Math.abs(v) < 1e15 ? Long.toString((long) v) : Double.toString(v);
    }

    private static double target(Action action)
    {
        String v = action.params.get(PARAM_VALUE);
        if (v == null)
            throw new IllegalArgumentException("missing param " + PARAM_VALUE);
        return Double.parseDouble(v);
    }

    @Override
    public Optional<String> validate(Action action, KnowledgeView knowledge)
    {
        double target;
        try
        {
            target = target(action);
        }
        catch (RuntimeException e)
        {
            return Optional.of(e.getMessage());
        }
        if (!managed())
            return Optional.of(name + " is not managed: its configured value " + format(configured) + " leaves no envelope");
        if (target < min - EPSILON || target > max + EPSILON)
            return Optional.of("target " + format(target) + " is outside the envelope [" + format(min) + ", " + format(max) + ']');
        if (integral && Math.abs(target - Math.rint(target)) > EPSILON)
            return Optional.of("target " + format(target) + " is not an integer");
        double current = handle.get();
        if (!differs(target, current))
            return Optional.of("target equals the current value");
        if (operatorOverride())
            return Optional.of("operator override: live value " + format(current) + " differs from the last value written, "
                               + format(lastWritten));
        if (locked(Direction.of(current, target), action.params.get(PARAM_DIAGNOSIS)))
            return Optional.of("direction lock: an increase for this diagnosis was rolled back");
        return Optional.empty();
    }

    @Override
    public Optional<String> blastRadius(Action action, KnowledgeView knowledge, double maxStepFraction)
    {
        double target = target(action);
        double current = handle.get();
        double allowed = Math.max(Math.abs(current) * maxStepFraction, minStep);
        if (Math.abs(target - current) > allowed + EPSILON)
            return Optional.of("step " + format(current) + " to " + format(target) + " exceeds " + format(allowed));
        if (target < min - EPSILON || target > max + EPSILON)
            return Optional.of("target " + format(target) + " is outside the envelope");
        return Optional.empty();
    }

    @Override
    public Collection<String> knobs(Action action)
    {
        return List.of(name);
    }

    @Override
    public Applied apply(Action action, ActionContext ctx) throws Exception
    {
        Applied done = applied.get(action.id);
        if (done != null)
            return done;
        double target = target(action);
        Map<String, String> undo = new LinkedHashMap<>();
        undo.put("knob", name);
        undo.put("previous", format(handle.get()));
        undo.put("set", format(target));
        Applied result = new Applied(undo);
        ctx.recordUndo(result.undo);
        handle.set(target);
        lastWritten = target;
        applied.put(action.id, result);
        return result;
    }

    @Override
    public void rollback(Action action, Applied applied, ActionContext ctx) throws Exception
    {
        double set = Double.parseDouble(applied.undo.get("set"));
        double previous = Double.parseDouble(applied.undo.get("previous"));
        // compare-and-restore: an operator's value is left alone
        if (differs(handle.get(), set))
            return;
        handle.set(previous);
        lastWritten = previous;
        if (set > previous)
            increaseLock = action.params.get(PARAM_DIAGNOSIS);
    }

    @Override
    public boolean overridden(Action action, Applied applied)
    {
        return differs(handle.get(), Double.parseDouble(applied.undo.get("set")));
    }

    /** Restores the configured value; true if it changed anything. Also lifts the direction lock and the override. */
    public boolean revert() throws Exception
    {
        increaseLock = null;
        boolean changed = drifted();
        if (changed)
            handle.set(configured);
        lastWritten = null;
        return changed;
    }
}
