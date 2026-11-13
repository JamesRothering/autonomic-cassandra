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

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import org.apache.cassandra.autonomic.knowledge.KnowledgeView;
import org.apache.cassandra.autonomic.plan.Action;
import org.apache.cassandra.autonomic.plan.ActionScope;

/**
 * Execute. The verify/rollback contract is overview 7.3. {@code apply}, {@code verify}, {@code rollback} and
 * {@code overridden} are called on the actuator thread only; {@code validate}, {@code blastRadius} and {@code knobs}
 * are called by the SafetyGovernor on the control thread (or a JMX thread for an operator approval) and must be
 * cheap, side-effect free and thread-safe.
 */
public interface Actuator
{
    String name();

    ActionScope scope();

    boolean reversible();

    /** Cheap precondition check; no side effects. Returns a rejection reason, or empty if valid. */
    Optional<String> validate(Action action, KnowledgeView knowledge);

    /**
     * Governor check {@code BLAST_RADIUS}: returns a rejection detail if the action steps a knob by more than
     * {@code maxStepFraction} of its current value or leaves its envelope; empty if acceptable.
     */
    default Optional<String> blastRadius(Action action, KnowledgeView knowledge, double maxStepFraction)
    {
        return Optional.empty();
    }

    /**
     * Names of the knobs the action changes, for governor check {@code CONFLICT} between different actuators. Actions
     * with the same {@code dedupeKey} always conflict, so most actuators need not override this.
     */
    default Collection<String> knobs(Action action)
    {
        return Collections.emptyList();
    }

    /**
     * Makes the change. Must return what rollback needs. Must be idempotent for a given action id. If it can throw
     * after changing something it must have called {@link ActionContext#recordUndo} first.
     */
    Applied apply(Action action, ActionContext ctx) throws Exception;

    /** Verdict after the observation window; rule 6 of overview 7.3 by default. */
    default Verdict verify(Action action, Applied applied, Map<String, Double> baseline, KnowledgeView knowledge)
    {
        return MetricVerifier.compare(action.verify, baseline, knowledge);
    }

    /** Restores the state captured in applied. Must be idempotent. Must not throw for "already restored". */
    void rollback(Action action, Applied applied, ActionContext ctx) throws Exception;

    /** True if the live value is no longer what apply set, i.e. an operator changed it. */
    boolean overridden(Action action, Applied applied);
}
