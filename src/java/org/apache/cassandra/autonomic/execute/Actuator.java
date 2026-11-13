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
import java.util.Optional;

import org.apache.cassandra.autonomic.knowledge.KnowledgeView;
import org.apache.cassandra.autonomic.plan.Action;
import org.apache.cassandra.autonomic.plan.ActionScope;

/** Execute. The verify/rollback contract is overview 7.3. Called on the actuator thread only. */
public interface Actuator
{
    String name();

    ActionScope scope();

    boolean reversible();

    /** Cheap precondition check; no side effects. Returns a rejection reason, or empty if valid. */
    Optional<String> validate(Action action, KnowledgeView knowledge);

    /** Makes the change. Must return what rollback needs. Must be idempotent for a given action id. */
    Applied apply(Action action, ActionContext ctx) throws Exception;

    // TODO T2.6: make this a default method delegating to MetricVerifier.compare(action.verify, baseline, knowledge)
    Verdict verify(Action action, Applied applied, Map<String, Double> baseline, KnowledgeView knowledge);

    /** Restores the state captured in applied. Must be idempotent. Must not throw for "already restored". */
    void rollback(Action action, Applied applied, ActionContext ctx) throws Exception;

    /** True if the live value is no longer what apply set, i.e. an operator changed it. */
    boolean overridden(Action action, Applied applied);
}
