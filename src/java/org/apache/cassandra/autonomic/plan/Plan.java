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
package org.apache.cassandra.autonomic.plan;

import java.util.List;
import java.util.Objects;
import java.util.UUID;

public final class Plan
{
    public final UUID id;
    public final UUID diagnosisId;
    public final String policy;
    /** MVP: exactly one. */
    public final List<Action> actions;
    public final String rationale;

    public Plan(UUID id, UUID diagnosisId, String policy, List<Action> actions, String rationale)
    {
        this.id = Objects.requireNonNull(id);
        this.diagnosisId = Objects.requireNonNull(diagnosisId);
        this.policy = Objects.requireNonNull(policy);
        this.actions = List.copyOf(actions);
        this.rationale = rationale;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (!(o instanceof Plan))
            return false;
        Plan p = (Plan) o;
        return id.equals(p.id) && diagnosisId.equals(p.diagnosisId) && policy.equals(p.policy)
               && actions.equals(p.actions) && Objects.equals(rationale, p.rationale);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id, diagnosisId, policy, actions, rationale);
    }

    @Override
    public String toString()
    {
        return "Plan{" + id + " policy=" + policy + " diagnosis=" + diagnosisId + " actions=" + actions + '}';
    }
}
