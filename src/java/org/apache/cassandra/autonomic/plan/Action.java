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

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import org.apache.cassandra.autonomic.monitor.Subject;

public final class Action
{
    /** Timeuuid. */
    public final UUID id;
    /** {@code Actuator.name()}. */
    public final String actuator;
    public final ActionScope scope;
    public final Subject target;
    public final Map<String, String> params;
    /** {@code actuator + ":" + target.asString()}; cooldown and conflict key. */
    public final String dedupeKey;
    public final VerifySpec verify;

    public Action(UUID id, String actuator, ActionScope scope, Subject target, Map<String, String> params, VerifySpec verify)
    {
        this.id = Objects.requireNonNull(id);
        this.actuator = Objects.requireNonNull(actuator);
        this.scope = Objects.requireNonNull(scope);
        this.target = Objects.requireNonNull(target);
        this.params = Collections.unmodifiableMap(new LinkedHashMap<>(params));
        this.dedupeKey = actuator + ":" + target.asString();
        this.verify = verify;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (!(o instanceof Action))
            return false;
        Action a = (Action) o;
        return id.equals(a.id) && actuator.equals(a.actuator) && scope == a.scope && target.equals(a.target)
               && params.equals(a.params) && Objects.equals(verify, a.verify);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(id, actuator, scope, target, params, verify);
    }

    @Override
    public String toString()
    {
        return "Action{" + id + " " + dedupeKey + " " + scope + " " + params + '}';
    }
}
