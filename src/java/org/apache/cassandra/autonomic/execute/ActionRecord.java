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

import java.util.Objects;
import java.util.UUID;

import org.apache.cassandra.autonomic.plan.Action;

/**
 * Local record of an action and its current state.
 * TODO T2.6: add undo data, verdict, baseline/result, reason, timestamps per transition and persistence mapping.
 */
public final class ActionRecord
{
    public final Action action;
    public final ActionState state;
    public final long updatedMillis;

    public ActionRecord(Action action, ActionState state, long updatedMillis)
    {
        this.action = Objects.requireNonNull(action);
        this.state = Objects.requireNonNull(state);
        this.updatedMillis = updatedMillis;
    }

    public UUID id()
    {
        return action.id;
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof ActionRecord))
            return false;
        ActionRecord r = (ActionRecord) o;
        return action.equals(r.action) && state == r.state && updatedMillis == r.updatedMillis;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(action, state, updatedMillis);
    }

    @Override
    public String toString()
    {
        return "ActionRecord{" + action.id + " " + state + '}';
    }
}
