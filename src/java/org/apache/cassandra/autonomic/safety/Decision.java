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
package org.apache.cassandra.autonomic.safety;

import java.util.Objects;

/** Outcome of {@link SafetyGovernor#evaluate}. */
public final class Decision
{
    public enum Type { APPROVE, REJECT, HOLD_FOR_APPROVAL }

    private static final Decision APPROVE = new Decision(Type.APPROVE, null, null);

    public final Type type;
    /** Name of the failing check, used as {@code state_reason}; nullable for APPROVE. */
    public final String reason;
    /** Nullable: human-readable explanation, for the audit trail only. */
    public final String detail;

    private Decision(Type type, String reason, String detail)
    {
        this.type = type;
        this.reason = reason;
        this.detail = detail;
    }

    public static Decision approve()
    {
        return APPROVE;
    }

    public static Decision reject(String reason)
    {
        return new Decision(Type.REJECT, Objects.requireNonNull(reason), null);
    }

    public static Decision reject(String reason, String detail)
    {
        return new Decision(Type.REJECT, Objects.requireNonNull(reason), detail);
    }

    public static Decision holdForApproval(String reason)
    {
        return new Decision(Type.HOLD_FOR_APPROVAL, reason, null);
    }

    /** Equality ignores {@link #detail}. */
    @Override
    public boolean equals(Object o)
    {
        return o instanceof Decision && ((Decision) o).type == type && Objects.equals(((Decision) o).reason, reason);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(type, reason);
    }

    @Override
    public String toString()
    {
        return reason == null ? type.name() : type + "(" + reason + ")";
    }
}
