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
package org.apache.cassandra.autonomic.layout;

import java.io.IOException;
import java.util.Objects;
import java.util.UUID;

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.membership.NodeId;

/** The arguments of {@code ClusterLedger.withAdvancedClaim} that a claim-advancing layout transformation carries. Immutable. */
final class ClaimStep
{
    static final long SERIALIZED_SIZE = 2 * Long.BYTES + 2 * Integer.BYTES + 2 * Long.BYTES;

    final UUID actionId;
    final NodeId owner;
    final int expectedStep;
    final long nowMillis;
    final long leaseMillis;

    ClaimStep(UUID actionId, NodeId owner, int expectedStep, long nowMillis, long leaseMillis)
    {
        this.actionId = Objects.requireNonNull(actionId);
        this.owner = Objects.requireNonNull(owner);
        this.expectedStep = expectedStep;
        this.nowMillis = nowMillis;
        this.leaseMillis = leaseMillis;
    }

    void serialize(DataOutputPlus out) throws IOException
    {
        LayoutTransformation.writeUuid(actionId, out);
        out.writeInt(owner.id());
        out.writeInt(expectedStep);
        out.writeLong(nowMillis);
        out.writeLong(leaseMillis);
    }

    static ClaimStep deserialize(DataInputPlus in) throws IOException
    {
        UUID id = LayoutTransformation.readUuid(in);
        NodeId owner = new NodeId(in.readInt());
        int step = in.readInt();
        return new ClaimStep(id, owner, step, in.readLong(), in.readLong());
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof ClaimStep))
            return false;
        ClaimStep c = (ClaimStep) o;
        return actionId.equals(c.actionId) && owner.equals(c.owner) && expectedStep == c.expectedStep
               && nowMillis == c.nowMillis && leaseMillis == c.leaseMillis;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(actionId, owner, expectedStep, nowMillis, leaseMillis);
    }

    @Override
    public String toString()
    {
        return actionId + " owner=" + owner.id() + " expectedStep=" + expectedStep + " now=" + nowMillis + " lease=" + leaseMillis;
    }
}
