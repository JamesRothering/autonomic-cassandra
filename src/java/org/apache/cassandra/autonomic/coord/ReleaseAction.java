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
package org.apache.cassandra.autonomic.coord;

import java.io.IOException;
import java.util.Objects;
import java.util.UUID;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

/**
 * Removes the claim of an action and records it as recent; see {@link ClusterLedger#withReleased}. A null
 * {@code owner} is a release forced by an operator.
 */
public final class ReleaseAction extends LedgerTransformation
{
    public static final String NAME = "org.apache.cassandra.autonomic.coord.ReleaseAction";
    public static final Serializer serializer = new Serializer();

    public final UUID actionId;
    /** Nullable: forced by an operator. */
    public final NodeId owner;
    public final String finalState;
    public final long nowMillis;

    public ReleaseAction(UUID actionId, NodeId owner, String finalState, long nowMillis)
    {
        this.actionId = Objects.requireNonNull(actionId);
        this.owner = owner;
        this.finalState = Objects.requireNonNull(finalState);
        this.nowMillis = nowMillis;
    }

    @Override
    public String name()
    {
        return NAME;
    }

    @Override
    ClusterLedger.Result apply(ClusterLedger ledger, ClusterMetadata prev)
    {
        return ledger.withReleased(actionId, owner, finalState, nowMillis);
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof ReleaseAction))
            return false;
        ReleaseAction r = (ReleaseAction) o;
        return actionId.equals(r.actionId) && Objects.equals(owner, r.owner) && finalState.equals(r.finalState) && nowMillis == r.nowMillis;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(actionId, owner, finalState, nowMillis);
    }

    @Override
    public String toString()
    {
        return "ReleaseAction{" + actionId + " owner=" + (owner == null ? "forced" : String.valueOf(owner.id())) + ' ' + finalState + " now=" + nowMillis + '}';
    }

    public static final class Serializer implements AsymmetricMetadataSerializer<Transformation, ReleaseAction>
    {
        private Serializer()
        {
        }

        @Override
        public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
        {
            ReleaseAction r = (ReleaseAction) t;
            out.writeByte(FORMAT);
            out.writeLong(r.actionId.getMostSignificantBits());
            out.writeLong(r.actionId.getLeastSignificantBits());
            out.writeBoolean(r.owner != null);
            if (r.owner != null)
                out.writeInt(r.owner.id());
            out.writeUTF(r.finalState);
            out.writeLong(r.nowMillis);
        }

        @Override
        public ReleaseAction deserialize(DataInputPlus in, Version version) throws IOException
        {
            checkFormat(in.readByte(), NAME);
            UUID id = new UUID(in.readLong(), in.readLong());
            NodeId owner = in.readBoolean() ? new NodeId(in.readInt()) : null;
            return new ReleaseAction(id, owner, in.readUTF(), in.readLong());
        }

        @Override
        public long serializedSize(Transformation t, Version version)
        {
            ReleaseAction r = (ReleaseAction) t;
            return 1 + 2 * Long.BYTES + 1 + (r.owner == null ? 0 : Integer.BYTES) + TypeSizes.sizeof(r.finalState) + Long.BYTES;
        }
    }
}
