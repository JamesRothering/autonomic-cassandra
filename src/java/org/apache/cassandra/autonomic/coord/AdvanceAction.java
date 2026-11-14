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
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.membership.NodeId;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

/**
 * Moves a claim to its next step with a new phase and state and renews its lease; see
 * {@link ClusterLedger#withAdvancedClaim}. {@code expectedStep} is the compare-and-set token. A lease renewal is an
 * advance that keeps phase and state.
 */
public final class AdvanceAction extends LedgerTransformation
{
    public static final String NAME = "org.apache.cassandra.autonomic.coord.AdvanceAction";
    public static final Serializer serializer = new Serializer();

    public final UUID actionId;
    public final NodeId owner;
    public final int expectedStep;
    public final String phase;
    public final ImmutableMap<String, String> state;
    public final long nowMillis;
    public final long leaseMillis;

    public AdvanceAction(UUID actionId, NodeId owner, int expectedStep, String phase, Map<String, String> state, long nowMillis, long leaseMillis)
    {
        this.actionId = Objects.requireNonNull(actionId);
        this.owner = Objects.requireNonNull(owner);
        this.expectedStep = expectedStep;
        this.phase = Objects.requireNonNull(phase);
        this.state = ImmutableSortedMap.copyOf(state);
        this.nowMillis = nowMillis;
        this.leaseMillis = leaseMillis;
    }

    @Override
    public String name()
    {
        return NAME;
    }

    @Override
    ClusterLedger.Result apply(ClusterLedger ledger, ClusterMetadata prev)
    {
        return ledger.withAdvancedClaim(actionId, owner, expectedStep, phase, state, nowMillis, leaseMillis);
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof AdvanceAction))
            return false;
        AdvanceAction a = (AdvanceAction) o;
        return actionId.equals(a.actionId) && owner.equals(a.owner) && expectedStep == a.expectedStep && phase.equals(a.phase)
               && state.equals(a.state) && nowMillis == a.nowMillis && leaseMillis == a.leaseMillis;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(actionId, owner, expectedStep, phase, state, nowMillis, leaseMillis);
    }

    @Override
    public String toString()
    {
        return "AdvanceAction{" + actionId + " owner=" + owner.id() + " expectedStep=" + expectedStep + " phase=" + phase
               + " state=" + state + " now=" + nowMillis + " lease=" + leaseMillis + '}';
    }

    public static final class Serializer implements AsymmetricMetadataSerializer<Transformation, AdvanceAction>
    {
        private Serializer()
        {
        }

        @Override
        public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
        {
            AdvanceAction a = (AdvanceAction) t;
            out.writeByte(FORMAT);
            out.writeLong(a.actionId.getMostSignificantBits());
            out.writeLong(a.actionId.getLeastSignificantBits());
            out.writeInt(a.owner.id());
            out.writeInt(a.expectedStep);
            out.writeUTF(a.phase);
            Claim.serializeState(a.state, out);
            out.writeLong(a.nowMillis);
            out.writeLong(a.leaseMillis);
        }

        @Override
        public AdvanceAction deserialize(DataInputPlus in, Version version) throws IOException
        {
            checkFormat(in.readByte(), NAME);
            UUID id = new UUID(in.readLong(), in.readLong());
            NodeId owner = new NodeId(in.readInt());
            int expectedStep = in.readInt();
            String phase = in.readUTF();
            Map<String, String> state = Claim.deserializeState(in);
            return new AdvanceAction(id, owner, expectedStep, phase, state, in.readLong(), in.readLong());
        }

        @Override
        public long serializedSize(Transformation t, Version version)
        {
            AdvanceAction a = (AdvanceAction) t;
            return 1 + 2 * Long.BYTES + 2 * Integer.BYTES + TypeSizes.sizeof(a.phase) + Claim.stateSize(a.state) + 2 * Long.BYTES;
        }
    }
}
