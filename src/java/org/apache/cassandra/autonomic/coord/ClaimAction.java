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

import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.serialization.AsymmetricMetadataSerializer;
import org.apache.cassandra.tcm.serialization.Version;

/**
 * Claims a lane for an action, or takes over a lane whose lease has expired; see
 * {@link ClusterLedger#withClaim}. {@code nowMillis} and {@code cooldownMillis} are the proposer's clock and limit.
 */
public final class ClaimAction extends LedgerTransformation
{
    public static final String NAME = "org.apache.cassandra.autonomic.coord.ClaimAction";
    public static final Serializer serializer = new Serializer();

    public final Lane lane;
    public final Claim claim;
    public final long nowMillis;
    public final long cooldownMillis;

    public ClaimAction(Lane lane, Claim claim, long nowMillis, long cooldownMillis)
    {
        this.lane = Objects.requireNonNull(lane);
        this.claim = Objects.requireNonNull(claim);
        this.nowMillis = nowMillis;
        this.cooldownMillis = cooldownMillis;
    }

    @Override
    public String name()
    {
        return NAME;
    }

    @Override
    ClusterLedger.Result apply(ClusterLedger ledger, ClusterMetadata prev)
    {
        return ledger.withClaim(lane, claim, nowMillis, cooldownMillis, !prev.inProgressSequences.isEmpty());
    }

    @Override
    public boolean equals(Object o)
    {
        if (!(o instanceof ClaimAction))
            return false;
        ClaimAction c = (ClaimAction) o;
        return lane == c.lane && claim.equals(c.claim) && nowMillis == c.nowMillis && cooldownMillis == c.cooldownMillis;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(lane, claim, nowMillis, cooldownMillis);
    }

    @Override
    public String toString()
    {
        return "ClaimAction{" + lane + ' ' + claim + " now=" + nowMillis + " cooldown=" + cooldownMillis + '}';
    }

    public static final class Serializer implements AsymmetricMetadataSerializer<Transformation, ClaimAction>
    {
        private Serializer()
        {
        }

        @Override
        public void serialize(Transformation t, DataOutputPlus out, Version version) throws IOException
        {
            ClaimAction c = (ClaimAction) t;
            out.writeByte(FORMAT);
            out.writeByte(c.lane.id);
            Claim.serialize(c.claim, out);
            out.writeLong(c.nowMillis);
            out.writeLong(c.cooldownMillis);
        }

        @Override
        public ClaimAction deserialize(DataInputPlus in, Version version) throws IOException
        {
            checkFormat(in.readByte(), NAME);
            int laneId = in.readByte();
            Lane lane = Lane.fromId(laneId);
            if (lane == null)
                throw new IOException("Unknown autonomic ledger lane id " + laneId);
            Claim claim = Claim.deserialize(in);
            return new ClaimAction(lane, claim, in.readLong(), in.readLong());
        }

        @Override
        public long serializedSize(Transformation t, Version version)
        {
            return 2 + Claim.serializedSize(((ClaimAction) t).claim) + 2 * Long.BYTES;
        }
    }
}
