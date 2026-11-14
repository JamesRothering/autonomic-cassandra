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

/** A released claim, kept in the ledger for cooldowns and for owners that find their claim gone. Immutable. */
public final class Recent
{
    public final UUID actionId;
    public final String actuator;
    public final String target;
    public final Lane lane;
    public final String finalState;
    public final long endedAtMillis;

    public Recent(UUID actionId, String actuator, String target, Lane lane, String finalState, long endedAtMillis)
    {
        this.actionId = Objects.requireNonNull(actionId);
        this.actuator = Objects.requireNonNull(actuator);
        this.target = Objects.requireNonNull(target);
        this.lane = Objects.requireNonNull(lane);
        this.finalState = Objects.requireNonNull(finalState);
        this.endedAtMillis = endedAtMillis;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (!(o instanceof Recent))
            return false;
        Recent r = (Recent) o;
        return actionId.equals(r.actionId) && actuator.equals(r.actuator) && target.equals(r.target) && lane == r.lane
               && finalState.equals(r.finalState) && endedAtMillis == r.endedAtMillis;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(actionId, actuator, target, lane, finalState, endedAtMillis);
    }

    @Override
    public String toString()
    {
        return "Recent{" + actionId + ' ' + actuator + ' ' + target + ' ' + lane + ' ' + finalState + " at " + endedAtMillis + '}';
    }

    static void serialize(Recent r, DataOutputPlus out) throws IOException
    {
        out.writeLong(r.actionId.getMostSignificantBits());
        out.writeLong(r.actionId.getLeastSignificantBits());
        out.writeUTF(r.actuator);
        out.writeUTF(r.target);
        out.writeByte(r.lane.id);
        out.writeUTF(r.finalState);
        out.writeLong(r.endedAtMillis);
    }

    static Recent deserialize(DataInputPlus in) throws IOException
    {
        UUID id = new UUID(in.readLong(), in.readLong());
        String actuator = in.readUTF();
        String target = in.readUTF();
        int laneId = in.readByte();
        Lane lane = Lane.fromId(laneId);
        if (lane == null)
            throw new IOException("Unknown autonomic ledger lane id " + laneId);
        return new Recent(id, actuator, target, lane, in.readUTF(), in.readLong());
    }

    static long serializedSize(Recent r)
    {
        return 2 * Long.BYTES + TypeSizes.sizeof(r.actuator) + TypeSizes.sizeof(r.target) + 1 + TypeSizes.sizeof(r.finalState) + Long.BYTES;
    }
}
