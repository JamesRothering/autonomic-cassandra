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
import java.nio.ByteBuffer;
import java.util.UUID;

import org.apache.cassandra.autonomic.coord.Claim;
import org.apache.cassandra.autonomic.coord.ClusterLedger;
import org.apache.cassandra.autonomic.coord.ClusterLedgerValue;
import org.apache.cassandra.autonomic.coord.Lane;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.exceptions.ExceptionCode;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.sequences.LockedRanges;
import org.apache.cassandra.tcm.transformations.CustomTransformation;

/**
 * Base of the layout transformations (design 2.4), written like {@code LedgerTransformation}: a {@code CUSTOM}
 * transformation, deterministic, with every time supplied as a field, rejected with {@code INVALID}.
 *
 * <p>A transformation with a {@link ClaimStep} advances the claim of its action in the same commit as the registry
 * (FC2), through {@link ClusterLedger#withAdvancedClaim}: both values are put into one transformer, so they cannot
 * disagree. It additionally requires that the action holds the {@link Lane#SCHEMA_LAYOUT} lane.
 */
public abstract class LayoutTransformation implements Transformation
{
    static final int FORMAT = 1;

    /** The name its serializer is registered under; part of the log format and never changed. */
    public abstract String name();

    /** The registry transition; {@code prev} is passed for the parts of the metadata a rule depends on. */
    abstract LayoutRegistry.Result apply(LayoutRegistry registry, ClusterMetadata prev);

    /** The claim advance committed with the registry change, or null if the transformation needs no claim. */
    abstract ClaimStep claimStep();

    /** The phase the claim is advanced to; only called if there is a {@link #claimStep()}. */
    abstract String claimPhase();

    @Override
    public final Kind kind()
    {
        return Kind.CUSTOM;
    }

    @Override
    public final Result execute(ClusterMetadata prev)
    {
        ClusterLedger.Result claimed = null;
        LayoutRegistry.Result result;
        try
        {
            ClaimStep step = claimStep();
            if (step != null)
            {
                ClusterLedger ledger = ClusterLedger.of(prev);
                Lane lane = ledger.lane(step.actionId);
                if (lane != Lane.SCHEMA_LAYOUT)
                    return new Rejected(ExceptionCode.INVALID, "action " + step.actionId + " does not hold lane " + Lane.SCHEMA_LAYOUT
                                                               + (lane == null ? "" : ", it holds " + lane));
                Claim held = ledger.claim(step.actionId);
                claimed = ledger.withAdvancedClaim(step.actionId, step.owner, step.expectedStep, claimPhase(), held.state, step.nowMillis, step.leaseMillis);
                if (claimed.isRejected())
                    return new Rejected(ExceptionCode.INVALID, claimed.rejection);
            }
            result = apply(LayoutRegistry.of(prev), prev);
        }
        catch (RuntimeException e)
        {
            return new Rejected(ExceptionCode.INVALID, "invalid layout transformation " + this + ": " + e);
        }
        if (result.isRejected())
            return new Rejected(ExceptionCode.INVALID, result.rejection);
        ClusterMetadata.Transformer transformer = prev.transformer();
        // always new value objects: build() sets the last-modified epoch of a value in place
        if (!result.unchanged)
            transformer = transformer.with(LayoutRegistryValue.KEY, new LayoutRegistryValue(result.registry));
        if (claimed != null && !claimed.unchanged)
            transformer = transformer.with(ClusterLedgerValue.KEY, new ClusterLedgerValue(claimed.ledger));
        return Transformation.success(transformer, LockedRanges.AffectedRanges.EMPTY);
    }

    /** The form in which the transformation is committed to the metadata log. */
    public final CustomTransformation wrap()
    {
        return new CustomTransformation(name(), this);
    }

    static void checkFormat(int format, String what) throws IOException
    {
        if (format != FORMAT)
            throw new IOException("Unknown format " + format + " of autonomic transformation " + what + "; this node reads format " + FORMAT);
    }

    // ---- field codecs shared by the serializers ----

    static void writeUuid(UUID id, DataOutputPlus out) throws IOException
    {
        out.writeLong(id.getMostSignificantBits());
        out.writeLong(id.getLeastSignificantBits());
    }

    static UUID readUuid(DataInputPlus in) throws IOException
    {
        return new UUID(in.readLong(), in.readLong());
    }

    static void writeKey(ByteBuffer key, DataOutputPlus out) throws IOException
    {
        out.writeUnsignedVInt32(key.remaining());
        out.write(key.duplicate());
    }

    static ByteBuffer readKey(DataInputPlus in) throws IOException
    {
        int length = in.readUnsignedVInt32();
        if (length < 0 || length > LayoutRegistry.MAX_KEY_BYTES)
            throw new IOException("Corrupt autonomic layout transformation: partition key of " + length + " bytes");
        byte[] bytes = new byte[length];
        in.readFully(bytes);
        return ByteBuffer.wrap(bytes);
    }

    static long keySize(ByteBuffer key)
    {
        return TypeSizes.sizeofUnsignedVInt(key.remaining()) + key.remaining();
    }

    static Phase readPhase(DataInputPlus in) throws IOException
    {
        int id = in.readByte();
        Phase phase = Phase.fromId(id);
        if (phase == null)
            throw new IOException("Unknown autonomic layout phase id " + id);
        return phase;
    }
}
