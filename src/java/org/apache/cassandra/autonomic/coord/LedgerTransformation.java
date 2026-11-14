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

import org.apache.cassandra.exceptions.ExceptionCode;
import org.apache.cassandra.tcm.ClusterMetadata;
import org.apache.cassandra.tcm.Transformation;
import org.apache.cassandra.tcm.sequences.LockedRanges;
import org.apache.cassandra.tcm.transformations.CustomTransformation;

/**
 * Base of the four ledger transformations (overview 6.2). Each is a pure function of the previous metadata and its
 * own fields: {@link #execute} runs on every node when the metadata log is replayed, so nothing here or in a
 * subclass may read a clock, configuration or any other local state. All times and limits are fields supplied by
 * the proposer.
 *
 * <p>A transformation is committed wrapped in a {@code CustomTransformation} ({@link #wrap()}). Its serialized form
 * starts with its own format byte, independent of the TCM serialization version.
 */
public abstract class LedgerTransformation implements Transformation
{
    static final int FORMAT = 1;

    /** The name its serializer is registered under; part of the log format and never changed. */
    public abstract String name();

    /** The transition of the ledger; {@code prev} is passed for the parts of the metadata a rule depends on. */
    abstract ClusterLedger.Result apply(ClusterLedger ledger, ClusterMetadata prev);

    @Override
    public final Kind kind()
    {
        return Kind.CUSTOM;
    }

    @Override
    public final Result execute(ClusterMetadata prev)
    {
        ClusterLedger.Result result;
        try
        {
            result = apply(ClusterLedger.of(prev), prev);
        }
        catch (RuntimeException e)
        {
            return new Rejected(ExceptionCode.INVALID, "invalid ledger transformation " + this + ": " + e);
        }
        if (result.isRejected())
            return new Rejected(ExceptionCode.INVALID, result.rejection);
        ClusterMetadata.Transformer transformer = prev.transformer();
        // always a new value object: build() sets the last-modified epoch of the value in place
        if (!result.unchanged || !ClusterLedger.present(prev))
            transformer = transformer.with(ClusterLedgerValue.KEY, new ClusterLedgerValue(result.ledger));
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
}
