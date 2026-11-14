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
import org.apache.cassandra.tcm.Epoch;
import org.apache.cassandra.tcm.extensions.ExtensionKey;
import org.apache.cassandra.tcm.serialization.Version;

/**
 * The {@link ClusterLedger} as stored in {@code ClusterMetadata.extensions} under {@link #KEY}.
 *
 * <p><b>This class must never be renamed or moved</b>: {@code ExtensionKey} serializes the name of the value class
 * into every metadata snapshot and constructs it reflectively through the public no-argument constructor.
 *
 * <p>The ledger it holds is immutable and can be set once. The only mutable part is the last-modified epoch, which
 * {@code ClusterMetadata.Transformer.build()} sets in place; a transformation must therefore always put a
 * <em>new</em> instance into the transformer and never the one it read from the previous metadata.
 */
public final class ClusterLedgerValue extends LedgerValueBase<ClusterLedger>
{
    public static final String ID = "org.apache.cassandra.autonomic.ledger";
    public static final ExtensionKey<ClusterLedger, ClusterLedgerValue> KEY = new ExtensionKey<>(ID, ClusterLedgerValue.class);

    private volatile ClusterLedger ledger;

    /** For deserialization only. */
    public ClusterLedgerValue()
    {
    }

    public ClusterLedgerValue(ClusterLedger ledger)
    {
        this.ledger = Objects.requireNonNull(ledger);
    }

    @Override
    public void setValue(ClusterLedger value)
    {
        if (ledger != null)
            throw new IllegalStateException("The ledger of a ClusterLedgerValue is immutable; create a new value");
        ledger = Objects.requireNonNull(value);
    }

    @Override
    public ClusterLedger getValue()
    {
        return ledger;
    }

    @Override
    public void serialize(DataOutputPlus out, Version version) throws IOException
    {
        ClusterLedger.serialize(ledger, out);
        Epoch.serializer.serialize(lastModified(), out, version);
    }

    @Override
    public void deserialize(DataInputPlus in, Version version) throws IOException
    {
        setValue(ClusterLedger.deserialize(in));
        withLastModified(Epoch.serializer.deserialize(in, version));
    }

    @Override
    public long serializedSize(Version version)
    {
        return ClusterLedger.serializedSize(ledger) + Epoch.serializer.serializedSize(lastModified(), version);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (!(o instanceof ClusterLedgerValue))
            return false;
        ClusterLedgerValue that = (ClusterLedgerValue) o;
        return Objects.equals(lastModified(), that.lastModified()) && Objects.equals(ledger, that.ledger);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(lastModified(), ledger);
    }

    @Override
    public String toString()
    {
        return "ClusterLedgerValue{" + ledger + " lastModified=" + lastModified() + '}';
    }
}
