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

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableId;

/**
 * What identifies a layout and does not change while it exists (design 2.3): carried by {@code RegisterKey} and
 * repeated in every {@link TableLayout}. Immutable.
 */
public final class LayoutHeader
{
    public final UUID layoutId;
    public final TableId logical;
    public final TableId physical;
    public final String keyspace;
    public final String logicalName;
    public final String physicalName;
    public final TableLayout.Transform transform;
    public final int hashAlgo;
    public final int buckets;
    public final int hashedClusteringColumns;

    public LayoutHeader(UUID layoutId, TableId logical, TableId physical, String keyspace, String logicalName, String physicalName,
                        TableLayout.Transform transform, int hashAlgo, int buckets, int hashedClusteringColumns)
    {
        this.layoutId = Objects.requireNonNull(layoutId);
        this.logical = Objects.requireNonNull(logical);
        this.physical = Objects.requireNonNull(physical);
        this.keyspace = Objects.requireNonNull(keyspace);
        this.logicalName = Objects.requireNonNull(logicalName);
        this.physicalName = Objects.requireNonNull(physicalName);
        this.transform = Objects.requireNonNull(transform);
        this.hashAlgo = hashAlgo;
        this.buckets = buckets;
        this.hashedClusteringColumns = hashedClusteringColumns;
    }

    /** Null if every bound that does not depend on the schema holds, otherwise why the header is refused. */
    String invalid()
    {
        if (hashAlgo != LayoutRegistry.HASH_ALGO)
            return "unknown hash algorithm " + hashAlgo;
        if (buckets < LayoutRegistry.MIN_BUCKETS || buckets > LayoutRegistry.MAX_BUCKETS)
            return "buckets " + buckets + " is outside " + LayoutRegistry.MIN_BUCKETS + ".." + LayoutRegistry.MAX_BUCKETS;
        if (hashedClusteringColumns < 1)
            return "hashed clustering columns " + hashedClusteringColumns + " is below 1";
        if (logical.equals(physical))
            return "the logical and the physical table are the same table";
        for (String name : new String[]{ keyspace, logicalName, physicalName })
            if (name.isEmpty() || name.length() > LayoutRegistry.MAX_NAME_LENGTH)
                return "name '" + name + "' is empty or longer than " + LayoutRegistry.MAX_NAME_LENGTH;
        return null;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (!(o instanceof LayoutHeader))
            return false;
        LayoutHeader h = (LayoutHeader) o;
        return layoutId.equals(h.layoutId) && logical.equals(h.logical) && physical.equals(h.physical) && keyspace.equals(h.keyspace)
               && logicalName.equals(h.logicalName) && physicalName.equals(h.physicalName) && transform == h.transform
               && hashAlgo == h.hashAlgo && buckets == h.buckets && hashedClusteringColumns == h.hashedClusteringColumns;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(layoutId, logical, physical, keyspace, logicalName, physicalName, transform, hashAlgo, buckets, hashedClusteringColumns);
    }

    @Override
    public String toString()
    {
        return "LayoutHeader{" + layoutId + ' ' + keyspace + '.' + logicalName + " -> " + physicalName + ' ' + transform
               + " hashAlgo=" + hashAlgo + " buckets=" + buckets + " hashedClusteringColumns=" + hashedClusteringColumns + '}';
    }

    static void serialize(LayoutHeader h, DataOutputPlus out) throws IOException
    {
        out.writeLong(h.layoutId.getMostSignificantBits());
        out.writeLong(h.layoutId.getLeastSignificantBits());
        h.logical.serialize(out);
        h.physical.serialize(out);
        out.writeUTF(h.keyspace);
        out.writeUTF(h.logicalName);
        out.writeUTF(h.physicalName);
        out.writeByte(h.transform.id);
        out.writeUnsignedVInt32(h.hashAlgo);
        out.writeUnsignedVInt32(h.buckets);
        out.writeUnsignedVInt32(h.hashedClusteringColumns);
    }

    static LayoutHeader deserialize(DataInputPlus in) throws IOException
    {
        UUID layoutId = new UUID(in.readLong(), in.readLong());
        TableId logical = TableId.deserialize(in);
        TableId physical = TableId.deserialize(in);
        String keyspace = in.readUTF();
        String logicalName = in.readUTF();
        String physicalName = in.readUTF();
        int transformId = in.readByte();
        TableLayout.Transform transform = TableLayout.Transform.fromId(transformId);
        if (transform == null)
            throw new IOException("Unknown autonomic layout transform id " + transformId);
        return new LayoutHeader(layoutId, logical, physical, keyspace, logicalName, physicalName, transform,
                                in.readUnsignedVInt32(), in.readUnsignedVInt32(), in.readUnsignedVInt32());
    }

    static long serializedSize(LayoutHeader h)
    {
        return 2 * Long.BYTES + 2 * h.logical.serializedSize() + TypeSizes.sizeof(h.keyspace) + TypeSizes.sizeof(h.logicalName)
               + TypeSizes.sizeof(h.physicalName) + 1 + TypeSizes.sizeofUnsignedVInt(h.hashAlgo)
               + TypeSizes.sizeofUnsignedVInt(h.buckets) + TypeSizes.sizeofUnsignedVInt(h.hashedClusteringColumns);
    }
}
