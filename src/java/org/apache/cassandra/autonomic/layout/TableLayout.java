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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import com.google.common.collect.ImmutableSortedMap;

import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.tcm.serialization.Version;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * The layout of one logical table (design 2.3): its overflow table and the partitions that were split into it.
 * Immutable; every change goes through a method that returns a new layout with {@link #version} plus one.
 */
public final class TableLayout
{
    /** The kind of split. The {@link #id} is the serialized form and must never change. */
    public enum Transform
    {
        HASH_SPLIT(0);

        public final int id;

        Transform(int id)
        {
            this.id = id;
        }

        /** Null if the id is unknown. */
        public static Transform fromId(int id)
        {
            for (Transform t : values())
                if (t.id == id)
                    return t;
            return null;
        }
    }

    /** Name of the bucket column of every overflow table (design 2.1). */
    public static final String BUCKET_COLUMN = "__al_bucket";

    public final UUID layoutId;
    public final TableId logical;
    public final TableId physical;
    public final String keyspace;
    public final String logicalName;
    public final String physicalName;
    public final Transform transform;
    public final int hashAlgo;
    public final int buckets;
    public final int hashedClusteringColumns;
    /** Starts at 1, never reused. */
    public final int nextIncarnation;
    /** Raw partition key bytes (private copies, see {@link #key}) to the state of the key; at most {@link LayoutRegistry#MAX_KEYS}. */
    public final ImmutableSortedMap<ByteBuffer, KeyState> keys;
    /** Plus one on every change to this layout. */
    public final long version;

    public TableLayout(LayoutHeader header, int nextIncarnation, Map<ByteBuffer, KeyState> keys, long version)
    {
        if (nextIncarnation < 1 || nextIncarnation > LayoutRegistry.MAX_INCARNATION)
            throw new IllegalArgumentException("next incarnation " + nextIncarnation + " is out of range");
        if (keys.size() > LayoutRegistry.MAX_KEYS)
            throw new IllegalArgumentException("layout has " + keys.size() + " keys, the limit is " + LayoutRegistry.MAX_KEYS);
        ImmutableSortedMap.Builder<ByteBuffer, KeyState> sorted = ImmutableSortedMap.naturalOrder();
        for (Map.Entry<ByteBuffer, KeyState> e : keys.entrySet())
        {
            if (e.getKey().remaining() == 0 || e.getKey().remaining() > LayoutRegistry.MAX_KEY_BYTES)
                throw new IllegalArgumentException("partition key of " + e.getKey().remaining() + " bytes, the limit is " + LayoutRegistry.MAX_KEY_BYTES);
            sorted.put(key(e.getKey()), e.getValue());
        }
        this.layoutId = header.layoutId;
        this.logical = header.logical;
        this.physical = header.physical;
        this.keyspace = header.keyspace;
        this.logicalName = header.logicalName;
        this.physicalName = header.physicalName;
        this.transform = header.transform;
        this.hashAlgo = header.hashAlgo;
        this.buckets = header.buckets;
        this.hashedClusteringColumns = header.hashedClusteringColumns;
        this.nextIncarnation = nextIncarnation;
        this.keys = sorted.build();
        this.version = version;
    }

    /**
     * A private copy of the bytes of a partition key, positioned at 0; the form in which keys are held. It is a plain
     * heap buffer, not a read-only view: the serializers' fast copy paths do not support read-only heap buffers.
     * Nothing in this package modifies a held key.
     */
    public static ByteBuffer key(ByteBuffer bytes)
    {
        return ByteBuffer.wrap(ByteBufferUtil.getArray(bytes));
    }

    public LayoutHeader header()
    {
        return new LayoutHeader(layoutId, logical, physical, keyspace, logicalName, physicalName, transform, hashAlgo, buckets, hashedClusteringColumns);
    }

    /** The state of the key, or null. */
    public KeyState keyState(ByteBuffer key)
    {
        return keys.get(key(key));
    }

    /** This layout with the key set to {@code state}; the version grows by one and {@code nextIncarnation} is kept. */
    TableLayout withKey(ByteBuffer key, KeyState state)
    {
        ImmutableSortedMap.Builder<ByteBuffer, KeyState> map = ImmutableSortedMap.naturalOrder();
        ByteBuffer k = key(key);
        keys.forEach((existing, s) -> {
            if (!existing.equals(k))
                map.put(existing, s);
        });
        map.put(k, state);
        return new TableLayout(header(), nextIncarnation, map.build(), version + 1);
    }

    /** This layout with a key added and the next incarnation consumed. */
    TableLayout withNewKey(ByteBuffer key, KeyState state)
    {
        TableLayout added = withKey(key, state);
        return new TableLayout(added.header(), nextIncarnation + 1, added.keys, added.version);
    }

    /** This layout without the key; the version grows by one. The result may have no keys. */
    TableLayout withoutKey(ByteBuffer key)
    {
        ImmutableSortedMap.Builder<ByteBuffer, KeyState> map = ImmutableSortedMap.naturalOrder();
        ByteBuffer k = key(key);
        keys.forEach((existing, s) -> {
            if (!existing.equals(k))
                map.put(existing, s);
        });
        return new TableLayout(header(), nextIncarnation, map.build(), version + 1);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;
        if (!(o instanceof TableLayout))
            return false;
        TableLayout l = (TableLayout) o;
        return header().equals(l.header()) && nextIncarnation == l.nextIncarnation && keys.equals(l.keys) && version == l.version;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(header(), nextIncarnation, keys, version);
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder("TableLayout{").append(keyspace).append('.').append(logicalName).append(" -> ").append(physicalName)
                                                            .append(" id=").append(layoutId).append(" buckets=").append(buckets)
                                                            .append(" nextIncarnation=").append(nextIncarnation).append(" version=").append(version)
                                                            .append(" keys={");
        String sep = "";
        for (Map.Entry<ByteBuffer, KeyState> e : keys.entrySet())
        {
            sb.append(sep).append(ByteBufferUtil.bytesToHex(e.getKey())).append('=').append(e.getValue());
            sep = ", ";
        }
        return sb.append("}}").toString();
    }

    static void serialize(TableLayout l, DataOutputPlus out, Version version) throws IOException
    {
        LayoutHeader.serialize(l.header(), out);
        out.writeUnsignedVInt32(l.nextIncarnation);
        out.writeUnsignedVInt(l.version);
        out.writeUnsignedVInt32(l.keys.size());
        for (Map.Entry<ByteBuffer, KeyState> e : l.keys.entrySet())
        {
            out.writeUnsignedVInt32(e.getKey().remaining());
            out.write(e.getKey().duplicate());
            KeyState.serialize(e.getValue(), out, version);
        }
    }

    static TableLayout deserialize(DataInputPlus in, Version version) throws IOException
    {
        LayoutHeader header = LayoutHeader.deserialize(in);
        int nextIncarnation = in.readUnsignedVInt32();
        long layoutVersion = in.readUnsignedVInt();
        int count = in.readUnsignedVInt32();
        if (count < 0 || count > LayoutRegistry.MAX_KEYS)
            throw new IOException("Corrupt autonomic layout registry: " + count + " keys");
        Map<ByteBuffer, KeyState> keys = new HashMap<>();
        for (int i = 0; i < count; i++)
        {
            int length = in.readUnsignedVInt32();
            if (length < 1 || length > LayoutRegistry.MAX_KEY_BYTES)
                throw new IOException("Corrupt autonomic layout registry: partition key of " + length + " bytes");
            byte[] bytes = new byte[length];
            in.readFully(bytes);
            keys.put(ByteBuffer.wrap(bytes), KeyState.deserialize(in, version));
        }
        try
        {
            return new TableLayout(header, nextIncarnation, keys, layoutVersion);
        }
        catch (IllegalArgumentException e)
        {
            throw new IOException("Corrupt autonomic layout registry: " + e.getMessage(), e);
        }
    }

    static long serializedSize(TableLayout l, Version version)
    {
        long size = LayoutHeader.serializedSize(l.header()) + TypeSizes.sizeofUnsignedVInt(l.nextIncarnation)
                    + TypeSizes.sizeofUnsignedVInt(l.version) + TypeSizes.sizeofUnsignedVInt(l.keys.size());
        for (Map.Entry<ByteBuffer, KeyState> e : l.keys.entrySet())
            size += TypeSizes.sizeofUnsignedVInt(e.getKey().remaining()) + e.getKey().remaining() + KeyState.serializedSize(e.getValue(), version);
        return size;
    }
}
