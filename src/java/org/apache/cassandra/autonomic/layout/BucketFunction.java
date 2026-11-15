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

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.utils.MurmurHash;

/**
 * The bucket of a row (design 2.2). {@link LayoutRegistry#HASH_ALGO} 1 is frozen: it is
 * {@code MurmurHash.hash2_64} with seed 0 over the concatenation, for the first {@code hashedClusteringColumns}
 * clustering columns, of a 4-byte big-endian length followed by the serialized value, and the bucket is
 * {@code floorMod(hash, buckets)}. Changing any part of it would misroute data that is already split, so the golden
 * vectors of the unit test pin it. A new definition needs a new algorithm number.
 */
public final class BucketFunction
{
    /** The seed of {@code hash2_64}. */
    public static final long SEED = 0;
    /** The physical bucket column holds {@code incarnation * BUCKET_RADIX + bucket}; buckets are below this. */
    public static final int BUCKET_RADIX = 256;

    private BucketFunction()
    {
    }

    /** The 64-bit hash of the given values (the first clustering values of a row). */
    public static long hash(List<ByteBuffer> values)
    {
        int size = 0;
        for (ByteBuffer value : values)
            size += Integer.BYTES + value.remaining();
        ByteBuffer bytes = ByteBuffer.allocate(size);
        for (ByteBuffer value : values)
        {
            bytes.putInt(value.remaining());
            bytes.put(value.duplicate());
        }
        return MurmurHash.hash2_64(bytes, 0, size, SEED);
    }

    /** The bucket, in {@code [0, buckets)}, of the clustering of a row. */
    public static int bucket(Clustering<?> clustering, int hashedClusteringColumns, int buckets)
    {
        if (buckets < 1 || buckets > LayoutRegistry.MAX_BUCKETS)
            throw new IllegalArgumentException("buckets " + buckets);
        if (hashedClusteringColumns < 1 || hashedClusteringColumns > clustering.size())
            throw new IllegalArgumentException("cannot hash " + hashedClusteringColumns + " of " + clustering.size() + " clustering columns");
        ByteBuffer[] values = new ByteBuffer[hashedClusteringColumns];
        for (int i = 0; i < values.length; i++)
            values[i] = clustering.bufferAt(i);
        return bucket(Arrays.asList(values), buckets);
    }

    /** The bucket of already extracted clustering values. */
    public static int bucket(List<ByteBuffer> values, int buckets)
    {
        return (int) Math.floorMod(hash(values), (long) buckets);
    }

    /** The value of the bucket column of the overflow table (design 2.1). */
    public static int physicalValue(int incarnation, int bucket)
    {
        if (incarnation < 1 || incarnation >= LayoutRegistry.MAX_INCARNATION)
            throw new IllegalArgumentException("incarnation " + incarnation);
        if (bucket < 0 || bucket >= BUCKET_RADIX)
            throw new IllegalArgumentException("bucket " + bucket);
        return incarnation * BUCKET_RADIX + bucket;
    }
}
