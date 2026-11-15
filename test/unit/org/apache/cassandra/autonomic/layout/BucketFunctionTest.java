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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.utils.ByteBufferUtil;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * The bucket function is frozen (design 2.2, hash algorithm 1). The expected numbers below were computed once with
 * {@code MurmurHash.hash2_64(bytes, 0, bytes.length, 0)} over the hand-assembled bytes shown (a 4-byte big-endian
 * length, then the value, for each hashed clustering column), not with {@link BucketFunction}. They must never change:
 * a change would route rows of already split partitions to a different bucket.
 */
public class BucketFunctionTest
{
    /** Hex of the hashed bytes, the hash, and the buckets for 2, 7, 8 and 64 buckets. */
    private static final Object[][] VECTORS = {
        { "0000000400000000", 6264804687187945365L, 1, 5, 5, 21 },            // int 0
        { "0000000400000001", -1401715054742112241L, 1, 2, 7, 15 },           // int 1
        { "00000004ffffffff", 4534201398820809735L, 1, 5, 7, 7 },             // int -1
        { "000000040000002a", -3170502994757829822L, 0, 0, 2, 2 },            // int 42
        { "00000003616263", 7732612195388986517L, 1, 3, 5, 21 },              // text 'abc'
        { "00000000", 6960269033020761575L, 1, 5, 7, 39 },                    // the empty value
        { "000000088000000000000000", -3701427887511554374L, 0, 1, 2, 58 },   // bigint Long.MIN_VALUE
        { "000000040000002a0000000178", 5477566727463552969L, 1, 2, 1, 9 },   // int 42, text 'x'
        { "00000004000000070000000568656c6c6f", -682782114138217462L, 0, 2, 2, 10 } // int 7, text 'hello'
    };

    private static final int[] BUCKET_COUNTS = { 2, 7, 8, 64 };

    /** The values whose length-prefixed concatenation is the given hex. */
    private static List<ByteBuffer> values(String hex)
    {
        ByteBuffer all = ByteBufferUtil.hexToBytes(hex);
        List<ByteBuffer> values = new ArrayList<>();
        while (all.hasRemaining())
        {
            int length = all.getInt();
            ByteBuffer value = all.duplicate();
            value.limit(value.position() + length);
            values.add(value);
            all.position(all.position() + length);
        }
        return values;
    }

    @Test
    public void goldenVectors()
    {
        for (Object[] vector : VECTORS)
        {
            String hex = (String) vector[0];
            List<ByteBuffer> values = values(hex);
            assertEquals(hex, (long) (Long) vector[1], BucketFunction.hash(values));
            for (int i = 0; i < BUCKET_COUNTS.length; i++)
                assertEquals(hex + " in " + BUCKET_COUNTS[i], (int) (Integer) vector[2 + i], BucketFunction.bucket(values, BUCKET_COUNTS[i]));
            // through a clustering, hashing all of its columns
            Clustering<?> clustering = Clustering.make(values.toArray(new ByteBuffer[0]));
            assertEquals(hex, (int) (Integer) vector[2 + 2], BucketFunction.bucket(clustering, values.size(), 8));
        }
        // the sign of the hash does not matter: negative hashes land in [0, buckets) by floorMod
        assertTrue((Long) VECTORS[1][1] < 0);
    }

    @Test
    public void onlyTheHashedPrefixCounts()
    {
        Clustering<?> clustering = Clustering.make(ByteBufferUtil.hexToBytes("0000002a"), ByteBufferUtil.bytes("x"));
        // int 42 alone is the vector 'int 42'; the second column is ignored with a prefix of 1
        assertEquals((long) (Long) VECTORS[3][1], BucketFunction.hash(Arrays.asList(clustering.bufferAt(0))));
        assertEquals((int) (Integer) VECTORS[3][2 + 3], BucketFunction.bucket(clustering, 1, 64));
        assertEquals((int) (Integer) VECTORS[7][2 + 3], BucketFunction.bucket(clustering, 2, 64));
        try
        {
            BucketFunction.bucket(clustering, 3, 8);
            fail();
        }
        catch (IllegalArgumentException expected)
        {
            // more than the clustering has
        }
        try
        {
            BucketFunction.bucket(clustering, 0, 8);
            fail();
        }
        catch (IllegalArgumentException expected)
        {
            // at least one column
        }
    }

    /** The length prefix keeps {"ab","c"} and {"a","bc"} apart. */
    @Test
    public void lengthPrefixSeparatesColumns()
    {
        long one = BucketFunction.hash(Arrays.asList(ByteBufferUtil.bytes("ab"), ByteBufferUtil.bytes("c")));
        long two = BucketFunction.hash(Arrays.asList(ByteBufferUtil.bytes("a"), ByteBufferUtil.bytes("bc")));
        assertTrue(one != two);
        // and the hash does not consume the buffers it is given
        ByteBuffer value = ByteBufferUtil.bytes("abc");
        BucketFunction.hash(Arrays.asList(value));
        assertEquals(3, value.remaining());
    }

    @Test
    public void bucketColumnValue()
    {
        assertEquals(256, BucketFunction.physicalValue(1, 0));
        assertEquals(511, BucketFunction.physicalValue(1, 255));
        assertEquals(773, BucketFunction.physicalValue(3, 5));
        assertEquals(Integer.MAX_VALUE, BucketFunction.physicalValue(LayoutRegistry.MAX_INCARNATION - 1, 255));
        for (int[] bad : new int[][]{ { 0, 0 }, { LayoutRegistry.MAX_INCARNATION, 0 }, { 1, -1 }, { 1, 256 } })
        {
            try
            {
                BucketFunction.physicalValue(bad[0], bad[1]);
                fail(bad[0] + "," + bad[1]);
            }
            catch (IllegalArgumentException expected)
            {
                // out of range
            }
        }
        assertEquals(1, LayoutRegistry.HASH_ALGO);
        assertEquals(0L, BucketFunction.SEED);
    }
}
