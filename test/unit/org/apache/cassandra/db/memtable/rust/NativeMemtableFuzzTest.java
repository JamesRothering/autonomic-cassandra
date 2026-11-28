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

package org.apache.cassandra.db.memtable.rust;

import java.util.Random;

import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Throws random and corrupted input at every native method of {@link NativeMemtable}. The only acceptable outcomes
 * are a normal return or an {@link IllegalArgumentException} / {@link IllegalStateException}; a crash, an
 * {@link Error} or any other exception fails the test (a crash fails the JVM). No Cassandra setup is needed.
 * Iterations: {@code -Drust_memtable.fuzz_iterations=N} (default 200000).
 */
public class NativeMemtableFuzzTest
{
    private static final int ITERATIONS = SpikeProperties.getInt("rust_memtable.fuzz_iterations", 200_000);

    @BeforeClass
    public static void setUpLibrary()
    {
        JavaMemtableHarness.setLibraryPathIfUnset();
    }

    /** A valid one-row record: key, live partition deletion, one row with one live cell. */
    private static byte[] validRecord(Random random)
    {
        java.nio.ByteBuffer b = java.nio.ByteBuffer.allocate(128);
        byte[] key = new byte[1 + random.nextInt(4)];
        random.nextBytes(key);
        b.putInt(key.length).put(key);
        b.putLong(Long.MIN_VALUE).putLong(Long.MAX_VALUE);              // partition deletion: live
        b.putInt(1);                                                   // one row
        b.putInt(2).put((byte) random.nextInt(4)).put((byte) 0x38);    // clustering
        b.putLong(Long.MIN_VALUE).putInt(0).putLong(Long.MAX_VALUE);   // liveness: empty
        b.putLong(Long.MIN_VALUE).putLong(Long.MAX_VALUE).put((byte) 0); // row deletion: live
        b.putInt(1);                                                   // one cell
        b.putInt(random.nextInt(3)).putLong(random.nextInt(5)).putInt(0).putLong(Long.MAX_VALUE);
        b.putInt(1).put((byte) random.nextInt());
        byte[] out = new byte[b.position()];
        b.flip();
        b.get(out);
        return out;
    }

    @Test
    public void randomAndCorruptedInputNeverCrashes()
    {
        Random random = new Random(SpikeProperties.getLong("rust_memtable.seed", System.nanoTime()));
        long baseline = NativeMemtable.liveObjects();
        long memtable = NativeMemtable.create(new byte[]{ 0, 0, 0, 1, 0, 0, 0, 1, (byte) 0x80 }, 9);
        long accepted = 0, rejected = 0;
        byte[] out = new byte[64];
        for (int i = 0; i < ITERATIONS; i++)
        {
            byte[] input;
            switch (random.nextInt(4))
            {
                case 0: // random bytes
                    input = new byte[random.nextInt(80)];
                    random.nextBytes(input);
                    break;
                case 1: // valid record with one flipped byte
                    input = validRecord(random);
                    input[random.nextInt(input.length)] ^= (byte) (1 + random.nextInt(255));
                    break;
                case 2: // valid record, truncated
                    input = validRecord(random);
                    input = java.util.Arrays.copyOf(input, random.nextInt(input.length + 1));
                    break;
                default:
                    input = validRecord(random);
            }
            int len = random.nextInt(20) == 0 ? input.length + random.nextInt(3) - 1 : input.length;
            long handle = random.nextInt(50) == 0 ? random.nextLong() : memtable;
            try
            {
                switch (random.nextInt(10))
                {
                    case 0:
                        NativeMemtable.read(handle, input, len, out);
                        break;
                    case 1:
                        long h = NativeMemtable.create(input, len);
                        NativeMemtable.release(h);
                        break;
                    case 2:
                        NativeMemtable.flushNext(handle, out);
                        break;
                    default:
                        NativeMemtable.put(handle, input, len);
                }
                accepted++;
            }
            catch (IllegalArgumentException | IllegalStateException e)
            {
                rejected++;
            }
        }
        // The memtable must still be fully usable: freeze and flush everything it accepted.
        NativeMemtable.freeze(memtable);
        long it = NativeMemtable.openFlush(memtable);
        int n;
        long bytes = 0;
        while ((n = NativeMemtable.flushNext(it, out)) != 0)
        {
            if (n < 0)
                out = new byte[-n];
            else
                bytes += n;
        }
        NativeMemtable.closeFlush(it);
        NativeMemtable.release(memtable);
        assertEquals("native objects leaked", baseline, NativeMemtable.liveObjects());
        assertTrue("nothing was accepted; the generator is broken", accepted > 0 && bytes > 0);
        System.out.println("NativeMemtableFuzzTest: " + ITERATIONS + " calls, " + accepted + " accepted, " + rejected + " rejected, " + bytes + " bytes flushed");
    }
}
