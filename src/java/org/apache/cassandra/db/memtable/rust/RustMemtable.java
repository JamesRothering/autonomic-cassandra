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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.db.ClusteringBound;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.bytecomparable.ByteSource;
import org.apache.cassandra.utils.bytecomparable.ByteSourceInverse;

/**
 * A memtable whose data lives in Rust ({@code native/memtable-rs}), behind {@link NativeMemtable}. This is a spike:
 * it is not a {@link org.apache.cassandra.db.memtable.Memtable} and nothing in Cassandra uses it.
 * <p>
 * Thread safety: {@link #put}, {@link #read} and the counters may be called from any number of threads at once.
 * {@link #freeze} stops writes. {@link #close} must not race with other calls on this instance (it is the
 * owner's "discard"); a call after close throws {@link IllegalStateException} from Rust rather than touching freed
 * memory.
 */
public final class RustMemtable implements AutoCloseable
{
    private static final int INITIAL_BUFFER = 4096;
    private static final ThreadLocal<DataOutputBuffer> REQUEST = ThreadLocal.withInitial(() -> new DataOutputBuffer(INITIAL_BUFFER));
    private static final ThreadLocal<byte[][]> RESPONSE = ThreadLocal.withInitial(() -> new byte[][]{ new byte[INITIAL_BUFFER] });

    private final long handle;

    /** The Rust implementation behind a memtable ({@code native/memtable-rs/jni/src/store.rs}). */
    public enum Kind
    {
        /** Each shard's data in a few large chunks, so discarding it is cheap (the default). */
        ARENA(0),
        /** Spike 1's implementation, one heap allocation per key, row and value; kept for comparison. */
        BOXED(1);

        final int code;

        Kind(int code)
        {
            this.code = code;
        }
    }

    private RustMemtable(long handle)
    {
        this.handle = handle;
    }

    /** Creates an arena memtable sharded at the given byte-comparable boundaries, which must be strictly increasing. */
    public static RustMemtable create(List<byte[]> boundaries)
    {
        return create(Kind.ARENA, boundaries);
    }

    /** Creates a memtable of the given kind sharded at the given byte-comparable boundaries. */
    public static RustMemtable create(Kind kind, List<byte[]> boundaries)
    {
        try (DataOutputBuffer out = new DataOutputBuffer())
        {
            out.writeInt(kind.code);
            out.writeInt(boundaries.size());
            for (byte[] b : boundaries)
            {
                out.writeInt(b.length);
                out.write(b);
            }
            return new RustMemtable(NativeMemtable.create(out.getData(), out.getLength()));
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Boundaries splitting the Murmur3 token range into {@code shards} equal parts. Each boundary is the
     * byte-comparable prefix of a decorated key with that token (the component separator, then the token), which
     * sorts before every key with an equal or greater token. Other partitioners get a single shard.
     */
    public static List<byte[]> tokenBoundaries(IPartitioner partitioner, int shards)
    {
        List<byte[]> boundaries = new ArrayList<>();
        if (!(partitioner instanceof Murmur3Partitioner))
            return boundaries;
        // (2^64 - 1) / shards, as an unsigned step; MIN_VALUE + i * step wraps correctly in two's complement.
        long step = Long.divideUnsigned(-1L, shards);
        for (int i = 1; i < shards; i++)
        {
            long token = Long.MIN_VALUE + i * step;
            byte[] tokenBytes = ByteSourceInverse.readBytes(new Murmur3Partitioner.LongToken(token).asComparableBytes(PartitionRecords.VERSION));
            byte[] boundary = new byte[tokenBytes.length + 1];
            boundary[0] = (byte) ByteSource.NEXT_COMPONENT;
            System.arraycopy(tokenBytes, 0, boundary, 1, tokenBytes.length);
            boundaries.add(boundary);
        }
        return boundaries;
    }

    /** @return the change in owned bytes */
    public long put(PartitionUpdate update)
    {
        DataOutputBuffer out = REQUEST.get();
        out.clear();
        try
        {
            PartitionRecords.write(out, update);
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
        return NativeMemtable.put(handle, out.getData(), out.getLength());
    }

    /** The whole partition as one record, or null if absent. */
    public ByteBuffer read(DecoratedKey key)
    {
        return read(key, null, null, null);
    }

    /**
     * The partition restricted to the rows between {@code start} and {@code end} (each null for unbounded), as one
     * record, or null if the partition is absent. The returned buffer is valid until this thread's next read.
     */
    public ByteBuffer read(DecoratedKey key, TableMetadata metadata, ClusteringBound<?> start, ClusteringBound<?> end)
    {
        DataOutputBuffer out = REQUEST.get();
        out.clear();
        try
        {
            PartitionRecords.writeKey(out, key);
            if (start == null)
                out.writeInt(0);
            else
                PartitionRecords.writeComparable(out, metadata.comparator.asByteComparable(start).asComparableBytes(PartitionRecords.VERSION));
            if (end == null)
                out.writeInt(-1);
            else
                PartitionRecords.writeComparable(out, metadata.comparator.asByteComparable(end).asComparableBytes(PartitionRecords.VERSION));
        }
        catch (IOException e)
        {
            throw new UncheckedIOException(e);
        }
        byte[][] response = RESPONSE.get();
        while (true)
        {
            int n = NativeMemtable.read(handle, out.getData(), out.getLength(), response[0]);
            if (n == 0)
                return null;
            if (n > 0)
                return ByteBuffer.wrap(response[0], 0, n);
            response[0] = new byte[Math.max(-n, response[0].length * 2)];
        }
    }

    public long ownedBytes()
    {
        return NativeMemtable.ownedBytes(handle);
    }

    public long partitionCount()
    {
        return NativeMemtable.partitionCount(handle);
    }

    /** Stops writes. When this returns no write is in progress; later writes throw. */
    public void freeze()
    {
        NativeMemtable.freeze(handle);
    }

    /** Requires {@link #freeze}. The iterator keeps the data alive until it is closed, even after {@link #close}. */
    public FlushIterator openFlush()
    {
        return new FlushIterator(NativeMemtable.openFlush(handle));
    }

    /** Releases Java's reference ("discard"). */
    @Override
    public void close()
    {
        NativeMemtable.release(handle);
    }

    /** Partitions in key order, in batches of whole partition records. Single-threaded. */
    public static final class FlushIterator implements AutoCloseable
    {
        private final long handle;
        private byte[] buffer = new byte[1 << 16];

        private FlushIterator(long handle)
        {
            this.handle = handle;
        }

        /** The next batch of records, or null at the end. Valid until the next call. */
        public ByteBuffer nextBatch()
        {
            while (true)
            {
                int n = NativeMemtable.flushNext(handle, buffer);
                if (n == 0)
                    return null;
                if (n > 0)
                    return ByteBuffer.wrap(buffer, 0, n);
                buffer = new byte[Math.max(-n, buffer.length * 2)];
            }
        }

        @Override
        public void close()
        {
            NativeMemtable.closeFlush(handle);
        }
    }
}
