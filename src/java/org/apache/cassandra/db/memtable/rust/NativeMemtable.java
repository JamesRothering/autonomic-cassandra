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

/**
 * The native methods of the Rust memtable spike ({@code native/memtable-rs/jni}). Nothing in Cassandra uses this
 * class unless a caller loads it; the library is loaded from the absolute path in the system property
 * {@value #LIBRARY_PROPERTY} on first use.
 * <p>
 * Every {@code long} handle is an opaque id, not a pointer: a stale or unknown handle throws
 * {@link IllegalStateException}. Malformed input throws {@link IllegalArgumentException}; a Rust panic throws
 * {@link RuntimeException}. Methods that fill a {@code byte[]} return the bytes written, 0 for nothing, or
 * {@code -n} when the result needs {@code n} bytes and the array is shorter (nothing is consumed; call again with a
 * larger array). The record formats are documented in {@code native/memtable-rs/core/src/codec.rs}.
 */
final class NativeMemtable
{
    static final String LIBRARY_PROPERTY = "cassandra.rust_memtable.library";

    static
    {
        // A spike-only property, deliberately not in CassandraRelevantProperties so that no core file changes.
        String path = System.getProperty(LIBRARY_PROPERTY); // checkstyle: suppress nearby 'blockSystemPropertyUsage'
        if (path == null)
            throw new IllegalStateException("Set -D" + LIBRARY_PROPERTY + " to the absolute path of libmemtable_jni.so");
        System.load(path);
    }

    private NativeMemtable()
    {
    }

    /**
     * @param request {@code kind:u32 count:u32 (len:u32 bytes)*}: the implementation ({@link RustMemtable.Kind}),
     *                then strictly increasing byte-comparable shard boundaries
     */
    static native long create(byte[] request, int len);

    /** Drops Java's reference; memory is freed once every flush iterator over the memtable is closed too. */
    static native void release(long handle);

    /** @return the change in owned bytes */
    static native long put(long handle, byte[] update, int len);

    /** @param request {@code key_len:u32 key start_len:u32 start end_len:i32 end}, {@code end_len = -1} for no end */
    static native int read(long handle, byte[] request, int len, byte[] out);

    static native long ownedBytes(long handle);

    static native long partitionCount(long handle);

    /** Stops writes; when it returns no write is in progress. */
    static native void freeze(long handle);

    /** Requires {@link #freeze}. The iterator keeps the memtable's memory alive until closed. */
    static native long openFlush(long handle);

    static native int flushNext(long iterator, byte[] out);

    static native void closeFlush(long iterator);

    /** Live memtables in the high 32 bits, live iterators in the low 32 bits; for leak checks. */
    static native long liveObjects();

    /** Returns {@code x} without touching the JNI environment; measures the bare JNI transition. */
    static native long nopRaw(long x);

    /** Returns {@code x} through the same environment upgrade and error policy as every other method. */
    static native long nop(long x);

    /** Panics in Rust; for tests of the panic-to-exception path. */
    static native void panicForTest();
}
