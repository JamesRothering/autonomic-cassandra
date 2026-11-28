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

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DataRange;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.memtable.Memtable;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.index.transactions.UpdateTransaction;
import org.apache.cassandra.io.sstable.SSTableReadsListener;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.concurrent.OpOrder;

/**
 * Drives one of Cassandra's own memtables directly (no commit log, no flush to disk) and serialises its contents
 * in the Rust memtable's record format, so the two can be compared byte for byte. Shared by the differential
 * test and the benchmarks.
 */
final class JavaMemtableHarness
{
    private final ColumnFamilyStore cfs;
    private final OpOrder writeOrder = new OpOrder();
    private Memtable memtable;

    JavaMemtableHarness(ColumnFamilyStore cfs)
    {
        this.cfs = cfs;
        this.memtable = newMemtable();
    }

    /** Points the Rust spike at the library built by {@code cargo build --release}, unless already set. */
    static void setLibraryPathIfUnset()
    {
        if (System.getProperty(NativeMemtable.LIBRARY_PROPERTY) == null)
            System.setProperty(NativeMemtable.LIBRARY_PROPERTY,
                               new File("native/memtable-rs/target/release/libmemtable_jni.so").getAbsolutePath());
    }

    private Memtable newMemtable()
    {
        return cfs.metadata().params.memtable.factory()
                  .create(new AtomicReference<>(CommitLogPosition.NONE), cfs.metadata, cfs);
    }

    Memtable memtable()
    {
        return memtable;
    }

    TableMetadata metadata()
    {
        return cfs.metadata();
    }

    void put(PartitionUpdate update)
    {
        try (OpOrder.Group group = writeOrder.start())
        {
            memtable.put(update, UpdateTransaction.NO_OP, group, false);
        }
    }

    /** Switches out and discards the current memtable (releasing its allocator) and starts a new one. */
    void discardAndRenew()
    {
        OpOrder.Barrier barrier = writeOrder.newBarrier();
        barrier.issue();
        memtable.switchOut(barrier, new AtomicReference<>(CommitLogPosition.NONE));
        barrier.await();
        memtable.discard();
        memtable = newMemtable();
    }

    /** The partition as one record, or null if absent. */
    ByteBuffer read(DecoratedKey key, Slices slices) throws IOException
    {
        try (UnfilteredRowIterator partition = memtable.rowIterator(key, slices, ColumnFilter.all(metadata()), false,
                                                                    SSTableReadsListener.NOOP_LISTENER))
        {
            if (partition == null)
                return null;
            DataOutputBuffer out = new DataOutputBuffer();
            PartitionRecords.write(out, metadata(), partition);
            return out.asNewBuffer();
        }
    }

    /** Every partition, in token order, as concatenated records. */
    ByteBuffer readAll() throws IOException
    {
        DataOutputBuffer out = new DataOutputBuffer();
        try (UnfilteredPartitionIterator partitions = memtable.partitionIterator(ColumnFilter.all(metadata()),
                                                                                  DataRange.allData(cfs.getPartitioner()),
                                                                                  SSTableReadsListener.NOOP_LISTENER))
        {
            while (partitions.hasNext())
            {
                try (UnfilteredRowIterator partition = partitions.next())
                {
                    PartitionRecords.write(out, metadata(), partition);
                }
            }
        }
        return out.asNewBuffer();
    }

    /** Every partition of a frozen Rust memtable, as concatenated records from its flush iterator. */
    static ByteBuffer readAll(RustMemtable rust) throws IOException
    {
        DataOutputBuffer out = new DataOutputBuffer();
        try (RustMemtable.FlushIterator it = rust.openFlush())
        {
            ByteBuffer batch;
            while ((batch = it.nextBatch()) != null)
                out.write(batch);
        }
        return out.asNewBuffer();
    }
}
